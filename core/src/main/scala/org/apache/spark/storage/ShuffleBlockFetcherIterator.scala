/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import java.io.{File, InputStream, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashSet, Queue}

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, ShuffleClient}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBufferOutputStream

/**
 * An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
 * manager. For remote blocks, it fetches them using the provided BlockTransferService.
 *
 * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
 * in a pipelined fashion as they are received.
 *
 * The implementation throttles the remote fetches so they don't exceed maxBytesInFlight to avoid
 * using too much memory.
 *
 * @param context [[TaskContext]], used for metrics update
 * @param shuffleClient [[ShuffleClient]] for fetching remote blocks
 * @param blockManager [[BlockManager]] for reading local blocks
 * @param blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
 *                        For each block we also require the size (in bytes as a long field) in
 *                        order to throttle the memory usage.
 * @param streamWrapper A function to wrap the returned input stream.
 * @param maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.
 * @param maxReqsInFlight max number of remote requests to fetch blocks at any given point.
 * @param maxReqSizeShuffleToMem max size (in bytes) of a request that can be shuffled to memory.
 * @param detectCorrupt whether to detect any corruption in fetched blocks.
 */
private[spark]
final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: ShuffleClient,
    blockManager: BlockManager,
    blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqsInFlight: Int,
    maxReqSizeShuffleToMem: Long,
    detectCorrupt: Boolean)
  extends Iterator[(BlockId, InputStream)] with Logging {

  import ShuffleBlockFetcherIterator._

  /**
   * Total number of blocks to fetch. This can be smaller than the total number of blocks
   * in [[blocksByAddress]] because we filter out zero-sized blocks in [[initialize]].
   *
   * This should equal localBlocks.size + remoteBlocks.size.
   */
  private[this] var numBlocksToFetch = 0

  /**
   * The number of blocks processed by the caller. The iterator is exhausted when
   * [[numBlocksProcessed]] == [[numBlocksToFetch]].
   */
  private[this] var numBlocksProcessed = 0

  private[this] val startTime = System.currentTimeMillis

  /** Local blocks to fetch, excluding zero-sized blocks. */
  private[this] val localBlocks = new ArrayBuffer[BlockId]()

  /** Remote blocks to fetch, excluding zero-sized blocks. */
  private[this] val remoteBlocks = new HashSet[BlockId]()

  /**
   * A queue to hold our results. This turns the asynchronous model provided by
   * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
   */
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  /**
   * Current [[FetchResult]] being processed. We track this so we can release the current buffer
   * in case of a runtime exception when processing the current buffer.
   */
  @volatile private[this] var currentResult: SuccessFetchResult = null

  /**
   * Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
   * the number of bytes in flight is limited to maxBytesInFlight.
   */
  private[this] val fetchRequests = new Queue[FetchRequest]

  /** Current bytes in flight from our requests */
  private[this] var bytesInFlight = 0L

  /** Current number of requests in flight */
  private[this] var reqsInFlight = 0

  /**
   * The blocks that can't be decompressed successfully, it is used to guarantee that we retry
   * at most once for those corrupted blocks.
   */
  private[this] val corruptedBlocks = mutable.HashSet[BlockId]()

  private[this] val shuffleMetrics = context.taskMetrics().createTempShuffleReadMetrics()

  /**
   * Whether the iterator is still active. If isZombie is true, the callback interface will no
   * longer place fetched blocks into [[results]].
   */
  @GuardedBy("this")
  private[this] var isZombie = false

  /**
   * A set to store the files used for shuffling remote huge blocks. Files in this set will be
   * deleted when cleanup. This is a layer of defensiveness against disk file leaks.
   */
  val shuffleFilesSet = mutable.HashSet[File]()

  initialize()

  // Decrements the buffer reference count.
  // The currentResult is set to null to prevent releasing the buffer again on cleanup()
  private[storage] def releaseCurrentResultBuffer(): Unit = {
    // Release the current buffer if necessary
    if (currentResult != null) {
      currentResult.buf.release()
    }
    currentResult = null
  }

  /**
   * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
   */
  private[this] def cleanup() {
    synchronized {
      isZombie = true
    }
    releaseCurrentResultBuffer()
    // Release buffers in the results queue
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(_, address, _, buf, _) =>
          if (address != blockManager.blockManagerId) {
            shuffleMetrics.incRemoteBytesRead(buf.size)
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          buf.release()
        case _ =>
      }
    }
    shuffleFilesSet.foreach { file =>
      if (!file.delete()) {
        logInfo("Failed to cleanup shuffle fetch temp file " + file.getAbsolutePath());
      }
    }
  }

  /**
    * sendRequest方法用于远程请求中间结果。sendRequest利用FetchRequest里封装的blockId,size,address等信息，调用shuffleClient
    * 的fetchBlocks方法获取其他节点上的中间计算结果。
    */
  private[this] def sendRequest(req: FetchRequest) {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    bytesInFlight += req.size
    reqsInFlight += 1

    // so we can look up the size of each blockID
    val sizeMap = req.blocks.map { case (blockId, size) => (blockId.toString, size) }.toMap
    val remainingBlocks = new HashSet[String]() ++= sizeMap.keys
    val blockIds = req.blocks.map(_._1.toString)
    val address = req.address

    val blockFetchingListener = new BlockFetchingListener {
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        // Only add the buffer to results queue if the iterator is not zombie,
        // i.e. cleanup() has not been called yet.
        ShuffleBlockFetcherIterator.this.synchronized {
          if (!isZombie) {
            // Increment the ref count because we need to pass this to a different thread.
            // This needs to be released after use.
            buf.retain()
            remainingBlocks -= blockId
            results.put(new SuccessFetchResult(BlockId(blockId), address, sizeMap(blockId), buf,
              remainingBlocks.isEmpty))
            logDebug("remainingBlocks: " + remainingBlocks)
          }
        }
        logTrace("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
      }

      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
        results.put(new FailureFetchResult(BlockId(blockId), address, e))
      }
    }

    // Fetch remote shuffle blocks to disk when the request is too large. Since the shuffle data is
    // already encrypted and compressed over the wire(w.r.t. the related configs), we can just fetch
    // the data and write it to file directly.
    if (req.size > maxReqSizeShuffleToMem) {
      val shuffleFiles = blockIds.map { _ =>
        blockManager.diskBlockManager.createTempLocalBlock()._2
      }.toArray
      shuffleFilesSet ++= shuffleFiles
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, shuffleFiles)
    } else {
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, null)
    }
  }

  /**
    * splitLocalRemoteBlocks方法用于划分哪些Block从本地获取，哪些需要远程拉取，是获取中间结果的关键。为了便与描述，先解释一下定义：
    *   1.targetRequestSize:每个远程请求的最大尺寸。
    *   2.totalBlocks:统计Block
    *   3.localBlocks:ArrayBuffer[BlockId]，缓存可以在本地获取的Block的BlockId.
    *   4.remoteBlocks:HashSet[BlockId],缓存需要远程获取的Block的blockId。
    *   5.curBlocks:ArrayBuffer[(BlockId,Long)],远程获取的累加缓存，用于保证每个远程请求的尺寸不超过targetRequestSize.
    *     为什么要累加缓存呢？如果向一个机器节点频繁的请求字节数很小的Block，那么势必造成网络拥塞并且增加节点负担。将多个小数据量
    *     的请求合并成一个大的请求将避免这些问题，提高系统性能。
    *   6.curRequestSize：当前累加到curBlocks中的所有Block的大小之和，用于保证每个远程请求的尺寸不超过TargetRequestSize.
    *   7.remoteRequests:new ArrayBuffer[FetchRequest],缓存需要远程请求的FetchRequest对象。
    *   8.numBlocksToFetch:一共要获取的Block数量。
    *   9.maxBytesInFlight:单次航班请求的最大字节数，什么叫航班？其实就是一批请求，这批请求的字节总数不能超过maxBytesInFlight,
    *     而且每个请求的字节数不能超过maxBytesInFlight的五分之一，可以通过参数spark.reducer.maxMbInFlight来控制大小。为什么
    *     每个请求的字节数不能超过maxBytesInFlight的五分之一呢？这样做是为了提高请求的并发度，允许5个请求分别从5个节点获取数据，
    *     最大限度的利用各个节点的资源。
    *
    * 下面是splitLocalRemoteBlocks的处理逻辑：
    *   1.遍历已经在BlockStoreShuffleFetcher的fetch方法中按照BlockManagerId分组的blockInfo,如果blockInfo所在的Executor
    *     与当前的Executor相同，则将它的BlockId存入localBlocks，否则，将blockInfo的BlockId和size累加到curBlocks，将blockId
    *     存入remoteBlocks，curRequestSize增加size的大小，每当curRequestSize 》= targetRequestSize，则新建FetchRequest放入
    *     remoteRequests,并且为生成下一个FetchRequest做一些准备（如curBlocks，curRequestSize置位0）。
    *   2.遍历结束，curBlocks中如果仍然有缓存的（BlockId,Long）,新建FetchRequest放入remoteRequests，此次请求不受
    *     maxBytesInFlight和targetRequestSize的影响。
    *
    */
  private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
    // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
    // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
    // nodes, rather than blocking on reading output from one node.
    // 在大多数 maxBytesInFlight/5 中进行远程请求;让它们比maxBytesInFlight更小的原因是允许多个并行获取最多5个节点，而不是阻塞读取一个节点的输出。
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
    logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize)

    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytesInFlight in order to limit the amount of data in flight.
    // 分割本地和远程块。在maxBytesInFlight中，为了限制飞行中的数据量，远程块被进一步拆分为最大尺寸的FetchRequests。
    val remoteRequests = new ArrayBuffer[FetchRequest]

    // Tracks total number of blocks (including zero sized blocks) 跟踪块的总数(包括零大小块)
    var totalBlocks = 0
    // 遍历已经在BlockStoreShuffleFetcher的fetch方法中按照BlockManagerId分组的blockInfo
    for ((address, blockInfos) <- blocksByAddress) {
      totalBlocks += blockInfos.size
      // 如果blockInfo所在的Executor,与当前的Executor相同，则将它的BlockId存入localBlocks
      if (address.executorId == blockManager.blockManagerId.executorId) {
        // Filter out zero-sized blocks
        localBlocks ++= blockInfos.filter(_._2 != 0).map(_._1)
        numBlocksToFetch += localBlocks.size
      } else {
        // 否则，将blockInfo的BlockId和size累加到curBlocks，将blockId
        // 存入remoteBlocks，curRequestSize增加size的大小，每当curRequestSize 》= targetRequestSize，则新建FetchRequest放入
        // remoteRequests,并且为生成下一个FetchRequest做一些准备（如curBlocks，curRequestSize置位0）。
        val iterator = blockInfos.iterator
        var curRequestSize = 0L
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          // Skip empty blocks
          if (size > 0) {
            curBlocks += ((blockId, size))
            remoteBlocks += blockId
            numBlocksToFetch += 1
            curRequestSize += size
          } else if (size < 0) {
            throw new BlockException(blockId, "Negative block size " + size)
          }
          if (curRequestSize >= targetRequestSize) {
            // Add this FetchRequest
            remoteRequests += new FetchRequest(address, curBlocks)
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            logDebug(s"Creating fetch request of $curRequestSize at $address")
            curRequestSize = 0
          }
        }
        // Add in the final request
        if (curBlocks.nonEmpty) {
          remoteRequests += new FetchRequest(address, curBlocks)
        }
      }
    }
    logInfo(s"Getting $numBlocksToFetch non-empty blocks out of $totalBlocks blocks")
    remoteRequests
  }

  /**
   * Fetch the local blocks while we are fetching remote blocks. This is ok because
   * `ManagedBuffer`'s memory is allocated lazily when we create the input stream, so all we
   * track in-memory are the ManagedBuffer references themselves.
    *
    * fetchLocalBlocks用于对本地中间计算结果的获取，fetchLocalBlocks方法很简单，利用熟悉的BlockManager的getBlockData方法
    * 获取本地Block，最后将取到的中间结果存入result = new LinkedBlockingQueue[FetchResult]中。
   */
  private[this] def fetchLocalBlocks() {
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val blockId = iter.next()
      try {
        val buf = blockManager.getBlockData(blockId)
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        buf.retain()
        results.put(new SuccessFetchResult(blockId, blockManager.blockManagerId, 0, buf, false))
      } catch {
        case e: Exception =>
          // If we see an exception, stop immediately.
          logError(s"Error occurred while fetching local blocks", e)
          results.put(new FailureFetchResult(blockId, blockManager.blockManagerId, e))
          return
      }
    }
  }

  /**
    * 划分本地和远程Block
    *   无论是从本地还是从MapOutputTrackerMasterActor获取的状态信息，都需要按照地址划分并且转换为BlockId,
    * ShuffleBlockFetcherIterator是读取中间结果的关键。构造ShuffleBlockFetcherIterator的时候会调用initialize，
    * 它的初始化过程如下：
    *   1.使用splitLocalRemoteBlocks方法划分本地读取和远程读取的Block的请求。
    *   2.将FetchRequest随机排序后存入fetchRequests: newQueue[FetchRequest].
    *   3.遍历fetchRequests中的所有FetchRequest，远程请求Block中间结果。
    *   4.调用fetchLocalBlocks获取本地Block.
    */
  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    // 添加任务完成回调(在成功案例和失败案例中都被调用)来进行清理。
    context.addTaskCompletionListener(_ => cleanup())

    // Split local and remote blocks.
    // 1.使用splitLocalRemoteBlocks方法划分本地读取和远程读取的Block的请求。
    val remoteRequests = splitLocalRemoteBlocks()
    // Add the remote requests into our queue in a random order
    // 2.将FetchRequest随机排序后存入fetchRequests: newQueue[FetchRequest].
    fetchRequests ++= Utils.randomize(remoteRequests)
    assert ((0 == reqsInFlight) == (0 == bytesInFlight),
      "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
      ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    // 3.遍历fetchRequests中的所有FetchRequest，远程请求Block中间结果。
    fetchUpToMaxBytes()

    val numFetches = remoteRequests.size - fetchRequests.size
    logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

    // Get Local Blocks
    // 4.调用fetchLocalBlocks获取本地Block.
    fetchLocalBlocks()
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime))
  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  /**
   * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
   * underlying each InputStream will be freed by the cleanup() method registered with the
   * TaskCompletionListener. However, callers should close() these InputStreams
   * as soon as they are no longer needed, in order to release memory as early as possible.
   *
   * Throws a FetchFailedException if the next block could not be fetched.
   */
  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw new NoSuchElementException
    }

    numBlocksProcessed += 1

    var result: FetchResult = null
    var input: InputStream = null
    // Take the next fetched result and try to decompress it to detect data corruption,
    // then fetch it one more time if it's corrupt, throw FailureFetchResult if the second fetch
    // is also corrupt, so the previous stage could be retried.
    // For local shuffle block, throw FailureFetchResult for the first IOException.
    while (result == null) {
      val startFetchWait = System.currentTimeMillis()
      result = results.take()
      val stopFetchWait = System.currentTimeMillis()
      shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

      result match {
        case r @ SuccessFetchResult(blockId, address, size, buf, isNetworkReqDone) =>
          if (address != blockManager.blockManagerId) {
            shuffleMetrics.incRemoteBytesRead(buf.size)
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          bytesInFlight -= size
          if (isNetworkReqDone) {
            reqsInFlight -= 1
            logDebug("Number of requests in flight " + reqsInFlight)
          }

          val in = try {
            buf.createInputStream()
          } catch {
            // The exception could only be throwed by local shuffle block
            case e: IOException =>
              assert(buf.isInstanceOf[FileSegmentManagedBuffer])
              logError("Failed to create input stream from local block", e)
              buf.release()
              throwFetchFailedException(blockId, address, e)
          }

          input = streamWrapper(blockId, in)
          // Only copy the stream if it's wrapped by compression or encryption, also the size of
          // block is small (the decompressed block is smaller than maxBytesInFlight)
          if (detectCorrupt && !input.eq(in) && size < maxBytesInFlight / 3) {
            val originalInput = input
            val out = new ChunkedByteBufferOutputStream(64 * 1024, ByteBuffer.allocate)
            try {
              // Decompress the whole block at once to detect any corruption, which could increase
              // the memory usage tne potential increase the chance of OOM.
              // TODO: manage the memory used here, and spill it into disk in case of OOM.
              Utils.copyStream(input, out)
              out.close()
              input = out.toChunkedByteBuffer.toInputStream(dispose = true)
            } catch {
              case e: IOException =>
                buf.release()
                if (buf.isInstanceOf[FileSegmentManagedBuffer]
                  || corruptedBlocks.contains(blockId)) {
                  throwFetchFailedException(blockId, address, e)
                } else {
                  logWarning(s"got an corrupted block $blockId from $address, fetch again", e)
                  corruptedBlocks += blockId
                  fetchRequests += FetchRequest(address, Array((blockId, size)))
                  result = null
                }
            } finally {
              // TODO: release the buf here to free memory earlier
              originalInput.close()
              in.close()
            }
          }

        case FailureFetchResult(blockId, address, e) =>
          throwFetchFailedException(blockId, address, e)
      }

      // Send fetch requests up to maxBytesInFlight
      fetchUpToMaxBytes()
    }

    currentResult = result.asInstanceOf[SuccessFetchResult]
    (currentResult.blockId, new BufferReleasingInputStream(input, this))
  }

  private def fetchUpToMaxBytes(): Unit = {
    // Send fetch requests up to maxBytesInFlight
    while (fetchRequests.nonEmpty &&
      (bytesInFlight == 0 ||
        (reqsInFlight + 1 <= maxReqsInFlight &&
          bytesInFlight + fetchRequests.front.size <= maxBytesInFlight))) {
      sendRequest(fetchRequests.dequeue())
    }
  }

  private def throwFetchFailedException(blockId: BlockId, address: BlockManagerId, e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block", e)
    }
  }
}

/**
 * Helper class that ensures a ManagedBuffer is released upon InputStream.close()
 */
private class BufferReleasingInputStream(
    private val delegate: InputStream,
    private val iterator: ShuffleBlockFetcherIterator)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      iterator.releaseCurrentResultBuffer()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = delegate.skip(n)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

private[storage]
object ShuffleBlockFetcherIterator {

  /**
   * A request to fetch blocks from a remote BlockManager.
   * @param address remote BlockManager to fetch from.
   * @param blocks Sequence of tuple, where the first element is the block id,
   *               and the second element is the estimated size, used to calculate bytesInFlight.
   */
  case class FetchRequest(address: BlockManagerId, blocks: Seq[(BlockId, Long)]) {
    val size = blocks.map(_._2).sum
  }

  /**
   * Result of a fetch from a remote block.
   */
  private[storage] sealed trait FetchResult {
    val blockId: BlockId
    val address: BlockManagerId
  }

  /**
   * Result of a fetch from a remote block successfully.
   * @param blockId block id
   * @param address BlockManager that the block was fetched from.
   * @param size estimated size of the block, used to calculate bytesInFlight.
   *             Note that this is NOT the exact bytes.
   * @param buf `ManagedBuffer` for the content.
   * @param isNetworkReqDone Is this the last network request for this host in this fetch request.
   */
  private[storage] case class SuccessFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      size: Long,
      buf: ManagedBuffer,
      isNetworkReqDone: Boolean) extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  /**
   * Result of a fetch from a remote block unsuccessfully.
   * @param blockId block id
   * @param address BlockManager that the block was attempted to be fetched from
   * @param e the failure exception
   */
  private[storage] case class FailureFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      e: Throwable)
    extends FetchResult
}
