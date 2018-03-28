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

package org.apache.spark.broadcast

import java.io._
import java.nio.ByteBuffer
import java.util.zip.Adler32

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage._
import org.apache.spark.util.{ByteBufferInputStream, Utils}
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

/**
  * A BitTorrent-like implementation of [[org.apache.spark.broadcast.Broadcast]].
  *
  * BitTorrent实现[[org.apache.spark.broadcast.Broadcast]]。
  *
  * The mechanism is as follows:   其机制如下：
  *
  * The driver divides the serialized object into small chunks and
  * stores those chunks in the BlockManager of the driver.
  *
  * 驱动程序driver将序列化的对象分为小块并存储在驱动器的blockmanager中。
  *
  * On each executor, the executor first attempts to fetch the object from its BlockManager. If
  * it does not exist, it then uses remote fetches to fetch the small chunks from the driver and/or
  * other executors if available. Once it gets the chunks, it puts the chunks in its own
  * BlockManager, ready for other executors to fetch from.
  *
  * 在每一个执行者，执行者首先尝试从blockmanager获取对象。如果它不存在，那么它将使用远程从驱动程序and/or其他执行器获取小的块，
  * 如果可用的话。一旦获取它的块，将块放在自己的blockmanager，准备好让其他的executors去获取它。
  * （大概意思就是我没有，我就去从别的地方拿来，然后自己用，而且别人用的时候到我这里来拿）
  *
  *
  * This prevents the driver from being the bottleneck in sending out multiple copies of the
  * broadcast data (one per executor).
  *
  * 这可以防止驱动程序成为发送广播数据的多个副本的瓶颈（每个执行器中有一个）。
  *
  *
  * When initialized, TorrentBroadcast objects read SparkEnv.get.conf.
  * 当torrentbroadcast对象初始化的时候读取SparkEnv.get.conf。
  *
  * @param obj object to broadcast   广播对象
  * @param id A unique identifier for the broadcast variable.  编播变量的标识
  *
  *
  * TorrentBroadcast的过程分为三步。
  *   1.设置广播配置信息。根据spark.broadcast.compress配置属性确认是否对广播消息进行压缩，并且生成CompressionCodec对象。
  *     根据spark.broadcast.blockSize配置属性确认块的大小，默认为4MB.
  *   2.生成BroadcastBlockId。
  *   3.块的写入操作，返回广播变量包含的块数。
  *
  *  整体解释：driver把序列化后的对象(即value)分块很多块，并且把这些块存到driver的BlockManager里。
  *  在executor端，executor首先试图从自己的BlockManager中获取被broadcast变量的块，如果它不存在，就使用远程抓取从driver 以及/或者其它的
  *  executor上获取这个块。当executor获取了一个块，它就把这个块放在自己的BlockManager里，以使得其它的executor可以抓取它。
  *  这防止了被广播的数据只从driver端被拷贝，这样当要拷贝的次数很多的时候(每个executor都会拷贝一次)，driver端容易成为瓶颈(就像HttpBroadcast所做的一样)
  *
  *  这段注释时的代词用得不准确，executor是没有专门的机制用于处理Broadcast变量的，所有的魔法都在Broadcast变量本身。可以这么描述：
  *     driver端把数据分块，每个块做为一个block存进driver端的BlockManager，每个executor会试图获取所有的块，
  *   来组装成一个被broadcast的变量。“获取块”的方法是首先从executor自身的BlockManager中获取，如果自己的BlockManager
  *   中没有这个块，就从别的BlockManager中获取。这样最初的时候，driver是获取这些块的唯一的源，但是随着各个BlockManager
  *   从driver端获取了不同的块(TorrentBroadcast会有意避免各个executor以同样的顺序获取这些块)，“块”的源就多了起来，
  *   每个executor就可能从多个源中的一个,包括driver和其它executor的BlockManager中获取块，这要就使得流量在整个集群中更均匀，
  *   而不是由driver作为唯一的源。
  */
private[spark] class TorrentBroadcast[T: ClassTag](obj: T, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {

  /**
    * Value of the broadcast object on executors. This is reconstructed by [[readBroadcastBlock]],
    * which builds this value by reading blocks from the driver and/or other executors.
    *
    * 在执行器executors中的广播对象的值。这是[[readBroadcastBlock]]重建的，
    * 它重建这些值是通过读取驱动程序或者（和）其他的执行者executors的块blocks。
    *
    * On the driver, if the value is required, it is read lazily from the block manager.
    * 在驱动程序中，如果该值是必需的，它是从块管理者中懒加载的读取数据。
    *
    */
  @transient private lazy val _value: T = readBroadcastBlock()

  /** The compression codec to use, or None if compression is disabled
    * 压缩编解码器的使用，或者如果设置none,MAME就是压缩被禁用
    * */
  @transient private var compressionCodec: Option[CompressionCodec] = _

  /** Size of each block. Default value is 4MB.  This value is only read by the broadcaster.
    * 每个block块的大小，默认是4MB,这个数据仅仅被广播者（broadcaster）读取
    * */
  @transient private var blockSize: Int = _

  private def setConf(conf: SparkConf) {
    compressionCodec = if (conf.getBoolean("spark.broadcast.compress", true)) {
      Some(CompressionCodec.createCodec(conf))
    } else {
      None
    }
    // Note: use getSizeAsKb (not bytes) to maintain compatibility if no units are provided
    blockSize = conf.getSizeAsKb("spark.broadcast.blockSize", "4m").toInt * 1024
    checksumEnabled = conf.getBoolean("spark.broadcast.checksum", true)
  }
  setConf(SparkEnv.get.conf)

  private val broadcastId = BroadcastBlockId(id)

  /** Total number of blocks this broadcast variable contains.
    * 这个广播变量总共包含了多少块
    * */
  private val numBlocks: Int = writeBlocks(obj)

  /** Whether to generate checksum for blocks or not.
    *  是否为块生成校验和。
    * */
  private var checksumEnabled: Boolean = false


  /** The checksum for all the blocks.
    * 所有块的校验和
    * */
  private var checksums: Array[Int] = _

  //获取值
  override protected def getValue() = {
    _value
  }

  private def calcChecksum(block: ByteBuffer): Int = {
    val adler = new Adler32()
    if (block.hasArray) {
      adler.update(block.array, block.arrayOffset + block.position, block.limit - block.position)
    } else {
      val bytes = new Array[Byte](block.remaining())
      block.duplicate.get(bytes)
      adler.update(bytes)
    }
    adler.getValue.toInt
  }

  /**
    * Divide the object into multiple blocks and put those blocks in the block manager.
    * 将对象分成多个块，把这些块的put给块管理者。
    *
    * @param value the object to divide
    * @return number of blocks this broadcast variable is divided into
    *
    * writeBlocks的工作分为3步：
    *   1.将要写入的对象在本地的存储体系中备份一份，以便于Task也可以在本地的Driver上运行。
    *   2.给ByteArrayChunkOutputStream指定压缩算法。并且将对象以序列化方式写入ByteArrayChunkOutputStream后转换为Array[ByteBuffer].
    *   3.将每一个ByteBuffer作为一个Block，使用putbytes方法写入存储体系。
    */
  private def writeBlocks(value: T): Int = {
    import StorageLevel._
    // Store a copy of the broadcast variable in the driver so that tasks run on the driver
    // do not create a duplicate copy of the broadcast variable's value.
    val blockManager = SparkEnv.get.blockManager
    if (!blockManager.putSingle(broadcastId, value, MEMORY_AND_DISK, tellMaster = false)) {
      throw new SparkException(s"Failed to store $broadcastId in BlockManager")
    }
    val blocks =
      TorrentBroadcast.blockifyObject(value, blockSize, SparkEnv.get.serializer, compressionCodec)
    if (checksumEnabled) {
      checksums = new Array[Int](blocks.length)
    }
    blocks.zipWithIndex.foreach { case (block, i) =>
      if (checksumEnabled) {
        checksums(i) = calcChecksum(block)
      }
      val pieceId = BroadcastBlockId(id, "piece" + i)
      val bytes = new ChunkedByteBuffer(block.duplicate())
      if (!blockManager.putBytes(pieceId, bytes, MEMORY_AND_DISK_SER, tellMaster = true)) {
        throw new SparkException(s"Failed to store $pieceId of $broadcastId in local BlockManager")
      }
    }
    blocks.length
  }

  /** Fetch torrent blocks from the driver and/or other executors.
    * 从驱动程序或者（和）其他的执行者executors连续不断的获取块
    * */
  private def readBlocks(): Array[BlockData] = {
    // Fetch chunks of data. Note that all these chunks are stored in the BlockManager and reported
    // to the driver, so other executors can pull these chunks from this executor as well.
    val blocks = new Array[BlockData](numBlocks)
    val bm = SparkEnv.get.blockManager

    for (pid <- Random.shuffle(Seq.range(0, numBlocks))) {
      val pieceId = BroadcastBlockId(id, "piece" + pid)
      logDebug(s"Reading piece $pieceId of $broadcastId")
      // First try getLocalBytes because there is a chance that previous attempts to fetch the
      // broadcast blocks have already fetched some of the blocks. In that case, some blocks
      // would be available locally (on this executor).
      bm.getLocalBytes(pieceId) match {
        case Some(block) =>
          blocks(pid) = block
          releaseLock(pieceId)
        case None =>
          bm.getRemoteBytes(pieceId) match {
            case Some(b) =>
              if (checksumEnabled) {
                val sum = calcChecksum(b.chunks(0))
                if (sum != checksums(pid)) {
                  throw new SparkException(s"corrupt remote block $pieceId of $broadcastId:" +
                    s" $sum != ${checksums(pid)}")
                }
              }
              // We found the block from remote executors/driver's BlockManager, so put the block
              // in this executor's BlockManager.
              if (!bm.putBytes(pieceId, b, StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)) {
                throw new SparkException(
                  s"Failed to store $pieceId of $broadcastId in local BlockManager")
              }
              blocks(pid) = new ByteBufferBlockData(b, true)
            case None =>
              throw new SparkException(s"Failed to get $pieceId of $broadcastId")
          }
      }
    }
    blocks
  }

  /**
    * Remove all persisted state associated with this Torrent broadcast on the executors.
    * 在执行者executors上移除所有已经持久化的state，这些state和当前的广播有关联
    */
  override protected def doUnpersist(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = false, blocking)
  }

  /**
    * Remove all persisted state associated with this Torrent broadcast on the executors
    * and driver.
    * 在执行者executors和驱动程序driver上移除所有已经持久化的state，这些state和当前的广播有关联
    */
  override protected def doDestroy(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  /** Used by the JVM when serializing this object.
    * 由JVM在序列化该对象的时候使用。
    * */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    assertValid()
    out.defaultWriteObject()
  }

  private def readBroadcastBlock(): T = Utils.tryOrIOException {
    TorrentBroadcast.synchronized {
      setConf(SparkEnv.get.conf)
      val blockManager = SparkEnv.get.blockManager
      blockManager.getLocalValues(broadcastId) match {
        case Some(blockResult) =>
          if (blockResult.data.hasNext) {
            val x = blockResult.data.next().asInstanceOf[T]
            releaseLock(broadcastId)
            x
          } else {
            throw new SparkException(s"Failed to get locally stored broadcast data: $broadcastId")
          }
        case None =>
          logInfo("Started reading broadcast variable " + id)
          val startTimeMs = System.currentTimeMillis()
          val blocks = readBlocks()
          logInfo("Reading broadcast variable " + id + " took" + Utils.getUsedTimeMs(startTimeMs))

          try {
            val obj = TorrentBroadcast.unBlockifyObject[T](
              blocks.map(_.toInputStream()), SparkEnv.get.serializer, compressionCodec)
            // Store the merged copy in BlockManager so other tasks on this executor don't
            // need to re-fetch it.
            val storageLevel = StorageLevel.MEMORY_AND_DISK
            if (!blockManager.putSingle(broadcastId, obj, storageLevel, tellMaster = false)) {
              throw new SparkException(s"Failed to store $broadcastId in BlockManager")
            }
            obj
          } finally {
            blocks.foreach(_.dispose())
          }
      }
    }
  }

  /**
    * If running in a task, register the given block's locks for release upon task completion.
    * Otherwise, if not running in a task then immediately release the lock.
    *
    * 如果运行在一个任务中，在任务完成时将给定的块锁注册为释放，否则，如果不在任务中运行，那么立即释放锁。
    */
  private def releaseLock(blockId: BlockId): Unit = {
    val blockManager = SparkEnv.get.blockManager
    Option(TaskContext.get()) match {
      case Some(taskContext) =>
        taskContext.addTaskCompletionListener(_ => blockManager.releaseLock(blockId))
      case None =>
        // This should only happen on the driver, where broadcast variables may be accessed
        // outside of running tasks (e.g. when computing rdd.partitions()). In order to allow
        // broadcast variables to be garbage collected we need to free the reference here
        // which is slightly unsafe but is technically okay because broadcast variables aren't
        // stored off-heap.
        blockManager.releaseLock(blockId)
    }
  }

}


private object TorrentBroadcast extends Logging {

  def blockifyObject[T: ClassTag](
                                   obj: T,
                                   blockSize: Int,
                                   serializer: Serializer,
                                   compressionCodec: Option[CompressionCodec]): Array[ByteBuffer] = {
    val cbbos = new ChunkedByteBufferOutputStream(blockSize, ByteBuffer.allocate)
    val out = compressionCodec.map(c => c.compressedOutputStream(cbbos)).getOrElse(cbbos)
    val ser = serializer.newInstance()
    val serOut = ser.serializeStream(out)
    Utils.tryWithSafeFinally {
      serOut.writeObject[T](obj)
    } {
      serOut.close()
    }
    cbbos.toChunkedByteBuffer.getChunks()
  }

  def unBlockifyObject[T: ClassTag](
                                     blocks: Array[InputStream],
                                     serializer: Serializer,
                                     compressionCodec: Option[CompressionCodec]): T = {
    require(blocks.nonEmpty, "Cannot unblockify an empty array of blocks")
    val is = new SequenceInputStream(blocks.iterator.asJavaEnumeration)
    val in: InputStream = compressionCodec.map(c => c.compressedInputStream(is)).getOrElse(is)
    val ser = serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    val obj = Utils.tryWithSafeFinally {
      serIn.readObject[T]()
    } {
      serIn.close()
    }
    obj
  }

  /**
    * Remove all persisted blocks associated with this torrent broadcast on the executors.
    * If removeFromDriver is true, also remove these persisted blocks on the driver.
    *
    * 删除所有的坚持与这股洪流块播出的执行者有关。如果removefromdriver是真实的，也将对司机坚持块。
    */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    logDebug(s"Unpersisting TorrentBroadcast $id")
    SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
  }
}
