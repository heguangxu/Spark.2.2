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

package org.apache.spark.network

import java.io.{Closeable, File}
import java.nio.ByteBuffer

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, ShuffleClient}
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.ThreadUtils

private[spark]
abstract class BlockTransferService extends ShuffleClient with Closeable with Logging {

  /**
   * Initialize the transfer service by giving it the BlockDataManager that can be used to fetch
   * local blocks or put local blocks.
    * 通过提供可以用来获取本地块或放置本地块的BlockDataManager来初始化传输服务。
   */
  def init(blockDataManager: BlockDataManager): Unit

  /**
   * Tear down the transfer service.
    * 拆除transfer服务
   */
  def close(): Unit

  /**
   * Port number the service is listening on, available only after [[init]] is invoked.
    * 服务正在监听的端口号，只有在[[init]]调用后才可用。
   */
  def port: Int

  /**
   * Host name the service is listening on, available only after [[init]] is invoked.
    * 服务的主机名是监听的，只有在[[init]]调用后才可以使用。
   */
  def hostName: String

  /**
   * Fetch a sequence of blocks from a remote node asynchronously,
   * available only after [[init]] is invoked.
    * 以异步方式从远程节点获取块序列，仅在[[init]]调用后才可用。
   *
   * Note that this API takes a sequence so the implementation can batch requests, and does not
   * return a future so the underlying implementation can invoke onBlockFetchSuccess as soon as
   * the data of a block is fetched, rather than waiting for all blocks to be fetched.
    *
    * 请注意，这个API采用了一个序列，因此实现可以批量请求，而且不会返回一个future，因此底层实现可以在一个块的数据被获取时调用
    * onBlockFetchSuccess，而不是等待所有的块都被获取。
   */
  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener,
      shuffleFiles: Array[File]): Unit

  /**
   * Upload a single block to a remote node, available only after [[init]] is invoked.
    * 将单个块上载到远程节点，仅在[[init]]之后才可使用。
    *
   */
  def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Future[Unit]

  /**
   * A special case of [[fetchBlocks]], as it fetches only one block and is blocking.
    * 一个特殊的例子[[fetchBlocks]]，因为它只读取一个块并且阻塞。
   *
   * It is also only available after [[init]] is invoked.
    * 只有在调用[[init]]后才可以使用它。
   */
  def fetchBlockSync(host: String, port: Int, execId: String, blockId: String): ManagedBuffer = {
    // 监控等待的线程.
    // A monitor for the thread to wait on.
    val result = Promise[ManagedBuffer]()
    fetchBlocks(host, port, execId, Array(blockId),
      new BlockFetchingListener {
        override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
          result.failure(exception)
        }
        override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
          val ret = ByteBuffer.allocate(data.size.toInt)
          ret.put(data.nioByteBuffer())
          ret.flip()
          result.success(new NioManagedBuffer(ret))
        }
      }, shuffleFiles = null)
    ThreadUtils.awaitResult(result.future, Duration.Inf)
  }

  /**
   * Upload a single block to a remote node, available only after [[init]] is invoked.
    * 将单个块上载到远程节点，仅在[[init]]之后才可使用。
   *
   * This method is similar to [[uploadBlock]], except this one blocks the thread
   * until the upload finishes.
    * 这种方法类似于[[uploadBlock]]，除非这个方法阻塞线程，直到上传完成。
   */
  def uploadBlockSync(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Unit = {
    val future = uploadBlock(hostname, port, execId, blockId, blockData, level, classTag)
    ThreadUtils.awaitResult(future, Duration.Inf)
  }
}
