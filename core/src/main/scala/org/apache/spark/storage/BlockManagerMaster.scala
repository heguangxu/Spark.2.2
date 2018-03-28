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

import scala.collection.Iterable
import scala.collection.generic.CanBuildFrom
import scala.concurrent.Future

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{RpcUtils, ThreadUtils}

/**
     BlockManagerMaster负责对Block的管理和协调，具体操作依赖于BlockManagerMasterActor，Driver和Executor处理BlockManager的
  方式不同：
    1.如果当前应用程序是Driver，则创建BlockManagerMasterActor，并且注册到ActorSystrm中。
    2.如果当前应用程序是Execotor，则从ActorSystem中找到BlockManagerActor。无论是Driver还是Executor，最后BlockManager的属性driverActor
      将持有对BlockManagerMasterActor的引用。

      在Driver的DAGScheduler中还有一个很重要的对象BlockManagerMaster,它的功能，其实很简单，就是负责对各个节点 的BlockManager内部管理的数据的元数据，
  进行管理与维护，比如block的增删等操作，都会在这里维护变化的操作。

    每个Worker中的blockManager在创建后的第一件事就是向Driver中的BlockManagerMaster进行注册 。此时在BlockManagerMaster内部里就会为这个
  blockManager创建一个BlockManagerInfo

  */
private[spark]
class BlockManagerMaster(
    var driverEndpoint: RpcEndpointRef,
    conf: SparkConf,
    isDriver: Boolean)
  extends Logging {

  // 请求超时的时间默认是120秒
  val timeout = RpcUtils.askRpcTimeout(conf)

  /** Remove a dead executor from the driver endpoint. This is only called on the driver side.
    * 从driver endpoint移除一个死掉的executor，这个方法仅仅被driver端调用
    * */
  def removeExecutor(execId: String) {
    tell(RemoveExecutor(execId))
    logInfo("Removed " + execId + " successfully in removeExecutor")
  }

  /** Request removal of a dead executor from the driver endpoint.
   *  This is only called on the driver side. Non-blocking
    *  从driver endpoint移除一个死掉的executor，这个方法仅仅被driver端调用 非阻塞
   */
  def removeExecutorAsync(execId: String) {
    driverEndpoint.ask[Boolean](RemoveExecutor(execId))
    logInfo("Removal of executor " + execId + " requested")
  }

  /**
   * Register the BlockManager's id with the driver. The input BlockManagerId does not contain
   * topology information. This information is obtained from the master and we respond with an
   * updated BlockManagerId fleshed out with this information.
    *
    * 用驱动程序注册此id的BlockManager，输入BlockManagerId不包含topology信息。这个信息是从主服务器获取的，
    * 我们用这个信息来处理更新的BlockManagerId。
   */
  def registerBlockManager(
      blockManagerId: BlockManagerId,
      maxOnHeapMemSize: Long,
      maxOffHeapMemSize: Long,
      slaveEndpoint: RpcEndpointRef): BlockManagerId = {
    //28=>17/12/05 11:56:51 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 192.168.1.161, 55290, None)
    logInfo(s"Registering BlockManager $blockManagerId")

    // 这里调用了org.apache.spark.storage.BlockManagerMasterEndpoint.receiveAndReply().case RegisterBlockManager
    val updatedId = driverEndpoint.askSync[BlockManagerId](
      RegisterBlockManager(blockManagerId, maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint))
    //30=> 17/12/05 11:56:51 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.1.161, 55290, None)
    logInfo(s"Registered BlockManager $updatedId")
    updatedId
  }

  def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): Boolean = {
    val res = driverEndpoint.askSync[Boolean](
      UpdateBlockInfo(blockManagerId, blockId, storageLevel, memSize, diskSize))
    logDebug(s"Updated info of block $blockId")
    res
  }

  /** Get locations of the blockId from the driver
    * 从driver获取指定blockId的Block位置
    * */
  def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    driverEndpoint.askSync[Seq[BlockManagerId]](GetLocations(blockId))
  }

  /** Get locations of multiple blockIds from the driver
    * 从driver获取多个blockId的Block位置
    * */
  def getLocations(blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    driverEndpoint.askSync[IndexedSeq[Seq[BlockManagerId]]](
      GetLocationsMultipleBlockIds(blockIds))
  }

  /**
   * Check if block manager master has a block. Note that this can be used to check for only
   * those blocks that are reported to block manager master.
    *
    * 检查BlockManagerMaster是否包含一个指定块id的块。
    * 请注意，这可以用于仅检查报告给块管理器的那些块。
   */
  def contains(blockId: BlockId): Boolean = {
    !getLocations(blockId).isEmpty
  }

  /** Get ids of other nodes in the cluster from the driver
    * 从driver中获得集群中其他节点的id
    * */
  def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    // BlockManagerMasterEndpoint的receiveAndReply（）方法去处理
    driverEndpoint.askSync[Seq[BlockManagerId]](GetPeers(blockManagerId))
  }

  def getExecutorEndpointRef(executorId: String): Option[RpcEndpointRef] = {
    driverEndpoint.askSync[Option[RpcEndpointRef]](GetExecutorEndpointRef(executorId))
  }

  /**
   * Remove a block from the slaves that have it. This can only be used to remove
   * blocks that the driver knows about.
    *
    * 从子节点移除存在于子节点的块。这仅仅被用于driver移除driver知道的那些子节点的块。
   */
  def removeBlock(blockId: BlockId) {
    driverEndpoint.askSync[Boolean](RemoveBlock(blockId))
  }

  /** Remove all blocks belonging to the given RDD.
    * 移除所有属于指定RDD的块
    * */
  def removeRdd(rddId: Int, blocking: Boolean) {
    val future = driverEndpoint.askSync[Future[Seq[Int]]](RemoveRdd(rddId))
    future.onFailure {
      case e: Exception =>
        logWarning(s"Failed to remove RDD $rddId - ${e.getMessage}", e)
    }(ThreadUtils.sameThread)
    if (blocking) {
      timeout.awaitResult(future)
    }
  }

  /** Remove all blocks belonging to the given shuffle.
    * 移除说有属于指定shuffle的块
    * */
  def removeShuffle(shuffleId: Int, blocking: Boolean) {
    val future = driverEndpoint.askSync[Future[Seq[Boolean]]](RemoveShuffle(shuffleId))
    future.onFailure {
      case e: Exception =>
        logWarning(s"Failed to remove shuffle $shuffleId - ${e.getMessage}", e)
    }(ThreadUtils.sameThread)
    if (blocking) {
      timeout.awaitResult(future)
    }
  }

  /** Remove all blocks belonging to the given broadcast.
    * 移除所有属于指定广播的块
    * */
  def removeBroadcast(broadcastId: Long, removeFromMaster: Boolean, blocking: Boolean) {
    val future = driverEndpoint.askSync[Future[Seq[Int]]](
      RemoveBroadcast(broadcastId, removeFromMaster))
    future.onFailure {
      case e: Exception =>
        logWarning(s"Failed to remove broadcast $broadcastId" +
          s" with removeFromMaster = $removeFromMaster - ${e.getMessage}", e)
    }(ThreadUtils.sameThread)
    if (blocking) {
      timeout.awaitResult(future)
    }
  }

  /**
   * Return the memory status for each block manager, in the form of a map from
   * the block manager's id to two long values. The first value is the maximum
   * amount of memory allocated for the block manager, while the second is the
   * amount of remaining memory.
    *
    * 为每个block manager返回内存的状态，从块管理器的id映射到两个长值。第一个值是内存分配给block manager的最大内存数目。
    * 第二个值是保留的内存大小。
   */
  def getMemoryStatus: Map[BlockManagerId, (Long, Long)] = {
    driverEndpoint.askSync[Map[BlockManagerId, (Long, Long)]](GetMemoryStatus)
  }

  def getStorageStatus: Array[StorageStatus] = {
    driverEndpoint.askSync[Array[StorageStatus]](GetStorageStatus)
  }

  /**
   * Return the block's status on all block managers, if any. NOTE: This is a
   * potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, this invokes the master to query each block manager for the most
   * updated block statuses. This is useful when the master is not informed of the given block
   * by all block managers.
    *
    *
    * 返回所有block managers的块信息，如果可能的话。注意：这是一个很昂贵的操作，应该仅仅用于测试。
    *
    * 如果askSlaves为真，这将调用主程序查询每个块管理器，以获得最新的块状态。当master没有被所有块管理器告知给定的块时，这是很有用的。
   */
  def getBlockStatus(
      blockId: BlockId,
      askSlaves: Boolean = true): Map[BlockManagerId, BlockStatus] = {
    val msg = GetBlockStatus(blockId, askSlaves)
    /*
     * To avoid potential deadlocks, the use of Futures is necessary, because the master endpoint
     * should not block on waiting for a block manager, which can in turn be waiting for the
     * master endpoint for a response to a prior message.
     */
    val response = driverEndpoint.
      askSync[Map[BlockManagerId, Future[Option[BlockStatus]]]](msg)
    val (blockManagerIds, futures) = response.unzip
    implicit val sameThread = ThreadUtils.sameThread
    val cbf =
      implicitly[
        CanBuildFrom[Iterable[Future[Option[BlockStatus]]],
        Option[BlockStatus],
        Iterable[Option[BlockStatus]]]]
    val blockStatus = timeout.awaitResult(
      Future.sequence[Option[BlockStatus], Iterable](futures)(cbf, ThreadUtils.sameThread))
    if (blockStatus == null) {
      throw new SparkException("BlockManager returned null for BlockStatus query: " + blockId)
    }
    blockManagerIds.zip(blockStatus).flatMap { case (blockManagerId, status) =>
      status.map { s => (blockManagerId, s) }
    }.toMap
  }

  /**
   * Return a list of ids of existing blocks such that the ids match the given filter. NOTE: This
   * is a potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, this invokes the master to query each block manager for the most
   * updated block statuses. This is useful when the master is not informed of the given block
   * by all block managers.
    *
    * 返回现有块的id列表，这些id与给定的过滤器匹配。注意:这是一个可能很昂贵的操作，应该只用于测试。
    *
    * 如果askSlaves是true，这将调用主程序查询每个块管理器，以获得最新的块状态。当master没有被所有块管理器告知给定的块时，这是很有用的。
   */
  def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askSlaves: Boolean): Seq[BlockId] = {
    val msg = GetMatchingBlockIds(filter, askSlaves)
    val future = driverEndpoint.askSync[Future[Seq[BlockId]]](msg)
    timeout.awaitResult(future)
  }

  /**
   * Find out if the executor has cached blocks. This method does not consider broadcast blocks,
   * since they are not reported the master.
    *
    * 查看executor是否缓存了块。这种方法不考虑广播块，因为它们没有向主节点报告。
   */
  def hasCachedBlocks(executorId: String): Boolean = {
    driverEndpoint.askSync[Boolean](HasCachedBlocks(executorId))
  }

  /** Stop the driver endpoint, called only on the Spark driver node
    * 停止 driver endpoint，这个仅仅被Spark的driver节点调用
    * */
  def stop() {
    if (driverEndpoint != null && isDriver) {
      tell(StopBlockManagerMaster)
      driverEndpoint = null
      logInfo("BlockManagerMaster stopped")
    }
  }

  /** Send a one-way message to the master endpoint, to which we expect it to reply with true.
    * 向主端点发送一条单向消息，我们期望它以true回复。
    *
    * tell方法作为askWithRetry的代理也经常被调用
    * */
  private def tell(message: Any) {
    if (!driverEndpoint.askSync[Boolean](message)) {
      throw new SparkException("BlockManagerMasterEndpoint returned false, expected true.")
    }
  }

}

private[spark] object BlockManagerMaster {
  val DRIVER_ENDPOINT_NAME = "BlockManagerMaster"
}
