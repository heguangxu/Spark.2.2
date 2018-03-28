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

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.Channels

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.executor.{DataReadMethod, ShuffleWriteMetrics}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.network._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.ExternalShuffleClient
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage.memory._
import org.apache.spark.unsafe.Platform
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer

/* Class for returning a fetched block and associated metrics. */
private[spark] class BlockResult(
    val data: Iterator[Any],
    val readMethod: DataReadMethod.Value,
    val bytes: Long)

/**
 * Abstracts away how blocks are stored and provides different ways to read the underlying block
 * data. Callers should call [[dispose()]] when they're done with the block.
 */
private[spark] trait BlockData {

  def toInputStream(): InputStream

  /**
   * Returns a Netty-friendly wrapper for the block's data.
   *
   * Please see `ManagedBuffer.convertToNetty()` for more details.
   */
  def toNetty(): Object

  def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer

  def toByteBuffer(): ByteBuffer

  def size: Long

  def dispose(): Unit

}

private[spark] class ByteBufferBlockData(
    val buffer: ChunkedByteBuffer,
    val shouldDispose: Boolean) extends BlockData {

  override def toInputStream(): InputStream = buffer.toInputStream(dispose = false)

  override def toNetty(): Object = buffer.toNetty

  override def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer = {
    buffer.copy(allocator)
  }

  override def toByteBuffer(): ByteBuffer = buffer.toByteBuffer

  override def size: Long = buffer.size

  override def dispose(): Unit = {
    if (shouldDispose) {
      buffer.dispose()
    }
  }

}

/**
 * Manager running on every node (driver and executors) which provides interfaces for putting and
 * retrieving blocks both locally and remotely into various stores (memory, disk, and off-heap).
 *
 * Note that [[initialize()]] must be called before the BlockManager is usable.
  *
  * Manager运行在所有节点上，(driver and executors)它提供了在本地和远程到不同存储区(内存、磁盘和堆栈)中放置和检索块的接口。
  *
  * 注意，在使用的BlockManager之前必须调用[[initialize(])]来初始化。
  *
  * BlockManager负责对Block的管理，只有在BlockManager的痴实话方法initialize被调用之后，它才是有效的。Blockmanager作为
  * 存储系统的一部分。
  *
  * BlockManager是Spark存储体系中的核心组件。
  * BlockManager主要由以下部分组成：
  *   1.shuffle客户端ShuffleClient;
  *   2.BlockManagerMaster(对存在于所有Executor上的BlockManager统一管理)
  *   3.磁盘块管理器DiskBlockManager;
  *   4.内存存储MemoryStore；
  *   5.磁盘存储DiskStore;
  *   6.Tachyon存储TachyonStore;
  *   7.非广播Block清理器metadataCleanner和广播Block清理器broadcastCleaner;
  *   8.压缩算法实现CompressionCodee;
  *
  * BlockManager要生效，必须初始化，初始化步骤如下：
  *   1.BlockTransService的初始化和ShuffleClient的初始化。ShuffleClient默认是BlockTransferService,
  *     当有外部的ShuffleServer时，调用外部ShuffleServer的初始化方法。
  *   2.BlockManagerId和ShuffleServiceId的创建。当有外部的ShuffleService时，创建新的BlockManagerId,
  *     否则ShuffleServerId默认使用当前的BlockManager的BlockManagerId.
 */
private[spark] class BlockManager(
    executorId: String,                               //BlockManager运行在哪个Executor之上
    rpcEnv: RpcEnv,                                   //远程通信体
    val master: BlockManagerMaster,                   //BlockManagerMaster,管理整个集群的BlockManger
    val serializerManager: SerializerManager,         //默认序列化器
    val conf: SparkConf,
    memoryManager: MemoryManager,                     //内存管理器
    mapOutputTracker: MapOutputTracker,               //shuffle输出跟踪器
    shuffleManager: ShuffleManager,                   //shuffle管理器
    val blockTransferService: BlockTransferService,   //用于Block间的网络通信(进行备份时)
    securityManager: SecurityManager,                 //安全管理器
    numUsableCores: Int)
  extends BlockDataManager with BlockEvictionHandler with Logging {

  private[spark] val externalShuffleServiceEnabled =
    conf.getBoolean("spark.shuffle.service.enabled", false)

  //磁盘中的Block管理器
  val diskBlockManager = {
    // Only perform cleanup if an external service is not serving our shuffle files.
    // 只有在外部服务不服务于我们的shuffle文件时才执行清理。
    val deleteFilesOnStop =
      !externalShuffleServiceEnabled || executorId == SparkContext.DRIVER_IDENTIFIER
    new DiskBlockManager(conf, deleteFilesOnStop)
  }

  // Visible for testing 可见测试
  private[storage] val blockInfoManager = new BlockInfoManager

  private val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("block-manager-future", 128))

  // Actual storage of where blocks are kept  内存中存储块
  private[spark] val memoryStore =
    new MemoryStore(conf, blockInfoManager, serializerManager, memoryManager, this)

  // 在磁盘上存储区块管理器块。
  private[spark] val diskStore = new DiskStore(conf, diskBlockManager, securityManager)
  memoryManager.setMemoryStore(memoryStore)

  // Note: depending on the memory manager, `maxMemory` may actually vary over time.
  // However, since we use this only for reporting and logging, what we actually want here is
  // the absolute maximum value that `maxMemory` can ever possibly reach. We may need
  // to revisit whether reporting this value as the "max" is intuitive to the user.
  /**
    * 注意:根据内存管理器，“maxMemory”可能会随时间而变化。然而，由于我们仅用于报告和日志记录，所以我们真正想要的是“maxMemory”
    * 可能达到的绝对最大值。我们可能需要重新考虑是否将这个值称为“max”，这对于用户来说是直观的。
    * */
  private val maxOnHeapMemory = memoryManager.maxOnHeapStorageMemory
  private val maxOffHeapMemory = memoryManager.maxOffHeapStorageMemory

  // Port used by the external shuffle service. In Yarn mode, this may be already be
  // set through the Hadoop configuration as the server is launched in the Yarn NM.
  // 由外部shuffle service使用的端口。在Yarn模式中，这可能已经通过Hadoop配置来设置，因为服务器是在Yarn NM处启动的。
  private val externalShuffleServicePort = {
    val tmpPort = Utils.getSparkOrYarnConfig(conf, "spark.shuffle.service.port", "7337").toInt
    if (tmpPort == 0) {
      // for testing, we set "spark.shuffle.service.port" to 0 in the yarn config, so yarn finds
      // an open port.  But we still need to tell our spark apps the right port to use.  So
      // only if the yarn config has the port set to 0, we prefer the value in the spark config
      // 为了测试，我们在yarn的配置中设置了“spark.shuffle.service.port”为0，因此yarn找到一个打开的端口。
      // 但是我们仍然需要告诉我们的spark应用程序正确的使用端口。因此，只有当yarn配置的端口设置为0时，我们更喜欢spark配置中的值
      conf.get("spark.shuffle.service.port").toInt
    } else {
      tmpPort
    }
  }

  var blockManagerId: BlockManagerId = _

  // Address of the server that serves this executor's shuffle files. This is either an external
  // service, or just our own Executor's BlockManager.
  // 服务器的地址，服务于执行器的shuffle文件。这不是一个外部服务，就是我们自己的执行器的BlockManager。
  private[spark] var shuffleServerId: BlockManagerId = _

  // Client to read other executors' shuffle files. This is either an external service, or just the
  // standard BlockTransferService to directly connect to other Executors.
  // 客户端读取其他执行程序的拖拽文件。这不是一个外部服务，就是一个直接连接到其他执行器的标准的BlockTransferService。
  // 因为externalShuffleServiceEnabled=false，所以这里默认使用blockTransferService
  private[spark] val shuffleClient = if (externalShuffleServiceEnabled) {
    val transConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores)
    new ExternalShuffleClient(transConf, securityManager, securityManager.isAuthenticationEnabled())
  } else {
    blockTransferService
  }

  // Max number of failures before this block manager refreshes the block locations from the driver
  // 在块管理器从driver获得块位置的最大失败次数
  private val maxFailuresBeforeLocationRefresh =
    conf.getInt("spark.block.failures.beforeLocationRefresh", 5)

  // 创建BlockManagerSlaveEndpoint这个消息循环体来接受Driver中的BlockManagerMaster发过来的指令，例如删除Block等；
  private val slaveEndpoint = rpcEnv.setupEndpoint(
    "BlockManagerEndpoint" + BlockManager.ID_GENERATOR.next,
    new BlockManagerSlaveEndpoint(rpcEnv, this, mapOutputTracker))

  // Pending re-registration action being executed asynchronously or null if none is pending.
  // Accesses should synchronize on asyncReregisterLock.
  // 如果没有挂起，终止重新注册的操作将被异步执行。cUNK应该在asyncReregisterLock上同步。
  private var asyncReregisterTask: Future[Unit] = null
  private val asyncReregisterLock = new Object

  // Field related to peer block managers that are necessary for block replication
  @volatile private var cachedPeers: Seq[BlockManagerId] = _
  private val peerFetchLock = new Object
  private var lastPeerFetchTime = 0L

  private var blockReplicationPolicy: BlockReplicationPolicy = _

  /**
   * Initializes the BlockManager with the given appId. This is not performed in the constructor as
   * the appId may not be known at BlockManager instantiation time (in particular for the driver,
   * where it is only learned after registration with the TaskScheduler).
    *
    * 用给定的appId初始化BlockManager。这不是在构造函数中执行的，因为appId在BlockManager实例化的时候
    * (特别是对于驱动程序driver，只有在任务调度程序TaskScheduler注册后才会知道)的情况下是不知道的。
   *
   * This method initializes the BlockTransferService and ShuffleClient, registers with the
   * BlockManagerMaster, starts the BlockManagerWorker endpoint, and registers with a local shuffle
   * service if configured.
    *
    * 此方法初始化BlockTransferService和ShuffleClient，使用BlockManagerMaster注册，
    * 启动BlockManagerWorker endpoint，并在注册一个本地的shuffle服务，如果配置了的话。
    *
    * BlockManager要生效，必须初始化，初始化步骤如下：
    * 1.BlockTransService的初始化和ShuffleClient的初始化。ShuffleClient默认是BlockTransferService,
    *   当有外部的ShuffleServer时，调用外部ShuffleServer的初始化方法。
    * 2.BlockManagerId和ShuffleServiceId的创建。当有外部的ShuffleService时，创建新的BlockManagerId,
    *   否则ShuffleServerId默认使用当前的BlockManager的BlockManagerId.
    * 3.向BlockManagerMaster注册BlockmanagerId。
    *
    *
    * 该方法在SparkContexte中 _env.blockManager.initialize(_applicationId)被调用
   */
  def initialize(appId: String): Unit = {
    blockTransferService.init(this)
    // 默认是BlockTransferService，调用的是org.apache.spark.network.shuffle.ShuffleClient.init()方法
    // ShuffleClient:提供从执行器Executor或外部服务中读取shuffle文件的接口。这是一个抽象类，因此需要看它的实现类
    // BlockTransferService extends ShuffleClient,但是BlockTransferService也是一个抽象类
    // NettyBlockTransferService继承BlockTransferService，所以，最终调用的是NettyBlockTransferService的init方法
    // 这里shuffleClient默认是BlockTransferService，当有外部的ShuffleService时候，调用外部ShuffleService的初始化方法。
    shuffleClient.init(appId)

    // 没看懂这一点是干嘛的？
    blockReplicationPolicy = {
      val priorityClass = conf.get(
        "spark.storage.replication.policy", classOf[RandomBlockReplicationPolicy].getName)
      val clazz = Utils.classForName(priorityClass)
      val ret = clazz.newInstance.asInstanceOf[BlockReplicationPolicy]
      //27=> 17/12/05 11:56:51 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
      logInfo(s"Using $priorityClass for block replication policy")
      ret
    }

    //一个blockManager对应一个executorId，blockTransferService的host，port
    // 这一点使用了apply方法初始化的
    val id = BlockManagerId(executorId, blockTransferService.hostName, blockTransferService.port, None)

    /**
      * 这里是Executor中的BlockManager初始化的时候，向Driver中的BlockManagerMaster注册，此时BlockManagerMaster会
      * 创建BlockManagerInfo来进行元数据管理
      *
      * slaveEndpoint:创建BlockManagerSlaveEndpoint这个消息循环体来接受Driver中的BlockManagerMaster发过来的指令，
      * 例如删除Block等.使用BlockManagerSlaveEndpoint这个类；
      * */
    val idFromMaster = master.registerBlockManager(
      id,
      maxOnHeapMemory,
      maxOffHeapMemory,
      slaveEndpoint)

    // 如果BlockManager向BlockManagerMaster注册成功，就返回更新后的ID,否则就是没更新前的ID
    blockManagerId = if (idFromMaster != null) idFromMaster else id

    // 当有外部的ShuffleService时，创建新的BlockManagerId，否则使用当前BlockManager的BlockManagerId
    // 这里externalShuffleServiceEnabled默认为false
    shuffleServerId = if (externalShuffleServiceEnabled) {
      logInfo(s"external shuffle service port = $externalShuffleServicePort")
      BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort)
    } else {
      blockManagerId
    }

    // 当有外部的ShuffleService且是Executor的BlockManager时，还需要向BlockManagerMaster注册ShuffleServerId
    // Register Executors' configuration with the local shuffle service, if one should exist.
    if (externalShuffleServiceEnabled && !blockManagerId.isDriver) {
      registerWithExternalShuffleServer()
    }

    // 31=>17/12/05 11:56:51 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.1.161, 55290, None)
    logInfo(s"Initialized BlockManager: $blockManagerId")
  }





  private def registerWithExternalShuffleServer() {
    logInfo("Registering executor with local external shuffle service.")
    val shuffleConfig = new ExecutorShuffleInfo(
      diskBlockManager.localDirs.map(_.toString),
      diskBlockManager.subDirsPerLocalDir,
      shuffleManager.getClass.getName)

    val MAX_ATTEMPTS = 3
    val SLEEP_TIME_SECS = 5

    for (i <- 1 to MAX_ATTEMPTS) {
      try {
        // Synchronous and will throw an exception if we cannot connect.
        shuffleClient.asInstanceOf[ExternalShuffleClient].registerWithShuffleServer(
          shuffleServerId.host, shuffleServerId.port, shuffleServerId.executorId, shuffleConfig)
        return
      } catch {
        case e: Exception if i < MAX_ATTEMPTS =>
          logError(s"Failed to connect to external shuffle server, will retry ${MAX_ATTEMPTS - i}"
            + s" more times after waiting $SLEEP_TIME_SECS seconds...", e)
          Thread.sleep(SLEEP_TIME_SECS * 1000)
        case NonFatal(e) =>
          throw new SparkException("Unable to register with external shuffle server due to : " +
            e.getMessage, e)
      }
    }
  }

  /**
   * Report all blocks to the BlockManager again. This may be necessary if we are dropped
   * by the BlockManager and come back or if we become capable of recovering blocks on disk after
   * an executor crash.
   *
   * This function deliberately fails silently if the master returns false (indicating that
   * the slave needs to re-register). The error condition will be detected again by the next
   * heart beat attempt or new block registration and another try to re-register all blocks
   * will be made then.
    *
    *
    * BlockManager报告其管理的所有Blocks信息
    *     再次向BlockManager汇报所有的blocks。这样做是非常有必要的，如果掉线需要通过
    * BlockManager找回blocks，或者在executor崩溃后，来恢复磁盘上的blocks。
    * 如果master返回是false时，这个函数则也是失败的。通过下次心跳执行，错误的条件将会被发现
    * 或者一个新的block的注册和将重新注册全部blocks的时候，这个方法将被使用。
   */
  private def reportAllBlocks(): Unit = {
    logInfo(s"Reporting ${blockInfoManager.size} blocks to the master.")
    for ((blockId, info) <- blockInfoManager.entries) {
      val status = getCurrentBlockStatus(blockId, info)
      if (info.tellMaster && !tryToReportBlockStatus(blockId, status)) {
        logError(s"Failed to report $blockId to master; giving up.")
        return
      }
    }
  }

  /**
   * Re-register with the master and report all blocks to it. This will be called by the heart beat
   * thread if our heartbeat to the block manager indicates that we were not registered.
   *
   * Note that this method must be called without any BlockInfo locks held.
   */
  def reregister(): Unit = {
    // TODO: We might need to rate limit re-registering.
    logInfo(s"BlockManager $blockManagerId re-registering with master")
    master.registerBlockManager(blockManagerId, maxOnHeapMemory, maxOffHeapMemory, slaveEndpoint)
    reportAllBlocks()
  }

  /**
   * Re-register with the master sometime soon.
   */
  private def asyncReregister(): Unit = {
    asyncReregisterLock.synchronized {
      if (asyncReregisterTask == null) {
        asyncReregisterTask = Future[Unit] {
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool
          reregister()
          asyncReregisterLock.synchronized {
            asyncReregisterTask = null
          }
        }(futureExecutionContext)
      }
    }
  }

  /**
   * For testing. Wait for any pending asynchronous re-registration; otherwise, do nothing.
   */
  def waitForAsyncReregister(): Unit = {
    val task = asyncReregisterTask
    if (task != null) {
      try {
        ThreadUtils.awaitReady(task, Duration.Inf)
      } catch {
        case NonFatal(t) =>
          throw new Exception("Error occurred while waiting for async. reregistration", t)
      }
    }
  }

  /**
   * Interface to get local block data. Throws an exception if the block cannot be found or
   * cannot be read successfully.
    *
    * 获取本地块数据的接口。如果不能找到块，或者不能成功读取，则抛出异常。
    *
    * getBlockData用于从本地获取Block的数据。
    * getBlockData的处理实现如下：
    * 1.如果Block是ShuffleMapTask的输出，那么多个partition的中间结果都写入了同一个文件，怎样读取不同的partition的中间结果？
    *   IndexShuffleBlockManager的getBlockData方法解决了这个问题。
    * 2.如果Block是ResultTask的输出，则使用getLocalBytes来获取本地中间结果数据。
   */
  override def getBlockData(blockId: BlockId): ManagedBuffer = {
    if (blockId.isShuffle) {
      shuffleManager.shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId])
    } else {
      getLocalBytes(blockId) match {
        case Some(blockData) =>
          new BlockManagerManagedBuffer(blockInfoManager, blockId, blockData, true)
        case None =>
          // If this block manager receives a request for a block that it doesn't have then it's
          // likely that the master has outdated block statuses for this block. Therefore, we send
          // an RPC so that this block is marked as being unavailable from this block manager.
          reportBlockStatus(blockId, BlockStatus.empty)
          throw new BlockNotFoundException(blockId.toString)
      }
    }
  }

  /**
   * Put the block locally, using the given storage level.
   *
   * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
   * so may corrupt or change the data stored by the `BlockManager`.
   */
  override def putBlockData(
      blockId: BlockId,
      data: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Boolean = {
    putBytes(blockId, new ChunkedByteBuffer(data.nioByteBuffer()), level)(classTag)
  }

  /**
   * Get the BlockStatus for the block identified by the given ID, if it exists.
   * NOTE: This is mainly for testing.
   */
  def getStatus(blockId: BlockId): Option[BlockStatus] = {
    blockInfoManager.get(blockId).map { info =>
      val memSize = if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
      val diskSize = if (diskStore.contains(blockId)) diskStore.getSize(blockId) else 0L
      BlockStatus(info.level, memSize = memSize, diskSize = diskSize)
    }
  }

  /**
   * Get the ids of existing blocks that match the given filter. Note that this will
   * query the blocks stored in the disk block manager (that the block manager
   * may not know of).
   */
  def getMatchingBlockIds(filter: BlockId => Boolean): Seq[BlockId] = {
    // The `toArray` is necessary here in order to force the list to be materialized so that we
    // don't try to serialize a lazy iterator when responding to client requests.
    (blockInfoManager.entries.map(_._1) ++ diskBlockManager.getAllBlocks())
      .filter(filter)
      .toArray
      .toSeq
  }

  /**
   * Tell the master about the current storage status of a block. This will send a block update
   * message reflecting the current status, *not* the desired storage level in its block info.
   * For example, a block with MEMORY_AND_DISK set might have fallen out to be only on disk.
    *
    * 告诉master一个块的当前存储状态。这将发送一个块更新消息，以反映当前状态，*not*在它的块信息中显示所需的存储级别。
    * 例如，一个带有MEMORY_AND_DISK集合的块可能已经掉到只在磁盘上了。
   *
   * droppedMemorySize exists to account for when the block is dropped from memory to disk (so
   * it is still valid). This ensures that update in master will compensate for the increase in
   * memory on slave.
    *
    * 当块从内存到磁盘(所以它仍然有效)时，需要考虑droppedMemorySize。这确保了主版本的更新将补偿从slave增加的内存。
    *
    * reportBlockStatus用于向BlockManagerMasterActor报告Block的状态并且重新注册BlockManager.
    * 1.调用tryToReportBlockStatus方法，tryToReportBlockStatus调用了BlockManagerMaster的dateBlockInfo方法
    *   向BlockManagerMasterActor发送UpdateBlockInfo消息更新Block占用的内存大小，磁盘大小，存储级别等信息。
    * 2.如果BlockManager还没有向BlockManagerMasterActor注册，则调用asyncReregister方法，asyncReregister调用了reregister,
    *   最后reregister实际调用了BlockManagerMaster的registerBlockManager方法和reportAllBlocks方法，reportAllBlocks方法
    *   实际上也是调用了tryToReportBlockStatus.
   */
  private def reportBlockStatus(
      blockId: BlockId,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Unit = {
    val needReregister = !tryToReportBlockStatus(blockId, status, droppedMemorySize)
    if (needReregister) {
      logInfo(s"Got told to re-register updating block $blockId")
      // Re-registering will report our new block for free.
      asyncReregister()
    }
    logDebug(s"Told master about block $blockId")
  }

  /**
   * Actually send a UpdateBlockInfo message. Returns the master's response,
   * which will be true if the block was successfully recorded and false if
   * the slave needs to re-register.
    *
    * 实际上发送一个UpdateBlockInfo消息。返回主节点的响应，如果该块被成功地记录，则返回真，如果该块需要子节点重新注册，将返回false。
   */
  private def tryToReportBlockStatus(
      blockId: BlockId,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Boolean = {
    val storageLevel = status.storageLevel
    val inMemSize = Math.max(status.memSize, droppedMemorySize)
    val onDiskSize = status.diskSize
    master.updateBlockInfo(blockManagerId, blockId, storageLevel, inMemSize, onDiskSize)
  }

  /**
   * Return the updated storage status of the block with the given ID. More specifically, if
   * the block is dropped from memory and possibly added to disk, return the new storage level
   * and the updated in-memory and on-disk sizes.
    *
    *
    *     返回指定block所在存储块的最新信息。特别的，当block从内存移到磁盘时，更改其存储级别并
    * 更新内存和磁盘大小。
   */
  private def getCurrentBlockStatus(blockId: BlockId, info: BlockInfo): BlockStatus = {
    info.synchronized {
      info.level match {
        case null =>
          BlockStatus.empty
        case level =>
          val inMem = level.useMemory && memoryStore.contains(blockId)
          val onDisk = level.useDisk && diskStore.contains(blockId)
          val deserialized = if (inMem) level.deserialized else false
          val replication = if (inMem  || onDisk) level.replication else 1
          val storageLevel = StorageLevel(
            useDisk = onDisk,
            useMemory = inMem,
            useOffHeap = level.useOffHeap,
            deserialized = deserialized,
            replication = replication)
          val memSize = if (inMem) memoryStore.getSize(blockId) else 0L
          val diskSize = if (onDisk) diskStore.getSize(blockId) else 0L
          BlockStatus(storageLevel, memSize, diskSize)
      }
    }
  }

  /**
   * Get locations of an array of blocks.
   */
  private def getLocationBlockIds(blockIds: Array[BlockId]): Array[Seq[BlockManagerId]] = {
    val startTimeMs = System.currentTimeMillis
    //这里需要向Master要Block所在的BlockManager的信息。
    val locations = master.getLocations(blockIds).toArray
    logDebug("Got multiple block location in %s".format(Utils.getUsedTimeMs(startTimeMs)))
    locations
  }

  /**
   * Cleanup code run in response to a failed local read.
   * Must be called while holding a read lock on the block.
   */
  private def handleLocalReadFailure(blockId: BlockId): Nothing = {
    releaseLock(blockId)
    // Remove the missing block so that its unavailability is reported to the driver
    removeBlock(blockId)
    throw new SparkException(s"Block $blockId was not found even though it's read-locked")
  }

  /**
   * Get block from local block manager as an iterator of Java objects.
   */
  def getLocalValues(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting local block $blockId")
    blockInfoManager.lockForReading(blockId) match {
      case None =>
        logDebug(s"Block $blockId was not found")
        None
      case Some(info) =>
        val level = info.level
        logDebug(s"Level for block $blockId is $level")
        val taskAttemptId = Option(TaskContext.get()).map(_.taskAttemptId())
        if (level.useMemory && memoryStore.contains(blockId)) {
          val iter: Iterator[Any] = if (level.deserialized) {
            memoryStore.getValues(blockId).get
          } else {
            serializerManager.dataDeserializeStream(
              blockId, memoryStore.getBytes(blockId).get.toInputStream())(info.classTag)
          }
          // We need to capture the current taskId in case the iterator completion is triggered
          // from a different thread which does not have TaskContext set; see SPARK-18406 for
          // discussion.
          val ci = CompletionIterator[Any, Iterator[Any]](iter, {
            releaseLock(blockId, taskAttemptId)
          })
          Some(new BlockResult(ci, DataReadMethod.Memory, info.size))
        } else if (level.useDisk && diskStore.contains(blockId)) {
          val diskData = diskStore.getBytes(blockId)
          val iterToReturn: Iterator[Any] = {
            if (level.deserialized) {
              val diskValues = serializerManager.dataDeserializeStream(
                blockId,
                diskData.toInputStream())(info.classTag)
              maybeCacheDiskValuesInMemory(info, blockId, level, diskValues)
            } else {
              val stream = maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
                .map { _.toInputStream(dispose = false) }
                .getOrElse { diskData.toInputStream() }
              serializerManager.dataDeserializeStream(blockId, stream)(info.classTag)
            }
          }
          val ci = CompletionIterator[Any, Iterator[Any]](iterToReturn, {
            releaseLockAndDispose(blockId, diskData, taskAttemptId)
          })
          Some(new BlockResult(ci, DataReadMethod.Disk, info.size))
        } else {
          handleLocalReadFailure(blockId)
        }
    }
  }

  /**
   * Get block from the local block manager as serialized bytes.
    * 以序列化字节的方式从本地块管理器中获取块。
   */
  def getLocalBytes(blockId: BlockId): Option[BlockData] = {
    logDebug(s"Getting local block $blockId as bytes")
    // As an optimization for map output fetches, if the block is for a shuffle, return it
    // without acquiring a lock; the disk store never deletes (recent) items so this should work
    if (blockId.isShuffle) {
      val shuffleBlockResolver = shuffleManager.shuffleBlockResolver
      // TODO: This should gracefully handle case where local block is not available. Currently
      // downstream code will throw an exception.
      val buf = new ChunkedByteBuffer(
        shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId]).nioByteBuffer())
      Some(new ByteBufferBlockData(buf, true))
    } else {
      blockInfoManager.lockForReading(blockId).map { info => doGetLocalBytes(blockId, info) }
    }
  }

  /**
   * Get block from the local block manager as serialized bytes.
   *
   * Must be called while holding a read lock on the block.
   * Releases the read lock upon exception; keeps the read lock upon successful return.
   */
  private def doGetLocalBytes(blockId: BlockId, info: BlockInfo): BlockData = {
    val level = info.level
    logDebug(s"Level for block $blockId is $level")
    // In order, try to read the serialized bytes from memory, then from disk, then fall back to
    // serializing in-memory objects, and, finally, throw an exception if the block does not exist.
    if (level.deserialized) {
      // Try to avoid expensive serialization by reading a pre-serialized copy from disk:
      if (level.useDisk && diskStore.contains(blockId)) {
        // Note: we purposely do not try to put the block back into memory here. Since this branch
        // handles deserialized blocks, this block may only be cached in memory as objects, not
        // serialized bytes. Because the caller only requested bytes, it doesn't make sense to
        // cache the block's deserialized objects since that caching may not have a payoff.
        diskStore.getBytes(blockId)
      } else if (level.useMemory && memoryStore.contains(blockId)) {
        // The block was not found on disk, so serialize an in-memory copy:
        new ByteBufferBlockData(serializerManager.dataSerializeWithExplicitClassTag(
          blockId, memoryStore.getValues(blockId).get, info.classTag), true)
      } else {
        handleLocalReadFailure(blockId)
      }
    } else {  // storage level is serialized
      if (level.useMemory && memoryStore.contains(blockId)) {
        new ByteBufferBlockData(memoryStore.getBytes(blockId).get, false)
      } else if (level.useDisk && diskStore.contains(blockId)) {
        val diskData = diskStore.getBytes(blockId)
        maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
          .map(new ByteBufferBlockData(_, false))
          .getOrElse(diskData)
      } else {
        handleLocalReadFailure(blockId)
      }
    }
  }

  /**
   * Get block from remote block managers.
   *
   * This does not acquire a lock on this block in this JVM.
   */
  private def getRemoteValues[T: ClassTag](blockId: BlockId): Option[BlockResult] = {
    val ct = implicitly[ClassTag[T]]
    getRemoteBytes(blockId).map { data =>
      val values =
        serializerManager.dataDeserializeStream(blockId, data.toInputStream(dispose = true))(ct)
      new BlockResult(values, DataReadMethod.Network, data.size)
    }
  }

  /**
   * Return a list of locations for the given block, prioritizing the local machine since
   * multiple block managers can share the same host.
    * 返回给定块的位置列表，将本地机器的优先级排序，因为多个块管理器可以共享同一个主机。
   */
  private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    val locs = Random.shuffle(master.getLocations(blockId))
    val (preferredLocs, otherLocs) = locs.partition { loc => blockManagerId.host == loc.host }
    preferredLocs ++ otherLocs
  }

  /**
   * Get block from remote block managers as serialized bytes.
    * 从远程块管理器中获取块作为序列化的字节。
    *
    * 1.5以前是doGetRemote  获取远程Block数据方法
    *
    *   1.向BlockManagerMasterActor发送GetLocations消息获取Block数据存储的BlockManagerId，如果Block数据
    *     复制份数多余1个，则会返回多个BlockManagerId，对这些BlockManagerId洗牌，避免总是从一个远程
    *     BlockManager获取Block数据。发送GetLocations消息使用了getLocations方法。
    *   2.根据返回的BlockManagerId信息，使用blockTransferService远程同步获取Block数据。
   */
  def getRemoteBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
    logDebug(s"Getting remote block $blockId")
    require(blockId != null, "BlockId is null")
    var runningFailureCount = 0
    var totalFailureCount = 0
    val locations = getLocations(blockId)
    val maxFetchFailures = locations.size
    var locationIterator = locations.iterator
    while (locationIterator.hasNext) {
      val loc = locationIterator.next()
      logDebug(s"Getting remote block $blockId from $loc")
      val data = try {
        blockTransferService.fetchBlockSync(
          loc.host, loc.port, loc.executorId, blockId.toString).nioByteBuffer()
      } catch {
        case NonFatal(e) =>
          runningFailureCount += 1
          totalFailureCount += 1

          if (totalFailureCount >= maxFetchFailures) {
            // Give up trying anymore locations. Either we've tried all of the original locations,
            // or we've refreshed the list of locations from the master, and have still
            // hit failures after trying locations from the refreshed list.
            logWarning(s"Failed to fetch block after $totalFailureCount fetch failures. " +
              s"Most recent failure cause:", e)
            return None
          }

          logWarning(s"Failed to fetch remote block $blockId " +
            s"from $loc (failed attempt $runningFailureCount)", e)

          // If there is a large number of executors then locations list can contain a
          // large number of stale entries causing a large number of retries that may
          // take a significant amount of time. To get rid of these stale entries
          // we refresh the block locations after a certain number of fetch failures
          if (runningFailureCount >= maxFailuresBeforeLocationRefresh) {
            locationIterator = getLocations(blockId).iterator
            logDebug(s"Refreshed locations from the driver " +
              s"after ${runningFailureCount} fetch failures.")
            runningFailureCount = 0
          }

          // This location failed, so we retry fetch from a different one by returning null here
          null
      }

      if (data != null) {
        return Some(new ChunkedByteBuffer(data))
      }
      logDebug(s"The value of block $blockId is null")
    }
    logDebug(s"Block $blockId not found")
    None
  }

  /**
   * Get a block from the block manager (either local or remote).
   *
   * This acquires a read lock on the block if the block was stored locally and does not acquire
   * any locks if the block was fetched from a remote block manager. The read lock will
   * automatically be freed once the result's `data` iterator is fully consumed.
   */
  def get[T: ClassTag](blockId: BlockId): Option[BlockResult] = {
    val local = getLocalValues(blockId)
    if (local.isDefined) {
      logInfo(s"Found block $blockId locally")
      return local
    }
    val remote = getRemoteValues[T](blockId)
    if (remote.isDefined) {
      logInfo(s"Found block $blockId remotely")
      return remote
    }
    None
  }

  /**
   * Downgrades an exclusive write lock to a shared read lock.
   */
  def downgradeLock(blockId: BlockId): Unit = {
    blockInfoManager.downgradeLock(blockId)
  }

  /**
   * Release a lock on the given block with explicit TID.
   * The param `taskAttemptId` should be passed in case we can't get the correct TID from
   * TaskContext, for example, the input iterator of a cached RDD iterates to the end in a child
   * thread.
   */
  def releaseLock(blockId: BlockId, taskAttemptId: Option[Long] = None): Unit = {
    blockInfoManager.unlock(blockId, taskAttemptId)
  }

  /**
   * Registers a task with the BlockManager in order to initialize per-task bookkeeping structures.
   */
  def registerTask(taskAttemptId: Long): Unit = {
    blockInfoManager.registerTask(taskAttemptId)
  }

  /**
   * Release all locks for the given task.
   *
   * @return the blocks whose locks were released.
   */
  def releaseAllLocksForTask(taskAttemptId: Long): Seq[BlockId] = {
    blockInfoManager.releaseAllLocksForTask(taskAttemptId)
  }

  /**
   * Retrieve the given block if it exists, otherwise call the provided `makeIterator` method
   * to compute the block, persist it, and return its values.
   *
   * @return either a BlockResult if the block was successfully cached, or an iterator if the block
   *         could not be cached.
   */
  def getOrElseUpdate[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      makeIterator: () => Iterator[T]): Either[BlockResult, Iterator[T]] = {
    // Attempt to read the block from local or remote storage. If it's present, then we don't need
    // to go through the local-get-or-put path.
    get[T](blockId)(classTag) match {
      case Some(block) =>
        return Left(block)
      case _ =>
        // Need to compute the block.
    }
    // Initially we hold no locks on this block.
    doPutIterator(blockId, makeIterator, level, classTag, keepReadLock = true) match {
      case None =>
        // doPut() didn't hand work back to us, so the block already existed or was successfully
        // stored. Therefore, we now hold a read lock on the block.
        val blockResult = getLocalValues(blockId).getOrElse {
          // Since we held a read lock between the doPut() and get() calls, the block should not
          // have been evicted, so get() not returning the block indicates some internal error.
          releaseLock(blockId)
          throw new SparkException(s"get() failed for block $blockId even though we held a lock")
        }
        // We already hold a read lock on the block from the doPut() call and getLocalValues()
        // acquires the lock again, so we need to call releaseLock() here so that the net number
        // of lock acquisitions is 1 (since the caller will only call release() once).
        releaseLock(blockId)
        Left(blockResult)
      case Some(iter) =>
        // The put failed, likely because the data was too large to fit in memory and could not be
        // dropped to disk. Therefore, we need to pass the input iterator back to the caller so
        // that they can decide what to do with the values (e.g. process them without caching).
       Right(iter)
    }
  }

  /**
   * @return true if the block was stored or false if an error occurred.
   */
  def putIterator[T: ClassTag](
      blockId: BlockId,
      values: Iterator[T],
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    require(values != null, "Values is null")
    doPutIterator(blockId, () => values, level, implicitly[ClassTag[T]], tellMaster) match {
      case None =>
        true
      case Some(iter) =>
        // Caller doesn't care about the iterator values, so we can close the iterator here
        // to free resources earlier
        iter.close()
        false
    }
  }

  /**
   * A short circuited method to get a block writer that can write data directly to disk.
   * The Block will be appended to the File specified by filename. Callers should handle error
   * cases.
    *
    * 一个短路的方法，用来获取一个可以将数据直接写入磁盘的块写入器。该块将被追加到文件名指定的文件中。调用者应该处理错误用例。
    *
    * getDiskWriter用于创建DiskBlockObjectWriter，属性spark.shuffle.sync决定写操作是同步还是异步。
   */
  def getDiskWriter(
      blockId: BlockId,
      file: File,
      serializerInstance: SerializerInstance,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetrics): DiskBlockObjectWriter = {
    val syncWrites = conf.getBoolean("spark.shuffle.sync", false)
    new DiskBlockObjectWriter(file, serializerManager, serializerInstance, bufferSize,
      syncWrites, writeMetrics, blockId)
  }

  /**
   * Put a new block of serialized bytes to the block manager.
   *
   * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
   * so may corrupt or change the data stored by the `BlockManager`.
   *
   * @return true if the block was stored or false if an error occurred.
   */
  def putBytes[T: ClassTag](
      blockId: BlockId,
      bytes: ChunkedByteBuffer,
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    require(bytes != null, "Bytes is null")
    doPutBytes(blockId, bytes, level, implicitly[ClassTag[T]], tellMaster)
  }

  /**
   * Put the given bytes according to the given level in one of the block stores, replicating
   * the values if necessary.
   *
   * If the block already exists, this method will not overwrite it.
   *
   * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
   * so may corrupt or change the data stored by the `BlockManager`.
   *
   * @param keepReadLock if true, this method will hold the read lock when it returns (even if the
   *                     block already exists). If false, this method will hold no locks when it
   *                     returns.
   * @return true if the block was already present or if the put succeeded, false otherwise.
   */
  private def doPutBytes[T](
      blockId: BlockId,
      bytes: ChunkedByteBuffer,
      level: StorageLevel,
      classTag: ClassTag[T],
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false): Boolean = {
    doPut(blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock) { info =>
      val startTimeMs = System.currentTimeMillis
      // Since we're storing bytes, initiate the replication before storing them locally.
      // This is faster as data is already serialized and ready to send.
      val replicationFuture = if (level.replication > 1) {
        Future {
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool. The ByteBufferBlockData wrapper is not disposed of to avoid releasing
          // buffers that are owned by the caller.
          replicate(blockId, new ByteBufferBlockData(bytes, false), level, classTag)
        }(futureExecutionContext)
      } else {
        null
      }

      val size = bytes.size

      if (level.useMemory) {
        // Put it in memory first, even if it also has useDisk set to true;
        // We will drop it to disk later if the memory store can't hold it.
        val putSucceeded = if (level.deserialized) {
          val values =
            serializerManager.dataDeserializeStream(blockId, bytes.toInputStream())(classTag)
          memoryStore.putIteratorAsValues(blockId, values, classTag) match {
            case Right(_) => true
            case Left(iter) =>
              // If putting deserialized values in memory failed, we will put the bytes directly to
              // disk, so we don't need this iterator and can close it to free resources earlier.
              iter.close()
              false
          }
        } else {
          val memoryMode = level.memoryMode
          memoryStore.putBytes(blockId, size, memoryMode, () => {
            if (memoryMode == MemoryMode.OFF_HEAP &&
                bytes.chunks.exists(buffer => !buffer.isDirect)) {
              bytes.copy(Platform.allocateDirectBuffer)
            } else {
              bytes
            }
          })
        }
        if (!putSucceeded && level.useDisk) {
          logWarning(s"Persisting block $blockId to disk instead.")
          diskStore.putBytes(blockId, bytes)
        }
      } else if (level.useDisk) {
        diskStore.putBytes(blockId, bytes)
      }

      val putBlockStatus = getCurrentBlockStatus(blockId, info)
      val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
      if (blockWasSuccessfullyStored) {
        // Now that the block is in either the memory or disk store,
        // tell the master about it.
        info.size = size
        if (tellMaster && info.tellMaster) {
          reportBlockStatus(blockId, putBlockStatus)
        }
        addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
      }
      logDebug("Put block %s locally took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs)))
      if (level.replication > 1) {
        // Wait for asynchronous replication to finish
        try {
          ThreadUtils.awaitReady(replicationFuture, Duration.Inf)
        } catch {
          case NonFatal(t) =>
            throw new Exception("Error occurred while waiting for replication to finish", t)
        }
      }
      if (blockWasSuccessfullyStored) {
        None
      } else {
        Some(bytes)
      }
    }.isEmpty
  }

  /**
   * Helper method used to abstract common code from [[doPutBytes()]] and [[doPutIterator()]].
    * 用于抽象普通代码的助手方法[[doPutBytes()]]和[[doPutIterator(]]]。
   *
   * @param putBody a function which attempts the actual put() and returns None on success
   *                or Some on failure.
   */
  private def doPut[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_],
      tellMaster: Boolean,
      keepReadLock: Boolean)(putBody: BlockInfo => Option[T]): Option[T] = {

    require(blockId != null, "BlockId is null")
    require(level != null && level.isValid, "StorageLevel is null or invalid")

    val putBlockInfo = {
      val newInfo = new BlockInfo(level, classTag, tellMaster)
      if (blockInfoManager.lockNewBlockForWriting(blockId, newInfo)) {
        newInfo
      } else {
        logWarning(s"Block $blockId already exists on this machine; not re-adding it")
        if (!keepReadLock) {
          // lockNewBlockForWriting returned a read lock on the existing block, so we must free it:
          releaseLock(blockId)
        }
        return None
      }
    }

    val startTimeMs = System.currentTimeMillis
    var exceptionWasThrown: Boolean = true
    val result: Option[T] = try {
      val res = putBody(putBlockInfo)
      exceptionWasThrown = false
      if (res.isEmpty) {
        // the block was successfully stored
        if (keepReadLock) {
          blockInfoManager.downgradeLock(blockId)
        } else {
          blockInfoManager.unlock(blockId)
        }
      } else {
        removeBlockInternal(blockId, tellMaster = false)
        logWarning(s"Putting block $blockId failed")
      }
      res
    } finally {
      // This cleanup is performed in a finally block rather than a `catch` to avoid having to
      // catch and properly re-throw InterruptedException.
      if (exceptionWasThrown) {
        logWarning(s"Putting block $blockId failed due to an exception")
        // If an exception was thrown then it's possible that the code in `putBody` has already
        // notified the master about the availability of this block, so we need to send an update
        // to remove this block location.
        removeBlockInternal(blockId, tellMaster = tellMaster)
        // The `putBody` code may have also added a new block status to TaskMetrics, so we need
        // to cancel that out by overwriting it with an empty block status. We only do this if
        // the finally block was entered via an exception because doing this unconditionally would
        // cause us to send empty block statuses for every block that failed to be cached due to
        // a memory shortage (which is an expected failure, unlike an uncaught exception).
        addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
      }
    }
    if (level.replication > 1) {
      logDebug("Putting block %s with replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    } else {
      logDebug("Putting block %s without replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    }
    result
  }

  /**
   * Put the given block according to the given level in one of the block stores, replicating
   * the values if necessary.
   *
   * If the block already exists, this method will not overwrite it.
   *
   * @param keepReadLock if true, this method will hold the read lock when it returns (even if the
   *                     block already exists). If false, this method will hold no locks when it
   *                     returns.
   * @return None if the block was already present or if the put succeeded, or Some(iterator)
   *         if the put failed.
   */
  private def doPutIterator[T](
      blockId: BlockId,
      iterator: () => Iterator[T],
      level: StorageLevel,
      classTag: ClassTag[T],
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false): Option[PartiallyUnrolledIterator[T]] = {
    doPut(blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock) { info =>
      val startTimeMs = System.currentTimeMillis
      var iteratorFromFailedMemoryStorePut: Option[PartiallyUnrolledIterator[T]] = None
      // Size of the block in bytes
      var size = 0L
      if (level.useMemory) {
        // Put it in memory first, even if it also has useDisk set to true;
        // We will drop it to disk later if the memory store can't hold it.
        if (level.deserialized) {
          memoryStore.putIteratorAsValues(blockId, iterator(), classTag) match {
            case Right(s) =>
              size = s
            case Left(iter) =>
              // Not enough space to unroll this block; drop to disk if applicable
              if (level.useDisk) {
                logWarning(s"Persisting block $blockId to disk instead.")
                diskStore.put(blockId) { channel =>
                  val out = Channels.newOutputStream(channel)
                  serializerManager.dataSerializeStream(blockId, out, iter)(classTag)
                }
                size = diskStore.getSize(blockId)
              } else {
                iteratorFromFailedMemoryStorePut = Some(iter)
              }
          }
        } else { // !level.deserialized
          memoryStore.putIteratorAsBytes(blockId, iterator(), classTag, level.memoryMode) match {
            case Right(s) =>
              size = s
            case Left(partiallySerializedValues) =>
              // Not enough space to unroll this block; drop to disk if applicable
              if (level.useDisk) {
                logWarning(s"Persisting block $blockId to disk instead.")
                diskStore.put(blockId) { channel =>
                  val out = Channels.newOutputStream(channel)
                  partiallySerializedValues.finishWritingToStream(out)
                }
                size = diskStore.getSize(blockId)
              } else {
                iteratorFromFailedMemoryStorePut = Some(partiallySerializedValues.valuesIterator)
              }
          }
        }

      } else if (level.useDisk) {
        diskStore.put(blockId) { channel =>
          val out = Channels.newOutputStream(channel)
          serializerManager.dataSerializeStream(blockId, out, iterator())(classTag)
        }
        size = diskStore.getSize(blockId)
      }

      val putBlockStatus = getCurrentBlockStatus(blockId, info)
      val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
      if (blockWasSuccessfullyStored) {
        // Now that the block is in either the memory or disk store, tell the master about it.
        info.size = size
        if (tellMaster && info.tellMaster) {
          reportBlockStatus(blockId, putBlockStatus)
        }
        addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
        logDebug("Put block %s locally took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs)))
        if (level.replication > 1) {
          val remoteStartTime = System.currentTimeMillis
          val bytesToReplicate = doGetLocalBytes(blockId, info)
          // [SPARK-16550] Erase the typed classTag when using default serialization, since
          // NettyBlockRpcServer crashes when deserializing repl-defined classes.
          // TODO(ekl) remove this once the classloader issue on the remote end is fixed.
          val remoteClassTag = if (!serializerManager.canUseKryo(classTag)) {
            scala.reflect.classTag[Any]
          } else {
            classTag
          }
          try {
            replicate(blockId, bytesToReplicate, level, remoteClassTag)
          } finally {
            bytesToReplicate.dispose()
          }
          logDebug("Put block %s remotely took %s"
            .format(blockId, Utils.getUsedTimeMs(remoteStartTime)))
        }
      }
      assert(blockWasSuccessfullyStored == iteratorFromFailedMemoryStorePut.isEmpty)
      iteratorFromFailedMemoryStorePut
    }
  }

  /**
   * Attempts to cache spilled bytes read from disk into the MemoryStore in order to speed up
   * subsequent reads. This method requires the caller to hold a read lock on the block.
   *
   * @return a copy of the bytes from the memory store if the put succeeded, otherwise None.
   *         If this returns bytes from the memory store then the original disk store bytes will
   *         automatically be disposed and the caller should not continue to use them. Otherwise,
   *         if this returns None then the original disk store bytes will be unaffected.
   */
  private def maybeCacheDiskBytesInMemory(
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      diskData: BlockData): Option[ChunkedByteBuffer] = {
    require(!level.deserialized)
    if (level.useMemory) {
      // Synchronize on blockInfo to guard against a race condition where two readers both try to
      // put values read from disk into the MemoryStore.
      blockInfo.synchronized {
        if (memoryStore.contains(blockId)) {
          diskData.dispose()
          Some(memoryStore.getBytes(blockId).get)
        } else {
          val allocator = level.memoryMode match {
            case MemoryMode.ON_HEAP => ByteBuffer.allocate _
            case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
          }
          val putSucceeded = memoryStore.putBytes(blockId, diskData.size, level.memoryMode, () => {
            // https://issues.apache.org/jira/browse/SPARK-6076
            // If the file size is bigger than the free memory, OOM will happen. So if we
            // cannot put it into MemoryStore, copyForMemory should not be created. That's why
            // this action is put into a `() => ChunkedByteBuffer` and created lazily.
            diskData.toChunkedByteBuffer(allocator)
          })
          if (putSucceeded) {
            diskData.dispose()
            Some(memoryStore.getBytes(blockId).get)
          } else {
            None
          }
        }
      }
    } else {
      None
    }
  }

  /**
   * Attempts to cache spilled values read from disk into the MemoryStore in order to speed up
   * subsequent reads. This method requires the caller to hold a read lock on the block.
   *
   * @return a copy of the iterator. The original iterator passed this method should no longer
   *         be used after this method returns.
   */
  private def maybeCacheDiskValuesInMemory[T](
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      diskIterator: Iterator[T]): Iterator[T] = {
    require(level.deserialized)
    val classTag = blockInfo.classTag.asInstanceOf[ClassTag[T]]
    if (level.useMemory) {
      // Synchronize on blockInfo to guard against a race condition where two readers both try to
      // put values read from disk into the MemoryStore.
      blockInfo.synchronized {
        if (memoryStore.contains(blockId)) {
          // Note: if we had a means to discard the disk iterator, we would do that here.
          memoryStore.getValues(blockId).get
        } else {
          memoryStore.putIteratorAsValues(blockId, diskIterator, classTag) match {
            case Left(iter) =>
              // The memory store put() failed, so it returned the iterator back to us:
              iter
            case Right(_) =>
              // The put() succeeded, so we can read the values back:
              memoryStore.getValues(blockId).get
          }
        }
      }.asInstanceOf[Iterator[T]]
    } else {
      diskIterator
    }
  }

  /**
   * Get peer block managers in the system.
    * 在系统中获取对等块管理器。
    *
    * 为了容灾，peersForReplication中缓存的BlockManager不应当是当前的BlockManager，获取其他所有的BlockManager的方法是getPeers。
    *
    * cachedPeers：Seq[BlockManagerId]       当前BlockManager缓存的BlockManagerId
    * cachedPeersTtl                         cachedPeers缓存的超超时事件，默认为60秒，可以修改苏醒spark.storage.cacahedPeersTtl改变大小
    * forceFetch                             标记是否强制从BlockManagermasterActor获取最新的BlockManagerId
    *
    *   当cachedPeers为空或者forceFetch为True或者当前时间超时，则会调用BlockManagerMaster的getPeers方法，从BlockManagerMasterActor
    * 获取最新的BlockManagerId.
    *
   */
  private def getPeers(forceFetch: Boolean): Seq[BlockManagerId] = {
    peerFetchLock.synchronized {
      val cachedPeersTtl = conf.getInt("spark.storage.cachedPeersTtl", 60 * 1000) // milliseconds
      val timeout = System.currentTimeMillis - lastPeerFetchTime > cachedPeersTtl
      // 当cachedPeers为空或者forceFetch为True或者当前时间超时
      if (cachedPeers == null || forceFetch || timeout) {
        cachedPeers = master.getPeers(blockManagerId).sortBy(_.hashCode)
        lastPeerFetchTime = System.currentTimeMillis
        logDebug("Fetched peers from master: " + cachedPeers.mkString("[", ",", "]"))
      }
      cachedPeers
    }
  }

  /**
   * Called for pro-active replenishment of blocks lost due to executor failures
   *
   * @param blockId blockId being replicate
   * @param existingReplicas existing block managers that have a replica
   * @param maxReplicas maximum replicas needed
   */
  def replicateBlock(
      blockId: BlockId,
      existingReplicas: Set[BlockManagerId],
      maxReplicas: Int): Unit = {
    logInfo(s"Using $blockManagerId to pro-actively replicate $blockId")
    blockInfoManager.lockForReading(blockId).foreach { info =>
      val data = doGetLocalBytes(blockId, info)
      val storageLevel = StorageLevel(
        useDisk = info.level.useDisk,
        useMemory = info.level.useMemory,
        useOffHeap = info.level.useOffHeap,
        deserialized = info.level.deserialized,
        replication = maxReplicas)
      // we know we are called as a result of an executor removal, so we refresh peer cache
      // this way, we won't try to replicate to a missing executor with a stale reference
      getPeers(forceFetch = true)
      try {
        replicate(blockId, data, storageLevel, info.classTag, existingReplicas)
      } finally {
        logDebug(s"Releasing lock for $blockId")
        releaseLockAndDispose(blockId, data)
      }
    }
  }

  /**
   * Replicate block to another node. Note that this is a blocking call that returns after
   * the block has been replicated.
    *
    * //复制块到另一个节点。实际上会开辟一个线程来执行备份操作。
   */
  private def replicate(
      blockId: BlockId,
      data: BlockData,
      level: StorageLevel,
      classTag: ClassTag[_],
      existingReplicas: Set[BlockManagerId] = Set.empty): Unit = {

    // 最大的复制失败次数
    val maxReplicationFailures = conf.getInt("spark.storage.maxReplicationFailures", 1)
    val tLevel = StorageLevel(
      useDisk = level.useDisk,
      useMemory = level.useMemory,
      useOffHeap = level.useOffHeap,
      deserialized = level.deserialized,
      replication = 1)

    val numPeersToReplicateTo = level.replication - 1
    val startTime = System.nanoTime

    var peersReplicatedTo = mutable.HashSet.empty ++ existingReplicas
    var peersFailedToReplicateTo = mutable.HashSet.empty[BlockManagerId]
    var numFailures = 0

    val initialPeers = getPeers(false).filterNot(existingReplicas.contains(_))

    var peersForReplication = blockReplicationPolicy.prioritize(
      blockManagerId,
      initialPeers,
      peersReplicatedTo,
      blockId,
      numPeersToReplicateTo)

    while(numFailures <= maxReplicationFailures &&
      !peersForReplication.isEmpty &&
      peersReplicatedTo.size < numPeersToReplicateTo) {
      val peer = peersForReplication.head
      try {
        val onePeerStartTime = System.nanoTime
        logTrace(s"Trying to replicate $blockId of ${data.size} bytes to $peer")
        blockTransferService.uploadBlockSync(
          peer.host,
          peer.port,
          peer.executorId,
          blockId,
          new BlockManagerManagedBuffer(blockInfoManager, blockId, data, false),
          tLevel,
          classTag)
        logTrace(s"Replicated $blockId of ${data.size} bytes to $peer" +
          s" in ${(System.nanoTime - onePeerStartTime).toDouble / 1e6} ms")
        peersForReplication = peersForReplication.tail
        peersReplicatedTo += peer
      } catch {
        case NonFatal(e) =>
          logWarning(s"Failed to replicate $blockId to $peer, failure #$numFailures", e)
          peersFailedToReplicateTo += peer
          // we have a failed replication, so we get the list of peers again
          // we don't want peers we have already replicated to and the ones that
          // have failed previously
          val filteredPeers = getPeers(true).filter { p =>
            !peersFailedToReplicateTo.contains(p) && !peersReplicatedTo.contains(p)
          }

          numFailures += 1
          peersForReplication = blockReplicationPolicy.prioritize(
            blockManagerId,
            filteredPeers,
            peersReplicatedTo,
            blockId,
            numPeersToReplicateTo - peersReplicatedTo.size)
      }
    }
    logDebug(s"Replicating $blockId of ${data.size} bytes to " +
      s"${peersReplicatedTo.size} peer(s) took ${(System.nanoTime - startTime) / 1e6} ms")
    if (peersReplicatedTo.size < numPeersToReplicateTo) {
      logWarning(s"Block $blockId replicated to only " +
        s"${peersReplicatedTo.size} peer(s) instead of $numPeersToReplicateTo peers")
    }

    logDebug(s"block $blockId replicated to ${peersReplicatedTo.mkString(", ")}")
  }

  /**
   * Read a block consisting of a single object.
   */
  def getSingle[T: ClassTag](blockId: BlockId): Option[T] = {
    get[T](blockId).map(_.data.next().asInstanceOf[T])
  }

  /**
   * Write a block consisting of a single object.
    * 编写一个由单个对象组成的块。
    *
    * putSingle方法用于将由一个对象构成的Block写入存储系统。putSingle经过层层调用，实际调用了doPut方法。
   *
   * @return true if the block was stored or false if the block was already stored or an
   *         error occurred.
   */
  def putSingle[T: ClassTag](
      blockId: BlockId,
      value: T,
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    putIterator(blockId, Iterator(value), level, tellMaster)
  }

  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
    *
    * 从内存中删除一个块，如果可以的话，可能将它放到磁盘上。当内存存储达到其极限并需要释放空间时调用。
   *
   * If `data` is not put on disk, it won't be created.
    * 如果“数据”没有放在磁盘上，它就不会被创建。
   *
   * The caller of this method must hold a write lock on the block before calling this method.
   * This method does not release the write lock.
    * 调用此方法的调用者必须在调用此方法之前在块上保存写锁。这个方法不会释放写锁。
    *
    * 当内存不足的时候，可能需要腾出部分内存空间，dropFromMemory实现了这个歌功能。
    * 1.从blockInfo：TimeStampedHashMap[BlockId,BlockInfo]中检查是否存在要迁移的blockId，如果存在，从BlockInfo中获取Block的Storagelevel.
    * 2.如果StorageLevel允许存入硬盘，并且DiskStroe中不存在此文件，那么调用DiskStore的putArray或者putBytes方法，将Block存入磁盘。
    * 3.从MemoryStore中清除此BlockId对应的Block.
    * 4.使用getCurrentBlockStatus方法获取Block的最新状态，如果此Block的tellMaster属性为true，则调用reportBlockStatus方法
    *   给BlockManagerMasterActor报告状态。
    * 5.从blockInfo中清除此BlockId，并且返回Block的状态。
    *
    *
    * 该方法用于内存不足时，将指定的Block移出内存。其处理步骤如下：
    * 1. 从blockInfo中查找是否存在要移除的blockId，如果存在则继续操作，否则返回None；
    * 2. 判断该Block是否可以移除，不可移除返回None；
    * 3. 判断该Block内容是否为空，如果为空，则已经移除过了，返回None；
    * 4. 获取该Block的StorageLevel，如果StorageLevel允许存入磁盘，且DiskStore中没有保存此Block，那个调用DiskStore的putArray或者putBytes方法，将此Block写入磁盘；
    * 5. 从内存（MemoryStore）中清除此Block；
    * 6. 使用getCurrentBlockStatus方法获取Block最新的状态。如果此Block的tellMaster属性为true，则调用reportBlockStatus方法给BlockManagerMasterEndpoint报告状态；
    * 7. 如果此Block没有存入磁盘，则从blockInfo中清除此BlockId；
    * 8. 返回Block状态
   *
   * @return the block's new effective StorageLevel.
   */
  private[storage] override def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel = {
    logInfo(s"Dropping block $blockId from memory")
    // 判断要删除的块是否处于锁定状态，如果锁定说明该快正在使用，否则就获取要删除块的信息。
    val info = blockInfoManager.assertBlockIsLockedForWriting(blockId)
    var blockIsUpdated = false
    val level = info.level //获取存储级别

    // Drop to disk, if storage level requires  ，移除内存放到磁盘
    if (level.useDisk && !diskStore.contains(blockId)) {
      logInfo(s"Writing block $blockId to disk")
      data() match {
        case Left(elements) =>
          diskStore.put(blockId) { channel =>
            val out = Channels.newOutputStream(channel)
            serializerManager.dataSerializeStream(
              blockId,
              out,
              elements.toIterator)(info.classTag.asInstanceOf[ClassTag[T]])
          }
        case Right(bytes) =>
          diskStore.putBytes(blockId, bytes)
      }
      blockIsUpdated = true
    }

    // Actually drop from memory store  完全从内存中删除块
    val droppedMemorySize =
      if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
    // 移除内存块
    val blockIsRemoved = memoryStore.remove(blockId)
    if (blockIsRemoved) {
      blockIsUpdated = true
    } else {
      logWarning(s"Block $blockId could not be dropped from memory as it does not exist")
    }

    val status = getCurrentBlockStatus(blockId, info)
    if (info.tellMaster) {
      reportBlockStatus(blockId, status, droppedMemorySize)
    }
    if (blockIsUpdated) {
      addUpdatedBlockStatusToTaskMetrics(blockId, status)
    }
    status.storageLevel
  }

  /**
   * Remove all blocks belonging to the given RDD.
   *
   * @return The number of blocks removed.
   */
  def removeRdd(rddId: Int): Int = {
    // TODO: Avoid a linear scan by creating another mapping of RDD.id to blocks.
    logInfo(s"Removing RDD $rddId")
    val blocksToRemove = blockInfoManager.entries.flatMap(_._1.asRDDId).filter(_.rddId == rddId)
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster = false) }
    blocksToRemove.size
  }

  /**
   * Remove all blocks belonging to the given broadcast.
   */
  def removeBroadcast(broadcastId: Long, tellMaster: Boolean): Int = {
    logDebug(s"Removing broadcast $broadcastId")
    val blocksToRemove = blockInfoManager.entries.map(_._1).collect {
      case bid @ BroadcastBlockId(`broadcastId`, _) => bid
    }
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster) }
    blocksToRemove.size
  }

  /**
   * Remove a block from both memory and disk.
   */
  def removeBlock(blockId: BlockId, tellMaster: Boolean = true): Unit = {
    logDebug(s"Removing block $blockId")
    blockInfoManager.lockForWriting(blockId) match {
      case None =>
        // The block has already been removed; do nothing.
        logWarning(s"Asked to remove block $blockId, which does not exist")
      case Some(info) =>
        removeBlockInternal(blockId, tellMaster = tellMaster && info.tellMaster)
        addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
    }
  }

  /**
   * Internal version of [[removeBlock()]] which assumes that the caller already holds a write
   * lock on the block.
   */
  private def removeBlockInternal(blockId: BlockId, tellMaster: Boolean): Unit = {
    // Removals are idempotent in disk store and memory store. At worst, we get a warning.
    val removedFromMemory = memoryStore.remove(blockId)
    val removedFromDisk = diskStore.remove(blockId)
    if (!removedFromMemory && !removedFromDisk) {
      logWarning(s"Block $blockId could not be removed as it was not found on disk or in memory")
    }
    blockInfoManager.removeBlock(blockId)
    if (tellMaster) {
      reportBlockStatus(blockId, BlockStatus.empty)
    }
  }

  private def addUpdatedBlockStatusToTaskMetrics(blockId: BlockId, status: BlockStatus): Unit = {
    Option(TaskContext.get()).foreach { c =>
      c.taskMetrics().incUpdatedBlockStatuses(blockId -> status)
    }
  }

  def releaseLockAndDispose(
      blockId: BlockId,
      data: BlockData,
      taskAttemptId: Option[Long] = None): Unit = {
    releaseLock(blockId, taskAttemptId)
    data.dispose()
  }

  def stop(): Unit = {
    blockTransferService.close()
    if (shuffleClient ne blockTransferService) {
      // Closing should be idempotent, but maybe not for the NioBlockTransferService.
      shuffleClient.close()
    }
    diskBlockManager.stop()
    rpcEnv.stop(slaveEndpoint)
    blockInfoManager.clear()
    memoryStore.clear()
    futureExecutionContext.shutdownNow()
    logInfo("BlockManager stopped")
  }
}


private[spark] object BlockManager {
  private val ID_GENERATOR = new IdGenerator

  def blockIdsToHosts(
      blockIds: Array[BlockId],
      env: SparkEnv,
      blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[String]] = {

    // blockManagerMaster != null is used in tests
    assert(env != null || blockManagerMaster != null)
    val blockLocations: Seq[Seq[BlockManagerId]] = if (blockManagerMaster == null) {
      env.blockManager.getLocationBlockIds(blockIds)
    } else {
      blockManagerMaster.getLocations(blockIds)
    }

    val blockManagers = new HashMap[BlockId, Seq[String]]
    for (i <- 0 until blockIds.length) {
      blockManagers(blockIds(i)) = blockLocations(i).map(_.host)
    }
    blockManagers.toMap
  }
}
