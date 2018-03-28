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

package org.apache.spark

import java.io._
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, ThreadPoolExecutor}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.broadcast.{Broadcast, BroadcastManager}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util._

private[spark] sealed trait MapOutputTrackerMessage


private[spark] case class GetMapOutputStatuses(shuffleId: Int)
  extends MapOutputTrackerMessage



private[spark] case object StopMapOutputTracker extends MapOutputTrackerMessage



private[spark] case class GetMapOutputMessage(shuffleId: Int, context: RpcCallContext)



/** RpcEndpoint class for MapOutputTrackerMaster
  *
  * */
private[spark] class MapOutputTrackerMasterEndpoint(
                                                     override val rpcEnv: RpcEnv, tracker: MapOutputTrackerMaster, conf: SparkConf)
  extends RpcEndpoint with Logging {

  logDebug("init") // force eager creation of logger

  /** 处理RpcEndpointRef.ask方法，如果不匹配消息，将抛出SparkException
    *
    * 这一段代码没看懂？
    * */
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetMapOutputStatuses(shuffleId: Int) =>
      val hostPort = context.senderAddress.hostPort
      logInfo("Asked to send map output locations for shuffle " + shuffleId + " to " + hostPort)
      val mapOutputStatuses = tracker.post(new GetMapOutputMessage(shuffleId, context))

    case StopMapOutputTracker =>
      logInfo("MapOutputTrackerMasterEndpoint stopped!")
      context.reply(true)
      stop()
  }
}






/**
  * Class that keeps track of the location of the map output of
  * a stage. This is abstract because different versions of MapOutputTracker
  * (driver and executor) use different HashMap to store its metadata.
  *
  * 类跟踪一个阶段map输出的位置。这是抽象的，因为不同版本的MapOutputTracker(驱动程序和执行器)
  * 使用不同的HashMap来存储它的元数据。
  *
  * MapOutputTracker是一个abstract抽象类。获取的Map out的信息根据master和worker有不同的用途：
  * master上，用来记录ShuffleMapTasks所需的map out的源；
  * worker上，仅仅作为cache用来执行shuffle计算。
  *
  * 它通过serializedMapStatuses将map out流通过gzip的压缩方式压缩（压缩是可行的，因为很多map out基于同样的hostname），
  * 这样方便数据流传递给reduce进行操作。
  *
  * MapOutputTracker是 SparkEnv初始化是初始化的重要组件之一  是master-slave的结构
  * 用来跟踪记录shuffleMapTask的输出位置 （shuffleMapTask要写到哪里去），
  * shuffleReader读取shuffle文件之前就是去请求MapOutputTrackerMaster 要自己处理的数据 在哪里？
  * MapOutputTracker给它返回一批 MapOutputTrackerWorker的列表（地址，port等信息）
  * shuffleReader开始读取文件  进行后期处理
  *
  */
private[spark] abstract class MapOutputTracker(conf: SparkConf) extends Logging {

  /** Set to the MapOutputTrackerMasterEndpoint living on the driver.
    * 在driver上设置MapOutputTrackerMasterEndpoint为living活动的。
    * */
  var trackerEndpoint: RpcEndpointRef = _

  /**
    * This HashMap has different behavior for the driver and the executors.
    * 这个HashMap对驱动程序和执行器有不同的行为。
    *
    * On the driver, it serves as the source of map outputs recorded from ShuffleMapTasks.
    * On the executors, it simply serves as a cache, in which a miss triggers a fetch from the
    * driver's corresponding HashMap.
    *
    * 在驱动程序中，它充当从ShuffleMapTasks记录的映射输出的源。
    * 在执行器上，它只是充当一个缓存，在这个缓存中，一个失误触发了驱动程序相应的HashMap的获取。
    *
    * Note: because mapStatuses is accessed concurrently, subclasses should make sure it's a
    * thread-safe map.
    * 注意:由于mapStatuses同时被访问，子类应该确保它是一个线程安全的映射。
    */
  protected val mapStatuses: Map[Int, Array[MapStatus]]

  /**
    * Incremented every time a fetch fails so that client nodes know to clear
    * their cache of map output locations if this happens.
    * 每次当一个fetch失败时递增，这样客户节点知道如果发生这种情况，就可以清除它们的映射输出位置缓存。
    */
  protected var epoch: Long = 0
  protected val epochLock = new AnyRef

  /** Remembers which map output locations are currently being fetched on an executor.
    * 记住，哪个映射输出位置当前正在被执行器获取。
    * */
  private val fetching = new HashSet[Int]

  /**
    * Send a message to the trackerEndpoint and get its result within a default timeout, or
    * throw a SparkException if this fails.
    *
    * 向trackerEndpoint发送一条消息，并在默认超时中获取它的结果，如果失败，则抛出一个SparkException。
    *
    * askTracker()：检查MapOutputTracker的连接是否正常。
    */
  protected def askTracker[T: ClassTag](message: Any): T = {
    try {
      trackerEndpoint.askSync[T](message)
    } catch {
      case e: Exception =>
        logError("Error communicating with MapOutputTracker", e)
        throw new SparkException("Error communicating with MapOutputTracker", e)
    }
  }

  /** Send a one-way message to the trackerEndpoint, to which we expect it to reply with true.
    * 发送一条到trackerEndpoint的单向消息，我们期望它以true回复。
    *
    * 检查MapOutPutTracker是否正常工作（发送任意信息返回true）。
    * */
  protected def sendTracker(message: Any) {
    val response = askTracker[Boolean](message)
    if (response != true) {
      throw new SparkException(
        "Error reply received from MapOutputTracker. Expecting true, got " + response.toString)
    }
  }

  /**
    * Called from executors to get the server URIs and output sizes for each shuffle block that
    * needs to be read from a given reduce task.
    * 从executors调用服务器uri，并从给定的reduce任务中读取每个需要读取的shuffle块的输出大小。
    *
    * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
    *         and the second item is a sequence of (shuffle block id, shuffle block size) tuples
    *         describing the shuffle blocks that are stored at that block manager.
    */
  def getMapSizesByExecutorId(shuffleId: Int, reduceId: Int)
  : Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    getMapSizesByExecutorId(shuffleId, reduceId, reduceId + 1)
  }

  /**
    * Called from executors to get the server URIs and output sizes for each shuffle block that
    * needs to be read from a given range of map output partitions (startPartition is included but
    * endPartition is excluded from the range).
    * 从executors调用，获取服务器uri和每个shuffle块的输出大小，需要从给定范围的映射输出分区读取
    * (包括startPartition，但是endPartition被排除在范围之外)。
    *
    * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
    *         and the second item is a sequence of (shuffle block id, shuffle block size) tuples
    *         describing the shuffle blocks that are stored at that block manager.
    */
  def getMapSizesByExecutorId(shuffleId: Int, startPartition: Int, endPartition: Int)
  : Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    logDebug(s"Fetching outputs for shuffle $shuffleId, partitions $startPartition-$endPartition")
    val statuses = getStatuses(shuffleId)
    // Synchronize on the returned array because, on the driver, it gets mutated in place
    statuses.synchronized {
      return MapOutputTracker.convertMapStatuses(shuffleId, startPartition, endPartition, statuses)
    }
  }

  /**
    * Return statistics about all of the outputs for a given shuffle.
    * 对给定的shuffle的所有输出返回统计信息。
    */
  def getStatistics(dep: ShuffleDependency[_, _, _]): MapOutputStatistics = {
    val statuses = getStatuses(dep.shuffleId)
    // Synchronize on the returned array because, on the driver, it gets mutated in place
    statuses.synchronized {
      val totalSizes = new Array[Long](dep.partitioner.numPartitions)
      for (s <- statuses) {
        for (i <- 0 until totalSizes.length) {
          totalSizes(i) += s.getSizeForBlock(i)
        }
      }
      new MapOutputStatistics(dep.shuffleId, totalSizes)
    }
  }

  /**
    * Get or fetch the array of MapStatuses for a given shuffle ID. NOTE: clients MUST synchronize
    * on this array when reading it, because on the driver, we may be changing it in place.
    *
    * (It would be nice to remove this restriction in the future.)
    *
    *  根据参数shuffle id来获取shuffle对应的map out所在的位置及信息。如果没有直接的对应shuffle id的信息，
    *  则需要从所有的map中匹配对应shuffle id的map out。
    *
    * 获取map任务状态：
    *   Spark通过调用MapOutputTracker的getStatuses（1.5版本是getServerStatuses）来获取map任务执行的状态信息，
    * 其中处理步骤如下：
    *   1.从当前BlockManager的MapOutputTracker中获取MapStatus，若没有就进入第2步。否则进入第4步。
    *   2.如果获取列表（fetching）中已经存在要取的shuffleId，那么久等待其他线程获取，如果获取列表中不存在要获取的shuffleId，
    *     那么就将shuffleId放入获取列表。
    *   3.调用askTracker方法向MapOutputTrackerMasterActor发送GetMapOutputStatuses消息获取map任务的状态信息。
    *     MapOutputTrackerMasterActor接收到GetMapOutputStatuses消息后，将请求的map任务状态信息序列化后发送给请求方，
    *     请求方接收到map任务状态信息后进行反序列化操作，然后放入本地的mapStatuses中。
    *   4.调用MapOutputTracker的convertMapStatuses方法将或得到的MapStatus转换为map任务所在的地址（即BlockManagerId）
    *     和map任务输出中分配给当前reduce任务的Block大小。
    */
  private def getStatuses(shuffleId: Int): Array[MapStatus] = {
    // 1.从当前BlockManager的MapOutputTracker中获取MapStatus
    val statuses = mapStatuses.get(shuffleId).orNull
    if (statuses == null) {
      // 2.如果获取列表（fetching）中已经存在要取的shuffleId
      logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
      val startTime = System.currentTimeMillis
      var fetchedStatuses: Array[MapStatus] = null
      fetching.synchronized {
        // Someone else is fetching it; wait for them to be done
        while (fetching.contains(shuffleId)) {
          try {
            // 那么久等待其他线程获取
            fetching.wait()
          } catch {
            case e: InterruptedException =>
          }
        }

        // Either while we waited the fetch happened successfully, or
        // someone fetched it in between the get and the fetching.synchronized.
        fetchedStatuses = mapStatuses.get(shuffleId).orNull
        // 如果获取列表中不存在要获取的shuffleId，  那么就将shuffleId放入获取列表。
        if (fetchedStatuses == null) {
          // We have to do the fetch, get others to wait for us.
          fetching += shuffleId
        }
      }

      if (fetchedStatuses == null) {
        // We won the race to fetch the statuses; do so
        logInfo("Doing the fetch; tracker endpoint = " + trackerEndpoint)
        // This try-finally prevents hangs due to timeouts:
        try {
          // 调用askTracker方法向MapOutputTrackerMasterActor发送GetMapOutputStatuses消息获取map任务的状态信息。
          val fetchedBytes = askTracker[Array[Byte]](GetMapOutputStatuses(shuffleId))
          // MapOutputTrackerMasterActor接收到GetMapOutputStatuses消息后，将请求的map任务状态信息序列化后发送给请求方，
          fetchedStatuses = MapOutputTracker.deserializeMapStatuses(fetchedBytes)
          logInfo("Got the output locations")
          mapStatuses.put(shuffleId, fetchedStatuses)
        } finally {
          fetching.synchronized {
            fetching -= shuffleId
            fetching.notifyAll()
          }
        }
      }
      logDebug(s"Fetching map output statuses for shuffle $shuffleId took " +
        s"${System.currentTimeMillis - startTime} ms")

      if (fetchedStatuses != null) {
        return fetchedStatuses
      } else {
        logError("Missing all output locations for shuffle " + shuffleId)
        throw new MetadataFetchFailedException(
          shuffleId, -1, "Missing all output locations for shuffle " + shuffleId)
      }
    } else {
      return statuses
    }
  }

  /** Called to get current epoch number.
    * 获取当前的epoch数值
    * epoch number是什么有什么用？
    * epoch的值是与master同步的，保证map outs是最新的有用的。
    * */
  def getEpoch: Long = {
    epochLock.synchronized {
      return epoch
    }
  }

  /**
    * Called from executors to update the epoch number, potentially clearing old outputs
    * because of a fetch failure. Each executor task calls this with the latest epoch
    * number on the driver at the time it was created.
    *
    * 由执行器调用来更新epoch号，可能会清除旧的输出，因为它会导致fetch失败。每个执行器任务都在创建时使用最新的epoch号来调用此任务。
    */
  def updateEpoch(newEpoch: Long) {
    epochLock.synchronized {
      if (newEpoch > epoch) {
        logInfo("Updating epoch to " + newEpoch + " and clearing cache")
        epoch = newEpoch
        mapStatuses.clear()
      }
    }
  }

  /** Unregister shuffle data.
    * 注销洗牌数据。
    * */
  def unregisterShuffle(shuffleId: Int) {
    mapStatuses.remove(shuffleId)
  }

  /** Stop the tracker.
    * 停止tracker
    * */
  def stop() { }
}








/**
  * MapOutputTracker for the driver.
  * driver的MapOutputTracker内部使用mapStatuses：TimeStampedHashMap[int,Array[MapStatus]]来维护跟踪各个map任务的输出状态，
  * 其中key对应shuffleld,Array存储各个map任务对应的状态信息MapStatus.
  *
  * MapOutPutTrackerMaster针对master的MapOutPutTracker，按照前文的意思，它的作用是为每个shuffle
  * 准备其所需要的所有map out，可以加速map outs传送给shuffle的速度。在存储map out的HashMap中，HashMap
  * 是基于时间戳的，因此map outs被减少只能因为它被注销掉或者生命周期耗尽。
  */
private[spark] class MapOutputTrackerMaster(conf: SparkConf,
                                            broadcastManager: BroadcastManager, isLocal: Boolean)
  extends MapOutputTracker(conf) {

  /** Cache a serialized version of the output statuses for each shuffle to send them out faster
    * 将输出状态的序列化版本缓存到每个洗牌中，以更快地将它们发送出去
    * */
  private var cacheEpoch = epoch

  // The size at which we use Broadcast to send the map output statuses to the executors
  // 我们使用广播将输出状态的map发送给执行器的大小
  private val minSizeForBroadcast =
    conf.getSizeAsBytes("spark.shuffle.mapOutput.minSizeForBroadcast", "512k").toInt

  /** Whether to compute locality preferences for reduce tasks
    * 是否计算局部首选项以减少任务
    * */
  private val shuffleLocalityEnabled = conf.getBoolean("spark.shuffle.reduceLocality.enabled", true)

  // Number of map and reduce tasks above which we do not assign preferred locations based on map
  // output sizes. We limit the size of jobs for which assign preferred locations as computing the
  // top locations by size becomes expensive.
  /**
    * map数量和reduce任务的数量，我们不根据map的输出大小分配优先位置。我们限制了作业的大小,
    * 他根据指定的优先位置作为计算顶级位置变得昂贵起来。
    */
  private val SHUFFLE_PREF_MAP_THRESHOLD = 1000
  // NOTE: This should be less than 2000 as we use HighlyCompressedMapStatus beyond that
  // 注意:这个应该小于2000作为我们使用HighlyCompressedMapStatus除此之外
  private val SHUFFLE_PREF_REDUCE_THRESHOLD = 1000

  // Fraction of total map output that must be at a location for it to considered as a preferred
  // location for a reduce task. Making this larger will focus on fewer locations where most data
  // can be read locally, but may lead to more delay in scheduling if those locations are busy.
  /** 总映射输出的一小部分，它必须位于一个位置，以作为减少任务的首选位置。使这个更大的目标集中在更少的地方，因为大多数数据可以在本地读取，
    *  但是如果这些位置很忙，可能会导致调度的延迟。
    */
  private val REDUCER_PREF_LOCS_FRACTION = 0.2

  // HashMaps for storing mapStatuses and cached serialized statuses in the driver.
  // Statuses are dropped only by explicit de-registering.
  protected val mapStatuses = new ConcurrentHashMap[Int, Array[MapStatus]]().asScala
  private val cachedSerializedStatuses = new ConcurrentHashMap[Int, Array[Byte]]().asScala

  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)

  // Kept in sync with cachedSerializedStatuses explicitly
  // This is required so that the Broadcast variable remains in scope until we remove
  // the shuffleId explicitly or implicitly.
  private val cachedSerializedBroadcast = new HashMap[Int, Broadcast[Array[Byte]]]()

  // This is to prevent multiple serializations of the same shuffle - which happens when
  // there is a request storm when shuffle start.
  private val shuffleIdLocks = new ConcurrentHashMap[Int, AnyRef]()

  // requests for map output statuses
  private val mapOutputRequests = new LinkedBlockingQueue[GetMapOutputMessage]

  // Thread pool used for handling map output status requests. This is a separate thread pool
  // to ensure we don't block the normal dispatcher threads.
  private val threadpool: ThreadPoolExecutor = {
    val numThreads = conf.getInt("spark.shuffle.mapOutput.dispatcher.numThreads", 8)
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "map-output-dispatcher")
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    pool
  }

  // Make sure that we aren't going to exceed the max RPC message size by making sure
  // we use broadcast to send large map output statuses.
  if (minSizeForBroadcast > maxRpcMessageSize) {
    val msg = s"spark.shuffle.mapOutput.minSizeForBroadcast ($minSizeForBroadcast bytes) must " +
      s"be <= spark.rpc.message.maxSize ($maxRpcMessageSize bytes) to prevent sending an rpc " +
      "message that is too large."
    logError(msg)
    throw new IllegalArgumentException(msg)
  }

  def post(message: GetMapOutputMessage): Unit = {
    mapOutputRequests.offer(message)
  }

  /** Message loop used for dispatching messages. */
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            val data = mapOutputRequests.take()
            if (data == PoisonPill) {
              // Put PoisonPill back so that other MessageLoops can see it.
              mapOutputRequests.offer(PoisonPill)
              return
            }
            val context = data.context
            val shuffleId = data.shuffleId
            val hostPort = context.senderAddress.hostPort
            logDebug("Handling request to send map output locations for shuffle " + shuffleId +
              " to " + hostPort)
            val mapOutputStatuses = getSerializedMapOutputStatuses(shuffleId)
            context.reply(mapOutputStatuses)
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }

  /** A poison endpoint that indicates MessageLoop should exit its message loop. */
  private val PoisonPill = new GetMapOutputMessage(-99, null)

  // Exposed for testing
  private[spark] def getNumCachedSerializedBroadcast = cachedSerializedBroadcast.size

  /** 在map out的集合mapStatuses中注册新的Shuffle，参数为Shuffle id和map的个数。  */
  def registerShuffle(shuffleId: Int, numMaps: Int) {
    if (mapStatuses.put(shuffleId, new Array[MapStatus](numMaps)).isDefined) {
      throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
    }
    // add in advance
    shuffleIdLocks.putIfAbsent(shuffleId, new Object())
  }

  /** 根据Shuffle id在mapStatuses中为Shuffle添加map out的状态（存储的map out其实就是map out的状态）。 */
  def registerMapOutput(shuffleId: Int, mapId: Int, status: MapStatus) {
    val array = mapStatuses(shuffleId)
    array.synchronized {
      array(mapId) = status
    }
  }

  /** Register multiple map output information for the given shuffle
    *
    * 同时添加多个map out。
    * */
  def registerMapOutputs(shuffleId: Int, statuses: Array[MapStatus], changeEpoch: Boolean = false) {
    mapStatuses.put(shuffleId, statuses.clone())
    if (changeEpoch) {
      incrementEpoch()
    }
  }

  /** Unregister map output information of the given shuffle, mapper and block manager *
    *
    * 在mapStatuses中注销给定Shuffle的map out。
    */
  def unregisterMapOutput(shuffleId: Int, mapId: Int, bmAddress: BlockManagerId) {
    val arrayOpt = mapStatuses.get(shuffleId)
    if (arrayOpt.isDefined && arrayOpt.get != null) {
      val array = arrayOpt.get
      array.synchronized {
        if (array(mapId) != null && array(mapId).location == bmAddress) {
          array(mapId) = null
        }
      }
      incrementEpoch()
    } else {
      throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
    }
  }

  /** Unregister shuffle data
    *
    * 不注册Shuffle的data数据
    * */
  override def unregisterShuffle(shuffleId: Int) {
    //
    mapStatuses.remove(shuffleId)
    cachedSerializedStatuses.remove(shuffleId)
    cachedSerializedBroadcast.remove(shuffleId).foreach(v => removeBroadcast(v))
    shuffleIdLocks.remove(shuffleId)
  }

  /** Check if the given shuffle is being tracked
    *
    *  判断是否存在给定的Shuffle。
    * */
  def containsShuffle(shuffleId: Int): Boolean = {
    cachedSerializedStatuses.contains(shuffleId) || mapStatuses.contains(shuffleId)
  }

  /**
    * Return the preferred hosts on which to run the given map output partition in a given shuffle,
    * i.e. the nodes that the most outputs for that partition are on.
    *
    * @param dep shuffle dependency object
    * @param partitionId map output partition that we want to read
    * @return a sequence of host names
    */
  def getPreferredLocationsForShuffle(dep: ShuffleDependency[_, _, _], partitionId: Int)
  : Seq[String] = {
    if (shuffleLocalityEnabled && dep.rdd.partitions.length < SHUFFLE_PREF_MAP_THRESHOLD &&
      dep.partitioner.numPartitions < SHUFFLE_PREF_REDUCE_THRESHOLD) {
      val blockManagerIds = getLocationsWithLargestOutputs(dep.shuffleId, partitionId,
        dep.partitioner.numPartitions, REDUCER_PREF_LOCS_FRACTION)
      if (blockManagerIds.nonEmpty) {
        blockManagerIds.get.map(_.host)
      } else {
        Nil
      }
    } else {
      Nil
    }
  }

  /**
    * Return a list of locations that each have fraction of map output greater than the specified
    * threshold.
    *
    * @param shuffleId id of the shuffle
    * @param reducerId id of the reduce task
    * @param numReducers total number of reducers in the shuffle
    * @param fractionThreshold fraction of total map output size that a location must have
    *                          for it to be considered large.
    */
  def getLocationsWithLargestOutputs(
                                      shuffleId: Int,
                                      reducerId: Int,
                                      numReducers: Int,
                                      fractionThreshold: Double)
  : Option[Array[BlockManagerId]] = {

    val statuses = mapStatuses.get(shuffleId).orNull
    if (statuses != null) {
      statuses.synchronized {
        if (statuses.nonEmpty) {
          // HashMap to add up sizes of all blocks at the same location
          val locs = new HashMap[BlockManagerId, Long]
          var totalOutputSize = 0L
          var mapIdx = 0
          while (mapIdx < statuses.length) {
            val status = statuses(mapIdx)
            // status may be null here if we are called between registerShuffle, which creates an
            // array with null entries for each output, and registerMapOutputs, which populates it
            // with valid status entries. This is possible if one thread schedules a job which
            // depends on an RDD which is currently being computed by another thread.
            if (status != null) {
              val blockSize = status.getSizeForBlock(reducerId)
              if (blockSize > 0) {
                locs(status.location) = locs.getOrElse(status.location, 0L) + blockSize
                totalOutputSize += blockSize
              }
            }
            mapIdx = mapIdx + 1
          }
          val topLocs = locs.filter { case (loc, size) =>
            size.toDouble / totalOutputSize >= fractionThreshold
          }
          // Return if we have any locations which satisfy the required threshold
          if (topLocs.nonEmpty) {
            return Some(topLocs.keys.toArray)
          }
        }
      }
    }
    None
  }

  /** 同步epoch加一。 */
  def incrementEpoch() {
    epochLock.synchronized {
      epoch += 1
      logDebug("Increasing epoch to " + epoch)
    }
  }

  private def removeBroadcast(bcast: Broadcast[_]): Unit = {
    if (null != bcast) {
      broadcastManager.unbroadcast(bcast.id,
        removeFromDriver = true, blocking = false)
    }
  }

  private def clearCachedBroadcast(): Unit = {
    for (cached <- cachedSerializedBroadcast) removeBroadcast(cached._2)
    cachedSerializedBroadcast.clear()
  }

  /**
    * 给定Shuffle id，返回其map out集合。首先是对epoch进行锁状态下的同步，保证获取资源的正确性；
    * 其次，根据Shuffle id获取指定位置的statuses，如果指定位置没有对应Shuffle id的statuses，
    * 那么获取这个位置的statuses快照返回，作为参考；最后，如果操作的epoch与锁状态下的epoch是一致的，
    * 将获取到的statuses存入缓存。
    *
    * */
  def getSerializedMapOutputStatuses(shuffleId: Int): Array[Byte] = {
    var statuses: Array[MapStatus] = null
    var retBytes: Array[Byte] = null
    var epochGotten: Long = -1

    // Check to see if we have a cached version, returns true if it does
    // and has side effect of setting retBytes.  If not returns false
    // with side effect of setting statuses
    def checkCachedStatuses(): Boolean = {
      epochLock.synchronized {
        if (epoch > cacheEpoch) {
          cachedSerializedStatuses.clear()
          clearCachedBroadcast()
          cacheEpoch = epoch
        }
        cachedSerializedStatuses.get(shuffleId) match {
          case Some(bytes) =>
            retBytes = bytes
            true
          case None =>
            logDebug("cached status not found for : " + shuffleId)
            statuses = mapStatuses.getOrElse(shuffleId, Array.empty[MapStatus])
            epochGotten = epoch
            false
        }
      }
    }

    if (checkCachedStatuses()) return retBytes
    var shuffleIdLock = shuffleIdLocks.get(shuffleId)
    if (null == shuffleIdLock) {
      val newLock = new Object()
      // in general, this condition should be false - but good to be paranoid
      val prevLock = shuffleIdLocks.putIfAbsent(shuffleId, newLock)
      shuffleIdLock = if (null != prevLock) prevLock else newLock
    }
    // synchronize so we only serialize/broadcast it once since multiple threads call
    // in parallel
    shuffleIdLock.synchronized {
      // double check to make sure someone else didn't serialize and cache the same
      // mapstatus while we were waiting on the synchronize
      if (checkCachedStatuses()) return retBytes

      // If we got here, we failed to find the serialized locations in the cache, so we pulled
      // out a snapshot of the locations as "statuses"; let's serialize and return that
      val (bytes, bcast) = MapOutputTracker.serializeMapStatuses(statuses, broadcastManager,
        isLocal, minSizeForBroadcast)
      logInfo("Size of output statuses for shuffle %d is %d bytes".format(shuffleId, bytes.length))
      // Add them into the table only if the epoch hasn't changed while we were working
      epochLock.synchronized {
        if (epoch == epochGotten) {
          cachedSerializedStatuses(shuffleId) = bytes
          if (null != bcast) cachedSerializedBroadcast(shuffleId) = bcast
        } else {
          logInfo("Epoch changed, not caching!")
          removeBroadcast(bcast)
        }
      }
      bytes
    }
  }

  /** 停止MapOutPutTracker，清除mapStatuses，清空缓存。 */
  override def stop() {
    mapOutputRequests.offer(PoisonPill)
    threadpool.shutdown()
    sendTracker(StopMapOutputTracker)
    mapStatuses.clear()
    trackerEndpoint = null
    cachedSerializedStatuses.clear()
    clearCachedBroadcast()
    shuffleIdLocks.clear()
  }
}










/**
  * MapOutputTracker for the executors, which fetches map output information from the driver's
  * MapOutputTrackerMaster.
  */
private[spark] class MapOutputTrackerWorker(conf: SparkConf) extends MapOutputTracker(conf) {
  protected val mapStatuses: Map[Int, Array[MapStatus]] =
    new ConcurrentHashMap[Int, Array[MapStatus]]().asScala
}











private[spark] object MapOutputTracker extends Logging {

  val ENDPOINT_NAME = "MapOutputTracker"
  private val DIRECT = 0
  private val BROADCAST = 1

  // Serialize an array of map output locations into an efficient byte format so that we can send
  // it to reduce tasks. We do this by compressing the serialized bytes using GZIP. They will
  // generally be pretty compressible because many map outputs will be on the same hostname.
  def serializeMapStatuses(statuses: Array[MapStatus], broadcastManager: BroadcastManager,
                           isLocal: Boolean, minBroadcastSize: Int): (Array[Byte], Broadcast[Array[Byte]]) = {
    val out = new ByteArrayOutputStream
    out.write(DIRECT)
    val objOut = new ObjectOutputStream(new GZIPOutputStream(out))
    Utils.tryWithSafeFinally {
      // Since statuses can be modified in parallel, sync on it
      statuses.synchronized {
        objOut.writeObject(statuses)
      }
    } {
      objOut.close()
    }
    val arr = out.toByteArray
    if (arr.length >= minBroadcastSize) {
      // Use broadcast instead.
      // Important arr(0) is the tag == DIRECT, ignore that while deserializing !
      val bcast = broadcastManager.newBroadcast(arr, isLocal)
      // toByteArray creates copy, so we can reuse out
      out.reset()
      out.write(BROADCAST)
      val oos = new ObjectOutputStream(new GZIPOutputStream(out))
      oos.writeObject(bcast)
      oos.close()
      val outArr = out.toByteArray
      logInfo("Broadcast mapstatuses size = " + outArr.length + ", actual size = " + arr.length)
      (outArr, bcast)
    } else {
      (arr, null)
    }
  }

  // Opposite of serializeMapStatuses.
  def deserializeMapStatuses(bytes: Array[Byte]): Array[MapStatus] = {
    assert (bytes.length > 0)

    def deserializeObject(arr: Array[Byte], off: Int, len: Int): AnyRef = {
      val objIn = new ObjectInputStream(new GZIPInputStream(
        new ByteArrayInputStream(arr, off, len)))
      Utils.tryWithSafeFinally {
        objIn.readObject()
      } {
        objIn.close()
      }
    }

    bytes(0) match {
      case DIRECT =>
        deserializeObject(bytes, 1, bytes.length - 1).asInstanceOf[Array[MapStatus]]
      case BROADCAST =>
        // deserialize the Broadcast, pull .value array out of it, and then deserialize that
        val bcast = deserializeObject(bytes, 1, bytes.length - 1).
          asInstanceOf[Broadcast[Array[Byte]]]
        logInfo("Broadcast mapstatuses size = " + bytes.length +
          ", actual size = " + bcast.value.length)
        // Important - ignore the DIRECT tag ! Start from offset 1
        deserializeObject(bcast.value, 1, bcast.value.length - 1).asInstanceOf[Array[MapStatus]]
      case _ => throw new IllegalArgumentException("Unexpected byte tag = " + bytes(0))
    }
  }

  /**
    * Given an array of map statuses and a range of map output partitions, returns a sequence that,
    * for each block manager ID, lists the shuffle block IDs and corresponding shuffle block sizes
    * stored at that block manager.
    *
    * If any of the statuses is null (indicating a missing location due to a failed mapper),
    * throws a FetchFailedException.
    *
    * @param shuffleId Identifier for the shuffle
    * @param startPartition Start of map output partition ID range (included in range)
    * @param endPartition End of map output partition ID range (excluded from range)
    * @param statuses List of map statuses, indexed by map ID.
    * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
    *         and the second item is a sequence of (shuffle block ID, shuffle block size) tuples
    *         describing the shuffle blocks that are stored at that block manager.
    */
  private def convertMapStatuses(
                                  shuffleId: Int,
                                  startPartition: Int,
                                  endPartition: Int,
                                  statuses: Array[MapStatus]): Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    assert (statuses != null)
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long)]]
    for ((status, mapId) <- statuses.zipWithIndex) {
      if (status == null) {
        val errorMessage = s"Missing an output location for shuffle $shuffleId"
        logError(errorMessage)
        throw new MetadataFetchFailedException(shuffleId, startPartition, errorMessage)
      } else {
        for (part <- startPartition until endPartition) {
          splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
            ((ShuffleBlockId(shuffleId, mapId, part), status.getSizeForBlock(part)))
        }
      }
    }

    splitsByAddress.toSeq
  }
}
