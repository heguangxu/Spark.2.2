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

import java.lang.ref.{ReferenceQueue, WeakReference}
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{RDD, ReliableRDDCheckpointData}
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2, ThreadUtils, Utils}

/**
 * Classes that represent cleaning tasks.  表示清洗tasks的类。
 */
private sealed trait CleanupTask
private case class CleanRDD(rddId: Int) extends CleanupTask
private case class CleanShuffle(shuffleId: Int) extends CleanupTask
private case class CleanBroadcast(broadcastId: Long) extends CleanupTask
private case class CleanAccum(accId: Long) extends CleanupTask
private case class CleanCheckpoint(rddId: Int) extends CleanupTask

/**
 * A WeakReference associated with a CleanupTask. 清除Task的一个弱参考
 *
 * When the referent object becomes only weakly reachable, the corresponding
 * CleanupTaskWeakReference is automatically added to the given reference queue.
  *
  * 当所指对象成为弱可达，相应的cleanuptaskweakreference自动添加到给定的参考队列。
  *
  * CleanupTaskWeakReference是整个cleanup的核心，这是一个普通类，有三个成员，
  * 并且继承WeakReference。referent是一个weak reference，当指向的对象被gc时，
  * 把CleanupTaskWeakReference放到队列里，
  *
 */
private class CleanupTaskWeakReference(
    val task: CleanupTask,
    referent: AnyRef,
    referenceQueue: ReferenceQueue[AnyRef])
  extends WeakReference(referent, referenceQueue)

/**
 * An asynchronous cleaner for RDD, shuffle, and broadcast state.
  * 一个异步的RDD, shuffle, and broadcast 状态
 *
 * This maintains a weak reference for each RDD, ShuffleDependency, and Broadcast of interest,
 * to be processed when the associated object goes out of scope of the application. Actual
 * cleanup is performed in a separate daemon thread.
  *
  * 这个为了维持一个弱的参考，为了每一个RDD,清洗状态，和广播的兴趣，要处理时，相关的对象超出了适用范围。
  * 实际清理是在单独的守护线程中执行的。
  *
  * ContextCleaner用于清理那些超出应用范围的RDD，shuffleDenpendency和Broadcast对象。
  * 由于配置属性spark.cleanner.referenceTracking默认为True,所以构造并且启动ContextCleaner。
  * ContextCleaner组成如下：
  *   1.referenceQueue：缓存顶级的AnyRef引用。
  *   2.referenceBuffer:缓存AnyRef的虚应用。
  *   3.listeners:缓存清理工作的监听器数组。
  *   4.cleaningThread:用于具体清理工作的线程。
  * ContextCleaner的工作原理和listenerBus一样，也采用监听器模式，由线程来处理，此线程实际还是那个只是调用keepCleanning
  * 方法。
  *
 */
private[spark] class ContextCleaner(sc: SparkContext) extends Logging {

  /**
   * A buffer to ensure that `CleanupTaskWeakReference`s are not garbage collected as long as they
   * have not been handled by the reference queue.
    *
    * 一个缓冲区，以确保` cleanuptaskweakreference `不进行垃圾收集，只要他们没有被引用队列处理垃圾。
   */
  private val referenceBuffer =
    Collections.newSetFromMap[CleanupTaskWeakReference](new ConcurrentHashMap)


  private val referenceQueue = new ReferenceQueue[AnyRef]

  // 这里存储的是CleanerListener 在这个文件中
  private val listeners = new ConcurrentLinkedQueue[CleanerListener]()

  // 清理线程===》很重要
  private val cleaningThread = new Thread() { override def run() { keepCleaning() }}

  private val periodicGCService: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("context-cleaner-periodic-gc")

  /**
   * How often to trigger a garbage collection in this JVM.
    *
    * 在这个JVM中触发垃圾收集的频率。
   *
   * This context cleaner triggers cleanups only when weak references are garbage collected.
   * In long-running applications with large driver JVMs, where there is little memory pressure
   * on the driver, this may happen very occasionally or not at all. Not cleaning at all may
   * lead to executors running out of disk space after a while.
    *
    * 这个context 清除者的触发器清理只有弱引用垃圾收集。在大驱动JVM运行的应用程序，有很少的内存压力的驱动，
    * 这可能偶尔或根本不发生。不进行清理可能会导致执行器在一段时间内耗尽磁盘空间。
    *
    *   垃圾收集的周期是30分钟
   */
  private val periodicGCInterval: Long =
    sc.conf.getTimeAsSeconds("spark.cleaner.periodicGC.interval", "30min")

  /**
   * Whether the cleaning thread will block on cleanup tasks (other than shuffle, which
   * is controlled by the `spark.cleaner.referenceTracking.blocking.shuffle` parameter).
    *
    * 清理线程是否会阻塞清除任务（除shuffle之外，它死由`spark.cleaner.referenceTracking.blocking.shuffle`参数控制
    *
   *
   * Due to SPARK-3015, this is set to true by default. This is intended to be only a temporary
   * workaround for the issue, which is ultimately caused by the way the BlockManager endpoints
   * issue inter-dependent blocking RPC messages to each other at high frequencies. This happens,
   * for instance, when the driver performs a GC and cleans up all broadcast blocks that are no
   * longer in scope.
    *
    * 由于spark-3015，这是默认设置为true。这是因为问题只是暂时的解决方法，这是最终的方式blockmanager端点问题
    * 相互依存的阻塞RPC消息给对方造成高频率。例如，当驱动程序执行GC并清理不再在范围内的所有广播块时，
    * 就会发生这种情况。
    *
   */
  private val blockOnCleanupTasks = sc.conf.getBoolean(
    "spark.cleaner.referenceTracking.blocking", true)

  /**
   * Whether the cleaning thread will block on shuffle cleanup tasks.
    * 清理线程是否会阻碍shuffle清理task任务
    *
   *
   * When context cleaner is configured to block on every delete request, it can throw timeout
   * exceptions on cleanup of shuffle blocks, as reported in SPARK-3139. To avoid that, this
   * parameter by default disables blocking on shuffle cleanups. Note that this does not affect
   * the cleanup of RDDs and broadcasts. This is intended to be a temporary workaround,
   * until the real RPC issue (referred to in the comment above `blockOnCleanupTasks`) is
   * resolved.
    *
    * 当context cleaner配置为阻止每一个删除请求，它可以抛出超时异常在洗牌块的时候，据报道在spark-3139。
    * 为了避免这种情况，这个参数默认情况下禁用阻塞洗牌清理。请注意，这并不影清理RDDs和广播。
    * 这是一个临时的解决方案，直到真正的RPC问题（指上面的评论` blockoncleanuptasks `）是解决。
    *
   */
  private val blockOnShuffleCleanupTasks = sc.conf.getBoolean(
    "spark.cleaner.referenceTracking.blocking.shuffle", false)

  @volatile private var stopped = false

  /** Attach a listener object to get information of when objects are cleaned.
    * 附加侦听器对象以获取何时清理对象的时候的信息。
    * */
  def attachListener(listener: CleanerListener): Unit = {
    listeners.add(listener)
  }

  /** Start the cleaner.
    * 开始清理
    * */
  def start(): Unit = {
    // 设置清理线程为守护进程
    cleaningThread.setDaemon(true)
    // 设置守护进程的名称
    cleaningThread.setName("Spark Context Cleaner")
    // 启动守护进程
    cleaningThread.start()

    // scheduleAtFixedRate 在给定的初始延迟之后，并随后在给定的时间内，创建并执行一个已启用的周期操作
    // periodicGCInterval=30分钟 也就是没=每过30分钟运行一次清理线程清理垃圾
    periodicGCService.scheduleAtFixedRate(new Runnable {
      // 执行系统的垃圾清理
      override def run(): Unit = System.gc()
    }, periodicGCInterval, periodicGCInterval, TimeUnit.SECONDS)
  }

  /**
   * Stop the cleaning thread and wait until the thread has finished running its current task.
    * 停止清理线程并等待线程完成其当前任务。
   */
  def stop(): Unit = {
    stopped = true
    // Interrupt the cleaning thread, but wait until the current task has finished before
    // doing so. This guards against the race condition where a cleaning thread may
    // potentially clean similarly named variables created by a different SparkContext,
    // resulting in otherwise inexplicable block-not-found exceptions (SPARK-6132).
    // 中断清理线程，但等待当前任务完成后再执行。
    // This guards against the race condition where a cleaning thread may
    // potentially clean similarly named variables created by a different SparkContext,
    // ，导致其他令人费解的块未发现异常（spark-6132）。
    synchronized {
      // 打断线程
      cleaningThread.interrupt()
    }
    // 设置0 等待这个线程死掉
    cleaningThread.join()
    // 关闭垃圾清理
    periodicGCService.shutdown()
  }

  /** Register an RDD for cleanup when it is garbage collected.
    * 登记清理RDD当它被垃圾收集。  注册一个RDD为了清理   当它被当做垃圾清理
    *
    * 以RDD为例，当注册一个RDD的cleanup的操作时，初始化一个对象CleanRDD(rdd.id)，这样就记住了哪个rdd被回收。
    * */
  def registerRDDForCleanup(rdd: RDD[_]): Unit = {
    registerForCleanup(rdd, CleanRDD(rdd.id))
  }

  // 注册累加器 清理
  def registerAccumulatorForCleanup(a: AccumulatorV2[_, _]): Unit = {
    registerForCleanup(a, CleanAccum(a.id))
  }

  /** Register a ShuffleDependency for cleanup when it is garbage collected.
    * 注册一个shuffle依赖 当垃圾收集开始的时候
    * */
  def registerShuffleForCleanup(shuffleDependency: ShuffleDependency[_, _, _]): Unit = {
    registerForCleanup(shuffleDependency, CleanShuffle(shuffleDependency.shuffleId))
  }

  /** Register a Broadcast for cleanup when it is garbage collected.
    * 注册广播清理
    * */
  def registerBroadcastForCleanup[T](broadcast: Broadcast[T]): Unit = {
    registerForCleanup(broadcast, CleanBroadcast(broadcast.id))
  }

  /** Register a RDDCheckpointData for cleanup when it is garbage collected.
    * 注册RDD检查点RDDCheckpointData
    * */
  def registerRDDCheckpointDataForCleanup[T](rdd: RDD[_], parentId: Int): Unit = {
    registerForCleanup(rdd, CleanCheckpoint(parentId))
  }

  /** Register an object for cleanup.
    *
    * 注册一个对象，为了回收
    * */
  private def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask): Unit = {
    referenceBuffer.add(new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue))
  }

  /** Keep cleaning RDD, shuffle, and broadcast state.
    * 保持一个干净的RDD,shuffle和broadcast状态
    *
    * ContextCleaner的工作原理和listenerBus一样，也采用监听器模式，由线程来处理，此线程实际还是那个只是
    * 调用keepCleanning方法。
    * */
  private def keepCleaning(): Unit = Utils.tryOrStopSparkContext(sc) {
    // 默认一直为真true
    while (!stopped) {
      try {
        val reference = Option(referenceQueue.remove(ContextCleaner.REF_QUEUE_POLL_TIMEOUT))
          .map(_.asInstanceOf[CleanupTaskWeakReference])
        // Synchronize here to avoid being interrupted on stop()
        synchronized {
          reference.foreach { ref =>
            logDebug("Got cleaning task " + ref.task)
            referenceBuffer.remove(ref)
            // 清除Shuffle和Broadcast相关的数据会分别调用doCleanupShuffle和doCleanupBroadcast函数。根据需要清除数据的类型分别调用
            ref.task match {
              case CleanRDD(rddId) =>
                doCleanupRDD(rddId, blocking = blockOnCleanupTasks)
              case CleanShuffle(shuffleId) =>
                doCleanupShuffle(shuffleId, blocking = blockOnShuffleCleanupTasks)
              case CleanBroadcast(broadcastId) =>
                doCleanupBroadcast(broadcastId, blocking = blockOnCleanupTasks)
              case CleanAccum(accId) =>
                doCleanupAccum(accId, blocking = blockOnCleanupTasks)
              case CleanCheckpoint(rddId) =>
                doCleanCheckpoint(rddId)
            }
          }
        }
      } catch {
        case ie: InterruptedException if stopped => // ignore
        case e: Exception => logError("Error in cleaning thread", e)
      }
    }
  }

  /** Perform RDD cleanup.
    * 在ContextCleaner 中会调用RDD.unpersist()来清除已经持久化的RDD数据
    * */
  def doCleanupRDD(rddId: Int, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning RDD " + rddId)
      // 被SparkContext的unpersistRDD方法
      sc.unpersistRDD(rddId, blocking)
      listeners.asScala.foreach(_.rddCleaned(rddId))
      logInfo("Cleaned RDD " + rddId)
    } catch {
      case e: Exception => logError("Error cleaning RDD " + rddId, e)
    }
  }

  /** Perform shuffle cleanup.
    *
    * 清理Shuffle
    * */
  def doCleanupShuffle(shuffleId: Int, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning shuffle " + shuffleId)
      // 把mapOutputTrackerMaster跟踪的shuffle数据不注册（具体做了什么，还没处理）
      mapOutputTrackerMaster.unregisterShuffle(shuffleId)
      // 删除shuffle的块数据
      blockManagerMaster.removeShuffle(shuffleId, blocking)
      listeners.asScala.foreach(_.shuffleCleaned(shuffleId))
      logInfo("Cleaned shuffle " + shuffleId)
    } catch {
      case e: Exception => logError("Error cleaning shuffle " + shuffleId, e)
    }
  }

  /** Perform broadcast cleanup.
    * 清除广播
    * */
  def doCleanupBroadcast(broadcastId: Long, blocking: Boolean): Unit = {
    try {
      logDebug(s"Cleaning broadcast $broadcastId")
      // 广播管理器 清除广播
      broadcastManager.unbroadcast(broadcastId, true, blocking)
      listeners.asScala.foreach(_.broadcastCleaned(broadcastId))
      logDebug(s"Cleaned broadcast $broadcastId")
    } catch {
      case e: Exception => logError("Error cleaning broadcast " + broadcastId, e)
    }
  }

  /** Perform accumulator cleanup.
    * 清除累加器
    * */
  def doCleanupAccum(accId: Long, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning accumulator " + accId)
      AccumulatorContext.remove(accId)
      listeners.asScala.foreach(_.accumCleaned(accId))
      logInfo("Cleaned accumulator " + accId)
    } catch {
      case e: Exception => logError("Error cleaning accumulator " + accId, e)
    }
  }

  /**
   * Clean up checkpoint files written to a reliable storage.
   * Locally checkpointed files are cleaned up separately through RDD cleanups.
    *
    * 清理记录到可靠存储的检查点文件。
    * 局部检查点文件通过RDD清理被单独清理。
   */
  def doCleanCheckpoint(rddId: Int): Unit = {
    try {
      logDebug("Cleaning rdd checkpoint data " + rddId)
      // 这里直接调用文件系统删除  是本地 就本地删除，是hdfs就hdfs删除
      ReliableRDDCheckpointData.cleanCheckpoint(sc, rddId)
      listeners.asScala.foreach(_.checkpointCleaned(rddId))
      logInfo("Cleaned rdd checkpoint data " + rddId)
    }
    catch {
      case e: Exception => logError("Error cleaning rdd checkpoint data " + rddId, e)
    }
  }

  private def blockManagerMaster = sc.env.blockManager.master
  private def broadcastManager = sc.env.broadcastManager
  private def mapOutputTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
}

private object ContextCleaner {
  private val REF_QUEUE_POLL_TIMEOUT = 100
}

/**
 * Listener class used for testing when any item has been cleaned by the Cleaner class.
  *
  * 侦听器类用于测试，当任何项被清洁类清除时，监听。
 */
private[spark] trait CleanerListener {
  def rddCleaned(rddId: Int): Unit
  def shuffleCleaned(shuffleId: Int): Unit
  def broadcastCleaned(broadcastId: Long): Unit
  def accumCleaned(accId: Long): Unit
  def checkpointCleaned(rddId: Long): Unit
}
