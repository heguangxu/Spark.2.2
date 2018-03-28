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

import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable
import scala.concurrent.Future

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util._

/**
  * A heartbeat from executors to the driver. This is a shared message used by several internal
  * components to convey liveness or execution information for in-progress tasks. It will also
  * expire the hosts that have not heartbeated for more than spark.network.timeout.
  * spark.executor.heartbeatInterval should be significantly less than spark.network.timeout.
  *
  * 从执行器到驱动程序的心跳。这是几个内部组件用于传递正在进行任务的活动或执行信息的共享消息。它也将到期，
  * 没有主机heartbeated超过spark.network.timeout.spark.executor.heartbeatinterval应该明显小于spark.network.timeout。
  *
  */
private[spark] case class Heartbeat(
                                     executorId: String,
                                     accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])], // taskId -> accumulator updates
                                     blockManagerId: BlockManagerId)

/**
  * An event that SparkContext uses to notify HeartbeatReceiver that SparkContext.taskScheduler is
  * created.
  *
  * 一个事件，sparkcontext使用它去通知heartbeatreceiver SparkContext.taskScheduler已经被创建。
  *
  */
private[spark] case object TaskSchedulerIsSet

private[spark] case object ExpireDeadHosts

private case class ExecutorRegistered(executorId: String)

private case class ExecutorRemoved(executorId: String)

private[spark] case class HeartbeatResponse(reregisterBlockManager: Boolean)

/**
  * Lives in the driver to receive heartbeats from executors..
  * 运行在driver中从执行器接收心跳…
  */
private[spark] class HeartbeatReceiver(sc: SparkContext, clock: Clock)
  extends SparkListener with ThreadSafeRpcEndpoint with Logging {

  def this(sc: SparkContext) {
    this(sc, new SystemClock)
  }

  sc.addSparkListener(this)

  override val rpcEnv: RpcEnv = sc.env.rpcEnv

  private[spark] var scheduler: TaskScheduler = null

  // executor ID -> timestamp of when the last heartbeat from this executor was received
  // 执行器id >该执行器最后一次心跳收到的时间戳
  private val executorLastSeen = new mutable.HashMap[String, Long]

  // "spark.network.timeout" uses "seconds", while `spark.storage.blockManagerSlaveTimeoutMs` uses
  // "milliseconds"
  // spark.network.timeout的单位是秒，spark.storage.blockManagerSlaveTimeoutMs单位是毫秒
  private val slaveTimeoutMs =
  sc.conf.getTimeAsMs("spark.storage.blockManagerSlaveTimeoutMs", "120s")
  private val executorTimeoutMs =
    sc.conf.getTimeAsSeconds("spark.network.timeout", s"${slaveTimeoutMs}ms") * 1000

  // "spark.network.timeoutInterval" uses "seconds", while
  // "spark.storage.blockManagerTimeoutIntervalMs" uses "milliseconds"
  // spark.network.timeoutInterval的单位是秒，spark.storage.blockManagerTimeoutIntervalMs单位是毫秒
  private val timeoutIntervalMs =
  sc.conf.getTimeAsMs("spark.storage.blockManagerTimeoutIntervalMs", "60s")
  private val checkTimeoutIntervalMs =
    sc.conf.getTimeAsSeconds("spark.network.timeoutInterval", s"${timeoutIntervalMs}ms") * 1000

  private var timeoutCheckingTask: ScheduledFuture[_] = null

  // "eventLoopThread" is used to run some pretty fast actions. The actions running in it should not
  // block the thread for a long time.
  // “eventLoopThread”用于运行一些非常快的actions。运行中的操作不应该阻塞线程很长时间。
  private val eventLoopThread =
  ThreadUtils.newDaemonSingleThreadScheduledExecutor("heartbeat-receiver-event-loop-thread")

  // 杀死Executor的守护进程
  private val killExecutorThread = ThreadUtils.newDaemonSingleThreadExecutor("kill-executor-thread")

  override def onStart(): Unit = {
    // 这里调用的是ScheduledThreadPoolExecutor的scheduleAtFixedRate方法 定时任务
    timeoutCheckingTask = eventLoopThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        Option(self).foreach(_.ask[Boolean](ExpireDeadHosts))
      }
    }, 0, checkTimeoutIntervalMs, TimeUnit.MILLISECONDS)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    // Messages sent and received locally 在本地发送和接收的消息
    case ExecutorRegistered(executorId) =>
      executorLastSeen(executorId) = clock.getTimeMillis()
      // context.repl() 返回一个回调信息给信息的发送者，如果发送者是[[RpcEndpoint]]，那么它的[[RpcEndpoint.receive]]方法将会被调用
      context.reply(true)
    case ExecutorRemoved(executorId) =>
      executorLastSeen.remove(executorId)
      context.reply(true)

      // 处理TaskSchedulerIsSet事件
    case TaskSchedulerIsSet =>
      scheduler = sc.taskScheduler
      context.reply(true)
    case ExpireDeadHosts =>
      expireDeadHosts()
      context.reply(true)

    // Messages received from executors   从executors接收信息
    case heartbeat @ Heartbeat(executorId, accumUpdates, blockManagerId) =>
      // 如果调度器存在，那么接收心跳
      if (scheduler != null) {
        // 如果存储一次接收心跳的executor（格式:executor id >该执行器最后一次心跳收到的时间戳）里面有这个executor，说明这个executor从上个时间到现在都是正常的
        if (executorLastSeen.contains(executorId)) {
          // 更改这个executor的最后心跳时间
          executorLastSeen(executorId) = clock.getTimeMillis()

          eventLoopThread.submit(new Runnable {
            // 执行给定的块。如果有错误，记录非致命错误，并且只抛出致命错误
            override def run(): Unit = Utils.tryLogNonFatalError {
              // 更新正在执行的任务的度量标准，并让master知道BlockManager还活着。
              val unknownExecutor = !scheduler.executorHeartbeatReceived(
                executorId, accumUpdates, blockManagerId)
              // 心跳响应事件HeartbeatResponse
              val response = HeartbeatResponse(reregisterBlockManager = unknownExecutor)
              // 这个响应事件有谁去处理呢？
              context.reply(response)
            }
          })

          // 如果存储一次接收心跳的executor（格式:executor id >该执行器最后一次心跳收到的时间戳）里面没有这个executor，
          // 说明这个executor从上个时间到可能还不存在，或者没有注册，或者死掉了
        } else {
          // This may happen if we get an executor's in-flight heartbeat immediately
          // after we just removed it. It's not really an error condition so we should
          // not log warning here. Otherwise there may be a lot of noise especially if
          // we explicitly remove executors (SPARK-4134).
          // 如果我们在刚取出一个执行器executor的飞行心跳时，可能会发生这种情况。这并不是一个错误的条件，所以我们不应该在这里记录警告。
          // 否则有可能如果我们明确排除执行者是一个很大的噪声尤其是（spark-4134）。
          logDebug(s"Received heartbeat from unknown executor $executorId")
          context.reply(HeartbeatResponse(reregisterBlockManager = true))
        }

        // 如果调度器都不存在，那么心跳没人接受
      } else {
        // Because Executor will sleep several seconds before sending the first "Heartbeat", this
        // case rarely happens. However, if it really happens, log it and ask the executor to
        // register itself again.
        // 因为执行器在发送第一个“心跳”之前会睡几秒钟，这种情况很少发生。但是，如果真的发生了，记录它，并要求执行器再次注册自己。
        logWarning(s"Dropping $heartbeat because TaskScheduler is not ready yet")
        context.reply(HeartbeatResponse(reregisterBlockManager = true))
      }
  }

  /**
    * Send ExecutorRegistered to the event loop to add a new executor. Only for test.
    * 送executorregistered的事件循环来添加一个新的执行者。仅供测试。
    *
    * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
    *         indicate if this operation is successful.
    */
  def addExecutor(executorId: String): Option[Future[Boolean]] = {
    Option(self).map(_.ask[Boolean](ExecutorRegistered(executorId)))
  }

  /**
    * If the heartbeat receiver is not stopped, notify it of executor registrations.
    * 如果心跳接收器没有停止，请通知执行器重新注册。
    */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    addExecutor(executorAdded.executorId)
  }

  /**
    * Send ExecutorRemoved to the event loop to remove an executor. Only for test.
    *
    * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
    *         indicate if this operation is successful.
    */
  def removeExecutor(executorId: String): Option[Future[Boolean]] = {
    Option(self).map(_.ask[Boolean](ExecutorRemoved(executorId)))
  }

  /**
    * If the heartbeat receiver is not stopped, notify it of executor removals so it doesn't
    * log superfluous errors.
    *
    * 如果心跳接收器没有停止，通知它执行器清除它，这样它就不会记录多余的错误。
    *
    * Note that we must do this after the executor is actually removed to guard against the
    * following race condition: if we remove an executor's metadata from our data structure
    * prematurely, we may get an in-flight heartbeat from the executor before the executor is
    * actually removed, in which case we will still mark the executor as a dead host later
    * and expire it with loud error messages.
    *
    * 请注意，我们必须这样做，在执行器实际上是除去防范以下比赛的条件：如果我们从数据结构过早地删除一个executor的元数据，
    * 我们可以在执行者实际上是从执行者得到一个飞行中的心跳，在这种情况下，我们仍将马克遗嘱执行人是一个死的主机后，将它大声的错误消息。
    *
    */
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    removeExecutor(executorRemoved.executorId)
  }

  private def expireDeadHosts(): Unit = {
    // 检查在HeartbeatReceiver最近没有心跳的主机
    logTrace("Checking for hosts with no recent heartbeats in HeartbeatReceiver.")
    val now = clock.getTimeMillis()
    for ((executorId, lastSeenMs) <- executorLastSeen) {
      if (now - lastSeenMs > executorTimeoutMs) {
        logWarning(s"Removing executor $executorId with no recent heartbeats: " +
          s"${now - lastSeenMs} ms exceeds timeout $executorTimeoutMs ms")
        scheduler.executorLost(executorId, SlaveLost("Executor heartbeat " +
          s"timed out after ${now - lastSeenMs} ms"))
        // Asynchronously kill the executor to avoid blocking the current thread
        killExecutorThread.submit(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            // Note: we want to get an executor back after expiring this one,
            // so do not simply call `sc.killExecutor` here (SPARK-8119)
            sc.killAndReplaceExecutor(executorId)
          }
        })
        executorLastSeen.remove(executorId)
      }
    }
  }

  override def onStop(): Unit = {
    if (timeoutCheckingTask != null) {
      timeoutCheckingTask.cancel(true)
    }
    eventLoopThread.shutdownNow()
    killExecutorThread.shutdownNow()
  }
}


private[spark] object HeartbeatReceiver {
  val ENDPOINT_NAME = "HeartbeatReceiver"
}
