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

import java.util.Properties
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util._

/**
 * A [[TaskContext]] implementation.
 *
 * A small note on thread safety. The interrupted & fetchFailed fields are volatile, this makes
 * sure that updates are always visible across threads. The complete & failed flags and their
 * callbacks are protected by locking on the context instance. For instance, this ensures
 * that you cannot add a completion listener in one thread while we are completing (and calling
 * the completion listeners) in another thread. Other state is immutable, however the exposed
 * `TaskMetrics` & `MetricsSystem` objects are not thread safe.
  * 线程安全的小说明。中断与拿取属性值是多变的，这可以确保在跨线程中更新总是可见的。
  * 完整的和失败的标志和回调的上下文实例的锁定保护。例如，这确保了在另外一个线程中我们无法添加一个listener
  * （并调用完成侦听器）时，其他状态是不可变的，但是暴露的` taskmetrics `和` metricssystem `对象
  * 不是线程安全的。
  *
 */
private[spark] class TaskContextImpl(
    val stageId: Int,
    val partitionId: Int,
    override val taskAttemptId: Long,
    override val attemptNumber: Int,
    override val taskMemoryManager: TaskMemoryManager,
    localProperties: Properties,
    @transient private val metricsSystem: MetricsSystem,
    // The default value is only used in tests.  默认值仅仅用用于测试
    override val taskMetrics: TaskMetrics = TaskMetrics.empty)
  extends TaskContext
  with Logging {

  /** List of callback functions to execute when the task completes.
    * 当task完成的时候，返回execute的回调函数
    * */
  @transient private val onCompleteCallbacks = new ArrayBuffer[TaskCompletionListener]

  /** List of callback functions to execute when the task fails.
    * 当task失败的时候，返回execute的回调函数
    * */
  @transient private val onFailureCallbacks = new ArrayBuffer[TaskFailureListener]

  // If defined, the corresponding task has been killed and this option contains the reason.
  // 如果定义，相应的任务已被杀死，此选项包含原因。
  @volatile private var reasonIfKilled: Option[String] = None

  // Whether the task has completed.
  // task任务是否完成
  private var completed: Boolean = false

  // Whether the task has failed.
  // task任务是否完成失败
  private var failed: Boolean = false

  // Throwable that caused the task to fail
  // 因为task失败抛出的异常
  private var failure: Throwable = _

  // If there was a fetch failure in the task, we store it here, to make sure user-code doesn't
  // hide the exception.  See SPARK-19276
  // 如果task获取失败，那么我们在这里存储它，确保用户的代码没有隐藏的异常。具体看SPARK-19276
  @volatile private var _fetchFailedException: Option[FetchFailedException] = None

  @GuardedBy("this")
  override def addTaskCompletionListener(listener: TaskCompletionListener)
      : this.type = synchronized {
    if (completed) {
      listener.onTaskCompletion(this)
    } else {
      onCompleteCallbacks += listener
    }
    this
  }

  @GuardedBy("this")
  override def addTaskFailureListener(listener: TaskFailureListener)
      : this.type = synchronized {
    if (failed) {
      listener.onTaskFailure(this, failure)
    } else {
      onFailureCallbacks += listener
    }
    this
  }

  /** Marks the task as failed and triggers the failure listeners.
    * 标志着任务失败，触发失败的listeners。
    * */
  @GuardedBy("this")
  private[spark] def markTaskFailed(error: Throwable): Unit = synchronized {
    if (failed) return
    failed = true
    failure = error
    invokeListeners(onFailureCallbacks, "TaskFailureListener", Option(error)) {
      _.onTaskFailure(this, error)
    }
  }

  /** Marks the task as completed and triggers the completion listeners.
    * 标记完成的任务并触发完成侦听器。
    * */
  @GuardedBy("this")
  private[spark] def markTaskCompleted(error: Option[Throwable]): Unit = synchronized {
    if (completed) return
    completed = true
    invokeListeners(onCompleteCallbacks, "TaskCompletionListener", error) {
      _.onTaskCompletion(this)
    }
  }

  private def invokeListeners[T](
      listeners: Seq[T],
      name: String,
      error: Option[Throwable])(
      callback: T => Unit): Unit = {
    val errorMsgs = new ArrayBuffer[String](2)
    // Process callbacks in the reverse order of registration
    // 这一句啥意思
    listeners.reverse.foreach { listener =>
      try {
        callback(listener)
      } catch {
        case e: Throwable =>
          errorMsgs += e.getMessage
          logError(s"Error in $name", e)
      }
    }
    if (errorMsgs.nonEmpty) {
      throw new TaskCompletionListenerException(errorMsgs, error)
    }
  }

  /** Marks the task for interruption, i.e. cancellation.
    *  标志task任务被打断  例如取消了
    * */
  private[spark] def markInterrupted(reason: String): Unit = {
    reasonIfKilled = Some(reason)
  }

  private[spark] override def killTaskIfInterrupted(): Unit = {
    val reason = reasonIfKilled
    if (reason.isDefined) {
      throw new TaskKilledException(reason.get)
    }
  }

  private[spark] override def getKillReason(): Option[String] = {
    reasonIfKilled
  }

  @GuardedBy("this")
  override def isCompleted(): Boolean = synchronized(completed)

  override def isRunningLocally(): Boolean = false

  override def isInterrupted(): Boolean = reasonIfKilled.isDefined

  override def getLocalProperty(key: String): String = localProperties.getProperty(key)

  override def getMetricsSources(sourceName: String): Seq[Source] =
    metricsSystem.getSourcesByName(sourceName)

  private[spark] override def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
    taskMetrics.registerAccumulator(a)
  }

  private[spark] override def setFetchFailed(fetchFailed: FetchFailedException): Unit = {
    this._fetchFailedException = Option(fetchFailed)
  }

  private[spark] def fetchFailed: Option[FetchFailedException] = _fetchFailedException

}
