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

import java.io.Serializable
import java.util.Properties

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.source.Source
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.{AccumulatorV2, TaskCompletionListener, TaskFailureListener}


object TaskContext {
  /**
   * Return the currently active TaskContext. This can be called inside of
   * user functions to access contextual information about running tasks.
    * 返回一个当前的活动的TaskContext，这个可以被内部调用去访问contextual信息关于正在运行的task信息
   */
  def get(): TaskContext = taskContext.get

  /**
   * Returns the partition id of currently active TaskContext. It will return 0
   * if there is no active TaskContext for cases like local execution.
    *
    * 返回一个当前活动的TaskContext的分区，他将会返回0如果没有活动的TaskContext比如本地的execution
    *
   */
  def getPartitionId(): Int = {
    val tc = taskContext.get()
    if (tc eq null) {
      0
    } else {
      tc.partitionId()
    }
  }

  private[this] val taskContext: ThreadLocal[TaskContext] = new ThreadLocal[TaskContext]

  // Note: protected[spark] instead of private[spark] to prevent the following two from
  // showing up in JavaDoc.
  // 注意：protected[spark] 代替 private[spark] 是为了防止JavaDoc显示这些


  /**
   * Set the thread local TaskContext. Internal to Spark.
    * 内部设置本地的TaskContext
   */
  protected[spark] def setTaskContext(tc: TaskContext): Unit = taskContext.set(tc)

  /**
   * Unset the thread local TaskContext. Internal to Spark.
    * 内部取消本地的TaskContext
   */
  protected[spark] def unset(): Unit = taskContext.remove()

  /**
   * An empty task context that does not represent an actual task.  This is only used in tests.
    * 一个空的任务上下文，并不代表实际的任务。这仅仅被用于测试
   */
  private[spark] def empty(): TaskContextImpl = {
    new TaskContextImpl(0, 0, 0, 0, null, new Properties, null)
  }
}


/**
 * Contextual information about a task which can be read or mutated during
 * execution. To access the TaskContext for a running task, use:
 * {{{
 *   org.apache.spark.TaskContext.get()
 * }}}
  *
  * 可在任务期间读取或突变的任务的上下文信息执行.访问正在运行的任务的taskcontext，使用：
  *
 */
abstract class TaskContext extends Serializable {
  // Note: TaskContext must NOT define a get method. Otherwise it will prevent the Scala compiler
  // from generating a static get method (based on the companion object's get method).
  // 注：taskcontext不能定义一个获取方法。否则将阻止Scala编译器生成静态get方法（基于伙伴对象的get方法）。

  // Note: Update JavaTaskContextCompileCheck when new methods are added to this class.
  // 注：更新javataskcontextcompilecheck当新的方法加入到这个类。

  // Note: getters in this class are defined with parentheses to maintain backward compatibility.
  // 注意：getters方法在这个类中定义是为了保持向后兼容性。


  /**
   * Returns true if the task has completed.  返回真，如果task已经完成
   */
  def isCompleted(): Boolean

  /**
   * Returns true if the task has been killed. 返回真如果task被杀死了
   */
  def isInterrupted(): Boolean

  /**
   * Returns true if the task is running locally in the driver program.
    *   返回真，如果task是运行在本地的driver驱动程序里
   * @return false
   */
  @deprecated("Local execution was removed, so this always returns false", "2.0.0")
  def isRunningLocally(): Boolean

  /**
   * Adds a (Java friendly) listener to be executed on task completion.
   * This will be called in all situations - success, failure, or cancellation. Adding a listener
   * to an already completed task will result in that listener being called immediately.
   *
    *  在task完成时给executed添加一个（java有好的）监听器
    *  这个将会被各个状态下调用--success, failure, or cancellation（取消），添加一个监听器
    *  给一个已经完成的task，listener将会被立即调用
    *
   * An example use is for HadoopRDD to register a callback to close the input stream.
    *  一个使用HadoopRDD去注册一个回调函数去关闭输入流的例子
   *
   * Exceptions thrown by the listener will result in failure of the task.
    *  如果listener在task中失败，将会抛出异常信息
   */
  def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext

  /**
   * Adds a listener in the form of a Scala closure to be executed on task completion.
   * This will be called in all situations - success, failure, or cancellation. Adding a listener
   * to an already completed task will result in that listener being called immediately.
   *
    * 以一个Scala闭包的形式添加侦听器，以便在任务完成时执行。
    * 这将在所有情况下被调用——成功、失败或取消。添加一个监听器对于已经完成的任务，将立即调用该侦听器。
    *
   * An example use is for HadoopRDD to register a callback to close the input stream.
    *
   *  一个使用HadoopRDD去注册一个回调函数去关闭输入流的例子
    *
   * Exceptions thrown by the listener will result in failure of the task.
    *
    * 如果listener在task中失败，将会抛出异常信息
   */
  def addTaskCompletionListener(f: (TaskContext) => Unit): TaskContext = {
    addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = f(context)
    })
  }

  /**
   * Adds a listener to be executed on task failure. Adding a listener to an already failed task
   * will result in that listener being called immediately.
    *
    *  添加一个侦听器将执行任务失败。将侦听器添加到已经失败的任务中将导致立即调用。
    *
   */
  def addTaskFailureListener(listener: TaskFailureListener): TaskContext

  /**
   * Adds a listener to be executed on task failure.  Adding a listener to an already failed task
   * will result in that listener being called immediately.
    *
    * 添加一个侦听器将执行任务失败。将侦听器添加到已经失败的任务中将导致立即调用。
   */
  def addTaskFailureListener(f: (TaskContext, Throwable) => Unit): TaskContext = {
    addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = f(context, error)
    })
  }

  /**
   * The ID of the stage that this task belong to.           此task属于的阶段ID。
   */
  def stageId(): Int

  /**
   * The ID of the RDD partition that is computed by this task.
    *
    * RDD分区的ID是由这个任务计算。
   */
  def partitionId(): Int

  /**
   * How many times this task has been attempted.  The first task attempt will be assigned
   * attemptNumber = 0, and subsequent attempts will have increasing attempt numbers.
    *
    *  这个task任务尝试了多少次启动。第一次尝试启动attemptNumber会被设置为0，以后逐渐递增
   */
  def attemptNumber(): Int

  /**
   * An ID that is unique to this task attempt (within the same SparkContext, no two task attempts
   * will share the same attempt ID).  This is roughly equivalent to Hadoop's TaskAttemptID.
    *
    *  一个独一无二的task尝试，（相同的SparkContext，没有两个相同的task attempts使用相同的attempt ID）
    *  这大约相当于Hadoop的taskattemptid。
   */
  def taskAttemptId(): Long

  /**
   * Get a local property set upstream in the driver, or null if it is missing. See also
   * `org.apache.spark.SparkContext.setLocalProperty`.
    *
    *  获取驱动程序上游的本地属性集，或者null 如果消失，具体看`org.apache.spark.SparkContext.setLocalProperty`.
   */
  def getLocalProperty(key: String): String

  @DeveloperApi
  def taskMetrics(): TaskMetrics

  /**
   * ::DeveloperApi::
   * Returns all metrics sources with the given name which are associated with the instance
   * which runs the task. For more information see `org.apache.spark.metrics.MetricsSystem`.
    *
    *   返回所有的metrics资源，根据指定的名称，他将是和正在运行的task有关，详情请看`org.apache.spark.metrics.MetricsSystem`.
   */
  @DeveloperApi
  def getMetricsSources(sourceName: String): Seq[Source]

  /**
   * If the task is interrupted, throws TaskKilledException with the reason for the interrupt.
    *  如果task被打断了，将会抛出TaskKilledException异常
   */
  private[spark] def killTaskIfInterrupted(): Unit

  /**
   * If the task is interrupted, the reason this task was killed, otherwise None.
    *
    *  task杀死的时候的interrupted打断
   */
  private[spark] def getKillReason(): Option[String]

  /**
   * Returns the manager for this task's managed memory.
    * 返回此任务的托管内存管理。
   */
  private[spark] def taskMemoryManager(): TaskMemoryManager

  /**
   * Register an accumulator that belongs to this task. Accumulators must call this method when
   * deserializing in executors.
    *
    *   注册一个属于这个任务的累加器，当executors反序列化的时候，必须调用这个函数
   */
  private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit

  /**
   * Record that this task has failed due to a fetch failure from a remote host.  This allows
   * fetch-failure handling to get triggered by the driver, regardless of intervening user-code.
    *
    *   记录一个task失败的原因是因为一个远程主机的失败。
    *   这允许获取失败处理，由驱动程序触发，而不受中间用户代码的影响。
   */
  private[spark] def setFetchFailed(fetchFailed: FetchFailedException): Unit

}
