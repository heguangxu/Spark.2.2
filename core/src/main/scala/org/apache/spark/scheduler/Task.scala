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

package org.apache.spark.scheduler

import java.nio.ByteBuffer
import java.util.Properties

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.APP_CALLER_CONTEXT
import org.apache.spark.memory.{MemoryMode, TaskMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.util._

/**
 * A unit of execution. We have two kinds of Task's in Spark:
 *
 *  - [[org.apache.spark.scheduler.ShuffleMapTask]]
 *  - [[org.apache.spark.scheduler.ResultTask]]
 *
 * A Spark job consists of one or more stages. The very last stage in a job consists of multiple
 * ResultTasks, while earlier stages consist of ShuffleMapTasks. A ResultTask executes the task
 * and sends the task output back to the driver application. A ShuffleMapTask executes the task
 * and divides the task output to multiple buckets (based on the task's partitioner).
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param partitionId index of the number in the RDD
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 */
private[spark] abstract class Task[T](
    val stageId: Int,
    val stageAttemptId: Int,
    val partitionId: Int,
    @transient var localProperties: Properties = new Properties,
    // The default value is only used in tests.
    serializedTaskMetrics: Array[Byte] =
      SparkEnv.get.closureSerializer.newInstance().serialize(TaskMetrics.registered).array(),
    val jobId: Option[Int] = None,
    val appId: Option[String] = None,
    val appAttemptId: Option[String] = None) extends Serializable {

  @transient lazy val metrics: TaskMetrics =
    SparkEnv.get.closureSerializer.newInstance().deserialize(ByteBuffer.wrap(serializedTaskMetrics))

  /**
   * Called by [[org.apache.spark.executor.Executor]] to run this task.
   *
    * 被Executor调用以执行Task
    *
   * @param taskAttemptId an identifier for this task attempt that is unique within a SparkContext.
   * @param attemptNumber how many times this task has been attempted (0 for the first attempt)
   * @return the result of the task along with updates of Accumulators.
    *   代码逻辑非常简单，概述如下：
        1、需要创建一个Task上下文实例，即TaskContextImpl类型的context，这个TaskContextImpl主要包括以下内容：Task所属Stage的stageId、Task对应数据分区的partitionId、Task执行的taskAttemptId、Task执行的序号attemptNumber、Task内存管理器taskMemoryManager、指标度量系统metricsSystem、内部累加器internalAccumulators、是否本地运行的标志位runningLocally（为false）；
        2、将context放入TaskContext的taskContext变量中，这个taskContext变量为ThreadLocal[TaskContext]；
        3、在任务上下文context中设置主机名localHostName、内部累加器internalAccumulators等Metrics信息；
        4、设置task线程为当前线程；
        5、如果需要杀死task，调用kill()方法，且调用的方式为不中断线程；
        6、调用runTask()方法，传入Task上下文信息context，执行Task，并调用Task上下文的collectAccumulators()方法，收集累加器；
        7、最后，任务上下文context标记Task完成，为unrolling块释放当前线程使用的内存，清楚任务上下文等。
   */
  final def run(
      taskAttemptId: Long,
      attemptNumber: Int,
      metricsSystem: MetricsSystem): T = {
    SparkEnv.get.blockManager.registerTask(taskAttemptId)

    // 创建一个Task上下文实例：TaskContextImpl类型的context
    context = new TaskContextImpl(
      stageId,
      partitionId,
      taskAttemptId,
      attemptNumber,
      taskMemoryManager,
      localProperties,
      metricsSystem,
      metrics)

    //  将context放入TaskContext的taskContext变量中
    // taskContext变量为ThreadLocal[TaskContext]
    TaskContext.setTaskContext(context)

    // task线程为当前线程
    taskThread = Thread.currentThread()

    if (_reasonIfKilled != null) {
      // 如果需要杀死task，调用kill()方法，且调用的方式为不中断线程
      kill(interruptThread = false, _reasonIfKilled)
    }

    // 这个不知道是干嘛的？
    new CallerContext(
      "TASK",
      SparkEnv.get.conf.get(APP_CALLER_CONTEXT),
      appId,
      appAttemptId,
      jobId,
      Option(stageId),
      Option(stageAttemptId),
      Option(taskAttemptId),
      Option(attemptNumber)).setCurrentContext()

    try {
      // 调用runTask()方法，传入Task上下文信息context，执行Task，并调用Task上下文的collectAccumulators()方法，收集累加器
      runTask(context)
    } catch {
      case e: Throwable =>
        // Catch all errors; run task failure callbacks, and rethrow the exception.
        try {
          context.markTaskFailed(e)
        } catch {
          case t: Throwable =>
            e.addSuppressed(t)
        }
        // 上下文标记Task完成
        context.markTaskCompleted(Some(e))
        throw e
    } finally {
      try {
        // Call the task completion callbacks. If "markTaskCompleted" is called twice, the second
        // one is no-op.
        context.markTaskCompleted(None)
      } finally {
        try {
          Utils.tryLogNonFatalError {
            // Release memory used by this thread for unrolling blocks
            // 为unrolling块释放当前线程使用的内存
            SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP)
            SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(
              MemoryMode.OFF_HEAP)
            // Notify any tasks waiting for execution memory to be freed to wake up and try to
            // acquire memory again. This makes impossible the scenario where a task sleeps forever
            // because there are no other tasks left to notify it. Since this is safe to do but may
            // not be strictly necessary, we should revisit whether we can remove this in the
            // future.
            val memoryManager = SparkEnv.get.memoryManager
            memoryManager.synchronized { memoryManager.notifyAll() }
          }
        } finally {
          // Though we unset the ThreadLocal here, the context member variable itself is still
          // queried directly in the TaskRunner to check for FetchFailedExceptions.
          // 释放TaskContext
          TaskContext.unset()
        }
      }
    }
  }

  private var taskMemoryManager: TaskMemoryManager = _

  def setTaskMemoryManager(taskMemoryManager: TaskMemoryManager): Unit = {
    this.taskMemoryManager = taskMemoryManager
  }

  def runTask(context: TaskContext): T

  def defpreferredLocations: Seq[TaskLocation] = Nil

  // Map output tracker epoch. Will be set by TaskSetManager.
  var epoch: Long = -1

  // Task context, to be initialized in run(). 在run()中初始化任务上下文。
  @transient var context: TaskContextImpl = _

  // The actual Thread on which the task is running, if any. Initialized in run().
  @volatile @transient private var taskThread: Thread = _

  // If non-null, this task has been killed and the reason is as specified. This is used in case
  // context is not yet initialized when kill() is invoked.
  @volatile @transient private var _reasonIfKilled: String = null

  protected var _executorDeserializeTime: Long = 0
  protected var _executorDeserializeCpuTime: Long = 0

  /**
   * If defined, this task has been killed and this option contains the reason.
   */
  def reasonIfKilled: Option[String] = Option(_reasonIfKilled)

  /**
   * Returns the amount of time spent deserializing the RDD and function to be run.
   */
  def executorDeserializeTime: Long = _executorDeserializeTime
  def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime

  /**
   * Collect the latest values of accumulators used in this task. If the task failed,
   * filter out the accumulators whose values should not be included on failures.
   */
  def collectAccumulatorUpdates(taskFailed: Boolean = false): Seq[AccumulatorV2[_, _]] = {
    if (context != null) {
      // Note: internal accumulators representing task metrics always count failed values
      context.taskMetrics.nonZeroInternalAccums() ++
        // zero value external accumulators may still be useful, e.g. SQLMetrics, we should not
        // filter them out.
        context.taskMetrics.externalAccums.filter(a => !taskFailed || a.countFailedValues)
    } else {
      Seq.empty
    }
  }

  /**
   * Kills a task by setting the interrupted flag to true. This relies on the upper level Spark
   * code and user code to properly handle the flag. This function should be idempotent so it can
   * be called multiple times.
   * If interruptThread is true, we will also call Thread.interrupt() on the Task's executor thread.
    *
    * 通过将中断的标志设置为true来杀死一个任务。这依赖于上层的Spark代码和用户代码来正确处理标记。这个函数应该是幂等的，所以它可以被多次调用。
    * 如果interruptThread是真的，我们也将在任务的executor线程上调用thread.interrupt()。
   */
  def kill(interruptThread: Boolean, reason: String) {
    require(reason != null)
    _reasonIfKilled = reason
    if (context != null) {
      // TaskContextImpl  志task任务被打断  例如取消了
      context.markInterrupted(reason)
    }

    // 如果 interruptThread为真 而且 taskThread是空的  那么就在线程级别杀死任务，这个是最低级的杀死
    if (interruptThread && taskThread != null) {
      taskThread.interrupt()
    }
  }
}
