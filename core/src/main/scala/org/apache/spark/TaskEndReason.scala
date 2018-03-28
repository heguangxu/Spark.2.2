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

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, Utils}

// ==============================================================================================
// NOTE: new task end reasons MUST be accompanied with serialization logic in util.JsonProtocol!
// 注意：任务结束的原因伴随着逻辑的序列化在util.JsonProtocol
// ==============================================================================================

/**
 * :: DeveloperApi ::
 * Various possible reasons why a task ended. The low-level TaskScheduler is supposed to retry
 * tasks several times for "ephemeral" failures, and only report back failures that require some
 * old stages to be resubmitted, such as shuffle map fetch failures.
  *
  * 各种可能的原因，为什么任务结束了。低级别taskscheduler任务调度应该是重试几次“短暂ephemeral”任务失败，
  * 需要一些老的stages重新提交。例如shuffle map的获取失败
 */
@DeveloperApi
sealed trait TaskEndReason

/**
 * :: DeveloperApi ::
 * Task succeeded.      任务成功
 */
@DeveloperApi
case object Success extends TaskEndReason

/**
 * :: DeveloperApi ::
 * Various possible reasons why a task failed.        各种任务失败的原因
 */
@DeveloperApi
sealed trait TaskFailedReason extends TaskEndReason {
  /** Error message displayed in the web UI. 错误的原因将会被显示在web UI界面上*/
  def toErrorString: String

  /**
   * Whether this task failure should be counted towards the maximum number of times the task is
   * allowed to fail before the stage is aborted.  Set to false in cases where the task's failure
   * was unrelated to the task; for example, if the task failed because the executor it was running
   * on was killed.
    *
    * 在stage终止之前，计算任务失败的次数是否超过被允许失败的最大次数。在任务的失败与任务无关的情况下设置
    * 为false；例如，如果任务失败，因为它运行的执行器被杀死了。
    *
   */
  def countTowardsTaskFailures: Boolean = true
}

/**
 * :: DeveloperApi ::
 * A `org.apache.spark.scheduler.ShuffleMapTask` that completed successfully earlier, but we
 * lost the executor before the stage completed. This means Spark needs to reschedule the task
 * to be re-executed on a different executor.
  *
  *    一个`org.apache.spark.scheduler.ShuffleMapTask`任务提前成功。但是在stage完成之前，我们丢失了executor
  *    这意味着Spark需要重新带调度任务在另外一个executor上
 */
@DeveloperApi
case object Resubmitted extends TaskFailedReason {
  override def toErrorString: String = "Resubmitted (resubmitted due to lost executor)"
}

/**
 * :: DeveloperApi ::
 * Task failed to fetch shuffle data from a remote node. Probably means we have lost the remote
 * executors the task is trying to fetch from, and thus need to rerun the previous stage.
  *
  *  Task任务失败从远程主机上获取shuffle的数据，可能的原因是我们丢失了远程主机的executors，因此我们需要返回
  *  以前的stage
 */
@DeveloperApi
case class FetchFailed(
    bmAddress: BlockManagerId,  // Note that bmAddress can be null
    shuffleId: Int,
    mapId: Int,
    reduceId: Int,
    message: String)
  extends TaskFailedReason {
  override def toErrorString: String = {
    val bmAddressString = if (bmAddress == null) "null" else bmAddress.toString
    s"FetchFailed($bmAddressString, shuffleId=$shuffleId, mapId=$mapId, reduceId=$reduceId, " +
      s"message=\n$message\n)"
  }

  /**
   * Fetch failures lead to a different failure handling path: (1) we don't abort the stage after
   * 4 task failures, instead we immediately go back to the stage which generated the map output,
   * and regenerate the missing data.  (2) we don't count fetch failures for blacklisting, since
   * presumably its not the fault of the executor where the task ran, but the executor which
   * stored the data. This is especially important because we might rack up a bunch of
   * fetch-failures in rapid succession, on all nodes of the cluster, due to one bad node.
    *
    * 获取失败导致了不同的故障处理路径：
    * （1）在4个任务失败后，我们不会中止stage，而是立即返回生成map输出的阶段，并重新生成丢失的数据。
    * （2）我们不指望获取失败的黑名单，大概是因为其没有过错的执行者的任务就跑，但是executor存储这些数据。
    *       这一点尤其重要，因为由于一个坏节点在我们可能在集群的所有节点上 迅速地接连捕获一批故障。
   */
  override def countTowardsTaskFailures: Boolean = false
}

/**
 * :: DeveloperApi ::
 * Task failed due to a runtime exception. This is the most common failure case and also captures
 * user program exceptions.
  * 运行时异常导致任务失败。这是最常见的故障情况，也捕获用户程序异常。
 *
 * `stackTrace` contains the stack trace of the exception itself. It still exists for backward
 * compatibility. It's better to use `this(e: Throwable, metrics: Option[TaskMetrics])` to
 * create `ExceptionFailure` as it will handle the backward compatibility properly.
 *
  * `stackTrace`包含stack的异常跟踪。它仍然存在向后兼容性。它的更好的使用这
  * （e: Throwable, metrics: Option[TaskMetrics]）创造` exceptionfailure `将手柄向后兼容性好。
  *
 * `fullStackTrace` is a better representation of the stack trace because it contains the whole
 * stack trace including the exception and its causes
  *
  * ` fullstacktrace `是一个更好的表示的堆栈跟踪，因为它包含整个堆栈跟踪 包括异常及其成因
 *
 * `exception` is the actual exception that caused the task to fail. It may be `None` in
 * the case that the exception is not in fact serializable. If a task fails more than
 * once (due to retries), `exception` is that one that caused the last failure.
  *
  * “异常”是导致任务失败的实际异常。这可能是`None`万一例外事实上不可序列化。如果任务失败不止一次（由于重试）
  * ，`例外`exception`，导致最后的失败。
 */
@DeveloperApi
case class ExceptionFailure(
    className: String,
    description: String,
    stackTrace: Array[StackTraceElement],
    fullStackTrace: String,
    private val exceptionWrapper: Option[ThrowableSerializationWrapper],
    accumUpdates: Seq[AccumulableInfo] = Seq.empty,
    private[spark] var accums: Seq[AccumulatorV2[_, _]] = Nil)
  extends TaskFailedReason {

  /**
   * `preserveCause` is used to keep the exception itself so it is available to the
   * driver. This may be set to `false` in the event that the exception is not in fact
   * serializable.
    * ` preservecause `是用来防止异常本身是可用的驱动程序。这可以被设置为false`在event事件上，
    *  万一例外事实上不可序列化。
   */
  private[spark] def this(
      e: Throwable,
      accumUpdates: Seq[AccumulableInfo],
      preserveCause: Boolean) {
    this(e.getClass.getName, e.getMessage, e.getStackTrace, Utils.exceptionString(e),
      if (preserveCause) Some(new ThrowableSerializationWrapper(e)) else None, accumUpdates)
  }

  private[spark] def this(e: Throwable, accumUpdates: Seq[AccumulableInfo]) {
    this(e, accumUpdates, preserveCause = true)
  }

  private[spark] def withAccums(accums: Seq[AccumulatorV2[_, _]]): ExceptionFailure = {
    this.accums = accums
    this
  }

  def exception: Option[Throwable] = exceptionWrapper.flatMap(w => Option(w.exception))

  override def toErrorString: String =
    if (fullStackTrace == null) {
      // fullStackTrace is added in 1.2.0
      // If fullStackTrace is null, use the old error string for backward compatibility
      exceptionString(className, description, stackTrace)
    } else {
      fullStackTrace
    }

  /**
   * Return a nice string representation of the exception, including the stack trace.
   * Note: It does not include the exception's causes, and is only used for backward compatibility.
    * 返回异常的好字符串表示形式，包括stack堆栈的跟踪。
    * 注意：它不包括异常的原因，只用于向后兼容。
    *
   */
  private def exceptionString(
      className: String,
      description: String,
      stackTrace: Array[StackTraceElement]): String = {
    val desc = if (description == null) "" else description
    val st = if (stackTrace == null) "" else stackTrace.map("        " + _).mkString("\n")
    s"$className: $desc\n$st"
  }
}

/**
 * A class for recovering from exceptions when deserializing a Throwable that was
 * thrown in user task code. If the Throwable cannot be deserialized it will be null,
 * but the stacktrace and message will be preserved correctly in SparkException.
  *
  * 这个类是为了当反序列化在用户的代码中抛出异常的时候能够恢复过来。如果不能反序列化将会抛出NULL,
  * 一种从异常抛出时，将被回收类用户任务代码。如果错误不能被反序列化，它会是空的，
  * 但堆栈跟踪和消息将会被立即保存在sparkexception。
 */
private[spark] class ThrowableSerializationWrapper(var exception: Throwable) extends
    Serializable with Logging {
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(exception)
  }
  private def readObject(in: ObjectInputStream): Unit = {
    try {
      exception = in.readObject().asInstanceOf[Throwable]
    } catch {
      case e : Exception => log.warn("Task exception could not be deserialized", e)
    }
  }
}

/**
 * :: DeveloperApi ::
 * The task finished successfully, but the result was lost from the executor's block manager before
 * it was fetched.
  * task任务成功完成了，但是结果在没取出来之前在executor的块管理中丢失了
 */
@DeveloperApi
case object TaskResultLost extends TaskFailedReason {
  override def toErrorString: String = "TaskResultLost (result lost from block manager)"
}

/**
 * :: DeveloperApi ::
 * Task was killed intentionally and needs to be rescheduled.
  * Task任务被故意杀掉了，而且需要改期运行
 */
@DeveloperApi
case class TaskKilled(reason: String) extends TaskFailedReason {
  override def toErrorString: String = s"TaskKilled ($reason)"
  override def countTowardsTaskFailures: Boolean = false
}

/**
 * :: DeveloperApi ::
 * Task requested the driver to commit, but was denied.
  * Task任务请求driver去提交，但是被否认了
 */
@DeveloperApi
case class TaskCommitDenied(
    jobID: Int,
    partitionID: Int,
    attemptNumber: Int) extends TaskFailedReason {
  override def toErrorString: String = s"TaskCommitDenied (Driver denied task commit)" +
    s" for job: $jobID, partition: $partitionID, attemptNumber: $attemptNumber"
  /**
   * If a task failed because its attempt to commit was denied, do not count this failure
   * towards failing the stage. This is intended to prevent spurious stage failures in cases
   * where many speculative tasks are launched and denied to commit.
    *
    * 如果task任务尝试提交失败了，不要把这次失败看成是stage的失败。
    * 这是为了防止在许多投机性任务被启动和拒绝提交的情况下出现虚假stage故障。
   */
  override def countTowardsTaskFailures: Boolean = false
}

/**
 * :: DeveloperApi ::
 * The task failed because the executor that it was running on was lost. This may happen because
 * the task crashed the JVM.
  * 任务失败了，因为它正在运行的执行器丢失了。这是可能发生的因为task任务造成了JVM的崩溃
 */
@DeveloperApi
case class ExecutorLostFailure(
    execId: String,
    exitCausedByApp: Boolean = true,
    reason: Option[String]) extends TaskFailedReason {
  override def toErrorString: String = {
    val exitBehavior = if (exitCausedByApp) {
      "caused by one of the running tasks"
    } else {
      "unrelated to the running tasks"
    }
    s"ExecutorLostFailure (executor ${execId} exited ${exitBehavior})" +
      reason.map { r => s" Reason: $r" }.getOrElse("")
  }

  override def countTowardsTaskFailures: Boolean = exitCausedByApp
}

/**
 * :: DeveloperApi ::
 * We don't know why the task ended -- for example, because of a ClassNotFound exception when
 * deserializing the task result.
  * 我们不知道任务结束的原因。--例如。因为当任务的结果反序列化的时候报错ClassNotFound
 */
@DeveloperApi
case object UnknownReason extends TaskFailedReason {
  override def toErrorString: String = "UnknownReason"
}
