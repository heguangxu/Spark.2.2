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

import java.util.Collections
import java.util.concurrent.TimeUnit

import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.Try

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.java.JavaFutureAction
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.JobWaiter
import org.apache.spark.util.ThreadUtils


/**
  * A future for the result of an action to support cancellation. This is an extension of the
  * Scala Future interface to support cancellation.
  *
  * 一个支持取消action结果的的特性。这是Scala未来接口的扩展，支持取消。
  *
  */
trait FutureAction[T] extends Future[T] {
  // Note that we redefine methods of the Future trait here explicitly so we can specify a different
  // documentation (with reference to the word "action").
  // 注意，我们在这里显式地定义了未来特质的方法，因此我们可以指定一个不同的文档（引用“动作”这个词）。

  /**
    * Cancels the execution of this action.
    * 在这个action中取消execution执行
    */
  def cancel(): Unit

  /**
    * Blocks until this action completes.  阻碍直到这个action完成
    *
    * @param atMost maximum wait time, which may be negative (no waiting is done), Duration.Inf
    *               for unbounded waiting, or a finite positive duration
    *               最长等待时间
    * @return this FutureAction 未来特性
    */
  override def ready(atMost: Duration)(implicit permit: CanAwait): FutureAction.this.type

  /**
    * Awaits and returns the result (of type T) of this action.   等返回结果的类型
    *
    * @param atMost maximum wait time, which may be negative (no waiting is done), Duration.Inf
    *               for unbounded waiting, or a finite positive duration
    * @throws Exception exception during action execution
    * @return the result value if the action is completed within the specific maximum wait time
    */
  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T

  /**
    * When this action is completed, either through an exception, or a value, applies the provided
    * function.
    * 当一个action完成的时候，可以通过一个异常或一个值应用所提供的函数。
    */
  def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit

  /**
    * Returns whether the action has already been completed with a value or an exception.
    * 返回是否已经用值或异常完成操作。
    */
  override def isCompleted: Boolean

  /**
    * Returns whether the action has been cancelled.
    * 返回无论action是否被取消了
    */
  def isCancelled: Boolean

  /**
    * The value of this Future.  未来的特质
    *
    * If the future is not completed the returned value will be None. If the future is completed
    * the value will be Some(Success(t)) if it contains a valid result, or Some(Failure(error)) if
    * it contains an exception.
    * 如果future未完成，返回的值将为零。如果将来完成，值将是Some(Success(t))，如果它包含一个有效的结果，
    * 或者Some(Failure(error))，如果它包含一个异常。
    */
  override def value: Option[Try[T]]

  /**
    * Blocks and returns the result of this job.   阻碍并返回此作业的结果。
    */
  @throws(classOf[SparkException])
  def get(): T = ThreadUtils.awaitResult(this, Duration.Inf)

  /**
    * Returns the job IDs run by the underlying async operation.     返回由底层异步操作获取正在运行的Job ID。
    *
    * This returns the current snapshot of the job list. Certain operations may run multiple
    * jobs, so multiple calls to this method may return different lists.
    *
    * 这返回一个当前的job list快照，可能有很多jobs在调用这个函数，所以会返回不同的结果list
    */
  def jobIds: Seq[Int]

}


/**
  * A [[FutureAction]] holding the result of an action that triggers a single job. Examples include
  * count, collect, reduce.
  *
  *  一个[[FutureAction]]拿着结果一动作触发一个任务。例如计数、收集、减少。
  *
  */
@DeveloperApi
class SimpleFutureAction[T] private[spark](jobWaiter: JobWaiter[_], resultFunc: => T)
  extends FutureAction[T] {

  @volatile private var _cancelled: Boolean = false

  override def cancel() {
    _cancelled = true
    jobWaiter.cancel()
  }

  override def ready(atMost: Duration)(implicit permit: CanAwait): SimpleFutureAction.this.type = {
    jobWaiter.completionFuture.ready(atMost)
    this
  }

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    jobWaiter.completionFuture.ready(atMost)
    assert(value.isDefined, "Future has not completed properly")
    value.get.get
  }

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext) {
    jobWaiter.completionFuture onComplete {_ => func(value.get)}
  }

  override def isCompleted: Boolean = jobWaiter.jobFinished

  override def isCancelled: Boolean = _cancelled

  override def value: Option[Try[T]] =
    jobWaiter.completionFuture.value.map {res => res.map(_ => resultFunc)}

  def jobIds: Seq[Int] = Seq(jobWaiter.jobId)
}


/**
  * Handle via which a "run" function passed to a [[ComplexFutureAction]]
  * can submit jobs for execution.
  *
  * 通过这一柄"run" 功能通过一个[[ComplexFutureAction]]可以提交作业执行。
  *
  */
@DeveloperApi
trait JobSubmitter {
  /**
    * Submit a job for execution and return a FutureAction holding the result.
    * This is a wrapper around the same functionality provided by SparkContext
    * to enable cancellation.
    *
    * 提交一个作业执行并返回一个futureaction  holding 的结果。这是在由sparkcontext使消除相同的功能包装。
    *
    */
  def submitJob[T, U, R](
                          rdd: RDD[T],
                          processPartition: Iterator[T] => U,
                          partitions: Seq[Int],
                          resultHandler: (Int, U) => Unit,
                          resultFunc: => R): FutureAction[R]
}


/**
  * A [[FutureAction]] for actions that could trigger multiple Spark jobs. Examples include take,
  * takeSample. Cancellation works by setting the cancelled flag to true and cancelling any pending
  * jobs.
  */
@DeveloperApi
class ComplexFutureAction[T](run : JobSubmitter => Future[T])
  extends FutureAction[T] { self =>

  @volatile private var _cancelled = false

  @volatile private var subActions: List[FutureAction[_]] = Nil

  // A promise used to signal the future.
  private val p = Promise[T]().tryCompleteWith(run(jobSubmitter))

  override def cancel(): Unit = synchronized {
    _cancelled = true
    p.tryFailure(new SparkException("Action has been cancelled"))
    subActions.foreach(_.cancel())
  }

  private def jobSubmitter = new JobSubmitter {
    def submitJob[T, U, R](
                            rdd: RDD[T],
                            processPartition: Iterator[T] => U,
                            partitions: Seq[Int],
                            resultHandler: (Int, U) => Unit,
                            resultFunc: => R): FutureAction[R] = self.synchronized {
      // If the action hasn't been cancelled yet, submit the job. The check and the submitJob
      // command need to be in an atomic block.
      if (!isCancelled) {
        val job = rdd.context.submitJob(
          rdd,
          processPartition,
          partitions,
          resultHandler,
          resultFunc)
        subActions = job :: subActions
        job
      } else {
        throw new SparkException("Action has been cancelled")
      }
    }
  }

  override def isCancelled: Boolean = _cancelled

  @throws(classOf[InterruptedException])
  @throws(classOf[scala.concurrent.TimeoutException])
  override def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
    p.future.ready(atMost)(permit)
    this
  }

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    p.future.result(atMost)(permit)
  }

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit = {
    p.future.onComplete(func)(executor)
  }

  override def isCompleted: Boolean = p.isCompleted

  override def value: Option[Try[T]] = p.future.value

  def jobIds: Seq[Int] = subActions.flatMap(_.jobIds)

}


private[spark]
class JavaFutureActionWrapper[S, T](futureAction: FutureAction[S], converter: S => T)
  extends JavaFutureAction[T] {

  import scala.collection.JavaConverters._

  override def isCancelled: Boolean = futureAction.isCancelled

  override def isDone: Boolean = {
    // According to java.util.Future's Javadoc, this returns True if the task was completed,
    // whether that completion was due to successful execution, an exception, or a cancellation.
    futureAction.isCancelled || futureAction.isCompleted
  }

  override def jobIds(): java.util.List[java.lang.Integer] = {
    Collections.unmodifiableList(futureAction.jobIds.map(Integer.valueOf).asJava)
  }

  private def getImpl(timeout: Duration): T = {
    // This will throw TimeoutException on timeout:
    ThreadUtils.awaitReady(futureAction, timeout)
    futureAction.value.get match {
      case scala.util.Success(value) => converter(value)
      case scala.util.Failure(exception) =>
        if (isCancelled) {
          throw new CancellationException("Job cancelled").initCause(exception)
        } else {
          // java.util.Future.get() wraps exceptions in ExecutionException
          throw new ExecutionException("Exception thrown by job", exception)
        }
    }
  }

  override def get(): T = getImpl(Duration.Inf)

  override def get(timeout: Long, unit: TimeUnit): T =
    getImpl(Duration.fromNanos(unit.toNanos(timeout)))

  override def cancel(mayInterruptIfRunning: Boolean): Boolean = synchronized {
    if (isDone) {
      // According to java.util.Future's Javadoc, this should return false if the task is completed.
      false
    } else {
      // We're limited in terms of the semantics we can provide here; our cancellation is
      // asynchronous and doesn't provide a mechanism to not cancel if the job is running.
      futureAction.cancel()
      true
    }
  }

}
