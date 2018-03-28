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
import java.util.concurrent.{ExecutorService, RejectedExecutionException}

import scala.language.existentials
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{LongAccumulator, ThreadUtils, Utils}

/**
 * Runs a thread pool that deserializes and remotely fetches (if necessary) task results.
  * 运行一个线程池，该线程池对任务结果进行反序列化和远程提取(如果需要)。
  *
  * 利用一个线程池来反序列化Task执行结果或者在必要是抓取Task结果。
  * 这里TaskScheduler处理Task执行结果时，会交给一个后台守护线程池负责。
 */
private[spark] class TaskResultGetter(sparkEnv: SparkEnv, scheduler: TaskSchedulerImpl)
  extends Logging {

  // 设置线程池内线程数，可配置通过spark.resultGetter.threads
  private val THREADS = sparkEnv.conf.getInt("spark.resultGetter.threads", 4)

  // Exposed for testing.
  protected val getTaskResultExecutor: ExecutorService =
    ThreadUtils.newDaemonFixedThreadPool(THREADS, "task-result-getter")

  // Exposed for testing. //设置序列化器
  protected val serializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      sparkEnv.closureSerializer.newInstance()
    }
  }

  protected val taskResultSerializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      sparkEnv.serializer.newInstance()
    }
  }

  /**
    * 定义Task成功执行得到的结果的处理逻辑，处被调用
     */
  def enqueueSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      serializedData: ByteBuffer): Unit = {
    // 通过线程池来获取Task执行的结果
    getTaskResultExecutor.execute(new Runnable {
      override def run(): Unit = Utils.logUncaughtExceptions {
        try {
          val (result, size) = serializer.get().deserialize[TaskResult[_]](serializedData) match {
            //如果是直接发送到Driver端的Task执行结果，未利用BlockManager即Executor
            //发送Task的最后一种情况，考参照Executor端执行步骤9，判断传回Driver的方
            //式
            case directResult: DirectTaskResult[_] =>
              // 不符合抓取Task的大小限制
              if (!taskSetManager.canFetchMoreResults(serializedData.limit())) {
                return
              }
              // deserialize "value" without holding any lock so that it won't block other threads.
              // We should call it here, so that when it's called again in
              // "TaskSetManager.handleSuccessfulTask", it does not need to deserialize the value.
              //这里反序列化的值是不加锁的，这主要是为了保证多线程对其访问时，不会出现
              //其他线程因为本线程而堵塞的情况，这里我们应该先调用它，获得反序列化的
              //值，以便在TaskSetManager#handleSuccessfulTask中再次调用时，不需要再次
              //反序列化该值
              directResult.value(taskResultSerializer.get())
              //得到Task执行的结果，由于是directResult，所以不需要远程读取。
              (directResult, serializedData.limit())
            case IndirectTaskResult(blockId, size) =>
              // 如果Executor返回给Driver的Task执行结果是间接的，需要借助BlockManager
              if (!taskSetManager.canFetchMoreResults(size)) {
                // 如果结果大小比maxResultSize，则在远程节点上（worker）删除该
                // blockManager
                // dropped by executor if size is larger than maxResultSize
                sparkEnv.blockManager.master.removeBlock(blockId)
                return
              }
              //需要从远程节点上抓取Task执行的结果
              logDebug("Fetching indirect task result for TID %s".format(tid))
              //标记Task为需要远程抓取的Task并通知DAGScheduler
              scheduler.handleTaskGettingResult(taskSetManager, tid)
              //从远程节点的BlockManager上获取Task计算结果
              val serializedTaskResult = sparkEnv.blockManager.getRemoteBytes(blockId)
              if (!serializedTaskResult.isDefined) {
                // 在Task执行结束获得结果后到driver远程去抓取结果之间，如果运行task的
                // 机器挂掉，或者该机器的BlockManager已经刷新掉了Task执行结果，都会导致
                // 远程抓取结果失败，即结果丢失。
                /* We won't be able to get the task result if the machine that ran the task failed
                 * between when the task ended and when we tried to fetch the result, or if the
                 * block manager had to flush the result. */
                scheduler.handleFailedTask(
                  taskSetManager, tid, TaskState.FINISHED, TaskResultLost)
                return
              }
              // 远程抓取结果成功，反序列化结果
              val deserializedResult = serializer.get().deserialize[DirectTaskResult[_]](
                serializedTaskResult.get.toByteBuffer)
              // force deserialization of referenced value
              deserializedResult.value(taskResultSerializer.get())
              // 删除远程的结果
              sparkEnv.blockManager.master.removeBlock(blockId)
              // 得到IndirectResult类型的结果
              (deserializedResult, size)
          }

          // Set the task result size in the accumulator updates received from the executors.
          // We need to do this here on the driver because if we did this on the executors then
          // we would have to serialize the result again after updating the size.
          result.accumUpdates = result.accumUpdates.map { a =>
            if (a.name == Some(InternalAccumulator.RESULT_SIZE)) {
              val acc = a.asInstanceOf[LongAccumulator]
              assert(acc.sum == 0L, "task result size should not have been set on the executors")
              acc.setValue(size.toLong)
              acc
            } else {
              a
            }
          }
          /** 该方法主要调用了TaskSchedulerImpl的handleSuccessfulTask方法
            *
            * */
          scheduler.handleSuccessfulTask(taskSetManager, tid, result)
        } catch {
          case cnf: ClassNotFoundException =>
            val loader = Thread.currentThread.getContextClassLoader
            taskSetManager.abort("ClassNotFound with classloader: " + loader)
          // Matching NonFatal so we don't catch the ControlThrowable from the "return" above.
          case NonFatal(ex) =>
            logError("Exception while getting task result", ex)
            taskSetManager.abort("Exception while getting task result: %s".format(ex))
        }
      }
    })
  }

  /**
    * 对于出错或是执行失败的Task，TaskSchedulerImpl#statsUpdate会调用TaskResultGetter#enqueueFailedTask
    * 来处理。这个处理过程与执行成功的Task的处理过程是类似的，它们(执行成功和执行失败的Task)会是公用一个线程池
    * 来执行处理逻辑。
    *
    * // TaskResultGetter# enqueueFailedTask定义Task执行失败的处理逻辑，3处被调用
    * //这部分可以理解为Scheduler的容错功能。
    */
  def enqueueFailedTask(taskSetManager: TaskSetManager, tid: Long, taskState: TaskState,
    serializedData: ByteBuffer) {
    //记录执行失败的原因
    var reason : TaskFailedReason = UnknownReason
    try {
      //调用线程池的一个线程来处理。
      getTaskResultExecutor.execute(new Runnable {
        override def run(): Unit = Utils.logUncaughtExceptions {
          val loader = Utils.getContextOrSparkClassLoader
          try {
            //如果是序列化结果为空或是序列化结果大于规定值，则是序列化失败导致Task执行
            //失败。
            if (serializedData != null && serializedData.limit() > 0) {
              reason = serializer.get().deserialize[TaskFailedReason](
                serializedData, loader)
            }
          } catch {
            // 序列化过程中抛出异常。
            case cnd: ClassNotFoundException =>
              // Log an error but keep going here -- the task failed, so not catastrophic
              // if we can't deserialize the reason.
              //由于Task执行失败并非致命性错误，所以这里只需将信息记录到日志里之后，仍然
              //可以继续执行程序
              logError(
                "Could not deserialize TaskEndReason: ClassNotFound with classloader " + loader)
            case ex: Exception => // No-op
          } finally {
            //调用TaskSchulerImpl#handleFailedTask来处理Task失败，该方法中定义了处理
            //Task失败的核心逻辑。
            // If there's an error while deserializing the TaskEndReason, this Runnable
            // will die. Still tell the scheduler about the task failure, to avoid a hang
            // where the scheduler thinks the task is still running.
            scheduler.handleFailedTask(taskSetManager, tid, taskState, reason)
          }
        }
      })
    } catch {
      case e: RejectedExecutionException if sparkEnv.isStopped =>
        // ignore it
    }
  }

  def stop() {
    getTaskResultExecutor.shutdownNow()
  }
}
