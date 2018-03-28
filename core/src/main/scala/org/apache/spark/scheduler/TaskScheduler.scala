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

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AccumulatorV2

/**
 * Low-level task scheduler interface, currently implemented exclusively by
 * [[org.apache.spark.scheduler.TaskSchedulerImpl]].
 * This interface allows plugging in different task schedulers. Each TaskScheduler schedules tasks
 * for a single SparkContext. These schedulers get sets of tasks submitted to them from the
 * DAGScheduler for each stage, and are responsible for sending the tasks to the cluster, running
 * them, retrying if there are failures, and mitigating stragglers. They return events to the
 * DAGScheduler.
  *
  * 低级任务调度程序接口,目前实现完全是由[[org.apache.spark.scheduler.TaskSchedulerImpl]]。
  *
  * 这个接口允许插入不同的任务调度程序。每个任务调度程序为单独一个SparkContext安排任务。这些调度程序从
  * 每个阶段的DAGScheduler中获取提交给它们的任务集，负责将任务发送到集群，运行它们，如果有故障，则重新尝试，
  * 以及减轻掉队的任务。他们将事件返回到DAGScheduler。
  *
  * SchedulerBackend主要起到的作用是为Task分配计算资源。
  *
 */
private[spark] trait TaskScheduler {

  private val appId = "spark-application-" + System.currentTimeMillis

  def rootPool: Pool

  def schedulingMode: SchedulingMode

  def start(): Unit

  // Invoked after system has successfully initialized (typically in spark context).
  // Yarn uses this to bootstrap allocation of resources based on preferred locations,
  // wait for slave registrations, etc.
  // 在系统成功初始化后调用(通常在spark context 环境中)。Yarn利用这一点来引导资源的分配，基于首选地点，等待从属注册，等等。
  def postStartHook() { }

  // Disconnect from the cluster. 断开与集群。
  def stop(): Unit

  // Submit a sequence of tasks to run. 提交一系列要运行的任务。
  def submitTasks(taskSet: TaskSet): Unit

  // Cancel a stage.  取消一个Stage
  def cancelTasks(stageId: Int, interruptThread: Boolean): Unit

  /**
   * Kills a task attempt. 尝试杀死一个任务
   *
   * @return Whether the task was successfully killed.  不管任务是否被成功杀死
   */
  def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean

  // Set the DAG scheduler for upcalls. This is guaranteed to be set before submitTasks is called.
  // 设置upcalls的DAG调度器。这保证在调用submittask之前设置。
  def setDAGScheduler(dagScheduler: DAGScheduler): Unit

  // Get the default level of parallelism to use in the cluster, as a hint for sizing jobs.
  // 在集群中使用默认的并行度，作为大小调整工作的提示。
  def defaultParallelism(): Int

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
    *
    *   更新正在执行的任务的度量标准，并让master知道BlockManager还活着。如果驱动程序driver知道给定的块管理器block manager，则返回true。否则，
    * 返回false，指示区块管理器block manager应该重新注册。
   */
  def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId): Boolean

  /**
   * Get an application ID associated with the job.
    * 获取与该作业相关的应用程序ID。
   *
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
   * Process a lost executor  过程中失去的执行者
   */
  def executorLost(executorId: String, reason: ExecutorLossReason): Unit

  /**
   * Get an application's attempt ID associated with the job.
    * 获取与该作业相关的应用程序的尝试ID。
   *
   * @return An application's Attempt ID
   */
  def applicationAttemptId(): Option[String]

}
