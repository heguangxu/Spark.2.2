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

/**
 * A backend interface for scheduling systems that allows plugging in different ones under
 * TaskSchedulerImpl. We assume a Mesos-like model where the application gets resource offers as
 * machines become available and can launch tasks on them.
  *
  * 用于调度系统的后端接口，允许在TaskSchedulerImpl中插入不同的系统。我们假设一个类似mesos的模型，当机器可用时，
  * 应用程序可以获得资源，并可以在它们上启动任务。
  *
  * 在TaskScheduler下层，用于对接不同的资源管理系统，SchedulerBackend是个接口.
  *
  * SchedulerBackend, 两个任务, 申请资源和task执行和管理
  *
  * SchedulerBackend的实现用于与底层资源调度系统交互（如mesos/YARN），配合TaskScheduler实现具体任务执行所需的资源分配，
  * 核心接口是receiveOffers
 */
private[spark] trait SchedulerBackend {
  private val appId = "spark-application-" + System.currentTimeMillis

  def start(): Unit
  def stop(): Unit
  // 重要方法：SchedulerBackend把自己手头上的可用资源交给TaskScheduler，TaskScheduler根据调度策略分配给排队的任务吗，
  // 返回一批可执行的任务描述，SchedulerBackend负责launchTask，即最终把task塞到了executor模型上，executor里的线程池
  // 会执行task的run()
  def reviveOffers(): Unit
  def defaultParallelism(): Int

  /**
   * Requests that an executor kills a running task. 请求执行器杀死正在运行的任务。
   *
   * @param taskId Id of the task.
   * @param executorId Id of the executor the task is running on.
   * @param interruptThread Whether the executor should interrupt the task thread.
   * @param reason The reason for the task kill.
   */
  def killTask(
      taskId: Long,
      executorId: String,
      interruptThread: Boolean,
      reason: String): Unit =
    throw new UnsupportedOperationException

  def isReady(): Boolean = true

  /**
   * Get an application ID associated with the job. 获取与该作业相关的应用程序ID。
   *
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
   * Get the attempt ID for this run, if the cluster manager supports multiple
   * attempts. Applications run in client mode will not have attempt IDs.
    *
    * 如果集群管理器支持多次尝试，则获取该运行的尝试ID。在客户端模式下运行的应用程序将不会有尝试id。
   *
   * @return The application attempt id, if available.
   */
  def applicationAttemptId(): Option[String] = None

  /**
   * Get the URLs for the driver logs. These URLs are used to display the links in the UI
   * Executors tab for the driver.
    *
    * 获取驱动日志的url。这些url用于显示驱动程序的UI executor选项卡中的链接。
    *
   * @return Map containing the log names and their respective URLs
   */
  def getDriverLogUrls: Option[Map[String, String]] = None

}
