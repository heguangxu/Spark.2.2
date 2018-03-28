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

package org.apache.spark.scheduler.local

import java.io.File
import java.net.URL
import java.nio.ByteBuffer

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, TaskState}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.{Executor, ExecutorBackend}
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo

private case class ReviveOffers()

private case class StatusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer)

private case class KillTask(taskId: Long, interruptThread: Boolean, reason: String)

private case class StopExecutor()

/**
 * Calls to [[LocalSchedulerBackend]] are all serialized through LocalEndpoint. Using an
 * RpcEndpoint makes the calls on [[LocalSchedulerBackend]] asynchronous, which is necessary
 * to prevent deadlock between [[LocalSchedulerBackend]] and the [[TaskSchedulerImpl]].
  *
  * 调用LocalSchedulerBackend都是通过LocalEndpoint序列化的。使用RpcEndpoint可以调用local调度程序的异步调用，
  * 这对于防止本地调度程序和任务调度程序之间的死锁是必要的。
  *
  *  1.5 版本是LocalActor现在是LocalEndpoint
  *
  *  创建LocalActor的过程主要是构建本地的Executor
 */
private[spark] class LocalEndpoint(
    override val rpcEnv: RpcEnv,
    userClassPath: Seq[URL],
    scheduler: TaskSchedulerImpl,
    executorBackend: LocalSchedulerBackend,
    private val totalCores: Int)
  extends ThreadSafeRpcEndpoint with Logging {

  // 这里totalCores就是threadCount（线程数）,也就是本地空闲的核数（一个核运行一个线程的情况下）
  private var freeCores = totalCores

  val localExecutorId = SparkContext.DRIVER_IDENTIFIER
  val localExecutorHostname = "localhost"

  /** 在本地创建一个executor */
  private val executor = new Executor(
    localExecutorId, localExecutorHostname, SparkEnv.get, userClassPath, isLocal = true)

  // 处理一个Ref调用send或者reply发送过过来的消息
  override def receive: PartialFunction[Any, Unit] = {
    case ReviveOffers =>
      // 分配资源 运行任务
      reviveOffers()

      // 状态更新
    case StatusUpdate(taskId, state, serializedData) =>
      // 在这个方法中，主要分析完成状态和失败状态的Task后续处理流程的入口。
      scheduler.statusUpdate(taskId, state, serializedData)
      if (TaskState.isFinished(state)) {
        // freeCores数量增加（每个task分配的CPU数目）
        freeCores += scheduler.CPUS_PER_TASK
        // 分配资源 运行任务
        reviveOffers()
      }

      // 杀死任务
    case KillTask(taskId, interruptThread, reason) =>
      executor.killTask(taskId, interruptThread, reason)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case StopExecutor =>
      executor.stop()
      context.reply(true)
  }

  /**
    * 处理步骤：
    *   1.使用ExecutorId，ExecutorHostsName，freeCores(空闲CPU核数)创建WorkerOffer。
    *   2.调用TaskSchedulerImpl的resourceOffers方法分配资源；
    *   3.调用Executor的launchTask方法运行任务。
    */
  def reviveOffers() {
    // WorkerOffer 表示执行器上可用的空闲资源。
    val offers = IndexedSeq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores))
    // 使用TaskSchedulerImpl的resourceOffers方法分配资源
    /** scheduler.resourceOffers(offers) 申请资源 */
    for (task <- scheduler.resourceOffers(offers).flatten) {    //分配资源
      freeCores -= scheduler.CPUS_PER_TASK // 执行器上可用的空闲资源减少
      executor.launchTask(executorBackend, task)  // 执行任务
    }
  }
}

/**
 * Used when running a local version of Spark where the executor, backend, and master all run in
 * the same JVM. It sits behind a [[TaskSchedulerImpl]] and handles launching tasks on a single
 * Executor (created by the [[LocalSchedulerBackend]]) running locally.
  *
  * 在运行一个本地版本的Spark时使用，该版本的executor、后端和master都在同一个JVM中运行。它位于[[TaskSchedulerImpl]]后面，
  * 并处理单个执行器上的启动任务(由[[LocalSchedulerBackend]])在本地运行。
 */
private[spark] class LocalSchedulerBackend(
    conf: SparkConf,
    scheduler: TaskSchedulerImpl,
    val totalCores: Int)
  extends SchedulerBackend with ExecutorBackend with Logging {

  private val appId = "local-" + System.currentTimeMillis
  private var localEndpoint: RpcEndpointRef = null
  private val userClassPath = getUserClasspath(conf)
  private val listenerBus = scheduler.sc.listenerBus
  private val launcherBackend = new LauncherBackend() {
    override def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }

  /**
   * Returns a list of URLs representing the user classpath.
    *
    * 返回一个列表代表用户的classpath
   *
   * @param conf Spark configuration.
   */
  def getUserClasspath(conf: SparkConf): Seq[URL] = {
    val userClassPathStr = conf.getOption("spark.executor.extraClassPath")
    userClassPathStr.map(_.split(File.pathSeparator)).toSeq.flatten.map(new File(_).toURI.toURL)
  }

  // local模式下，都没有用
  launcherBackend.connect()

  override def start() {
    val rpcEnv = SparkEnv.get.rpcEnv
    // 创建一个本地的LocalEndpoint，这里totalCores就是threadCount（线程数）
    // 这一步在本地创建一个executor
    val executorEndpoint = new LocalEndpoint(rpcEnv, userClassPath, scheduler, this, totalCores)
    // 使用一个name名称注册一个[[RpcEndpoint]]，并返回其[[RpcEndpointRef]] rpcEnv也相当于一个listenerbus的功能
    // 调用的是
    localEndpoint = rpcEnv.setupEndpoint("LocalSchedulerBackendEndpoint", executorEndpoint)
    // 发送SparkListenerExecutorAdded事件,这里是否调用了SparkListenerBus.receive()方法？
    listenerBus.post(SparkListenerExecutorAdded(
      System.currentTimeMillis,
      executorEndpoint.localExecutorId,
      new ExecutorInfo(executorEndpoint.localExecutorHostname, totalCores, Map.empty)))
    // 设置这个launcherBackend是为那个应用服务的
    launcherBackend.setAppId(appId)
    // 设置状态是正在运行
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  override def stop() {
    stop(SparkAppHandle.State.FINISHED)
  }

  override def reviveOffers() {
    // 发送单方面的异步消息  ===> LocalEndpoint.reviveOffers()
    localEndpoint.send(ReviveOffers)
  }

  override def defaultParallelism(): Int =
    scheduler.conf.getInt("spark.default.parallelism", totalCores)

  override def killTask(
      taskId: Long, executorId: String, interruptThread: Boolean, reason: String) {
    localEndpoint.send(KillTask(taskId, interruptThread, reason))
  }

  override def statusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer) {
    // 调用的是这个文件中的LocalEndpoint.receive方法
    localEndpoint.send(StatusUpdate(taskId, state, serializedData))
  }

  override def applicationId(): String = appId

  private def stop(finalState: SparkAppHandle.State): Unit = {
    localEndpoint.ask(StopExecutor)
    try {
      launcherBackend.setState(finalState)
    } finally {
      launcherBackend.close()
    }
  }

}
