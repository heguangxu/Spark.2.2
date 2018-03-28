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

package org.apache.spark.deploy.client

import java.util.concurrent._
import java.util.concurrent.{Future => JFuture, ScheduledFuture => JScheduledFuture}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.Master
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.util.{RpcUtils, ThreadUtils}

/**
  * Interface allowing applications to speak with a Spark standalone cluster manager.
  * 接口允许一个spark应用程序使用一个独立的集群管理器。
  *
  * Takes a master URL, an app description, and a listener for cluster events, and calls
  * back the listener when various events occur.
  * 获取集群的主节点的URL、应用程序描述和集群事件的侦听器，并在发生各种事件时调用侦听器。
  *
  *
  * AppClient主要用于代表Application和Master通讯。
  *
  * @param masterUrls Each url should look like spark://host:port.
  *
  *
  *  StandaloneAppClient的创建和启动start() 是在StandaloneSchedulerBackend.start()方法里执行的
  */
private[spark] class StandaloneAppClient(
                                          rpcEnv: RpcEnv,
                                          masterUrls: Array[String],
                                          appDescription: ApplicationDescription,
                                          listener: StandaloneAppClientListener,
                                          conf: SparkConf)
  extends Logging {

  // 主节点RPC的地址  .master("spark://192.168.10.83:7077,spark://192.168.10.84:7077")中的 spark://192.168.10.83:7077,spark://192.168.10.84:7077
  private val masterRpcAddresses = masterUrls.map(RpcAddress.fromSparkURL(_))

  // 注册超时事件
  private val REGISTRATION_TIMEOUT_SECONDS = 20

  // 注册默认次数为3次
  private val REGISTRATION_RETRIES = 3

  private val endpoint = new AtomicReference[RpcEndpointRef]
  private val appId = new AtomicReference[String]
  // 标志着一个主节点注册成功还是是失败
  private val registered = new AtomicBoolean(false)







  // 内部类
  private class ClientEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint
    with Logging {

    private var master: Option[RpcEndpointRef] = None
    // To avoid calling listener.disconnected() multiple times
    // 为了防止多次调用 listener.disconnected() 方法
    private var alreadyDisconnected = false
    // To avoid calling listener.dead() multiple times
    // 为了防止多次调用 listener.dead() 方法
    private val alreadyDead = new AtomicBoolean(false)
    private val registerMasterFutures = new AtomicReference[Array[JFuture[_]]]
    private val registrationRetryTimer = new AtomicReference[JScheduledFuture[_]]

    // A thread pool for registering with masters. Because registering with a master is a blocking
    // action, this thread pool must be able to create "masterRpcAddresses.size" threads at the same
    // time so that we can register with all masters.

    // 用于注册主节点的线程池。因为注册主节点是阻断操作，这个线程池必须能够创建"masterRpcAddresses.size"的线程，同时可以让我们与所有主节点登记。
    private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
      "appclient-register-master-threadpool",
      masterRpcAddresses.length // Make sure we can register with all masters at the same time
      // 确保我们可以在同一时间与所有主节点注册
    )

    // A scheduled executor for scheduling the registration actions
    // 为注册action设置一个注册执行者
    private val registrationRetryThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("appclient-registration-retry-thread")

    override def onStart(): Unit = {
      try {
        // 异步注册所有主节点。向所有的Master注册当前Application.
        // 为什么注册所有主节点？因为可以有多个
        registerWithMaster(1)
      } catch {
        case e: Exception =>
          logWarning("Failed to connect to master", e)
          markDisconnected()
          stop()
      }
    }

    /**
      *  Register with all masters asynchronously and returns an array `Future`s for cancellation.
      *  异步注册所有主节点，并返回一个数组`Future`s以注销。
      *  给Master发送RegisterApplication信号
      */
    private def tryRegisterAllMasters(): Array[JFuture[_]] = {
      // 循环遍历spark://192.168.10.83:7077,spark://192.168.10.84:7077
      for (masterAddress <- masterRpcAddresses) yield {
        registerMasterThreadPool.submit(new Runnable {
          override def run(): Unit = try {
            if (registered.get) {
              return
            }
            logInfo("Connecting to master " + masterAddress.toSparkURL + "...")
            // 获取masterRef，便于向masterEndpoint发送消息
            val masterRef = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
            /** 发送一个RegisterApplication消息 ===》 master.receive方法处理 */
            masterRef.send(RegisterApplication(appDescription, self))
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
          }
        })
      }
    }

    /**
      * Register with all masters asynchronously. It will call `registerWithMaster` every
      * REGISTRATION_TIMEOUT_SECONDS seconds until exceeding REGISTRATION_RETRIES times.
      * Once we connect to a master successfully, all scheduling work and Futures will be cancelled.
      *
      * 异步注册所有主节点。它将会根据REGISTRATION_TIMEOUT_SECONDS设置的时间间隔去调用`registerWithMaster`，
      * 一直到超过REGISTRATION_RETRIES设置的时间的几倍
      * 一旦连接主节点成功，所有的调度工作和Futures将被取消。
      *
      *
      * nthRetry means this is the nth attempt to register with master.
      *
      * 这里因为 .master("spark://192.168.10.83:7077,spark://192.168.10.84:7077")可以这样写，所以，多个都需要注册
      *
      * 代码逻辑：注册主节点，20秒内失败了，就再次注册，如果连续失败3次，那么这个主节点就注册失败了
      */
    private def registerWithMaster(nthRetry: Int) {
      //向所有的Master注册当前App
      //一旦成功连接的一个master，其他将被取消
      registerMasterFutures.set(tryRegisterAllMasters())
      registrationRetryTimer.set(registrationRetryThread.schedule(new Runnable {
        override def run(): Unit = {
          // 如果主节点注册成功
          if (registered.get) {
            registerMasterFutures.get.foreach(_.cancel(true))
            // 停止所有active执行任务的尝试，暂停处理等待任务，并返回等待执行的任务列表。当从这个方法返回时，这些任务从任务队列中被抽干(删除)。
            registerMasterThreadPool.shutdownNow()
            // 如果注册次数超过3次，就表示这个主节点没有注册成功
          } else if (nthRetry >= REGISTRATION_RETRIES) {
            markDead("All masters are unresponsive! Giving up.")
            // 否则取消这一次注册，20秒（REGISTRATION_TIMEOUT_SECONDS=20）后重新注册，重试次数加一
          } else {
            registerMasterFutures.get.foreach(_.cancel(true))
            registerWithMaster(nthRetry + 1)
          }
        }
      }, REGISTRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS))
    }

    /**
      * Send a message to the current master. If we have not yet registered successfully with any
      * master, the message will be dropped.
      *  发送信息给当前的主节点，如果我们没有注册成功到任何一条主节点，那么这个消息将会被丢弃。
      */
    private def sendToMaster(message: Any): Unit = {
      master match {
        case Some(masterRef) => masterRef.send(message)
        case None => logWarning(s"Drop $message because has not yet connected to master")
      }
    }

    private def isPossibleMaster(remoteAddress: RpcAddress): Boolean = {
      masterRpcAddresses.contains(remoteAddress)
    }

    override def receive: PartialFunction[Any, Unit] = {

      // 注册应用成成功事件
      case RegisteredApplication(appId_, masterRef) =>
        // FIXME How to handle the following cases?
        // 1. A master receives multiple registrations and sends back multiple
        // RegisteredApplications due to an unstable network.
        // 2. Receive multiple RegisteredApplication from different masters because the master is
        // changing.
        // 如何处理以下情况?
        //  1 .工作由于网络不稳定，master接收多个注册并返回多个RegisteredApplications。
        //  2。从不同的主人那里收到多个注册申请，因为主人在改变。
        //这里的代码有两个缺陷：
        //1. 一个Master可能接收到多个注册请求，
        // 并且回复多个RegisteredApplication信号，
        //这会导致网络不稳定。
        //2.若master正在变化，
        //则会接收到多个RegisteredApplication信号
        //设置appId
        appId.set(appId_)
        //编辑已经注册
        registered.set(true)
        //创建master引用信息
        master = Some(masterRef)
        //绑定监听
        listener.connected(appId.get)

        // 应用移除事件
      case ApplicationRemoved(message) =>
        markDead("Master removed our application: %s".format(message))
        stop()

        // Executor添加事件
      case ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) =>
        val fullId = appId + "/" + id
        logInfo("Executor added: %s on %s (%s) with %d cores".format(fullId, workerId, hostPort,
          cores))
        listener.executorAdded(fullId, workerId, hostPort, cores, memory)

        // executor更新事件
      case ExecutorUpdated(id, state, message, exitStatus, workerLost) =>
        val fullId = appId + "/" + id
        val messageText = message.map(s => " (" + s + ")").getOrElse("")
        logInfo("Executor updated: %s is now %s%s".format(fullId, state, messageText))
        if (ExecutorState.isFinished(state)) {
          listener.executorRemoved(fullId, message.getOrElse(""), exitStatus, workerLost)
        }

        // 主节点改变事件
      case MasterChanged(masterRef, masterWebUiUrl) =>
        logInfo("Master has changed, new master is at " + masterRef.address.toSparkURL)
        master = Some(masterRef)
        alreadyDisconnected = false
        masterRef.send(MasterChangeAcknowledged(appId.get))
    }


    // 接受事件 并且响应 返回真假
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case StopAppClient =>
        markDead("Application has been stopped.")
        sendToMaster(UnregisterApplication(appId.get))
        context.reply(true)
        stop()

      case r: RequestExecutors =>
        master match {
          case Some(m) => askAndReplyAsync(m, context, r)
          case None =>
            logWarning("Attempted to request executors before registering with Master.")
            context.reply(false)
        }

      case k: KillExecutors =>
        master match {
          case Some(m) => askAndReplyAsync(m, context, k)
          case None =>
            logWarning("Attempted to kill executors before registering with Master.")
            context.reply(false)
        }
    }

    private def askAndReplyAsync[T](
                                     endpointRef: RpcEndpointRef,
                                     context: RpcCallContext,
                                     msg: T): Unit = {
      // Ask a message and create a thread to reply with the result.  Allow thread to be
      // interrupted during shutdown, otherwise context must be notified of NonFatal errors.
      endpointRef.ask[Boolean](msg).andThen {
        case Success(b) => context.reply(b)
        case Failure(ie: InterruptedException) => // Cancelled
        case Failure(NonFatal(t)) => context.sendFailure(t)
      }(ThreadUtils.sameThread)
    }

    override def onDisconnected(address: RpcAddress): Unit = {
      if (master.exists(_.address == address)) {
        logWarning(s"Connection to $address failed; waiting for master to reconnect...")
        markDisconnected()
      }
    }

    override def onNetworkError(cause: Throwable, address: RpcAddress): Unit = {
      if (isPossibleMaster(address)) {
        logWarning(s"Could not connect to $address: $cause")
      }
    }

    /**
      * Notify the listener that we disconnected, if we hadn't already done so before.
      * 如果我们之前没有这样做，请通知侦听器我们断开连接。
      */
    def markDisconnected() {
      if (!alreadyDisconnected) {
        listener.disconnected()
        alreadyDisconnected = true
      }
    }

    // 标志死亡状态
    def markDead(reason: String) {
      if (!alreadyDead.get) {
        // StandaloneAppClientListener死亡了
        listener.dead(reason)
        // 标志已经死亡了
        alreadyDead.set(true)
      }
    }

    override def onStop(): Unit = {
      if (registrationRetryTimer.get != null) {
        registrationRetryTimer.get.cancel(true)
      }
      registrationRetryThread.shutdownNow()
      registerMasterFutures.get.foreach(_.cancel(true))
      registerMasterThreadPool.shutdownNow()
    }

  }







  /** AppClient主要用于代表Application和Master通讯，AppClient在启动的时候，会向Driver的Endpoint注册ClientEndpoint
    *
    * StandaloneAppClient的创建和启动start() 是在StandaloneSchedulerBackend.start()方法里执行的
    * */
  def start() {
    // Just launch an rpcEndpoint; it will call back into the listener.
    // 执行rpcEnv.setupEndpoint("AppClient", new ClientEndpoint(rpcEnv)之前会先调用ClientEndpoint的onStart()方法,
    // 这时候会发送RegisterApplication消息给MasterEndpoint
    endpoint.set(rpcEnv.setupEndpoint("AppClient", new ClientEndpoint(rpcEnv)))
  }

  def stop() {
    if (endpoint.get != null) {
      try {
        val timeout = RpcUtils.askRpcTimeout(conf)
        timeout.awaitResult(endpoint.get.ask[Boolean](StopAppClient))
      } catch {
        case e: TimeoutException =>
          logInfo("Stop request to Master timed out; it may already be shut down.")
      }
      endpoint.set(null)
    }
  }

  /**
    * Request executors from the Master by specifying the total number desired,
    * including existing pending and running executors.
    *
    * 根据指定数量的executor向master主节点请求executors，包括正在准备的executors和正在运行的executors
    *
    * @return whether the request is acknowledged.   请求是否阻塞
    */
  def requestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    if (endpoint.get != null && appId.get != null) {
      endpoint.get.ask[Boolean](RequestExecutors(appId.get, requestedTotal))
    } else {
      logWarning("Attempted to request executors before driver fully initialized.")
      Future.successful(false)
    }
  }

  /**
    * Kill the given list of executors through the Master.
    * 通过主节点杀死给定list集合中的executors
    * @return whether the kill request is acknowledged.  是否确认了杀戮请求。
    */
  def killExecutors(executorIds: Seq[String]): Future[Boolean] = {
    if (endpoint.get != null && appId.get != null) {
      endpoint.get.ask[Boolean](KillExecutors(appId.get, executorIds))
    } else {
      logWarning("Attempted to kill executors before driver fully initialized.")
      Future.successful(false)
    }
  }

}
