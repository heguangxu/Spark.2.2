/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
  * 授权给Apache软件基金会（ASF）下的一个或多个贡献者许可协议。有关此项版权所有权的额外信息，
  * 请参见此工作分发的通知文件。ASF的许可证文件你的Apache许可证下，版本2（“许可证”）；你可以不使用这个文件除根据本许可证。您可以获得许可证副本。
  *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
  *
  * 除非适用法律或书面同意，在许可证下分发的软件按“是”的方式分发，没有任何保证或条件，无论是明示的还是默示的。查看许可证特定语言的许可证：许可证下的限制。
 */

package org.apache.spark.deploy

import scala.collection.mutable.HashSet
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

import org.apache.log4j.Logger

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.{DriverState, Master}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.util.{SparkExitCode, ThreadUtils, Utils}

/**
 * Proxy that relays messages to the driver.
  * 将消息传递给驱动程序的代理。
 *
 * We currently don't support retry if submission fails. In HA mode, client will submit request to
 * all masters and see which one could handle it.
  * 我们目前不支持重试，如果提交失败。在HA模式下，客户端将向所有主机提交请求，并查看哪一个可以处理它。
  *
  * 这个貌似要废除了
 */
private class ClientEndpoint(
    override val rpcEnv: RpcEnv,
    driverArgs: ClientArguments,
    masterEndpoints: Seq[RpcEndpointRef],
    conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging {

  // A scheduled executor used to send messages at the specified time.
  // 一个计划的executor用于在指定的时间发送messages
  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("client-forward-message")
  // Used to provide the implicit parameter of `Future` methods.\
  // 用于提供隐式参数的`Future`方法。
  private val forwardMessageExecutionContext =
    ExecutionContext.fromExecutor(forwardMessageThread,
      t => t match {
        case ie: InterruptedException => // Exit normally
        case e: Throwable =>
          logError(e.getMessage, e)
          System.exit(SparkExitCode.UNCAUGHT_EXCEPTION)
      })

   private val lostMasters = new HashSet[RpcAddress]
   private var activeMasterEndpoint: RpcEndpointRef = null


  // 这个貌似要废除了
  override def onStart(): Unit = {
    driverArgs.cmd match {
      case "launch" =>
        // TODO: We could add an env variable here and intercept it in `sc.addJar` that would
        //       truncate filesystem paths similar to what YARN does. For now, we just require
        //       people call `addJar` assuming the jar is in the same directory.
        // TODO：我们可以添加一个环境变量来拦截它` sc.addjar `会
        // 截断的文件系统路径类似于yarn做的。现在，我们只是需要调用` addjar `假设jar包是在同一个目录。
        val mainClass = "org.apache.spark.deploy.worker.DriverWrapper"

        val classPathConf = "spark.driver.extraClassPath"
        val classPathEntries = sys.props.get(classPathConf).toSeq.flatMap { cp =>
          cp.split(java.io.File.pathSeparator)
        }

        val libraryPathConf = "spark.driver.extraLibraryPath"
        val libraryPathEntries = sys.props.get(libraryPathConf).toSeq.flatMap { cp =>
          cp.split(java.io.File.pathSeparator)
        }

        val extraJavaOptsConf = "spark.driver.extraJavaOptions"
        val extraJavaOpts = sys.props.get(extraJavaOptsConf)
          .map(Utils.splitCommandString).getOrElse(Seq.empty)
        val sparkJavaOpts = Utils.sparkJavaOpts(conf)
        val javaOpts = sparkJavaOpts ++ extraJavaOpts
        val command = new Command(mainClass,
          Seq("{{WORKER_URL}}", "{{USER_JAR}}", driverArgs.mainClass) ++ driverArgs.driverOptions,
          sys.env, classPathEntries, libraryPathEntries, javaOpts)

        val driverDescription = new DriverDescription(
          driverArgs.jarUrl,
          driverArgs.memory,
          driverArgs.cores,
          driverArgs.supervise,
          command)
        ayncSendToMasterAndForwardReply[SubmitDriverResponse](
          RequestSubmitDriver(driverDescription))

      case "kill" =>
        val driverId = driverArgs.driverId
        ayncSendToMasterAndForwardReply[KillDriverResponse](RequestKillDriver(driverId))
    }
  }

  /**
   * Send the message to master and forward the reply to self asynchronously.
    * 将消息发送给master并异步地将应答转发给自己。
   */
  private def ayncSendToMasterAndForwardReply[T: ClassTag](message: Any): Unit = {
    for (masterEndpoint <- masterEndpoints) {
      masterEndpoint.ask[T](message).onComplete {
        case Success(v) => self.send(v)
        case Failure(e) =>
          logWarning(s"Error sending messages to master $masterEndpoint", e)
      }(forwardMessageExecutionContext)
    }
  }

  /* Find out driver status then exit the JVM
  *  找出驱动程序状态，然后退出JVM
  * */
  def pollAndReportStatus(driverId: String): Unit = {
    // Since ClientEndpoint is the only RpcEndpoint in the process, blocking the event loop thread
    // is fine.
    // 因为clientendpoint是过程中的唯一rpcendpoint，阻塞事件循环线是好的。
    logInfo("... waiting before polling master for driver state")
    Thread.sleep(5000)
    logInfo("... polling master for driver state")
    // ClientEndpoint向Master发送RequestDriverStatus消息，请求Driver状态
    val statusResponse =
      activeMasterEndpoint.askSync[DriverStatusResponse](RequestDriverStatus(driverId))

    // 集群主节点，里面有Driver的id
    if (statusResponse.found) {
      logInfo(s"State of $driverId is ${statusResponse.state.get}")
      // Worker node, if present 工作者节点,如果存在
      (statusResponse.workerId, statusResponse.workerHostPort, statusResponse.state) match {
        case (Some(id), Some(hostPort), Some(DriverState.RUNNING)) =>
          logInfo(s"Driver running on $hostPort ($id)")
        case _ =>
      }
      // Exception, if present
      statusResponse.exception match {
        case Some(e) =>
          logError(s"Exception from cluster was: $e")
          e.printStackTrace()
          System.exit(-1)// -1结束程序
        case _ =>
          // exit方法用于中断正在运行之中的java虚拟机，其中包含的整形参数用来表示状态码。
          // 惯例来说，非零的状态码表示异常终止。零状态码表示正常终止整个程序。 里面的参数自己可以自由设置、、
          // 一般非0 都是非正常关闭、
          System.exit(0)//  0--正常结束程序 1--异常关闭程序
      }
    } else {
      logError(s"ERROR: Cluster master did not recognize $driverId")
      System.exit(-1)
    }
  }

  override def receive: PartialFunction[Any, Unit] = {

    // Master收到RequestSubmitDriver消息，向ClientEndpoint回复SubmitDriverResponse，
    // 表示用户程序已经完成注册
    case SubmitDriverResponse(master, success, driverId, message) =>
      logInfo(message)
      if (success) {
        activeMasterEndpoint = master
        // 找出驱动程序状态，然后退出JVM
        pollAndReportStatus(driverId.get)
      } else if (!Utils.responseFromBackup(message)) {
        System.exit(-1)
      }


    case KillDriverResponse(master, driverId, success, message) =>
      logInfo(message)
      if (success) {
        activeMasterEndpoint = master
        pollAndReportStatus(driverId)
      } else if (!Utils.responseFromBackup(message)) {
        System.exit(-1)
      }
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (!lostMasters.contains(remoteAddress)) {
      logError(s"Error connecting to master $remoteAddress.")
      lostMasters += remoteAddress
      // Note that this heuristic does not account for the fact that a Master can recover within
      // the lifetime of this client. Thus, once a Master is lost it is lost to us forever. This
      // is not currently a concern, however, because this client does not retry submissions.
      if (lostMasters.size >= masterEndpoints.size) {
        logError("No master is available, exiting.")
        System.exit(-1)
      }
    }
  }

  override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    if (!lostMasters.contains(remoteAddress)) {
      logError(s"Error connecting to master ($remoteAddress).")
      logError(s"Cause was: $cause")
      lostMasters += remoteAddress
      if (lostMasters.size >= masterEndpoints.size) {
        logError("No master is available, exiting.")
        System.exit(-1)
      }
    }
  }

  override def onError(cause: Throwable): Unit = {
    logError(s"Error processing messages, exiting.")
    cause.printStackTrace()
    System.exit(-1)
  }

  override def onStop(): Unit = {
    forwardMessageThread.shutdownNow()
  }
}












/**
 * Executable utility for starting and terminating drivers inside of a standalone cluster.
  * 在独立集群中启动和终止驱动程序的可执行实用程序
 */
object Client {
  def main(args: Array[String]) {
    // scalastyle:off println
    if (!sys.props.contains("SPARK_SUBMIT")) {
      println("WARNING: This client is deprecated and will be removed in a future version of Spark")
      println("Use ./bin/spark-submit with \"--master spark://host:port\"")
    }
    // scalastyle:on println

    val conf = new SparkConf()
    val driverArgs = new ClientArguments(args)

    if (!conf.contains("spark.rpc.askTimeout")) {
      conf.set("spark.rpc.askTimeout", "10s")
    }
    Logger.getRootLogger.setLevel(driverArgs.logLevel)

    val rpcEnv =
      RpcEnv.create("driverClient", Utils.localHostName(), 0, conf, new SecurityManager(conf))

    val masterEndpoints = driverArgs.masters.map(RpcAddress.fromSparkURL).
      map(rpcEnv.setupEndpointRef(_, Master.ENDPOINT_NAME))
    // 使用ClientActor初始化actorSystem
    rpcEnv.setupEndpoint("client", new ClientEndpoint(rpcEnv, driverArgs, masterEndpoints, conf))

    //启动并等待actorSystem的结束  
    rpcEnv.awaitTermination()B
  }
}
