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

package org.apache.spark.deploy.master

import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.{ApplicationDescription, DriverDescription,
  ExecutorState, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.MasterMessages._
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.deploy.rest.StandaloneRestServer
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc._
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.util.{ThreadUtils, Utils}

//private[deploy] 意思只能在deploy这个包下访问
// 这个类下面有一个class Master 还有一个伴生对象object Master
private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    webUiPort: Int,
    val securityMgr: SecurityManager,
    val conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging with LeaderElectable {

  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")

  // 给hadoop添加一些配置
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

  // For application IDs  为了生成application的id
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)

  private val WORKER_TIMEOUT_MS = conf.getLong("spark.worker.timeout", 60) * 1000
  private val RETAINED_APPLICATIONS = conf.getInt("spark.deploy.retainedApplications", 200)
  private val RETAINED_DRIVERS = conf.getInt("spark.deploy.retainedDrivers", 200)
  private val REAPER_ITERATIONS = conf.getInt("spark.dead.worker.persistence", 15)

  // 如果想利用Spark本身实现选举和故障恢复，可以设置spark.deploy.recoveryMode为FILESYSTEM.
  // 两种：Zookeeper和FILESYSTEM.
  // Zookeeper:需要设置属性spark.deploy.zookeeper.url==>zookeeper集群的地址
  //           spark.deploy.zookeeper.dir ===》 spark在zookeeper集群上注册节点的路径
  // FILESYSTEM:文件系统故障恢复模式
  //              spark.deploy.recoveryMode ==》 FILESYSTEM
  //              spark.deploy.recoveryDirectory ==》 存储Spark的Master节点的相关信息，以便于故障恢复使用，此种模式可以结合NFS等共享文件系统，
  //                                                  可以再当前Master进程挂掉时候，在另外一个捷电气动Master继续提供服务。
  private val RECOVERY_MODE = conf.get("spark.deploy.recoveryMode", "NONE")
  private val MAX_EXECUTOR_RETRIES = conf.getInt("spark.deploy.maxExecutorRetries", 10)

  val workers = new HashSet[WorkerInfo]
  val idToApp = new HashMap[String, ApplicationInfo]
  private val waitingApps = new ArrayBuffer[ApplicationInfo]
  val apps = new HashSet[ApplicationInfo]

  private val idToWorker = new HashMap[String, WorkerInfo]
  private val addressToWorker = new HashMap[RpcAddress, WorkerInfo]

  private val endpointToApp = new HashMap[RpcEndpointRef, ApplicationInfo]
  private val addressToApp = new HashMap[RpcAddress, ApplicationInfo]
  private val completedApps = new ArrayBuffer[ApplicationInfo]
  private var nextAppNumber = 0

  private val drivers = new HashSet[DriverInfo]
  private val completedDrivers = new ArrayBuffer[DriverInfo]
  // Drivers currently spooled for scheduling
  private val waitingDrivers = new ArrayBuffer[DriverInfo]
  private var nextDriverNumber = 0

  Utils.checkHost(address.host, "Expected hostname")

  //http://blog.csdn.net/u013597009/article/details/53945177
  /*
   Spark测量系统 MetricsSystem
    Spark测量系统，由指定的instance创建，由source、sink组成，周期性地从source获取指标然后发送到sink，其中instance、source、sink的概念如下：
    Instance：指定了谁在使用测量系统，在spark中有一些列如master、worker、executor、client driver这些角色，这些角色创建测量系统用于监控spark状态，目前在spark中已经实现的角色包括master、worker、executor、driver、applications
    Source：指定了从哪里收集测量数据。在Spark测量系统中有两种来源：
      (1) Spark内部指标，比如MasterSource、WorkerSource等，这些源将会收集Spark组件的内部状态
      (2) 普通指标，比例JvmSource，通过配置文件进行配置
    Sink：指定了往哪里输出测量数据
   */
  private val masterMetricsSystem = MetricsSystem.createMetricsSystem("master", conf, securityMgr)
  // 应用程序的监测系统
  private val applicationMetricsSystem = MetricsSystem.createMetricsSystem("applications", conf,
    securityMgr)
  private val masterSource = new MasterSource(this)

  // After onStart, webUi will be set
  private var webUi: MasterWebUI = null

  private val masterPublicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else address.host
  }

  private val masterUrl = address.toSparkURL
  private var masterWebUiUrl: String = _

  private var state = RecoveryState.STANDBY

  //持久化引擎，主要是处理 Master节点/worker节点新创建/移除的时候 持久化一些状态，用于故障恢复
  private var persistenceEngine: PersistenceEngine = _

  // 选举代理人的主要接口（特质） 但是不知道选举什么代理人
  private var leaderElectionAgent: LeaderElectionAgent = _

  private var recoveryCompletionTask: ScheduledFuture[_] = _

  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each app
  // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.
  /** 作为一种临时的解决方案，我们允许用户设置一个标志，它将在节点上执行循环调度(在所有节点中扩展每个应用程序)，
    * 而不是试图将每个应用程序合并到一个小的节点上。
    *
    * Spark默认提供了一种在各个节点进行round-robin的调度，用户可以自己设置这个flag
    * */
  private val spreadOutApps = conf.getBoolean("spark.deploy.spreadOut", true)

  // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
  //spark默认部署的spark.deploy.defaultCores core核数
  private val defaultCores = conf.getInt("spark.deploy.defaultCores", Int.MaxValue)
  //反向代理
  val reverseProxy = conf.getBoolean("spark.ui.reverseProxy", false)
  if (defaultCores < 1) {
    throw new SparkException("spark.deploy.defaultCores must be positive")
  }

  // Alternative application submission gateway that is stable across Spark versions
  // 替代的应用程序提交网关，在Spark版本中是稳定的。
  private val restServerEnabled = conf.getBoolean("spark.master.rest.enabled", true)
  private var restServer: Option[StandaloneRestServer] = None
  private var restServerBoundPort: Option[Int] = None


  override def onStart(): Unit = {
    /**
    17/09/11 11:17:14 INFO Master: Starting Spark master at spark://biluos.com:7079
    17/09/11 11:17:14 INFO Master: Running Spark version 2.2.0
     */
    logInfo("Starting Spark master at " + masterUrl)
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")

    /** 创建MasterUI */
    webUi = new MasterWebUI(this, webUiPort)
    // 绑定
    webUi.bind()
    masterWebUiUrl = "http://" + masterPublicAddress + ":" + webUi.boundPort
    /** 反向代理 默认为false */
    if (reverseProxy) {
      masterWebUiUrl = conf.get("spark.ui.reverseProxyUrl", masterWebUiUrl)
      logInfo(s"Spark Master is acting as a reverse proxy. Master, Workers and " +
       s"Applications UIs are available at $masterWebUiUrl")
    }

    /** 这是一个定时任务啊 WORKER_TIMEOUT_MS=60，所以是每60秒执行一次任务  检查worker节点是否超时 */
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        // self是endpointRef，也就是说这里定时发送masterEndpoint的CheckForWorkerTimeOut信息
        // CheckForWorkerTimeOut：字面意思是检查worker节点是否超时
        // 发到哪呢？
        // 调用的是NettyRpcEndpointRef.send(message: Any)方法===》NettyRpcEnv.send(message: RequestMessage)方法 ===>最后还是自己 ，master.receivec处理，
        // 主要是检查 移除 超时没有发送心跳的 worker节点
        self.send(CheckForWorkerTimeOut)
      }
    }, 0, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)

    // 默认为true
    if (restServerEnabled) {
      val port = conf.getInt("spark.master.rest.port", 6066)
      restServer = Some(new StandaloneRestServer(address.host, port, conf, self, masterUrl))
    }
    restServerBoundPort = restServer.map(_.start())

    // 主节点注册资源 格式如  <app ID>.<executor ID (or "driver")>.<source name>.
    masterMetricsSystem.registerSource(masterSource)
    //启动主节点的MetricsSystem貌似只能启动一次，否则会出问题
    masterMetricsSystem.start()
    //启动应用程序的监测系统
    applicationMetricsSystem.start()
    // Attach the master and app metrics servlet handler to the web ui after the metrics systems are
    // started.
    masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)

    //  以上都是启动master和app的元数据统计系统

    // java的序列化框架
    val serializer = new JavaSerializer(conf)

    //  RECOVERY_MODE = conf.get("spark.deploy.recoveryMode", "NONE")
    //持久化引擎，主要是处理 Master节点/worker节点新创建/移除的时候 持久化一些状态，用于故障恢复
    /** 选择故障恢复的持久化引擎，选择领导选举代理*/
    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, serializer)
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      /**
        * 1.创建一个持久化类，此处为zookeeperPersistenceEngine，主要用来把注册上来的work，driver保存到zk上
        * 2.创建一个选举代理，主要作用是用来主备切换，此处为ZookeeperLeaderElectionAgent
        */
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, serializer)
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))

      /**
        * 用户自定义的恢复策略
        */
      case "CUSTOM" =>
        val clazz = Utils.classForName(conf.get("spark.deploy.recoveryMode.factory"))
        val factory = clazz.getConstructor(classOf[SparkConf], classOf[Serializer])
          .newInstance(conf, serializer)
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
        // 因为spark.deploy.recoveryMode配置默认为NONE，所以匹配这个
        // BlockHolePersistenceEngine:默认的持久化引擎，实际上并不是提供故障恢复的持久化能力。里面都是空方法
        // 这种实现的好处是可以做到对外接口的统一
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_

    // 主要通过利用Curator框架进行主备切换  leaderElectionAgent在ZooKeeperLeaderElectionAgent类中
    leaderElectionAgent = leaderElectionAgent_
  }

  override def onStop() {
    masterMetricsSystem.report()
    applicationMetricsSystem.report()
    // prevent the CompleteRecovery message sending to restarted master
    // 防止已经恢复的信息送到重启的主节点上
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel(true)
    }
    if (checkForWorkerTimeOutTask != null) {
      checkForWorkerTimeOutTask.cancel(true)
    }
    forwardMessageThread.shutdownNow()
    //停止服务绑定web接口
    webUi.stop()
    restServer.foreach(_.stop())
    //资源监控停止
    masterMetricsSystem.stop()
    applicationMetricsSystem.stop()
    // 持久化状态的引擎停止
    persistenceEngine.close()
    // 选举代理停止
    leaderElectionAgent.stop()
  }

  override def electedLeader() {
    // 发送消息
    // local-cluster模式：RpcEndpointRef.send() ===》 最后还是这个类处理下面的receive方法处理
    self.send(ElectedLeader)
  }

  override def revokedLeadership() {
    self.send(RevokedLeadership)
  }

  /**
    *   master的消息处理
    *   Master接收到ElectedLeader消息后会进行选举操作，由于local-cluster模式中只有一个Master，所以persistenceEngine没有持久化的App,
    *   Driver,Worker的信息，所以当前Master即为激活（ALIVE）状态的。这里涉及到Master的故障恢复。
    */
  override def receive: PartialFunction[Any, Unit] = {
    case ElectedLeader =>
      // 被选为Master，首先判断是否该Master原来为active，如果是那么进行Recovery。
      // persistenceEngine.readPersistedData 利用persistenceEngine从ZK上读取App,driver,work的信息
      val (storedApps, storedDrivers, storedWorkers) = persistenceEngine.readPersistedData(rpcEnv)
      // 如果持久化的数据都为空，
      state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedWorkers.isEmpty) {
        // 那么说明都在活着 ，没有持久化数据，壁纸复活状态为活着
        RecoveryState.ALIVE
      } else {
        //正在恢复状态中，否则就是出于复活状态
        RecoveryState.RECOVERING
      }
      /** 我被选为领导人！Master: I have been elected leader! New state: ALIVE */
      logInfo("I have been elected leader! New state: " + state)
      // zk上有数据，说明需要复活
      if (state == RecoveryState.RECOVERING) {
        // 利用ZK上读取App,driver,work的信息进行恢复,回复实际上就是通知Application和Worker
        beginRecovery(storedApps, storedDrivers, storedWorkers)
        recoveryCompletionTask = forwardMessageThread.schedule(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            // 发送复活完成事件，紧接着，由下面的case处理
            self.send(CompleteRecovery)
          }
        }, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
      }


    //  主要是去除回复之后已经离线的App，Driver和Work，这样整个恢复流程才算正式结束
    //  删除没有响应的worker和app，并且将所有没有worker的Driver分配worker
    case CompleteRecovery => completeRecovery()

    // Master将关闭。
    case RevokedLeadership =>
      logError("Leadership has been revoked -- master shutting down.")
      System.exit(0)

    /** 等待work注册上来
      * Master收到RegisterWorker消息后的处理步骤：
      *   1.创建WorkerInfo。
      *   2.注册WorkerInfo.
      *   3.向Worker发送RegisteredWorker消息，表示注册完成。
      *   4.调用schedule方法进行资源调度。
      *
      */
    case RegisterWorker(
      id, workerHost, workerPort, workerRef, cores, memory, workerWebUiUrl, masterAddress) =>
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        workerHost, workerPort, cores, Utils.megabytesToString(memory)))、
      // 如果该Master不是active，不做任何操作，返回
      // 如果注册过该worker id，向sender返回错误
      // worker节点正在恢复中
      if (state == RecoveryState.STANDBY) {
        // workerRef.send()==>RpcEndpointRef.send()===>NettyRpcEnv.send()
        workerRef.send(MasterInStandby)
      } else if (idToWorker.contains(id)) {
        // worker的id重复
        workerRef.send(RegisterWorkerFailed("Duplicate worker ID"))

        // worker 节点不在恢复中
      } else {
        val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
          workerRef, workerWebUiUrl)
        /** registerWorker(worker)：注册WorkerInfo，其实就是将其添加到workers：HashSet[WorkerInfo]中，
          * 并且更新worker id与worker以及worker address与worker的映射关系
          */
        if (registerWorker(worker)) {
          // 如果worker节点注册成功，就持久化worker节点的相关信息，为了以后恢复
          persistenceEngine.addWorker(worker)
          // ===》 由worker.handleRegisterResponse(msg: RegisterWorkerResponse)方法去处理，
          // 这一点有点小郁闷，为啥不是worker.receive方法五处理呢？send方法不应该都是吗？
          // 我查看了一下RegisteredWorker extends DeployMessage with RegisterWorkerResponse ，
          // 原来它的类型是RegisterWorkerResponse
          // ===》 worker.receive ==> 由worker.handleRegisterResponse(msg: RegisterWorkerResponse)方法去处理，
          workerRef.send(RegisteredWorker(self, masterWebUiUrl, masterAddress))
          schedule()
        } else {
          // 如果worker注册失败，发送消息到sender
          val workerAddress = worker.endpoint.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          workerRef.send(RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress))
        }
      }

    // 注册应用程序App
    //如果是standby，那么忽略这个消息
    //否则注册application；返回结果并且开始调度
    case RegisterApplication(description, driver) =>
      // TODO Prevent repeated registrations from some driver
      // 若之前注册过
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
        // StandBy的master会忽略注册消息。如果AppClient发送请求道Standby的master
        // 会粗发超时机制（默认是20秒），超时会重试
      } else {
        logInfo("Registering app " + description.name)
        //创建app
        //创建App应用程序 包括创建时间 appID ，描述，驱动driver,默认的cores(defaultCores)
        val app = createApplication(description, driver)
        // 保存Application相关信息，主要是保存到waitingApps列表中，供待会调用
        registerApplication(app)
        logInfo("Registered app " + description.name + " with ID " + app.id)
        // 将此App的信息持久化到ZK中
        persistenceEngine.addApplication(app)
        // 向ClientEndpoint发送RegisteredApplication ==> org.apache.spark.deploy.client.StandaloneAppClient.ClientEndpoint.receive方法处理
        driver.send(RegisteredApplication(app.id, self))
        // 调度driver和app
        schedule()
      }

    /** 执行进程状态改变的时候执行这个方法
      *
      * Master收到ExecutorStateChanged消息后对推出状态的处理步骤如下：
      *   1.找到占有Executor的Application的ApplicationInfo，以及Executor对应的ExecutorInfo。
      *   2.将ExecutorInfo的状态改为EXITED。
      *   3.EXITED也属于Executor完成状态，所以会将ExecutorInfo从ApplicationInfo和WorkerInfo中移除。
      *   4.由于Executor是非正常退出。所以重新调用schedule给Application进行资源调度。
      */
    case ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
      // 通过idToApp获得app，然后通过app获得executors，从而通过execId获得executor
      // 根据ExecutorId和ApplicationId找到对应的信息
      val execOption = idToApp.get(appId).flatMap(app => app.executors.get(execId))
      execOption match {
        case Some(exec) =>
          val appInfo = idToApp(appId)
          val oldState = exec.state
          exec.state = state

          if (state == ExecutorState.RUNNING) {
            assert(oldState == ExecutorState.LAUNCHING,
              s"executor $execId state transfer from $oldState to RUNNING is illegal")
            appInfo.resetRetryCount()
          }

          // 这个又是发送给谁的呢？发给Application对应的Driver
          exec.application.driver.send(ExecutorUpdated(execId, state, message, exitStatus, false))

          /**
            * executor运行结束了
            */
          if (ExecutorState.isFinished(state)) {
            // Remove this executor from the worker and app
            logInfo(s"Removing executor ${exec.fullId} because it is $state")
            // If an application has already finished, preserve its
            // state to display its information properly on the UI
            // 如果应用程序结束了
            if (!appInfo.isFinished) {
              // 移除executor
              appInfo.removeExecutor(exec)
            }
            // worker节点移除executor
            exec.worker.removeExecutor(exec)

            val normalExit = exitStatus == Some(0)
            // Only retry certain number of times so we don't go into an infinite loop.
            // Important note: this code path is not exercised by tests, so be very careful when
            // changing this `if` condition.
            // 如果不是正常退出，尝试次数大于设置的最大尝试次数  最大尝试次数大于0
            if (!normalExit
                && appInfo.incrementRetryCount() >= MAX_EXECUTOR_RETRIES
                && MAX_EXECUTOR_RETRIES >= 0) { // < 0 disables this application-killing path
              val execs = appInfo.executors.values
              if (!execs.exists(_.state == ExecutorState.RUNNING)) {
                logError(s"Application ${appInfo.desc.name} with ID ${appInfo.id} failed " +
                  s"${appInfo.retryCount} times; removing it")
                // 移除应用程序
                removeApplication(appInfo, ApplicationState.FAILED)
              }
            }
          }

          /**
            * executor重新调度资源
            */
          schedule()
        case None =>
          logWarning(s"Got status update for unknown executor $appId/$execId")
      }

      // driver概念：Spark的驱动器是执行开发程序中的 main方法的进程。它负责开发人员编写的用来创建SparkContext、
    // 创建 RDD，以及进行 RDD 的转化操作和行动操作代码的执行，
    //
    // master在接收到worker上传递过来的driver完成信息后，对driver进行移除处理，移除driver的一些缓存信息。
    // 然后重新调用schedule重新调度资源
    case DriverStateChanged(driverId, state, exception) =>
      state match {
          //当driver的状态是错误，运行结束，被杀死，运行失败 都移除掉driver
        case DriverState.ERROR | DriverState.FINISHED | DriverState.KILLED | DriverState.FAILED =>
          removeDriver(driverId, state, exception)
        case _ =>
          throw new Exception(s"Received unexpected state update for driver $driverId: $state")
      }

    /** worker节点的心跳检测 更新worker的时间戳 workerInfo.lastHeartbeat = System.currentTimeMillis()
      *
      * Master收到Heartbeat消息后的实现用于更新WorkerInfo的lastHeartbeat，即最后一次接收到心跳的时间戳。如果Worker的id与Worker的映射关系
      * （idToWorker）中找不到匹配的Worker,但是Worker的缓存（workers）中却存在此id,那么向Worker发送ReconnectWorker消息。
      */
    case Heartbeat(workerId, worker) =>
      idToWorker.get(workerId) match {
          // 如果worker节点心跳来了
        case Some(workerInfo) =>
          // 记录worker节点的最后心跳时间
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          if (workers.map(_.id).contains(workerId)) {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " Asking it to re-register.")
            worker.send(ReconnectWorker(masterUrl))
          } else {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }

      // 将appId的app的状态置为WAITING，为切换Master做准备。
    case MasterChangeAcknowledged(appId) =>
      idToApp.get(appId) match {
        case Some(app) =>
          logInfo("Application has been re-registered: " + appId)
          app.state = ApplicationState.WAITING
        case None =>
          logWarning("Master change ack from unknown app: " + appId)
      }

      if (canCompleteRecovery) { completeRecovery() }

      // worker节点的调度器状态相应
     // 通过workerId查找到worker，那么worker的state置为ALIVE，
     // 并且查找状态为idDefined的executors，并且将这些executors都加入到app中，
     // 然后保存这些app到worker中。可以理解为Worker在Master端的Recovery
    case WorkerSchedulerStateResponse(workerId, executors, driverIds) =>
      idToWorker.get(workerId) match {
        case Some(worker) =>
          logInfo("Worker has been re-registered: " + workerId)
          worker.state = WorkerState.ALIVE

          val validExecutors = executors.filter(exec => idToApp.get(exec.appId).isDefined)
          for (exec <- validExecutors) {
            val app = idToApp.get(exec.appId).get
            val execInfo = app.addExecutor(worker, exec.cores, Some(exec.execId))
            worker.addExecutor(execInfo)
            execInfo.copyState(exec)
          }

          // 将所有的driver设置为RUNNING然后加入到worker中。
          for (driverId <- driverIds) {
            drivers.find(_.id == driverId).foreach { driver =>
              driver.worker = Some(worker)
              driver.state = DriverState.RUNNING
              worker.addDriver(driver)
            }
          }
        case None =>
          logWarning("Scheduler state from unknown worker: " + workerId)
      }

      if (canCompleteRecovery) { completeRecovery() }

      // worker节点的最后状态
    case WorkerLatestState(workerId, executors, driverIds) =>
      idToWorker.get(workerId) match {
        case Some(worker) =>
          for (exec <- executors) {
            val executorMatches = worker.executors.exists {
              case (_, e) => e.application.id == exec.appId && e.id == exec.execId
            }
            if (!executorMatches) {
              // master doesn't recognize this executor. So just tell worker to kill it.
              worker.endpoint.send(KillExecutor(masterUrl, exec.appId, exec.execId))
            }
          }

          for (driverId <- driverIds) {
            val driverMatches = worker.drivers.exists { case (id, _) => id == driverId }
            if (!driverMatches) {
              // master doesn't recognize this driver. So just tell worker to kill it.
              worker.endpoint.send(KillDriver(driverId))
            }
          }
        case None =>
          logWarning("Worker state from unknown worker: " + workerId)
      }

      // 没有注册的应用程序
    case UnregisterApplication(applicationId) =>
      logInfo(s"Received unregister request from application $applicationId")
      idToApp.get(applicationId).foreach(finishApplication)


    case CheckForWorkerTimeOut =>
      //检查 移除 超时没有发送心跳的 worker节点
      timeOutDeadWorkers()

  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestSubmitDriver(description) =>
      // 如果master不是active，返回错误
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only accept driver submissions in ALIVE state."
        context.reply(SubmitDriverResponse(self, false, None, msg))
        // 否则创建driver，返回成功的消息
      } else {
        logInfo("Driver submitted " + description.command.mainClass)
        val driver = createDriver(description)
        // 创建Driver
        persistenceEngine.addDriver(driver)
        waitingDrivers += driver
        drivers.add(driver)
        // 开始调度
        schedule()

        // TODO: It might be good to instead have the submission client poll the master to determine
        //       the current status of the driver. For now it's simply "fire and forget".

        // org.apache.spark.deploy.ClientEndpoint.receive()方法处理
        context.reply(SubmitDriverResponse(self, true, Some(driver.id),
          s"Driver successfully submitted as ${driver.id}"))
      }

      // 请求杀死driver
    case RequestKillDriver(driverId) =>
      // 如果master不是active，返回错误
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          s"Can only kill drivers in ALIVE state."
        context.reply(KillDriverResponse(self, driverId, success = false, msg))
      } else {
        logInfo("Asked to kill driver " + driverId)
        val driver = drivers.find(_.id == driverId)
        driver match {
          case Some(d) =>
            //如果driver仍然在等待队列，从等待队列删除并且更新driver状态为KILLED
            if (waitingDrivers.contains(d)) {
              waitingDrivers -= d
              self.send(DriverStateChanged(driverId, DriverState.KILLED, None))
            } else {
              // We just notify the worker to kill the driver here. The final bookkeeping occurs
              // on the return path when the worker submits a state change back to the master
              // to notify it that the driver was successfully killed.
              // 通知worker kill driver id的driver。结果会由workder发消息给master ! DriverStateChanged
              // 注意，此时driver不一定被kill，master只是通知了worker去kill driver。
              d.worker.foreach { w =>
                w.endpoint.send(KillDriver(driverId))
              }
            }
            // TODO: It would be nice for this to be a synchronous response
            val msg = s"Kill request for $driverId submitted"
            logInfo(msg)
            context.reply(KillDriverResponse(self, driverId, success = true, msg))

          // driver已经被kill，直接返回结果
          case None =>
            val msg = s"Driver $driverId has already finished or does not exist"
            logWarning(msg)
            context.reply(KillDriverResponse(self, driverId, success = false, msg))
        }
      }

      // request driver status
     // 查找请求的driver，如果找到则返回driver的状态
    case RequestDriverStatus(driverId) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only request driver status in ALIVE state."
        context.reply(
          DriverStatusResponse(found = false, None, None, None, Some(new Exception(msg))))
      } else {
        (drivers ++ completedDrivers).find(_.id == driverId) match {
          case Some(driver) =>
            context.reply(DriverStatusResponse(found = true, Some(driver.state),
              driver.worker.map(_.id), driver.worker.map(_.hostPort), driver.exception))
          case None =>
            context.reply(DriverStatusResponse(found = false, None, None, None, None))
        }
      }

      //请求master的状态
    //向sender返回master的状态
    case RequestMasterState =>
      context.reply(MasterStateResponse(
        address.host, address.port, restServerBoundPort,
        workers.toArray, apps.toArray, completedApps.toArray,
        drivers.toArray, completedDrivers.toArray, state))

    case BoundPortsRequest =>
      context.reply(BoundPortsResponse(address.port, webUi.boundPort, restServerBoundPort))

    case RequestExecutors(appId, requestedTotal) =>
      context.reply(handleRequestExecutors(appId, requestedTotal))

    case KillExecutors(appId, executorIds) =>
      val formattedExecutorIds = formatExecutorIds(executorIds)
      context.reply(handleKillExecutors(appId, formattedExecutorIds))
  }

  // 主节点链接失效
  override def onDisconnected(address: RpcAddress): Unit = {
    // The disconnected client could've been either a worker or an app; remove whichever it was
    logInfo(s"$address got disassociated, removing it.")
    addressToWorker.get(address).foreach(removeWorker)
    addressToApp.get(address).foreach(finishApplication)
    if (state == RecoveryState.RECOVERING && canCompleteRecovery) { completeRecovery() }
  }

  private def canCompleteRecovery =
    workers.count(_.state == WorkerState.UNKNOWN) == 0 &&
      apps.count(_.state == ApplicationState.UNKNOWN) == 0

  // 开始恢复APP
  private def beginRecovery(storedApps: Seq[ApplicationInfo], storedDrivers: Seq[DriverInfo],
      storedWorkers: Seq[WorkerInfo]) {
    // 恢复APP
    for (app <- storedApps) {
      logInfo("Trying to recover app: " + app.id)
      try {
        // 重新注册APP
        registerApplication(app)
        // 将不响应APP 设置为unkonwn 为了在恢复完成的时候，把这些都杀死
        app.state = ApplicationState.UNKNOWN
        app.driver.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo("App " + app.id + " had exception on reconnect")
      }
    }

    //回复Driver
    for (driver <- storedDrivers) {
      // Here we just read in the list of drivers. Any drivers associated with now-lost workers
      // will be re-launched when we detect that the worker is missing.
      drivers += driver
    }

    //恢复Driver
    for (worker <- storedWorkers) {
      logInfo("Trying to recover worker: " + worker.id)
      try {
        registerWorker(worker)
        // 将不响应worker 设置为unkonwn 为了在恢复完成的时候，把这些都杀死
        worker.state = WorkerState.UNKNOWN
        worker.endpoint.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
      }
    }
  }

  // 完成恢复  杀死掉unknown的app  drive  worker
  //调用时机
  // 1. 在恢复开始后的60s会被强制调用
  // 2. 在每次收到AppClient和Worker的消息回复后会检查如果Application和worker的状态都不为UNKNOWN，则调用
  /**
    * 调用步骤：
    *   1.通过同步保证对于集群恢复只发生一次
    *   2.将所有没有响应的Worker通过调用removeWorker方法，清除
    *   3.将所有没有响应的Application通过finishApplication方法清除
    *   4.将所有没有被调度的Driver重新调度
    */
  private def completeRecovery() {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    // 确保“一次性”恢复语义使用一个短的同步周期。
    if (state != RecoveryState.RECOVERING) { return }
    state = RecoveryState.COMPLETING_RECOVERY

    // Kill off any workers and apps that didn't respond to us.
    // 杀死那些没有回应我们的workers和应用程序。 执行移除worker节点方法removeWorker(),finishApplication()
    // 移除所有未响应的worker和Application
    workers.filter(_.state == WorkerState.UNKNOWN).foreach(removeWorker)
    apps.filter(_.state == ApplicationState.UNKNOWN).foreach(finishApplication)

    // Update the state of recovered apps to RUNNING
    // 更新已恢复应用程序的运行状态。
    apps.filter(_.state == ApplicationState.WAITING).foreach(_.state = ApplicationState.RUNNING)

    // Reschedule drivers which were not claimed by any workers
    // 重新启动那些没有分配Worker的Driver(有可能Worker已经死掉了)
    drivers.filter(_.worker.isEmpty).foreach { d =>
      logWarning(s"Driver ${d.id} was not found after master recovery")
      // Driver是否处于监督状态？这个监督是什么鬼？
      if (d.desc.supervise) {
        // 需要重新启动Driver Client
        logWarning(s"Re-launching ${d.id}")
        /** 重新启动Driver */
        relaunchDriver(d)
      } else {
        /** 移除Driver */
        removeDriver(d.id, DriverState.ERROR, None)
        logWarning(s"Did not re-launch ${d.id} because it was not supervised")
      }
    }

    // 设置Master的状态为ALIVE，此后Master开始正常工作
    state = RecoveryState.ALIVE
    // 目前可用的资源在等待调度程序。每当新应用程序连接或资源可用性更改时，该方法将被调用。开始新一轮的资源调度
    schedule()
    logInfo("Recovery complete - resuming operations!")
  }

  /**
   * Schedule executors to be launched on the workers.
    * 计划调度器运行在worker节点上
   * Returns an array containing number of cores assigned to each worker.
    * 返回一个数组，该数组包含分配给每个worker的核心数。
   *
   * There are two modes of launching executors. The first attempts to spread out an application's
   * executors on as many workers as possible, while the second does the opposite (i.e. launch them
   * on as few workers as possible). The former is usually better for data locality purposes and is
   * the default.
   *
    * 执行者有两种模式。第一次尝试将应用程序的执行器尽可能地分散到尽可能多的workers上，而第二个尝试则相反（即尽可能少地启动workers）。前者通常对数据局部性更好，是默认的。
    *
   * The number of cores assigned to each executor is configurable. When this is explicitly set,
   * multiple executors from the same application may be launched on the same worker if the worker
   * has enough cores and memory. Otherwise, each executor grabs all the cores available on the
   * worker by default, in which case only one executor may be launched on each worker.
   *
    * 分配给每个执行内核数量是可配置。当显式设置时，如果应用程序具有足够的内核和内存，则同一应用程序的多个执行器可以在同一个worker上启动。
    * 否则，每个执行器默认地捕获所有可用的内核，在这种情况下，只有一个执行器可以在每个工人上启动。
    *
   * It is important to allocate coresPerExecutor on each worker at a time (instead of 1 core
   * at a time). Consider the following example: cluster has 4 workers with 16 cores each.
   * User requests 3 executors (spark.cores.max = 48, spark.executor.cores = 16). If 1 core is
   * allocated at a time, 12 cores from each worker would be assigned to each executor.
   * Since 12 < 16, no executors would launch [SPARK-8881].
    *
    *  一次性给每个worker节点分配到足够的coresperexecutor是很重要的（而不是分配1个核心在一个时间）。
    *  考虑下面的例子：集群有4个workers，每个workers核心有16个。用户请求3个executors（spark.cores.max = 48，spark.executor.cores = 16）。
    *  如果每次分配1个内核，则每个执行器将分配12个内核给每个执行器。因为12＜16，没有executors将会运行[ spark-8881 ]。
    *
    *
    * 为Application分配资源选择Worker(Executor),现在有两种策略：
    *   1.尽量打散
    *     将一个Application尽可能的分配到不同的节点，可以通过设置spark.deploy.spreadOut来实现，默认值为true
    *   2.尽量集中
    *     将一个Application金肯呢个分配到尽可能少的节点，CPU密集型而内存占用比较少的Application比较适合这种策略
   */
  private def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean): Array[Int] = {
    val coresPerExecutor = app.desc.coresPerExecutor
    val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
    val oneExecutorPerWorker = coresPerExecutor.isEmpty
    val memoryPerExecutor = app.desc.memoryPerExecutorMB
    val numUsable = usableWorkers.length
    val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
    val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
    var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)

    /** Return whether the specified worker can launch an executor for this app.
      *返回指定的工作者是否可以启动此应用程序的执行器。
      * */
    def canLaunchExecutor(pos: Int): Boolean = {
      val keepScheduling = coresToAssign >= minCoresPerExecutor
      val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor

      // If we allow multiple executors per worker, then we can always launch new executors.
      // 如果允许每个worker执行多个执行器，那么我们总是可以启动新的执行器。
      // Otherwise, if there is already an executor on this worker, just give it more cores.
      // 否则，如果已经有一个执行器在这个worker节点上，就给它更多的核心。
      val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutors(pos) == 0
      if (launchingNewExecutor) {
        val assignedMemory = assignedExecutors(pos) * memoryPerExecutor
        val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor
        val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
        keepScheduling && enoughCores && enoughMemory && underLimit
      } else {
        // We're adding cores to an existing executor, so no need
        // to check memory and executor limits
        // 我们正在向现有的执行器添加内核，因此不需要检查内存和执行器限制。
        keepScheduling && enoughCores
      }
    }

    // Keep launching executors until no more workers can accommodate any
    // more executors, or if we have reached this application's limits
    // 继续启动执行器，直到没有更多的workers可以容纳任何执行器，或者如果我们已经达到了这个应用程序的限制。
    var freeWorkers = (0 until numUsable).filter(canLaunchExecutor)
    while (freeWorkers.nonEmpty) {
      freeWorkers.foreach { pos =>
        var keepScheduling = true
        while (keepScheduling && canLaunchExecutor(pos)) {
          coresToAssign -= minCoresPerExecutor
          assignedCores(pos) += minCoresPerExecutor

          // If we are launching one executor per worker, then every iteration assigns 1 core
          // to the executor. Otherwise, every iteration assigns cores to a new executor.
          // 如果我们在每个worker节点上启动一个executor，那么每个迭代都会为执行器分配1个核心。否则，每次迭代都将内核分配给一个新的执行器。
          if (oneExecutorPerWorker) {
            assignedExecutors(pos) = 1
          } else {
            assignedExecutors(pos) += 1
          }

          // Spreading out an application means spreading out its executors across as
          // many workers as possible. If we are not spreading out, then we should keep
          // scheduling executors on this worker until we use all of its resources.
          // Otherwise, just move on to the next worker.
          // 分发应用程序意味着尽可能多地分散其执行者。如果我们没有展开，那么我们应该继续在这个工作者上调度执行器，直到我们使用它所有的资源。
          /**
            * 尽量打散
            *   如有可能，每个executor分配一个core
            */
          if (spreadOutApps) {
            keepScheduling = false
          }
        }
      }
      freeWorkers = freeWorkers.filter(canLaunchExecutor)
    }
    assignedCores
  }

  /**
   * Schedule and launch executors on workers
    * 在workes节点上调度任务和运行executors进程
    *
   */
  private def startExecutorsOnWorkers(): Unit = {
    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    //这是一个先进先出的调度器，队列，首先处理的是第一个然后是第二个 等等

    //尽可能的分散负载，每轮分配1个
    for (app <- waitingApps if app.coresLeft > 0) {
      val coresPerExecutor: Option[Int] = app.desc.coresPerExecutor
      // Filter out workers that don't have enough resources to launch an executor
      // 筛选出没有足够资源来启动的worker来启动一个executor

      // 将剩余资源越多的work分配到最前面，即假设work1,work2,work3,分别剩余3，2，1个
      // core,则usableWorkers的顺序是work3,work2,work1,如果分配4个core，则work3分2个，work2分1个
      // work1分一个
      val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
        .filter(worker => worker.memoryFree >= app.desc.memoryPerExecutorMB &&
          worker.coresFree >= coresPerExecutor.getOrElse(1))
        .sortBy(_.coresFree).reverse
      val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)

      // Now that we've decided how many cores to allocate on each worker, let's allocate them
      // 既然我们已经决定了在每个工人worker上分配多少核心cores，我们来分配它们。
      for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
        /**
          *  给worker节点真正分配Executor
          *  */
        allocateWorkerResourceToExecutors(
          app, assignedCores(pos), coresPerExecutor, usableWorkers(pos))
      }
    }
  }

  /**
   * Allocate a worker's resources to one or more executors.
    * 给iworker节点分配一个或者更多的executors
   * @param app the info of the application which the executors belong to  这个executors属于哪个应用程序
   * @param assignedCores number of cores on this worker for this application  这个应用程序分配了多少cores在这个节点上的worker
   * @param coresPerExecutor number of cores per executor  每个exxcutor分配多少核数
   * @param worker the worker info
   */
  private def allocateWorkerResourceToExecutors(
      app: ApplicationInfo,
      assignedCores: Int,
      coresPerExecutor: Option[Int],
      worker: WorkerInfo): Unit = {
    // If the number of cores per executor is specified, we divide the cores assigned
    // to this worker evenly among the executors with no remainder.
    // Otherwise, we launch a single executor that grabs all the assignedCores on this worker.
    // 如果指定了每个执行器的内核数，我们将分配给这个工作人员的内核均匀地分配给执行器，没有余数。
    // 否则，我们推出一个单一的执行者，抓住这个worker所有的assignedcores。
    val numExecutors = coresPerExecutor.map { assignedCores / _ }.getOrElse(1)
    val coresToAssign = coresPerExecutor.getOrElse(assignedCores)
    for (i <- 1 to numExecutors) {
      val exec = app.addExecutor(worker, coresToAssign)
      /**
        * 真正开始启动executor
        */
      **/
      launchExecutor(worker, exec)
      app.state = ApplicationState.RUNNING
    }
  }

  /**
   * Schedule the currently available resources among waiting apps. This method will be called
   * every time a new app joins or resource availability changes.
    * 目前可用的资源在等待调度程序。每当新应用程序连接或资源可用性更改时，该方法将被调用。
    *
    * 为Application分配资源选择Worker(Executor),现在有两种策略：
    *   1.尽量打散
    *     将一个Application尽可能的分配到不同的节点，可以通过设置spark.deploy.spreadOut来实现，默认值为true
    *   2.尽量集中
    *     将一个Application金肯呢个分配到尽可能少的节点，CPU密集型而内存占用比较少的Application比较适合这种策略
   */
  private def schedule(): Unit = {
    if (state != RecoveryState.ALIVE) {
      return
    }
    // Drivers take strict precedence over executors 驱动程序对执行器有严格的优先级。
    // 得到活动状态的Worker
    val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(_.state == WorkerState.ALIVE))
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0
    for (driver <- waitingDrivers.toList) {
      // 如果是Standalone部署下的client方式提交的话，则driver的运行时本地提交节点
      // 如果是cluster方式提交的话，则driver的运行节点是有master调度的
      // iterate over a copy of waitingDrivers
      // We assign workers to each waiting driver in a round-robin fashion. For each driver, we
      // start from the last worker that was assigned a driver, and continue onwards until we have
      // explored all alive workers.
      // 迭代等待驱动程序的副本，我们以循环的方式分配给每个等待的驱动程序。对于每个driver，
      // 我们从最后一个被分配driver的worker开始，然后继续向前，直到我们探索所有活着的worker。
      var launched = false
      var numWorkersVisited = 0
      // 这里循环每个driver，比如
      //      A写了一个程序，（需要内存：100M,需要核数：1个）
      //      B写了一个程序，（需要内存：200M,需要核数：1个）
      //      c写了一个程序，（需要内存：300M,需要核数：2个）
      // 都已经注册成功了，但是因为资源不够，都在处于等待状态，
      // 这里要循环每个worker，只要worker处于活动状态，还没有运行任务，就尝试给它运行任务
      while (numWorkersVisited < numWorkersAlive && !launched) {
        val worker = shuffledAliveWorkers(curPos)
        numWorkersVisited += 1
        // 如果worker空闲的内存 > driver描述的内存   worker空闲的核数 > driver描述的核数
        // 假设现在worker空闲的内存是250M，核数是1个，那么符合条件的只要A和B,根据先来先运行，A去这个worker上运行，
        // 那么worker还剩下空闲的内存是150M，核数是0个，B和C都要继续等待
        if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {
          /**
            * 启动Driver程序，吧Driver放到executor中
            */
          launchDriver(worker, driver)
          waitingDrivers -= driver
          launched = true
        }
        curPos = (curPos + 1) % numWorkersAlive
      }
    }

    /**
      * 上面每次都会循环一边，然后执行launchExecutor
      * 启动executor，即为app分配资源，这个在while循环外，所以这里也会运行
      */
    startExecutorsOnWorkers()
  }

  // 启动executor
  /**
    * 计算资源物理分配
    *   计算资源物理分配U是指给Application无力分配Worker的内存以及核数。由于在逻辑分配的时候已经确定了每个Worker分配给Application的核数，并且
    * 这些Worker也都满足Application的内存需要，所以可以放心地进行物理分配了：分配步骤如下：
    *   1.首先使用WorkerInfo,逻辑分配的CPU核数以及内存大小创建ExecutorInfo，然后将此ExecutorInfo添加到ApplicationInfo的executors缓存中，最后
    *     增加已经授权得到的内核数。
    *   2.物理分配通过调用Master的launchExecutor方法来实现，其功能如下：
    *     （1）将ExecutorInfo添加到WorkerInfo的Executors缓存中，并且更新Worker已经使用的CPU核数和内存大小。
    *     （2）向Worker发送LaunchExecutor消息，运行Executor.
    *     （3）向ClientActor发送ExecutorAdded消息，ClientActor收到ExecutorAdded消息后，向Master发送ExecutorStateChange消息。Master收到
    *         ExecutorStateChanged消息后向DriverActor发送ExecutorUpdated消息，用于更新Driver上有关Executor的状态。
    */
  private def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc): Unit = {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    // worker节点添加executor,更新Worker的信息，可用core数和memory数减去本次分配的Executor占用的，因此Worker的信息无需Worker主动汇报
    worker.addExecutor(exec)
    // 向Worker发送LaunchExecutor消息，运行Executor.
    worker.endpoint.send(LaunchExecutor(masterUrl,
      exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory))

    // 向ClientActor发送ExecutorAdded消息
    exec.application.driver.send(
      ExecutorAdded(exec.id, worker.id, worker.hostPort, exec.cores, exec.memory))
  }

  /** 注册worker节点
    * 注册WorkerInfo，其实就是将其添加到workers：HashSet[WorkerInfo]中，并且更新worker id 与worker以及worker address与worker的映射关系。
    *
    * 处理逻辑：
    *   1.标记注册成功
    *   2.调用changeMaster方法更新activeMasterUrl,activeMasterWebUiUrl,master,masterAddress等信息。
    *   3.启动定时调度给自己发送SendHeartbeat消息。
    */
  private def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.
    // 可能会有死的worker节点如果有就删除他
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.endpoint.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        // 老的节点必须死掉，所以我们移除掉它接受新的节点
        removeWorker(oldWorker)
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }

    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker
    if (reverseProxy) {
       webUi.addProxyTargets(worker.id, worker.webUiAddress)
    }
    true
  }

  // 移除worker节点
  private def removeWorker(worker: WorkerInfo) {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    // 先设置worker的状态为死亡状态
    worker.setState(WorkerState.DEAD)
    idToWorker -= worker.id
    addressToWorker -= worker.endpoint.address
    if (reverseProxy) {
      webUi.removeProxyTargets(worker.id)
    }
    for (exec <- worker.executors.values) {
      logInfo("Telling app of lost executor: " + exec.id)
      // 发送Executor更新事件
      exec.application.driver.send(ExecutorUpdated(
        exec.id, ExecutorState.LOST, Some("worker lost"), None, workerLost = true))
      exec.state = ExecutorState.LOST
      // 移除worker上的executor
      exec.application.removeExecutor(exec)
    }
    for (driver <- worker.drivers.values) {
      if (driver.desc.supervise) {
        logInfo(s"Re-launching ${driver.id}")
        relaunchDriver(driver)
      } else {
        logInfo(s"Not re-launching ${driver.id} because it was not supervised")
        removeDriver(driver.id, DriverState.ERROR, None)
      }
    }
    persistenceEngine.removeWorker(worker)
  }



  // 启动driver驱动
  private def relaunchDriver(driver: DriverInfo) {
    driver.worker = None
    driver.state = DriverState.RELAUNCHING
    // 主要是把driver加到waitingDrivers队列中
    waitingDrivers += driver
    schedule()
  }

  //创建App应用程序 包括创建时间 appID ，描述，驱动driver,默认的cores(defaultCores)
  private def createApplication(desc: ApplicationDescription, driver: RpcEndpointRef):
      ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val appId = newApplicationId(date)
    new ApplicationInfo(now, appId, desc, date, driver, defaultCores)
  }

  // 注册应用程序 Application
  private def registerApplication(app: ApplicationInfo): Unit = {
    val appAddress = app.driver.address
    if (addressToApp.contains(appAddress)) {
      logInfo("Attempted to re-register application at same address: " + appAddress)
      return
    }

    // 注册App监控
    applicationMetricsSystem.registerSource(app.appSource)
    apps += app
    // 把应用程序的app id 用一个HashMap来保存
    idToApp(app.id) = app
    endpointToApp(app.driver) = app
    addressToApp(appAddress) = app
    waitingApps += app
    if (reverseProxy) {
      webUi.addProxyTargets(app.id, app.desc.appUiUrl)
    }
  }

  // 应用程序结束，移除结束的应用程序
  private def finishApplication(app: ApplicationInfo) {
    removeApplication(app, ApplicationState.FINISHED)
  }

  // 移除应用程序
  def removeApplication(app: ApplicationInfo, state: ApplicationState.Value) {
    if (apps.contains(app)) {
      logInfo("Removing app " + app.id)
      apps -= app
      idToApp -= app.id
      endpointToApp -= app.driver
      addressToApp -= app.driver.address
      if (reverseProxy) {
        webUi.removeProxyTargets(app.id)
      }
      if (completedApps.size >= RETAINED_APPLICATIONS) {
        val toRemove = math.max(RETAINED_APPLICATIONS / 10, 1)
        completedApps.take(toRemove).foreach { a =>
          applicationMetricsSystem.removeSource(a.appSource)
        }
        completedApps.trimStart(toRemove)
      }
      completedApps += app // Remember it in our history
      waitingApps -= app

      for (exec <- app.executors.values) {
        killExecutor(exec)
      }
      app.markFinished(state)
      if (state != ApplicationState.FINISHED) {
        app.driver.send(ApplicationRemoved(state.toString))
      }
      persistenceEngine.removeApplication(app)
      schedule()

      // Tell all workers that the application has finished, so they can clean up any app state.
      workers.foreach { w =>
        w.endpoint.send(ApplicationFinished(app.id))
      }
    }
  }

  /**
   * Handle a request to set the target number of executors for this application.
   *
    * 处理请求以设置此应用程序的执行器目标数量。
    *
   * If the executor limit is adjusted upwards, new executors will be launched provided
   * that there are workers with sufficient resources. If it is adjusted downwards, however,
   * we do not kill existing executors until we explicitly receive a kill request.
    * 如果执行机构的上限被调整，如果有足够的资源，新的执行者将被启动。但是，如果它向下调整，我们不会杀死现有的执行器，除非我们明确地接收了一个杀戮请求。
   *
   * @return whether the application has previously registered with this Master. 应用程序是否已在此主注册。
   */
  private def handleRequestExecutors(appId: String, requestedTotal: Int): Boolean = {
    idToApp.get(appId) match {
      case Some(appInfo) =>
        logInfo(s"Application $appId requested to set total executors to $requestedTotal.")
        appInfo.executorLimit = requestedTotal
        schedule()
        true
      case None =>
        logWarning(s"Unknown application $appId requested $requestedTotal total executors.")
        false
    }
  }

  /**
   * Handle a kill request from the given application.
    * 处理杀请求从给定的应用程序。
   * This method assumes the executor limit has already been adjusted downwards through
   * a separate [[RequestExecutors]] message, such that we do not launch new executors
   * immediately after the old ones are removed.
    * 此方法假定遗嘱执行人的限制已经向下调整通过一个单独的[ ] [ ] requestexecutors留言，这样我们不需要立即不推出新的executors在老的executors=除去后。
   *
   * @return whether the application has previously registered with this Master. 应用程序是否已在此主节点注册。
   */
  private def handleKillExecutors(appId: String, executorIds: Seq[Int]): Boolean = {
    idToApp.get(appId) match {
      case Some(appInfo) =>
        logInfo(s"Application $appId requests to kill executors: " + executorIds.mkString(", "))
        val (known, unknown) = executorIds.partition(appInfo.executors.contains)
        known.foreach { executorId =>
          val desc = appInfo.executors(executorId)
          appInfo.removeExecutor(desc)
          killExecutor(desc)
        }
        if (unknown.nonEmpty) {
          logWarning(s"Application $appId attempted to kill non-existent executors: "
            + unknown.mkString(", "))
        }
        schedule()
        true
      case None =>
        logWarning(s"Unregistered application $appId requested us to kill executors!")
        false
    }
  }

  /**
   * Cast the given executor IDs to integers and filter out the ones that fail.
    * 将给定的执行器ID转换为整数，并过滤掉失败的executor ID。
   *
   * All executors IDs should be integers since we launched these executors. However,
   * the kill interface on the driver side accepts arbitrary strings, so we need to
   * handle non-integer executor IDs just to be safe.
    * 自从我们启动这些执行器以来，所有的执行器ID都是整数。但是，驱动端的杀接口接受任意字符串，因此我们需要处理非整数执行器ID才是安全的。
   */
  private def formatExecutorIds(executorIds: Seq[String]): Seq[Int] = {
    executorIds.flatMap { executorId =>
      try {
        Some(executorId.toInt)
      } catch {
        case e: NumberFormatException =>
          logError(s"Encountered executor with a non-integer ID: $executorId. Ignoring")
          None
      }
    }
  }

  /**
   * Ask the worker on which the specified executor is launched to kill the executor.
    * 询问worker指定的executor去启动杀死另外一个executor
   */
  private def killExecutor(exec: ExecutorDesc): Unit = {
    exec.worker.removeExecutor(exec)
    exec.worker.endpoint.send(KillExecutor(masterUrl, exec.application.id, exec.id))
    exec.state = ExecutorState.KILLED
  }

  /** Generate a new app ID given an app's submission date
    *
    * 生成一个APP ID 根据APP提交的时间
    * */
  private def newApplicationId(submitDate: Date): String = {
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }

  /** Check for, and remove, any timed-out workers
    * 检查 移除 超时没有发送心跳的 worker节点
    *
    * timeOutDeadWorkers方法的处理步骤如下：
    *   1.过滤掉所有超时的Worker，即使用当前时间减去Worker最大超时时间仍然大于lastHeartbeat的Worker。
    *   2.如果WorkerInfo的状态是DEAD，则等待足够长的时间后将它从workers列表中移除。足够长的时间的计算公司为：属性spark.dead.worker.persistence
    *     的值成衣Worker最大超时时间。spark.dead.worker.persistence默认等于15.如果WorkerInfo的状态不是DEAD，则调用removeWorker方法将WorkerInfo
    *     的状态设置为DEAD，从idToWorker缓存中移除Worker的id,从addressToWorker的缓存中移除WorkerInfo，最后向此WorkerInfo的所有Executor所服务的Driver
    *     application发送ExecutorUpdated消息，更新Executor的状态为LOST，最后使用removeDriver重新调度之前调度给此Worker的Driver.
    *
    *
    * */
  private def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    // 将workers复制到一个数组中，这样我们就不会在迭代过程中修改hashset。
    val currentTime = System.currentTimeMillis()
    // 从workers过滤掉那些，最后心跳时间 < 当前时间 - 超时时间 就是超时的worker
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT_MS).toArray

    // 循环删除这些worker
    for (worker <- toRemove) {
      // 如果这些worker没有处于死亡状态，只要超时了，就删除
      if (worker.state != WorkerState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, WORKER_TIMEOUT_MS / 1000))
        /** 真正去删除Worker */
        removeWorker(worker)
      } else {
        // 否则，就是已经死掉了，直接从workers移除
        /**
          * 如果WorkerInfo的状态是DEAD，则等待足够长的时间后将它从workers列表中移除。足够长的时间的计算公司为：属性spark.dead.worker.persistence
          *     的值成衣Worker最大超时时间。spark.dead.worker.persistence默认等于15.如果WorkerInfo的状态不是DEAD，则调用removeWorker方法将WorkerInfo
          *     的状态设置为DEAD，从idToWorker缓存中移除Worker的id,从addressToWorker的缓存中移除WorkerInfo，最后向此WorkerInfo的所有Executor所服务的Driver
          *     application发送ExecutorUpdated消息，更新Executor的状态为LOST，最后使用removeDriver重新调度之前调度给此Worker的Driver.
          */
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT_MS)) {
          workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }

  private def newDriverId(submitDate: Date): String = {
    val appId = "driver-%s-%04d".format(createDateFormat.format(submitDate), nextDriverNumber)
    nextDriverNumber += 1
    appId
  }

  private def createDriver(desc: DriverDescription): DriverInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new DriverInfo(now, newDriverId(date), desc, date)
  }

  /**
    * 启动Driver程序
    * */
  private def launchDriver(worker: WorkerInfo, driver: DriverInfo) {
    logInfo("Launching driver " + driver.id + " on worker " + worker.id)
    // 在Worker节点运行Driver
    worker.addDriver(driver)
    driver.worker = Some(worker)
    // 发送LaunchDriver消息，由Worker.reveive()方法处理
    worker.endpoint.send(LaunchDriver(driver.id, driver.desc))
    driver.state = DriverState.RUNNING
  }

  /**
    * 删除Driver的实现
    */
  private def removeDriver(
      driverId: String,
      finalState: DriverState,
      exception: Option[Exception]) {
    //首先，查看是否存在该driver，如果不存在日志警告；如果存在该driver，将drivers中的该driver移除
    drivers.find(d => d.id == driverId) match {
      case Some(driver) =>
        logInfo(s"Removing driver: $driverId")
        //从set中移除该driver
        drivers -= driver
        if (completedDrivers.size >= RETAINED_DRIVERS) {
          val toRemove = math.max(RETAINED_DRIVERS / 10, 1)
          completedDrivers.trimStart(toRemove)
        }
        //将移除的driver加入到完成的driver记录容器中
        completedDrivers += driver
        //移除driver的持久化信息
        persistenceEngine.removeDriver(driver)
        //更改driver的状态
        driver.state = finalState
        driver.exception = exception
        //移除该driver对应的worker
        driver.worker.foreach(w => w.removeDriver(driver))
        schedule()
      case None =>
        logWarning(s"Asked to remove unknown driver: $driverId")
    }
  }
}


// ####################################### 伴生对象object Master ############################################################################################
/**
  * scla语法允许在object中定义main函数作为应用程序的启动入口，可以认为与java中的main方法一样，在Master.scala文件中定义了object Master。
  *   1.创建SparkConf
  *   2.master参数解析
  *   3.创建和启动ActorSystem，并且向ActorSystemMaster.
  *
  *   该main()方法主要在启动脚本 start-master.sh中调用
  *   http://blog.csdn.net/qq_21383435/article/details/78976148
  *   最终命令：/opt/java1.8/bin/java -Xmx128m -cp /opt/spark2.2/...../
  *   org.apache.spark.deploy.master.Master/org.apache.spark.launcher.Main  。。。
  *
  */
private[deploy] object Master extends Logging {
  val SYSTEM_NAME = "sparkMaster"
  val ENDPOINT_NAME = "Master"

  def main(argStrings: Array[String]) {
    //实用的功能主要是设置一些平常的诊断状态，，应该在main方法之前调用， 在类unix系统上注册一个信号处理程序来记录信号。
    Utils.initDaemon(log)
    val conf = new SparkConf
    // MasterArguments用于解析系统环境变量和启动Master时指定的命令行参数
    val args = new MasterArguments(argStrings, conf)
    val (rpcEnv, _, _) = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, conf)
    rpcEnv.awaitTermination()
  }

  /**
   * Start the Master and return a three tuple of:
   *   (1) The Master RpcEnv
   *   (2) The web UI bound port
   *   (3) The REST server bound port, if any
   */
  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
    val securityMgr = new SecurityManager(conf)
    // 创建rpcEnv主要是接收消息，然后交给backend处理
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
    // 调用这个方法之前，会调用,master的onStart()方法，调用的是NettyRpcEnv的setupEndpoint方法，里面貌似重复了？
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))

    // 发送一条消息到相应的[[RpcEndpoint.receiveAndReply]] 。在默认超时时间内得到它的结果，如果失败，则抛出异常。
    // Endpoint启动时，会默认启动TransportServer，且启动结束后会进行一次同步测试rpc可用性（askSync-BoundPortsRequest）
    val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)
    (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
  }
}
