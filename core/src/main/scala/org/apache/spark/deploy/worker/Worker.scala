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

package org.apache.spark.deploy.worker

import java.io.File
import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{Date, Locale, UUID}
import java.util.concurrent._
import java.util.concurrent.{Future => JFuture, ScheduledFuture => JScheduledFuture}

import scala.collection.mutable.{HashMap, HashSet, LinkedHashMap}
import scala.concurrent.ExecutionContext
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{Command, ExecutorDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.deploy.master.{DriverState, Master}
import org.apache.spark.deploy.worker.ui.WorkerWebUI
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc._
import org.apache.spark.util.{ThreadUtils, Utils}

private[deploy] class Worker(
    override val rpcEnv: RpcEnv,
    webUiPort: Int,
    cores: Int,
    memory: Int,
    masterRpcAddresses: Array[RpcAddress],
    endpointName: String,
    workDirPath: String = null,
    val conf: SparkConf,
    val securityMgr: SecurityManager)
  extends ThreadSafeRpcEndpoint with Logging {

  private val host = rpcEnv.address.host
  private val port = rpcEnv.address.port

  Utils.checkHost(host, "Expected hostname")
  assert (port > 0)

  // A scheduled executor used to send messages at the specified time.
  // 一个调度器发送消息在指定的时间
  private val forwordMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")

  // A separated thread to clean up the workDir and the directories of finished applications.
  // Used to provide the implicit parameter of `Future` methods.
  // 一个分离的线程清理workDir和目录应用程序目录。
  private val cleanupThreadExecutor = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonSingleThreadExecutor("worker-cleanup-thread"))

  // For worker and executor IDs
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
  // Send a heartbeat every (heartbeat timeout) / 4 milliseconds
  // 发送心跳
  private val HEARTBEAT_MILLIS = conf.getLong("spark.worker.timeout", 60) * 1000 / 4

  // Model retries to connect to the master, after Hadoop's model.
  // The first six attempts to reconnect are in shorter intervals (between 5 and 15 seconds)
  // Afterwards, the next 10 attempts are between 30 and 90 seconds.
  // A bit of randomness is introduced so that not all of the workers attempt to reconnect at
  // the same time.
  // model尝试连接主接地那，在hadoop的模型之后
  private val INITIAL_REGISTRATION_RETRIES = 6
  private val TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 10
  private val FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND = 0.500
  private val REGISTRATION_RETRY_FUZZ_MULTIPLIER = {
    val randomNumberGenerator = new Random(UUID.randomUUID.getMostSignificantBits)
    randomNumberGenerator.nextDouble + FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND
  }
  private val INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS = (math.round(10 *
    REGISTRATION_RETRY_FUZZ_MULTIPLIER))
  private val PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS = (math.round(60
    * REGISTRATION_RETRY_FUZZ_MULTIPLIER))

  private val CLEANUP_ENABLED = conf.getBoolean("spark.worker.cleanup.enabled", false)
  // How often worker will clean up old app folders
  private val CLEANUP_INTERVAL_MILLIS =
    conf.getLong("spark.worker.cleanup.interval", 60 * 30) * 1000
  // TTL for app folders/data;  after TTL expires it will be cleaned up
  private val APP_DATA_RETENTION_SECONDS =
    conf.getLong("spark.worker.cleanup.appDataTtl", 7 * 24 * 3600)

  private val testing: Boolean = sys.props.contains("spark.testing")
  private var master: Option[RpcEndpointRef] = None

  /**
   * Whether to use the master address in `masterRpcAddresses` if possible. If it's disabled, Worker
   * will just use the address received from Master.
    * 是否使用主地址在` masterrpcaddresses `如果可能的话。如果它是禁用的，工人将只使用从主人收到的地址。
   */
  private val preferConfiguredMasterAddress =
    conf.getBoolean("spark.worker.preferConfiguredMasterAddress", false)
  /**
   * The master address to connect in case of failure. When the connection is broken, worker will
   * use this address to connect. This is usually just one of `masterRpcAddresses`. However, when
   * a master is restarted or takes over leadership, it will be an address sent from master, which
   * may not be in `masterRpcAddresses`.
   */
  private var masterAddressToConnect: Option[RpcAddress] = None
  private var activeMasterUrl: String = ""
  private[worker] var activeMasterWebUiUrl : String = ""
  private var workerWebUiUrl: String = ""
  private val workerUri = RpcEndpointAddress(rpcEnv.address, endpointName).toString
  private var registered = false
  private var connected = false
  private val workerId = generateWorkerId()
  private val sparkHome =
    if (testing) {
      assert(sys.props.contains("spark.test.home"), "spark.test.home is not set!")
      new File(sys.props("spark.test.home"))
    } else {
      new File(sys.env.get("SPARK_HOME").getOrElse("."))
    }

  var workDir: File = null
  val finishedExecutors = new LinkedHashMap[String, ExecutorRunner]
  val drivers = new HashMap[String, DriverRunner]
  val executors = new HashMap[String, ExecutorRunner]
  val finishedDrivers = new LinkedHashMap[String, DriverRunner]
  val appDirectories = new HashMap[String, Seq[String]]
  val finishedApps = new HashSet[String]

  val retainedExecutors = conf.getInt("spark.worker.ui.retainedExecutors",
    WorkerWebUI.DEFAULT_RETAINED_EXECUTORS)
  val retainedDrivers = conf.getInt("spark.worker.ui.retainedDrivers",
    WorkerWebUI.DEFAULT_RETAINED_DRIVERS)

  // The shuffle service is not actually started unless configured.
  // 除非配置好，否则实际上不会启动shuffle服务。
  private val shuffleService = new ExternalShuffleService(conf, securityMgr)

  private val publicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }
  private var webUi: WorkerWebUI = null

  private var connectionAttemptCount = 0

  private val metricsSystem = MetricsSystem.createMetricsSystem("worker", conf, securityMgr)
  private val workerSource = new WorkerSource(this)

  private var registerMasterFutures: Array[JFuture[_]] = null
  private var registrationRetryTimer: Option[JScheduledFuture[_]] = None

  // A thread pool for registering with masters. Because registering with a master is a blocking
  // action, this thread pool must be able to create "masterRpcAddresses.size" threads at the same
  // time so that we can register with all masters.
  private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "worker-register-master-threadpool",
    masterRpcAddresses.length // Make sure we can register with all masters at the same time
  )

  var coresUsed = 0
  var memoryUsed = 0

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed


  /**
    * work文件夹里面的内容格式如下，这里是创建工作目录在$SPARK_HOME/work
    * [root@biluos work]# vim app-20170801093308-0003
    " ============================================================================
    " Netrw Directory Listing                                        (netrw v134)
    "   /opt/moudles/spark-2.2.0-bin-hadoop2.7/work/app-20170801093308-0003
    "   Sorted by      name
    "   Sort sequence: [\/]$,\.h$,\.c$,\.cpp$,*,\.o$,\.obj$,\.info$,\.swp$,\.bak$,\~$
    "   Quick Help: <F1>:help  -:go up dir  D:delete  R:rename  s:sort-by  x:exec
    " ============================================================================
    */
  private def createWorkDir() {
    workDir = Option(workDirPath).map(new File(_)).getOrElse(new File(sparkHome, "work"))
    try {
      // This sporadically fails - not sure why ... !workDir.exists() && !workDir.mkdirs()
      // So attempting to create and then check if directory was created or not.
      // 这偶尔会失败——不知道为什么…workDir exists() &！！mkdirs() workDir。
      // 因此，尝试创建并检查目录是否创建了。
      workDir.mkdirs()
      if ( !workDir.exists() || !workDir.isDirectory) {
        logError("Failed to create work directory " + workDir)
        System.exit(1)
      }
      assert (workDir.isDirectory)
    } catch {
      case e: Exception =>
        logError("Failed to create work directory " + workDir, e)
        System.exit(1)
    }
  }

  /**
    * work节点的启动函数
    */
  override def onStart() {
    assert(!registered)
    /**
      * 17/09/11 11:17:22 INFO Worker: Starting Spark worker 192.168.10.173:7080 with 1 cores, 512.0 MB RAM
        17/09/11 11:17:22 INFO Worker: Running Spark version 2.2.0
        17/09/11 11:17:22 INFO Worker: Spark home: /opt/moudles/spark-2.2.0-bin-hadoop2.7
      */
    logInfo("Starting Spark worker %s:%d with %d cores, %s RAM".format(
      host, port, cores, Utils.megabytesToString(memory)))
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    logInfo("Spark home: " + sparkHome)
    /**创建$SPARK_HOME/work目录 用来工作的
      *
      * local模式：
      *   F:\002-spark\00-IDEAWork\SparkOnHbaseScala\work\app-20171226143814-0000
      *   F:\002-spark\00-IDEAWork\SparkOnHbaseScala\work\app-20171226145705-0000
      * */
    createWorkDir()

    // 启动额外的shuffle服务，除非用户配置了 否则不会启动的
    shuffleService.startIfEnabled()

    /** work节点的网页监控界面http://192.168.10.173:8083/ */
    webUi = new WorkerWebUI(this, workDir, webUiPort)
    webUi.bind()

    workerWebUiUrl = s"http://$publicAddress:${webUi.boundPort}"

    // 开始注册Master,点开看逻辑
    registerWithMaster()

    /** work节点申请资源 */
    metricsSystem.registerSource(workerSource)

    /** 启动work 节点 的监控*/
    metricsSystem.start()
    // Attach the worker metrics servlet handler to the web ui after the metrics system is started.
    //在metrics system启动之后启动web ui的监控界面
    metricsSystem.getServletHandlers.foreach(webUi.attachHandler)
  }

  /**
   * Change to use the new master.
    * 改变使用心得master
   *
   * @param masterRef the new master ref
   * @param uiUrl the new master Web UI address
   * @param masterAddress the new master address which the worker should use to connect in case of
   *                      failure
   */
  private def changeMaster(masterRef: RpcEndpointRef, uiUrl: String, masterAddress: RpcAddress) {
    // activeMasterUrl it's a valid Spark url since we receive it from master.
    activeMasterUrl = masterRef.address.toSparkURL
    activeMasterWebUiUrl = uiUrl
    masterAddressToConnect = Some(masterAddress)
    master = Some(masterRef)
    connected = true
    if (conf.getBoolean("spark.ui.reverseProxy", false)) {
      logInfo(s"WorkerWebUI is available at $activeMasterWebUiUrl/proxy/$workerId")
    }
    // Cancel any outstanding re-registration attempts because we found a new master
    cancelLastRegistrationRetry()
  }

  private def tryRegisterAllMasters(): Array[JFuture[_]] = {
    masterRpcAddresses.map { masterAddress =>
      registerMasterThreadPool.submit(new Runnable {
        override def run(): Unit = {
          try {
            // INFO Worker: Connecting to master 192.168.2.89:60115...
            logInfo("Connecting to master " + masterAddress + "...")
            val masterEndpoint = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
            // 向所有的Master发送RegisterWorker消息 ===》master.receive: PartialFunction[Any, Unit]方法去处理
            sendRegisterMessageToMaster(masterEndpoint)
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
          }
        }
      })
    }
  }

  /**
   * Re-register with the master because a network failure or a master failure has occurred.
   * If the re-registration attempt threshold is exceeded, the worker exits with error.
   * Note that for thread-safety this should only be called from the rpcEndpoint.
    **由于发生网络故障或主节点master故障，重新注册主机。
    *如果超过了重新注册尝试阈值，则worker错误地退出。
    *注意线程安全，这应该只是从rpcendpoint。
   */
  private def reregisterWithMaster(): Unit = {
    Utils.tryOrExit {
      connectionAttemptCount += 1
      if (registered) {
        cancelLastRegistrationRetry()
      } else if (connectionAttemptCount <= TOTAL_REGISTRATION_RETRIES) {
        logInfo(s"Retrying connection to master (attempt # $connectionAttemptCount)")
        /**
         * Re-register with the active master this worker has been communicating with. If there
         * is none, then it means this worker is still bootstrapping and hasn't established a
         * connection with a master yet, in which case we should re-register with all masters.
         *
          * 重新注册与worker一直在通信的活动主节点。如果是没有，那么这意味着这个worker仍然在引导（bootstrapping），或者现在还没有建立一个
          *与master的连接，在这种情况下，我们应该重新注册所有masters（主节点）。
          *
         * It is important to re-register only with the active master during failures. Otherwise,
         * if the worker unconditionally attempts to re-register with all masters, the following
         * race condition may arise and cause a "duplicate worker" error detailed in SPARK-4592:
          *
          * 在故障期间重新注册与活动的主节点连接的worker是很重要的。否则，如果worker无条件地尝试重新注册所有的主人，如下
          *比赛情况可能会出现，导致“重复工作”的错误详细spark-4592：
         *
         *   (1) Master A fails and Worker attempts to reconnect to all masters
         *   (2) Master B takes over and notifies Worker
         *   (3) Worker responds by registering with Master B
         *   (4) Meanwhile, Worker's previous reconnection attempt reaches Master B,
         *       causing the same Worker to register with Master B twice
         *
          * （1）Master A 失败，Worker试图重新连接所有masters。
          *（2）Master B 接管并通知Worker
          *（3）Worker响应通过注册Master B
          *（4）同时， Worker's 以前的重接尝试到Master B，
          *     导致同样的Worker与Master B 两次注册
          *
         * Instead, if we only register with the known active master, we can assume that the
         * old master must have died because another master has taken over. Note that this is
         * still not safe if the old master recovers within this interval, but this is a much
         * less likely scenario.
          *
          * 相反，如果我们只注册已知的活动master，我们可以假设
          * 老master一定死了，因为另一个master接手了。注意，这是仍然不安全的，如果老的master在这个期间尝试恢复
          * 但是这是不太可能的情况(有可能但是几率很小)。
         */
        master match {
          case Some(masterRef) =>
            // registered == false && master != None means we lost the connection to master, so
            // masterRef cannot be used and we need to recreate it again. Note: we must not set
            // master to None due to the above comments.
            // registered == false && master != None这也意味和我们丢失了和主节点的连接，所以masterRef是不能使用了，
            // 我们需要重新创建它，注意：我们必须重新设置master=none因为上面的原因
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            val masterAddress =
              if (preferConfiguredMasterAddress) masterAddressToConnect.get else masterRef.address
            registerMasterFutures = Array(registerMasterThreadPool.submit(new Runnable {
              override def run(): Unit = {
                try {
                  logInfo("Connecting to master " + masterAddress + "...")
                  val masterEndpoint = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
                  sendRegisterMessageToMaster(masterEndpoint)
                } catch {
                  case ie: InterruptedException => // Cancelled
                  case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
                }
              }
            }))
          case None =>
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            // We are retrying the initial registration
            // 尝试初始注册master
            registerMasterFutures = tryRegisterAllMasters()
        }
        // We have exceeded the initial registration retry threshold
        // All retries from now on should use a higher interval
        if (connectionAttemptCount == INITIAL_REGISTRATION_RETRIES) {
          registrationRetryTimer.foreach(_.cancel(true))
          registrationRetryTimer = Some(
            forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
              override def run(): Unit = Utils.tryLogNonFatalError {
                self.send(ReregisterWithMaster)
              }
            }, PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              TimeUnit.SECONDS))
        }
      } else {
        logError("All masters are unresponsive! Giving up.")
        System.exit(1)
      }
    }
  }

  /**
   * Cancel last registeration retry, or do nothing if no retry
    * 取消上次重试的注册，或者如果没有重试，什么也不做
   */
  private def cancelLastRegistrationRetry(): Unit = {
    if (registerMasterFutures != null) {
      registerMasterFutures.foreach(_.cancel(true))
      registerMasterFutures = null
    }
    registrationRetryTimer.foreach(_.cancel(true))
    registrationRetryTimer = None
  }

  /**
    * 开始注册master
    * Worker的registerWithMaster方法用于将Worker注册到Master.其中调用了tryRegisterAllMasters方法向所有Master发送RegisterWorker消息。
    */
  private def registerWithMaster() {
    // onDisconnected may be triggered multiple times, so don't attempt registration
    // ondisconnected可能会多次触发，所以不要尝试注册
    // if there are outstanding registration attempts scheduled.
    // 如果有未完成的注册尝试。
    registrationRetryTimer match {
      case None =>
        registered = false
        // tryRegisterAllMasters方法向所有Master发送RegisterWorker消息。
        registerMasterFutures = tryRegisterAllMasters()
        connectionAttemptCount = 0
        // 这是一个定时任务 发送ReregisterWithMaster消息
        registrationRetryTimer = Some(forwordMessageScheduler.scheduleAtFixedRate(
          new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              Option(self).foreach(_.send(ReregisterWithMaster))
            }
          },
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          TimeUnit.SECONDS))
      case Some(_) =>
        logInfo("Not spawning another attempt to register with the master, since there is an" +
          " attempt scheduled already.")
    }
  }

  private def sendRegisterMessageToMaster(masterEndpoint: RpcEndpointRef): Unit = {
    masterEndpoint.send(RegisterWorker(
      workerId,
      host,
      port,
      self,
      cores,
      memory,
      workerWebUiUrl,
      masterEndpoint.address))
  }

  /**
    * 根据字面意思，处理注册响应
    * @param msg
    */
  private def handleRegisterResponse(msg: RegisterWorkerResponse): Unit = synchronized {
    msg match {
        // 匹配注册worker成功
      case RegisteredWorker(masterRef, masterWebUiUrl, masterAddress) =>
        if (preferConfiguredMasterAddress) {
          logInfo("Successfully registered with master " + masterAddress.toSparkURL)
        } else {
          logInfo("Successfully registered with master " + masterRef.address.toSparkURL)
        }
        registered = true     // 标记注册成功
        /** 调用changeMaster方法，更新activeMasterUrl,activeMasterWebUiUrl,master,masterAddress等信息*/
        changeMaster(masterRef, masterWebUiUrl, masterAddress)

        /** 开启一定定时任务给自己发送SendHeartbeat消息 ，这个最终是会向Master转发Heartbeat消息。*/
        forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(SendHeartbeat)
          }
        }, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)

        // worker的清除功能，默认没开启
        if (CLEANUP_ENABLED) {
          logInfo(
            s"Worker cleanup enabled; old application directories will be deleted in: $workDir")
          forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              self.send(WorkDirCleanup)
            }
          }, CLEANUP_INTERVAL_MILLIS, CLEANUP_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
        }

        val execs = executors.values.map { e =>
          new ExecutorDescription(e.appId, e.execId, e.cores, e.state)
        }
        masterRef.send(WorkerLatestState(workerId, execs.toList, drivers.keys.toSeq))

      case RegisterWorkerFailed(message) =>
        if (!registered) {
          logError("Worker registration failed: " + message)
          System.exit(1)
        }

      case MasterInStandby =>
        // Ignore. Master not yet ready.
    }
  }

  override def receive: PartialFunction[Any, Unit] = synchronized {
    case msg: RegisterWorkerResponse =>
      handleRegisterResponse(msg)

    // Worker收到SendHeartbeat消息后，会向Master转发Heartbeat消息。
    case SendHeartbeat =>
      if (connected) { sendToMaster(Heartbeat(workerId, self)) }

    case WorkDirCleanup =>
      // Spin up a separate thread (in a future) to do the dir cleanup; don't tie up worker
      // rpcEndpoint.
      // Copy ids so that it can be used in the cleanup thread.
      val appIds = executors.values.map(_.appId).toSet
      val cleanupFuture = concurrent.Future {
        val appDirs = workDir.listFiles()
        if (appDirs == null) {
          throw new IOException("ERROR: Failed to list files in " + appDirs)
        }
        appDirs.filter { dir =>
          // the directory is used by an application - check that the application is not running
          // when cleaning up
          val appIdFromDir = dir.getName
          val isAppStillRunning = appIds.contains(appIdFromDir)
          dir.isDirectory && !isAppStillRunning &&
          !Utils.doesDirectoryContainAnyNewFiles(dir, APP_DATA_RETENTION_SECONDS)
        }.foreach { dir =>
          logInfo(s"Removing directory: ${dir.getPath}")
          Utils.deleteRecursively(dir)
        }
      }(cleanupThreadExecutor)

      cleanupFuture.onFailure {
        case e: Throwable =>
          logError("App dir cleanup failed: " + e.getMessage, e)
      }(cleanupThreadExecutor)

    case MasterChanged(masterRef, masterWebUiUrl) =>
      logInfo("Master has changed, new master is at " + masterRef.address.toSparkURL)
      changeMaster(masterRef, masterWebUiUrl, masterRef.address)

      val execs = executors.values.
        map(e => new ExecutorDescription(e.appId, e.execId, e.cores, e.state))
      masterRef.send(WorkerSchedulerStateResponse(workerId, execs.toList, drivers.keys.toSeq))

    case ReconnectWorker(masterUrl) =>
      logInfo(s"Master with url $masterUrl requested this worker to reconnect.")
      registerWithMaster()


    /**
      * 启动Executor
      */
    case LaunchExecutor(masterUrl, appId, execId, appDesc, cores_, memory_) =>
      // 如果传入的masterUrl和正在活动的主节点url不一样，那么我们打印一个日志
      // 比如HA的时候，多个主节点
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor.")
      } else {
        try {
          logInfo("Asked to launch executor %s/%d for %s".format(appId, execId, appDesc.name))

          // Create the executor's working directory  创建executor的工作目录
          val executorDir = new File(workDir, appId + "/" + execId)
          if (!executorDir.mkdirs()) {
            throw new IOException("Failed to create directory " + executorDir)
          }

          // Create local dirs for the executor. These are passed to the executor via the
          // SPARK_EXECUTOR_DIRS environment variable, and deleted by the Worker when the
          // application finishes.
          // 为执行器创建本地的dirs。这些通过SPARK_EXECUTOR_DIRS环境变量传递给executor，
          // 并在应用程序完成时被Worker删除。
          val appLocalDirs = appDirectories.getOrElse(appId, {
            // 得到或者创建根目录
            val localRootDirs = Utils.getOrCreateLocalRootDirs(conf)
            val dirs = localRootDirs.flatMap { dir =>
              try {
                // 在给定的父目录中创建一个目录。该目录保证是新创建的，并没有标记为自动删除。
                val appDir = Utils.createDirectory(dir, namePrefix = "executor")
                Utils.chmod700(appDir)
                Some(appDir.getAbsolutePath())
              } catch {
                case e: IOException =>
                  logWarning(s"${e.getMessage}. Ignoring this directory.")
                  None
              }
            }.toSeq
            if (dirs.isEmpty) {
              throw new IOException("No subfolder can be created in " +
                s"${localRootDirs.mkString(",")}.")
            }
            dirs
          })

          // 因为一个worker下可能有很多executor，因此要区分一下
          appDirectories(appId) = appLocalDirs

          val manager = new ExecutorRunner(
            appId,
            execId,
            appDesc.copy(command = Worker.maybeUpdateSSLSettings(appDesc.command, conf)),
            cores_,
            memory_,
            self,
            workerId,
            host,
            webUi.boundPort,
            publicAddress,
            sparkHome,
            executorDir,
            workerUri,
            conf,
            appLocalDirs, ExecutorState.RUNNING)
          executors(appId + "/" + execId) = manager

          /**
            * 启动ExecutorRunner
            */
          manager.start()
          coresUsed += cores_
          memoryUsed += memory_

          /**
            * 向主节点发送ExecutorStateChanged事件  manager.state =  running
            */
          sendToMaster(ExecutorStateChanged(appId, execId, manager.state, None, None))
        } catch {
          case e: Exception =>
            logError(s"Failed to launch executor $appId/$execId for ${appDesc.name}.", e)
            if (executors.contains(appId + "/" + execId)) {
              executors(appId + "/" + execId).kill()
              executors -= appId + "/" + execId
            }
            sendToMaster(ExecutorStateChanged(appId, execId, ExecutorState.FAILED,
              Some(e.toString), None))
        }
      }

    case executorStateChanged @ ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
      handleExecutorStateChanged(executorStateChanged)

    case KillExecutor(masterUrl, appId, execId) =>
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to kill executor " + execId)
      } else {
        val fullId = appId + "/" + execId
        executors.get(fullId) match {
          case Some(executor) =>
            logInfo("Asked to kill executor " + fullId)
            executor.kill()
          case None =>
            logInfo("Asked to kill unknown executor " + fullId)
        }
      }

      // 启动Driver程序
    case LaunchDriver(driverId, driverDesc) =>
      logInfo(s"Asked to launch driver $driverId")

      // 创建一个DriverRunner：管理一个驱动程序Driver的执行execution
      val driver = new DriverRunner(
        conf,
        driverId,
        workDir,
        sparkHome,
        driverDesc.copy(command = Worker.maybeUpdateSSLSettings(driverDesc.command, conf)),
        self,
        workerUri,
        securityMgr)
      drivers(driverId) = driver

      /** 这里在启动Driver之前就调用了worker的onStart()方法，创建了工作目录  */
      // 启动Driver,
      driver.start()

      coresUsed += driverDesc.cores
      memoryUsed += driverDesc.mem

    case KillDriver(driverId) =>
      logInfo(s"Asked to kill driver $driverId")
      drivers.get(driverId) match {
        case Some(runner) =>
          runner.kill()
        case None =>
          logError(s"Asked to kill unknown driver $driverId")
      }

     // DriverRunner发送了driverStateChanged消息后， driver被启动。下面分析前面提到到向worker发送状态改变消息机制，
    // 也就是当driver的状态改变的时候通知worker。下面分析worker.send(DriverStateChanged(driverId, state, finalException))方法，
    // worker中处理的方法会调用handleDriverStateChanged
    case driverStateChanged @ DriverStateChanged(driverId, state, exception) =>
      handleDriverStateChanged(driverStateChanged)

    case ReregisterWithMaster =>
      reregisterWithMaster()

    case ApplicationFinished(id) =>
      finishedApps += id
      maybeCleanupApplication(id)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestWorkerState =>
      context.reply(WorkerStateResponse(host, port, workerId, executors.values.toList,
        finishedExecutors.values.toList, drivers.values.toList,
        finishedDrivers.values.toList, activeMasterUrl, cores, memory,
        coresUsed, memoryUsed, activeMasterWebUiUrl))
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (master.exists(_.address == remoteAddress) ||
      masterAddressToConnect.exists(_ == remoteAddress)) {
      logInfo(s"$remoteAddress Disassociated !")
      masterDisconnected()
    }
  }

  private def masterDisconnected() {
    logError("Connection to master failed! Waiting for master to reconnect...")
    connected = false
    registerWithMaster()
  }

  private def maybeCleanupApplication(id: String): Unit = {
    val shouldCleanup = finishedApps.contains(id) && !executors.values.exists(_.appId == id)
    if (shouldCleanup) {
      finishedApps -= id
      appDirectories.remove(id).foreach { dirList =>
        concurrent.Future {
          logInfo(s"Cleaning up local directories for application $id")
          dirList.foreach { dir =>
            Utils.deleteRecursively(new File(dir))
          }
        }(cleanupThreadExecutor).onFailure {
          case e: Throwable =>
            logError(s"Clean up app dir $dirList failed: ${e.getMessage}", e)
        }(cleanupThreadExecutor)
      }
      shuffleService.applicationRemoved(id)
    }
  }

  /**
   * Send a message to the current master. If we have not yet registered successfully with any
   * master, the message will be dropped.
    * 发送消息给当前的master，如果我们还没有向任何master注册成功，这个消息将会被丢弃
   */
  private def sendToMaster(message: Any): Unit = {
    master match {
      case Some(masterRef) => masterRef.send(message)
      case None =>
        logWarning(
          s"Dropping $message because the connection to master has not yet been established")
    }
  }

  private def generateWorkerId(): String = {
    "worker-%s-%s-%d".format(createDateFormat.format(new Date), host, port)
  }

  override def onStop() {
    cleanupThreadExecutor.shutdownNow()
    metricsSystem.report()
    cancelLastRegistrationRetry()
    forwordMessageScheduler.shutdownNow()
    registerMasterThreadPool.shutdownNow()
    executors.values.foreach(_.kill())
    drivers.values.foreach(_.kill())
    shuffleService.stop()
    webUi.stop()
    metricsSystem.stop()
  }

  private def trimFinishedExecutorsIfNecessary(): Unit = {
    // do not need to protect with locks since both WorkerPage and Restful server get data through
    // thread-safe RpcEndPoint
    if (finishedExecutors.size > retainedExecutors) {
      finishedExecutors.take(math.max(finishedExecutors.size / 10, 1)).foreach {
        case (executorId, _) => finishedExecutors.remove(executorId)
      }
    }
  }

  private def trimFinishedDriversIfNecessary(): Unit = {
    // do not need to protect with locks since both WorkerPage and Restful server get data through
    // thread-safe RpcEndPoint
    if (finishedDrivers.size > retainedDrivers) {
      finishedDrivers.take(math.max(finishedDrivers.size / 10, 1)).foreach {
        case (driverId, _) => finishedDrivers.remove(driverId)
      }
    }
  }

  /**
    * 处理Driver状态改变事件
    * @param driverStateChanged
    */
  private[worker] def handleDriverStateChanged(driverStateChanged: DriverStateChanged): Unit = {
    val driverId = driverStateChanged.driverId
    val exception = driverStateChanged.exception
    val state = driverStateChanged.state
    state match {
      case DriverState.ERROR =>
        logWarning(s"Driver $driverId failed with unrecoverable exception: ${exception.get}")
      case DriverState.FAILED =>
        logWarning(s"Driver $driverId exited with failure")
      case DriverState.FINISHED =>
        logInfo(s"Driver $driverId exited successfully")
      case DriverState.KILLED =>
        logInfo(s"Driver $driverId was killed by user")
      case _ =>
        logDebug(s"Driver $driverId changed state to $state")
    }
    // //worker接收到driver的完成/kill/失败等一些信息后，会将driver状态的变更发送给master,这里看master.receive()方法
    sendToMaster(driverStateChanged)
    //将该driver放入到已经完成的driver缓存中
    val driver = drivers.remove(driverId).get
    finishedDrivers(driverId) = driver
    //将该driver所占用的内存和CPU信息释放出来
    trimFinishedDriversIfNecessary()
    memoryUsed -= driver.driverDesc.mem
    coresUsed -= driver.driverDesc.cores
  }

  private[worker] def handleExecutorStateChanged(executorStateChanged: ExecutorStateChanged):
    Unit = {
    sendToMaster(executorStateChanged)
    val state = executorStateChanged.state
    if (ExecutorState.isFinished(state)) {
      val appId = executorStateChanged.appId
      val fullId = appId + "/" + executorStateChanged.execId
      val message = executorStateChanged.message
      val exitStatus = executorStateChanged.exitStatus
      executors.get(fullId) match {
        case Some(executor) =>
          logInfo("Executor " + fullId + " finished with state " + state +
            message.map(" message " + _).getOrElse("") +
            exitStatus.map(" exitStatus " + _).getOrElse(""))
          executors -= fullId
          finishedExecutors(fullId) = executor
          trimFinishedExecutorsIfNecessary()
          coresUsed -= executor.cores
          memoryUsed -= executor.memory
        case None =>
          logInfo("Unknown Executor " + fullId + " finished with state " + state +
            message.map(" message " + _).getOrElse("") +
            exitStatus.map(" exitStatus " + _).getOrElse(""))
      }
      maybeCleanupApplication(appId)
    }
  }
}

private[deploy] object Worker extends Logging {
  val SYSTEM_NAME = "sparkWorker"
  val ENDPOINT_NAME = "Worker"

  /**
    * Worker节点也通过main函数来启动，主要完成以下内容：
    *   1.创建SparkConf。
    *   2.Worker参数解析。
    *   3.创建，启动ActorSystem,并且向ActorSystem注册Worker.
    */
  def main(argStrings: Array[String]) {
    Utils.initDaemon(log)
    val conf = new SparkConf
    val args = new WorkerArguments(argStrings, conf)
    val rpcEnv = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, args.cores,
      args.memory, args.masters, args.workDir, conf = conf)
    rpcEnv.awaitTermination()
  }


  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      cores: Int,
      memory: Int,
      masterUrls: Array[String],
      workDir: String,
      workerNumber: Option[Int] = None,
      conf: SparkConf = new SparkConf): RpcEnv = {

    // The LocalSparkCluster runs multiple local sparkWorkerX RPC Environments
    // LocalSparkCluster运行多个本地sparkWorkerX RPC环境。
    val systemName = SYSTEM_NAME + workerNumber.map(_.toString).getOrElse("")
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(systemName, host, port, conf, securityMgr)
    val masterAddresses = masterUrls.map(RpcAddress.fromSparkURL(_))
    //  调用这个方法之前，会调用,worker的onStart()方法，
    rpcEnv.setupEndpoint(ENDPOINT_NAME, new Worker(rpcEnv, webUiPort, cores, memory,
      masterAddresses, ENDPOINT_NAME, workDir, conf, securityMgr))
    rpcEnv
  }

  def isUseLocalNodeSSLConfig(cmd: Command): Boolean = {
    val pattern = """\-Dspark\.ssl\.useNodeLocalConf\=(.+)""".r
    val result = cmd.javaOpts.collectFirst {
      case pattern(_result) => _result.toBoolean
    }
    result.getOrElse(false)
  }

  def maybeUpdateSSLSettings(cmd: Command, conf: SparkConf): Command = {
    val prefix = "spark.ssl."
    val useNLC = "spark.ssl.useNodeLocalConf"
    if (isUseLocalNodeSSLConfig(cmd)) {
      val newJavaOpts = cmd.javaOpts
          .filter(opt => !opt.startsWith(s"-D$prefix")) ++
          conf.getAll.collect { case (key, value) if key.startsWith(prefix) => s"-D$key=$value" } :+
          s"-D$useNLC=true"
      cmd.copy(javaOpts = newJavaOpts)
    } else {
      cmd
    }
  }
}
