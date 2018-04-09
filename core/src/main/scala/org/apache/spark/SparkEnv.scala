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

import java.io.File
import java.net.Socket
import java.util.Locale

import scala.collection.mutable
import scala.util.Properties

import com.google.common.collect.MapMaker

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonWorkerFactory
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryManager, StaticMemoryManager, UnifiedMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.{LiveListenerBus, OutputCommitCoordinator}
import org.apache.spark.scheduler.OutputCommitCoordinator.OutputCommitCoordinatorEndpoint
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.serializer.{JavaSerializer, Serializer, SerializerManager}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage._
import org.apache.spark.util.{RpcUtils, Utils}

/**
 * :: DeveloperApi ::
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
  * // 保存运行的Spark实例的所有运行时环境对象（无论是master的还是worker的）
 * including the serializer, RpcEnv, block manager, map output tracker, etc. Currently
  * //包括串行，RpcEnv，块管理、地图输出跟踪，等等
 * Spark code finds the SparkEnv through a global variable, so all the threads can access the same
 * SparkEnv. It can be accessed by SparkEnv.get (e.g. after creating a SparkContext).
  * // Spark代码查找sparkenv通过一个全局变量，那么所有的线程可以访问同一sparkenv
 *
 * NOTE: This is not intended for external use. This is exposed for Shark and may be made private
 *       in a future release.
  *       // 注意：这个不是暴露给外人用的，这个可能会在未来的版本中开放给Shark
 */
@DeveloperApi
class SparkEnv (
    val executorId: String,
    private[spark] val rpcEnv: RpcEnv,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val serializerManager: SerializerManager,
    val mapOutputTracker: MapOutputTracker,
    val shuffleManager: ShuffleManager,
    val broadcastManager: BroadcastManager,
    val blockManager: BlockManager,
    val securityManager: SecurityManager,
    val metricsSystem: MetricsSystem,
    val memoryManager: MemoryManager,
    val outputCommitCoordinator: OutputCommitCoordinator,
    val conf: SparkConf) extends Logging {

  private[spark] var isStopped = false
  private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()

  // A general, soft-reference map for metadata needed during HadoopRDD split computation
  // (e.g., HadoopFileRDD uses this to cache JobConfs and InputFormats).
  // 一般的，对于hadooprdd分割计算过程中所需的元数据软参考图
  // （例如，hadoopfilerdd使用该缓存jobconfs和InputFormats）。
  private[spark] val hadoopJobMetadata = new MapMaker().softValues().makeMap[String, Any]()

  private[spark] var driverTmpDir: Option[String] = None

  private[spark] def stop() {

    if (!isStopped) {
      isStopped = true
      pythonWorkers.values.foreach(_.stop())
      mapOutputTracker.stop()
      shuffleManager.stop()
      broadcastManager.stop()
      blockManager.stop()
      blockManager.master.stop()
      metricsSystem.stop()
      outputCommitCoordinator.stop()
      rpcEnv.shutdown()
      rpcEnv.awaitTermination()

      // If we only stop sc, but the driver process still run as a services then we need to delete
      // the tmp dir, if not, it will create too many tmp dirs.
      // We only need to delete the tmp dir create by driver····？“
      // 如果我们停止sc。但是driver进程仍然在后台作为一个服务。我们需要删除临时文件，如果没有，
      // 它将建立更多的tmp临时文件夹 我们仅仅需要我们启动的driver去删除tmp目录
      driverTmpDir match {
        case Some(path) =>
          try {
            Utils.deleteRecursively(new File(path))
          } catch {
            case e: Exception =>
              logWarning(s"Exception while deleting Spark temp dir: $path", e)
          }
        case None =>
        // We just need to delete tmp dir created by driver, so do nothing on executor
        // 我们仅仅需要driver去删除临时文件夹，所以这里什么都不需要做
      }
    }
  }

  // 创建python的工作目录
  private[spark]
  def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
    }
  }

  // 摧毁Python的工作目录
  private[spark]
  def destroyPythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }

  // 释放Python的工作工作节点
  private[spark]
  def releasePythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.releaseWorker(worker))
    }
  }
}

// SparkEnv的半生对象
object SparkEnv extends Logging {
  @volatile private var env: SparkEnv = _

  private[spark] val driverSystemName = "sparkDriver"
  private[spark] val executorSystemName = "sparkExecutor"

  def set(e: SparkEnv) {
    env = e
  }


  /**
   * Returns the SparkEnv.  获取SparkEnv对象
   */
  def get: SparkEnv = {
    env
  }

  /**
   * Create a SparkEnv for the driver.  为driver创建一个SparkEnv
    *
    * conf: SparkConf conf是对SparkConf的复制
    * listenerBus 才用监听器模式维护各类事件的处理
   */
  private[spark] def createDriverEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus,
      numCores: Int,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    assert(conf.contains(DRIVER_HOST_ADDRESS),
      s"${DRIVER_HOST_ADDRESS.key} is not set on the driver!")
    assert(conf.contains("spark.driver.port"), "spark.driver.port is not set on the driver!")
    val bindAddress = conf.get(DRIVER_BIND_ADDRESS) // bindAddress:"192.168.2.89"
    val advertiseAddress = conf.get(DRIVER_HOST_ADDRESS) // advertiseAddress:"192.168.2.89"
    val port = conf.get("spark.driver.port").toInt    //port:0
    val ioEncryptionKey = if (conf.get(IO_ENCRYPTION_ENABLED)) {
      Some(CryptoStreamUtils.createKey(conf))
    } else {
      None
    }

    // createDriverEnv最终调用的是create方法创建SparkEnv
    create(
      conf,
      SparkContext.DRIVER_IDENTIFIER,
      bindAddress,
      advertiseAddress,
      port,
      isLocal,
      numCores,
      ioEncryptionKey,
      listenerBus = listenerBus,
      mockOutputCommitCoordinator = mockOutputCommitCoordinator
    )
  }

  /**
   * Create a SparkEnv for an executor.   // 为executor创建一个SparkEnv对象
   * In coarse-grained mode, the executor provides an RpcEnv that is already instantiated.
    * 在粗粒度的方式，执行器提供了一个rpcenv已经实例化。
   */
  private[spark] def createExecutorEnv(
      conf: SparkConf,
      executorId: String,
      hostname: String,
      port: Int,
      numCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      isLocal: Boolean): SparkEnv = {
    val env = create(
      conf,
      executorId,
      hostname,
      hostname,
      port,
      isLocal,
      numCores,
      ioEncryptionKey
    )
    SparkEnv.set(env)
    env
  }

  /**
   * Helper method to create a SparkEnv for a driver or an executor.
    * 辅助方法来创建一个驱动程序或执行器sparkenv。
    *
    * SparkEnv的构造步骤如下：
    *     1.创建安全管理器SecurityManager
    *     2.创建给予AKKa的分布式消息系统ActorSystem;
    *     3.创建Map任务输出跟踪器mapOutputTracker;
    *     4.实例化ShuffleManager;
    *     5.创建ShuffleMemoryManager;
    *     6.创建块传输服务BlockTransferService;
    *     7.创建BlockManagerMaster;
    *     8.创建块管理器BlockManager;
    *     9.创建广播管理器BroadcastManager;
    *     10.创建缓存管理器CacheManager;
    *     11.创建HTTP文件服务器HttpFileServer;
    *     12.创建测量系统MetricsSystem;
    *     13.创建输出提交控制器OutputCommitCoordinator;
    *     14.创建SparkEnv;
   */
  private def create(
      conf: SparkConf,
      executorId: String,
      bindAddress: String,
      advertiseAddress: String,
      port: Int,
      isLocal: Boolean,
      numUsableCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      listenerBus: LiveListenerBus = null,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

    // 驱动程序的Executor的id。这里是一个布尔值，这里为什么要让他们相等呢？下面好多地方都用到了它
    val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER   // isDriver:true executorId:"driver"

    // Listener bus is only used on the driver 监听总线仅仅用在驱动程序上
    // 如果没有创建监听总线如果尝试创建驱动的SparkEnv，将会报错
    if (isDriver) {
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }


    // ===================== 1.创建安全管理器SecurityManager ======================
    // 安全管理器是什么呢？请看http://blog.csdn.net/qq_21383435/article/details/78560364
    val securityManager = new SecurityManager(conf, ioEncryptionKey) // TODO:调试注释 securityManager:SecurityManager@2227 conf:SparkConf@2141 ioEncyptionKey:"none"
    ioEncryptionKey.foreach { _ =>
      // 检查是否应启用网络加密。
      if (!securityManager.isEncryptionEnabled()) {
        logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the " +
          "wire.")
      }
    }

    /**
      * rpcEnv是个什么鬼？
      * 在SparkContext初始化环境时，初始化SparkEnv的时候使用下面代码创建RpcEnv
      */
    val systemName = if (isDriver) driverSystemName else executorSystemName //TODO: yarn-client模式下为 systemName:"sparkDriver"
    val rpcEnv = RpcEnv.create(systemName, bindAddress, advertiseAddress, port, conf,
      securityManager, clientMode = !isDriver)

    // Figure out which port RpcEnv actually bound to in case the original port is 0 or occupied.
    // In the non-driver case, the RPC env's address may be null since it may not be listening
    // for incoming connections.
    // 找出哪些端口rpcenv实际上必然在原来的端口是0或占领。在没有驱动的情况下，
    // RPC env的地址可能是零因为它可能不会侦听传入的连接。
    if (isDriver) {
      conf.set("spark.driver.port", rpcEnv.address.port.toString)
    } else if (rpcEnv.address != null) {
      conf.set("spark.executor.port", rpcEnv.address.port.toString)
      logInfo(s"Setting spark.executor.port to: ${rpcEnv.address.port.toString}")
    }

    // Create an instance of the class with the given name, possibly initializing it with our conf
    // 根据指定的名称创建一个类的实例，尽可能根据我们的配置初始化
    def instantiateClass[T](className: String): T = {
      val cls = Utils.classForName(className)
      // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
      // SparkConf, then one taking no arguments
      // 寻找一个构造函数以sparkconf和isdriver的布尔值，然后仅仅告诉SparkConf，然后连接没有传递参数
      try {
        cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
          .newInstance(conf, new java.lang.Boolean(isDriver))
          .asInstanceOf[T]
      } catch {
        case _: NoSuchMethodException =>
          try {
            cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
          } catch {
            case _: NoSuchMethodException =>
              cls.getConstructor().newInstance().asInstanceOf[T]
          }
      }
    }

    // Create an instance of the class named by the given SparkConf property, or defaultClassName
    // if the property is not set, possibly initializing it with our conf
    // 根据指定的SparkConf的配置创建这个类的实例，或使用defaultclassname如果没有设置的属性，
    // 尽可能根据我们的配置初始化
    def instantiateClassFromConf[T](propertyName: String, defaultClassName: String): T = {
      instantiateClass[T](conf.get(propertyName, defaultClassName))
    }

    // 序列化工具这里指定使用java的序列化
    val serializer = instantiateClassFromConf[Serializer](
      "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    logDebug(s"Using serializer: ${serializer.getClass}")

    // ============================= 创建序列化管理器 SerializerManager ===================
    //  http://blog.csdn.net/qq_21383435/article/details/78581511
    val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)

    val closureSerializer = new JavaSerializer(conf)

    // 如果当前应用程序是Driver，则创建BlockManagerMasterEndpoint，并且注册到RpcEnv中；
    // 如果当前应用程序是Executor，则从RpcEnv中找到BlockManagerMasterEndpoint的引用。
    def registerOrLookupEndpoint(
        name: String, endpointCreator: => RpcEndpoint):
      RpcEndpointRef = {
      if (isDriver) {
        // 17/12/05 11:56:50 INFO SparkEnv: Registering MapOutputTracker
        // 17/12/05 11:56:50 INFO SparkEnv: Registering BlockManagerMaster
        // 21=>17/12/05 11:56:50 INFO SparkEnv: Registering OutputCommitCoordinator
        logInfo("Registering " + name)
        rpcEnv.setupEndpoint(name, endpointCreator)
      } else {
        RpcUtils.makeDriverRef(name, conf, rpcEnv)
      }
    }


    // ============================== 9.创建广播管理器BroadcastManager;======================
    // BroadcastManager用于将配置信息和序列化后的RDD,job以及ShuffleDependency等信息在本地存储。
    // 如果为了容灾，也会复制到其他节点上。
    // spark学习-34-Spark的BroadcastManager广播管理
    // http://blog.csdn.net/qq_21383435/article/details/78592022
    val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)


    // =============================3.创建Map任务输出跟踪器mapOutputTracker;==================
    /*
        mapOutputTracker用于跟踪map阶段任务的输出状态，此状态便于reduce阶段任务获取地址以及中间输出结果。每个map任务或者
        reduce任务都会有唯一的标识。分别为mapId和reduceId.每个reduce任务的输入可能是多个map任务的输出，reduce会到各个map
        任务的所有节点上拉去Block，这一过程交shuffle，每批shuffle过程都有唯一的表示shuffleId。
     */
    val mapOutputTracker = if (isDriver) {
      new MapOutputTrackerMaster(conf, broadcastManager, isLocal)
    } else {
      new MapOutputTrackerWorker(conf)
    }

    // Have to assign trackerEndpoint after initialization as MapOutputTrackerEndpoint
    // requires the MapOutputTracker itself
    /** 必须在初始化后指定trackerEndpoint，因为MapOutputTrackerEndpoint需要MapOutputTracker */
    mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(
        rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))

    // Let the user specify short names for shuffle managers
    // 让用户给shuffle managers指定一个短名称
    /** 这里支持了两种类型的shuffle
      * 以前还有一种"hash"->"org.apache.spark.shuffle.hash.HashShuffleManager"
      * 但是HashShuffleManager被取消使用了
      * */
    val shortShuffleMgrNames = Map(
      "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
      "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName)
    // 默认采取SortedBasedShuffle的方式。
    val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
    val shuffleMgrClass =
      shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase(Locale.ROOT), shuffleMgrName)

    // =========================创建ShuffleManager============================================
    /** ShuffleManager负责管理本地及远程的block数据的Shuffle操作。
      * 详情：http://blog.csdn.net/qq_21383435/article/details/78634471
      * */
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)


    // =======================创建MemoryManager==================================================================
    /**
      * 根据 spark.memory.useLegacyMode 值的不同，会创建 MemoryManager 不同子类的实例：
      * 值为 false：创建 UnifiedMemoryManager 类实例，该类为新的内存管理模块的实现
      * 值为 true：创建 StaticMemoryManager类实例，该类为1.6版本以前旧的内存管理模块的实现
      * */
    val useLegacyMemoryManager = conf.getBoolean("spark.memory.useLegacyMode", false)
    val memoryManager: MemoryManager =
      if (useLegacyMemoryManager) {
        // 如果还是采用之前的方式，则使用StaticMemoryManager内存管理模型，即静态内存管理
        // 配套博客：https://blog.csdn.net/qq_21383435/article/details/78641586
        new StaticMemoryManager(conf, numUsableCores)
      } else {
        // 否则，使用最新的UnifiedMemoryManager内存管理模型，即统一内存管理模型
        // 我们再看下UnifiedMemoryManager，即统一内存管理器。在SparkEnv中，它是通过如下方式完成初始化的：
        // 读者这里可能有疑问了，为什么没有new关键字呢？这正是scala语言的特点。
        // 它其实是通过UnifiedMemoryManager类的apply()方法完成初始化的。
        UnifiedMemoryManager(conf, numUsableCores)
      }




    // =================================.创建块传输服务BlockTransferService;===========================
    /*
        blockTransferService默认为NettyBlockTransferService（可以配置属相spark.shuffle.blockTransferService使用NioBlockTransferService）
        ,它使用Netty法人一步时间驱动的网络应用框架，提供web服务及客户端，获取远程节点上的Block集合。
     */
    val blockManagerPort = if (isDriver) {
      conf.get(DRIVER_BLOCK_MANAGER_PORT)
    } else {
      conf.get(BLOCK_MANAGER_PORT)
    }
    val blockTransferService =
      new NettyBlockTransferService(conf, securityManager, bindAddress, advertiseAddress,
        blockManagerPort, numUsableCores)

    // ================================7.创建BlockManagerMaster; ========================
    val blockManagerMaster = new BlockManagerMaster(registerOrLookupEndpoint(
      BlockManagerMaster.DRIVER_ENDPOINT_NAME,
      new BlockManagerMasterEndpoint(rpcEnv, isLocal, conf, listenerBus)),
      conf, isDriver)


    // =========================创建BlockManager==================================================
    // NB: blockManager is not valid until initialize() is called later.
    // BlockManager负责对Block的管理，只有在BlockManager的痴实话方法initialize被调用之后，它才是有效的。
    // Blockmanager作为存储系统的一部分。
    val blockManager = new BlockManager(executorId, rpcEnv, blockManagerMaster,
      serializerManager, conf, memoryManager, mapOutputTracker, shuffleManager,
      blockTransferService, securityManager, numUsableCores)


    // =======================创建测量系统MetricsSystem====================================================
    /**
        createMetricsSystem方法主要调用了new MetricsSystem(instance, conf, securityMgr)方法
        Instance:制定了谁在使用测量系统

        这里val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER 而SparkContext.DRIVER_IDENTIFIER的值是driver
      如果executorId也是driver,那么isDriver就为真，创建的是driver的监测系统，否则就是创建executor的监测系统
     */
    val metricsSystem = if (isDriver) {
      // Don't start metrics system right now for Driver.
      // We need to wait for the task scheduler to give us an app ID.
      // Then we can start the metrics system.
      // 现在不要为Driver启动metrics system，我们需要task scheduler任务调度器给我们一个APP id，
      // 在这之后再启动 metrics system.
      MetricsSystem.createMetricsSystem("driver", conf, securityManager)
    } else {
      // We need to set the executor ID before the MetricsSystem is created because sources and
      // sinks specified in the metrics configuration file will want to incorporate this executor's
      // ID into the metrics they report.
      // 我们需要设置的executor 的ID在创建MetricsSystem之前，因为sources和sinks的标准配置文件指定了
      // 将要把这个 executor 的ID 传递到metrics的报告中。
      conf.set("spark.executor.id", executorId)
      val ms = MetricsSystem.createMetricsSystem("executor", conf, securityManager)
      ms.start()
      ms
    }


    // =======================创建输出提交控制器OutputCommitCoordinator====================================================
    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf, isDriver)
    }
    val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator",
      new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))
    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)


    // =============================创建SparkEnv===================================================
    /**
      serializer和closureSerializer都是使用Class.forName反射生成的org.apache.spark.serializer.JavaSerializer类的
      实例。其中closureSerializer实例特别用来对Scala中的闭包进行序列化。
      */
    val envInstance = new SparkEnv(
      executorId,
      rpcEnv,
      serializer,
      closureSerializer,
      serializerManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockManager,
      securityManager,
      metricsSystem,
      memoryManager,
      outputCommitCoordinator,
      conf)

    // Add a reference to tmp dir created by driver, we will delete this tmp dir when stop() is
    // called, and we only need to do it for driver. Because driver may run as a service, and if we
    // don't delete this tmp dir when sc is stopped, then will create too many tmp dirs.
    // driver创建一个临时目录的引用。我们将删除这些临时文件在stop()被调用之后而且我们仅仅需要为driver做这些。
    // 因为driver是运行一个服务，如果我们在sc停止后不删除这些临时文件夹的话，他将会创建很多这样的临时文件夹
    if (isDriver) {
      val sparkFilesDir = Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
      envInstance.driverTmpDir = Some(sparkFilesDir)
    }

    envInstance
  }






  /**
   * Return a map representation of jvm information, Spark properties, system properties, and
   * class paths. Map keys define the category, and map values represent the corresponding
   * attributes as a sequence of KV pairs. This is used mainly for SparkListenerEnvironmentUpdate.
    *
    * 返回JVM信息、Spark属性、系统属性和类路径的映射表示。映射键定义类别，map值表示相应的属性，
    * 作为一对KV对序列。这是主要用于sparklistenerenvironmentupdate。
    *
   */
  private[spark]
  def environmentDetails(
      conf: SparkConf,
      schedulingMode: String,
      addedJars: Seq[String],
      addedFiles: Seq[String]): Map[String, Seq[(String, String)]] = {

    import Properties._
    // java的JVM信息
    val jvmInformation = Seq(
      ("Java Version", s"$javaVersion ($javaVendor)"),
      ("Java Home", javaHome),
      ("Scala Version", versionString)
    ).sorted

    // Spark properties
    // This includes the scheduling mode whether or not it is configured (used by SparkUI)
    val schedulerMode =
      if (!conf.contains("spark.scheduler.mode")) {
        Seq(("spark.scheduler.mode", schedulingMode))
      } else {
        Seq[(String, String)]()
      }
    val sparkProperties = (conf.getAll ++ schedulerMode).sorted

    // System properties that are not java classpaths
    val systemProperties = Utils.getSystemProperties.toSeq
    val otherProperties = systemProperties.filter { case (k, _) =>
      k != "java.class.path" && !k.startsWith("spark.")
    }.sorted

    // Class paths including all added jars and files
    val classPathEntries = javaClassPath
      .split(File.pathSeparator)
      .filterNot(_.isEmpty)
      .map((_, "System Classpath"))
    val addedJarsAndFiles = (addedJars ++ addedFiles).map((_, "Added By User"))
    val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted

    Map[String, Seq[(String, String)]](
      "JVM Information" -> jvmInformation,
      "Spark Properties" -> sparkProperties,
      "System Properties" -> otherProperties,
      "Classpath Entries" -> classPaths)
  }
}
