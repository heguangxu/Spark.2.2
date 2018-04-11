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

import java.io._
import java.lang.reflect.Constructor
import java.net.URI
import java.util.{Arrays, Locale, Properties, ServiceLoader, UUID}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.generic.Growable
import scala.collection.mutable.HashMap
import scala.language.implicitConversions
import scala.reflect.{classTag, ClassTag}
import scala.util.control.NonFatal

import com.google.common.collect.MapMaker
import org.apache.commons.lang3.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{ArrayWritable, BooleanWritable, BytesWritable, DoubleWritable, FloatWritable, IntWritable, LongWritable, NullWritable, Text, Writable}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf, SequenceFileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, Job => NewHadoopJob}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.{LocalSparkCluster, SparkHadoopUtil}
import org.apache.spark.input.{FixedLengthBinaryInputFormat, PortableDataStream, StreamInputFormat, WholeTextFileInputFormat}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.partial.{ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, StandaloneSchedulerBackend}
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.storage._
import org.apache.spark.storage.BlockManagerMessages.TriggerThreadDump
import org.apache.spark.ui.{ConsoleProgressBar, SparkUI}
import org.apache.spark.ui.jobs.JobProgressListener
import org.apache.spark.util._



/**
 * Main entry point for Spark functionality. A SparkContext represents the connection to a Spark
 * cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
 *
  * Spark 功能的主要入口点。一个sparkcontext代表一个与Spark cluster的连接，可以用来创建RDDS，累加器和广播变量。
  *
 * Only one SparkContext may be active per JVM.  You must `stop()` the active SparkContext before
 * creating a new one.  This limitation may eventually be removed; see SPARK-2243 for more details.
  *
  * 每个JVM只有一个sparkcontext处于活跃状态。你必须` stop() `停止sparkcontext在你创建新的一个SparkContext之前，
  * 这种限制可能最终被删除；看到更多的细节spark-2243。
 *
 * @param config a Spark Config object describing the application configuration. Any settings in
 *   this config overrides the default configs as well as system properties.
  *  配置Spark配置对象描述应用程序配置。这个配置将覆盖默认的配置以及系统性能
  *
  *
  *  SparkContext的初始化步骤：
  *     1。创建Spark执行环境SparkEnv;
  *     2.创建RDD清理器metadataCleaner 这个已经丢弃;
  *     3.创建并且初始化Spark UI;
  *     4.hadoop相关配置以及Executor环境变量的设置；
  *     5.创建任务调度TaskScheduler;
  *     6.创建和启动DAGScheduler;
  *     7.TaskScheduler的启动；
  *     8.初始化管理器BlockManager（BlockManager是存储体系的主要组件之一）
  *     9.启动测量系统MetricsSystem;
  *     10.创建和启动Executor分配管理器ExecutorAllocationManager;
  *     11.ContextCleaner的启动和创建。
  *     12.Spark环境更新
  *     13.创建DAGSchedulerSource和BlockManagerSource;
  *     14.将SparkContext标记激活
 */
class SparkContext(config: SparkConf) extends Logging {

  // The call site where this SparkContext was constructed.
  // 这个SparkContext构建的调用站点。
  // 当在spark包中调用类时，返回调用spark的用户代码类的名称，以及它们调用的spark方法。
  // CallSite存储了线程栈中最靠近栈顶的用户类以及最靠近栈底的Scala或者Spark核心类信息。
  private val creationSite: CallSite = Utils.getCallSite()

  // If true, log warnings instead of throwing exceptions when multiple SparkContexts are active
  // 如果设置为true log日志里将会抛出多个SparkContext处于活动状态的异常
  private val allowMultipleContexts: Boolean =
    config.getBoolean("spark.driver.allowMultipleContexts", false)

  // In order to prevent multiple SparkContexts from being active at the same time, mark this
  // context as having started construction.
  // NOTE: this must be placed at the beginning of the SparkContext constructor.
  // 为了预防多SparkContexts同一时间处于活动状态，从这个上下文开始构建SparkContext
  // 注：这placed必须要开始的sparkcontext constructor。
  SparkContext.markPartiallyConstructed(this, allowMultipleContexts)

  // 得到系统的当前时间
  val startTime = System.currentTimeMillis()

  // AtomicBoolean是线程阻塞的，一个线程没结束，另外的线程都不能使用，就如两个人同时去操作一盆花，一个浇水，一个看花，
  // 普通的布尔值，可以同时做，但是AtomicBoolean是，有人看花的时候另外一个不能浇水，或者浇水的时候不能看花，除非一个完成了
  private[spark] val stopped: AtomicBoolean = new AtomicBoolean(false)


  /**
    * 判断sparkContext是否已经停止了，在每个算子，转子，都使用，在SparkSession中，也在使用
    */
  private[spark] def assertNotStopped(): Unit = {
    // 如果为真
    if (stopped.get()) {
      // SparkContext.activeContext 主动、全面构建sparkcontext。如果没有sparkcontext是活动的，那么这是`空`。
      // 得到当前活动的activeContext
      val activeContext = SparkContext.activeContext.get()
      val activeCreationSite =
        if (activeContext == null) {
          "(No active SparkContext.)"
        } else {
          activeContext.creationSite.longForm
        }
      throw new IllegalStateException(
        s"""Cannot call methods on a stopped SparkContext.
           |This stopped SparkContext was created at:
           |
           |${creationSite.longForm}
           |
           |The currently active SparkContext was created at:
           |
           |$activeCreationSite
         """.stripMargin)
    }
  }

  /**
   * Create a SparkContext that loads settings from system properties (for instance, when
   * launching with ./bin/spark-submit).
    * 创建一个SparkContext从系统的配置文件中得到相关信息（例如 当用./bin/spark-submit去启动的时候）
   */
  def this() = this(new SparkConf())

  /**
   * Alternative constructor that allows setting common Spark properties directly
   * 允许直接设置公共Spark属性的替代构造函数
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param conf a [[org.apache.spark.SparkConf]] object specifying other Spark parameters
   */
  def this(master: String, appName: String, conf: SparkConf) =
    this(SparkContext.updatedConf(conf, master, appName))

  /**
   * Alternative constructor that allows setting common Spark properties directly
   * 允许直接设置公共Spark属性的替代构造函数
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   * @param sparkHome Location where Spark is installed on cluster nodes.
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   * @param environment Environment variables to set on worker nodes.
   */
  def this(
      master: String,
      appName: String,
      sparkHome: String = null,
      jars: Seq[String] = Nil,
      environment: Map[String, String] = Map()) = {
    this(SparkContext.updatedConf(new SparkConf(), master, appName, sparkHome, jars, environment))
  }

  // NOTE: The below constructors could be consolidated using default arguments. Due to
  // Scala bug SI-8479, however, this causes the compile step to fail when generating docs.
  // Until we have a good workaround for that bug the constructors remain broken out.
  /**
    *
    * 注意：下面的构造函数可以使用默认参数进行合并。由于Scala的bug si-8479，然而，
    * 这会导致编译生成的文档步骤失败。直到我们有很好的解决方法去解决这个BUG,否则他会继续存在。
    */


  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   */
  private[spark] def this(master: String, appName: String) =
    this(master, appName, null, Nil, Map())

  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   * @param sparkHome Location where Spark is installed on cluster nodes.
   */
  private[spark] def this(master: String, appName: String, sparkHome: String) =
    this(master, appName, sparkHome, Nil, Map())

  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   * @param sparkHome Location where Spark is installed on cluster nodes.
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   */
  private[spark] def this(master: String, appName: String, sparkHome: String, jars: Seq[String]) =
    this(master, appName, sparkHome, jars, Map())

  // log out Spark Version in Spark driver log
  // 17/12/05 11:56:49 INFO SparkContext: Running Spark version 2.1.1 第一句打印的带时间的日志
  logInfo(s"Running Spark version $SPARK_VERSION")

  // spark2.1.0版本需要Scala2.10版本以上的否则 打印警告
  warnDeprecatedVersions()

  /* ------------------------------------------------------------------------------------- *
   | Private variables. These variables keep the internal state of the context, and are    |
   | not accessible by the outside world. They're mutable since we want to initialize all  |
   | of them to some neutral value ahead of time, so that calling "stop()" while the       |
   | constructor is still running is safe.                                                 |
   * ------------------------------------------------------------------------------------- */

  /**
  Private变量。这些变量在context中保持内部的状态 ，而且不允许外部访问。
  他们在我们初始化之前是可变的，当我们在构造函数正在安全模式下运行之前调用“stop()”函数

    */


  private var _conf: SparkConf = _
  private var _eventLogDir: Option[URI] = None
  private var _eventLogCodec: Option[String] = None
  private var _env: SparkEnv = _
  private var _jobProgressListener: JobProgressListener = _
  private var _statusTracker: SparkStatusTracker = _
  private var _progressBar: Option[ConsoleProgressBar] = None
  private var _ui: Option[SparkUI] = None
  private var _hadoopConfiguration: Configuration = _
  private var _executorMemory: Int = _
  private var _schedulerBackend: SchedulerBackend = _   // 如果是local模式下，这个内容为_schedulerBackend: LocalSchedulerBackend
  private var _taskScheduler: TaskScheduler = _
  private var _heartbeatReceiver: RpcEndpointRef = _
  @volatile private var _dagScheduler: DAGScheduler = _
  private var _applicationId: String = _
  private var _applicationAttemptId: Option[String] = None
  private var _eventLogger: Option[EventLoggingListener] = None
  private var _executorAllocationManager: Option[ExecutorAllocationManager] = None
  private var _cleaner: Option[ContextCleaner] = None
  private var _listenerBusStarted: Boolean = false
  private var _jars: Seq[String] = _
  private var _files: Seq[String] = _
  private var _shutdownHookRef: AnyRef = _

  /* ------------------------------------------------------------------------------------- *
   | Accessors and public fields. These provide access to the internal state of the        |
   | context.
          访问和公共领域。这些提供对上下文内部状态的访问。|
   * ------------------------------------------------------------------------------------- */

  private[spark] def conf: SparkConf = _conf

  /**
   * Return a copy of this SparkContext's configuration. The configuration ''cannot'' be
   * changed at runtime.
    *  返回一个SparkContext的配置对象，这个对象不能在运行的时候改变
   */
  def getConf: SparkConf = conf.clone()

  def jars: Seq[String] = _jars
  def files: Seq[String] = _files
  def master: String = _conf.get("spark.master")
  def deployMode: String = _conf.getOption("spark.submit.deployMode").getOrElse("client")
  def appName: String = _conf.get("spark.app.name")

  private[spark] def isEventLogEnabled: Boolean = _conf.getBoolean("spark.eventLog.enabled", false)
  private[spark] def eventLogDir: Option[URI] = _eventLogDir
  private[spark] def eventLogCodec: Option[String] = _eventLogCodec

  // 判断是否为本地模式
  def isLocal: Boolean = Utils.isLocalMaster(_conf)

  /**
    *  如果context被停止了 或者中间停止了 就返回真
   * @return true if context is stopped or in the midst of stopping.
   */
  def isStopped: Boolean = stopped.get()

  // An asynchronous listener bus for Spark events
  // 一个Spark事件的异步监听总线，先创建listenerBus，因为它作为参数创递给sparkEnv
  private[spark] val listenerBus = new LiveListenerBus(this)

  // This function allows components created by SparkEnv to be mocked in unit tests:
  // 这个功能允许在单元测试组件创建的sparkenv
  private[spark] def createSparkEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus): SparkEnv = {
    SparkEnv.createDriverEnv(conf, isLocal, listenerBus, SparkContext.numDriverCores(master))
  }

  private[spark] def env: SparkEnv = _env

  // Used to store a URL for each static file/jar together with the file's local timestamp
  // 用于为每个静态文件/ JAR存储一个URL，并与文件的本地时间戳一起存储。
  private[spark] val addedFiles = new ConcurrentHashMap[String, Long]().asScala
  private[spark] val addedJars = new ConcurrentHashMap[String, Long]().asScala

  // Keeps track of all persisted RDDs  跟踪所有持久化RDDs
  private[spark] val persistentRdds = {
    val map: ConcurrentMap[Int, RDD[_]] = new MapMaker().weakValues().makeMap[Int, RDD[_]]()
    map.asScala
  }

  // 这个类将会在2.2版本以后移除
  private[spark] def jobProgressListener: JobProgressListener = _jobProgressListener

  // 监视job和stage进度的低级别状态报告API。
  def statusTracker: SparkStatusTracker = _statusTracker

  // sparkUI显示进度条的
  private[spark] def progressBar: Option[ConsoleProgressBar] = _progressBar

  private[spark] def ui: Option[SparkUI] = _ui

  def uiWebUrl: Option[String] = _ui.map(_.webUrl)

  /**
   * A default Hadoop Configuration for the Hadoop code (e.g. file systems) that we reuse.
   *  一个默认的hadoop配置方法为了hadoop的代码被重用（例如，文件系统）
   * @note As it will be reused in all Hadoop RDDs, it's better not to modify it unless you
   * plan to set some global configurations for all Hadoop RDDs.
    *
    *  因为它将被所有的hadoop RDD重用 ,你尽量不要修改，除非你想为所有的hadoop rdds设置全局变量
    *
    *  这个就是当启动SparkApplication的时候，里面会包含很多其他的配置，比如hbase的hive，等。
    *  当你使用的功能越来越多，这里面保存的配置就越来越多。
   */
  def hadoopConfiguration: Configuration = _hadoopConfiguration

  private[spark] def executorMemory: Int = _executorMemory

  // Environment variables to pass to our executors.
  // 环境变量传递给我们的执行者
  private[spark] val executorEnvs = HashMap[String, String]()

  // Set SPARK_USER for user who is running SparkContext.
  // 设置运行SparkContext的用户 返回当前用户名。这是当前登录的用户，除非它被“SPARK_USER”环境变量覆盖。
  val sparkUser = Utils.getCurrentUserName()

  //
  private[spark] def schedulerBackend: SchedulerBackend = _schedulerBackend

  private[spark] def taskScheduler: TaskScheduler = _taskScheduler
  private[spark] def taskScheduler_=(ts: TaskScheduler): Unit = {
    _taskScheduler = ts
  }

  private[spark] def dagScheduler: DAGScheduler = _dagScheduler
  private[spark] def dagScheduler_=(ds: DAGScheduler): Unit = {
    _dagScheduler = ds
  }

  /**
   * A unique identifier for the Spark application.
    *  spark应用程序的一个唯一的标识
   * Its format depends on the scheduler implementation.
    * 它的格式取决于调度的实现。
   * (i.e.
   *  in case of local spark app something like 'local-1433865536131'
   *  in case of YARN something like 'application_1433865536131_34483'
   * )
   */
  def applicationId: String = _applicationId
  def applicationAttemptId: Option[String] = _applicationAttemptId

  private[spark] def eventLogger: Option[EventLoggingListener] = _eventLogger

  private[spark] def executorAllocationManager: Option[ExecutorAllocationManager] =
    _executorAllocationManager

  private[spark] def cleaner: Option[ContextCleaner] = _cleaner

  private[spark] var checkpointDir: Option[String] = None

  // Thread Local variable that can be used by users to pass information down the stack
  // 线程局部变量，用户可以使用它将信息传递到堆栈中
  protected[spark] val localProperties = new InheritableThreadLocal[Properties] {
    override protected def childValue(parent: Properties): Properties = {
      // Note: make a clone such that changes in the parent properties aren't reflected in
      // the those of the children threads, which has confusing semantics (SPARK-10563).
      // 注：做一个克隆，改变父属性并不体现在那些孩子的线程，有混乱的语义（spark-10563）。
      SerializationUtils.clone(parent)
    }
    override protected def initialValue(): Properties = new Properties()
  }

  /* ------------------------------------------------------------------------------------- *
   | Initialization. This code initializes the context in a manner that is exception-safe. |
   | All internal fields holding state are initialized here, and any error prompts the     |
   | stop() method to be called.                                                           |
   * ------------------------------------------------------------------------------------- */
  // 初始化。此代码初始化上下文中的方式是exception-safe.all内部字段初始化来保持状态，和任何错误提示stop()方法被调用。


  private def warnSparkMem(value: String): String = {
    logWarning("Using SPARK_MEM to set amount of memory to use per executor process is " +
      "deprecated, please use spark.executor.memory instead.")
    // 使用spark_mem设置内存使用每个执行过程是不赞成的，请使用spark.executor.memory代替。
    value
  }

  // spark2.1.0版本需要Scala2.10版本以上的否则 打印警告
  private def warnDeprecatedVersions(): Unit = {
    val javaVersion = System.getProperty("java.version").split("[+.\\-]+", 3)
    if (scala.util.Properties.releaseVersion.exists(_.startsWith("2.10"))) {
      logWarning("Support for Scala 2.10 is deprecated as of Spark 2.1.0")
    }
  }

  /** Control our logLevel. This overrides any user-defined log settings.
    * 控制我们的日志级别。这会覆盖任何用户定义的日志设置。
   * @param logLevel The desired log level as a string.
    * Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
   */
  def setLogLevel(logLevel: String) {
    // let's allow lowercase or mixed case too    让我们也允许小写或者混合的例子
    val upperCased = logLevel.toUpperCase(Locale.ROOT)
    require(SparkContext.VALID_LOG_LEVELS.contains(upperCased),
      s"Supplied level $logLevel did not match one of:" +
        s" ${SparkContext.VALID_LOG_LEVELS.mkString(",")}")
    Utils.setLogLevel(org.apache.log4j.Level.toLevel(upperCased))
  }

  try {
    _conf = config.clone()   /** 克隆一个配置对象 */
    _conf.validateSettings() /** //检查不合法的配置或者是过时的配置，跑出一个异常*/

    //必须设置master
    if (!_conf.contains("spark.master")) {
      throw new SparkException("A master URL must be set in your configuration")
    }
    // 必须设置应用程序的名称
    if (!_conf.contains("spark.app.name")) {
      throw new SparkException("An application name must be set in your configuration")
    }

    // log out spark.app.name in the Spark driver logs
    logInfo(s"Submitted application: $appName")

    // System property spark.yarn.app.id must be set if user code ran by AM on a YARN cluster
    // 如果用户代码在Yarn集群上运行一个AM 那么必须设置系统属性spark.yarn.app.id
    if (master == "yarn" && deployMode == "cluster" && !_conf.contains("spark.yarn.app.id")) {
      throw new SparkException("Detected yarn cluster mode, but isn't running on a cluster. " +
        "Deployment to YARN is not supported directly by SparkContext. Please use spark-submit.")
    }

    if (_conf.getBoolean("spark.logConf", false)) {
      logInfo("Spark configuration:\n" + _conf.toDebugString)
    }

    // Set Spark driver host and port system properties. This explicitly sets the configuration
    // instead of relying on the default value of the config constant.
    // 系统配置中设置Spark driver主机名和端口。这显式地设置了配置，而不是依赖于配置常量的默认值。
    _conf.set(DRIVER_HOST_ADDRESS, _conf.get(DRIVER_HOST_ADDRESS))
    // 如果没有设置driver的端口  那么就默认为0
    _conf.setIfMissing("spark.driver.port", "0")

    // 这里DRIVER_IDENTIFIER = "driver"
    _conf.set("spark.executor.id", SparkContext.DRIVER_IDENTIFIER)

    // 这里设置了_jars
    _jars = Utils.getUserJars(_conf)
    _files = _conf.getOption("spark.files").map(_.split(",")).map(_.filter(_.nonEmpty))
      .toSeq.flatten

    // eventLogDir的日志目录/tmp/spark-events，但是我没找到在哪里
    _eventLogDir =
      if (isEventLogEnabled) {
        val unresolvedDir = conf.get("spark.eventLog.dir", EventLoggingListener.DEFAULT_LOG_DIR)
          .stripSuffix("/")
        Some(Utils.resolveURI(unresolvedDir))
      } else {
        None
      }

    _eventLogCodec = {
      // event log日志是否压缩
      val compress = _conf.getBoolean("spark.eventLog.compress", false)
      // 是否开启event日志isEventLogEnabled默认false 这里为假
      if (compress && isEventLogEnabled) {
        Some(CompressionCodec.getCodecName(_conf)).map(CompressionCodec.getShortName)
      } else {
        None
      }
    }

    if (master == "yarn" && deployMode == "client") System.setProperty("SPARK_YARN_MODE", "true")

    // "_jobProgressListener" should be set up before creating SparkEnv because when creating
    // "SparkEnv", some messages will be posted to "listenerBus" and we should not miss them.
    // “_jobprogresslistener”应在在sparkenv创建之前，因为当创建“sparkenv”，一些信息将被psot到“listenerbus”我们不应该错过。
    /** 创建JobProgressListener 这个要在2.2版本以后移除*/
    _jobProgressListener = new JobProgressListener(_conf)
    listenerBus.addListener(jobProgressListener)

    // Create the Spark execution environment (cache, map output tracker, etc)
    // 设置Spark的execution的环境变量（缓存，tracker的输出map）
    /** 这里才真正开始创建sparkEnv */
    _env = createSparkEnv(_conf, isLocal, listenerBus)
    SparkEnv.set(_env)

    // If running the REPL, register the repl's output dir with the file server.
    // 如果运行REPL，将REPL的输出目录注册到文件服务器。
    _conf.getOption("spark.repl.class.outputDir").foreach { path =>
      val replUri = _env.rpcEnv.fileServer.addDirectory("/classes", new File(path))
      _conf.set("spark.repl.class.uri", replUri)
    }

    // 监视job和stage进度的低级别状态报告API。 (基本上都是调用jobProgressListener这个里面的方法)
    /** 创建 SparkStatusTracker */
    _statusTracker = new SparkStatusTracker(this)


    /**  创建UI界面进度条 */
    _progressBar =
      if (_conf.getBoolean("spark.ui.showConsoleProgress", true) && !log.isInfoEnabled) {
        Some(new ConsoleProgressBar(this))
      } else {
        None
      }

    /** 这里默认启动了SPark的UI界面 */
    _ui =
      if (conf.getBoolean("spark.ui.enabled", true)) {
        // 调用了SparkUI.createLiveUI（）-》create(）
        Some(SparkUI.createLiveUI(this, _conf, listenerBus, _jobProgressListener,
          _env.securityManager, appName, startTime = startTime))
      } else {
        // For tests, do not enable the UI
        None
      }

    // Bind the UI before starting the task scheduler to communicate
    // the bound port to the cluster manager properly
    // 在启动任务计划程序之前将UI绑定到正确地将绑定端口传达给群集管理器。调用WebUI.bind()方法
    _ui.foreach(_.bind()) //启动jetty。bind方法继承自WebUI，该类负责和真实的Jetty Server API打交道

    /**
        默认情况下，Spark使用HDFS作为分布式文件系统，所以需要获取Hadoop相关配置信息，获取的信息包括：
          1.将AmazonS3文件系统的AccessKeyId和SecretAccessKey加载到hadoop的Configuration;
          2.将SparkConf中的所有以spark.hadoop.开头的属性都复制到hadoop的Configuration;
          3.将SparkConf的属性spark.buffer.size复制为hadoop的Configuration的配置io.file.buffer.size

      注意：如果指定了SPARK_YARN_MODE属性，则会使用YarnSparkHadoopUtil，否则默认为SparkHadoopUtil.
     */
    _hadoopConfiguration = SparkHadoopUtil.get.newConfiguration(_conf)

    // Add each JAR given through the constructor
    // 通过构造函数添加每个JAR
    if (jars != null) {
      jars.foreach(addJar)
    }

    if (files != null) {
      files.foreach(addFile)
    }

    /**
      * 优化：
      * Master给Worker发送调度后，Worker最终使用executorEnvs提供的信息启动Executor.可以通过spark.executor.memory
      * 指定Executor占用的内存大小，也可以配置系统变量SPARK_EXECUTOR_MEMORY或者SPARK_MEM对其大小进行设置。
      *  默认值为0.6，官方文档建议这个比值不要超过JVM Old Gen区域的比值，因为RDD Cache数据通常都是长期驻留在内存的，也就是说
      * 最终会被转移到Old Gen区域（如果该Rdd还没被删除的的话），如果这部分的数据允许的尺寸太大，势必把Old Gen区域占满，
      * 造成频繁的圈梁的垃圾回收。
      *
      * 如何调整这个值，取决于你的应用对数据的使用模式和数据的规模，粗略的来说，如果频繁发生全量的垃圾回收，可以考虑降低这个
      * 值，这样RDD Cache可用的内存空间就会减少（剩下的部分Cache数据就需要通过Disk Store写到磁盘上了），虽然会带来一定的
      * 性能损失，但是腾出来更多的内存空间用于执行任务，减少全量的垃圾回收发生的次数，反而可能改善程序运行的整体性能。
      *
      */
    _executorMemory = _conf.getOption("spark.executor.memory")
      .orElse(Option(System.getenv("SPARK_EXECUTOR_MEMORY")))
      .orElse(Option(System.getenv("SPARK_MEM"))
      .map(warnSparkMem))
      .map(Utils.memoryStringToMb)
      .getOrElse(1024)

    // Convert java options to env vars as a work around
    // since we can't set env vars directly in sbt.
    // 将java选项env变量作为工作aroundsince我们不能设置环境变量直接在SBT。
    for { (envKey, propKey) <- Seq(("SPARK_TESTING", "spark.testing"))
      value <- Option(System.getenv(envKey)).orElse(Option(System.getProperty(propKey)))} {
      executorEnvs(envKey) = value
    }
    Option(System.getenv("SPARK_PREPEND_CLASSES")).foreach { v =>
      executorEnvs("SPARK_PREPEND_CLASSES") = v
    }
    // The Mesos scheduler backend relies on this environment variable to set executor memory.
    // 使用Mesos调度器的后端依赖于此环境变量设置executor的memory。
    // TODO: Set this only in the Mesos scheduler.
    executorEnvs("SPARK_EXECUTOR_MEMORY") = executorMemory + "m"
    executorEnvs ++= _conf.getExecutorEnv
    executorEnvs("SPARK_USER") = sparkUser

    // We need to register "HeartbeatReceiver" before "createTaskScheduler" because Executor will
    // retrieve "HeartbeatReceiver" in the constructor. (SPARK-6640)
    // 我们需要登记”heartbeatreceiver”在“createtaskscheduler“之前，
    // 因为Executor将检索“heartbeatreceiver”在构造函数。（spark-6640）

    /**
      *  rpcEnv是一个抽象类
      *  env.rpcEnv.setupEndpoint实际调用的是NettyRpcEnv的setupEndpoint方法
      *  driver端接收executor的心跳
      */
    _heartbeatReceiver = env.rpcEnv.setupEndpoint(
      HeartbeatReceiver.ENDPOINT_NAME, new HeartbeatReceiver(this))


    /**
      * Create and start the scheduler  创建和启动任务调度
      * 创建SparkDeployScheduler和TaskSchedulerImpl
      *
      * 这里才是真正创建
      * schedulerBackend
      * taskScheduler
      * dagScheduler
      *
      * createTaskScheduler方法会根据master的配置陪陪部署模式，创建TaskSchedulerImpl，并且生成不同的SchedulerBackend。
      * */
    val (sched, ts) = SparkContext.createTaskScheduler(this, master, deployMode)
    _schedulerBackend = sched   //生成 schedulerBackend
    _taskScheduler = ts  //生成 taskScheduler


    /** 创建DAGScheduler */
    _dagScheduler = new DAGScheduler(this)

    // 这一句代码是什么意思呢？ask方法是=》这个方法只发送一次消息，从不重试。上面代码有初始化_heartbeatReceiver的
    // TaskSchedulerIsSet仅仅是一个对象，表示一个TaskSchedulerIs设置事件，由HeartbeatReceiver类中的receiveAndReply方法处理
    // 这句话整体就是把_taskScheduler的值赋值给taskScheduler
    _heartbeatReceiver.ask[Boolean](TaskSchedulerIsSet)

    // start TaskScheduler after taskScheduler sets DAGScheduler reference in DAGScheduler's
    // constructor
    // 启动TaskScheduler在taskScheduler设置DAGScheduler的DAGScheduler的构造函数之后
    // 任务调度器TaskScheduler的创建，想要TaskScheduler发挥作用，必须启动它。
    /**
      * 启动taskScheduler，调用的是TaskSchedulerImpl的start()方法
      * 也会调用backend的start方法
      * */
    _taskScheduler.start()

    _applicationId = _taskScheduler.applicationId()
    _applicationAttemptId = taskScheduler.applicationAttemptId()
    _conf.set("spark.app.id", _applicationId)

    // sparkUI相关：这里设置了spark.ui.proxyBase，就是yar模式下uiRoot参数
    if (_conf.getBoolean("spark.ui.reverseProxy", false)) {
      System.setProperty("spark.ui.proxyBase", "/proxy/" + _applicationId)
    }
    _ui.foreach(_.setAppId(_applicationId))

    /** BlockManager的初始化 */
    _env.blockManager.initialize(_applicationId)


    /**
      * // The metrics system for Driver need to be set spark.app.id to app ID.
      * // So it should start after we get app ID from the task scheduler and set spark.app.id.
      * // Driver的度量系统需要为spark.app.id设置一个APP ID
      * 启动测量系统MetricsSyatem
      *
      * */
    _env.metricsSystem.start()
    // Attach the driver metrics servlet handler to the web ui after the metrics system is started.
    // 在度量系统启动后将驱动程序servlet处理程序附加到Web用户界面。
    _env.metricsSystem.getServletHandlers.foreach(handler => ui.foreach(_.attachHandler(handler)))


    /**
      * 启动eventLog，因为isEventLogEnabled默认为false，所以这个默认是不启动的
      */
    _eventLogger =
      if (isEventLogEnabled) {
        val logger =
          // 创建一个EventLoggingListener对象
          new EventLoggingListener(_applicationId, _applicationAttemptId, _eventLogDir.get,
            _conf, _hadoopConfiguration)
        // 调用Start方法
        logger.start()
        // 加入到监听Bus
        listenerBus.addListener(logger)
        Some(logger)
      } else {
        None
      }

    // Optionally scale number of executors dynamically based on workload. Exposed for testing.
    /**
      * dynamicAllocationEnabled用于对已经分配的Executor进行管理，创建和启动ExecutorAllocationManager。
      *
      * 返回在给定的conf中是否启用了动态分配。默认是没有启动的 false
      * 设置spark.dynamicAllocation.enabled为true 可以启动动态分配
      */
    val dynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(_conf)
    _executorAllocationManager =
      if (dynamicAllocationEnabled) {
        // 如果是local模式下，这个内容为_schedulerBackend: LocalSchedulerBackend 无法进行动态分配
        schedulerBackend match {
          case b: ExecutorAllocationClient =>
            /** 新建一个ExecutorAllocationManager 动态分配executor*/
            Some(new ExecutorAllocationManager(
              schedulerBackend.asInstanceOf[ExecutorAllocationClient], listenerBus, _conf))
          case _ =>
            None
        }
      } else {
        None
      }
    // 调用start方法
    _executorAllocationManager.foreach(_.start())




    /**  启用Spark的ContextCleaner 用于清理功能 */
    _cleaner =
      if (_conf.getBoolean("spark.cleaner.referenceTracking", true)) {
        Some(new ContextCleaner(this))
      } else {
        None
      }
    // 调用start方法
    _cleaner.foreach(_.start())





    /**
      * z这个方法主要执行每个监听器的静态代码块，启动监听总线listenBus
      */
    setupAndStartListenerBus()

    /**
      * 在SparkContext的初始化过程中，可能对其环境造成影响，所以需要更新环境。就是提交代码后，如果更新了环境
      */
    postEnvironmentUpdate()

    /**  发布应用程序启动事件 */
    postApplicationStart()

    // Post init  等待SchedulerBackend准备好，在创建DAGSchedulerSource,BlockManagerSource之前首先调用taskScheduler的postStartHook方法，
    // 其目的是为了等待backend就绪。
    _taskScheduler.postStartHook()
    // DAGSchedulerSource的测量信息是job和Satge相关的信息
    _env.metricsSystem.registerSource(_dagScheduler.metricsSource)
    // 注册BlockManagerSource
    _env.metricsSystem.registerSource(new BlockManagerSource(_env.blockManager))
    // 动态分配的executor信息
    _executorAllocationManager.foreach { e =>
      _env.metricsSystem.registerSource(e.executorAllocationManagerSource)
    }

    // Make sure the context is stopped if the user forgets about it. This avoids leaving
    // unfinished event logs around after the JVM exits cleanly. It doesn't help if the JVM
    // is killed, though.
    // 确保context是停止如果用户忘记了停止。这样可以避免在JVM退出之后留下未完成的事件日志。但是如果JVM是被杀了并不能帮助，
    logDebug("Adding shutdown hook") // force eager creation of logger


    /** ShutdownHookManager的创建，为了在Spark程序挂掉的时候，处理一些清理工作  */
    _shutdownHookRef = ShutdownHookManager.addShutdownHook(
      ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY) { () =>
      logInfo("Invoking stop() from shutdown hook")
      // 这调用停止方法。关闭SparkContext，我就搞不懂了
      stop()
    }


  } catch {
    case NonFatal(e) =>
      logError("Error initializing SparkContext.", e)
      try {
        stop()
      } catch {
        case NonFatal(inner) =>
          logError("Error stopping SparkContext after init error.", inner)
      } finally {
        throw e
      }
  }

  /**
   * Called by the web UI to obtain executor thread dumps.  This method may be expensive.
   * Logs an error and returns None if we failed to obtain a thread dump, which could occur due
   * to an executor being dead or unresponsive or due to network issues while sending the thread
   * dump message back to the driver.
   */
  // 由Web UI调用以获得执行器线程转储。这种方法可以是昂贵的。记录错误并返回None如果我们未能获得一个线程转储，
  // 这可能发生由于executor死亡或反应迟钝或因网络问题而发送线程转储信息返回给driver。
  private[spark] def getExecutorThreadDump(executorId: String): Option[Array[ThreadStackTrace]] = {
    try {
      if (executorId == SparkContext.DRIVER_IDENTIFIER) {
        Some(Utils.getThreadDump())
      } else {
        val endpointRef = env.blockManager.master.getExecutorEndpointRef(executorId).get
        Some(endpointRef.askSync[Array[ThreadStackTrace]](TriggerThreadDump))
      }
    } catch {
      case e: Exception =>
        logError(s"Exception getting thread dump from executor $executorId", e)
        None
    }
  }

  private[spark] def getLocalProperties: Properties = localProperties.get()

  private[spark] def setLocalProperties(props: Properties) {
    localProperties.set(props)
  }

  /**
   * Set a local property that affects jobs submitted from this thread, such as the Spark fair
   * scheduler pool. User-defined properties may also be set here. These properties are propagated
   * through to worker tasks and can be accessed there via
   * [[org.apache.spark.TaskContext#getLocalProperty]].
   *
   * These properties are inherited by child threads spawned from this thread. This
   * may have unexpected consequences when working with thread pools. The standard java
   * implementation of thread pools have worker threads spawn other worker threads.
   * As a result, local properties may propagate unpredictably.
    *
    * 设置一个本地属性影响此线程提交的作业，如Spark火公平调度程序池。用户定义的属性也可以在这里设置。
    * 这些属性传播到worker的任务，可以通过【[org.apache.spark.TaskContext#getLocalProperty] ]访问。
    *
    * 这些性质都是由孩子从这个线程的线程继承。当使用线程池时，这可能会带来意想不到的后果。
    * 线程池线程标准java实现产生其他工作线程。作为一个结果，局部变量可能传播不可预知的。
    *
   */
  def setLocalProperty(key: String, value: String) {
    if (value == null) {
      localProperties.get.remove(key)
    } else {
      localProperties.get.setProperty(key, value)
    }
  }

  /**
   * Get a local property set in this thread, or null if it is missing. See
   * `org.apache.spark.SparkContext.setLocalProperty`.
    * 获取此线程中的本地属性集，如果缺少该属性，则为null。`org.apache.spark.SparkContext.setLocalProperty`.
   */
  def getLocalProperty(key: String): String =
    Option(localProperties.get).map(_.getProperty(key)).orNull

  /** Set a human readable description of the current job.
    * 设置当前job人类可读的描述。
    * */
  def setJobDescription(value: String) {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, value)
  }

  /**
   * Assigns a group ID to all the jobs started by this thread until the group ID is set to a
   * different value or cleared.
   * 将组ID分配给该线程启动的所有作业，直到组ID被设置为不同的值或清除为止。
    *
   * Often, a unit of execution in an application consists of multiple Spark actions or jobs.
   * Application programmers can use this method to group all those jobs together and give a
   * group description. Once set, the Spark web UI will associate such jobs with this group.
    * 通常，应用程序中的执行单元由多个触发动作或作业组成。
    * 应用程序程序员可以使用这种方法将所有这些工作组合在一起，并给出一个组描述。
    * 一旦设置，Spark Web UI将把这样的作业与这个组相关联。
   *
   * The application can also use `org.apache.spark.SparkContext.cancelJobGroup` to cancel all
   * running jobs in this group. For example,
   * {{{
   * // In the main thread:
   * sc.setJobGroup("some_job_to_cancel", "some job description")
   * sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.count()
   *  应用程序可以使用`org.apache.spark.SparkContext.cancelJobGroup` 去取消运行在该group的jobs 停止运行，例如
    *
   * // In a separate thread:
   * sc.cancelJobGroup("some_job_to_cancel")
   * }}}
   *
   * @param interruptOnCancel If true, then job cancellation will result in `Thread.interrupt()`
   * being called on the job's executor threads. This is useful to help ensure that the tasks
   * are actually stopped in a timely manner, but is off by default due to HDFS-1208, where HDFS
   * may respond to Thread.interrupt() by marking nodes as dead.
    *
    * 如果是真的，那么job取消将导致`Thread.interrupt()` 这将被job's executor 调用。
    * 这是为了确保tasks能够及时停止是有用，但是由于hdfs-1208默认，在HDFS可以响应Thread.interrupt() to 标记节点已经死了。
   */
  def setJobGroup(groupId: String, description: String, interruptOnCancel: Boolean = false) {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, description)
    setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, groupId)
    // Note: Specifying interruptOnCancel in setJobGroup (rather than cancelJobGroup) avoids
    // changing several public APIs and allows Spark cancellations outside of the cancelJobGroup
    // APIs to also take advantage of this property (e.g., internal job failures or canceling from
    // JobProgressTab UI) on a per-job basis.

    // 注：在setjobgroup指定interruptoncancel（而不是canceljobgroup）
    // 避免改变几个公共API和允许Spark取消的canceljobgroupz之外API也利用这个特性
    // （例如，内部job的失败或取消从jobprogresstab UI）每一个工作基础上。
    setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, interruptOnCancel.toString)
  }

  /** Clear the current thread's job group ID and its description.
    * 清除当前线程的工作组ID及其描述。
    * */
  def clearJobGroup() {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, null)
    setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, null)
    setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, null)
  }

  /**
   * Execute a block of code in a scope such that all new RDDs created in this body will
   * be part of the same scope. For more detail, see {{org.apache.spark.rdd.RDDOperationScope}}.
    *
    * 在一个范围，在这个身体中创建的所有新的RDDS将相同的范围的一部分执行的代码块。
    * 更多的细节，看到{ { org.apache.spark.rdd.RDDOperationScope } }。
   *
   * @note Return statements are NOT allowed in the given body. 返回语句中不允许给定的身体。
   */
  private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](this)(body)

  // Methods for creating RDDs

  /** Distribute a local Scala collection to form an RDD. 分配本地Scala收集形成一个RDD。
   *
   * @note Parallelize acts lazily. If `seq` is a mutable collection and is altered after the call
   * to parallelize and before the first action on the RDD, the resultant RDD will reflect the
   * modified collection. Pass a copy of the argument to avoid this.
   * @note avoid using `parallelize(Seq())` to create an empty `RDD`. Consider `emptyRDD` for an
   * RDD with no partitions, or `parallelize(Seq[T]())` for an RDD of `T` with empty partitions.
   * @param seq Scala collection to distribute
   * @param numSlices number of partitions to divide the collection into
   * @return RDD representing distributed collection
    *
    *   注意Parallelize并行化行为是懒加载的。如果` SEQ `是一个可变的收集和在parallelize之后的并行化和
    *            RDD第一个action之前改变，所得的RDD将反映修改的collection。传递参数的argument以避免这一点。
    *   注意避免使用`parallelize(Seq())`创建一个空的` RDD `。考虑'emptyRDD `是一个没有分区的RDD，
    *                         或`parallelize(Seq[T]())`为` T `没有分区。
    *   param seq Scala并行计算集合
    *   参数分区 并行计算的分区数
    *   返回RDD代表分布式集合
   */
  def parallelize[T: ClassTag](
      seq: Seq[T],
      numSlices: Int = defaultParallelism): RDD[T] = withScope {
    assertNotStopped()
    new ParallelCollectionRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
  }

  /**
   * Creates a new RDD[Long] containing elements from `start` to `end`(exclusive), increased by
   * `step` every element.
    * 创建一个新的RDD [Long]从`start`含有元素`end`（独家），每个element增加了`step（步）`。
   *
   * @note if we need to cache this RDD, we should make sure each partition does not exceed limit.
   *        如果我们需要缓存RDD，我们应该确保每个分区不超过极限。
   * @param start the start value.
   * @param end the end value.
   * @param step the incremental step  增加的步伐
   * @param numSlices number of partitions to divide the collection into 将收集到的分区数
   * @return RDD representing distributed range  RDD  distributed 分布范围
   */
  def range(
      start: Long,
      end: Long,
      step: Long = 1,
      numSlices: Int = defaultParallelism): RDD[Long] = withScope {
    assertNotStopped()
    // when step is 0, range will run infinitely  当步骤为0时，范围将无限地运行。
    require(step != 0, "step cannot be 0")
    val numElements: BigInt = {
      val safeStart = BigInt(start)
      val safeEnd = BigInt(end)
      if ((safeEnd - safeStart) % step == 0 || (safeEnd > safeStart) != (step > 0)) {
        (safeEnd - safeStart) / step
      } else {
        // the remainder has the same sign with range, could add 1 more
        // 其余的符号与范围相同，可以加1个。
        (safeEnd - safeStart) / step + 1
      }
    }

    parallelize(0 until numSlices, numSlices).mapPartitionsWithIndex { (i, _) =>
      val partitionStart = (i * numElements) / numSlices * step + start
      val partitionEnd = (((i + 1) * numElements) / numSlices) * step + start
      def getSafeMargin(bi: BigInt): Long =
        if (bi.isValidLong) {
          bi.toLong
        } else if (bi > 0) {
          Long.MaxValue
        } else {
          Long.MinValue
        }
      val safePartitionStart = getSafeMargin(partitionStart)
      val safePartitionEnd = getSafeMargin(partitionEnd)

      new Iterator[Long] {
        private[this] var number: Long = safePartitionStart
        private[this] var overflow: Boolean = false

        override def hasNext =
          if (!overflow) {
            if (step > 0) {
              number < safePartitionEnd
            } else {
              number > safePartitionEnd
            }
          } else false

        override def next() = {
          val ret = number
          number += step
          if (number < ret ^ step < 0) {
            // we have Long.MaxValue + Long.MaxValue < Long.MaxValue
            // and Long.MinValue + Long.MinValue > Long.MinValue, so iff the step causes a step
            // back, we are pretty sure that we have an overflow.
            // 我们long.maxvalue + long.maxvalue < long.maxvalue和long.minvalue + long.minvalue > long.minvalue，
            // 所以敌我识别的步骤使得一步，我们确信，我们有一个溢出。
            overflow = true
          }
          ret
        }
      }
    }
  }

  /** Distribute a local Scala collection to form an RDD.
    * 分配本地Scala collection形成一个RDD。
   *
   * This method is identical to `parallelize`. 这个的方法和`parallelize`相同。
   * @param seq Scala collection to distribute  Scala 并行计算collection
   * @param numSlices number of partitions to divide the collection into   并行计算分配的分区数
   * @return RDD representing distributed collection  RDD代表分布式并行计算集合
   */
  def makeRDD[T: ClassTag](
      seq: Seq[T],
      numSlices: Int = defaultParallelism): RDD[T] = withScope {
    parallelize(seq, numSlices)
  }

  /**
   * Distribute a local Scala collection to form an RDD, with one or more
   * location preferences (hostnames of Spark nodes) for each object.
   * Create a new partition for each collection item.
    * 分配本地Scala集合形成一个RDD，与一个或多个地点（火花节点主机名）为每个对象创建每个集合项的一个新的分区
    *
   * @param seq list of tuples of data and location preferences (hostnames of Spark nodes)
    *            数据和位置元组列表（hostnames of Spark nodes）
   * @return RDD representing data partitioned according to location preferences
   */
  def makeRDD[T: ClassTag](seq: Seq[(T, Seq[String])]): RDD[T] = withScope {
    assertNotStopped()
    val indexToPrefs = seq.zipWithIndex.map(t => (t._2, t._1._2)).toMap
    new ParallelCollectionRDD[T](this, seq.map(_._1), math.max(seq.size, 1), indexToPrefs)
  }

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
    * 从HDFS读取一个文本文件，本地文件系统（所有节点上可用），或任何Hadoop支持的文件系统中，并返回为一个字符串RDD。
    *
   * @param path path to the text file on a supported file system
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return RDD of lines of the text file
   */
  def textFile(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
    // 判断sparkContext是否已经停止了
    assertNotStopped()
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
  }

  /**
   * Read a directory of text files from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI. Each file is read as a single record and returned in a
   * key-value pair, where the key is the path of each file, the value is the content of each file.
    *
    * 读目录从HDFS的文件，本地文件系统（所有节点上可用），或任何Hadoop支持的文件系统的URI。
    * 每个文件被读取为单个记录，并返回一个键值对，其中键是每个文件的路径，该值是每个文件的内容。
    *
    * 就是可以直接读取一个文件夹的内容
   *
   * <p> For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do `val rdd = sparkContext.wholeTextFile("hdfs://a-hdfs-path")`,
   *
   * <p> then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *
   * @note Small files are preferred, large file is also allowable, but may cause bad performance.
   * @note On some filesystems, `.../path/&#42;` can be a more efficient way to read all files
   *       in a directory rather than `.../path/` or `.../path`
   * @note Partitioning is determined by data locality. This may result in too few partitions
   *       by default.
   *
   * @param path Directory to the input data files, the path can be comma separated paths as the
   *             list of inputs.
   * @param minPartitions A suggestion value of the minimal splitting number for input data.
   * @return RDD representing tuples of file path and the corresponding file content
   */
  def wholeTextFiles(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[(String, String)] = withScope {
    assertNotStopped()
    val job = NewHadoopJob.getInstance(hadoopConfiguration)
    // Use setInputPaths so that wholeTextFiles aligns with hadoopFile/textFile in taking
    // comma separated files as input. (see SPARK-7155)
    NewFileInputFormat.setInputPaths(job, path)
    val updateConf = job.getConfiguration
    new WholeTextFileRDD(
      this,
      classOf[WholeTextFileInputFormat],
      classOf[Text],
      classOf[Text],
      updateConf,
      minPartitions).map(record => (record._1.toString, record._2.toString)).setName(path)
  }

  /**
   * Get an RDD for a Hadoop-readable dataset as PortableDataStream for each file
   * (useful for binary data)
   * 得到一个Hadoop可读的数据为每个文件portabledatastream RDD（对二进制数据有用）
    *
   * For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do
   * `val rdd = sparkContext.binaryFiles("hdfs://a-hdfs-path")`,
   *
   * then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *
   * @note Small files are preferred; very large files may cause bad performance.
    *       小文件是首选的；非常大的文件可能会导致坏的性能。
   * @note On some filesystems, `.../path/&#42;` can be a more efficient way to read all files
   *       in a directory rather than `.../path/` or `.../path`
    *      在一些文件系统，`…/路径/ # 42；`可以更有效的方式来读取所有文件在一个目录而不是`…/路径/ `或`…/路径`
   * @note Partitioning is determined by data locality. This may result in too few partitions
   *       by default.
    *      分区的数据局部性。这可能会导致分区的默认太少。
   *
   * @param path Directory to the input data files, the path can be comma separated paths as the
   *             list of inputs.
   * @param minPartitions A suggestion value of the minimal splitting number for input data.
   * @return RDD representing tuples of file path and corresponding file content
   */
  def binaryFiles(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[(String, PortableDataStream)] = withScope {
    assertNotStopped()
    val job = NewHadoopJob.getInstance(hadoopConfiguration)
    // Use setInputPaths so that binaryFiles aligns with hadoopFile/textFile in taking
    // comma separated files as input. (see SPARK-7155)
    NewFileInputFormat.setInputPaths(job, path)
    val updateConf = job.getConfiguration
    new BinaryFileRDD(
      this,
      classOf[StreamInputFormat],
      classOf[String],
      classOf[PortableDataStream],
      updateConf,
      minPartitions).setName(path)
  }

  /**
   * Load data from a flat binary file, assuming the length of each record is constant.
    * 假设每个记录的长度是恒定的，则从一个record二进制文件加载数据。
   *
   * @note We ensure that the byte array for each record in the resulting RDD
   * has the provided record length.
   *
   * @param path Directory to the input data files, the path can be comma separated paths as the
   *             list of inputs.
   * @param recordLength The length at which to split the records
   * @param conf Configuration for setting up the dataset.
   *
   * @return An RDD of data with values, represented as byte arrays
   */
  def binaryRecords(
      path: String,
      recordLength: Int,
      conf: Configuration = hadoopConfiguration): RDD[Array[Byte]] = withScope {
    assertNotStopped()
    conf.setInt(FixedLengthBinaryInputFormat.RECORD_LENGTH_PROPERTY, recordLength)
    val br = newAPIHadoopFile[LongWritable, BytesWritable, FixedLengthBinaryInputFormat](path,
      classOf[FixedLengthBinaryInputFormat],
      classOf[LongWritable],
      classOf[BytesWritable],
      conf = conf)
    br.map { case (k, v) =>
      val bytes = v.copyBytes()
      assert(bytes.length == recordLength, "Byte array does not have correct length")
      bytes
    }
  }

  /**
   * Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf given its InputFormat and other
   * necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable),
   * using the older MapReduce API (`org.apache.hadoop.mapred`).
    *
    * 从Hadoop Hadoop可读的数据dataset从一个Hadoop JobConf 给他的输入格式和其他需要的信息
    * （如文件名为基于文件系统的数据集，Hypertable表名称），使用旧的MapReduce API（` org。Apache Hadoop。mapred `）。
   *
   * @param conf JobConf for setting up the dataset. Note: This will be put into a Broadcast.
   *             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
   *             sure you won't modify the conf. A safe approach is always creating a new conf for
   *             a new RDD.
    *             JobConf设置建立数据集。注意：这将被放进广播中。因此，如果你打算使用这个配置创建多个RDDs，
    *             你需要确保你不会修改设置，一个安全的方法是创建一个新的RDD新配置。
   * @param inputFormatClass storage format of the data to be read
    *                         要读取的数据的存储格式
   * @param keyClass `Class` of the key associated with the `inputFormatClass` parameter
    *                `Class`的key与` inputformatclass `参数
   * @param valueClass `Class` of the value associated with the `inputFormatClass` parameter
   * @param minPartitions Minimum number of Hadoop Splits to generate.
    *                      要生成的Hadoop拆分的最小数目。
   * @return RDD of tuples of key and corresponding value
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
    *  由于Hadoop的RecordReader类重新使用相同的可写的对象为每个记录，
    *  直接缓存返回的RDD或直接传递到聚集或洗牌操作会产生很多引用相同的对象。如果您计划直接缓存、排序或聚合Hadoop可写对象，
    *  您应该首先使用“map”函数复制它们。
   */
  def hadoopRDD[K, V](
      conf: JobConf,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope {
    assertNotStopped()

    // This is a hack to enforce loading hdfs-site.xml.
    // See SPARK-11227 for details.
    // 这是一个黑客执行细节hdfs-site.xml.see spark-11227加载。
    FileSystem.getLocal(conf)

    // Add necessary security credentials to the JobConf before broadcasting it.
    // 在光播前就到jobconf添加必要的安全凭据。
    SparkHadoopUtil.get.addCredentials(conf)
    new HadoopRDD(this, conf, inputFormatClass, keyClass, valueClass, minPartitions)
  }

  /** Get an RDD for a Hadoop file with an arbitrary InputFormat
    *  得到任意一个InputFormat Hadoop文件RDD
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
    *     由于Hadoop的RecordReader类重新使用相同的可写的对象为每个记录，直接缓存返回的RDD
    *     或直接传递到聚集或洗牌操作会产生很多引用相同的对象。
    *     如果您计划直接缓存、排序或聚合Hadoop可写对象，您应该首先使用“map”函数复制它们。
    *
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @param inputFormatClass storage format of the data to be read
   * @param keyClass `Class` of the key associated with the `inputFormatClass` parameter
   * @param valueClass `Class` of the value associated with the `inputFormatClass` parameter
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return RDD of tuples of key and corresponding value
   */
  def hadoopFile[K, V](
      path: String,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope {
    assertNotStopped()

    // This is a hack to enforce loading hdfs-site.xml.
    // See SPARK-11227 for details.
    FileSystem.getLocal(hadoopConfiguration)

    // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
    // 将Hadoop的Configuration封装为SerializeableWritable用于序列化读写操作，然后广播Hadoop的Configuration。
    // hadoop的Configuration通常只有10KB,所以不会对性能产生影响。所以广播这个配置。
    val confBroadcast = broadcast(new SerializableConfiguration(hadoopConfiguration))
    // 定义偏函数用于以后设置输入路径。
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
    // 构建HadoopRDD
    new HadoopRDD(
      this,
      confBroadcast,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions).setName(path)
  }

  /**
   * Smarter version of hadoopFile() that uses class tags to figure out the classes of keys,
   * values and the InputFormat so that users don't need to pass them directly. Instead, callers
   * can just write, for example,
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path, minPartitions)
   * }}}
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return RDD of tuples of key and corresponding value
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]]
      (path: String, minPartitions: Int)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = withScope {
    hadoopFile(path,
      fm.runtimeClass.asInstanceOf[Class[F]],
      km.runtimeClass.asInstanceOf[Class[K]],
      vm.runtimeClass.asInstanceOf[Class[V]],
      minPartitions)
  }

  /**
   * Smarter version of hadoopFile() that uses class tags to figure out the classes of keys,
   * values and the InputFormat so that users don't need to pass them directly. Instead, callers
   * can just write, for example,
    *
    *  聪明的版本hadoopfile()使用类标签找出class的keys，values和InputFormat所以用户不需要直接通过他们。
    *  相反，用户就可以写，例如，
    *
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path)
   * }}}
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
    *
    *       由于Hadoop的RecordReader类重新使用相同的可写的对象为每个记录，
    *       直接缓存返回的RDD或直接传递到聚集或洗牌操作会产生很多引用相同的对象
    *       。如果您计划直接缓存、排序或聚合Hadoop可写对象，您应该首先使用“map”函数复制它们。
    *
   * @param path directory to the input data files, the path can be comma separated paths as
   * a list of inputs
   * @return RDD of tuples of key and corresponding value
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](path: String)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = withScope {
    hadoopFile[K, V, F](path, defaultMinPartitions)
  }

  /**
   * Smarter version of `newApiHadoopFile` that uses class tags to figure out the classes of keys,
   * values and the `org.apache.hadoop.mapreduce.InputFormat` (new MapReduce API) so that user
   * don't need to pass them directly. Instead, callers can just write, for example:
   * ```
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path)
   * ```
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @return RDD of tuples of key and corresponding value
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]]
      (path: String)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = withScope {
    newAPIHadoopFile(
      path,
      fm.runtimeClass.asInstanceOf[Class[F]],
      km.runtimeClass.asInstanceOf[Class[K]],
      vm.runtimeClass.asInstanceOf[Class[V]])
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @param fClass storage format of the data to be read
   * @param kClass `Class` of the key associated with the `fClass` parameter
   * @param vClass `Class` of the value associated with the `fClass` parameter
   * @param conf Hadoop configuration
   * @return RDD of tuples of key and corresponding value
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
      path: String,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V],
      conf: Configuration = hadoopConfiguration): RDD[(K, V)] = withScope {
    assertNotStopped()

    // This is a hack to enforce loading hdfs-site.xml.
    // See SPARK-11227 for details.
    FileSystem.getLocal(hadoopConfiguration)

    // The call to NewHadoopJob automatically adds security credentials to conf,
    // so we don't need to explicitly add them ourselves
    val job = NewHadoopJob.getInstance(conf)
    // Use setInputPaths so that newAPIHadoopFile aligns with hadoopFile/textFile in taking
    // comma separated files as input. (see SPARK-7155)
    NewFileInputFormat.setInputPaths(job, path)
    val updatedConf = job.getConfiguration
    new NewHadoopRDD(this, fClass, kClass, vClass, updatedConf).setName(path)
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
    *
    *   在给定的任意新的API InputFormat和额外的配置选项传递给输入格式Hadoop文件RDD。
   *
   * @param conf Configuration for setting up the dataset. Note: This will be put into a Broadcast.
   *             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
   *             sure you won't modify the conf. A safe approach is always creating a new conf for
   *             a new RDD.
   * @param fClass storage format of the data to be read
   * @param kClass `Class` of the key associated with the `fClass` parameter
   * @param vClass `Class` of the value associated with the `fClass` parameter
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   */
  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
      conf: Configuration = hadoopConfiguration,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V]): RDD[(K, V)] = withScope {
    assertNotStopped()

    // This is a hack to enforce loading hdfs-site.xml.
    // See SPARK-11227 for details.
    FileSystem.getLocal(conf)

    // Add necessary security credentials to the JobConf. Required to access secure HDFS.
    val jconf = new JobConf(conf)
    SparkHadoopUtil.get.addCredentials(jconf)
    new NewHadoopRDD(this, fClass, kClass, vClass, jconf)
  }

  /**
   * Get an RDD for a Hadoop SequenceFile with given key and value types.
    *
    *   得到一个RDD为Hadoop SequenceFile与给定的键和值的类型。
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @param keyClass `Class` of the key associated with `SequenceFileInputFormat`
   * @param valueClass `Class` of the value associated with `SequenceFileInputFormat`
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return RDD of tuples of key and corresponding value
   */
  def sequenceFile[K, V](path: String,
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int
      ): RDD[(K, V)] = withScope {
    assertNotStopped()
    val inputFormatClass = classOf[SequenceFileInputFormat[K, V]]
    hadoopFile(path, inputFormatClass, keyClass, valueClass, minPartitions)
  }

  /**
   * Get an RDD for a Hadoop SequenceFile with given key and value types.
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @param keyClass `Class` of the key associated with `SequenceFileInputFormat`
   * @param valueClass `Class` of the value associated with `SequenceFileInputFormat`
   * @return RDD of tuples of key and corresponding value
   */
  def sequenceFile[K, V](
      path: String,
      keyClass: Class[K],
      valueClass: Class[V]): RDD[(K, V)] = withScope {
    assertNotStopped()
    sequenceFile(path, keyClass, valueClass, defaultMinPartitions)
  }

  /**
   * Version of sequenceFile() for types implicitly convertible to Writables through a
   * WritableConverter. For example, to access a SequenceFile where the keys are Text and the
   * values are IntWritable, you could simply write
   * {{{
   * sparkContext.sequenceFile[String, Int](path, ...)
   * }}}
   *
   * WritableConverters are provided in a somewhat strange way (by an implicit function) to support
   * both subclasses of Writable and types for which we define a converter (e.g. Int to
   * IntWritable). The most natural thing would've been to have implicit objects for the
   * converters, but then we couldn't have an object for every subclass of Writable (you can't
   * have a parameterized singleton object). We use functions instead to create a new converter
   * for the appropriate type. In addition, we pass the converter a ClassTag of its type to
   * allow it to figure out the Writable class to use in the subclass case.
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return RDD of tuples of key and corresponding value
   */
   def sequenceFile[K, V]
       (path: String, minPartitions: Int = defaultMinPartitions)
       (implicit km: ClassTag[K], vm: ClassTag[V],
        kcf: () => WritableConverter[K], vcf: () => WritableConverter[V]): RDD[(K, V)] = {
    withScope {
      assertNotStopped()
      val kc = clean(kcf)()
      val vc = clean(vcf)()
      val format = classOf[SequenceFileInputFormat[Writable, Writable]]
      val writables = hadoopFile(path, format,
        kc.writableClass(km).asInstanceOf[Class[Writable]],
        vc.writableClass(vm).asInstanceOf[Class[Writable]], minPartitions)
      writables.map { case (k, v) => (kc.convert(k), vc.convert(v)) }
    }
  }

  /**
   * Load an RDD saved as a SequenceFile containing serialized objects, with NullWritable keys and
   * BytesWritable values that contain a serialized partition. This is still an experimental
   * storage format and may not be supported exactly as is in future Spark releases. It will also
   * be pretty slow if you use the default serializer (Java serialization),
   * though the nice thing about it is that there's very little effort required to save arbitrary
   * objects.
   *
    * 加载一个RDD保存为SequenceFile包含序列化的对象，与nullwritable键和byteswritable值包含序列化的分区。
    * 这仍然是一种实验性的存储格式，可能不会像将来的Spark 版本那样得到支持。这也将是相当缓慢，如果使用默认的序列化程序
    * （java序列化），但好的事情是有一点小小的努力，需要保存任意对象。
    *
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
    *
    *   输入数据文件的目录，路径可以以逗号分隔的路径作为输入的列表。
    *
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
    *                      建议的最低数量的分区结果RDD
    *
   * @return RDD representing deserialized data from the file(s)
    *         RDD代表反序列化的数据从文件（S）
   */
  def objectFile[T: ClassTag](
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[T] = withScope {
    assertNotStopped()
    sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
      .flatMap(x => Utils.deserialize[Array[T]](x._2.getBytes, Utils.getContextOrSparkClassLoader))
  }

  protected[spark] def checkpointFile[T: ClassTag](path: String): RDD[T] = withScope {
    new ReliableCheckpointRDD[T](this, path)
  }

  /** Build the union of a list of RDDs.
    * 建立一个列表的RDDS联盟。（连接许多RDDS）
    * */
  def union[T: ClassTag](rdds: Seq[RDD[T]]): RDD[T] = withScope {
    val partitioners = rdds.flatMap(_.partitioner).toSet
    if (rdds.forall(_.partitioner.isDefined) && partitioners.size == 1) {
      new PartitionerAwareUnionRDD(this, rdds)
    } else {
      new UnionRDD(this, rdds)
    }
  }

  /** Build the union of a list of RDDs passed as variable-length arguments.
    * 建立一个列表是可变长度参数传递RDDS联盟。
    * */
  def union[T: ClassTag](first: RDD[T], rest: RDD[T]*): RDD[T] = withScope {
    union(Seq(first) ++ rest)
  }

  /** Get an RDD that has no partitions or elements.
    * 得到一个RDD没有分区或元素。
    * */
  def emptyRDD[T: ClassTag]: RDD[T] = new EmptyRDD[T](this)

  // Methods for creating shared variables
  // 创建共享变量的方法

  /**
   * Create an [[org.apache.spark.Accumulator]] variable of a given type, which tasks can "add"
   * values to using the `+=` method. Only the driver can access the accumulator's `value`.
    *
    *  创建一个[[org.apache.spark.Accumulator]]给定类型的变量，哪些任务可以使用“+=”方法“add”值。
    *  只有驱动程序才能访问累加器的“值”。
   */
  @deprecated("use AccumulatorV2", "2.0.0")
  def accumulator[T](initialValue: T)(implicit param: AccumulatorParam[T]): Accumulator[T] = {
    val acc = new Accumulator(initialValue, param)
    cleaner.foreach(_.registerAccumulatorForCleanup(acc.newAcc))
    acc
  }

  /**
   * Create an [[org.apache.spark.Accumulator]] variable of a given type, with a name for display
   * in the Spark UI. Tasks can "add" values to the accumulator using the `+=` method. Only the
   * driver can access the accumulator's `value`.
    *
    *  创建一个[[org.apache.spark.Accumulator]]给定类型的变量，它的名称将会显示在Spark UI.
    *  哪些任务可以使用“+=”方法“add”值。只有驱动程序才能访问累加器的“值”。
    *
    *
   */
  @deprecated("use AccumulatorV2", "2.0.0")
  def accumulator[T](initialValue: T, name: String)(implicit param: AccumulatorParam[T])
    : Accumulator[T] = {
    val acc = new Accumulator(initialValue, param, Option(name))
    cleaner.foreach(_.registerAccumulatorForCleanup(acc.newAcc))
    acc
  }

  /**
   * Create an [[org.apache.spark.Accumulable]] shared variable, to which tasks can add values
   * with `+=`. Only the driver can access the accumulable's `value`.
   * @tparam R accumulator result type
   * @tparam T type that can be added to the accumulator
   */
  @deprecated("use AccumulatorV2", "2.0.0")
  def accumulable[R, T](initialValue: R)(implicit param: AccumulableParam[R, T])
    : Accumulable[R, T] = {
    val acc = new Accumulable(initialValue, param)
    cleaner.foreach(_.registerAccumulatorForCleanup(acc.newAcc))
    acc
  }

  /**
   * Create an [[org.apache.spark.Accumulable]] shared variable, with a name for display in the
   * Spark UI. Tasks can add values to the accumulable using the `+=` operator. Only the driver can
   * access the accumulable's `value`.
   * @tparam R accumulator result type
   * @tparam T type that can be added to the accumulator
   */
  @deprecated("use AccumulatorV2", "2.0.0")
  def accumulable[R, T](initialValue: R, name: String)(implicit param: AccumulableParam[R, T])
    : Accumulable[R, T] = {
    val acc = new Accumulable(initialValue, param, Option(name))
    cleaner.foreach(_.registerAccumulatorForCleanup(acc.newAcc))
    acc
  }

  /**
   * Create an accumulator from a "mutable collection" type.
    *
    *  从“可变集合”类型创建累加器。
   *
   * Growable and TraversableOnce are the standard APIs that guarantee += and ++=, implemented by
   * standard mutable collections. So you can use this with mutable Map, Set, etc.
    *
    *  成长性和traversableonce是标准的API，保证+ = + + =，标准可变集合的实现。
    *  所以你可以使用这个易变的Map，设置，等等。
    *
   */
  @deprecated("use AccumulatorV2", "2.0.0")
  def accumulableCollection[R <% Growable[T] with TraversableOnce[T] with Serializable: ClassTag, T]
      (initialValue: R): Accumulable[R, T] = {
    val param = new GrowableAccumulableParam[R, T]
    val acc = new Accumulable(initialValue, param)
    cleaner.foreach(_.registerAccumulatorForCleanup(acc.newAcc))
    acc
  }

  /**
   * Register the given accumulator.  注册给定的累加器
   *
   * @note Accumulators must be registered before use, or it will throw exception.
    *       累加器使用之前必须注册，否则会抛出异常
   */
  def register(acc: AccumulatorV2[_, _]): Unit = {
    acc.register(this)
  }

  /**
   * Register the given accumulator with given name. 根据指定的名称注册累加器
   *
   * @note Accumulators must be registered before use, or it will throw exception.
    *       累加器使用之前必须注册，否则会抛出异常
   */
  def register(acc: AccumulatorV2[_, _], name: String): Unit = {
    acc.register(this, name = Option(name))
  }

  /**
   * Create and register a long accumulator, which starts with 0 and accumulates inputs by `add`.
    *
    * 创建一个long型的累加器，他从0开始累加
   */
  def longAccumulator: LongAccumulator = {
    val acc = new LongAccumulator
    register(acc)
    acc
  }

  /**
   * Create and register a long accumulator, which starts with 0 and accumulates inputs by `add`.
    *
    * 创建一个long型的累加器，他从0开始累加
   */
  def longAccumulator(name: String): LongAccumulator = {
    val acc = new LongAccumulator
    register(acc, name)
    acc
  }

  /**
   * Create and register a double accumulator, which starts with 0 and accumulates inputs by `add`.
    * 创建一个double型的累加器，他从0开始累加
   */
  def doubleAccumulator: DoubleAccumulator = {
    val acc = new DoubleAccumulator
    register(acc)
    acc
  }

  /**
   * Create and register a double accumulator, which starts with 0 and accumulates inputs by `add`.
    *  创建并且注册一个double型的累加器，他从0开始累加
   */
  def doubleAccumulator(name: String): DoubleAccumulator = {
    val acc = new DoubleAccumulator
    register(acc, name)
    acc
  }

  /**
   * Create and register a `CollectionAccumulator`, which starts with empty list and accumulates
   * inputs by adding them into the list.
    *
    *   创建一个累加器集合，他是一个空的累加器list，累加器通过add方法添加进入
   */
  def collectionAccumulator[T]: CollectionAccumulator[T] = {
    val acc = new CollectionAccumulator[T]
    register(acc)
    acc
  }

  /**
   * Create and register a `CollectionAccumulator`, which starts with empty list and accumulates
   * inputs by adding them into the list.  创建一个累加器集合，他是一个空的累加器list，累加器通过add方法添加进入
   */
  def collectionAccumulator[T](name: String): CollectionAccumulator[T] = {
    val acc = new CollectionAccumulator[T]
    register(acc, name)
    acc
  }

  /**
   * Broadcast a read-only variable to the cluster, returning a
   * [[org.apache.spark.broadcast.Broadcast]] object for reading it in distributed functions.
   * The variable will be sent to each cluster only once.
    *
    *   对集群广播只读变量，返回一个[[org.apache.spark.broadcast.Broadcast]]对象在分布式函数中读取它。
    *   这个变量只会发送到每个集群一次。
    *   用于广播Hadoop的配置信息。
   *
   * @param value value to broadcast to the Spark nodes
   * @return `Broadcast` object, a read-only variable cached on each machine
   */
  def broadcast[T: ClassTag](value: T): Broadcast[T] = {
    assertNotStopped()
    require(!classOf[RDD[_]].isAssignableFrom(classTag[T].runtimeClass),
      "Can not directly broadcast RDDs; instead, call collect() and broadcast the result.")
    // 这里BroadcastManager的newBroadcast方法实际上代理了broadcastFactory的newBroadcast方法。通过TorrentBroadcastFactory
    // 生成TorrentBroadcast对象。
    val bc = env.broadcastManager.newBroadcast[T](value, isLocal)
    val callSite = getCallSite
    logInfo("Created broadcast " + bc.id + " from " + callSite.shortForm)
    //这段代码通过BroadCastManager发送广播，广播结束将广播对象注册到ContextCleanner中，以便于清理。
    cleaner.foreach(_.registerBroadcastForCleanup(bc))
    bc
  }

  /**
   * Add a file to be downloaded with this Spark job on every node.
    *
    *   在每个节点上添加一个要用这个Spark作业下载的文件。
   *
   * @param path can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI. To access the file in Spark jobs,
   * use `SparkFiles.get(fileName)` to find its download location.
   */
  def addFile(path: String): Unit = {
    addFile(path, false)
  }

  /**
   * Returns a list of file paths that are added to resources.
    *  返回一个列表，添加到资源文件的路径。
   */
  def listFiles(): Seq[String] = addedFiles.keySet.toSeq

  /**
   * Add a file to be downloaded with this Spark job on every node.
    *
    *   在每个节点上添加一个要用这个Spark作业下载的文件
   *
   * @param path can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI. To access the file in Spark jobs,
   * use `SparkFiles.get(fileName)` to find its download location.
    *
    *   可以是一个本地文件，在HDFS（Hadoop文件或其他支持文件），或者一个HTTP，HTTPS和FTP的URI。
    *   Spark jobs可以访问的文件，使用 `SparkFiles.get(fileName)`找到文件下载位置。
    *
   * @param recursive if true, a directory can be given in `path`. Currently directories are
   * only supported for Hadoop-supported filesystems.
    *
    *   如果是真的，可以在“path”中给出一个目录。目前目录只支持Hadoop支持的文件系统。
   */
  def addFile(path: String, recursive: Boolean): Unit = {
    val uri = new Path(path).toUri
    val schemeCorrectedPath = uri.getScheme match {
      case null | "local" => new File(path).getCanonicalFile.toURI.toString
      case _ => path
    }

    val hadoopPath = new Path(schemeCorrectedPath)
    val scheme = new URI(schemeCorrectedPath).getScheme
    if (!Array("http", "https", "ftp").contains(scheme)) {
      val fs = hadoopPath.getFileSystem(hadoopConfiguration)
      val isDir = fs.getFileStatus(hadoopPath).isDirectory
      if (!isLocal && scheme == "file" && isDir) {
        throw new SparkException(s"addFile does not support local directories when not running " +
          "local mode.")
      }
      if (!recursive && isDir) {
        throw new SparkException(s"Added file $hadoopPath is a directory and recursive is not " +
          "turned on.")
      }
    } else {
      // SPARK-17650: Make sure this is a valid URL before adding it to the list of dependencies
      Utils.validateURL(uri)
    }

    val key = if (!isLocal && scheme == "file") {
      env.rpcEnv.fileServer.addFile(new File(uri.getPath))
    } else {
      schemeCorrectedPath
    }
    val timestamp = System.currentTimeMillis
    if (addedFiles.putIfAbsent(key, timestamp).isEmpty) {
      logInfo(s"Added file $path at $key with timestamp $timestamp")
      // Fetch the file locally so that closures which are run on the driver can still use the
      // SparkFiles API to access files.
      Utils.fetchFile(uri.toString, new File(SparkFiles.getRootDirectory()), conf,
        env.securityManager, hadoopConfiguration, timestamp, useCache = false)
      postEnvironmentUpdate()
    }
  }

  /**
   * :: DeveloperApi ::
   * Register a listener to receive up-calls from events that happen during execution.
    *
    *   注册侦听器以接收执行期间发生的事件的调用。
   */
  @DeveloperApi
  def addSparkListener(listener: SparkListenerInterface) {
    listenerBus.addListener(listener)
  }

  /**
   * :: DeveloperApi ::
   * Deregister the listener from Spark's listener bus.
    *  注销listener从Spark的监听总线。
   */
  @DeveloperApi
  def removeSparkListener(listener: SparkListenerInterface): Unit = {
    listenerBus.removeListener(listener)
  }

  private[spark] def getExecutorIds(): Seq[String] = {
    schedulerBackend match {
      case b: CoarseGrainedSchedulerBackend =>
        b.getExecutorIds()
      case _ =>
        logWarning("Requesting executors is only supported in coarse-grained mode")
        Nil
    }
  }

  /**
   * Update the cluster manager on our scheduling needs. Three bits of information are included
   * to help it make decisions.
    *
    *   更新我们的调度的集群管理在scheduling需要的时候。包括三位bits帮助决策。
    *
   * @param numExecutors The total number of executors we'd like to have. The cluster manager
   *                     shouldn't kill any running executor to reach this number, but,
   *                     if all existing executors were to die, this is the number of executors
   *                     we'd want to be allocated.
    *
    *                     我们想要的executors的总数，。群集管理器不应该杀死任何运行的执行器来达到这个数字，
    *                     但是，如果所有现有的执行器都死了，这就是我们想要分配的执行器的数量。
    *
   * @param localityAwareTasks The number of tasks in all active stages that have a locality
   *                           preferences. This includes running, pending, and completed tasks.
    *
    *                           具有位置偏好的所有活动阶段中的任务数。这包括运行、挂起和完成的任务。
    *
   * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
   *                             that would like to like to run on that host.
   *                             This includes running, pending, and completed tasks.
    *
    *                             一个map的host映射到所有希望在该主机上运行的活动阶段的任务数，
    *                             包括运行、挂起和完成的任务。
    *
   * @return whether the request is acknowledged by the cluster manager. 该请求是否是由cluster manager承认。
   */
  @DeveloperApi
  def requestTotalExecutors(
      numExecutors: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: scala.collection.immutable.Map[String, Int]
    ): Boolean = {
    schedulerBackend match {
      case b: CoarseGrainedSchedulerBackend =>
        b.requestTotalExecutors(numExecutors, localityAwareTasks, hostToLocalTaskCount)
      case _ =>
        logWarning("Requesting executors is only supported in coarse-grained mode")
        false
    }
  }

  /**
   * :: DeveloperApi ::
   * Request an additional number of executors from the cluster manager.
    *   从群集管理器请求另一个执行器。
   * @return whether the request is received.  是否收到请求。
   */
  @DeveloperApi
  def requestExecutors(numAdditionalExecutors: Int): Boolean = {
    schedulerBackend match {
      case b: CoarseGrainedSchedulerBackend =>
        b.requestExecutors(numAdditionalExecutors)
      case _ =>
        logWarning("Requesting executors is only supported in coarse-grained mode")
        false
    }
  }

  /**
   * :: DeveloperApi ::
   * Request that the cluster manager kill the specified executors.
   *
    *    请求群集管理器cluster manager杀死指定的执行器。
    *
   * @note This is an indication to the cluster manager that the application wishes to adjust
   * its resource usage downwards. If the application wishes to replace the executors it kills
   * through this method with new ones, it should follow up explicitly with a call to
   * {{SparkContext#requestExecutors}}.
    *
    *   这是对集群管理器的一个指示，该应用程序希望向下调整其资源使用情况。
    *   如果应用程序希望替换的执行者会通过这种方法与新的，它应遵循明确调用{ { sparkcontext # requestexecutors } }。
   *
   * @return whether the request is received.
   */
  @DeveloperApi
  def killExecutors(executorIds: Seq[String]): Boolean = {
    schedulerBackend match {
      case b: CoarseGrainedSchedulerBackend =>
        b.killExecutors(executorIds, replace = false, force = true).nonEmpty
      case _ =>
        logWarning("Killing executors is only supported in coarse-grained mode")
        false
    }
  }

  /**
   * :: DeveloperApi ::
   * Request that the cluster manager kill the specified executor.
   *
   * @note This is an indication to the cluster manager that the application wishes to adjust
   * its resource usage downwards. If the application wishes to replace the executor it kills
   * through this method with a new one, it should follow up explicitly with a call to
   * {{SparkContext#requestExecutors}}.
   *
   * @return whether the request is received.
   */
  @DeveloperApi
  def killExecutor(executorId: String): Boolean = killExecutors(Seq(executorId))

  /**
   * Request that the cluster manager kill the specified executor without adjusting the
   * application resource requirements.
    *
    *   请求群集管理器在不调整应用程序资源需求的情况下杀死指定的执行器。
   *
   * The effect is that a new executor will be launched in place of the one killed by
   * this request. This assumes the cluster manager will automatically and eventually
   * fulfill all missing application resource requests.
   *
    *   这样做的结果是，新的执行者将代替被请求者杀死。这假设集群管理器将自动并最终完成所有缺少的应用程序资源请求。
    *
   * @note The replace is by no means guaranteed; another application on the same cluster
   * can steal the window of opportunity and acquire this application's resources in the
   * mean time.
   *
    *   @注意，替换并不意味着绝不保证；同一集群上的另一个应用程序可以窃取机会窗口，并在平均时间内获取该应用程序的资源。
    *
   * @return whether the request is received.
   */
  private[spark] def killAndReplaceExecutor(executorId: String): Boolean = {
    schedulerBackend match {
      case b: CoarseGrainedSchedulerBackend =>
        b.killExecutors(Seq(executorId), replace = true, force = true).nonEmpty
      case _ =>
        logWarning("Killing executors is only supported in coarse-grained mode")
        false
    }
  }

  /** The version of Spark on which this application is running.  应用程序运行的Spark版本。 */
  def version: String = SPARK_VERSION

  /**
   * Return a map from the slave to the max memory available for caching and the remaining
   * memory available for caching.
    *  将从slave的返回一个可用于缓存的最大内存和可用于缓存的剩余内存的map。
   */
  def getExecutorMemoryStatus: Map[String, (Long, Long)] = {
    assertNotStopped()
    env.blockManager.master.getMemoryStatus.map { case(blockManagerId, mem) =>
      (blockManagerId.host + ":" + blockManagerId.port, mem)
    }
  }

  /**
   * :: DeveloperApi ::
   * Return information about what RDDs are cached, if they are in mem or on disk, how much space
   * they take, etc.
    *
    *   返回的信息是RDDs是被缓存的，如果他们在内存或磁盘上，他们占用多少空间，等等。
   */
  @DeveloperApi
  def getRDDStorageInfo: Array[RDDInfo] = {
    getRDDStorageInfo(_ => true)
  }

  private[spark] def getRDDStorageInfo(filter: RDD[_] => Boolean): Array[RDDInfo] = {
    assertNotStopped()
    val rddInfos = persistentRdds.values.filter(filter).map(RDDInfo.fromRdd).toArray
    StorageUtils.updateRddInfo(rddInfos, getExecutorStorageStatus)
    rddInfos.filter(_.isCached)
  }

  /**
   * Returns an immutable map of RDDs that have marked themselves as persistent via cache() call.
    *
    *   返回一个不可变的map，标出自己persistent持久化 通过调用cache() RDDS
   *
   * @note This does not necessarily mean the caching or computation was successful.
    *       这并不一定意味着缓存或计算是成功的。
   */
  def getPersistentRDDs: Map[Int, RDD[_]] = persistentRdds.toMap

  /**
   * :: DeveloperApi ::
   * Return information about blocks stored in all of the slaves
    *
    *  返回存储在子节点的所有块的信息
   */
  @DeveloperApi
  @deprecated("This method may change or be removed in a future release.", "2.2.0")
  def getExecutorStorageStatus: Array[StorageStatus] = {
    assertNotStopped()
    env.blockManager.master.getStorageStatus
  }

  /**
   * :: DeveloperApi ::
   * Return pools for fair scheduler  公平调度程序的返回池
   */
  @DeveloperApi
  def getAllPools: Seq[Schedulable] = {
    assertNotStopped()
    // TODO(xiajunluan): We should take nested pools into account
    taskScheduler.rootPool.schedulableQueue.asScala.toSeq
  }

  /**
   * :: DeveloperApi ::
   * Return the pool associated with the given name, if one exists  ，给定一个池根据指定的名称，如果存在
   */
  @DeveloperApi
  def getPoolForName(pool: String): Option[Schedulable] = {
    assertNotStopped()
    Option(taskScheduler.rootPool.schedulableNameToSchedulable.get(pool))
  }

  /**
   * Return current scheduling mode  返回调度的模式
   */
  def getSchedulingMode: SchedulingMode.SchedulingMode = {
    assertNotStopped()
    taskScheduler.schedulingMode
  }

  /**
   * Gets the locality information associated with the partition in a particular rdd
    *   获取与特定RDD的分区有关的位置信息
   * @param rdd of interest
   * @param partition to be looked up for locality
   * @return list of preferred locations for the partition
   */
  private [spark] def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    dagScheduler.getPreferredLocs(rdd, partition)
  }

  /**
   * Register an RDD to be persisted in memory and/or disk storage
    *  注册一个RDD被保存在内存或磁盘存储
   */
  private[spark] def persistRDD(rdd: RDD[_]) {
    persistentRdds(rdd.id) = rdd
  }

  /**
   * Unpersist an RDD from memory and/or disk storage  注销一个被保存在内存或磁盘存储的RDD
    *
    * 这个方法被ContextCleaner类调用
   */
  private[spark] def unpersistRDD(rddId: Int, blocking: Boolean = true) {
    // 调用BlockManagerMaster清除RDD所有的块
    env.blockManager.master.removeRdd(rddId, blocking)
    // 哪些RDD被持久化了，保存在哪里呢？保存在persistentRdds中，然后被删除了
    persistentRdds.remove(rddId)
    // 这个被SparkListtenerBus类处理，doPostEvent（）方法处理--》
    listenerBus.post(SparkListenerUnpersistRDD(rddId))
  }

  /**
   * Adds a JAR dependency for all tasks to be executed on this `SparkContext` in the future.
    *
    * 添加一个jar包依赖在这个`SparkContext`所有的tasks 去运行executed
    *
   * @param path can be either a local file, a file in HDFS (or other Hadoop-supported filesystems),
   * an HTTP, HTTPS or FTP URI, or local:/path for a file on every worker node.
   */
  def addJar(path: String) {
    def addJarFile(file: File): String = {
      try {
        if (!file.exists()) {
          throw new FileNotFoundException(s"Jar ${file.getAbsolutePath} not found")
        }
        if (file.isDirectory) {
          throw new IllegalArgumentException(
            s"Directory ${file.getAbsoluteFile} is not allowed for addJar")
        }
        env.rpcEnv.fileServer.addJar(file)
      } catch {
        case NonFatal(e) =>
          logError(s"Failed to add $path to Spark environment", e)
          null
      }
    }

    if (path == null) {
      logWarning("null specified as parameter to addJar")
    } else {
      val key = if (path.contains("\\")) {
        // For local paths with backslashes on Windows, URI throws an exception
        addJarFile(new File(path))
      } else {
        val uri = new URI(path)
        // SPARK-17650: Make sure this is a valid URL before adding it to the list of dependencies
        Utils.validateURL(uri)
        uri.getScheme match {
          // A JAR file which exists only on the driver node
          case null | "file" => addJarFile(new File(uri.getPath))
          // A JAR file which exists locally on every worker node
          case "local" => "file:" + uri.getPath
          case _ => path
        }
      }
      if (key != null) {
        val timestamp = System.currentTimeMillis
        if (addedJars.putIfAbsent(key, timestamp).isEmpty) {
          logInfo(s"Added JAR $path at $key with timestamp $timestamp")
          postEnvironmentUpdate()
        }
      }
    }
  }

  /**
   * Returns a list of jar files that are added to resources. 返回一个列表，添加到资源的jar文件。
   */
  def listJars(): Seq[String] = addedJars.keySet.toSeq

  /**
   * When stopping SparkContext inside Spark components, it's easy to cause dead-lock since Spark
   * may wait for some internal threads to finish. It's better to use this method to stop
   * SparkContext instead.
    *
    *   当停止sparkcontext里面Spark的部件，很容易造成锁死因为Spark会等待一些内部的线程来完成。
    *   这是更好地使用此方法停止sparkcontext相反。
   */
  private[spark] def stopInNewThread(): Unit = {
    new Thread("stop-spark-context") {
      setDaemon(true)

      override def run(): Unit = {
        try {
          SparkContext.this.stop()
        } catch {
          case e: Throwable =>
            logError(e.getMessage, e)
            throw e
        }
      }
    }.start()
  }

  /**
   * Shut down the SparkContext.  关闭SparkContext
   */
  def stop(): Unit = {
    if (LiveListenerBus.withinListenerThread.value) {
      throw new SparkException(
        s"Cannot stop SparkContext within listener thread of ${LiveListenerBus.name}")
    }
    // Use the stopping variable to ensure no contention for the stop scenario.
    // Still track the stopped variable for use elsewhere in the code.
    if (!stopped.compareAndSet(false, true)) {
      logInfo("SparkContext already stopped.")
      return
    }
    if (_shutdownHookRef != null) {
      ShutdownHookManager.removeShutdownHook(_shutdownHookRef)
    }

    // tryLogNonFatalError:执行给定的代码块。如果有错误，记录非致命错误，并且只抛出致命错误
    Utils.tryLogNonFatalError {
      // 发布应用程序结束事件
      postApplicationEnd()
    }
    Utils.tryLogNonFatalError {
      _ui.foreach(_.stop())
    }
    if (env != null) {
      Utils.tryLogNonFatalError {
        env.metricsSystem.report()
      }
    }
    Utils.tryLogNonFatalError {
      _cleaner.foreach(_.stop())
    }
    Utils.tryLogNonFatalError {
      _executorAllocationManager.foreach(_.stop())
    }
    if (_listenerBusStarted) {
      Utils.tryLogNonFatalError {
        listenerBus.stop()
        _listenerBusStarted = false
      }
    }
    Utils.tryLogNonFatalError {
      _eventLogger.foreach(_.stop())
    }
    if (_dagScheduler != null) {
      Utils.tryLogNonFatalError {
        // DagScheduler的stop方法，涉及计算资源的回收
        _dagScheduler.stop()
      }
      _dagScheduler = null
    }
    if (env != null && _heartbeatReceiver != null) {
      Utils.tryLogNonFatalError {
        env.rpcEnv.stop(_heartbeatReceiver)
      }
    }
    Utils.tryLogNonFatalError {
      _progressBar.foreach(_.stop())
    }
    _taskScheduler = null
    // TODO: Cache.stop()?
    if (_env != null) {
      Utils.tryLogNonFatalError {
        _env.stop()
      }
      SparkEnv.set(null)
    }
    // Clear this `InheritableThreadLocal`, or it will still be inherited in child threads even this
    // `SparkContext` is stopped.
    localProperties.remove()
    // Unset YARN mode system env variable, to allow switching between cluster types.
    System.clearProperty("SPARK_YARN_MODE")
    SparkContext.clearActiveContext()
    logInfo("Successfully stopped SparkContext")
  }


  /**
   * Get Spark's home location from either a value set through the constructor,
   * or the spark.home Java property, or the SPARK_HOME environment variable
   * (in that order of preference). If neither of these is set, return None.
    *
    *   无论是通过构造函数的价值得到Spark home的位置，或spark.home java属性，或spark_home环境变量
    *   （按优先顺序排列）。如果两者都没有设置，则不返回任何一个。
    *
   */
  private[spark] def getSparkHome(): Option[String] = {
    conf.getOption("spark.home").orElse(Option(System.getenv("SPARK_HOME")))
  }

  /**
   * Set the thread-local property for overriding the call sites
   * of actions and RDDs.
    *
    *   设置本地线程属性为了重写call sites of RDD的actions方法
   */
  def setCallSite(shortCallSite: String) {
    setLocalProperty(CallSite.SHORT_FORM, shortCallSite)
  }

  /**
   * Set the thread-local property for overriding the call sites
   * of actions and RDDs.
   */
  private[spark] def setCallSite(callSite: CallSite) {
    setLocalProperty(CallSite.SHORT_FORM, callSite.shortForm)
    setLocalProperty(CallSite.LONG_FORM, callSite.longForm)
  }

  /**
   * Clear the thread-local property for overriding the call sites
   * of actions and RDDs.
    *
    *  清除
   */
  def clearCallSite() {
    setLocalProperty(CallSite.SHORT_FORM, null)
    setLocalProperty(CallSite.LONG_FORM, null)
  }

  /**
   * Capture the current user callsite and return a formatted version for printing. If the user
   * has overridden the call site using `setCallSite()`, this will return the user's version.
    *
    *   获取当前用户的调用点，并返回一个格式化版打印。如果用户重写调用site点使用` setcallsite() `，这将返回用户的版本。
    *
   */
  private[spark] def getCallSite(): CallSite = {
    lazy val callSite = Utils.getCallSite()
    CallSite(
      Option(getLocalProperty(CallSite.SHORT_FORM)).getOrElse(callSite.shortForm),
      Option(getLocalProperty(CallSite.LONG_FORM)).getOrElse(callSite.longForm)
    )
  }

  /**
   * Run a function on a given set of partitions in an RDD and pass the results to the given
   * handler function. This is the main entry point for all actions in Spark.
    *
    *  运行一个函数在给定一个RDD分区设置和结果传递到特定的处理函数。这是在所有Spark actions的主要入口点。
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   * partitions of the target RDD, e.g. for operations like `first()`
   * @param resultHandler callback to pass each result to
    *
    *
    *    一个Job实际上是从RDD调用一个Action操作开始的，该Action操作最终会进入到org.apache.spark.SparkContext.runJob()
    * 方法中，在SparkContext中有多个重载的runJob方法，最终入口是下面这个
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit): Unit = {

    // 判断SparkContext是否停止，这里使用AtomicBoolean是线程阻塞的
    if (stopped.get()) {
      throw new IllegalStateException("SparkContext has been shutdown")
    }

    // 返回调用点
    val callSite = getCallSite
    // clean方法实际上调用了ClosureCleaner的clean方法，这里一再清除闭包中的不能序列化的变量，防止RDD在网络传输过程中反序列化失败。
    val cleanedFunc = clean(func)
    logInfo("Starting job: " + callSite.shortForm)
    if (conf.getBoolean("spark.logLineage", false)) {
      logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
    }

    // job先要按依赖关系通过dagScheduler切分stage，stage通过dagScheduler进行调度
    // 这里调用dagScheduler.runJob()方法后，正式进入之前构造的DAGScheduler对象中。
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
    progressBar.foreach(_.finishAll())
    rdd.doCheckpoint()
  }

  /**
   * Run a function on a given set of partitions in an RDD and return the results as an array.
   * The function that is run against each partition additionally takes `TaskContext` argument.
    *
    *  运行一个函数对给定的一个RDD分区设置和返回结果数组。运行的函数对每个分区的另外` taskcontext `论点。
   *
   * @param rdd target RDD to run tasks on  目标RDD运行任务
   * @param func a function to run on each partition of the RDD  一个函数运行在每个分区的RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   * partitions of the target RDD, e.g. for operations like `first()`
    *
    *   设置分区上运行；有些工作可能不想所有分区上的目标盘计算，例如像` first() `操作
    *
   * @return in-memory collection with a result of the job (each collection element will contain
   * a result from one partition)
    *
    *   在内存中的集合是一个job运行的结果（每个集合元素将包含一个分区的结果）
    *
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int]): Array[U] = {
    val results = new Array[U](partitions.size)
    // 继续调用runJob方法
    runJob[T, U](rdd, func, partitions, (index, res) => results(index) = res)
    results
  }

  /**
   * Run a function on a given set of partitions in an RDD and return the results as an array.
    *
    *   运行一个函数对给定的一个RDD分区设置并返回结果为数组。
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   * partitions of the target RDD, e.g. for operations like `first()`
   * @return in-memory collection with a result of the job (each collection element will contain
   * a result from one partition)
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: Iterator[T] => U,
      partitions: Seq[Int]): Array[U] = {
    // 第一次调用clean方法防止闭包的反序列化错误
    val cleanedFunc = clean(func)
    // 接着又调用了两个重载的runJob
    runJob(rdd, (ctx: TaskContext, it: Iterator[T]) => cleanedFunc(it), partitions)
  }

  /**
   * Run a job on all partitions in an RDD and return the results in an array. The function
   * that is run against each partition additionally takes `TaskContext` argument.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @return in-memory collection with a result of the job (each collection element will contain
   * a result from one partition)
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.partitions.length)
  }

  /**
   * Run a job on all partitions in an RDD and return the results in an array.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @return in-memory collection with a result of the job (each collection element will contain
   * a result from one partition)
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    // SparkContext的runJob又调用了重载的runJob
    runJob(rdd, func, 0 until rdd.partitions.length)
  }

  /**
   * Run a job on all partitions in an RDD and pass the results to a handler function. The function
   * that is run against each partition additionally takes `TaskContext` argument.
   *
   * @param rdd target RDD to run tasks on
   * @param processPartition a function to run on each partition of the RDD
   * @param resultHandler callback to pass each result to
   */
  def runJob[T, U: ClassTag](
    rdd: RDD[T],
    processPartition: (TaskContext, Iterator[T]) => U,
    resultHandler: (Int, U) => Unit)
  {
    runJob[T, U](rdd, processPartition, 0 until rdd.partitions.length, resultHandler)
  }

  /**
   * Run a job on all partitions in an RDD and pass the results to a handler function.
    *
    *  运行在一个job在RDD所有分区，结果传递到一个处理函数。
   *
   * @param rdd target RDD to run tasks on  在task上运行一个RDD
   * @param processPartition a function to run on each partition of the RDD    在RDD每个分区上运行的函数
   * @param resultHandler callback to pass each result to
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      resultHandler: (Int, U) => Unit)
  {
    val processFunc = (context: TaskContext, iter: Iterator[T]) => processPartition(iter)
    runJob[T, U](rdd, processFunc, 0 until rdd.partitions.length, resultHandler)
  }

  /**
   * :: DeveloperApi ::
   * Run a job that can return approximate results.  运行工作，可以返回的结果。
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param evaluator `ApproximateEvaluator` to receive the partial results
   * @param timeout maximum time to wait for the job, in milliseconds
   * @return partial result (how partial depends on whether the job was finished before or
   * after timeout)
   */
  @DeveloperApi
  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      timeout: Long): PartialResult[R] = {
    assertNotStopped()
    val callSite = getCallSite
    logInfo("Starting job: " + callSite.shortForm)
    val start = System.nanoTime
    val cleanedFunc = clean(func)
    val result = dagScheduler.runApproximateJob(rdd, cleanedFunc, evaluator, callSite, timeout,
      localProperties.get)
    logInfo(
      "Job finished: " + callSite.shortForm + ", took " + (System.nanoTime - start) / 1e9 + " s")
    result
  }

  /**
   * Submit a job for execution and return a FutureJob holding the result.
    * 提交一个作业执行并返回他们持有的结果。
   *
   * @param rdd target RDD to run tasks on
   * @param processPartition a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   * partitions of the target RDD, e.g. for operations like `first()`
   * @param resultHandler callback to pass each result to
   * @param resultFunc function to be executed when the result is ready
   */
  def submitJob[T, U, R](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit,
      resultFunc: => R): SimpleFutureAction[R] =
  {
    assertNotStopped()
    val cleanF = clean(processPartition)
    val callSite = getCallSite
    val waiter = dagScheduler.submitJob(
      rdd,
      (context: TaskContext, iter: Iterator[T]) => cleanF(iter),
      partitions,
      callSite,
      resultHandler,
      localProperties.get)
    new SimpleFutureAction(waiter, resultFunc)
  }

  /**
   * Submit a map   for execution. This is currently an internal API only, but might be
   * promoted to DeveloperApi in the future.
    *
    * 提交map stage 执行。这不仅是当前的一个内部API，但可能在未来推动DeveloperApi。
    *
   */
  private[spark] def submitMapStage[K, V, C](dependency: ShuffleDependency[K, V, C])
      : SimpleFutureAction[MapOutputStatistics] = {
    assertNotStopped()
    val callSite = getCallSite()
    var result: MapOutputStatistics = null
    val waiter = dagScheduler.submitMapStage(
      dependency,
      (r: MapOutputStatistics) => { result = r },
      callSite,
      localProperties.get)
    new SimpleFutureAction[MapOutputStatistics](waiter, result)
  }

  /**
   * Cancel active jobs for the specified group. See `org.apache.spark.SparkContext.setJobGroup`
   * for more information.
    *
    * 取消指定组的活动jobs。详细请看` org.apache.spark.SparkContext.setJobGroup `。
    *
   */
  def cancelJobGroup(groupId: String) {
    assertNotStopped()
    dagScheduler.cancelJobGroup(groupId)
  }

  /** Cancel all jobs that have been scheduled or are running.
    * 取消所有的jobs已被预定或运行。
    * */
  def cancelAllJobs() {
    assertNotStopped()
    dagScheduler.cancelAllJobs()
  }

  /**
   * Cancel a given job if it's scheduled or running.
   *
    *  取消某项工作如果是定期或运行。
    *
   * @param jobId the job ID to cancel  要取消运行的job
   * @param reason optional reason for cancellation  可选的取消理由
   * @note Throws `InterruptedException` if the cancel message cannot be sent
    *       抛出InterruptedException ` `如果取消消息无法发送
   */
  def cancelJob(jobId: Int, reason: String): Unit = {
    dagScheduler.cancelJob(jobId, Option(reason))
  }

  /**
   * Cancel a given job if it's scheduled or running.
   *
   * @param jobId the job ID to cancel
   * @note Throws `InterruptedException` if the cancel message cannot be sent
   */
  def cancelJob(jobId: Int): Unit = {
    dagScheduler.cancelJob(jobId, None)
  }

  /**
   * Cancel a given stage and all jobs associated with it.
   *
   * @param stageId the stage ID to cancel
   * @param reason reason for cancellation
   * @note Throws `InterruptedException` if the cancel message cannot be sent
   */
  def cancelStage(stageId: Int, reason: String): Unit = {
    dagScheduler.cancelStage(stageId, Option(reason))
  }

  /**
   * Cancel a given stage and all jobs associated with it.
   *
   * @param stageId the stage ID to cancel
   * @note Throws `InterruptedException` if the cancel message cannot be sent
   */
  def cancelStage(stageId: Int): Unit = {
    dagScheduler.cancelStage(stageId, None)
  }

  /**
   * Kill and reschedule the given task attempt. Task ids can be obtained from the Spark UI
   * or through SparkListener.onTaskStart.
   *
    * 杀死并重新安排给定的任务尝试。任务ID可以从Spark UI或通过sparklistener.ontaskstart获得。
    *
   * @param taskId the task ID to kill. This id uniquely identifies the task attempt.
   * @param interruptThread whether to interrupt the thread running the task.
   * @param reason the reason for killing the task, which should be a short string. If a task
   *   is killed multiple times with different reasons, only one reason will be reported.
   *
   * @return Whether the task was successfully killed.
   */
  def killTaskAttempt(
      taskId: Long,
      interruptThread: Boolean = true,
      reason: String = "killed via SparkContext.killTaskAttempt"): Boolean = {
    dagScheduler.killTaskAttempt(taskId, interruptThread, reason)
  }

  /**
   * Clean a closure to make it ready to serialized and send to tasks
   * (removes unreferenced variables in $outer's, updates REPL variables)
   * If <tt>checkSerializable</tt> is set, <tt>clean</tt> will also proactively
   * check to see if <tt>f</tt> is serializable and throw a <tt>SparkException</tt>
   * if not.
   *
   * @param f the closure to clean
   * @param checkSerializable whether or not to immediately check <tt>f</tt> for serializability
   * @throws SparkException if <tt>checkSerializable</tt> is set but <tt>f</tt> is not
   *   serializable
   * @return the cleaned closure
    *
    * clean方法实际上调用了ClosureCleaner的clean方法，这里一再清除闭包中的不能序列化的变量，防止RDD在网络传输过程中反序列化失败。
   */
  private[spark] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    // clean方法实际上调用了ClosureCleaner的clean方法，这里一再清除闭包中的不能序列化的变量，防止RDD在网络传输过程中反序列化失败。
    ClosureCleaner.clean(f, checkSerializable)
    f
  }

  /**
   * Set the directory under which RDDs are going to be checkpointed.
    *
    *  设置目录下的RDDS将会被设置检查点。
    *  设置目录，用于存储checkpoint的RDD，如果是集群，这个目录必须是HDFS路径
    *
   * @param directory path to the directory where checkpoint files will be stored
   * (must be HDFS path if running in cluster)
    *
    *   要设置检查点的文件的路径，这里将会被存储
    *   如果是集群，必须是HDFS的路径
   */
  def setCheckpointDir(directory: String) {

    // If we are running on a cluster, log a warning if the directory is local.
    // Otherwise, the driver may attempt to reconstruct the checkpointed RDD from
    // its own local file system, which is incorrect because the checkpoint files
    // are actually on the executor machines.

    // 如果我们在集群上运行，如果目录是本地的，则记录一个警告。
    // 否则，driver 会尝试在本地重新构建checkpoint的RDD
    // 由于文件其实是在executor上的，所以会提出警告
    if (!isLocal && Utils.nonLocalPaths(directory).isEmpty) {
      logWarning("Spark is not running in local mode, therefore the checkpoint directory " +
        s"must not be on the local filesystem. Directory '$directory' " +
        "appears to be on the local filesystem.")
    }

    checkpointDir = Option(directory).map { dir =>
      val path = new Path(dir, UUID.randomUUID().toString) //path增加一个随机码
      val fs = path.getFileSystem(hadoopConfiguration)     //加载一个hadoop文件系统的配置
      fs.mkdirs(path) //创建一个hdfs下的文件
      fs.getFileStatus(path).getPath.toString //把hdfs下的文件路径通过string返回
    }
  }

  def getCheckpointDir: Option[String] = checkpointDir

  /** Default level of parallelism to use when not given by user (e.g. parallelize and makeRDD).
    * 用户未给定时  使用的默认并行级别
    * */
  def defaultParallelism: Int = {
    assertNotStopped()
    taskScheduler.defaultParallelism
  }

  /**
   * Default min number of partitions for Hadoop RDDs when not given by user
   * Notice that we use math.min so the "defaultMinPartitions" cannot be higher than 2.
   * The reasons for this are discussed in https://github.com/mesos/spark/pull/718
    *
    * 当用户没有设置分区数的时候，默认的最小分区数为Hadoop RDDS，我们使用math.min所以“defaultminpartitions”不能高于2。
    * 出现这种现象的原因进行了https://github.com/mesos/spark/pull/718
    *
   */
  def defaultMinPartitions: Int = math.min(defaultParallelism, 2)

  private val nextShuffleId = new AtomicInteger(0)

  private[spark] def newShuffleId(): Int = nextShuffleId.getAndIncrement()

  private val nextRddId = new AtomicInteger(0)

  /** Register a new RDD, returning its RDD ID
    * 注册一个新的RDD，并且返回该RDD的ID
    * */
  private[spark] def newRddId(): Int = nextRddId.getAndIncrement()

  /**
   * Registers listeners specified in spark.extraListeners, then starts the listener bus.
   * This should be called after all internal listeners have been registered with the listener bus
   * (e.g. after the web UI and event logging listeners have been registered).
    *
    * 注册listeners在指定的spark.extraListeners，然后开始监听总线（listener bus），这应该在所有内部的
    * listeners已经监听向（listener bus）监听总线注册后调用（例如在Web UI和event logging listeners已注册）。
    *
   */
  private def setupAndStartListenerBus(): Unit = {
    // Use reflection to instantiate listeners specified via `spark.extraListeners`
    try {
      // 得到所有监听器的名字比如（eventLog）
      val listenerClassNames: Seq[String] =
        conf.get("spark.extraListeners", "").split(',').map(_.trim).filter(_ != "")
      // 循环每个监听器的名字
      for (className <- listenerClassNames) {
        // Use reflection to find the right constructor
        val constructors = {
          // 这一点会调用Class.forName(className, true, getContextOrSparkClassLoader)
          // 而Class.forName(xxx.xx.xx)返回的是一个类,作用是要求JVM查找并加载指定的类,也就是说JVM会执行该类的静态代码段
          // 所以这一点会执行每个监听器的静态代码块  初始化属性之类的
          val listenerClass = Utils.classForName(className)
          // 返回构造函数，
          listenerClass
              .getConstructors
              .asInstanceOf[Array[Constructor[_ <: SparkListenerInterface]]]
        }
        val constructorTakingSparkConf = constructors.find { c =>
          c.getParameterTypes.sameElements(Array(classOf[SparkConf]))
        }
        lazy val zeroArgumentConstructor = constructors.find { c =>
          c.getParameterTypes.isEmpty
        }
        val listener: SparkListenerInterface = {
          // 有不为空的构造方法
          if (constructorTakingSparkConf.isDefined) {
            constructorTakingSparkConf.get.newInstance(conf)

            // 只有空构造方法
          } else if (zeroArgumentConstructor.isDefined) {
            zeroArgumentConstructor.get.newInstance()
          } else {
            throw new SparkException(
              // 没有空构造方法，没有其他构造方法就报错
              s"$className did not have a zero-argument constructor or a" +
                " single-argument constructor that accepts SparkConf. Note: if the class is" +
                " defined inside of another Scala class, then its constructors may accept an" +
                " implicit parameter that references the enclosing class; in this case, you must" +
                " define the listener as a top-level class in order to prevent this extra" +
                " parameter from breaking Spark's ability to find a valid constructor.")
          }
        }
        listenerBus.addListener(listener)
        logInfo(s"Registered listener $className")
      }
    } catch {
      case e: Exception =>
        try {
          // 如果有任何异常，就停止SparkContext
          stop()
        } finally {
          throw new SparkException(s"Exception when registering SparkListener", e)
        }
    }

    // 启动监听总线
    listenerBus.start()
    // 标志监听总线已经启动
    _listenerBusStarted = true
  }

  /** Post the application start event
    * 发布应用程序启动事件
    *
    * 在SparkContext的初始化过程中，可能对其环境造成影响，所以需要更新环境。
    * */
  private def postApplicationStart() {
    // Note: this code assumes that the task scheduler has been initialized and has contacted
    // the cluster manager to get an application ID (in case the cluster manager provides one).
    listenerBus.post(SparkListenerApplicationStart(appName, Some(applicationId),
      startTime, sparkUser, applicationAttemptId, schedulerBackend.getDriverLogUrls))
  }

  /** Post the application end event
    * 发布应用程序结束事件
    * */
  private def postApplicationEnd() {
    listenerBus.post(SparkListenerApplicationEnd(System.currentTimeMillis))
  }

  /** Post the environment update event once the task scheduler is ready
    * 任务调度程序就绪后，发布环境更新事件
    *
    * 在SparkContext的初始化过程中，可能对其环境造成影响，所以需要更新环境。
    * 处理步骤：
    *   1.调用SparkEnv的方法environmentDetails最终影响环境的JVM参数，Spark属性，系统属性，classPath等
    *   2.生成事件SparkListenerEnvironmentUpdate，并且post到listenerBus，此事件被EnvironmentListener监听，最终影响EnvironmentPage页面中的输出内容。
    * */
  private def postEnvironmentUpdate() {
    // 任务调度，必须初始化，否则都没任务，环境变量改变了，没改变都无所谓
    if (taskScheduler != null) {
      val schedulingMode = getSchedulingMode.toString   // 调度模式
      val addedJarPaths = addedJars.keys.toSeq          // 添加jar包路径
      val addedFilePaths = addedFiles.keys.toSeq        // 添加的文件路径
      val environmentDetails = SparkEnv.environmentDetails(conf, schedulingMode, addedJarPaths,
        addedFilePaths)
      // 调用SparkListenerEnvironmentUpdate extends SparkListenerEvent --》。。。。-》 最终调用EnvironmentListener extends SparkListener的onEnvironmentUpdate方法更新SparkUI界面的显示
      val environmentUpdate = SparkListenerEnvironmentUpdate(environmentDetails)
      listenerBus.post(environmentUpdate)
    }
  }

  // In order to prevent multiple SparkContexts from being active at the same time, mark this
  // context as having finished construction.
  // NOTE: this must be placed at the end of the SparkContext constructor.
  // 为了防止多个sparkcontexts从活跃的同时，纪念这一语境完成建设。
  // 注意：这个必须放在构造函数的sparkcontext结束。
  // SparkContext初始化的最后将当前的SparkContext的状态从contextBeingConstructed（正在构建中）改为activeContext(已激活)
  SparkContext.setActiveContext(this, allowMultipleContexts)
}

/**
 * The SparkContext object contains a number of implicit conversions and parameters for use with
 * various Spark features.
  *  sparkcontext对象包含用于各种Spark特征数隐式转换和参数。
  *
 */
object SparkContext extends Logging {
  private val VALID_LOG_LEVELS =
    Set("ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN")

  /**
   * Lock that guards access to global variables that track SparkContext construction.
    * 锁，警卫访问全局变量，跟踪sparkcontext建设。
   */
  private val SPARK_CONTEXT_CONSTRUCTOR_LOCK = new Object()

  /**
   * The active, fully-constructed SparkContext.  If no SparkContext is active, then this is `null`.
    * 主动、全面构建sparkcontext。如果没有sparkcontext是活动的，那么这是`空`。
   *
   * Access to this field is guarded by SPARK_CONTEXT_CONSTRUCTOR_LOCK.
    * 进入这个领域需要持有spark_context_constructor_lock。
    *
    *
    * 这个相当于存储当前活动的SparkContext
   */
  private val activeContext: AtomicReference[SparkContext] =
    new AtomicReference[SparkContext](null)

  /**
   * Points to a partially-constructed SparkContext if some thread is in the SparkContext
   * constructor, or `None` if no SparkContext is being constructed.
    *
    * 指向一个partially-constructed 的sparkcontext如果一些线程在sparkcontext构造函数，
    * 或`None`如果没有sparkcontext正在建设。
   *
   * Access to this field is guarded by SPARK_CONTEXT_CONSTRUCTOR_LOCK
   */
  private var contextBeingConstructed: Option[SparkContext] = None

  /**
   * Called to ensure that no other SparkContext is running in this JVM.
    *  调用该函数为确保没有其他sparkcontext在JVM中运行。
   *
   * Throws an exception if a running context is detected and logs a warning if another thread is
   * constructing a SparkContext.  This warning is necessary because the current locking scheme
   * prevents us from reliably distinguishing between cases where another context is being
   * constructed and cases where another constructor threw an exception.
    *
    * 抛出一个异常如果运行的context是过期的而且记录一个警告：如果另一个线程正在构建一个sparkcontext。
    * 此警告是必要的，因为当前的锁定方案阻止我们可靠地区分正在构造另一个上下文的情况以及其他构造函数抛出异常的情况。
    *
   */
  private def assertNoOtherContextIsRunning(
      sc: SparkContext,
      allowMultipleContexts: Boolean): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      Option(activeContext.get()).filter(_ ne sc).foreach { ctx =>
          val errMsg = "Only one SparkContext may be running in this JVM (see SPARK-2243)." +
            " To ignore this error, set spark.driver.allowMultipleContexts = true. " +
            s"The currently running SparkContext was created at:\n${ctx.creationSite.longForm}"
          val exception = new SparkException(errMsg)
          if (allowMultipleContexts) {
            logWarning("Multiple running SparkContexts detected in the same JVM!", exception)
          } else {
            throw exception
          }
        }

      contextBeingConstructed.filter(_ ne sc).foreach { otherContext =>
        // Since otherContext might point to a partially-constructed context, guard against
        // its creationSite field being null:
        val otherContextCreationSite =
          Option(otherContext.creationSite).map(_.longForm).getOrElse("unknown location")
        val warnMsg = "Another SparkContext is being constructed (or threw an exception in its" +
          " constructor).  This may indicate an error, since only one SparkContext may be" +
          " running in this JVM (see SPARK-2243)." +
          s" The other SparkContext was created at:\n$otherContextCreationSite"
        logWarning(warnMsg)
      }
    }
  }

  /**
   * This function may be used to get or instantiate a SparkContext and register it as a
   * singleton object. Because we can only have one active SparkContext per JVM,
   * this is useful when applications may wish to share a SparkContext.
    *
    * 这个函数可以用来获取或实例化一个sparkcontext和登记为一个单列对象。因为我们只能有一sparkcontext每个JVM活跃，
    * 这是有用的当应用程序可以共享一个sparkcontext。
   *
   * @note This function cannot be used to create multiple SparkContext instances
   * even if multiple contexts are allowed.
    *
    *   这个功能不能用于创建多个sparkcontext实例 即使允许多个上下文。

   * @param config `SparkConfig` that will be used for initialisation of the `SparkContext`
   * @return current `SparkContext` (or a new one if it wasn't created before the function call)
   */
  def getOrCreate(config: SparkConf): SparkContext = {
    // Synchronize to ensure that multiple create requests don't trigger an exception
    // from assertNoOtherContextIsRunning within setActiveContext
    // 同步以确保多个创建请求不从在setActiveContext assertNoOtherContextIsRunning引发一个异常
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      if (activeContext.get() == null) {
        // 开始创建Sparkcontest
        setActiveContext(new SparkContext(config), allowMultipleContexts = false)
      } else {
        if (config.getAll.nonEmpty) {
          logWarning("Using an existing SparkContext; some configuration may not take effect.")
        }
      }
      activeContext.get()
    }
  }

  /**
   * This function may be used to get or instantiate a SparkContext and register it as a
   * singleton object. Because we can only have one active SparkContext per JVM,
   * this is useful when applications may wish to share a SparkContext.
   *
   * This method allows not passing a SparkConf (useful if just retrieving).
    *
    *
    *
    * 这个函数可以用来获取或实例化一个sparkcontext和登记为单例对象。
    * 因为我们只能有一sparkcontext每个JVM活跃，这是有用的时候，可能希望应用程序共享一个sparkcontext。
    * 这种方法可以不通过sparkconf（如果只是检索）。
   *
   * @note This function cannot be used to create multiple SparkContext instances
   * even if multiple contexts are allowed.
   * @return current `SparkContext` (or a new one if wasn't created before the function call)
   */
  def getOrCreate(): SparkContext = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      if (activeContext.get() == null) {
        setActiveContext(new SparkContext(), allowMultipleContexts = false)
      }
      activeContext.get()
    }
  }

  /** Return the current active [[SparkContext]] if any.
    * 如果有的话，返回当前的活动[[SparkContext]]。
    *
    * */
  private[spark] def getActive: Option[SparkContext] = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      Option(activeContext.get())
    }
  }

  /**
   * Called at the beginning of the SparkContext constructor to ensure that no SparkContext is
   * running.  Throws an exception if a running context is detected and logs a warning if another
   * thread is constructing a SparkContext.  This warning is necessary because the current locking
   * scheme prevents us from reliably distinguishing between cases where another context is being
   * constructed and cases where another constructor threw an exception.
    *
    *
    * 在sparkcontext构造函数开始之前调用，去确保没有sparkcontext正在运行。抛出一个异常如果running context 是过时的而且
    * 记录一个警告：如果另一个线程正在构建一个sparkcontext。此警告是必要的，因为当前的锁定方案阻止我们可靠地区分正在
    * 构造另一个上下文的情况以及其他构造函数抛出异常的情况。
   */
  private[spark] def markPartiallyConstructed(
      sc: SparkContext,
      allowMultipleContexts: Boolean): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      assertNoOtherContextIsRunning(sc, allowMultipleContexts)
      contextBeingConstructed = Some(sc)
    }
  }

  /**
   * Called at the end of the SparkContext constructor to ensure that no other SparkContext has
   * raced with this constructor and started.
    *
    * 在sparkcontext构造函数结束之后调用来确保没有其他sparkcontext参加了这个构造函数而且开始构建SparkContext。
    *
    * SparkContext初始化的最后将当前的SparkContext的状态从contextBeingConstructed（正在构建中）
    * 改为activeContext(已激活)
    *
   */
  private[spark] def setActiveContext(
      sc: SparkContext,
      allowMultipleContexts: Boolean): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      // 确保其他线程没有同时在构建SparkContext
      assertNoOtherContextIsRunning(sc, allowMultipleContexts)
      contextBeingConstructed = None
      activeContext.set(sc)
    }
  }

  /**
   * Clears the active SparkContext metadata.  This is called by `SparkContext#stop()`.  It's
   * also called in unit tests to prevent a flood of warnings from test suites that don't / can't
   * properly clean up their SparkContexts.
    *
    * 清除活动的sparkcontext元数据。这是被称为`SparkContext#stop() `。它也被称为单元测试以防止洪水从测试套件
    * ，不警告不能正确地清理他们的sparkcontexts。
    *
   */
  private[spark] def clearActiveContext(): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      activeContext.set(null)
    }
  }

  private[spark] val SPARK_JOB_DESCRIPTION = "spark.job.description"
  private[spark] val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
  private[spark] val SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel"
  private[spark] val RDD_SCOPE_KEY = "spark.rdd.scope"
  private[spark] val RDD_SCOPE_NO_OVERRIDE_KEY = "spark.rdd.scope.noOverride"

  /**
   * Executor id for the driver.  In earlier versions of Spark, this was `<driver>`, but this was
   * changed to `driver` because the angle brackets caused escaping issues in URLs and XML (see
   * SPARK-6716 for more details).
    *
    * 驱动程序的Executor的id。在Spark的早期版本中，这是“<driver>”，但这被更改为“driver”，
    * 因为角括号导致URL和XML中的转义问题（参见spark-6716详情）。
    *
   */
  private[spark] val DRIVER_IDENTIFIER = "driver"

  /**
   * Legacy version of DRIVER_IDENTIFIER, retained for backwards-compatibility.
   */
  private[spark] val LEGACY_DRIVER_IDENTIFIER = "<driver>"

  private implicit def arrayToArrayWritable[T <% Writable: ClassTag](arr: Traversable[T])
    : ArrayWritable = {
    def anyToWritable[U <% Writable](u: U): Writable = u

    new ArrayWritable(classTag[T].runtimeClass.asInstanceOf[Class[Writable]],
        arr.map(x => anyToWritable(x)).toArray)
  }

  /**
   * Find the JAR from which a given class was loaded, to make it easy for users to pass
   * their JARs to SparkContext.
    *
    * 找到加载某个给定类的JAR，为了是他变得容易的让用户加载SparkContext
   *
   * @param cls class that should be inside of the jar
   * @return jar that contains the Class, `None` if not found
   */
  def jarOfClass(cls: Class[_]): Option[String] = {
    val uri = cls.getResource("/" + cls.getName.replace('.', '/') + ".class")
    if (uri != null) {
      val uriStr = uri.toString
      if (uriStr.startsWith("jar:file:")) {
        // URI will be of the form "jar:file:/path/foo.jar!/package/cls.class",
        // so pull out the /path/foo.jar
        Some(uriStr.substring("jar:file:".length, uriStr.indexOf('!')))
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
   * Find the JAR that contains the class of a particular object, to make it easy for users
   * to pass their JARs to SparkContext. In most cases you can call jarOfObject(this) in
   * your driver program.
   *
    * 查找包含某个特定对象的类的jar，以方便用户通过他们的jars去使用sparkcontext。在大多数情况下，
    * 你可以调用jarOfObject(this)在你的驱动程序中。
    *
    *
   * @param obj reference to an instance which class should be inside of the jar
   * @return jar that contains the class of the instance, `None` if not found
   */
  def jarOfObject(obj: AnyRef): Option[String] = jarOfClass(obj.getClass)

  /**
   * Creates a modified version of a SparkConf with the parameters that can be passed separately
   * to SparkContext, to make it easier to write SparkContext's constructors. This ignores
   * parameters that are passed as the default value of null, instead of throwing an exception
   * like SparkConf would.
   */
  private[spark] def updatedConf(
      conf: SparkConf,
      master: String,
      appName: String,
      sparkHome: String = null,
      jars: Seq[String] = Nil,
      environment: Map[String, String] = Map()): SparkConf =
  {
    val res = conf.clone()
    res.setMaster(master)
    res.setAppName(appName)
    if (sparkHome != null) {
      res.setSparkHome(sparkHome)
    }
    if (jars != null && !jars.isEmpty) {
      res.setJars(jars)
    }
    res.setExecutorEnv(environment.toSeq)
    res
  }

  /**
   * The number of driver cores to use for execution in local mode, 0 otherwise.
    *  要在本地模式下执行的驱动程序内核的数量，否则为0。
   */
  private[spark] def numDriverCores(master: String): Int = {
    def convertToInt(threads: String): Int = {
      if (threads == "*") Runtime.getRuntime.availableProcessors() else threads.toInt
    }
    master match {
      case "local" => 1
      case SparkMasterRegex.LOCAL_N_REGEX(threads) => convertToInt(threads)
      case SparkMasterRegex.LOCAL_N_FAILURES_REGEX(threads, _) => convertToInt(threads)
      case _ => 0 // driver is not used for execution
    }
  }

  /**
   * Create a task scheduler based on a given master URL.
   * Return a 2-tuple of the scheduler backend and the task scheduler.
    * 创建一个调度器基于给定的一个主节点的URL
    * 返回两个东西  调度器后台 和 任务调度器
    *
    * TaskScheduler也是SparkContext的重要组成部分，负责任务的提交，并且请求集群管理器对人物的调度。
    * TaskScheduler也可以看做是任务调度的客户端。
    *
    * createTaskScheduler方法会根据master的匹配部署模式，创建TaskSchedulerImpl,并且生成不同的SchedulerBackend.
    *
   */
  private def createTaskScheduler(
      sc: SparkContext,
      master: String,
      deployMode: String): (SchedulerBackend, TaskScheduler) = {
    import SparkMasterRegex._

    // When running locally, don't try to re-execute tasks on failure. 在本地运行时，不要尝试重新执行失败的任务。
    val MAX_LOCAL_TASK_FAILURES = 1

    // 根据不同的部署方式，生成不同的scheduler和backend，现在主要是跟踪Standlone部署下的scheduler和backend的生成
    master match {
        /**
        // 匹配格式 .master("local")的 ,注意这里失败了不会重新运行失败的任务的
        */
      case "local" =>  // 单机模式
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalSchedulerBackend(sc.getConf, scheduler, 1)
        scheduler.initialize(backend)
        (backend, scheduler)

        /**
        // 匹配格式 .master("local[*]")或者.master("local[4]")的
        // 单机模式，包含几个core，其中threads代表core数目
        //  val LOCAL_N_REGEX = """local\[([0-9]+|\*)\]""".r
        */
      case LOCAL_N_REGEX(threads) =>
        // 本地[*]估计机器上的核数;local[N]完全使用N个线程。因为是local[*]运行，鬼知道*代表几个线程，因此需要先看看本地的核数
        def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
        // local[*] estimates the number of cores on the machine; local[N] uses exactly N threads.
        // 本地[*]估计机器上的核数;local[N]完全使用N个线程。 如果是*就是最大使用本地核数，一个核数运行一个线程，否则就按照指定的数量local[4],就是四个线程
        val threadCount = if (threads == "*") localCpuCount else threads.toInt
        if (threadCount <= 0) {
          throw new SparkException(s"Asked to run locally with $threadCount threads")
        }
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
        scheduler.initialize(backend)
        (backend, scheduler)

      /**
      // 匹配格式 .master("local[N,M]") N代表线程数，M代表一个任务连续失败了后，重新运行M次后仍然不成功，就彻底失败
      // val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+|\*)\s*,\s*([0-9]+)\]""".r
      */
      case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
        def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
        // local[*, M] means the number of cores on the computer with M failures
        // local[N, M] means exactly N threads with M failures
        val threadCount = if (threads == "*") localCpuCount else threads.toInt
        val scheduler = new TaskSchedulerImpl(sc, maxFailures.toInt, isLocal = true)
        val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
        scheduler.initialize(backend)
        (backend, scheduler)

      /**
      // 类似这样的masterurl -> .master("spark://192.168.10.83:7077,spark://192.168.10.84:7077")
      // 其中TaskSchedulerImpl用于记录task的调度信息，真正 task下发给work上的executor的是backend
      //   val SPARK_REGEX = """spark://(.*)""".r
      */
      case SPARK_REGEX(sparkUrl) =>
        val scheduler = new TaskSchedulerImpl(sc)
        // 从这一句可以看出master可以指定多个以逗号分隔，经过测试是可以的
        val masterUrls = sparkUrl.split(",").map("spark://" + _)
        // 实例化SparkDeploySchedulerBackend(Standalone模式)
        val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
        // 执行TaskSchedulerImpl的初始化操作
        scheduler.initialize(backend)
        (backend, scheduler)

      /**
        * 匹配local-cluster[N, cores, memory]，它是一种伪分布式模式 // 匹配本地集群模式 Local-cluster，
        * val LOCAL_CLUSTER_REGEX = """local-cluster\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*]""".r
        *
        * 测试或实验性质的本地伪集群运行模式（单机模拟集群）
        *
        *     这种运行模式，和Local[N]很像，不同的是，它会在单机启动多个进程来模拟集群下的分布式场景，而不像Local[N]这种多个线程只能在一个
        *  进程下委屈求全的共享资源。通常也是用来验证开发出来的应用程序逻辑上有没有问题，或者想使用Spark的计算框架而没有太多资源。
        *
        *     用法是：提交应用程序时使用local-cluster[x,y,z]参数：x代表要生成的executor数，y和z分别代表每个executor所拥有的core
        *   和memory数。
        *
        *   spark-submit --master local-cluster[2, 3, 1024]
        *
        *   这个只有在：$SPARK_HOME/bin/spark-submit --name "lcc_sparkSql_check" --master local-cluster[2,3,1024]
        *             --class HbaseDataCheck.HbaseDataCheck /home/lcc/hbaseCount/SparkOnHbaseScala.jar
        *   下才可以用，代码直接运行不可以
        *
        *
        *   在集群中创建了一个Spark standalone线程，Master和Workers在同一个jvm中，而由workers节点启动的Execuotrs仍然运行在分离的JVM中
        *   有点类似于Standalone模式
        *
        *   如下：
        *   val sparkConf = new SparkConf().setMaster("local-cluster[1, 1, 200]").setAppName("aa")
        *   将会报错
        *   Exception in thread "main" org.apache.spark.SparkException: Asked to launch cluster with 200 MB RAM / worker but requested 1024 MB/worker
        *   memoryPerSlave要大于executorMemory=1024M
        *
        */
      case LOCAL_CLUSTER_REGEX(numSlaves, coresPerSlave, memoryPerSlave) =>
        // 其中taskschedule采用了TaskSchedulerImpl 资源调度采用了SparkDeploySchedulerBackend
        // Check to make sure memory requested <= memoryPerSlave. Otherwise Spark will just hang.
        // 检查以确保 内存请求< = memoryPerSlave。否则，Spark就会挂起。
        val memoryPerSlaveInt = memoryPerSlave.toInt
        if (sc.executorMemory > memoryPerSlaveInt) {
          throw new SparkException(
            "Asked to launch cluster with %d MB RAM / worker but requested %d MB/worker".format(
              memoryPerSlaveInt, sc.executorMemory))
        }

        val scheduler = new TaskSchedulerImpl(sc)
        // 测试类,在集群中创建了一个Spark standalone线程，Master和Workers在同一个jvm中，而由workers节点启动的Execuotrs仍然运行在分离的JVM中
        val localCluster = new LocalSparkCluster(
          numSlaves.toInt, coresPerSlave.toInt, memoryPerSlaveInt, sc.conf)
        val masterUrls = localCluster.start()

        // 1.5版本这里是SparkDeploySchedulerBackend(scheduler, sc, masterUrls)
        val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
        scheduler.initialize(backend)
        backend.shutdownCallback = (backend: StandaloneSchedulerBackend) => {
          localCluster.stop()
        }
        (backend, scheduler)


      /**
        // 这里匹配任意值,比如Yarn,mose，m3之类的其他资源管理器,这里以Yarn进行讲解
        */
      case masterUrl =>
        val cm = getClusterManager(masterUrl) match {
          case Some(clusterMgr) => clusterMgr
          case None => throw new SparkException("Could not parse Master URL: '" + master + "'")
        }
        try {
          // 一个集群管理器接口，用于插件外部调度器。 ExternalClusterManager，这个是一个特质，因此看它的继承类或者实现类，
          // 假设是Yarn模式，那么就看YarnClusterManager，然后我们去看看YarnClusterManager
          val scheduler = cm.createTaskScheduler(sc, masterUrl)
          val backend = cm.createSchedulerBackend(sc, masterUrl, scheduler)
          cm.initialize(scheduler, backend)
          (backend, scheduler)
        } catch {
          case se: SparkException => throw se
          case NonFatal(e) =>
            throw new SparkException("External scheduler cannot be instantiated", e)
        }
    }
  }

  private def getClusterManager(url: String): Option[ExternalClusterManager] = {
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoaders =
      ServiceLoader.load(classOf[ExternalClusterManager], loader).asScala.filter(_.canCreate(url))
    if (serviceLoaders.size > 1) {
      throw new SparkException(
        s"Multiple external cluster managers registered for the url $url: $serviceLoaders")
    }
    serviceLoaders.headOption
  }
}

/**
 * A collection of regexes for extracting information from the master string.
  *
  *   从master字符串中提取信息集合的正则表达式。
  *
 */
private object SparkMasterRegex {
  // Regular expression used for local[N] and local[*] master formats
  val LOCAL_N_REGEX = """local\[([0-9]+|\*)\]""".r
  // Regular expression for local[N, maxRetries], used in tests with failing tasks
  val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+|\*)\s*,\s*([0-9]+)\]""".r
  // Regular expression for simulating a Spark cluster of [N, cores, memory] locally
  val LOCAL_CLUSTER_REGEX = """local-cluster\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*]""".r
  // Regular expression for connecting to Spark deploy clusters
  val SPARK_REGEX = """spark://(.*)""".r
}

/**
 * A class encapsulating how to convert some type `T` from `Writable`. It stores both the `Writable`
 * class corresponding to `T` (e.g. `IntWritable` for `Int`) and a function for doing the
 * conversion.
 * The getter for the writable class takes a `ClassTag[T]` in case this is a generic object
 * that doesn't know the type of `T` when it is created. This sounds strange but is necessary to
 * support converting subclasses of `Writable` to themselves (`writableWritableConverter()`).
  *
  * 一个类封装如何转换成某种类型` T `从`Writable`。它存储的`Writable`类对应` T `（例如` intwritable `为` int `）
  * 和一个函数转换此功能
  * The getter for the writable class ` classtag [t] `的情况下，这是一个通用的对象，不知道类型的` T `创建时。
  * 这听起来很奇怪，但却是必要的。 支持转换子`Writable`自己（` writablewritableconverter() `）。
  *
  *
 */
private[spark] class WritableConverter[T](
    val writableClass: ClassTag[T] => Class[_ <: Writable],
    val convert: Writable => T)
  extends Serializable

object WritableConverter {

  // Helper objects for converting common types to Writable
  private[spark] def simpleWritableConverter[T, W <: Writable: ClassTag](convert: W => T)
  : WritableConverter[T] = {
    val wClass = classTag[W].runtimeClass.asInstanceOf[Class[W]]
    new WritableConverter[T](_ => wClass, x => convert(x.asInstanceOf[W]))
  }

  // The following implicit functions were in SparkContext before 1.3 and users had to
  // `import SparkContext._` to enable them. Now we move them here to make the compiler find
  // them automatically. However, we still keep the old functions in SparkContext for backward
  // compatibility and forward to the following functions directly.

  implicit def intWritableConverter(): WritableConverter[Int] =
    simpleWritableConverter[Int, IntWritable](_.get)

  implicit def longWritableConverter(): WritableConverter[Long] =
    simpleWritableConverter[Long, LongWritable](_.get)

  implicit def doubleWritableConverter(): WritableConverter[Double] =
    simpleWritableConverter[Double, DoubleWritable](_.get)

  implicit def floatWritableConverter(): WritableConverter[Float] =
    simpleWritableConverter[Float, FloatWritable](_.get)

  implicit def booleanWritableConverter(): WritableConverter[Boolean] =
    simpleWritableConverter[Boolean, BooleanWritable](_.get)

  implicit def bytesWritableConverter(): WritableConverter[Array[Byte]] = {
    simpleWritableConverter[Array[Byte], BytesWritable] { bw =>
      // getBytes method returns array which is longer then data to be returned
      Arrays.copyOfRange(bw.getBytes, 0, bw.getLength)
    }
  }

  implicit def stringWritableConverter(): WritableConverter[String] =
    simpleWritableConverter[String, Text](_.toString)

  implicit def writableWritableConverter[T <: Writable](): WritableConverter[T] =
    new WritableConverter[T](_.runtimeClass.asInstanceOf[Class[T]], _.asInstanceOf[T])
}

/**
 * A class encapsulating how to convert some type `T` to `Writable`. It stores both the `Writable`
 * class corresponding to `T` (e.g. `IntWritable` for `Int`) and a function for doing the
 * conversion.
 * The `Writable` class will be used in `SequenceFileRDDFunctions`.
 */
private[spark] class WritableFactory[T](
    val writableClass: ClassTag[T] => Class[_ <: Writable],
    val convert: T => Writable) extends Serializable

object WritableFactory {

  private[spark] def simpleWritableFactory[T: ClassTag, W <: Writable : ClassTag](convert: T => W)
    : WritableFactory[T] = {
    val writableClass = implicitly[ClassTag[W]].runtimeClass.asInstanceOf[Class[W]]
    new WritableFactory[T](_ => writableClass, convert)
  }

  implicit def intWritableFactory: WritableFactory[Int] =
    simpleWritableFactory(new IntWritable(_))

  implicit def longWritableFactory: WritableFactory[Long] =
    simpleWritableFactory(new LongWritable(_))

  implicit def floatWritableFactory: WritableFactory[Float] =
    simpleWritableFactory(new FloatWritable(_))

  implicit def doubleWritableFactory: WritableFactory[Double] =
    simpleWritableFactory(new DoubleWritable(_))

  implicit def booleanWritableFactory: WritableFactory[Boolean] =
    simpleWritableFactory(new BooleanWritable(_))

  implicit def bytesWritableFactory: WritableFactory[Array[Byte]] =
    simpleWritableFactory(new BytesWritable(_))

  implicit def stringWritableFactory: WritableFactory[String] =
    simpleWritableFactory(new Text(_))

  implicit def writableWritableFactory[T <: Writable: ClassTag]: WritableFactory[T] =
    simpleWritableFactory(w => w)

}
