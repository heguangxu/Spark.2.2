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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.LinkedHashSet

import org.apache.avro.{Schema, SchemaNormalization}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.Utils

/**
 * Configuration for a Spark application. Used to set various Spark parameters as key-value pairs.
  * 配置spark的application应用程序，实用key-value对的形式进行配置
 *
 * Most of the time, you would create a SparkConf object with `new SparkConf()`, which will load
 * values from any `spark.*` Java system properties set in your application as well. In this case,
 * parameters you set directly on the `SparkConf` object take priority over system properties.
 *
  * new SparkConf()将会读取任何spark.*的配置，包括你写的应用程序中的设置
  *
 * For unit tests, you can also call `new SparkConf(false)` to skip loading external settings and
 * get the same configuration no matter what the system properties are.
 *
  * 在配置单元里你可以new SparkConf(false) 跳过加载外在的配置，可以得到一样的配置不管system properties 系统配置在哪里
  *
 * All setter methods in this class support chaining. For example, you can write
 * `new SparkConf().setMaster("local").setAppName("My app")`.
 *
  * 在这个类中的set方法都允许修改里面的值
  *
 * @param loadDefaults whether to also load values from Java system properties
 *
 * @note Once a SparkConf object is passed to Spark, it is cloned and can no longer be modified
 * by the user. Spark does not support modifying the configuration at runtime.
  * spark不允许在运行的时候，修改配置
  *
 */
class SparkConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable {

  import SparkConf._

  /** Create a SparkConf that loads defaults from system properties and the classpath
    * 创建一个SparkConf，它读取系统配置和classpath的配置
    * */
  // 步骤：1
  def this() = this(true)

  // SparkConf的构造结构很简单，主要是通过ConcurrentHashMap来维护各种Spark的配置属性。
  private val settings = new ConcurrentHashMap[String, String]()

  //@transient在存储对象状态时，我们有时候会需要特定的对象数据在serialization时不进行存储。这时候transient关键字就派上用场了。
  @transient private lazy val reader: ConfigReader = {
    val _reader = new ConfigReader(new SparkConfigProvider(settings))
    _reader.bindEnv(new ConfigProvider {
      override def get(key: String): Option[String] = Option(getenv(key))
    })
    _reader
  }

  // 步骤：2
  if (loadDefaults) {
    loadFromSystemProperties(false)
  }

  // 步骤：3
  private[spark] def loadFromSystemProperties(silent: Boolean): SparkConf = {
    // Load any spark.* system properties
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("spark.")) {
      set(key, value, silent)
    }
    this
  }

  /** Set a configuration variable.
    * 设置配置的变量 false是指代这个配置是过时的配置
    *
    * */
  def set(key: String, value: String): SparkConf = {
    // 下面就是设置函数
    set(key, value, false)
  }

  //设置函数
  private[spark] def set(key: String, value: String, silent: Boolean): SparkConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    if (!silent) {
      logDeprecationWarning(key)
    }
    settings.put(key, value)
    this
  }

  private[spark] def set[T](entry: ConfigEntry[T], value: T): SparkConf = {
    set(entry.key, entry.stringConverter(value))
    this
  }

  private[spark] def set[T](entry: OptionalConfigEntry[T], value: T): SparkConf = {
    set(entry.key, entry.rawStringConverter(value))
    this
  }

  /**
   * The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to
   * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
    * 设置spark的spark.master 的 URL
   */
  def setMaster(master: String): SparkConf = {
    set("spark.master", master)
  }

  /** Set a name for your application. Shown in the Spark web UI.
    * 设置你的应用名称
    * */
  def setAppName(name: String): SparkConf = {
    set("spark.app.name", name)
  }

  /** Set JAR files to distribute to the cluster. */
  def setJars(jars: Seq[String]): SparkConf = {
    for (jar <- jars if (jar == null)) logWarning("null jar passed to SparkContext constructor")
    // 这是spark.jar参数，每个参数一逗号分隔
    set("spark.jars", jars.filter(_ != null).mkString(","))
  }

  /** Set JAR files to distribute to the cluster. (Java-friendly version.) */
  def setJars(jars: Array[String]): SparkConf = {
    setJars(jars.toSeq)
  }

  /**
   * Set an environment variable to be used when launching executors for this application.
   * These variables are stored as properties of the form spark.executorEnv.VAR_NAME
   * (for example spark.executorEnv.PATH) but this method makes them easier to set.
    * 设置你的应用程序的executors的环境变量
   */
  def setExecutorEnv(variable: String, value: String): SparkConf = {
    set("spark.executorEnv." + variable, value)
  }

  /**
   * Set multiple environment variables to be used when launching executors.
   * These variables are stored as properties of the form spark.executorEnv.VAR_NAME
   * (for example spark.executorEnv.PATH) but this method makes them easier to set.
    * 设置启动executors时要使用的多个环境变量。
    * 这些变量被存储为spark.executorEnv.VAR_NAME（例如 spark.executorEnv.PATH）
    * 但这种方法使他们更容易设置。
   */
  def setExecutorEnv(variables: Seq[(String, String)]): SparkConf = {
    for ((k, v) <- variables) {
      setExecutorEnv(k, v)
    }
    this
  }

  /**
   * Set multiple environment variables to be used when launching executors.
   * (Java-friendly version.)
    * 设置启动executors时候的环境变量
   */
  def setExecutorEnv(variables: Array[(String, String)]): SparkConf = {
    setExecutorEnv(variables.toSeq)
  }

  /**
   * Set the location where Spark is installed on worker nodes.
    * 设置spark的安装目录，在work节点，有可能安装节点不一样
   */
  def setSparkHome(home: String): SparkConf = {
    set("spark.home", home)
  }

  /** Set multiple parameters together
    * 设置多餐数据的配置
    * */
  def setAll(settings: Traversable[(String, String)]): SparkConf = {
    settings.foreach { case (k, v) => set(k, v) }
    this
  }

  /** Set a parameter if it isn't already configured
    * 设置没有默认配置的参数
    * */
  def setIfMissing(key: String, value: String): SparkConf = {
    if (settings.putIfAbsent(key, value) == null) {
      logDeprecationWarning(key)
    }
    this
  }

  private[spark] def setIfMissing[T](entry: ConfigEntry[T], value: T): SparkConf = {
    if (settings.putIfAbsent(entry.key, entry.stringConverter(value)) == null) {
      logDeprecationWarning(entry.key)
    }
    this
  }

  private[spark] def setIfMissing[T](entry: OptionalConfigEntry[T], value: T): SparkConf = {
    if (settings.putIfAbsent(entry.key, entry.rawStringConverter(value)) == null) {
      logDeprecationWarning(entry.key)
    }
    this
  }

  /**
   * Use Kryo serialization and register the given set of classes with Kryo.
   * If called multiple times, this will append the classes from all calls together.
    * 使用kryo序列化和登记给定的类集kryo。如果多次调用，它将从所有调用中追加类。
    * Kryo:快速、高效的序列化框架,序列化后的大小比Java序列化小，速度比Java快
    * 使用方法：
    * conf.registerKryoClasses(
    *   new Class[] {
    *     net.sf.json.JSONObject.class,
    *     java.util.List.class,
    *     java.util.Map.class,
    *     java.util.ArrayList.class,
    *     java.util.HashMap.class
    *     }
    * );
    */
  def registerKryoClasses(classes: Array[Class[_]]): SparkConf = {
    val allClassNames = new LinkedHashSet[String]()
    allClassNames ++= get("spark.kryo.classesToRegister", "").split(',').map(_.trim)
      .filter(!_.isEmpty)
    allClassNames ++= classes.map(_.getName)

    set("spark.kryo.classesToRegister", allClassNames.mkString(","))
    set("spark.serializer", classOf[KryoSerializer].getName)
    this
  }

  private final val avroNamespace = "avro.schema."

  /**
   * Use Kryo serialization and register the given set of Avro schemas so that the generic
   * record serializer can decrease network IO
   */
  def registerAvroSchemas(schemas: Schema*): SparkConf = {
    for (schema <- schemas) {
      set(avroNamespace + SchemaNormalization.parsingFingerprint64(schema), schema.toString)
    }
    this
  }

  /** Gets all the avro schemas in the configuration used in the generic Avro record serializer */
  def getAvroSchema: Map[Long, String] = {
    getAll.filter { case (k, v) => k.startsWith(avroNamespace) }
      .map { case (k, v) => (k.substring(avroNamespace.length).toLong, v) }
      .toMap
  }

  /** Remove a parameter from the configuration
    * 从配置中删除某个配置
    * */
  def remove(key: String): SparkConf = {
    settings.remove(key)
    this
  }

  private[spark] def remove(entry: ConfigEntry[_]): SparkConf = {
    remove(entry.key)
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set
    * 根据Key获取配置的值，如果值不存在，就报错NoSuchElementException
    * */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set
    * 得到一个默认配置，如果这个配置没有被手动设置
    * */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /**
   * Retrieves the value of a pre-defined configuration entry.
    * 检索预定义配置项的值。
   *
   * - This is an internal Spark API.
    * 这是一个内在Spark API
   * - The return type if defined by the configuration entry.
    * 返回类型如果由配置项定义。
   * - This will throw an exception is the config is not optional and the value is not set.
    * 这将抛出一个异常，该配置不是可选的，并且值没有设置
   */
  private[spark] def get[T](entry: ConfigEntry[T]): T = {
    entry.readFrom(reader)
  }

  /**
   * Get a time parameter as seconds; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then seconds are assumed.
    * 得到一个时间参数为秒；抛出一个NoSuchElementException，如果不设置。如果没有提供后缀就会假设。
   * @throws java.util.NoSuchElementException If the time parameter is not set
   */
  def getTimeAsSeconds(key: String): Long = {
    Utils.timeStringAsSeconds(get(key))
  }

  /**
   * Get a time parameter as seconds, falling back to a default if not set. If no
   * suffix is provided then seconds are assumed.
   */
  def getTimeAsSeconds(key: String, defaultValue: String): Long = {
    Utils.timeStringAsSeconds(get(key, defaultValue))
  }

  /**
   * Get a time parameter as milliseconds; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then milliseconds are assumed.
   * @throws java.util.NoSuchElementException If the time parameter is not set
   */
  def getTimeAsMs(key: String): Long = {
    Utils.timeStringAsMs(get(key))
  }

  /**
   * Get a time parameter as milliseconds, falling back to a default if not set. If no
   * suffix is provided then milliseconds are assumed.
   */
  def getTimeAsMs(key: String, defaultValue: String): Long = {
    Utils.timeStringAsMs(get(key, defaultValue))
  }

  /**
   * Get a size parameter as bytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then bytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   */
  def getSizeAsBytes(key: String): Long = {
    Utils.byteStringAsBytes(get(key))
  }

  /**
   * Get a size parameter as bytes, falling back to a default if not set. If no
   * suffix is provided then bytes are assumed.
   */
  def getSizeAsBytes(key: String, defaultValue: String): Long = {
    Utils.byteStringAsBytes(get(key, defaultValue))
  }

  /**
   * Get a size parameter as bytes, falling back to a default if not set.
   */
  def getSizeAsBytes(key: String, defaultValue: Long): Long = {
    Utils.byteStringAsBytes(get(key, defaultValue + "B"))
  }

  /**
   * Get a size parameter as Kibibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Kibibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   */
  def getSizeAsKb(key: String): Long = {
    Utils.byteStringAsKb(get(key))
  }

  /**
   * Get a size parameter as Kibibytes, falling back to a default if not set. If no
   * suffix is provided then Kibibytes are assumed.
   */
  def getSizeAsKb(key: String, defaultValue: String): Long = {
    Utils.byteStringAsKb(get(key, defaultValue))
  }

  /**
   * Get a size parameter as Mebibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Mebibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   */
  def getSizeAsMb(key: String): Long = {
    Utils.byteStringAsMb(get(key))
  }

  /**
   * Get a size parameter as Mebibytes, falling back to a default if not set. If no
   * suffix is provided then Mebibytes are assumed.
   */
  def getSizeAsMb(key: String, defaultValue: String): Long = {
    Utils.byteStringAsMb(get(key, defaultValue))
  }

  /**
   * Get a size parameter as Gibibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Gibibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   */
  def getSizeAsGb(key: String): Long = {
    Utils.byteStringAsGb(get(key))
  }

  /**
   * Get a size parameter as Gibibytes, falling back to a default if not set. If no
   * suffix is provided then Gibibytes are assumed.
   */
  def getSizeAsGb(key: String, defaultValue: String): Long = {
    Utils.byteStringAsGb(get(key, defaultValue))
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key)).orElse(getDeprecatedConfig(key, this))
  }

  /** Get all parameters as a list of pairs
    * 得到key-value形式的数组配置信息
    * */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /**
   * Get all parameters that start with `prefix`
    *
    * 得到所有前缀是prefix参数的配置
   */
  def getAllWithPrefix(prefix: String): Array[(String, String)] = {
    getAll.filter { case (k, v) => k.startsWith(prefix) }
      .map { case (k, v) => (k.substring(prefix.length), v) }
  }


  /** Get a parameter as an integer, falling back to a default if not set
    * 得到一个配置是integer的配置，如果出错就设置为默认的配置
    * */
  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Get all executor environment variables set on this SparkConf */
  def getExecutorEnv: Seq[(String, String)] = {
    getAllWithPrefix("spark.executorEnv.")
  }

  /**
   * Returns the Spark application id, valid in the Driver after TaskScheduler registration and
   * from the start in the Executor.
    * 得到应用程序的appID
   */
  def getAppId: String = get("spark.app.id")

  /** Does the configuration contain a given parameter?
    * 配置中是否包含某个key的配置
    * */
  def contains(key: String): Boolean = {
    settings.containsKey(key) ||
      configsWithAlternatives.get(key).toSeq.flatten.exists { alt => contains(alt.key) }
  }

  private[spark] def contains(entry: ConfigEntry[_]): Boolean = contains(entry.key)

  /** Copy this object
    * 克隆一个对象
    * 循环配置实体的key-value
    * */
  override def clone: SparkConf = {
    val cloned = new SparkConf(false)
    settings.entrySet().asScala.foreach { e =>
      cloned.set(e.getKey(), e.getValue(), true)
    }
    cloned
  }

  /**
   * By using this instead of System.getenv(), environment variables can be mocked
   * in unit tests.
    * 通过使用它而不是系统。getenv()，环境变量可以被单元测试。
   */
  private[spark] def getenv(name: String): String = System.getenv(name)

  /**
   * Checks for illegal or deprecated config settings. Throws an exception for the former. Not
   * idempotent - may mutate this conf object to convert deprecated settings to supported ones.
    *
    * 检查不合法的配置或者是过时的配置，跑出一个异常
    *
   */
  private[spark] def validateSettings() {
    if (contains("spark.local.dir")) {
      val msg = "In Spark 1.0 and later spark.local.dir will be overridden by the value set by " +
        "the cluster manager (via SPARK_LOCAL_DIRS in mesos/standalone and LOCAL_DIRS in YARN)."
      logWarning(msg)
    }

    val executorOptsKey = "spark.executor.extraJavaOptions"
    val executorClasspathKey = "spark.executor.extraClassPath"
    val driverOptsKey = "spark.driver.extraJavaOptions"
    val driverClassPathKey = "spark.driver.extraClassPath"
    val driverLibraryPathKey = "spark.driver.extraLibraryPath"
    val sparkExecutorInstances = "spark.executor.instances"

    // Used by Yarn in 1.1 and before
    sys.props.get("spark.driver.libraryPath").foreach { value =>
      val warning =
        s"""
          |spark.driver.libraryPath was detected (set to '$value').
          |This is deprecated in Spark 1.2+.
          |
          |Please instead use: $driverLibraryPathKey
        """.stripMargin
      logWarning(warning)
    }

    /*
    运行spark-shell的时候会出现
    This is deprecated in Spark 1.0+.

    Please instead use:
     - ./spark-submit with --num-executors to specify the number of executors
     - Or set SPARK_EXECUTOR_INSTANCES
     - spark.executor.instances to configure the number of instances in the spark config.
     */

    // Validate spark.executor.extraJavaOptions
    getOption(executorOptsKey).foreach { javaOpts =>
      if (javaOpts.contains("-Dspark")) {
        val msg = s"$executorOptsKey is not allowed to set Spark options (was '$javaOpts'). " +
          "Set them directly on a SparkConf or in a properties file when using ./bin/spark-submit."
        throw new Exception(msg)
      }
      if (javaOpts.contains("-Xmx")) {
        val msg = s"$executorOptsKey is not allowed to specify max heap memory settings " +
          s"(was '$javaOpts'). Use spark.executor.memory instead."
        throw new Exception(msg)
      }
    }

    // Validate memory fractions
    // 检测内存的一些函数
    val deprecatedMemoryKeys = Seq(
      "spark.storage.memoryFraction",
      "spark.shuffle.memoryFraction",
      "spark.shuffle.safetyFraction",
      "spark.storage.unrollFraction",
      "spark.storage.safetyFraction")
    val memoryKeys = Seq(
      "spark.memory.fraction",
      "spark.memory.storageFraction") ++
      deprecatedMemoryKeys
    for (key <- memoryKeys) {
      val value = getDouble(key, 0.5)
      if (value > 1 || value < 0) {
        throw new IllegalArgumentException(s"$key should be between 0 and 1 (was '$value').")
      }
    }

    // Warn against deprecated memory fractions (unless legacy memory management mode is enabled)
    // 反对过时的memory 分数（除非传统的内存管理模式已启用）
    // 在Spark-1.6.0中，引入了一个新的参数spark.memory.userLegacyMode（默认值为false），
    // 表示不使用Spark-1.6.0之前的内存管理机制，而是使用1.6.0中引入的动态内存分配这一概念。
    val legacyMemoryManagementKey = "spark.memory.useLegacyMode"
    val legacyMemoryManagement = getBoolean(legacyMemoryManagementKey, false)
    if (!legacyMemoryManagement) {
      val keyset = deprecatedMemoryKeys.toSet
      val detected = settings.keys().asScala.filter(keyset.contains)
      if (detected.nonEmpty) {
        logWarning("Detected deprecated memory fraction settings: " +
          detected.mkString("[", ", ", "]") + ". As of Spark 1.6, execution and storage " +
          "memory management are unified. All memory fractions used in the old model are " +
          "now deprecated and no longer read. If you wish to use the old memory management, " +
          s"you may explicitly enable `$legacyMemoryManagementKey` (not recommended).")
      }
    }

    if (contains("spark.master") && get("spark.master").startsWith("yarn-")) {
      val warning = s"spark.master ${get("spark.master")} is deprecated in Spark 2.0+, please " +
        "instead use \"yarn\" with specified deploy mode."

      get("spark.master") match {
        case "yarn-cluster" =>
          logWarning(warning)
          set("spark.master", "yarn")
          set("spark.submit.deployMode", "cluster")
        case "yarn-client" =>
          logWarning(warning)
          set("spark.master", "yarn")
          set("spark.submit.deployMode", "client")
        case _ => // Any other unexpected master will be checked when creating scheduler backend.
      }
    }

    if (contains("spark.submit.deployMode")) {
      get("spark.submit.deployMode") match {
        case "cluster" | "client" =>
        case e => throw new SparkException("spark.submit.deployMode can only be \"cluster\" or " +
          "\"client\".")
      }
    }

    val encryptionEnabled = get(NETWORK_ENCRYPTION_ENABLED) || get(SASL_ENCRYPTION_ENABLED)
    require(!encryptionEnabled || get(NETWORK_AUTH_ENABLED),
      s"${NETWORK_AUTH_ENABLED.key} must be enabled when enabling encryption.")
  }

  /**
   * Return a string listing all keys and values, one per line. This is useful to print the
   * configuration out for debugging.
    *
    * 这个返回一个key-value的行，这个主要是调试的时候，打印配置信息
   */
  def toDebugString: String = {
    getAll.sorted.map{case (k, v) => k + "=" + v}.mkString("\n")
  }

}




//################################ object SparkConf ##########################################################

private[spark] object SparkConf extends Logging {

  /**
   * Maps deprecated config keys to information about the deprecation.
    * map里存放过时的配置
   *
   * The extra information is logged as a warning when the config is present in the user's
   * configuration.
    * 当用户配置中存在配置时，额外的信息被记录为警告。
    *
    * 这里是一些过时的配置
   */
  private val deprecatedConfigs: Map[String, DeprecatedConfig] = {
    val configs = Seq(
      DeprecatedConfig("spark.cache.class", "0.8",
        "The spark.cache.class property is no longer being used! Specify storage levels using " +
        "the RDD.persist() method instead."),
      DeprecatedConfig("spark.yarn.user.classpath.first", "1.3",
        "Please use spark.{driver,executor}.userClassPathFirst instead."),
      DeprecatedConfig("spark.kryoserializer.buffer.mb", "1.4",
        "Please use spark.kryoserializer.buffer instead. The default value for " +
          "spark.kryoserializer.buffer.mb was previously specified as '0.064'. Fractional values " +
          "are no longer accepted. To specify the equivalent now, one may use '64k'."),
      DeprecatedConfig("spark.rpc", "2.0", "Not used any more."),
      DeprecatedConfig("spark.scheduler.executorTaskBlacklistTime", "2.1.0",
        "Please use the new blacklisting options, spark.blacklist.*")
    )

    Map(configs.map { cfg => (cfg.key -> cfg) } : _*)
  }

  /**
   * Maps a current config key to alternate keys that were used in previous version of Spark.
    * 将当前配置键映射到以前版本的Spark中使用的备用key。
    *
   * The alternates are used in the order defined in this map. If deprecated configs are
   * present in the user's configuration, a warning is logged.
    *
   */
  private val configsWithAlternatives = Map[String, Seq[AlternateConfig]](
    "spark.executor.userClassPathFirst" -> Seq(
      AlternateConfig("spark.files.userClassPathFirst", "1.3")),
    "spark.history.fs.update.interval" -> Seq(
      AlternateConfig("spark.history.fs.update.interval.seconds", "1.4"),
      AlternateConfig("spark.history.fs.updateInterval", "1.3"),
      AlternateConfig("spark.history.updateInterval", "1.3")),
    "spark.history.fs.cleaner.interval" -> Seq(
      AlternateConfig("spark.history.fs.cleaner.interval.seconds", "1.4")),
    "spark.history.fs.cleaner.maxAge" -> Seq(
      AlternateConfig("spark.history.fs.cleaner.maxAge.seconds", "1.4")),

    /**
      * 优化：
      *   spark.yarn.applicationMaster.waitTries在spark1.3版本开始过时了，这个是ApplicationMaster容许
      *   SparkContext初始化失败的最大次数。
      *
      *   现在改成配置spark.yarn.am.waitTime，重试的等待时间，默认是10秒，递增10 20 30 。。。
      */
    "spark.yarn.am.waitTime" -> Seq(
      AlternateConfig("spark.yarn.applicationMaster.waitTries", "1.3",
        // Translate old value to a duration, with 10s wait time per try.
        translation = s => s"${s.toLong * 10}s")),

    /**
      * 优化：spark.reducer.maxMbInFlight  1。4版本过时
      *     每个reduce任务同时获取map输出的最大值（以M为字节单位）。由于每个map输出都需要一个缓冲区
      *  来接收它，这代表着每个reduce任务有着固定的内存开销，所以要设置小点，除非有很大的内存。
      *
      */
    "spark.reducer.maxSizeInFlight" -> Seq(
      AlternateConfig("spark.reducer.maxMbInFlight", "1.4")),
    "spark.kryoserializer.buffer" ->
        Seq(AlternateConfig("spark.kryoserializer.buffer.mb", "1.4",
          translation = s => s"${(s.toDouble * 1000).toInt}k")),
    "spark.kryoserializer.buffer.max" -> Seq(
      AlternateConfig("spark.kryoserializer.buffer.max.mb", "1.4")),

    /**
      * 优化：spark.shuffle.file.buffer
      *
      *   spark中shuffle输出的ShuffleMapTask为每个ResultTask创建对应的Bucket，ShuffleMapTask
      * 产生的结果会根据设置的partitioner的到对应的BucketId，然后填充到相应的Bucket中。每个ShuffleMapTask的
      * 输出结果可能包含所有ResultTask素偶需要的数据，所以每个ShuffleMapTask创建的Bucket的数目和ResultTask的
      * 数目是相当的。
      *
      *   ShuffleMapTask创建的Bucket对应磁盘上的一个文件，用于存储结果，此文件被称为BlockFile。通过
      *  spark.shuffle.file.buffer.kb属性配置的缓冲区就是用来chuangjianFastBufferedOutputStream输出流的，
      *  如果配置文件中设置了spark.shuffle.consolidateFiles属性为true，则ShuffleMapTask所产生的Bucket就不一
      *  定单独对应一个文件了，而是对应文件的一部分，这样做会大量减少产生的BlockFile文件数量。
      */
    "spark.shuffle.file.buffer" -> Seq(
      AlternateConfig("spark.shuffle.file.buffer.kb", "1.4")),
    "spark.executor.logs.rolling.maxSize" -> Seq(
      AlternateConfig("spark.executor.logs.rolling.size.maxBytes", "1.4")),
    "spark.io.compression.snappy.blockSize" -> Seq(
      AlternateConfig("spark.io.compression.snappy.block.size", "1.4")),
    "spark.io.compression.lz4.blockSize" -> Seq(
      AlternateConfig("spark.io.compression.lz4.block.size", "1.4")),
    "spark.rpc.numRetries" -> Seq(
      AlternateConfig("spark.akka.num.retries", "1.4")),
    "spark.rpc.retry.wait" -> Seq(
      AlternateConfig("spark.akka.retry.wait", "1.4")),
    "spark.rpc.askTimeout" -> Seq(
      AlternateConfig("spark.akka.askTimeout", "1.4")),
    "spark.rpc.lookupTimeout" -> Seq(
      AlternateConfig("spark.akka.lookupTimeout", "1.4")),
    "spark.streaming.fileStream.minRememberDuration" -> Seq(
      AlternateConfig("spark.streaming.minRememberDuration", "1.5")),

    /**
      * 优化：spark.yarn.max.executor.failures
      *
      *   Executor失败的最大次数。
      *
      */
    "spark.yarn.max.executor.failures" -> Seq(
      AlternateConfig("spark.yarn.max.worker.failures", "1.5")),

    /**
      * 根据spark.memory.offHeap.enabled参数（默认为false）来决定是ON_HEAP还是OFF_HEAP
      */
    "spark.memory.offHeap.enabled" -> Seq(
      AlternateConfig("spark.unsafe.offHeap", "1.6")),
    "spark.rpc.message.maxSize" -> Seq(
      AlternateConfig("spark.akka.frameSize", "1.6")),
    "spark.yarn.jars" -> Seq(
      AlternateConfig("spark.yarn.jar", "2.0")),
    "spark.yarn.access.hadoopFileSystems" -> Seq(
      AlternateConfig("spark.yarn.access.namenodes", "2.2"))
  )

  /**
   * A view of `configsWithAlternatives` that makes it more efficient to look up deprecated
   * config keys.
   *
   * Maps the deprecated config name to a 2-tuple (new config name, alternate config info).
    *
    * 查看所有的过时的配置
   */
  private val allAlternatives: Map[String, (String, AlternateConfig)] = {
    configsWithAlternatives.keys.flatMap { key =>
      configsWithAlternatives(key).map { cfg => (cfg.key -> (key -> cfg)) }
    }.toMap
  }

  /**
   * Return whether the given config should be passed to an executor on start-up.
   *
   * Certain authentication configs are required from the executor when it connects to
   * the scheduler, while the rest of the spark configs can be inherited from the driver later.
    * 必须的配置信息
    *
   */
  def isExecutorStartupConf(name: String): Boolean = {
    (name.startsWith("spark.auth") && name != SecurityManager.SPARK_AUTH_SECRET_CONF) ||
    name.startsWith("spark.ssl") ||
    name.startsWith("spark.rpc") ||
    name.startsWith("spark.network") ||
    isSparkPortConf(name)
  }

  /**
   * Return true if the given config matches either `spark.*.port` or `spark.port.*`.
    * spark.*.port和spark.port.*都是被允许的
   */
  def isSparkPortConf(name: String): Boolean = {
    (name.startsWith("spark.") && name.endsWith(".port")) || name.startsWith("spark.port.")
  }

  /**
   * Looks for available deprecated keys for the given config option, and return the first
   * value available.
    * 在配置中查询可以使用的过时的key只，返回第一个key值对应的value
   */
  def getDeprecatedConfig(key: String, conf: SparkConf): Option[String] = {
    configsWithAlternatives.get(key).flatMap { alts =>
      alts.collectFirst { case alt if conf.contains(alt.key) =>
        val value = conf.get(alt.key)
        if (alt.translation != null) alt.translation(value) else value
      }
    }
  }

  /**
   * Logs a warning message if the given config key is deprecated.
    * 主要用来打印过时配置的一些日志信息 主要是处理过时的配置
   */
  def logDeprecationWarning(key: String): Unit = {
    deprecatedConfigs.get(key).foreach { cfg =>
      logWarning(
        s"The configuration key '$key' has been deprecated as of Spark ${cfg.version} and " +
        s"may be removed in the future. ${cfg.deprecationMessage}")
      return
    }

    //打印可选择的配置信息
    allAlternatives.get(key).foreach { case (newKey, cfg) =>
      logWarning(
        s"The configuration key '$key' has been deprecated as of Spark ${cfg.version} and " +
        s"may be removed in the future. Please use the new key '$newKey' instead.")
      return
    }

    // akka在笨笨2.0开始就没有使用了
    if (key.startsWith("spark.akka") || key.startsWith("spark.ssl.akka")) {
      logWarning(
        s"The configuration key $key is not supported any more " +
          s"because Spark doesn't use Akka since 2.0")
    }
  }

  /**
   * Holds information about keys that have been deprecated and do not have a replacement.
   * 对已经过时的和没有keys的信息
   * @param key The deprecated key.
   * @param version Version of Spark where key was deprecated.
   * @param deprecationMessage Message to include in the deprecation warning.
   */
  private case class DeprecatedConfig(
      key: String,
      version: String,
      deprecationMessage: String)

  /**
   * Information about an alternate configuration key that has been deprecated.
   * 关于一个配置key已经过时的信息。
   * @param key The deprecated config key.  过时的 配置属性名称
   * @param version The Spark version in which the key was deprecated.  过时的属性名称的版本
   * @param translation A translation function for converting old config values into new ones.  对应新的配置名称
   */
  private case class AlternateConfig(
      key: String,
      version: String,
      translation: String => String = null)

}
