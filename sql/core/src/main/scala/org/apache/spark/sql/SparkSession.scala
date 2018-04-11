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

package org.apache.spark.sql

import java.io.Closeable
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import org.apache.spark.{SPARK_VERSION, SparkConf, SparkContext}
import org.apache.spark.annotation.{DeveloperApi, Experimental, InterfaceStability}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Range}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.internal._
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.ExecutionListenerManager
import org.apache.spark.util.Utils


/**
 * The entry point to programming Spark with the Dataset and DataFrame API.
  * Dataset 和 DataFrame编程的实体入口API。
 *
 * In environments that this has been created upfront (e.g. REPL, notebooks), use the builder
 * to get an existing session:
 *
 * {{{
 *   SparkSession.builder().getOrCreate()
 * }}}
 *
 * The builder can also be used to create a new session:
 *
 * {{{
 *   SparkSession.builder
 *     .master("local")
 *     .appName("Word Count")
 *     .config("spark.some.config.option", "some-value")
 *     .getOrCreate()
 * }}}
 *
 * @param sparkContext The Spark context associated with this Spark session.
 * @param existingSharedState If supplied, use the existing shared state
 *                            instead of creating a new one.
 * @param parentSessionState If supplied, inherit all session state (i.e. temporary
 *                            views, SQL config, UDFs etc) from parent.
 */
@InterfaceStability.Stable
class SparkSession private(
    @transient val sparkContext: SparkContext,
    @transient private val existingSharedState: Option[SharedState],
    @transient private val parentSessionState: Option[SessionState],
    @transient private[sql] val extensions: SparkSessionExtensions)
  extends Serializable with Closeable with Logging { self =>

  private[sql] def this(sc: SparkContext) {
    this(sc, None, None, new SparkSessionExtensions)
  }

  //  判断sparkContext是否已经停止了，在每个算子，转子，都使用，在SparkSession中，也在使用
  sparkContext.assertNotStopped()

  /**
   * The version of Spark on which this application is running.
    * 此应用程序正在运行的Spark版本。
   *
   * @since 2.0.0
   */
  def version: String = SPARK_VERSION

  /* ----------------------- *
   |  Session-related state  |
   * ----------------------- */

  /**
   * State shared across sessions, including the `SparkContext`, cached data, listener,
   * and a catalog that interacts with external systems.
   *
   * This is internal to Spark and there is no guarantee on interface stability.
   *
    * state之间的共享会话。包括SparkContext，缓存数据，监听listener以及与外部系统交互的目录。
    *
    * 这是spark内部的，在界面稳定性上没有保证。
    *
   * @since 2.2.0
   */
  @InterfaceStability.Unstable
  @transient
  lazy val sharedState: SharedState = {
    existingSharedState.getOrElse(new SharedState(sparkContext))
  }

  /**
   * State isolated across sessions, including SQL configurations, temporary tables, registered
   * functions, and everything else that accepts a [[org.apache.spark.sql.internal.SQLConf]].
   * If `parentSessionState` is not null, the `SessionState` will be a copy of the parent.
   *
   * This is internal to Spark and there is no guarantee on interface stability.
    *
    * spark的状态信息和SparkSession是分离的,包括SQL配置、临时表、注册功能,和其他所有的事情,
    * 接受一个org.apache.spark.sql.internal.SQLConf.如果当前的sparkSession（parentSessionState）
    * 不是空值，,“SessionState”将拷贝一个当前的副本。
    *
    * 这是spark内部使用的，并不能保证接口的稳定性。
   *
   * @since 2.2.0
   */
  @InterfaceStability.Unstable
  @transient
  lazy val sessionState: SessionState = {
    parentSessionState
      .map(_.clone(this))
      .getOrElse {
        SparkSession.instantiateSessionState(
          SparkSession.sessionStateClassName(sparkContext.conf),
          self)
      }
  }

  /**
   * A wrapped version of this session in the form of a [[SQLContext]], for backward compatibility.
    *
    * 这个会话的包装版本以SQLContext的形式，用于向后兼容。
   *
   * @since 2.0.0
   */
  @transient
  val sqlContext: SQLContext = new SQLContext(this)

  /**
   * Runtime configuration interface for Spark. 用于Spark的运行时配置接口。
   *
   * This is the interface through which the user can get and set all Spark and Hadoop
   * configurations that are relevant to Spark SQL. When getting the value of a config,
   * this defaults to the value set in the underlying `SparkContext`, if any.
    *
    * 这是用户可以获得并设置与Spark SQL相关的所有Spark和Hadoop配置的接口。在获取配置的值时，
    * 默认值设置为底层“SparkContext”中的值(如果有的话)。
   *
   * @since 2.0.0
   */
  @transient lazy val conf: RuntimeConfig = new RuntimeConfig(sessionState.conf)

  /**
   * :: Experimental ::
   * An interface to register custom [[org.apache.spark.sql.util.QueryExecutionListener]]s
   * that listen for execution metrics.
    *
    * 一个用于注册自定义的接口org.apache.spark.sql.util.QueryExecutionListener。用于侦听执行度量的侦听器。
   *
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def listenerManager: ExecutionListenerManager = sessionState.listenerManager

  /**
   * :: Experimental ::
   * A collection of methods that are considered experimental, but can be used to hook into
   * the query planner for advanced functionality.
    *
    * 一组被认为是实验性的方法，但是可以用来连接到高级功能的查询规划器。
   *
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Unstable
  def experimental: ExperimentalMethods = sessionState.experimentalMethods

  /**
   * A collection of methods for registering user-defined functions (UDF).
   *
   * The following example registers a Scala closure as UDF:
   * {{{
   *   sparkSession.udf.register("myUDF", (arg1: Int, arg2: String) => arg2 + arg1)
   * }}}
   *
   * The following example registers a UDF in Java:
   * {{{
   *   sparkSession.udf().register("myUDF",
   *       (Integer arg1, String arg2) -> arg2 + arg1,
   *       DataTypes.StringType);
   * }}}
   *
   * @note The user-defined functions must be deterministic. Due to optimization,
   * duplicate invocations may be eliminated or the function may even be invoked more times than
   * it is present in the query.
   *
   * @since 2.0.0
   */
  def udf: UDFRegistration = sessionState.udfRegistration

  /**
   * :: Experimental ::
   * Returns a `StreamingQueryManager` that allows managing all the
   * `StreamingQuery`s active on `this`.
    *
    * 返回一个“StreamingQueryManager”，它允许管理所有“StreamingQuery”在“this”上的活动。
   *
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Unstable
  def streams: StreamingQueryManager = sessionState.streamingQueryManager

  /**
   * Start a new session with isolated SQL configurations, temporary tables, registered
   * functions are isolated, but sharing the underlying `SparkContext` and cached data.
    *
    * 使用隔离的SQL配置、临时表、已注册的函数启动新的会话，但共享底层的“SparkContext”和缓存数据。
   *
   * @note Other than the `SparkContext`, all shared state is initialized lazily.
   * This method will force the initialization of the shared state to ensure that parent
   * and child sessions are set up with the same shared state. If the underlying catalog
   * implementation is Hive, this will initialize the metastore, which may take some time.
    *
    * 除了“SparkContext”之外，所有共享状态都是惰性初始化的。此方法将强制共享状态的初始化，
    * 以确保父进程和子会话都设置为相同的共享状态。如果底层目录实现是Hive，这将初始化转移，这可能需要一些时间。
   *
   * @since 2.0.0
   */
  def newSession(): SparkSession = {
    new SparkSession(sparkContext, Some(sharedState), parentSessionState = None, extensions)
  }

  /**
   * Create an identical copy of this `SparkSession`, sharing the underlying `SparkContext`
   * and shared state. All the state of this session (i.e. SQL configurations, temporary tables,
   * registered functions) is copied over, and the cloned session is set up with the same shared
   * state as this session. The cloned session is independent of this session, that is, any
   * non-global change in either session is not reflected in the other.
    *
    * 创建一个相同的“SparkSession”副本，共享底层的“SparkContext”和共享状态。这个会话的所有状态(即SQL配置、
    * 临时表、注册函数)都被复制，并且克隆的会话被设置为与这个会话相同的共享状态。克隆的会话与此会话无关，
    * 也就是说，会话中的任何非全局更改都不会在另一个会话中反映出来。
   *
   * @note Other than the `SparkContext`, all shared state is initialized lazily.
   * This method will force the initialization of the shared state to ensure that parent
   * and child sessions are set up with the same shared state. If the underlying catalog
   * implementation is Hive, this will initialize the metastore, which may take some time.
    *
    * 除了“SparkContext”之外，所有共享状态都是惰性初始化的。此方法将强制共享状态的初始化，
    * 以确保父进程和子会话都设置为相同的共享状态。如果底层目录实现是Hive，这将初始化转移，这可能需要一些时间。
   */
  private[sql] def cloneSession(): SparkSession = {
    val result = new SparkSession(sparkContext, Some(sharedState), Some(sessionState), extensions)
    result.sessionState // force copy of SessionState
    result
  }


  /* --------------------------------- *
   |  Methods for creating DataFrames  |
   * --------------------------------- */

  /**
   * Returns a `DataFrame` with no rows or columns.
    *
    * 返回一个DataFrame没有rows和列
   *
   * @since 2.0.0
   */
  @transient
  lazy val emptyDataFrame: DataFrame = {
    createDataFrame(sparkContext.emptyRDD[Row], StructType(Nil))
  }

  /**
   * :: Experimental ::
   * Creates a new [[Dataset]] of type T containing zero elements.
   *
   * @return 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def emptyDataset[T: Encoder]: Dataset[T] = {
    val encoder = implicitly[Encoder[T]]
    new Dataset(self, LocalRelation(encoder.schema.toAttributes), encoder)
  }

  /**
   * :: Experimental ::
   * Creates a `DataFrame` from an RDD of Product (e.g. case classes, tuples).
    *
    * 从Product中的RDD中创建一个“DataFrame”(例如，case类、元组)。
   *
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def createDataFrame[A <: Product : TypeTag](rdd: RDD[A]): DataFrame = {
    SparkSession.setActiveSession(this)
    val encoder = Encoders.product[A]
    Dataset.ofRows(self, ExternalRDD(rdd, self)(encoder))
  }

  /**
   * :: Experimental ::
   * Creates a `DataFrame` from a local Seq of Product.
   *
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def createDataFrame[A <: Product : TypeTag](data: Seq[A]): DataFrame = {
    SparkSession.setActiveSession(this)
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = schema.toAttributes
    Dataset.ofRows(self, LocalRelation.fromProduct(attributeSeq, data))
  }

  /**
   * :: DeveloperApi ::
   * Creates a `DataFrame` from an `RDD` containing [[Row]]s using the given schema.
   * It is important to make sure that the structure of every [[Row]] of the provided RDD matches
   * the provided schema. Otherwise, there will be runtime exception.
    *
    * 使用给定的模式从包含[[Row]]的“RDD”中创建“DataFrame”。重要的是要确保提供的RDD的每个[[行]]的结构与所提供的模式相匹配。
    * 否则，将有运行时异常。
    *
   * Example:
   * {{{
   *  import org.apache.spark.sql._
   *  import org.apache.spark.sql.types._
   *  val sparkSession = new org.apache.spark.sql.SparkSession(sc)
   *
   *  val schema =
   *    StructType(
   *      StructField("name", StringType, false) ::
   *      StructField("age", IntegerType, true) :: Nil)
   *
   *  val people =
   *    sc.textFile("examples/src/main/resources/people.txt").map(
   *      _.split(",")).map(p => Row(p(0), p(1).trim.toInt))
   *  val dataFrame = sparkSession.createDataFrame(people, schema)
   *  dataFrame.printSchema
   *  // root
   *  // |-- name: string (nullable = false)
   *  // |-- age: integer (nullable = true)
   *
   *  dataFrame.createOrReplaceTempView("people")
   *  sparkSession.sql("select name from people").collect.foreach(println)
   * }}}
   *
   * @since 2.0.0
   */
  @DeveloperApi
  @InterfaceStability.Evolving
  def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame = {
    createDataFrame(rowRDD, schema, needsConversion = true)
  }

  /**
   * :: DeveloperApi ::
   * Creates a `DataFrame` from a `JavaRDD` containing [[Row]]s using the given schema.
   * It is important to make sure that the structure of every [[Row]] of the provided RDD matches
   * the provided schema. Otherwise, there will be runtime exception.
   *
   * @since 2.0.0
   */
  @DeveloperApi
  @InterfaceStability.Evolving
  def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): DataFrame = {
    createDataFrame(rowRDD.rdd, schema)
  }

  /**
   * :: DeveloperApi ::
   * Creates a `DataFrame` from a `java.util.List` containing [[Row]]s using the given schema.
   * It is important to make sure that the structure of every [[Row]] of the provided List matches
   * the provided schema. Otherwise, there will be runtime exception.
   *
   * @since 2.0.0
   */
  @DeveloperApi
  @InterfaceStability.Evolving
  def createDataFrame(rows: java.util.List[Row], schema: StructType): DataFrame = {
    Dataset.ofRows(self, LocalRelation.fromExternalRows(schema.toAttributes, rows.asScala))
  }

  /**
   * Applies a schema to an RDD of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   * SELECT * queries will return the columns in an undefined order.
   *
   * @since 2.0.0
   */
  def createDataFrame(rdd: RDD[_], beanClass: Class[_]): DataFrame = {
    val attributeSeq: Seq[AttributeReference] = getSchema(beanClass)
    val className = beanClass.getName
    val rowRdd = rdd.mapPartitions { iter =>
    // BeanInfo is not serializable so we must rediscover it remotely for each partition.
      SQLContext.beansToRows(iter, Utils.classForName(className), attributeSeq)
    }
    Dataset.ofRows(self, LogicalRDD(attributeSeq, rowRdd)(self))
  }

  /**
   * Applies a schema to an RDD of Java Beans.
   *
    * 将模式应用于Java bean的RDD。
    *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   * SELECT * queries will return the columns in an undefined order.
    *
    * 警告:由于在Java Bean中没有字段的保证排序，SELECT * 查询将以未定义的顺序返回列。
   *
   * @since 2.0.0
   */
  def createDataFrame(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame = {
    createDataFrame(rdd.rdd, beanClass)
  }

  /**
   * Applies a schema to a List of Java Beans.
    *
    * 将模式应用于Java bean的List。
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   *          SELECT * queries will return the columns in an undefined order.
   * @since 1.6.0
   */
  def createDataFrame(data: java.util.List[_], beanClass: Class[_]): DataFrame = {
    val attrSeq = getSchema(beanClass)
    val rows = SQLContext.beansToRows(data.asScala.iterator, beanClass, attrSeq)
    Dataset.ofRows(self, LocalRelation(attrSeq, rows.toSeq))
  }

  /**
   * Convert a `BaseRelation` created for external data sources into a `DataFrame`.
    *
    * 将为外部数据源创建的“BaseRelation”转换为“DataFrame”。
   *
   * @since 2.0.0
   */
  def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame = {
    Dataset.ofRows(self, LogicalRelation(baseRelation))
  }

  /* ------------------------------- *
   |  Methods for creating DataSets  |
   * ------------------------------- */

  /**
   * :: Experimental ::
   * Creates a [[Dataset]] from a local Seq of data of a given type. This method requires an
   * encoder (to convert a JVM object of type `T` to and from the internal Spark SQL representation)
   * that is generally created automatically through implicits from a `SparkSession`, or can be
   * created explicitly by calling static methods on [[Encoders]].
    *
    * 从给定类型的本地数据Seq中创建[[Dataset]]。这个方法需要一个编码器(来转换类型“T”的JVM对象和内部
    * Spark SQL表示)，它通常是通过“SparkSession”的implicits自动创建的，或者可以通过调用[[encoder]]
    * 上的静态方法来显式创建。
   *
   * == Example ==
   *
   * {{{
   *
   *   import spark.implicits._
   *   case class Person(name: String, age: Long)
   *   val data = Seq(Person("Michael", 29), Person("Andy", 30), Person("Justin", 19))
   *   val ds = spark.createDataset(data)
   *
   *   ds.show()
   *   // +-------+---+
   *   // |   name|age|
   *   // +-------+---+
   *   // |Michael| 29|
   *   // |   Andy| 30|
   *   // | Justin| 19|
   *   // +-------+---+
   * }}}
   *
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def createDataset[T : Encoder](data: Seq[T]): Dataset[T] = {
    /**
      * 对于输入的的信息，两个需要主要(此刻T = person)
      * 1、编码信息为T
      * 2、输入的数据类型为Seq[T]
      */
    val enc = encoderFor[T]
    /**
      * 此处通过对类型T进行提取
      */
    val attributes = enc.schema.toAttributes
    val encoded = data.map(d => enc.toRow(d).copy())
    /**
      * case class LocalRelation(output: Seq[Attribute], data: Seq[InternalRow] = Nil)
      * play包含了属性attributes和内部数据encoded
      */
    val plan = new LocalRelation(attributes, encoded)
    /**
      * Dataset[T](self, plan)  =>
      * 最终调用：Dataset(sparkSession, sparkSession.sessionState.executePlan(logicalPlan), encoder)
      * 其中：执行逻辑是 play部分，encoder就是play的encoded
      */
    Dataset[T](self, plan)
  }

  /**
   * :: Experimental ::
   * Creates a [[Dataset]] from an RDD of a given type. This method requires an
   * encoder (to convert a JVM object of type `T` to and from the internal Spark SQL representation)
   * that is generally created automatically through implicits from a `SparkSession`, or can be
   * created explicitly by calling static methods on [[Encoders]].
    *
    * 从给定类型的RDD中创建[[数据集]]。这个方法需要一个编码器(来转换类型“T”的JVM对象和内部Spark SQL表示)，
    * 它通常是通过“SparkSession”的implicits自动创建的，或者可以通过调用[[encoder]]上的静态方法来显式创建。
   *
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def createDataset[T : Encoder](data: RDD[T]): Dataset[T] = {
    Dataset[T](self, ExternalRDD(data, self))
  }

  /**
   * :: Experimental ::
   * Creates a [[Dataset]] from a `java.util.List` of a given type. This method requires an
   * encoder (to convert a JVM object of type `T` to and from the internal Spark SQL representation)
   * that is generally created automatically through implicits from a `SparkSession`, or can be
   * created explicitly by calling static methods on [[Encoders]].
    *
    * 从“java.util”中创建[[Dataset]]。给定类型的列表。这个方法需要一个编码器(来转换类型“T”的JVM对象和内部Spark SQL表示)，
    * 它通常是通过“SparkSession”的implicits自动创建的，或者可以通过调用[[encoder]]上的静态方法来显式创建。
   *
   * == Java Example ==
   *
   * {{{
   *     List<String> data = Arrays.asList("hello", "world");
   *     Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
   * }}}
   *
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def createDataset[T : Encoder](data: java.util.List[T]): Dataset[T] = {
    createDataset(data.asScala)
  }

  /**
   * :: Experimental ::
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements
   * in a range from 0 to `end` (exclusive) with step value 1.
    *
    * 使用一个名为“id”的单一“LongType”列创建一个[[Dataset]]，其中包含从0到“end”(独占)和步骤值1的范围内的元素。
   *
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def range(end: Long): Dataset[java.lang.Long] = range(0, end)

  /**
   * :: Experimental ::
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements
   * in a range from `start` to `end` (exclusive) with step value 1.
   *
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def range(start: Long, end: Long): Dataset[java.lang.Long] = {
    range(start, end, step = 1, numPartitions = sparkContext.defaultParallelism)
  }

  /**
   * :: Experimental ::
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements
   * in a range from `start` to `end` (exclusive) with a step value.
   *
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def range(start: Long, end: Long, step: Long): Dataset[java.lang.Long] = {
    range(start, end, step, numPartitions = sparkContext.defaultParallelism)
  }

  /**
   * :: Experimental ::
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements
   * in a range from `start` to `end` (exclusive) with a step value, with partition number
   * specified.
   *
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[java.lang.Long] = {
    new Dataset(self, Range(start, end, step, numPartitions), Encoders.LONG)
  }

  /**
   * Creates a `DataFrame` from an RDD[Row].
   * User can specify whether the input rows should be converted to Catalyst rows.
    *
    * 从RDD中创建“DataFrame”。用户可以指定输入行是否应该转换为Catalyst行。
   */
  private[sql] def internalCreateDataFrame(
      catalystRows: RDD[InternalRow],
      schema: StructType): DataFrame = {
    // TODO: use MutableProjection when rowRDD is another DataFrame and the applied
    // schema differs from the existing schema on any field data type.
    val logicalPlan = LogicalRDD(schema.toAttributes, catalystRows)(self)
    Dataset.ofRows(self, logicalPlan)
  }

  /**
   * Creates a `DataFrame` from an RDD[Row].
   * User can specify whether the input rows should be converted to Catalyst rows.
    *
    * 从RDD[Row]创建一个DataFrame
    * 用户可以指定输入行是否应该转换为Catalyst行。
   */
  private[sql] def createDataFrame(
      rowRDD: RDD[Row],
      schema: StructType,
      needsConversion: Boolean) = {
    // TODO: use MutableProjection when rowRDD is another DataFrame and the applied
    // schema differs from the existing schema on any field data type.
    // 模式不同于任何字段数据类型上的现有模式。
    // 默认为needsConversion = true
    val catalystRows = if (needsConversion) {
      // 主要是每列数据的类型指定对应关系
      val encoder = RowEncoder(schema)
      rowRDD.map(encoder.toRow)
    } else {
      rowRDD.map{r: Row => InternalRow.fromSeq(r.toSeq)}
    }
    val logicalPlan = LogicalRDD(schema.toAttributes, catalystRows)(self)
    Dataset.ofRows(self, logicalPlan)
  }


  /* ------------------------- *
   |  Catalog-related methods  |
   * ------------------------- */

  /**
   * Interface through which the user may create, drop, alter or query underlying
   * databases, tables, functions etc.
    *
    * 用户可以通过该接口创建、删除、修改或查询底层数据库、表、函数等。
   *  Catalog:字典表，用于注册表，对表缓存后便于查询
   * @since 2.0.0
   */
  @transient lazy val catalog: Catalog = new CatalogImpl(self)

  /**
   * Returns the specified table/view as a `DataFrame`.
   *
   * @param tableName is either a qualified or unqualified name that designates a table or view.
   *                  If a database is specified, it identifies the table/view from the database.
   *                  Otherwise, it first attempts to find a temporary view with the given name
   *                  and then match the table/view from the current database.
   *                  Note that, the global temporary view database is also valid here.
   * @since 2.0.0
   */
  def table(tableName: String): DataFrame = {
    table(sessionState.sqlParser.parseTableIdentifier(tableName))
  }

  private[sql] def table(tableIdent: TableIdentifier): DataFrame = {
    Dataset.ofRows(self, sessionState.catalog.lookupRelation(tableIdent))
  }

  /* ----------------- *
   |  Everything else  |
   * ----------------- */

  /**
   * Executes a SQL query using Spark, returning the result as a `DataFrame`.
   * The dialect that is used for SQL parsing can be configured with 'spark.sql.dialect'.
   *
    * 使用Spark执行一个SQL查询，将结果返回为“DataFrame”。用于SQL解析的方言可以配置为“spark.sql.dialect”。
    * spark sql的入口是最后一句，SparkSession类里的sql函数，传入一个sql字符串，返回一个dataframe对象。
   * @since 2.0.0
   */
  def sql(sqlText: String): DataFrame = {
    // ParserInterface ==> parsePlan的具体实现在AbstractSqlParser类中
    // parsePlan(sqlText)对SQL语句进行解析
    Dataset.ofRows(self, sessionState.sqlParser.parsePlan(sqlText))
  }

  /**
   * Returns a [[DataFrameReader]] that can be used to read non-streaming data in as a
   * `DataFrame`.
    *
    * 返回一个[[DataFrameReader]]，可用于将非流数据以“DataFrame”的形式读取。
    *
   * {{{
   *   sparkSession.read.parquet("/path/to/file.parquet")
   *   sparkSession.read.schema(schema).json("/path/to/file.json")
   * }}}
   *
   * @since 2.0.0
   */
  def read: DataFrameReader = new DataFrameReader(self)

  /**
   * Returns a `DataStreamReader` that can be used to read streaming data in as a `DataFrame`.
    *
    * 返回一个“DataStreamReader”，可用于将流数据作为“DataFrame”读取。
    *
   * {{{
   *   sparkSession.readStream.parquet("/path/to/directory/of/parquet/files")
   *   sparkSession.readStream.schema(schema).json("/path/to/directory/of/json/files")
   * }}}
   *
   * @since 2.0.0
   */
  @InterfaceStability.Evolving
  def readStream: DataStreamReader = new DataStreamReader(self)

  /**
   * Executes some code block and prints to stdout the time taken to execute the block. This is
   * available in Scala only and is used primarily for interactive testing and debugging.
    *
    * 执行一些代码块和打印输出以执行该块执行的时间。这仅在Scala中可用，主要用于交互式测试和调试。
   *
   * @since 2.1.0
   */
  def time[T](f: => T): T = {
    val start = System.nanoTime()
    val ret = f
    val end = System.nanoTime()
    // scalastyle:off println
    println(s"Time taken: ${(end - start) / 1000 / 1000} ms")
    // scalastyle:on println
    ret
  }

  // scalastyle:off
  // Disable style checker so "implicits" object can start with lowercase i
  /**
   * :: Experimental ::
   * (Scala-specific) Implicit methods available in Scala for converting
   * common Scala objects into `DataFrame`s.
   *
   * {{{
   *   val sparkSession = SparkSession.builder.getOrCreate()
   *   import sparkSession.implicits._
   * }}}
   *
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  object implicits extends SQLImplicits with Serializable {
    protected override def _sqlContext: SQLContext = SparkSession.this.sqlContext
  }
  // scalastyle:on

  /**
   * Stop the underlying `SparkContext`. 阻止潜在的“SparkContext”。
   *
   * @since 2.0.0
   */
  def stop(): Unit = {
    sparkContext.stop()
  }

  /**
   * Synonym for `stop()`. 同义词“stop()”。
   *
   * @since 2.1.0
   */
  override def close(): Unit = stop()

  /**
   * Parses the data type in our internal string representation. The data type string should
   * have the same format as the one generated by `toString` in scala.
   * It is only used by PySpark.
    *
    * 解析内部字符串表示中的数据类型。数据类型字符串应该具有与scala“toString”生成的格式相同的格式。它只被PySpark使用。
   */
  protected[sql] def parseDataType(dataTypeString: String): DataType = {
    DataType.fromJson(dataTypeString)
  }

  /**
   * Apply a schema defined by the schemaString to an RDD. It is only used by PySpark.
    * 应用由schemaString到RDD定义的模式。它只被PySpark使用。
   */
  private[sql] def applySchemaToPythonRDD(
      rdd: RDD[Array[Any]],
      schemaString: String): DataFrame = {
    val schema = DataType.fromJson(schemaString).asInstanceOf[StructType]
    applySchemaToPythonRDD(rdd, schema)
  }

  /**
   * Apply a schema defined by the schema to an RDD. It is only used by PySpark.
    * 将模式定义的模式应用于RDD。它只被PySpark使用。
   */
  private[sql] def applySchemaToPythonRDD(
      rdd: RDD[Array[Any]],
      schema: StructType): DataFrame = {
    val rowRdd = rdd.map(r => python.EvaluatePython.fromJava(r, schema).asInstanceOf[InternalRow])
    Dataset.ofRows(self, LogicalRDD(schema.toAttributes, rowRdd)(self))
  }

  /**
   * Returns a Catalyst Schema for the given java bean class.
    * 为给定的java bean类返回一个Catalyst模式。
   */
  private def getSchema(beanClass: Class[_]): Seq[AttributeReference] = {
    val (dataType, _) = JavaTypeInference.inferDataType(beanClass)
    dataType.asInstanceOf[StructType].fields.map { f =>
      AttributeReference(f.name, f.dataType, f.nullable)()
    }
  }

}


@InterfaceStability.Stable
object SparkSession {

  /**
   * Builder for [[SparkSession]].
   */
  @InterfaceStability.Stable
  class Builder extends Logging {

    private[this] val options = new scala.collection.mutable.HashMap[String, String]

    // spark的一些外部的扩展点（分析规则，检查分析规则，优化器规则，规划策略，自定义解析器，(外部)目录侦听器）
    private[this] val extensions = new SparkSessionExtensions

    private[this] var userSuppliedContext: Option[SparkContext] = None

    private[spark] def sparkContext(sparkContext: SparkContext): Builder = synchronized {
      userSuppliedContext = Option(sparkContext)
      this
    }

    /**
     * Sets a name for the application, which will be shown in the Spark web UI.
     * If no application name is set, a randomly generated name will be used.
     *
     * @since 2.0.0
     */
    def appName(name: String): Builder = config("spark.app.name", name)

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: String): Builder = synchronized {
      options += key -> value
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: Long): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: Double): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: Boolean): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a list of config options based on the given `SparkConf`.
     *
      * 根据给定的“SparkConf”设置一个配置选项列表。
      *
     * @since 2.0.0
     */
    def config(conf: SparkConf): Builder = synchronized {
      conf.getAll.foreach { case (k, v) => options += k -> v }
      this
    }

    /**
     * Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]" to
     * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
     *
     * @since 2.0.0
     */
    def master(master: String): Builder = config("spark.master", master)

    /**
     * Enables Hive support, including connectivity to a persistent Hive metastore, support for
     * Hive serdes, and Hive user-defined functions.
      *
      * 启用hive支持，包括连接到一个持久的hive元数据，支持hive serdes，以及hive的定义函数。
     *  这里是否启用hive
     * @since 2.0.0
     */
    def enableHiveSupport(): Builder = synchronized {
      if (hiveClassesArePresent) {
        config(CATALOG_IMPLEMENTATION.key, "hive")
      } else {
        throw new IllegalArgumentException(
          "Unable to instantiate SparkSession with Hive support because " +
            "Hive classes are not found.")
      }
    }

    /**
     * Inject extensions into the [[SparkSession]]. This allows a user to add Analyzer rules,
     * Optimizer rules, Planning Strategies or a customized parser.
      *
      * 将扩展注入[[SparkSession]]。这允许用户添加分析器规则、优化器规则、规划策略或定制的解析器。
     *
     * @since 2.2.0
     */
    def withExtensions(f: SparkSessionExtensions => Unit): Builder = {
      f(extensions)
      this
    }

    /**
     * Gets an existing [[SparkSession]] or, if there is no existing one, creates a new
     * one based on the options set in this builder.
     *
     * This method first checks whether there is a valid thread-local SparkSession,
     * and if yes, return that one. It then checks whether there is a valid global
     * default SparkSession, and if yes, return that one. If no valid global default
     * SparkSession exists, the method creates a new SparkSession and assigns the
     * newly created SparkSession as the global default.
     *
     * In case an existing SparkSession is returned, the config options specified in
     * this builder will be applied to the existing SparkSession.
      *
      * 得到一个已经存在的[[SparkSession]] 或者 如果没有一个已经存在的[[SparkSession]]，那么就根据设置的选项重新创建一个。
      *
      * 这个方法首先检查是否有一个有效的线程本地SparkSession，如果是，返回那个。
      * 然后它检查是否有一个有效的全局默认SparkSession，如果是，返回那个。
      * 如果没有有效的全局缺省SparkSession，该方法将创建一个新的SparkSession，并将新创建的SparkSession分配为全局默认值。
      *
      * 如果返回现有的SparkSession，则该构建器中指定的配置选项将应用于现有的SparkSession。
     *
     * @since 2.0.0
     */
    def getOrCreate(): SparkSession = synchronized {
      // Get the session from current thread's active session. 直接获取该线程对应的SparkSession
      var session = activeThreadSession.get()
      // 如果SparkSecssion不为空，或者 SparkSession没有停止 ，那么就直接返回当前的SparkSession
      if ((session ne null) && !session.sparkContext.isStopped) {
        options.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
        if (options.nonEmpty) {
          logWarning("Using an existing SparkSession; some configuration may not take effect.")
        }
        return session
      }

      // Global synchronization so we will only set the default session once. 全局同步，因此我们只设置一次默认会话。
      SparkSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        // 如果当前线程不存在一个活动的SparkSession，那么就从全局获取一个
        session = defaultSession.get()
        if ((session ne null) && !session.sparkContext.isStopped) {
          options.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
          if (options.nonEmpty) {
            logWarning("Using an existing SparkSession; some configuration may not take effect.")
          }
          return session
        }

        // No active nor global default session. Create a new one.
        // 没有活动的和全局的默认的SparkSession，那么就创建一个
        val sparkContext = userSuppliedContext.getOrElse {
          // set app name if not given
          val randomAppName = java.util.UUID.randomUUID().toString
          // 初始化Spark的配置类
          val sparkConf = new SparkConf()
          options.foreach { case (k, v) => sparkConf.set(k, v) }
          // 如果Spark没有指定名称，那么我们就默认一个随机的
          if (!sparkConf.contains("spark.app.name")) {
            sparkConf.setAppName(randomAppName)
          }

          /** 直接调用 SparkContext的伴生类创建sc */
          val sc = SparkContext.getOrCreate(sparkConf)

          // maybe this is an existing SparkContext, update its SparkConf which maybe used
          // by SparkSession
          // 也许这是一个现有的SparkContext，更新它的SparkConf，可能是SparkSession使用的
          options.foreach { case (k, v) => sc.conf.set(k, v) }
          if (!sc.conf.contains("spark.app.name")) {
            sc.conf.setAppName(randomAppName)
          }
          sc
        }

        // Initialize extensions if the user has defined a configurator class.
        // 如果用户定义了配置器类，则初始化扩展。
        val extensionConfOption = sparkContext.conf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS)
        if (extensionConfOption.isDefined) {
          val extensionConfClassName = extensionConfOption.get
          try {
            val extensionConfClass = Utils.classForName(extensionConfClassName)
            val extensionConf = extensionConfClass.newInstance()
              .asInstanceOf[SparkSessionExtensions => Unit]
            extensionConf(extensions)
          } catch {
            // Ignore the error if we cannot find the class or when the class has the wrong type.
            case e @ (_: ClassCastException |
                      _: ClassNotFoundException |
                      _: NoClassDefFoundError) =>
              logWarning(s"Cannot use $extensionConfClassName to configure session extensions.", e)
          }
        }

        session = new SparkSession(sparkContext, None, None, extensions)
        options.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
        defaultSession.set(session)

        // Register a successfully instantiated context to the singleton. This should be at the
        // end of the class definition so that the singleton is updated only if there is no
        // exception in the construction of the instance.
        // 向singleton注册一个成功的实例化context。这应该在类定义的末尾，只有在实例的构造中没有异常时才更新singleton。

        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
            defaultSession.set(null)
            sqlListener.set(null)
          }
        })
      }

      return session
    }
  }

  /**
   * Creates a [[SparkSession.Builder]] for constructing a [[SparkSession]].
   *
   * @since 2.0.0
   */
  def builder(): Builder = new Builder

  /**
   * Changes the SparkSession that will be returned in this thread and its children when
   * SparkSession.getOrCreate() is called. This can be used to ensure that a given thread receives
   * a SparkSession with an isolated session, instead of the global (first created) context.
   *
    * 当调用SparkSession. getorcreate()时，将在该线程和其子元素中返回的SparkSession更改。
    * 这可以用来确保给定的线程通过一个单独的会话来接收SparkSession，而不是全局(第一个创建的)上下文。
    *
   * @since 2.0.0
   */
  def setActiveSession(session: SparkSession): Unit = {
    activeThreadSession.set(session)
  }

  /**
   * Clears the active SparkSession for current thread. Subsequent calls to getOrCreate will
   * return the first created context instead of a thread-local override.
    *
    * 为当前线程清除活动的SparkSession。随后对getOrCreate的调用将返回第一个创建的上下文，而不是线程本地覆盖。
   *
   * @since 2.0.0
   */
  def clearActiveSession(): Unit = {
    activeThreadSession.remove()
  }

  /**
   * Sets the default SparkSession that is returned by the builder.
    * 设置构建器返回的默认SparkSession。
   *
   * @since 2.0.0
   */
  def setDefaultSession(session: SparkSession): Unit = {
    defaultSession.set(session)
  }

  /**
   * Clears the default SparkSession that is returned by the builder.
    * 清除生成器返回的默认SparkSession。
   *
   * @since 2.0.0
   */
  def clearDefaultSession(): Unit = {
    defaultSession.set(null)
  }

  /**
   * Returns the active SparkSession for the current thread, returned by the builder.
    *
    * 返回由构建器返回的当前线程的活动SparkSession。
   *
   * @since 2.2.0
   */
  def getActiveSession: Option[SparkSession] = Option(activeThreadSession.get)

  /**
   * Returns the default SparkSession that is returned by the builder.
   *
   * @since 2.2.0
   */
  def getDefaultSession: Option[SparkSession] = Option(defaultSession.get)

  /** A global SQL listener used for the SQL UI. */
  private[sql] val sqlListener = new AtomicReference[SQLListener]()

  ////////////////////////////////////////////////////////////////////////////////////////
  // Private methods from now on
  ////////////////////////////////////////////////////////////////////////////////////////

  /** The active SparkSession for the current thread.
    * 当前线程的活动SparkSession。
    *
    * InheritableThreadLocal类继承于ThreadLocal类，所以它具有ThreadLocal类的特性，
    * 但又是一种特殊的ThreadLocal，其特殊性在于InheritableThreadLocal变量值会自动传递给所有子线程，
    * 而普通ThreadLocal变量不行。
    *
    * InheritableThreadLocal比ThreadLocal多一个特性，继承性，可以从父线程中得到初始值。
    *
    * */
  private val activeThreadSession = new InheritableThreadLocal[SparkSession]

  /** Reference to the root SparkSession.
    * 引用根SparkSession。
    * */
  private val defaultSession = new AtomicReference[SparkSession]

  private val HIVE_SESSION_STATE_BUILDER_CLASS_NAME =
    "org.apache.spark.sql.hive.HiveSessionStateBuilder"

  private def sessionStateClassName(conf: SparkConf): String = {
    conf.get(CATALOG_IMPLEMENTATION) match {
      case "hive" => HIVE_SESSION_STATE_BUILDER_CLASS_NAME
      case "in-memory" => classOf[SessionStateBuilder].getCanonicalName
    }
  }

  /**
   * Helper method to create an instance of `SessionState` based on `className` from conf.
   * The result is either `SessionState` or a Hive based `SessionState`.
   */
  private def instantiateSessionState(
      className: String,
      sparkSession: SparkSession): SessionState = {
    try {
      // invoke `new [Hive]SessionStateBuilder(SparkSession, Option[SessionState])`
      val clazz = Utils.classForName(className)
      val ctor = clazz.getConstructors.head
      ctor.newInstance(sparkSession, None).asInstanceOf[BaseSessionStateBuilder].build()
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }

  /**
   * @return true if Hive classes can be loaded, otherwise false.
    *         如果hive的类能够加载就返回true，否则返回false
   */
  private[spark] def hiveClassesArePresent: Boolean = {
    try {
      // HIVE_SESSION_STATE_BUILDER_CLASS_NAME为org.apache.spark.sql.hive.HiveSessionStateBuilder
      Utils.classForName(HIVE_SESSION_STATE_BUILDER_CLASS_NAME)
      Utils.classForName("org.apache.hadoop.hive.conf.HiveConf")
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }

}
