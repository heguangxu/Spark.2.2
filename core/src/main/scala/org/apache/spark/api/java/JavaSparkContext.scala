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

package org.apache.spark.api.java

import java.io.Closeable
import java.util
import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{InputFormat, JobConf}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}

import org.apache.spark._
import org.apache.spark.AccumulatorParam._
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.{EmptyRDD, HadoopRDD, NewHadoopRDD, RDD}

/**
 * A Java-friendly version of [[org.apache.spark.SparkContext]] that returns
 * [[org.apache.spark.api.java.JavaRDD]]s and works with Java collections instead of Scala ones.
  *
  * 一个java有好的版本[[org.apache.spark.SparkContext]]返回[[org.apache.spark.api.java.JavaRDD]]的
  *  Java collections 而不是 Scala ones.
  *
 * Only one SparkContext may be active per JVM.  You must `stop()` the active SparkContext before
 * creating a new one.  This limitation may eventually be removed; see SPARK-2243 for more details.
  *
  * 每个jvm只能有一个活动的SparkContext，你必须调用 `stop()`方法去停止活动的SparkContext，在你开启一个新的
  * SparkContext之前。这种限制最终可能会被移除。详情请看SPARK-2243
  */
class JavaSparkContext(val sc: SparkContext)
  extends JavaSparkContextVarargsWorkaround with Closeable {

  /**
   * Create a JavaSparkContext that loads settings from system properties (for instance, when
   * launching with ./bin/spark-submit).
    *
    * 创建JavaSparkContext加载系统的属性配置（例如使用 ./bin/spark-submit）
   */
  def this() = this(new SparkContext())

  /**
   * @param conf a [[org.apache.spark.SparkConf]] object specifying Spark parameters
    *             一个[[org.apache.spark.SparkConf]]对象，该对象指定了一些spark的参数
   */
  def this(conf: SparkConf) = this(new SparkContext(conf))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
    *               集群的URL
   * @param appName A name for your application, to display on the cluster web UI
    *                应用程序的名称，会显示在集群的web界面上
   */
  def this(master: String, appName: String) = this(new SparkContext(master, appName))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
    *                应用程序的名称，会显示在集群的web界面上
   * @param conf a [[org.apache.spark.SparkConf]] object specifying other Spark parameters
    *             SparkConf的对象为了配置Spark的参数
   */
  def this(master: String, appName: String, conf: SparkConf) =
    this(conf.setMaster(master).setAppName(appName))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
    *               集群的URL
   * @param appName A name for your application, to display on the cluster web UI
    *                应用程序的名称，会显示在集群的web界面上
   * @param sparkHome The SPARK_HOME directory on the slave nodes
    *                  子节点的SPARK_HOME目录
   * @param jarFile JAR file to send to the cluster. This can be a path on the local file system
   *                or an HDFS, HTTP, HTTPS, or FTP URL.
    *                要送到集群的jar文件，这些可以是一个路径，在hdfs上，http，https或者ftp url
   */
  def this(master: String, appName: String, sparkHome: String, jarFile: String) =
    this(new SparkContext(master, appName, sparkHome, Seq(jarFile)))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
    *               集群的URL
   * @param appName A name for your application, to display on the cluster web UI
    *                应用程序的名称，会显示在集群的web界面上
   * @param sparkHome The SPARK_HOME directory on the slave nodes
    *                子节点的SPARK_HOME目录
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
    *                要送到集群的jar文件，这些可以是一个路径，在hdfs上，http，https或者ftp url
   */
  def this(master: String, appName: String, sparkHome: String, jars: Array[String]) =
    this(new SparkContext(master, appName, sparkHome, jars.toSeq))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
    *                集群的URL
   * @param appName A name for your application, to display on the cluster web UI
    *                应用程序的名称，会显示在集群的web界面上
   * @param sparkHome The SPARK_HOME directory on the slave nodes
    *                子节点的SPARK_HOME目录
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
    *             要送到集群的jar文件，这些可以是一个路径，在hdfs上，http，https或者ftp url
   * @param environment Environment variables to set on worker nodes
    *                    设置在worker节点上的环境变量
   */
  def this(master: String, appName: String, sparkHome: String, jars: Array[String],
      environment: JMap[String, String]) =
    this(new SparkContext(master, appName, sparkHome, jars.toSeq, environment.asScala))

  private[spark] val env = sc.env

  def statusTracker: JavaSparkStatusTracker = new JavaSparkStatusTracker(sc)

  def isLocal: java.lang.Boolean = sc.isLocal

  def sparkUser: String = sc.sparkUser

  def master: String = sc.master

  def appName: String = sc.appName

  def jars: util.List[String] = sc.jars.asJava

  def startTime: java.lang.Long = sc.startTime

  /** The version of Spark on which this application is running.
    * 应用程序运行的Spark版本。
    * */
  def version: String = sc.version

  /** Default level of parallelism to use when not given by user (e.g. parallelize and makeRDD).
    * 当用户没指定的时候，默认的并行级别(e.g. parallelize and makeRDD).
    * */
  def defaultParallelism: java.lang.Integer = sc.defaultParallelism

  /** Default min number of partitions for Hadoop RDDs when not given by user
    * 当用户没指定的时候，默认最小的分区数
    * */
  def defaultMinPartitions: java.lang.Integer = sc.defaultMinPartitions

  /** Distribute a local Scala collection to form an RDD.
    * 分配本地Scala集合形成一个RDD。
    * */
  def parallelize[T](list: java.util.List[T], numSlices: Int): JavaRDD[T] = {
    implicit val ctag: ClassTag[T] = fakeClassTag
    sc.parallelize(list.asScala, numSlices)
  }

  /** Get an RDD that has no partitions or elements.
    * 得到一个RDD 没有分区，或者没有元素
    * */
  def emptyRDD[T]: JavaRDD[T] = {
    implicit val ctag: ClassTag[T] = fakeClassTag
    JavaRDD.fromRDD(new EmptyRDD[T](sc))
  }


  /** Distribute a local Scala collection to form an RDD.
    * 分配本地Scala集合形成一个RDD。
    * */
  def parallelize[T](list: java.util.List[T]): JavaRDD[T] =
    parallelize(list, sc.defaultParallelism)

  /** Distribute a local Scala collection to form an RDD.
    * 分配本地Scala集合形成一个RDD。
    * */
  def parallelizePairs[K, V](list: java.util.List[Tuple2[K, V]], numSlices: Int)
  : JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = fakeClassTag
    implicit val ctagV: ClassTag[V] = fakeClassTag
    JavaPairRDD.fromRDD(sc.parallelize(list.asScala, numSlices))
  }

  /** Distribute a local Scala collection to form an RDD.
    * 分配本地Scala集合形成一个RDD。
    * */
  def parallelizePairs[K, V](list: java.util.List[Tuple2[K, V]]): JavaPairRDD[K, V] =
    parallelizePairs(list, sc.defaultParallelism)

  /** Distribute a local Scala collection to form an RDD.
    * 分配本地Scala集合形成一个RDD。
    * */
  def parallelizeDoubles(list: java.util.List[java.lang.Double], numSlices: Int): JavaDoubleRDD =
    JavaDoubleRDD.fromRDD(sc.parallelize(list.asScala.map(_.doubleValue()), numSlices))

  /** Distribute a local Scala collection to form an RDD.
    * 分配本地Scala集合形成一个RDD。
    * */
  def parallelizeDoubles(list: java.util.List[java.lang.Double]): JavaDoubleRDD =
    parallelizeDoubles(list, sc.defaultParallelism)

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
    * 从HDFS，或者本地文件系统（所有节点都可以得到数据） 或者任何hadoop支持的文件系统的url
    * 上读取一个text文件，返回一个String型的RDD
   */
  def textFile(path: String): JavaRDD[String] = sc.textFile(path)

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
    *
    * 从HDFS，或者本地文件系统（所有节点都可以得到数据） 或者任何hadoop支持的文件系统的url
    * 上读取一个text文件，返回一个String型的RDD
   */
  def textFile(path: String, minPartitions: Int): JavaRDD[String] =
    sc.textFile(path, minPartitions)



  /**
   * Read a directory of text files from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI. Each file is read as a single record and returned in a
   * key-value pair, where the key is the path of each file, the value is the content of each file.
    *
    * 从HDFS，或者本地文件系统（所有节点都可以得到数据） 或者任何hadoop支持的文件系统的url
    * 上读取一个文件夹里面都是text文件，每个文件被读取为单个记录，并返回一个键值对，其中键是每个文件的路径，
    * 该值是每个文件的内容。
    *
   *
   * <p> For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do
   * {{{
   *   JavaPairRDD<String, String> rdd = sparkContext.wholeTextFiles("hdfs://a-hdfs-path")
   * }}}
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
    *       小文件优先，大文件也是允许的，但可能会造成性能不好。
   *
   * @param minPartitions A suggestion value of the minimal splitting number for input data.
    *                      用于输入数据的最小分区数建议值。
   */
  def wholeTextFiles(path: String, minPartitions: Int): JavaPairRDD[String, String] =
    new JavaPairRDD(sc.wholeTextFiles(path, minPartitions))

  /**
   * Read a directory of text files from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI. Each file is read as a single record and returned in a
   * key-value pair, where the key is the path of each file, the value is the content of each file.
    *
    * 读目录从HDFS的文件，本地文件系统（所有节点上可用），或任何Hadoop支持的文件系统的URI。
    * 每个文件被读取为单个记录，并返回一个键值对，其中键是每个文件的路径，该值是每个文件的内容。
    *
    *
   *
   * @see `wholeTextFiles(path: String, minPartitions: Int)`.
   */
  def wholeTextFiles(path: String): JavaPairRDD[String, String] =
    new JavaPairRDD(sc.wholeTextFiles(path))

  /**
   * Read a directory of binary files from HDFS, a local file system (available on all nodes),
   * or any Hadoop-supported file system URI as a byte array. Each file is read as a single
   * record and returned in a key-value pair, where the key is the path of each file,
   * the value is the content of each file.
    *
   *  读目录从HDFS的文件，本地文件系统（所有节点上可用），或任何Hadoop支持的文件系统的URI作为一个byte数组。
    * 每个文件被读取为单个记录，并返回一个键值对，其中键是每个文件的路径，该值是每个文件的内容。
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
   * {{{
   *   JavaPairRDD<String, byte[]> rdd = sparkContext.dataStreamFiles("hdfs://a-hdfs-path")
   * }}}
   *
   * then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *
   * @note Small files are preferred; very large files but may cause bad performance.
   *       小文件优先，大文件也是允许的，但可能会造成性能不好。
   * @param minPartitions A suggestion value of the minimal splitting number for input data.
    *                       用于输入数据的最小分区数建议值。
   */
  def binaryFiles(path: String, minPartitions: Int): JavaPairRDD[String, PortableDataStream] =
    new JavaPairRDD(sc.binaryFiles(path, minPartitions))

  /**
   * Read a directory of binary files from HDFS, a local file system (available on all nodes),
   * or any Hadoop-supported file system URI as a byte array. Each file is read as a single
   * record and returned in a key-value pair, where the key is the path of each file,
   * the value is the content of each file.
    *
    * 读二进制文件从HDFS的文件，本地文件系统（所有节点上可用），或任何Hadoop支持的文件系统的URI作为一个byte数组。
    * 每个文件被读取为单个记录，并返回一个键值对，其中键是每个文件的路径，该值是每个文件的内容。
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
   * {{{
   *   JavaPairRDD<String, byte[]> rdd = sparkContext.dataStreamFiles("hdfs://a-hdfs-path")
   * }}},
   *
   * then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *
   * @note Small files are preferred; very large files but may cause bad performance.
   */
  def binaryFiles(path: String): JavaPairRDD[String, PortableDataStream] =
    new JavaPairRDD(sc.binaryFiles(path, defaultMinPartitions))

  /**
   * Load data from a flat binary file, assuming the length of each record is constant.
   *
    * 从二进制文件加载数据，假设每个记录的长度是恒定的。
    *
   * @param path Directory to the input data files
    *             输入目录
   * @return An RDD of data with values, represented as byte arrays
    *         一个RDD里面是值，作为一个byte数组
   */
  def binaryRecords(path: String, recordLength: Int): JavaRDD[Array[Byte]] = {
    new JavaRDD(sc.binaryRecords(path, recordLength))
  }

  /**
   * Get an RDD for a Hadoop SequenceFile with given key and value types.
    * 得到一个RDD为Hadoop SequenceFile使用给定的键和值的类型。
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
    *
    * 由于Hadoop的RecordReader类re-uses 相同的可写的对象为每个记录，直接缓存返回的RDD会创造出很多引用
    * 相同的对象。如果你打算直接缓存Hadoop可写的对象，你应该首先将它们复制使用`map`函数。
    *
   */
  def sequenceFile[K, V](path: String,
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int
    ): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    new JavaPairRDD(sc.sequenceFile(path, keyClass, valueClass, minPartitions))
  }

  /**
   * Get an RDD for a Hadoop SequenceFile.  得到一个Hadoop SequenceFile RDD。
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
    *
    * 由于Hadoop的RecordReader类re-uses 相同的可写的对象为每个记录，直接缓存返回的RDD会创造出很多引用
    * 相同的对象。如果你打算直接缓存Hadoop可写的对象，你应该首先将它们复制使用`map`函数。
   */
  def sequenceFile[K, V](path: String, keyClass: Class[K], valueClass: Class[V]):
  JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    new JavaPairRDD(sc.sequenceFile(path, keyClass, valueClass))
  }

  /**
   * Load an RDD saved as a SequenceFile containing serialized objects, with NullWritable keys and
   * BytesWritable values that contain a serialized partition. This is still an experimental storage
   * format and may not be supported exactly as is in future Spark releases. It will also be pretty
   * slow if you use the default serializer (Java serialization), though the nice thing about it is
   * that there's very little effort required to save arbitrary objects.
    *
    * 加载一个RDD保存为SequenceFile包含序列化的对象，使用nullwritable keys和byteswritable values包含
    * 序列化的分区。这仍然是一个实验性的存储。格式可能不支持正是因为未来的Spark版本。这也将是相当缓慢，
    * 如果使用默认的序列化程序（java序列化），但好的事情是有一点小小的努力，需要保存任意对象。
   */
  def objectFile[T](path: String, minPartitions: Int): JavaRDD[T] = {
    implicit val ctag: ClassTag[T] = fakeClassTag
    sc.objectFile(path, minPartitions)(ctag)
  }

  /**
   * Load an RDD saved as a SequenceFile containing serialized objects, with NullWritable keys and
   * BytesWritable values that contain a serialized partition. This is still an experimental storage
   * format and may not be supported exactly as is in future Spark releases. It will also be pretty
   * slow if you use the default serializer (Java serialization), though the nice thing about it is
   * that there's very little effort required to save arbitrary objects.
    *
    * 加载一个RDD保存为SequenceFile包含序列化的对象，使用nullwritable键和byteswritable值包含序列化的分区。
    * 这仍然是一个实验性的存储。格式可能不支持正是因为未来的Spark版本。。这也将是相当缓慢，
    * 如果使用默认的序列化程序（java序列化），但好的事情是有一点小小的努力，需要保存任意对象。
   */
  def objectFile[T](path: String): JavaRDD[T] = {
    implicit val ctag: ClassTag[T] = fakeClassTag
    sc.objectFile(path)(ctag)
  }

  /**
   * Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf giving its InputFormat and any
   * other necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable,
   * etc).
    *
    * 从Hadoop jobconf给InputFormat和任何其他必要信息得到一个Hadoop可读数据RDD
    * （一个基于文件系统的数据集，HyperTable的表名称等）。
   *
   * @param conf JobConf for setting up the dataset. Note: This will be put into a Broadcast.
   *             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
   *             sure you won't modify the conf. A safe approach is always creating a new conf for
   *             a new RDD.
    *
    *            jobconf设置数据集。注意：这将投入广播。因此如果你计划使用这个配置创建多个RDDs，
    *            你需要确保你不会修改设置一个安全的方法是创建一个新的RDD新配置。
    *
   * @param inputFormatClass Class of the InputFormat
   * @param keyClass Class of the keys
   * @param valueClass Class of the values
   * @param minPartitions Minimum number of Hadoop Splits to generate.
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
    *
    * 由于Hadoop的RecordReader类re-uses 相同的可写的对象为每个记录，直接缓存返回的RDD会创造出很多引用
    * 相同的对象。如果你打算直接缓存Hadoop可写的对象，你应该首先将它们复制使用`map`函数。
    *
   */
  def hadoopRDD[K, V, F <: InputFormat[K, V]](
    conf: JobConf,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int
    ): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    val rdd = sc.hadoopRDD(conf, inputFormatClass, keyClass, valueClass, minPartitions)
    new JavaHadoopRDD(rdd.asInstanceOf[HadoopRDD[K, V]])
  }

  /**
   * Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf giving its InputFormat and any
   * other necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable,
    *
    * 从Hadoop jobconf给InputFormat和任何其他必要信息得到一个Hadoop可读数据RDD
    * （一个基于文件系统的数据集，HyperTable的表名称等）。
    *
   * @param conf JobConf for setting up the dataset. Note: This will be put into a Broadcast.
   *             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
   *             sure you won't modify the conf. A safe approach is always creating a new conf for
   *             a new RDD.
    *
    *             jobconf设置数据集。注意：这将投入广播。因此如果你计划使用这个配置创建多个RDDs，
    *            你需要确保你不会修改设置一个安全的方法是创建一个新的RDD新配置。
    *
   * @param inputFormatClass Class of the InputFormat
   * @param keyClass Class of the keys
   * @param valueClass Class of the values
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
    *
    * 由于Hadoop的RecordReader类re-uses 相同的可写的对象为每个记录，直接缓存返回的RDD会创造出很多引用
    * 相同的对象。如果你打算直接缓存Hadoop可写的对象，你应该首先将它们复制使用`map`函数。
   */
  def hadoopRDD[K, V, F <: InputFormat[K, V]](
    conf: JobConf,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V]
    ): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    val rdd = sc.hadoopRDD(conf, inputFormatClass, keyClass, valueClass)
    new JavaHadoopRDD(rdd.asInstanceOf[HadoopRDD[K, V]])
  }

  /**
   * Get an RDD for a Hadoop file with an arbitrary InputFormat.
    * 得到一个任意InputFormat输入格式的Hadoop文件得到一个RDD。
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
    *
    * 由于Hadoop的RecordReader类re-uses 相同的可写的对象为每个记录，直接缓存返回的RDD会创造出很多引用
    * 相同的对象。如果你打算直接缓存Hadoop可写的对象，你应该首先将它们复制使用`map`函数。
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](
    path: String,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int
    ): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    val rdd = sc.hadoopFile(path, inputFormatClass, keyClass, valueClass, minPartitions)
    new JavaHadoopRDD(rdd.asInstanceOf[HadoopRDD[K, V]])
  }

  /**
   * Get an RDD for a Hadoop file with an arbitrary InputFormat
   * 得到一个任意InputFormat输入格式的Hadoop文件得到一个RDD。
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
    *
    *  由于Hadoop的RecordReader类re-uses 相同的可写的对象为每个记录，直接缓存返回的RDD会创造出很多引用
    * 相同的对象。如果你打算直接缓存Hadoop可写的对象，你应该首先将它们复制使用`map`函数。
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](
    path: String,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V]
    ): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    val rdd = sc.hadoopFile(path, inputFormatClass, keyClass, valueClass)
    new JavaHadoopRDD(rdd.asInstanceOf[HadoopRDD[K, V]])
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
    * 在给定的任意新的API InputFormat和额外的配置选项传递给输入格式Hadoop文件RDD。
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
    *
    * 由于Hadoop的RecordReader类re-uses 相同的可写的对象为每个记录，直接缓存返回的RDD会创造出很多引用
    * 相同的对象。如果你打算直接缓存Hadoop可写的对象，你应该首先将它们复制使用`map`函数。
    *
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
    path: String,
    fClass: Class[F],
    kClass: Class[K],
    vClass: Class[V],
    conf: Configuration): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(kClass)
    implicit val ctagV: ClassTag[V] = ClassTag(vClass)
    val rdd = sc.newAPIHadoopFile(path, fClass, kClass, vClass, conf)
    new JavaNewHadoopRDD(rdd.asInstanceOf[NewHadoopRDD[K, V]])
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
    *
    * 在给定的任意新的API InputFormat和额外的配置选项传递给输入格式Hadoop文件RDD。
   *
   * @param conf Configuration for setting up the dataset. Note: This will be put into a Broadcast.
   *             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
   *             sure you won't modify the conf. A safe approach is always creating a new conf for
   *             a new RDD.
    *
    *              jobconf设置数据集。注意：这将投入广播。因此如果你计划使用这个配置创建多个RDDs，
    *            你需要确保你不会修改设置一个安全的方法是创建一个新的RDD新配置。
    *
   * @param fClass Class of the InputFormat
   * @param kClass Class of the keys
   * @param vClass Class of the values
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
    *
    * 由于Hadoop的RecordReader类re-uses 相同的可写的对象为每个记录，直接缓存返回的RDD会创造出很多引用
    * 相同的对象。如果你打算直接缓存Hadoop可写的对象，你应该首先将它们复制使用`map`函数。
    *
   */
  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
    conf: Configuration,
    fClass: Class[F],
    kClass: Class[K],
    vClass: Class[V]): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(kClass)
    implicit val ctagV: ClassTag[V] = ClassTag(vClass)
    val rdd = sc.newAPIHadoopRDD(conf, fClass, kClass, vClass)
    new JavaNewHadoopRDD(rdd.asInstanceOf[NewHadoopRDD[K, V]])
  }

  /** Build the union of two or more RDDs.
    * 求2个或者多个RDD的交集
    * */
  override def union[T](first: JavaRDD[T], rest: java.util.List[JavaRDD[T]]): JavaRDD[T] = {
    val rdds: Seq[RDD[T]] = (Seq(first) ++ rest.asScala).map(_.rdd)
    implicit val ctag: ClassTag[T] = first.classTag
    sc.union(rdds)
  }

  /** Build the union of two or more RDDs.
    * 求2个或者多个RDD的交集
    * */
  override def union[K, V](first: JavaPairRDD[K, V], rest: java.util.List[JavaPairRDD[K, V]])
      : JavaPairRDD[K, V] = {
    val rdds: Seq[RDD[(K, V)]] = (Seq(first) ++ rest.asScala).map(_.rdd)
    implicit val ctag: ClassTag[(K, V)] = first.classTag
    implicit val ctagK: ClassTag[K] = first.kClassTag
    implicit val ctagV: ClassTag[V] = first.vClassTag
    new JavaPairRDD(sc.union(rdds))
  }

  /** Build the union of two or more RDDs.
    * 求2个或者多个RDD的交集
    * */
  override def union(first: JavaDoubleRDD, rest: java.util.List[JavaDoubleRDD]): JavaDoubleRDD = {
    val rdds: Seq[RDD[Double]] = (Seq(first) ++ rest.asScala).map(_.srdd)
    new JavaDoubleRDD(sc.union(rdds))
  }

  /**
   * Create an [[org.apache.spark.Accumulator]] integer variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
    *
    * 创建一个[[org.apache.spark.Accumulator]]整形累加器，它可以通过`add`方法添加值，只有主节点
    * 能访问它的值。
    */
  @deprecated("use sc().longAccumulator()", "2.0.0")
  def intAccumulator(initialValue: Int): Accumulator[java.lang.Integer] =
    sc.accumulator(initialValue)(IntAccumulatorParam).asInstanceOf[Accumulator[java.lang.Integer]]

  /**
   * Create an [[org.apache.spark.Accumulator]] integer variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
    *
    * 创建一个[[org.apache.spark.Accumulator]]整形累加器，它可以通过`add`方法添加值，
    * 只有主节点能访问它的值。
   *
   * This version supports naming the accumulator for display in Spark's web UI.
    * 在Spark的web UI中显示累加器的值
   */
  @deprecated("use sc().longAccumulator(String)", "2.0.0")
  def intAccumulator(initialValue: Int, name: String): Accumulator[java.lang.Integer] =
    sc.accumulator(initialValue, name)(IntAccumulatorParam)
      .asInstanceOf[Accumulator[java.lang.Integer]]

  /**
   * Create an [[org.apache.spark.Accumulator]] double variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
    *
    * 创建一个[[org.apache.spark.Accumulator]]整形累加器，它可以通过`add`方法添加值，
    * 只有主节点能访问它的值。
   */
  @deprecated("use sc().doubleAccumulator()", "2.0.0")
  def doubleAccumulator(initialValue: Double): Accumulator[java.lang.Double] =
    sc.accumulator(initialValue)(DoubleAccumulatorParam).asInstanceOf[Accumulator[java.lang.Double]]

  /**
   * Create an [[org.apache.spark.Accumulator]] double variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
   *
    * 创建一个[[org.apache.spark.Accumulator]]整形累加器，它可以通过`add`方法添加值，
    * 只有主节点能访问它的值。
    *
   * This version supports naming the accumulator for display in Spark's web UI.
    * 在Spark的web UI中显示累加器的值
   */
  @deprecated("use sc().doubleAccumulator(String)", "2.0.0")
  def doubleAccumulator(initialValue: Double, name: String): Accumulator[java.lang.Double] =
    sc.accumulator(initialValue, name)(DoubleAccumulatorParam)
      .asInstanceOf[Accumulator[java.lang.Double]]

  /**
   * Create an [[org.apache.spark.Accumulator]] integer variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
    *
    * 创建一个[[org.apache.spark.Accumulator]]整形累加器，它可以通过`add`方法添加值，
    * 只有主节点能访问它的值。
   */
  @deprecated("use sc().longAccumulator()", "2.0.0")
  def accumulator(initialValue: Int): Accumulator[java.lang.Integer] = intAccumulator(initialValue)

  /**
   * Create an [[org.apache.spark.Accumulator]] integer variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
   *
    * 创建一个[[org.apache.spark.Accumulator]]整形累加器，它可以通过`add`方法添加值，
    * 只有主节点能访问它的值。
    *
   * This version supports naming the accumulator for display in Spark's web UI.
    * 在Spark的web UI中显示累加器的值
   */
  @deprecated("use sc().longAccumulator(String)", "2.0.0")
  def accumulator(initialValue: Int, name: String): Accumulator[java.lang.Integer] =
    intAccumulator(initialValue, name)

  /**
   * Create an [[org.apache.spark.Accumulator]] double variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
    *
    * 创建一个[[org.apache.spark.Accumulator]]整形累加器，它可以通过`add`方法添加值，
    * 只有主节点能访问它的值。
    *
   */
  @deprecated("use sc().doubleAccumulator()", "2.0.0")
  def accumulator(initialValue: Double): Accumulator[java.lang.Double] =
    doubleAccumulator(initialValue)


  /**
   * Create an [[org.apache.spark.Accumulator]] double variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
   *
    * 创建一个[[org.apache.spark.Accumulator]]整形累加器，它可以通过`add`方法添加值，
    * 只有主节点能访问它的值。
    *
   * This version supports naming the accumulator for display in Spark's web UI.
    * 在Spark的web UI中显示累加器的值
   */
  @deprecated("use sc().doubleAccumulator(String)", "2.0.0")
  def accumulator(initialValue: Double, name: String): Accumulator[java.lang.Double] =
    doubleAccumulator(initialValue, name)

  /**
   * Create an [[org.apache.spark.Accumulator]] variable of a given type, which tasks can "add"
   * values to using the `add` method. Only the master can access the accumulator's `value`.
    *
    * 创建一个[[org.apache.spark.Accumulator]]整形累加器，它可以通过`add`方法添加值，
    * 只有主节点能访问它的值。
   */
  @deprecated("use AccumulatorV2", "2.0.0")
  def accumulator[T](initialValue: T, accumulatorParam: AccumulatorParam[T]): Accumulator[T] =
    sc.accumulator(initialValue)(accumulatorParam)

  /**
   * Create an [[org.apache.spark.Accumulator]] variable of a given type, which tasks can "add"
   * values to using the `add` method. Only the master can access the accumulator's `value`.
   *
    *  创建一个[[org.apache.spark.Accumulator]]整形累加器，它可以通过`add`方法添加值，
    * 只有主节点能访问它的值。
    *
   * This version supports naming the accumulator for display in Spark's web UI.
    * 在Spark的web UI中显示累加器的值
   */
  @deprecated("use AccumulatorV2", "2.0.0")
  def accumulator[T](initialValue: T, name: String, accumulatorParam: AccumulatorParam[T])
      : Accumulator[T] =
    sc.accumulator(initialValue, name)(accumulatorParam)

  /**
   * Create an [[org.apache.spark.Accumulable]] shared variable of the given type, to which tasks
   * can "add" values with `add`. Only the master can access the accumulable's `value`.
    *
    * 创建一个[[org.apache.spark.Accumulator]]整形累加器，它可以通过`add`方法添加值，
    * 只有主节点能访问它的值。
    *
   */
  @deprecated("use AccumulatorV2", "2.0.0")
  def accumulable[T, R](initialValue: T, param: AccumulableParam[T, R]): Accumulable[T, R] =
    sc.accumulable(initialValue)(param)

  /**
   * Create an [[org.apache.spark.Accumulable]] shared variable of the given type, to which tasks
   * can "add" values with `add`. Only the master can access the accumulable's `value`.
    *
   *  创建一个[[org.apache.spark.Accumulator]]整形累加器，它可以通过`add`方法添加值，
    * 只有主节点能访问它的值。
    *
   * This version supports naming the accumulator for display in Spark's web UI.
    * 在Spark的web UI中显示累加器的值
   */
  @deprecated("use AccumulatorV2", "2.0.0")
  def accumulable[T, R](initialValue: T, name: String, param: AccumulableParam[T, R])
      : Accumulable[T, R] =
    sc.accumulable(initialValue, name)(param)

  /**
   * Broadcast a read-only variable to the cluster, returning a
   * [[org.apache.spark.broadcast.Broadcast]] object for reading it in distributed functions.
   * The variable will be sent to each cluster only once.
    *
    * 集群可读的广播变量，返回一个[[org.apache.spark.broadcast.Broadcast]]对象为了读取数据从分布式函数中，
    * 这个变量将会被送到集群的每个节点，仅仅发送一次。
    */
  def broadcast[T](value: T): Broadcast[T] = sc.broadcast(value)(fakeClassTag)

  /** Shut down the SparkContext.
    * 停止SparkContext
    * */
  def stop() {
    sc.stop()
  }

  override def close(): Unit = stop()

  /**
   * Get Spark's home location from either a value set through the constructor,
   * or the spark.home Java property, or the SPARK_HOME environment variable
   * (in that order of preference). If neither of these is set, return None.
    *
    * 无论是通过构造函数的值得到或spark.home java属性获取Spark的家目录。或spark_home环境变量
    * （按优先顺序排列）。如果两者都没有设置，则返回none。
   */
  def getSparkHome(): Optional[String] = JavaUtils.optionToOptional(sc.getSparkHome())

  /**
   * Add a file to be downloaded with this Spark job on every node.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.  To access the file in Spark jobs,
   * use `SparkFiles.get(fileName)` to find its download location.
    *
    * 添加一个文件要下载这个Spark的工作在每个节点上，`path`路径可以是本地文件，一个在HDFS中的文件
    * （或者任何hadoop支持的文件），或者一个HTTP，HTTPS和FTP的URI。访问火Spark jobs的文件，
    *  使用`SparkFiles.get(fileName)` 找到下载位置。
    *
   */
  def addFile(path: String) {
    sc.addFile(path)
  }

  /**
   * Add a file to be downloaded with this Spark job on every node.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.  To access the file in Spark jobs,
   * use `SparkFiles.get(fileName)` to find its download location.
    *
    * 添加一个文件要下载这个Spark的工作在每个节点上，`path`路径可以是本地文件，一个在HDFS中的文件
    * （或者任何hadoop支持的文件），或者一个HTTP，HTTPS和FTP的URI。访问火Spark jobs的文件，
    *  使用`SparkFiles.get(fileName)` 找到下载位置。
    *
   * A directory can be given if the recursive option is set to true. Currently directories are only
   * supported for Hadoop-supported filesystems.
    *
    * 如果递归选项设置为true，可以给出一个目录。目前目录只支持Hadoop支持的文件系统。
   */
  def addFile(path: String, recursive: Boolean): Unit = {
    sc.addFile(path, recursive)
  }

  /**
   * Adds a JAR dependency for all tasks to be executed on this SparkContext in the future.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.
    *
    * 为所有的task任务添加jar的依赖在每个executed的SparkContext `path`路径可以是本地文件，
    * 在HDFS（Hadoop文件或其他支持文件），或者一个HTTP，HTTPS和FTP的URI。
    *
   */
  def addJar(path: String) {
    sc.addJar(path)
  }

  /**
   * Returns the Hadoop configuration used for the Hadoop code (e.g. file systems) we reuse.
   * 返回hadoop的configuration配置代码（如文件系统）我们重用。
    *
   * @note As it will be reused in all Hadoop RDDs, it's better not to modify it unless you
   * plan to set some global configurations for all Hadoop RDDs.
    *
    * 它将在所有的Hadoop RDDS重用，最好不要去修改它，除非你计划为所有的Hadoop RDDS设置一些全局配置。
   */
  def hadoopConfiguration(): Configuration = {
    sc.hadoopConfiguration
  }

  /**
   * Set the directory under which RDDs are going to be checkpointed. The directory must
   * be a HDFS path if running on a cluster.
    * 设置一个目录，该目录下的RDDS将设置检查点。如果运行在集群上该目录必须是一个HDFS路径。
   */
  def setCheckpointDir(dir: String) {
    sc.setCheckpointDir(dir)
  }

  def getCheckpointDir: Optional[String] = JavaUtils.optionToOptional(sc.getCheckpointDir)

  protected def checkpointFile[T](path: String): JavaRDD[T] = {
    implicit val ctag: ClassTag[T] = fakeClassTag
    new JavaRDD(sc.checkpointFile(path))
  }

  /**
   * Return a copy of this JavaSparkContext's configuration. The configuration ''cannot'' be
   * changed at runtime.
    * 返回一个JavaSparkContext's configuration的拷贝，配置的'cannot”可以在运行时改变。
   */
  def getConf: SparkConf = sc.getConf

  /**
   * Pass-through to SparkContext.setCallSite.  For API support only.
    * 设置SparkContext.setCallSite。仅用于API支持。
   */
  def setCallSite(site: String) {
    sc.setCallSite(site)
  }

  /**
   * Pass-through to SparkContext.setCallSite.  For API support only.
    * 清除SparkContext.setCallSite。仅用于API支持。
   */
  def clearCallSite() {
    sc.clearCallSite()
  }

  /**
   * Set a local property that affects jobs submitted from this thread, and all child
   * threads, such as the Spark fair scheduler pool.
    * 设置一个影响此线程提交的作业的所有本地属性，以及所有子线程，如Spark公平调度程序池。
    *
   *
   * These properties are inherited by child threads spawned from this thread. This
   * may have unexpected consequences when working with thread pools. The standard java
   * implementation of thread pools have worker threads spawn other worker threads.
   * As a result, local properties may propagate unpredictably.
    *
    * 这些性质都是由孩子从这个线程的线程继承。这可能会有意想不到的后果当工作在线程池。
    * 线程池线程标准java实现产卵其他工作线程，性能可能传播不可预知的地方。
    *
   */
  def setLocalProperty(key: String, value: String): Unit = sc.setLocalProperty(key, value)

  /**
   * Get a local property set in this thread, or null if it is missing. See
   * `org.apache.spark.api.java.JavaSparkContext.setLocalProperty`.
    * 得到一个本地的属性在这个线程中，如果丢失则返回NULL,看`org.apache.spark.api.java.JavaSparkContext.setLocalProperty`.
   */
  def getLocalProperty(key: String): String = sc.getLocalProperty(key)

  /** Control our logLevel. This overrides any user-defined log settings.
    *   控制我们的日志级别，这个将重写用户定义的log设置
   * @param logLevel The desired log level as a string.
    *                 参数是一个字符串
   * Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    *

    *
   */
  def setLogLevel(logLevel: String) {
    sc.setLogLevel(logLevel)
  }

  /**
   * Assigns a group ID to all the jobs started by this thread until the group ID is set to a
   * different value or cleared.
    *
    * 将组ID分配给该线程启动的所有作业，直到组ID被设置为不同的值或清除为止。
   *
   * Often, a unit of execution in an application consists of multiple Spark actions or jobs.
   * Application programmers can use this method to group all those jobs together and give a
   * group description. Once set, the Spark web UI will associate such jobs with this group.
    *
    * 通常，应用程序中的一个执行单元由多个 Spark actions或作业组成。
    * 应用程序程序员可以使用此方法将所有这些工作组合在一起，并进行组描述。
    * 一旦设置，Spark的Web UI就会联想associate到这样的工作，与本组。
   *
   * The application can also use `org.apache.spark.api.java.JavaSparkContext.cancelJobGroup`
   * to cancel all running jobs in this group. For example,
   * {{{
   * // In the main thread:
   * sc.setJobGroup("some_job_to_cancel", "some job description");
   * rdd.map(...).count();
   *
   * // In a separate thread:
   * sc.cancelJobGroup("some_job_to_cancel");
   * }}}
   *
   * If interruptOnCancel is set to true for the job group, then job cancellation will result
   * in Thread.interrupt() being called on the job's executor threads. This is useful to help ensure
   * that the tasks are actually stopped in a timely manner, but is off by default due to HDFS-1208,
   * where HDFS may respond to Thread.interrupt() by marking nodes as dead.
   */
  def setJobGroup(groupId: String, description: String, interruptOnCancel: Boolean): Unit =
    sc.setJobGroup(groupId, description, interruptOnCancel)

  /**
   * Assigns a group ID to all the jobs started by this thread until the group ID is set to a
   * different value or cleared.
   * 将组ID分配给该线程启动的所有作业，直到组ID被设置为不同的值或清除为止。
    *
   * @see `setJobGroup(groupId: String, description: String, interruptThread: Boolean)`.
   *      This method sets interruptOnCancel to false.
   */
  def setJobGroup(groupId: String, description: String): Unit = sc.setJobGroup(groupId, description)

  /** Clear the current thread's job group ID and its description.
    * 清除当前线程的工作组ID及其描述。
    * */
  def clearJobGroup(): Unit = sc.clearJobGroup()

  /**
   * Cancel active jobs for the specified group. See
   * `org.apache.spark.api.java.JavaSparkContext.setJobGroup` for more information.
    *
    * 取消指定组的活动作业。详情查看`org.apache.spark.api.java.JavaSparkContext.setJobGroup`
    */
  def cancelJobGroup(groupId: String): Unit = sc.cancelJobGroup(groupId)

  /** Cancel all jobs that have been scheduled or are running.
    * 取消所有已被调度的或运行的工作。
    * */
  def cancelAllJobs(): Unit = sc.cancelAllJobs()

  /**
   * Returns a Java map of JavaRDDs that have marked themselves as persistent via cache() call.
   * 返回一个包含JavaRDDs的java map,这些RDD是被标志被持久化的通过调用cache()方法。
    *
   * @note This does not necessarily mean the caching or computation was successful.
    *       这并不一定意味着缓存或计算是成功的。
   */
  def getPersistentRDDs: JMap[java.lang.Integer, JavaRDD[_]] = {
    sc.getPersistentRDDs.mapValues(s => JavaRDD.fromRDD(s))
      .asJava.asInstanceOf[JMap[java.lang.Integer, JavaRDD[_]]]
  }

}

object JavaSparkContext {
  implicit def fromSparkContext(sc: SparkContext): JavaSparkContext = new JavaSparkContext(sc)

  implicit def toSparkContext(jsc: JavaSparkContext): SparkContext = jsc.sc

  /**
   * Find the JAR from which a given class was loaded, to make it easy for users to pass
   * their JARs to SparkContext.
    * 发现jar从一个给定的类被加载，让用户很容易通过sparkcontext得到他的jars。
   */
  def jarOfClass(cls: Class[_]): Array[String] = SparkContext.jarOfClass(cls).toArray

  /**
   * Find the JAR that contains the class of a particular object, to make it easy for users
   * to pass their JARs to SparkContext. In most cases you can call jarOfObject(this) in
   * your driver program.
    *
    * 发现jar从一个给定的类被加载，让用户很容易通过sparkcontext得到他的jars。在大多数情况下，
    * 在你的驱动程序可以调用arOfObject(this)。
   */
  def jarOfObject(obj: AnyRef): Array[String] = SparkContext.jarOfObject(obj).toArray

  /**
   * Produces a ClassTag[T], which is actually just a casted ClassTag[AnyRef].
    * 产生一个ClassTag[T]，其实就是一个铸造的ClassTag[AnyRef].。
   *
   * This method is used to keep ClassTags out of the external Java API, as the Java compiler
   * cannot produce them automatically. While this ClassTag-faking does please the compiler,
   * it can cause problems at runtime if the Scala API relies on ClassTags for correctness.
    *
    * 这个方法是用来保持ClassTags暴露在外的java API，java编译器不能自动产生。虽然这classtag伪造并请编译器，
    * 它能引起的问题在运行时如果Scala API依赖classtags正确。
   *
   * Often, though, a ClassTag[AnyRef] will not lead to incorrect behavior, just worse performance
   * or security issues. For instance, an Array[AnyRef] can hold any type T, but may lose primitive
   * specialization.
    *
    * 通常，虽然，一个classtag [ anyref ]不会导致错误的行为，只是糟糕的性能或安全问题。
    * 例如，一个数组[ anyref ]可以持有任何类型T，但可能会失去原始的专业化。
    *
   */
  private[spark]
  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]
}
