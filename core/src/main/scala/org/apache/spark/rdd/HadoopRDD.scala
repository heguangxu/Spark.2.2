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

package org.apache.spark.rdd

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.collection.immutable.Map
import scala.reflect.ClassTag

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapred.lib.CombineFileSplit
import org.apache.hadoop.mapreduce.TaskType
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.IGNORE_CORRUPT_FILES
import org.apache.spark.rdd.HadoopRDD.HadoopMapPartitionsWithSplitRDD
import org.apache.spark.scheduler.{HDFSCacheTaskLocation, HostTaskLocation}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{NextIterator, SerializableConfiguration, ShutdownHookManager}

/**
  * A Spark split class that wraps around a Hadoop InputSplit.
  *
  * 一个Spark分割类，环绕一个Hadoop的InputSplit。
  *
  */
private[spark] class HadoopPartition(rddId: Int, override val index: Int, s: InputSplit)
  extends Partition {

  val inputSplit = new SerializableWritable[InputSplit](s)

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)

  /**
    * Get any environment variables that should be added to the users environment when running pipes
    *
    * 获取运行管道pipes时应添加到用户环境中的任何环境变量
    *
    * @return a Map with the environment variables and corresponding values, it could be empty
    *        带有环境变量和对应值的映射，它可以是空的。
    */
  def getPipeEnvVars(): Map[String, String] = {
    val envVars: Map[String, String] = if (inputSplit.value.isInstanceOf[FileSplit]) {
      val is: FileSplit = inputSplit.value.asInstanceOf[FileSplit]
      // map_input_file is deprecated in favor of mapreduce_map_input_file but set both
      // since it's not removed yet
      Map("map_input_file" -> is.getPath().toString(),
        "mapreduce_map_input_file" -> is.getPath().toString())
    } else {
      Map()
    }
    envVars
  }
}

/**
  * :: DeveloperApi ::
  * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
  * sources in HBase, or S3), using the older MapReduce API (`org.apache.hadoop.mapred`).
  *
  * 提供一个RDD读取存储在hadoop上数据的核心功能（例如，文件在HDFS中，数据源在HBase中，或S3中），
  * 使用旧的MapReduce API (`org.apache.hadoop.mapred`).
  *
  * @param sc The SparkContext to associate the RDD with.
  *           与RDD有关的SparkContext
  * @param broadcastedConf A general Hadoop Configuration, or a subclass of it. If the enclosed
  *   variable references an instance of JobConf, then that JobConf will be used for the Hadoop job.
  *   Otherwise, a new JobConf will be created on each slave using the enclosed Configuration.
  *
  *   一般的Hadoop的配置，或是Hadoop Configuration的子类。如果封闭jobconf变量引用实例，那么JobConf将用于Hadoop的工作。
  *   否则，一个新的jobconf将被创建在每个奴隶使用封闭式结构。
  *
  * @param initLocalJobConfFuncOpt Optional closure used to initialize any JobConf that HadoopRDD
  *     creates.
  *     可选关闭用来初始化任何jobconf，hadooprdd创建。
  * @param inputFormatClass Storage format of the data to be read.
  *                         要读取的数据的存储格式。
  * @param keyClass Class of the key associated with the inputFormatClass.
  *                 inputformatclass相关key类。
  * @param valueClass Class of the value associated with the inputFormatClass.
  *                   inputformatclass相关value类。
  * @param minPartitions Minimum number of HadoopRDD partitions (Hadoop Splits) to generate.
  *                      HadoopRDD要生成的最小的分区（Hadoop Splits）
  *
  * @note Instantiating this class directly is not recommended, please use
  * `org.apache.spark.SparkContext.hadoopRDD()`
  *
  * @注意  实例化这个类，直接不推荐，请使用`org.apache.spark.SparkContext.hadoopRDD()`。
  */
@DeveloperApi
class HadoopRDD[K, V](
                       sc: SparkContext,
                       broadcastedConf: Broadcast[SerializableConfiguration],
                       initLocalJobConfFuncOpt: Option[JobConf => Unit],
                       inputFormatClass: Class[_ <: InputFormat[K, V]],
                       keyClass: Class[K],
                       valueClass: Class[V],
                       minPartitions: Int)
  extends RDD[(K, V)](sc, Nil) with Logging {   // 由于HadoopRDD继承RDD 因此其分区函数partitioner=null

  //  分区函数：因为其hadooprdd继承自rdd，因此其分区函数为null,实际上rdd的每个分区对应的hadoop上文件的数据块
  // 假设1个文件100M，hdfs配置的快大小为64M,则该hadoopRdd的每个分区已经对应一个数据块，因此其分区函数就不需要设置了


  if (initLocalJobConfFuncOpt.isDefined) {
    sparkContext.clean(initLocalJobConfFuncOpt.get)
  }

  def this(
            sc: SparkContext,
            conf: JobConf,
            inputFormatClass: Class[_ <: InputFormat[K, V]],
            keyClass: Class[K],
            valueClass: Class[V],
            minPartitions: Int) = {
    this(
      sc,
      sc.broadcast(new SerializableConfiguration(conf))
        .asInstanceOf[Broadcast[SerializableConfiguration]],
      initLocalJobConfFuncOpt = None,
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions)
  }

  protected val jobConfCacheKey: String = "rdd_%d_job_conf".format(id)

  protected val inputFormatCacheKey: String = "rdd_%d_input_format".format(id)

  // used to build JobTracker ID
  private val createTime = new Date()

  private val shouldCloneJobConf = sparkContext.conf.getBoolean("spark.hadoop.cloneConf", false)

  private val ignoreCorruptFiles = sparkContext.conf.get(IGNORE_CORRUPT_FILES)

  // Returns a JobConf that will be used on slaves to obtain input splits for Hadoop reads.
  // 返回一个jobconf将用于slaves节点获得输入将Hadoop的读取。
  protected def getJobConf(): JobConf = {
    // 从broadcast中获取jobConf，此处的jobConf正是hadoopConfiguration.
    val conf: Configuration = broadcastedConf.value.value
    if (shouldCloneJobConf) {
      // Hadoop Configuration objects are not thread-safe, which may lead to various problems if
      // one job modifies a configuration while another reads it (SPARK-2546).  This problem occurs
      // somewhat rarely because most jobs treat the configuration as though it's immutable.  One
      // solution, implemented here, is to clone the Configuration object.  Unfortunately, this
      // clone can be very expensive.  To avoid unexpected performance regressions for workloads and
      // Hadoop versions that do not suffer from these thread-safety issues, this cloning is
      // disabled by default.

      // Hadoop的配置对象不是线程安全的，这可能会导致各种各样的问题，如果一个job修改配置而另一个读它（spark-2546）。
      // 这个问题很少发生，因为大多数工作将配置视为不可变的。在这里实现的一个解决方案是克隆配置对象。不幸的是，
      // 这个克隆可能非常昂贵。为了避免对不受这些线程安全问题影响的工作负载和Hadoop版本的意外性能回归，默认情况下禁用此克隆。

      HadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
        logDebug("Cloning Hadoop Configuration")
        val newJobConf = new JobConf(conf)
        if (!conf.isInstanceOf[JobConf]) {
          initLocalJobConfFuncOpt.foreach(f => f(newJobConf))
        }
        newJobConf
      }
    } else {
      if (conf.isInstanceOf[JobConf]) {
        logDebug("Re-using user-broadcasted JobConf")
        conf.asInstanceOf[JobConf]
      } else if (HadoopRDD.containsCachedMetadata(jobConfCacheKey)) {
        logDebug("Re-using cached JobConf")
        HadoopRDD.getCachedMetadata(jobConfCacheKey).asInstanceOf[JobConf]
      } else {
        // Create a JobConf that will be cached and used across this RDD's getJobConf() calls in the
        // local process. The local cache is accessed through HadoopRDD.putCachedMetadata().
        // The caching helps minimize GC, since a JobConf can contain ~10KB of temporary objects.
        // Synchronize to prevent ConcurrentModificationException (SPARK-1097, HADOOP-10456).

        // 创建一个jobconf将被缓存在这个RDD的getjobconf()用于本地过程调用。本地高速缓存的访问是通过hadooprdd。putcachedmetadata()。
        // 缓存有助于减少GC，因为jobconf可以包含~临时对象10kb。为了防止concurrentmodificationexception
        // 同步（spark-1097，hadoop-10456）。

        HadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
          logDebug("Creating new JobConf and caching it for later re-use")
          val newJobConf = new JobConf(conf)
          initLocalJobConfFuncOpt.foreach(f => f(newJobConf))
          HadoopRDD.putCachedMetadata(jobConfCacheKey, newJobConf)
          newJobConf
        }
      }
    }
  }

  protected def getInputFormat(conf: JobConf): InputFormat[K, V] = {
    val newInputFormat = ReflectionUtils.newInstance(inputFormatClass.asInstanceOf[Class[_]], conf)
      .asInstanceOf[InputFormat[K, V]]
    newInputFormat match {
      case c: Configurable => c.setConf(conf)
      case _ =>
    }
    newInputFormat
  }




  // 分片列表函数
  // 代表着RDD中的每个分片，其中代码里的inputSplits.size实际上就是hdfs文件的块总数
  // 按照块的个数划分，每个数据块一个分片
  override def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    val inputFormat = getInputFormat(jobConf)
    val inputSplits = inputFormat.getSplits(jobConf, minPartitions)
    val array = new Array[Partition](inputSplits.size)
    for (i <- 0 until inputSplits.size) {
      array(i) = new HadoopPartition(id, i, inputSplits(i))
    }
    array
  }


  /** 用于计算每个分片的数据
      就像是mapReduce中通过RecordReader来读取每个分片的数据，其中输入参数中的
      第一个就是Partition，代表某片分片
      HadoopRDD的的compute方法来创建Nextiterator的匿名内部类。然后将其封装为InterruptibleIterator.
    */
  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    /**
      *  NextIterator在这里是一个匿名内部类.
      *  其中构造 NextIterator步骤如下：
      *     1.从broadcast中获取jobConf，此处的jobConf正是hadoopConfiguration.
      *     2.创建InputMetrics用于计算字节读取的测量信息，然后在RecoderReader正式读取数据之前创建bytesReadCallback,
      *       bytesreadCallback用于获取当前线程从文件系统读取的字节数。
      *     3.获取inputFormat,此处的inputFormat正是TextInputFormat.
      *     4.使用addlocalConfiguration给JobConf添加Hadoop任务相关配置。
      *     5.创建RecoderdReader，调用reader.createKey()和reader.createValue()得到的正是LongWritable和Text.
      *       NextIterator的getNext实际上是代理了RecordReader的next方法并且没读取一些记录后使用bytesReadCallback
      *       更新InputMetrics的bytesRead字段。
      *     6.将NextIterator封装为InterruptibleIterator.
      *
      *     InterruptibleIterator只是对Nextiterator的代理。
      */
    val iter = new NextIterator[(K, V)] {

      private val split = theSplit.asInstanceOf[HadoopPartition]
      logInfo("Input split: " + split.inputSplit)
      // 从broadcast中获取jobConf，此处的jobConf正是hadoopConfiguration.
      private val jobConf = getJobConf()

      // 创建InputMetrics用于计算字节读取的测量信息
      private val inputMetrics = context.taskMetrics().inputMetrics
      // 总共读取的字节数
      private val existingBytesRead = inputMetrics.bytesRead

      // Sets InputFileBlockHolder for the file block's information
      // 为文件块的信息设置InputFileBlockHolder
      split.inputSplit.value match {
        case fs: FileSplit =>
          InputFileBlockHolder.set(fs.getPath.toString, fs.getStart, fs.getLength)
        case _ =>
          InputFileBlockHolder.unset()
      }

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      // 找到一个函数，该函数将返回该线程读取的文件系统字节。在创建RecordReader之前要这样做，因为RecordReader的构造函数可能会读取一些字节

      // 在RecoderReader正式读取数据之前创建bytesReadCallback,bytesreadCallback用于获取当前线程从文件系统读取的字节数。
      private val getBytesReadCallback: Option[() => Long] = split.inputSplit.value match {
        case _: FileSplit | _: CombineFileSplit =>
          Some(SparkHadoopUtil.get.getFSBytesReadOnThreadCallback())
        case _ => None
      }

      // We get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
      // 我们从线程本地的Hadoop文件系统统计数据中获取输入字节。但是，如果我们进行合并，我们很可能会在同一个任务和
      // 同一个线程中计算多个分区，在这种情况下，我们需要避免重写前面的分区(spark - 13071)所写的值。

      // 更新读取的字节数
      private def updateBytesRead(): Unit = {
        getBytesReadCallback.foreach { getBytesRead =>
          inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
        }
      }

      private var reader: RecordReader[K, V] = null
      // 获取inputFormat,此处的inputFormat正是TextInputFormat.
      private val inputFormat = getInputFormat(jobConf)
      // 4.使用addlocalConfiguration给JobConf添加Hadoop任务相关配置。
      HadoopRDD.addLocalConfiguration(
        new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(createTime),
        context.stageId, theSplit.index, context.attemptNumber, jobConf)

      //  创建RecoderdReader，调用reader.createKey()和reader.createValue()得到的正是LongWritable和Text.
      //  NextIterator的getNext实际上是代理了RecordReader的next方法并且没读取一些记录后使用bytesReadCallback
      //  更新InputMetrics的bytesRead字段。
      reader =
        try {
          inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL)
        } catch {
          case e: IOException if ignoreCorruptFiles =>
            logWarning(s"Skipped the rest content in the corrupted file: ${split.inputSplit}", e)
            finished = true
            null
        }

      // Register an on-task-completion callback to close the input stream.
      // 注册一个回调函数，当任务完成的时候，关闭输入流
      context.addTaskCompletionListener { context =>
        // Update the bytes read before closing is to make sure lingering bytesRead statistics in
        // this thread get correctly added.
        updateBytesRead()
        closeIfNeeded()
      }

      private val key: K = if (reader == null) null.asInstanceOf[K] else reader.createKey()
      private val value: V = if (reader == null) null.asInstanceOf[V] else reader.createValue()

      override def getNext(): (K, V) = {
        try {
          finished = !reader.next(key, value)
        } catch {
          case e: IOException if ignoreCorruptFiles =>
            logWarning(s"Skipped the rest content in the corrupted file: ${split.inputSplit}", e)
            finished = true
        }
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
          updateBytesRead()
        }
        (key, value)
      }

      override def close(): Unit = {
        if (reader != null) {
          InputFileBlockHolder.unset()
          try {
            reader.close()
          } catch {
            case e: Exception =>
              if (!ShutdownHookManager.inShutdown()) {
                logWarning("Exception in RecordReader.close()", e)
              }
          } finally {
            reader = null
          }
          if (getBytesReadCallback.isDefined) {
            updateBytesRead()
          } else if (split.inputSplit.value.isInstanceOf[FileSplit] ||
            split.inputSplit.value.isInstanceOf[CombineFileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.incBytesRead(split.inputSplit.value.getLength)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        }
      }
    }

    // 将NextIterator封装为InterruptibleIterator.
    new InterruptibleIterator[(K, V)](context, iter)
  }

  /** Maps over a partition, providing the InputSplit that was used as the base of the partition.
    * 在分区Maps，提供InputSplit作为分区的基础。
    * */
  @DeveloperApi
  def mapPartitionsWithInputSplit[U: ClassTag](
                                                f: (InputSplit, Iterator[(K, V)]) => Iterator[U],
                                                preservesPartitioning: Boolean = false): RDD[U] = {
    new HadoopMapPartitionsWithSplitRDD(this, f, preservesPartitioning)
  }

  // 获取每个分片的优先计算位置
  // 由于hdfs文件的数据块分散在整个hdfs集群，通过getPreferredLocations可以获取每个数据分片的位置
  // 有限在拥有该数据分片的节点进行计算，由此来实现数据本地化。
  // 输入数据源的不同，RDD 可能具有不同的优先位置，通过 RDD 的以下方法可以返回指定 partition 的最优先位置
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val hsplit = split.asInstanceOf[HadoopPartition].inputSplit.value
    val locs = hsplit match {
      case lsplit: InputSplitWithLocationInfo =>
        HadoopRDD.convertSplitLocationInfo(lsplit.getLocationInfo)
      case _ => None
    }
    locs.getOrElse(hsplit.getLocations.filter(_ != "localhost"))
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
    // 什么都不做。Hadoop RDD不应设置检查点。
  }

  override def persist(storageLevel: StorageLevel): this.type = {
    if (storageLevel.deserialized) {
      // 缓存hadooprdds作为反序列化的对象通常会导致意外的行为，因为Hadoop的RecordReader重用
      // 同一写对象的所有记录使用地图的变换使记录的副本。
      logWarning("Caching HadoopRDDs as deserialized objects usually leads to undesired" +
        " behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
        " Use a map transformation to make copies of the records.")
    }
    super.persist(storageLevel)
  }

  def getConf: Configuration = getJobConf()
}

private[spark] object HadoopRDD extends Logging {
  /**
    * Configuration's constructor is not threadsafe (see SPARK-1097 and HADOOP-10456).
    * Therefore, we synchronize on this lock before calling new JobConf() or new Configuration().
    *
    * 配置的构造函数不是线程安全的（见spark-1097和hadoop-10456）。
    * 因此，我们使用同步锁在使用 new JobConf()或 new Configuration()之前。
    *
    */
  val CONFIGURATION_INSTANTIATION_LOCK = new Object()

  /** Update the input bytes read metric each time this number of records has been read
    * 每次读取此记录时，更新输入字节读公制。
    * */
  val RECORDS_BETWEEN_BYTES_READ_METRIC_UPDATES = 256

  /**
    * The three methods below are helpers for accessing the local map, a property of the SparkEnv of
    * the local process.
    *
    * 下面的三个方法是访问本地map的助手，对局部过程的sparkenv属性。
    *
    */
  def getCachedMetadata(key: String): Any = SparkEnv.get.hadoopJobMetadata.get(key)

  def containsCachedMetadata(key: String): Boolean = SparkEnv.get.hadoopJobMetadata.containsKey(key)

  private def putCachedMetadata(key: String, value: Any): Unit =
    SparkEnv.get.hadoopJobMetadata.put(key, value)

  /** Add Hadoop configuration specific to a single partition and attempt.
    * 将Hadoop配置添加到指定的单个分区并尝试。
    *
    * 给JobConf添加Hadoop任务相关配置
    * */
  def addLocalConfiguration(jobTrackerId: String, jobId: Int, splitId: Int, attemptId: Int,
                            conf: JobConf) {
    val jobID = new JobID(jobTrackerId, jobId)
    val taId = new TaskAttemptID(new TaskID(jobID, TaskType.MAP, splitId), attemptId)

    conf.set("mapreduce.task.id", taId.getTaskID.toString)
    conf.set("mapreduce.task.attempt.id", taId.toString)
    conf.setBoolean("mapreduce.task.ismap", true)
    conf.setInt("mapreduce.task.partition", splitId)
    conf.set("mapreduce.job.id", jobID.toString)
  }

  /**
    * Analogous to [[org.apache.spark.rdd.MapPartitionsRDD]], but passes in an InputSplit to
    * the given function rather than the index of the partition.
    *
    * 和[[org.apache.spark.rdd.MapPartitionsRDD]]相似，但在一个inputsplit照顾两个特定的功能而不是索引的分区。
    *
    */
  private[spark] class HadoopMapPartitionsWithSplitRDD[U: ClassTag, T: ClassTag](
                                                                                  prev: RDD[T],
                                                                                  f: (InputSplit, Iterator[T]) => Iterator[U],
                                                                                  preservesPartitioning: Boolean = false)
    extends RDD[U](prev) {

    override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

    override def getPartitions: Array[Partition] = firstParent[T].partitions

    override def compute(split: Partition, context: TaskContext): Iterator[U] = {
      val partition = split.asInstanceOf[HadoopPartition]
      val inputSplit = partition.inputSplit.value
      f(inputSplit, firstParent[T].iterator(split, context))
    }
  }

  private[spark] def convertSplitLocationInfo(
                                               infos: Array[SplitLocationInfo]): Option[Seq[String]] = {
    Option(infos).map(_.flatMap { loc =>
      val locationStr = loc.getLocation
      if (locationStr != "localhost") {
        if (loc.isInMemory) {
          logDebug(s"Partition $locationStr is cached by Hadoop.")
          Some(HDFSCacheTaskLocation(locationStr).toString)
        } else {
          Some(HostTaskLocation(locationStr).toString)
        }
      } else {
        None
      }
    })
  }
}
