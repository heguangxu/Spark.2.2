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

package org.apache.spark.scheduler

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{JsonProtocol, Utils}

/**
 * A SparkListener that logs events to persistent storage.
  *  将事件记录到持久存储的SparkListener。
 *
 * Event logging is specified by the following configurable parameters:
 *   spark.eventLog.enabled - Whether event logging is enabled.
 *   spark.eventLog.compress - Whether to compress logged events
 *   spark.eventLog.overwrite - Whether to overwrite any existing files.
 *   spark.eventLog.dir - Path to the directory in which events are logged.
 *   spark.eventLog.buffer.kb - Buffer size to use when writing to output streams
  *
  * 事件日志由以下可配置参数指定:
  *  spark.eventLog.enabled -是否启用event日志
  *  spark.eventLog.compress - 日志是否压缩
  *  spark.eventLog.overwrite - 是否覆盖已经存在的文件
  *  spark.eventLog.dir - 日志的存储位置
  *  spark.eventLog.buffer.kb - 输出流的缓存大小（KB）
 */
private[spark] class EventLoggingListener(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends SparkListener with Logging {

  import EventLoggingListener._

  def this(appId: String, appAttemptId : Option[String], logBaseDir: URI, sparkConf: SparkConf) =
    this(appId, appAttemptId, logBaseDir, sparkConf,
      SparkHadoopUtil.get.newConfiguration(sparkConf))

  private val shouldCompress = sparkConf.getBoolean("spark.eventLog.compress", false)
  private val shouldOverwrite = sparkConf.getBoolean("spark.eventLog.overwrite", false)
  private val testing = sparkConf.getBoolean("spark.eventLog.testing", false)
  private val outputBufferSize = sparkConf.getInt("spark.eventLog.buffer.kb", 100) * 1024
  private val fileSystem = Utils.getHadoopFileSystem(logBaseDir, hadoopConf)
  private val compressionCodec =
    if (shouldCompress) {
      Some(CompressionCodec.createCodec(sparkConf))
    } else {
      None
    }
  private val compressionCodecName = compressionCodec.map { c =>
    CompressionCodec.getShortName(c.getClass.getName)
  }

  // Only defined if the file system scheme is not local
  private var hadoopDataStream: Option[FSDataOutputStream] = None

  private var writer: Option[PrintWriter] = None

  // For testing. Keep track of all JSON serialized events that have been logged.
  private[scheduler] val loggedEvents = new ArrayBuffer[JValue]

  // Visible for tests only.
  private[scheduler] val logPath = getLogPath(logBaseDir, appId, appAttemptId, compressionCodecName)

  /**
   * Creates the log file in the configured log directory.
    * 在配置的日志目录中创建日志文件。
   */
  def start() {

    // 如果给定的日志目录不是一个目录 那么就抛出异常
    if (!fileSystem.getFileStatus(new Path(logBaseDir)).isDirectory) {
      throw new IllegalArgumentException(s"Log directory $logBaseDir is not a directory.")
    }

    val workingPath = logPath + IN_PROGRESS     // workingPath:"file:/F:/002-spark/local-1513058931664.inprogress"
    val uri = new URI(workingPath)
    val path = new Path(workingPath)
    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme // defaultFs:"file"
    val isDefaultLocal = defaultFs == null || defaultFs == "file"

    if (shouldOverwrite && fileSystem.delete(path, true)) {
      logWarning(s"Event log $path already exists. Overwriting...")
    }

    /* The Hadoop LocalFileSystem (r1.0.4) has known issues with syncing (HADOOP-7844).
     * Therefore, for local files, use FileOutputStream instead. */
    val dstream =
      if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
        new FileOutputStream(uri.getPath)                             // 因为是本地所以： dstream:FileOutputStream
      } else {
        // 如果不是本地就是hadoopDataStream
        hadoopDataStream = Some(fileSystem.create(path))
        hadoopDataStream.get
      }

    try {
      val cstream = compressionCodec.map(_.compressedOutputStream(dstream)).getOrElse(dstream)
      val bstream = new BufferedOutputStream(cstream, outputBufferSize)

      // event log日志的第一行 内容如下：{"Event":"SparkListenerLogStart","Spark Version":"2.1.1"}
      EventLoggingListener.initEventLog(bstream)
      // 给文件设置权限770
      fileSystem.setPermission(path, LOG_FILE_PERMISSIONS)
      writer = Some(new PrintWriter(bstream))
      logInfo("Logging events to %s".format(logPath))
    } catch {
      case e: Exception =>
        dstream.close()
        throw e
    }
  }

  /** Log the event as JSON.
    * json格式的日志
    * */
  private def logEvent(event: SparkListenerEvent, flushLogger: Boolean = false) {
    val eventJson = JsonProtocol.sparkEventToJson(event)
    // scalastyle:off println
    writer.foreach(_.println(compact(render(eventJson))))
    // scalastyle:on println
    if (flushLogger) {
      writer.foreach(_.flush())
      hadoopDataStream.foreach(_.hflush())
    }
    if (testing) {
      loggedEvents += eventJson
    }
  }

  /** 这里只要是产生了以下几种event，那么就往event log日志中写一行 json的数据  （格式各不相同）格式如下：
    *
    * 以SparkListenerStageSubmitted的event为例
    * {"Event":"SparkListenerStageSubmitted","Stage Info":{"Stage ID":0,"Stage Attempt ID":0,"Stage Name":"main at <unknown>:0","Number of Tasks":20,"RDD Info":[
    * {"RDD ID":2,"Name":"MapPartitionsRDD","Scope":"{\"id\":\"2\",\"name\":\"map\"}","Callsite":"main at <unknown>:0","Parent IDs":[1],"Storage Level":{"Use Disk":false,"Use Memory":false,"Deserialized":false,"Replication":1},"Number of Partitions":20,"Number of Cached Partitions":0,"Memory Size":0,"Disk Size":0},
    * {"RDD ID":1,"Name":"MapPartitionsRDD","Scope":"{\"id\":\"1\",\"name\":\"map\"}","Callsite":"main at <unknown>:0","Parent IDs":[0],"Storage Level":{"Use Disk":false,"Use Memory":false,"Deserialized":false,"Replication":1},"Number of Partitions":20,"Number of Cached Partitions":0,"Memory Size":0,"Disk Size":0},
    * {"RDD ID":0,"Name":"NewHadoopRDD","Scope":"{\"id\":\"0\",\"name\":\"newAPIHadoopRDD\"}","Callsite":"main at <unknown>:0","Parent IDs":[],"Storage Level":{"Use Disk":false,"Use Memory":false,"Deserialized":false,"Replication":1},"Number of Partitions":20,"Number of Cached Partitions":0,"Memory Size":0,"Disk Size":0}],"Parent IDs":[],"Details":"scala.SparkOnHbase.main(SparkOnHbase.scala)","Accumulables":[]},"Properties":{"spark.rdd.scope.noOverride":"true","spark.rdd.scope":"{\"id\":\"10\",\"name\":\"saveAsNewAPIHadoopDataset\"}"}}

    * {"Event":"SparkListenerStageSubmitted","Stage Info":{"Stage ID":1,"Stage Attempt ID":0,"Stage Name":"main at <unknown>:0","Number of Tasks":40,"RDD Info":[
    * {"RDD ID":9,"Name":"MapPartitionsRDD","Scope":"{\"id\":\"9\",\"name\":\"map\"}","Callsite":"main at <unknown>:0","Parent IDs":[8],"Storage Level":{"Use Disk":false,"Use Memory":false,"Deserialized":false,"Replication":1},"Number of Partitions":40,"Number of Cached Partitions":0,"Memory Size":0,"Disk Size":0},
    * {"RDD ID":5,"Name":"MapPartitionsRDD","Scope":"{\"id\":\"5\",\"name\":\"map\"}","Callsite":"main at <unknown>:0","Parent IDs":[4],"Storage Level":{"Use Disk":false,"Use Memory":false,"Deserialized":false,"Replication":1},"Number of Partitions":20,"Number of Cached Partitions":0,"Memory Size":0,"Disk Size":0},
    * {"RDD ID":8,"Name":"UnionRDD","Scope":"{\"id\":\"8\",\"name\":\"union\"}","Callsite":"main at <unknown>:0","Parent IDs":[5,7],"Storage Level":{"Use Disk":false,"Use Memory":false,"Deserialized":false,"Replication":1},"Number of Partitions":40,"Number of Cached Partitions":0,"Memory Size":0,"Disk Size":0},
    * {"RDD ID":6,"Name":"MapPartitionsRDD","Scope":"{\"id\":\"6\",\"name\":\"filter\"}","Callsite":"main at <unknown>:0","Parent IDs":[3],"Storage Level":{"Use Disk":false,"Use Memory":false,"Deserialized":false,"Replication":1},"Number of Partitions":20,"Number of Cached Partitions":0,"Memory Size":0,"Disk Size":0},
    * {"RDD ID":4,"Name":"MapPartitionsRDD","Scope":"{\"id\":\"4\",\"name\":\"filter\"}","Callsite":"main at <unknown>:0","Parent IDs":[3],"Storage Level":{"Use Disk":false,"Use Memory":false,"Deserialized":false,"Replication":1},"Number of Partitions":20,"Number of Cached Partitions":0,"Memory Size":0,"Disk Size":0},
    * {"RDD ID":7,"Name":"MapPartitionsRDD","Scope":"{\"id\":\"7\",\"name\":\"map\"}","Callsite":"main at <unknown>:0","Parent IDs":[6],"Storage Level":{"Use Disk":false,"Use Memory":false,"Deserialized":false,"Replication":1},"Number of Partitions":20,"Number of Cached Partitions":0,"Memory Size":0,"Disk Size":0},
    * {"RDD ID":3,"Name":"ShuffledRDD","Scope":"{\"id\":\"3\",\"name\":\"groupByKey\"}","Callsite":"main at <unknown>:0","Parent IDs":[2],"Storage Level":{"Use Disk":false,"Use Memory":false,"Deserialized":false,"Replication":1},"Number of Partitions":20,"Number of Cached Partitions":0,"Memory Size":0,"Disk Size":0}],"Parent IDs":[0],"Details":"scala.SparkOnHbase.main(SparkOnHbase.scala)","Accumulables":[]},"Properties":{"spark.rdd.scope.noOverride":"true","spark.rdd.scope":"{\"id\":\"10\",\"name\":\"saveAsNewAPIHadoopDataset\"}"}}

    */

  // Events that do not trigger a flush  触发一个刷新的事件
  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = logEvent(event)

  override def onTaskStart(event: SparkListenerTaskStart): Unit = logEvent(event)

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit = logEvent(event)

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = logEvent(event)

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {
    logEvent(redactEvent(event))
  }

  // Events that trigger a flush
  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = logEvent(event, flushLogger = true)

  override def onJobEnd(event: SparkListenerJobEnd): Unit = logEvent(event, flushLogger = true)

  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    logEvent(event, flushLogger = true)
  }
  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onExecutorBlacklisted(event: SparkListenerExecutorBlacklisted): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onExecutorUnblacklisted(event: SparkListenerExecutorUnblacklisted): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onNodeBlacklisted(event: SparkListenerNodeBlacklisted): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onNodeUnblacklisted(event: SparkListenerNodeUnblacklisted): Unit = {
    logEvent(event, flushLogger = true)
  }

  // No-op because logging every update would be overkill
  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {}

  // No-op because logging every update would be overkill
  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = { }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    if (event.logEvent) {
      logEvent(event, flushLogger = true)
    }
  }

  /**
   * Stop logging events. The event log file will be renamed so that it loses the
   * ".inprogress" suffix.
    *
    * 停止日志事件。事件日志文件将被重命名，以使它失去“。inprogress”后缀。
   */
  def stop(): Unit = {
    // 关闭所有PrintWriter输出流
    writer.foreach(_.close())

    // 日志目录 config("spark.eventLog.dir", "F:\\002-spark") 设置的目录
    val target = new Path(logPath)

    // 如果设置的目录存在
    if (fileSystem.exists(target)) {
      // 是否覆盖重写 默认不能重写
      if (shouldOverwrite) {
        logWarning(s"Event log $target already exists. Overwriting...")
        if (!fileSystem.delete(target, true)) {
          logWarning(s"Error deleting $target")
        }
      } else {
        // 否则抛出异常
        throw new IOException("Target log file already exists (%s)".format(logPath))
      }
    }

    // 最后一步重命名 F:\002-spark\local-1513057683233.inprogress 临时文件 命名为F:\002-spark\local-1513057683233文件
    fileSystem.rename(new Path(logPath + IN_PROGRESS), target)
    // touch file to ensure modtime is current across those filesystems where rename()
    // does not set it, -and which support setTimes(); it's a no-op on most object stores
    try {
      fileSystem.setTimes(target, System.currentTimeMillis(), -1)
    } catch {
      case e: Exception => logDebug(s"failed to set time of $target", e)
    }
  }

  private[spark] def redactEvent(
      event: SparkListenerEnvironmentUpdate): SparkListenerEnvironmentUpdate = {
    // environmentDetails maps a string descriptor to a set of properties
    // Similar to:
    // "JVM Information" -> jvmInformation,
    // "Spark Properties" -> sparkProperties,
    // ...
    // where jvmInformation, sparkProperties, etc. are sequence of tuples.
    // We go through the various  of properties and redact sensitive information from them.
    val redactedProps = event.environmentDetails.map{ case (name, props) =>
      name -> Utils.redact(sparkConf, props)
    }
    SparkListenerEnvironmentUpdate(redactedProps)
  }

}

private[spark] object EventLoggingListener extends Logging {
  // Suffix applied to the names of files still being written by applications.
  // Suffix应用于仍然由应用程序编写的文件的名称。
  val IN_PROGRESS = ".inprogress"
  val DEFAULT_LOG_DIR = "/tmp/spark-events"

  // 文件系统的权限
  private val LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)

  // A cache for compression codecs to avoid creating the same codec many times
  // 一个用于压缩编解码器的缓存，以避免多次创建相同的编解码器
  private val codecMap = new mutable.HashMap[String, CompressionCodec]

  /**
   * Write metadata about an event log to the given stream.
   * The metadata is encoded in the first line of the event log as JSON.
    *
    * event log日志的第一行 内容如下：{"Event":"SparkListenerLogStart","Spark Version":"2.1.1"}
   *
   * @param logStream Raw output stream to the event log file.
   */
  def initEventLog(logStream: OutputStream): Unit = {
    val metadata = SparkListenerLogStart(SPARK_VERSION)
    val metadataJson = compact(JsonProtocol.logStartToJson(metadata)) + "\n"
    logStream.write(metadataJson.getBytes(StandardCharsets.UTF_8))
  }

  /**
   * Return a file-system-safe path to the log file for the given application.
    * 返回一个给定应用程序application的日志文件的安全路径
   *
   * Note that because we currently only create a single log file for each application,
   * we must encode all the information needed to parse this event log in the file name
   * instead of within the file itself. Otherwise, if the file is compressed, for instance,
   * we won't know which codec to use to decompress the metadata needed to open the file in
   * the first place.
    *
    * 注意，因为我们目前只为每个应用程序创建一个日志文件，所以我们必须编码所有需要的信息，
    * 以解析这个事件日志的文件名而不是文件本身。否则，如果文件被压缩，例如，我们将不知道要
    * 使用哪个编解码器来解压缩需要打开文件的元数据。
   *
   * The log file name will identify the compression codec used for the contents, if any.
   * For example, app_123 for an uncompressed log, app_123.lzf for an LZF-compressed log.
    *
    * 日志文件名称将标识用于内容的压缩编解码器(如果有的话)。例如，app_123用于未压缩的日志 app_123.lzf app_123.lzf的lzf压缩日志。
   *
   * @param logBaseDir Directory where the log file will be written.
   * @param appId A unique app ID.
   * @param appAttemptId A unique attempt id of appId. May be the empty string.
   * @param compressionCodecName Name to identify the codec used to compress the contents
   *                             of the log, or None if compression is not enabled.
   * @return A path which consists of file-system-safe characters.
   */
  def getLogPath(
      logBaseDir: URI,
      appId: String,
      appAttemptId: Option[String],
      compressionCodecName: Option[String] = None): String = {
    val base = logBaseDir.toString.stripSuffix("/") + "/" + sanitize(appId)
    val codec = compressionCodecName.map("." + _).getOrElse("")
    if (appAttemptId.isDefined) {
      base + "_" + sanitize(appAttemptId.get) + codec
    } else {
      base + codec
    }
  }

  private def sanitize(str: String): String = {
    str.replaceAll("[ :/]", "-").replaceAll("[.${}'\"]", "_").toLowerCase(Locale.ROOT)
  }

  /**
   * Opens an event log file and returns an input stream that contains the event data.
    * 打开事件日志文件并返回包含事件数据的输入流。
   *
   * @return input stream that holds one JSON record per line.
   */
  def openEventLog(log: Path, fs: FileSystem): InputStream = {
    val in = new BufferedInputStream(fs.open(log))

    // Compression codec is encoded as an extension, e.g. app_123.lzf
    // Since we sanitize the app ID to not include periods, it is safe to split on it
    val logName = log.getName.stripSuffix(IN_PROGRESS)
    val codecName: Option[String] = logName.split("\\.").tail.lastOption
    val codec = codecName.map { c =>
      codecMap.getOrElseUpdate(c, CompressionCodec.createCodec(new SparkConf, c))
    }

    try {
      codec.map(_.compressedInputStream(in)).getOrElse(in)
    } catch {
      case e: Exception =>
        in.close()
        throw e
    }
  }

}
