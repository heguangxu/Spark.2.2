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

package org.apache.spark.deploy

import java.io.IOException
import java.security.PrivilegedExceptionAction
import java.text.DateFormat
import java.util.{Arrays, Comparator, Date, Locale}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import com.google.common.primitives.Longs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * Contains util methods to interact with Hadoop from Spark.
  * 包含与来自Spark的Hadoop交互的util方法。
 */
@DeveloperApi
class SparkHadoopUtil extends Logging {
  private val sparkConf = new SparkConf(false).loadFromSystemProperties(true)
  val conf: Configuration = newConfiguration(sparkConf)
  UserGroupInformation.setConfiguration(conf)

  /**
   * Runs the given function with a Hadoop UserGroupInformation as a thread local variable
   * (distributed to child threads), used for authenticating HDFS and YARN calls.
    *
    * 使用一个hadoop的UserGroupInformation作为一个本地局部变量运行给定的函数(分发给子线程)，
    * 用于HDFS authenticating验证和YARN调用。
   *
   * IMPORTANT NOTE: If this function is going to be called repeated in the same process
   * you need to look https://issues.apache.org/jira/browse/HDFS-3545 and possibly
   * do a FileSystem.closeAllForUGI in order to avoid leaking Filesystems
    *
    * 重要提示:如果这个函数在同一个程序中被称为重复调用,你需要看看https://issues.apache.org/jira/browse/HDFS-3545
    * 和可能做一个文件系统。为了避免泄漏文件系统，关闭了关闭系统
   */
  def runAsSparkUser(func: () => Unit) {
    val user = Utils.getCurrentUserName()
    logDebug("running as user: " + user)
    val ugi = UserGroupInformation.createRemoteUser(user)
    transferCredentials(UserGroupInformation.getCurrentUser(), ugi)
    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      def run: Unit = func()
    })
  }

  def transferCredentials(source: UserGroupInformation, dest: UserGroupInformation) {
    for (token <- source.getTokens.asScala) {
      dest.addToken(token)
    }
  }


  /**
   * Appends S3-specific, spark.hadoop.*, and spark.buffer.size configurations to a Hadoop
   * configuration.
    * 给hadoop添加一些配置
   */
  def appendS3AndSparkHadoopConfigurations(conf: SparkConf, hadoopConf: Configuration): Unit = {
    // Note: this null check is around more than just access to the "conf" object to maintain
    // the behavior of the old implementation of this code, for backwards compatibility.
    if (conf != null) {
      // Explicitly check for S3 environment variables
      val keyId = System.getenv("AWS_ACCESS_KEY_ID")
      val accessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
      if (keyId != null && accessKey != null) {
        hadoopConf.set("fs.s3.awsAccessKeyId", keyId)
        hadoopConf.set("fs.s3n.awsAccessKeyId", keyId)
        hadoopConf.set("fs.s3a.access.key", keyId)
        hadoopConf.set("fs.s3.awsSecretAccessKey", accessKey)
        hadoopConf.set("fs.s3n.awsSecretAccessKey", accessKey)
        hadoopConf.set("fs.s3a.secret.key", accessKey)

        val sessionToken = System.getenv("AWS_SESSION_TOKEN")
        if (sessionToken != null) {
          hadoopConf.set("fs.s3a.session.token", sessionToken)
        }
      }
      // Copy any "spark.hadoop.foo=bar" system properties into conf as "foo=bar"
      conf.getAll.foreach { case (key, value) =>
        if (key.startsWith("spark.hadoop.")) {
          hadoopConf.set(key.substring("spark.hadoop.".length), value)
        }
      }
      val bufferSize = conf.get("spark.buffer.size", "65536")
      hadoopConf.set("io.file.buffer.size", bufferSize)
    }
  }

  /**
   * Return an appropriate (subclass) of Configuration. Creating config can initializes some Hadoop
   * subsystems.
    * 返回一个适当的配置，创建一个config他能初始化一些hadoop配置
   */
  def newConfiguration(conf: SparkConf): Configuration = {
    val hadoopConf = new Configuration()
    appendS3AndSparkHadoopConfigurations(conf, hadoopConf)
    hadoopConf
  }

  /**
   * Add any user credentials to the job conf which are necessary for running on a secure Hadoop
   * cluster.
    *
    * 为在安全的Hadoop集群上运行所必需的job conf添加任何用户凭证。
   */
  def addCredentials(conf: JobConf) {}

  // 是否是Yarn模式
  def isYarnMode(): Boolean = { false }

  // 得到当前用户的用户凭证
  def getCurrentUserCredentials(): Credentials = { null }

  // 添加当前用户的用户凭证
  def addCurrentUserCredentials(creds: Credentials) {}

  // 添加当前用户的用户凭证的秘钥
  def addSecretKeyToUserCredentials(key: String, secret: String) {}

  // 得到当前用户的用户凭证的秘钥
  def getSecretKeyFromUserCredentials(key: String): Array[Byte] = { null }

  def loginUserFromKeytab(principalName: String, keytabFilename: String) {
    UserGroupInformation.loginUserFromKeytab(principalName, keytabFilename)
  }

  /**
   * Returns a function that can be called to find Hadoop FileSystem bytes read. If
   * getFSBytesReadOnThreadCallback is called from thread r at time t, the returned callback will
   * return the bytes read on r since t.
    *
    * 返回一个函数，该函数可以调用读取的Hadoop文件系统字节。如果从线程getFSBytesReadOnThreadCallback叫做r在时间t,
    * 返回的回调将返回字节读以来r t。
   */
  private[spark] def getFSBytesReadOnThreadCallback(): () => Long = {
    val f = () => FileSystem.getAllStatistics.asScala.map(_.getThreadStatistics.getBytesRead).sum
    val baseline = (Thread.currentThread().getId, f())

    /**
     * This function may be called in both spawned child threads and parent task thread (in
     * PythonRDD), and Hadoop FileSystem uses thread local variables to track the statistics.
     * So we need a map to track the bytes read from the child threads and parent thread,
     * summing them together to get the bytes read of this task.
      *
      * 这个函数可以在派生子线程和父任务线程(在PythonRDD中)中调用，Hadoop文件系统使用线程本地变量来跟踪统计数据。
      * 因此，我们需要一个映射来跟踪从子线程和父线程读取的字节，并将它们相加，以得到该任务的字节读取。
     */
    new Function0[Long] {
      private val bytesReadMap = new mutable.HashMap[Long, Long]()

      override def apply(): Long = {
        bytesReadMap.synchronized {
          bytesReadMap.put(Thread.currentThread().getId, f())
          bytesReadMap.map { case (k, v) =>
            v - (if (k == baseline._1) baseline._2 else 0)
          }.sum
        }
      }
    }
  }

  /**
   * Returns a function that can be called to find Hadoop FileSystem bytes written. If
   * getFSBytesWrittenOnThreadCallback is called from thread r at time t, the returned callback will
   * return the bytes written on r since t.
    *
    * 返回一个函数，该函数可以调用编写的Hadoop文件系统字节。如果从线程getFSBytesWrittenOnThreadCallback
    * 叫做r在时间t,返回的回调将返回字节写在r从t。
   *
   * @return None if the required method can't be found.
    *         如果不能找到所需的方法，则没有。
   */
  private[spark] def getFSBytesWrittenOnThreadCallback(): () => Long = {
    val threadStats = FileSystem.getAllStatistics.asScala.map(_.getThreadStatistics)
    val f = () => threadStats.map(_.getBytesWritten).sum
    val baselineBytesWritten = f()
    () => f() - baselineBytesWritten
  }

  /**
   * Get [[FileStatus]] objects for all leaf children (files) under the given base path. If the
   * given path points to a file, return a single-element collection containing [[FileStatus]] of
   * that file.
    *
    * 在给定的基本路径下，为所有叶节点(文件)获取[[FileStatus]]对象。如果给定路径指向一个文件
    * ，返回该文件的一个包含[[FileStatus]]的单个元素集合。
   */
  def listLeafStatuses(fs: FileSystem, basePath: Path): Seq[FileStatus] = {
    listLeafStatuses(fs, fs.getFileStatus(basePath))
  }

  /**
   * Get [[FileStatus]] objects for all leaf children (files) under the given base path. If the
   * given path points to a file, return a single-element collection containing [[FileStatus]] of
   * that file.
   */
  def listLeafStatuses(fs: FileSystem, baseStatus: FileStatus): Seq[FileStatus] = {
    def recurse(status: FileStatus): Seq[FileStatus] = {
      val (directories, leaves) = fs.listStatus(status.getPath).partition(_.isDirectory)
      leaves ++ directories.flatMap(f => listLeafStatuses(fs, f))
    }

    if (baseStatus.isDirectory) recurse(baseStatus) else Seq(baseStatus)
  }

  def listLeafDirStatuses(fs: FileSystem, basePath: Path): Seq[FileStatus] = {
    listLeafDirStatuses(fs, fs.getFileStatus(basePath))
  }

  def listLeafDirStatuses(fs: FileSystem, baseStatus: FileStatus): Seq[FileStatus] = {
    def recurse(status: FileStatus): Seq[FileStatus] = {
      val (directories, files) = fs.listStatus(status.getPath).partition(_.isDirectory)
      val leaves = if (directories.isEmpty) Seq(status) else Seq.empty[FileStatus]
      leaves ++ directories.flatMap(dir => listLeafDirStatuses(fs, dir))
    }

    assert(baseStatus.isDirectory)
    recurse(baseStatus)
  }

  def isGlobPath(pattern: Path): Boolean = {
    pattern.toString.exists("{}[]*?\\".toSet.contains)
  }

  def globPath(pattern: Path): Seq[Path] = {
    val fs = pattern.getFileSystem(conf)
    Option(fs.globStatus(pattern)).map { statuses =>
      statuses.map(_.getPath.makeQualified(fs.getUri, fs.getWorkingDirectory)).toSeq
    }.getOrElse(Seq.empty[Path])
  }

  def globPathIfNecessary(pattern: Path): Seq[Path] = {
    if (isGlobPath(pattern)) globPath(pattern) else Seq(pattern)
  }

  /**
   * Lists all the files in a directory with the specified prefix, and does not end with the
   * given suffix. The returned {{FileStatus}} instances are sorted by the modification times of
   * the respective files.
    *
    * 列出带有指定前缀的目录中的所有文件，并没有以给定的后缀结束。返回的{{FileStatus}实例由各自文件的修改时间排序。
   */
  def listFilesSorted(
      remoteFs: FileSystem,
      dir: Path,
      prefix: String,
      exclusionSuffix: String): Array[FileStatus] = {
    try {
      val fileStatuses = remoteFs.listStatus(dir,
        new PathFilter {
          override def accept(path: Path): Boolean = {
            val name = path.getName
            name.startsWith(prefix) && !name.endsWith(exclusionSuffix)
          }
        })
      Arrays.sort(fileStatuses, new Comparator[FileStatus] {
        override def compare(o1: FileStatus, o2: FileStatus): Int = {
          Longs.compare(o1.getModificationTime, o2.getModificationTime)
        }
      })
      fileStatuses
    } catch {
      case NonFatal(e) =>
        logWarning("Error while attempting to list files from application staging dir", e)
        Array.empty
    }
  }

  private[spark] def getSuffixForCredentialsPath(credentialsPath: Path): Int = {
    val fileName = credentialsPath.getName
    fileName.substring(
      fileName.lastIndexOf(SparkHadoopUtil.SPARK_YARN_CREDS_COUNTER_DELIM) + 1).toInt
  }


  private val HADOOP_CONF_PATTERN = "(\\$\\{hadoopconf-[^\\}\\$\\s]+\\})".r.unanchored

  /**
   * Substitute variables by looking them up in Hadoop configs. Only variables that match the
   * ${hadoopconf- .. } pattern are substituted.
   */
  def substituteHadoopVariables(text: String, hadoopConf: Configuration): String = {
    text match {
      case HADOOP_CONF_PATTERN(matched) =>
        logDebug(text + " matched " + HADOOP_CONF_PATTERN)
        val key = matched.substring(13, matched.length() - 1) // remove ${hadoopconf- .. }
        val eval = Option[String](hadoopConf.get(key))
          .map { value =>
            logDebug("Substituted " + matched + " with " + value)
            text.replace(matched, value)
          }
        if (eval.isEmpty) {
          // The variable was not found in Hadoop configs, so return text as is.
          text
        } else {
          // Continue to substitute more variables.
          substituteHadoopVariables(eval.get, hadoopConf)
        }
      case _ =>
        logDebug(text + " didn't match " + HADOOP_CONF_PATTERN)
        text
    }
  }

  /**
   * Start a thread to periodically update the current user's credentials with new credentials so
   * that access to secured service does not fail.
    *
    * 启动一个线程，使用新的凭据定期更新当前用户的凭据，以便访问安全服务不会失败。
   */
  private[spark] def startCredentialUpdater(conf: SparkConf) {}

  /**
   * Stop the thread that does the credential updates.      停止执行凭据更新的线程。
   */
  private[spark] def stopCredentialUpdater() {}

  /**
   * Return a fresh Hadoop configuration, bypassing the HDFS cache mechanism.
   * This is to prevent the DFSClient from using an old cached token to connect to the NameNode.
    *
    * 返回一个新的Hadoop配置，绕过HDFS缓存机制。这是为了防止DFSClient使用一个旧的缓存令牌来连接到NameNode。
   */
  private[spark] def getConfBypassingFSCache(
      hadoopConf: Configuration,
      scheme: String): Configuration = {
    val newConf = new Configuration(hadoopConf)
    val confKey = s"fs.${scheme}.impl.disable.cache"
    newConf.setBoolean(confKey, true)
    newConf
  }

  /**
   * Dump the credentials' tokens to string values.     将凭证“标记”转储为字符串值。
   *
   * @param credentials credentials
   * @return an iterator over the string values. If no credentials are passed in: an empty list
   */
  private[spark] def dumpTokens(credentials: Credentials): Iterable[String] = {
    if (credentials != null) {
      credentials.getAllTokens.asScala.map(tokenToString)
    } else {
      Seq()
    }
  }

  /**
   * Convert a token to a string for logging.
   * If its an abstract delegation token, attempt to unmarshall it and then
   * print more details, including timestamps in human-readable form.
    *
    * 将一个令牌转换为用于日志记录的字符串。
    * 如果它是一个抽象的授权令牌，尝试将其解包，然后打印更多的细节，包括以人可读的形式发布的时间戳。
   *
   * @param token token to convert to a string
   * @return a printable string value.
   */
  private[spark] def tokenToString(token: Token[_ <: TokenIdentifier]): String = {
    val df = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT, Locale.US)
    val buffer = new StringBuilder(128)
    buffer.append(token.toString)
    try {
      val ti = token.decodeIdentifier
      buffer.append("; ").append(ti)
      ti match {
        case dt: AbstractDelegationTokenIdentifier =>
          // include human times and the renewer, which the HDFS tokens toString omits
          buffer.append("; Renewer: ").append(dt.getRenewer)
          buffer.append("; Issued: ").append(df.format(new Date(dt.getIssueDate)))
          buffer.append("; Max Date: ").append(df.format(new Date(dt.getMaxDate)))
        case _ =>
      }
    } catch {
      case e: IOException =>
        logDebug(s"Failed to decode $token: $e", e)
    }
    buffer.toString
  }

  private[spark] def checkAccessPermission(status: FileStatus, mode: FsAction): Boolean = {
    val perm = status.getPermission
    val ugi = UserGroupInformation.getCurrentUser

    if (ugi.getShortUserName == status.getOwner) {
      if (perm.getUserAction.implies(mode)) {
        return true
      }
    } else if (ugi.getGroupNames.contains(status.getGroup)) {
      if (perm.getGroupAction.implies(mode)) {
        return true
      }
    } else if (perm.getOtherAction.implies(mode)) {
      return true
    }

    logDebug(s"Permission denied: user=${ugi.getShortUserName}, " +
      s"path=${status.getPath}:${status.getOwner}:${status.getGroup}" +
      s"${if (status.isDirectory) "d" else "-"}$perm")
    false
  }
}

object SparkHadoopUtil {

  private lazy val hadoop = new SparkHadoopUtil
  private lazy val yarn = try {
    Utils.classForName("org.apache.spark.deploy.yarn.YarnSparkHadoopUtil")
      .newInstance()
      .asInstanceOf[SparkHadoopUtil]
  } catch {
    case e: Exception => throw new SparkException("Unable to load YARN support", e)
  }

  val SPARK_YARN_CREDS_TEMP_EXTENSION = ".tmp"

  val SPARK_YARN_CREDS_COUNTER_DELIM = "-"

  /**
   * Number of records to update input metrics when reading from HadoopRDDs.
    * 从hadoop rdds读取数据时，更新输入指标的记录数。
   *
   * Each update is potentially expensive because we need to use reflection to access the
   * Hadoop FileSystem API of interest (only available in 2.5), so we should do this sparingly.
    *
    * 每个更新可能都很昂贵，因为我们需要使用反射来访问Hadoop文件系统API(仅在2.5中可用)，因此我们应该谨慎地做这件事。
   */
  private[spark] val UPDATE_INPUT_METRICS_INTERVAL_RECORDS = 1000

  def get: SparkHadoopUtil = {
    // Check each time to support changing to/from YARN
    val yarnMode = java.lang.Boolean.parseBoolean(
        System.getProperty("SPARK_YARN_MODE", System.getenv("SPARK_YARN_MODE")))
    if (yarnMode) {
      yarn
    } else {
      hadoop
    }
  }
}
