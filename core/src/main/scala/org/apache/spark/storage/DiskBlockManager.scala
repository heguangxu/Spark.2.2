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

package org.apache.spark.storage

import java.io.{File, IOException}
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. One block is mapped to one file with a name given by its BlockId.
 *
  *  创建并维护逻辑块和物理磁盘之间的逻辑映射的位置。一个块被映射到一个文件，该文件由它的BlockId提供。
  *
 * Block files are hashed among the directories listed in spark.local.dir (or in
 * SPARK_LOCAL_DIRS, if it's set).
  *
  * 块文件被散列在spark . local中列出的目录中。dir(或在SPARK_LOCAL_DIRS中，如果设置)。
  *
  *
  * 磁盘管理器DiskblockManager
  *   DiskBlockManager的构造过程
  *     Blockmanager初始化的时候汇创建DiskblockManager，DiskBlockManager的构造步骤如下：
  *       1.调用createLocalDir方法创建本地文件目录，然后创建二维数组subDirs，用来缓存一级目录localDirs及二级目录，其中二级目录
  *         的数量根据配置spark.diskStore.subDirectories获取，默认为64.以笔者本地为例，创建的目录为：
  *         C;\Users\{username}\AppData\Local|Temp\spark-o16jbf-234345-3254-12sdxf23-3243tgsg\spark-34fdsgvfdx-sdrfes-sdf33-szdfew3-sdfs34435232
  *         其中spark-o16jbf-234345-3254-12sdxf23-3243tgsg是一级目录，
  *         spark-34fdsgvfdx-sdrfes-sdf33-szdfew3-sdfs34435232是二级目录
  *
  *        注意：createLocalDirs方法具体创建目录的过程实际调用了Utils的getOrCreateLocalRootDirs和createDirectory方法
  *
  *       2.添加运行时环境结束的钩子，用于在进程关闭时创建线程，通过调用DiskBlockManager的stop方法，清除一些临时目录。
  *
  *   DiskBlockManager为什么要创建二级目录？这是因为二级目录用于对文件进行三列存储，三列存储可以使所有文件都随机存放，写入或者删除
  *   文件更方便，存取速度快，节省空间。
 */
private[spark] class DiskBlockManager(conf: SparkConf, deleteFilesOnStop: Boolean) extends Logging {

  private[spark] val subDirsPerLocalDir = conf.getInt("spark.diskStore.subDirectories", 64)

  /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level.
   *
   * 为spark.local.dir(Yarn模式中被LOCAL_DIRS取代了)中提到的每条路径创建一个本地目录;然后，在这个目录中，
   * 创建多个子目录，我们将把这些子目录散列到其中，以避免在顶层有真正大的inode。
   *
   * */
  private[spark] val localDirs: Array[File] = createLocalDirs(conf)

  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }
  // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  // of subDirs(i) is protected by the lock of subDirs(i)
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  // 添加运行时环境结束时的钩子，用于在进程关闭时创建线程，通过调用DiskBlockManager的Stop方法，清除一些临时目录。
  private val shutdownHook = addShutdownHook()

  /** Looks up a file by hashing it into one of our local subdirectories.
    * 通过将文件散列到本地子目录中查找文件。
    *
    * 这种方法应该与org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getFile()保持同步
    *
    * 很多代码都是用DiskBlockManager的getFile，获取磁盘上的文件，通过getFile的分析，能够掌握Spark磁盘三列文件存储的实现机制
    *   1.根据文件名计算哈希值
    *   2.根据哈希值与本地文件爱你一级目录的总数求余数，记为dirId
    *   3.根据哈希值与本地文件一级目录的总数求商，此商与二级目录的数目再求余数，记为subDirId.
    *   4.如果dirId/subDirId目录存在，则获取dirId/subDirId目录下的文件，否则新建dirId/subDirId目录。
    * */
  // This method should be kept in sync with
  // org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getFile().
  def getFile(filename: String): File = {
    // Figure out which local directory it hashes to, and which subdirectory in that
    // 找出其散列在哪个本地目录，以及该目录下的哪个子目录。
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % localDirs.length
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir

    // Create the subdirectory if it doesn't already exist
    val subDir = subDirs(dirId).synchronized {
      val old = subDirs(dirId)(subDirId)
      if (old != null) {
        old
      } else {
        val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
        if (!newDir.exists() && !newDir.mkdir()) {
          throw new IOException(s"Failed to create local dir in $newDir.")
        }
        subDirs(dirId)(subDirId) = newDir
        newDir
      }
    }

    new File(subDir, filename)
  }

  def getFile(blockId: BlockId): File = getFile(blockId.name)

  /** Check if disk block manager has a block. */
  def containsBlock(blockId: BlockId): Boolean = {
    getFile(blockId.name).exists()
  }

  /** List all the files currently stored on disk by the disk manager. */
  def getAllFiles(): Seq[File] = {
    // Get all the files inside the array of array of directories
    subDirs.flatMap { dir =>
      dir.synchronized {
        // Copy the content of dir because it may be modified in other threads
        dir.clone()
      }
    }.filter(_ != null).flatMap { dir =>
      val files = dir.listFiles()
      if (files != null) files else Seq.empty
    }
  }

  /** List all the blocks currently stored on disk by the disk manager. */
  def getAllBlocks(): Seq[BlockId] = {
    getAllFiles().map(f => BlockId(f.getName))
  }

  /** Produces a unique block id and File suitable for storing local intermediate results. */
  def createTempLocalBlock(): (TempLocalBlockId, File) = {
    var blockId = new TempLocalBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempLocalBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /** Produces a unique block id and File suitable for storing shuffled intermediate results.
    * 生成一个唯一的块id和文件，适合存储被打乱的中间结果。
    *
    * 当ShuffleMapTask运行结束需要把中间结果临时保存，此时就调用createTempShuffleBlock方法创建临时的Block,并且返回
    * TempShuffleBlockId与文件的对偶。
    *
    * TempShuffleBlockId的生成规则：“temp_shuffle_”后面加上UUID字符串。
    * */
  def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
    var blockId = new TempShuffleBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempShuffleBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /**
   * Create local directories for storing block data. These directories are
   * located inside configured local directories and won't
   * be deleted on JVM exit when using the external shuffle service.
    *
    * 创建用于存储块数据的本地目录。这些目录位于配置的本地目录中，在使用外部洗牌服务时不会在JVM出口上删除。
   */
  private def createLocalDirs(conf: SparkConf): Array[File] = {
    Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      try {
        val localDir = Utils.createDirectory(rootDir, "blockmgr")
        // 19=> 17/12/05 11:56:50 INFO DiskBlockManager: Created local directory at C:\Users\hzjs\AppData\Local\Temp\blockmgr-c33e54ee-7223-4132-a118-2691d0ab8aad
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
          None
      }
    }
  }


  //=========================初始化分隔线的两个阶段==========================================================

  private def addShutdownHook(): AnyRef = {
    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
      logInfo("Shutdown hook called")
      DiskBlockManager.this.doStop()
    }
  }

  /** Cleanup local dirs and stop shuffle sender. */
  private[spark] def stop() {
    // Remove the shutdown hook.  It causes memory leaks if we leave it around.
    try {
      ShutdownHookManager.removeShutdownHook(shutdownHook)
    } catch {
      case e: Exception =>
        logError(s"Exception while removing shutdown hook.", e)
    }
    doStop()
  }

  private def doStop(): Unit = {
    if (deleteFilesOnStop) {
      localDirs.foreach { localDir =>
        // 如果本地目录是目录，并且存在
        if (localDir.isDirectory() && localDir.exists()) {
          try {
            if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(localDir)) {
              // 递归地删除文件或目录及其内容。
              Utils.deleteRecursively(localDir)
            }
          } catch {
            case e: Exception =>
              logError(s"Exception while deleting local spark dir: $localDir", e)
          }
        }
      }
    }
  }
}
