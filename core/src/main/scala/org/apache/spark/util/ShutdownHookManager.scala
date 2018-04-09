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

package org.apache.spark.util

import java.io.File
import java.util.PriorityQueue

import scala.util.Try

import org.apache.hadoop.fs.FileSystem

import org.apache.spark.internal.Logging

/**
 * Various utility methods used by Spark.
  *
  * Spark使用的各种实用方法。
 */
private[spark] object ShutdownHookManager extends Logging {
  val DEFAULT_SHUTDOWN_PRIORITY = 100   // 默认的ShutdownHookManager优先级

  /**
   * The shutdown priority of the SparkContext instance. This is lower than the default
   * priority, so that by default hooks are run before the context is shut down.
    *
    * SparkContext实例的shutdown优先级。这比默认的优先级要低，因此在默认情况下，在关闭上下文之前运行默认的hooks。
   */
  val SPARK_CONTEXT_SHUTDOWN_PRIORITY = 50

  /**
   * The shutdown priority of temp directory must be lower than the SparkContext shutdown
   * priority. Otherwise cleaning the temp directories while Spark jobs are running can
   * throw undesirable errors at the time of shutdown.
    *
    * temp目录的关闭优先级必须低于SparkContext关闭的优先级。否则，当Spark作业正在运行时，清理temp目录将会在关闭时抛出错误的错误。
   */
  val TEMP_DIR_SHUTDOWN_PRIORITY = 25

  // 懒加载
  private lazy val shutdownHooks = {
    val manager = new SparkShutdownHookManager()
    // 运行所有的hook,并且添加进去
    manager.install()
    manager
  }

  private val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()

  // Add a shutdown hook to delete the temp dirs when the JVM exits
  // 当JVM退出时，添加一个关闭钩子来删除temp dirs
  logDebug("Adding shutdown hook") // force eager creation of logger


  addShutdownHook(TEMP_DIR_SHUTDOWN_PRIORITY) { () =>
    logInfo("Shutdown hook called")
    // we need to materialize the paths to delete because deleteRecursively removes items from
    // shutdownDeletePaths as we are traversing through it.
    //
    // 我们需要实现删除路径，因为我们在遍历过程中从shutdowndeletepath中删除项目。
    shutdownDeletePaths.toArray.foreach { dirPath =>
      try {
        logInfo("Deleting directory " + dirPath)
        // 递归地删除文件或目录及其内容。 如果删除失败，则抛出异常。
        Utils.deleteRecursively(new File(dirPath))
      } catch {
        case e: Exception => logError(s"Exception while deleting Spark temp dir: $dirPath", e)
      }
    }
  }

  // Register the path to be deleted via shutdown hook
  // 通过关闭hook注册要删除的路径
  def registerShutdownDeleteDir(file: File) {
    // 得到文件的绝对路径
    val absolutePath = file.getAbsolutePath()

    // 假如到要删除文件路径的集合
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths += absolutePath
    }
  }

  // Remove the path to be deleted via shutdown hook 删除通过关闭hook删除的路径
  def removeShutdownDeleteDir(file: File) {
    val absolutePath = file.getAbsolutePath()
    // 删除文件
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.remove(absolutePath)
    }
  }

  // Is the path already registered to be deleted via a shutdown hook ?
  // 已经注册的路径是否通过关闭hook被删除?
  // 判断shutdownDeletePaths中是否包含给定的路径，如果包含返回true,否则返回false
  def hasShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.contains(absolutePath)
    }
  }

  // Note: if file is child of some registered path, while not equal to it, then return true;
  // else false. This is to ensure that two shutdown hooks do not try to delete each others
  // paths - resulting in IOException and incomplete cleanup.
  // 注意:如果文件是某个已注册路径的子元素，而不等于它，则返回true;其他错误的。
  // 这是为了确保两个关闭hooks不会试图删除彼此的路径——导致IOException和不完整的清理。
  def hasRootAsShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    val retval = shutdownDeletePaths.synchronized {
      shutdownDeletePaths.exists { path =>
        !absolutePath.equals(path) && absolutePath.startsWith(path)
      }
    }
    if (retval) {
      logInfo("path = " + file + ", already present as root for deletion.")
    }
    retval
  }

  /**
   * Detect whether this thread might be executing a shutdown hook. Will always return true if
   * the current thread is a running a shutdown hook but may spuriously return true otherwise (e.g.
   * if System.exit was just called by a concurrent thread).
    *
    * 检测此线程是否正在执行关闭hook。如果当前线程是一个正在运行的关闭hook，但可能会错误地返回true(例如，如果系统)，
    * 则将始终返回true。退出是由一个并发线程调用的。
   *
   * Currently, this detects whether the JVM is shutting down by Runtime#addShutdownHook throwing
   * an IllegalStateException.
    *
    * 当前，这检测到JVM是否在Runtime#addShutdownHook，抛出了一个IllegalStateException异常。
   */
  def inShutdown(): Boolean = {
    try {
      val hook = new Thread {
        override def run() {}
      }

      // 这一点先加入后移除 是什么意思啊？
      // scalastyle:off runtimeaddshutdownhook
      Runtime.getRuntime.addShutdownHook(hook)
      // scalastyle:on runtimeaddshutdownhook
      Runtime.getRuntime.removeShutdownHook(hook)
    } catch {
      case ise: IllegalStateException => return true
    }
    false
  }

  /**
   * Adds a shutdown hook with default priority. 添加默认优先级的 shutdown hook。
   *
   * @param hook The code to run during shutdown.
   * @return A handle that can be used to unregister the shutdown hook.
   */
  def addShutdownHook(hook: () => Unit): AnyRef = {
    addShutdownHook(DEFAULT_SHUTDOWN_PRIORITY)(hook)
  }

  /**
   * Adds a shutdown hook with the given priority. Hooks with lower priority values run
   * first.
    *
    * 根据一个指定的优先级添加一个shutdown hook，优先级低的Hooks优先被运行
   *
   * @param hook The code to run during shutdown.
   * @return A handle that can be used to unregister the shutdown hook.
   */
  def addShutdownHook(priority: Int)(hook: () => Unit): AnyRef = {
    shutdownHooks.add(priority, hook)
  }

  /**
   * Remove a previously installed shutdown hook. 删除先前安装的shutdown hook
   *
   * @param ref A handle returned by `addShutdownHook`.
   * @return Whether the hook was removed.
   */
  def removeShutdownHook(ref: AnyRef): Boolean = {
    shutdownHooks.remove(ref)
  }

}

private [util] class SparkShutdownHookManager {

  // 权限队列
  private val hooks = new PriorityQueue[SparkShutdownHook]()
  @volatile private var shuttingDown = false

  /**
   * Install a hook to run at shutdown and run all registered hooks in order.
    * 安装一个hook来运行关闭，并运行所有已注册的hooks。
   */
  def install(): Unit = {
    val hookTask = new Runnable() {
      override def run(): Unit = runAll()
    }
    org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(
      hookTask, FileSystem.SHUTDOWN_HOOK_PRIORITY + 30)
  }

  def runAll(): Unit = {
    shuttingDown = true
    var nextHook: SparkShutdownHook = null
    while ({ nextHook = hooks.synchronized { hooks.poll() }; nextHook != null }) {
      Try(Utils.logUncaughtExceptions(nextHook.run()))
    }
  }

  def add(priority: Int, hook: () => Unit): AnyRef = {
    hooks.synchronized {
      if (shuttingDown) {
        throw new IllegalStateException("Shutdown hooks cannot be modified during shutdown.")
      }
      val hookRef = new SparkShutdownHook(priority, hook)
      hooks.add(hookRef)
      hookRef
    }
  }

  def remove(ref: AnyRef): Boolean = {
    hooks.synchronized { hooks.remove(ref) }
  }

}

private class SparkShutdownHook(private val priority: Int, hook: () => Unit)
  extends Comparable[SparkShutdownHook] {

  override def compareTo(other: SparkShutdownHook): Int = {
    other.priority - priority
  }

  def run(): Unit = hook()

}
