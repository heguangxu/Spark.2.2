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

package org.apache.spark.deploy.worker

import java.io._
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.google.common.io.Files

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages.ExecutorStateChanged
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.util.logging.FileAppender

/**
  * Manages the execution of one executor process.
  * This is currently only used in standalone mode.
  *
  * 管理一个executor进程下的execution。
  * 目前只在独立模式下使用。
  */
private[deploy] class ExecutorRunner(
                                      val appId: String,
                                      val execId: Int,
                                      val appDesc: ApplicationDescription,
                                      val cores: Int,
                                      val memory: Int,
                                      val worker: RpcEndpointRef,
                                      val workerId: String,
                                      val host: String,
                                      val webUiPort: Int,
                                      val publicAddress: String,
                                      val sparkHome: File,
                                      val executorDir: File,
                                      val workerUrl: String,
                                      conf: SparkConf,
                                      val appLocalDirs: Seq[String],
                                      @volatile var state: ExecutorState.Value)
  extends Logging {

  private val fullId = appId + "/" + execId
  private var workerThread: Thread = null
  private var process: Process = null
  private var stdoutAppender: FileAppender = null
  private var stderrAppender: FileAppender = null

  // Timeout to wait for when trying to terminate an executor.
  // 在尝试终止执行程序时等待超时。
  private val EXECUTOR_TERMINATE_TIMEOUT_MS = 10 * 1000

  // NOTE: This is now redundant with the automated shut-down enforced by the Executor. It might
  // make sense to remove this in the future.
  // 注意:强制关闭Executor现在是冗余得了。在未来移除这些可能是有意义的。
  private var shutdownHook: AnyRef = null

  private[worker] def start() {
    /**
      * 新创建一个线程 下载并运行在我们的ApplicationDescription中描述的executor
      * fullId的格式为：appId+"/"+execId
      */
    workerThread = new Thread("ExecutorRunner for " + fullId) {
      // 这个run方法实际上调用的是ExecutorRunner中的fetchAndRunExecutor方法
      override def run() {
        // 新创建一个线程 下载并运行在我们的ApplicationDescription中描述的executor
        fetchAndRunExecutor()
      }
    }

    /**
      * 祁东线程
      */
    workerThread.start()
    // Shutdown hook that kills actors on shutdown.
    shutdownHook = ShutdownHookManager.addShutdownHook { () =>
      // It's possible that we arrive here before calling `fetchAndRunExecutor`, then `state` will
      // be `ExecutorState.RUNNING`. In this case, we should set `state` to `FAILED`.
      if (state == ExecutorState.RUNNING) {
        state = ExecutorState.FAILED
      }
      killProcess(Some("Worker shutting down")) }
  }

  /**
    * Kill executor process, wait for exit and notify worker to update resource status.
    *
    * Kill executor进程，等待退出，并通知worker更新资源状态。
    *
    * @param message the exception message which caused the executor's death   导致executor死亡的例外信息
    *
    * Worker异常退出：
    *   当Worker进程退出时，会发生什么呢？它将在Worker进程退出前调用KillProcess方法主动杀死CoarseGrainedExecutorBackend进程，
    * 然后收到进程返回的退出状态（KILLED）后向Worker发送ExecutorStateChange消息。由于Worker退出了，所以不会有Heartbeat消息发送给
    * Master,所以无法更新Master最后一次接收到心跳的时间戳（lastHeartbeat）.根据timeOutDeadWorkers的实现。Master会调用removeWorker
    * 方法删除长期失联的Worker的WorkerInfo信息，并将此Worker的所有Executor以LOST状态同步更新到它们服务的Driver Application，最后
    * Master还会为WorkerInfo所服务的Driver Application重新调度，分配到其他Worker上。
    */
  private def killProcess(message: Option[String]) {
    var exitCode: Option[Int] = None
    if (process != null) {
      logInfo("Killing process!")
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }
      exitCode = Utils.terminateProcess(process, EXECUTOR_TERMINATE_TIMEOUT_MS)
      if (exitCode.isEmpty) {
        logWarning("Failed to terminate process: " + process +
          ". This process will likely be orphaned.")
      }
    }
    try {
      worker.send(ExecutorStateChanged(appId, execId, state, message, exitCode))
    } catch {
      case e: IllegalStateException => logWarning(e.getMessage(), e)
    }
  }

  /** Stop this executor runner, including killing the process it launched
    * 停止executor runner，包括杀死它启动的进程
    * */
  private[worker] def kill() {
    if (workerThread != null) {
      // the workerThread will kill the child process when interrupted
      workerThread.interrupt()
      workerThread = null
      state = ExecutorState.KILLED
      try {
        ShutdownHookManager.removeShutdownHook(shutdownHook)
      } catch {
        case e: IllegalStateException => None
      }
    }
  }

  /** Replace variables such as {{EXECUTOR_ID}} and {{CORES}} in a command argument passed to us
    * 在传递给我们的命令参数中替换诸如{{EXECUTOR_ID}}和{{CORES}}之类的变量
    * */
  private[worker] def substituteVariables(argument: String): String = argument match {
    case "{{WORKER_URL}}" => workerUrl
    case "{{EXECUTOR_ID}}" => execId.toString
    case "{{HOSTNAME}}" => host
    case "{{CORES}}" => cores.toString
    case "{{APP_ID}}" => appId
    case other => other
  }

  /**
    * Download and run the executor described in our ApplicationDescription
    * 下载并运行在我们的ApplicationDescription中描述的executor
    *
    * fetchAndRunExecutor执行步骤：
    *   1.构造ProcessBuilder，进程主类是org.apache.spark.executor.CoarseGrainedExecutorBackend。
    *   2.位ProcessBuilder设置执行目录，环境变量。
    *   3.启动ProcessBuilder,生成进程。
    *   4.重定向进程的文件输出流与错误流executorDir目录下的文件stdout与stderr。
    *   5.等待获取进程的退出状态，一旦收到退出状态，则向Worker发送ExecutorStateChange消息。
    */
  private def fetchAndRunExecutor() {
    try {
      // Launch the process
      // 将Executor相关的启动信息封装成Comman对象
      val builder = CommandUtils.buildProcessBuilder(appDesc.command, new SecurityManager(conf),
        memory, sparkHome.getAbsolutePath, substituteVariables)

      val command = builder.command()
      val formattedCommand = command.asScala.mkString("\"", "\" \"", "\"")
      logInfo(s"Launch command: $formattedCommand")

      builder.directory(executorDir)
      builder.environment.put("SPARK_EXECUTOR_DIRS", appLocalDirs.mkString(File.pathSeparator))
      // In case we are running this from within the Spark Shell, avoid creating a "scala"
      // parent process for the executor command
      builder.environment.put("SPARK_LAUNCH_WITH_SCALA", "0")

      // Add webUI log urls
      val baseUrl =
        if (conf.getBoolean("spark.ui.reverseProxy", false)) {
          s"/proxy/$workerId/logPage/?appId=$appId&executorId=$execId&logType="
        } else {
          s"http://$publicAddress:$webUiPort/logPage/?appId=$appId&executorId=$execId&logType="
        }
      builder.environment.put("SPARK_LOG_URL_STDERR", s"${baseUrl}stderr")
      builder.environment.put("SPARK_LOG_URL_STDOUT", s"${baseUrl}stdout")

      // 一旦执行了builder.start()，我们利用JDK自带的工具VisualVM就会发现此时已经产生了一个新的进程。
      process = builder.start()
      val header = "Spark Executor Command: %s\n%s\n\n".format(
        formattedCommand, "=" * 40)

      /**
      重定向executor进程的彼岸准输出和输入，一般为SPARK_WORK_DIR目录下的指定APP ，即SAPRK_WORKER_DIR="/home/hadoop/spark/worker/"
        时，其目录为/home/hadoop/spark/worker/app-20150803164854-0002/0
        */
      // Redirect its stdout and stderr to files
      val stdout = new File(executorDir, "stdout")
      stdoutAppender = FileAppender(process.getInputStream, stdout, conf)

      val stderr = new File(executorDir, "stderr")
      Files.write(header, stderr, StandardCharsets.UTF_8)
      stderrAppender = FileAppender(process.getErrorStream, stderr, conf)

      // Wait for it to exit; executor may exit with code 0 (when driver instructs it to shutdown)
      // or with nonzero exit code
      val exitCode = process.waitFor() //等待excutor退出
      state = ExecutorState.EXITED
      val message = "Command exited with code " + exitCode

      /**
        * worker发出ExecutorStateChanged事件
        */
      worker.send(ExecutorStateChanged(appId, execId, state, Some(message), Some(exitCode)))
    } catch {
      case interrupted: InterruptedException =>
        logInfo("Runner thread for executor " + fullId + " interrupted")
        state = ExecutorState.KILLED
        killProcess(None)
      case e: Exception =>
        logError("Error running executor", e)
        state = ExecutorState.FAILED
        killProcess(Some(e.toString))
    }
  }
}
