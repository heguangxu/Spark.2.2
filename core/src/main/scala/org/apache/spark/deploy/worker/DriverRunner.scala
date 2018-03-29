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
import java.net.URI
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.google.common.io.Files

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{DriverDescription, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages.DriverStateChanged
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.{Clock, ShutdownHookManager, SystemClock, Utils}

/**
  * Manages the execution of one driver, including automatically restarting the driver on failure.
  * This is currently only used in standalone cluster deploy mode.
  *
  * 管理一个驱动程序Driver的执行execution，包括在driver运行失败的时候，自动重新启动驱动程序。
  * 目前只在独立集群部署模式中使用。
  */
private[deploy] class DriverRunner(
                                    conf: SparkConf,
                                    val driverId: String,
                                    val workDir: File,
                                    val sparkHome: File,
                                    val driverDesc: DriverDescription,
                                    val worker: RpcEndpointRef,
                                    val workerUrl: String,
                                    val securityManager: SecurityManager)
  extends Logging {

  @volatile private var process: Option[Process] = None
  @volatile private var killed = false

  // Populated once finished      填充一次完成
  @volatile private[worker] var finalState: Option[DriverState] = None
  @volatile private[worker] var finalException: Option[Exception] = None

  // Timeout to wait for when trying to terminate a driver. 在尝试终止驱动程序时等待超时。
  private val DRIVER_TERMINATE_TIMEOUT_MS =
    conf.getTimeAsMs("spark.worker.driverTerminateTimeout", "10s")

  // Decoupled for testing
  def setClock(_clock: Clock): Unit = {
    clock = _clock
  }

  def setSleeper(_sleeper: Sleeper): Unit = {
    sleeper = _sleeper
  }

  private var clock: Clock = new SystemClock()
  private var sleeper = new Sleeper {
    def sleep(seconds: Int): Unit = (0 until seconds).takeWhile { _ =>
      Thread.sleep(1000)
      !killed
    }
  }

  /** Starts a thread to run and manage the driver.
    * 启动一个线程来运行和管理driver。
    * */
  private[worker] def start() = {
    new Thread("DriverRunner for " + driverId) {
      override def run() {
        var shutdownHook: AnyRef = null
        try {
          // 这里不知道是干嘛？
          shutdownHook = ShutdownHookManager.addShutdownHook { () =>
            logInfo(s"Worker shutting down, killing driver $driverId")
            kill()
          }


          /**这里真正开始运行Driver*/
          // prepare driver jars and run driver 准备驱动程序jar和运行驱动程序。
          val exitCode = prepareAndRunDriver()

          // set final state depending on if forcibly killed and process exit code
          finalState = if (exitCode == 0) {
            Some(DriverState.FINISHED)
          } else if (killed) {
            Some(DriverState.KILLED)
          } else {
            Some(DriverState.FAILED)
          }
        } catch {
          case e: Exception =>
            kill()
            finalState = Some(DriverState.ERROR)
            finalException = Some(e)
        } finally {
          if (shutdownHook != null) {
            ShutdownHookManager.removeShutdownHook(shutdownHook)
          }
        }

        //driverRunner向该driver所属的worker发送driverStateChanged消息，worker会接收到该消息做处理
        // worke.receive()方法处理
        // notify worker of final driver state, possible exception
        worker.send(DriverStateChanged(driverId, finalState.get, finalException))
      }
    }.start()
  }

  /** Terminate this driver (or prevent it from ever starting if not yet started)
    *  终止这个驱动程序(或者如果还没有启动，就阻止它启动)
    * */
  private[worker] def kill(): Unit = {
    logInfo("Killing driver process!")
    killed = true
    synchronized {
      process.foreach { p =>
        val exitCode = Utils.terminateProcess(p, DRIVER_TERMINATE_TIMEOUT_MS)
        if (exitCode.isEmpty) {
          logWarning("Failed to terminate driver process: " + p +
            ". This process will likely be orphaned.")
        }
      }
    }
  }

  /**
    * Creates the working directory for this driver.
    * Will throw an exception if there are errors preparing the directory.
    *
    * 为该驱动程序创建工作目录。创建Driver的工作目录
    * 如果在准备目录时出现错误，将抛出异常。
    */
  private def createWorkingDirectory(): File = {
    // 这一点工作目录说什么呢？workDir
    val driverDir = new File(workDir, driverId)
    if (!driverDir.exists() && !driverDir.mkdirs()) {
      throw new IOException("Failed to create directory " + driverDir)
    }
    // 目录的名称就是driverId
    driverDir
  }

  /**
    * Download the user jar into the supplied directory and return its local path.
    * Will throw an exception if there are errors downloading the jar.
    *
    * 将用户jar下载到所提供的目录并返回其本地路径。
    * 如果有错误下载jar，将抛出异常。
    */
  private def downloadUserJar(driverDir: File): String = {
    val jarFileName = new URI(driverDesc.jarUrl).getPath.split("/").last
    val localJarFile = new File(driverDir, jarFileName)
    if (!localJarFile.exists()) { // May already exist if running multiple workers on one node
      logInfo(s"Copying user jar ${driverDesc.jarUrl} to $localJarFile")
      // 从远程节点下载用户需要的jar包
      Utils.fetchFile(
        driverDesc.jarUrl,
        driverDir,
        conf,
        securityManager,
        SparkHadoopUtil.get.newConfiguration(conf),
        System.currentTimeMillis(),
        useCache = false)

      // 验证是否下载成功
      if (!localJarFile.exists()) { // Verify copy succeeded
        throw new IOException(
          s"Can not find expected jar $jarFileName which should have been loaded in $driverDir")
      }
    }
    localJarFile.getAbsolutePath
  }

  private[worker] def prepareAndRunDriver(): Int = {
    // 创建Driver的工作目录
    val driverDir = createWorkingDirectory()
    // 将用户jar下载到所提供的目录并返回其本地路径。下载程序相关的依赖文件，在DriverDescription中会有依赖
    // 文件的url,spark这里直接使用hadoop的一个工具FileUtil，利用其copy方法将依赖文件复制到本地。
    val localJarFilename = downloadUserJar(driverDir)

    def substituteVariables(argument: String): String = argument match {
      case "{{WORKER_URL}}" => workerUrl
      case "{{USER_JAR}}" => localJarFilename
      case other => other
    }

    // TODO: If we add ability to submit multiple jars they should also be added here
    // 如果我们增加了提交多个jar的能力，它们也应该在这里添加。
    // 根据给定的参数构建一个ProcessBuilder。
    val builder = CommandUtils.buildProcessBuilder(driverDesc.command, securityManager,
      driverDesc.mem, sparkHome.getAbsolutePath, substituteVariables)

    /** drive的前期准备已经完成，开始运行Driver */
    runDriver(builder, driverDir, driverDesc.supervise)
  }

  /**
    * 开始运行Driver
    * @param builder
    * @param baseDir
    * @param supervise
    * @return
    */
  private def runDriver(builder: ProcessBuilder, baseDir: File, supervise: Boolean): Int = {
    // 创建目录，这个目录是什么？
    builder.directory(baseDir)

    // 主要把标准输出和标准错误输出的文件中，这个是启动一个线程
    def initialize(process: Process): Unit = {
      // Redirect stdout and stderr to files 将stdout和stderr重定向到文件。
      val stdout = new File(baseDir, "stdout")
      CommandUtils.redirectStream(process.getInputStream, stdout)

      val stderr = new File(baseDir, "stderr")
      val formattedCommand = builder.command.asScala.mkString("\"", "\" \"", "\"")
      val header = "Launch Command: %s\n%s\n\n".format(formattedCommand, "=" * 40)
      Files.append(header, stderr, StandardCharsets.UTF_8)
      CommandUtils.redirectStream(process.getErrorStream, stderr)
    }

    // 先执行initialize方法
    runCommandWithRetry(ProcessBuilderLike(builder), initialize, supervise)
  }

  /**
    * 运行命令
    * @param command
    * @param initialize
    * @param supervise
    * @return
    */
  private[worker] def runCommandWithRetry(command: ProcessBuilderLike, initialize: Process => Unit, supervise: Boolean): Int = {
    var exitCode = -1
    // Time to wait between submission retries. 在提交重试之间等待时间。
    var waitSeconds = 1
    // A run of this many seconds resets the exponential back-off.
    // 这几秒钟的运行重新设置了指数回撤。
    val successfulRunDuration = 5
    var keepTrying = !killed

    while (keepTrying) {
      logInfo("Launch Command: " + command.command.mkString("\"", "\" \"", "\""))

      synchronized {
        if (killed) { return exitCode }
        /** 最终开始运行命令 */
        process = Some(command.start())
        initialize(process.get)
      }

      val processStart = clock.getTimeMillis()
      //启动process，最终启动Drive
      /** 执行了linux的命令
        * java -cp  .... org.apache.spark.deploy.worker.DriverWrapper $WorkerUrl  /path/to/examples.jar
        * ora.apache.spark.examples.SparkPI 1000
        *
        * 因此 我们看看DriverWrapper的main方法
        * */
      exitCode = process.get.waitFor()

      //重试机制：如果driver被监控 并且 退出代码不等于0 并且 没有被kill
      // check if attempting another run 检查是否尝试再次运行。
      keepTrying = supervise && exitCode != 0 && !killed
      if (keepTrying) {
        if (clock.getTimeMillis() - processStart > successfulRunDuration * 1000) {
          waitSeconds = 1
        }
        logInfo(s"Command exited with status $exitCode, re-launching after $waitSeconds s.")
        //睡眠一秒钟后重试
        sleeper.sleep(waitSeconds)
        //等待时间乘以2
        waitSeconds = waitSeconds * 2 // exponential back-off
      }
    }

    exitCode
  }
}

private[deploy] trait Sleeper {
  def sleep(seconds: Int): Unit
}

// Needed because ProcessBuilder is a final class and cannot be mocked
private[deploy] trait ProcessBuilderLike {
  def start(): Process
  def command: Seq[String]
}

private[deploy] object ProcessBuilderLike {
  def apply(processBuilder: ProcessBuilder): ProcessBuilderLike = new ProcessBuilderLike {
    override def start(): Process = processBuilder.start()
    override def command: Seq[String] = processBuilder.command().asScala
  }
}
