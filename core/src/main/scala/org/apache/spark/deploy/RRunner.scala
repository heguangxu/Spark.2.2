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

import java.io._
import java.util.concurrent.{Semaphore, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkException, SparkUserAppException}
import org.apache.spark.api.r.{RBackend, RUtils, SparkRDefaults}
import org.apache.spark.util.RedirectThread

/**
 * Main class used to launch SparkR applications using spark-submit. It executes R as a
 * subprocess and then has it connect back to the JVM to access system properties etc.
  *
  * 主类，用于使用spark - submit启动SparkR应用程序。它将R作为子流程执行，然后将其连接回JVM，以访问系统属性等。
 */
object RRunner {
  def main(args: Array[String]): Unit = {
    // Python不理解路径中的URI方案。在将python文件添加到PYTHONPATH之前，我们需要从URI中提取路径。
    // 这是安全的，因为我们目前只支持本地python文件
    val rFile = PythonRunner.formatPath(args(0))

    val otherArgs = args.slice(1, args.length)

    // Time to wait for SparkR backend to initialize in seconds
    // 等待SparkR backend 初始化的时间
    val backendTimeout = sys.env.getOrElse("SPARKR_BACKEND_TIMEOUT", "120").toInt
    val rCommand = {
      // "spark.sparkr.r.command" is deprecated and replaced by "spark.r.command",
      // but kept here for backward compatibility.
      // "spark.sparkr.r.command" 命令过时了，现在被"spark.r.command",命令取代了，这里是为了兼容以前版本
      var cmd = sys.props.getOrElse("spark.sparkr.r.command", "Rscript")
      cmd = sys.props.getOrElse("spark.r.command", cmd)
      if (sys.props.getOrElse("spark.submit.deployMode", "client") == "client") {
        cmd = sys.props.getOrElse("spark.r.driver.command", cmd)
      }
      cmd
    }

    //  Connection timeout set by R process on its connection to RBackend in seconds.
    // 由R进程在秒内连接到r后台的连接超时设置。
    val backendConnectionTimeout = sys.props.getOrElse(
      "spark.r.backendConnectionTimeout", SparkRDefaults.DEFAULT_CONNECTION_TIMEOUT.toString)

    // Check if the file path exists.           检查文件的路径是否存在
    // If not, change directory to current working directory for YARN cluster mode
    // 如果没有，将目录更改为当前的YARN集群模式下的工作目录
    val rF = new File(rFile)
    val rFileNormalized = if (!rF.exists()) {
      new Path(rFile).getName
    } else {
      rFile
    }

    // Launch a SparkR backend server for the R process to connect to; this will let it see our
    // Java system properties etc.
    // 启动一个用于R进程的SparkR后端服务器;这会让它看到我们的Java系统属性等等。
    val sparkRBackend = new RBackend()
    @volatile var sparkRBackendPort = 0
    val initialized = new Semaphore(0)
    val sparkRBackendThread = new Thread("SparkR backend") {
      override def run() {
        sparkRBackendPort = sparkRBackend.init()
        initialized.release()
        sparkRBackend.run()
      }
    }

    sparkRBackendThread.start()
    // Wait for RBackend initialization to finish
    // 等待r后台初始化完成
    if (initialized.tryAcquire(backendTimeout, TimeUnit.SECONDS)) {
      // Launch R  启动R程序
      val returnCode = try {
        val builder = new ProcessBuilder((Seq(rCommand, rFileNormalized) ++ otherArgs).asJava)
        val env = builder.environment()
        env.put("EXISTING_SPARKR_BACKEND_PORT", sparkRBackendPort.toString)
        env.put("SPARKR_BACKEND_CONNECTION_TIMEOUT", backendConnectionTimeout)
        val rPackageDir = RUtils.sparkRPackagePath(isDriver = true)
        // Put the R package directories into an env variable of comma-separated paths
        env.put("SPARKR_PACKAGE_DIR", rPackageDir.mkString(","))
        env.put("R_PROFILE_USER",
          Seq(rPackageDir(0), "SparkR", "profile", "general.R").mkString(File.separator))
        builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
        val process = builder.start()

        new RedirectThread(process.getInputStream, System.out, "redirect R output").start()

        process.waitFor()
      } finally {
        sparkRBackend.close()
      }
      if (returnCode != 0) {
        throw new SparkUserAppException(returnCode)
      }
    } else {
      val errorMessage = s"SparkR backend did not initialize in $backendTimeout seconds"
      // scalastyle:off println
      System.err.println(errorMessage)
      // scalastyle:on println
      throw new SparkException(errorMessage)
    }
  }
}
