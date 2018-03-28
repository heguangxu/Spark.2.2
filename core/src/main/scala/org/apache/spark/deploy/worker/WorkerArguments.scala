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

import java.lang.management.ManagementFactory

import scala.annotation.tailrec

import org.apache.spark.util.{IntParam, MemoryParam, Utils}
import org.apache.spark.SparkConf

/**
  * Command-line parser for the worker.
  * worker的命令行解析器。
  */
private[worker] class WorkerArguments(args: Array[String], conf: SparkConf) {
  var host = Utils.localHostName()
  var port = 0
  var webUiPort = 8081
  var cores = inferDefaultCores()
  var memory = inferDefaultMemory()  // memory默认为操作系统最大内存减去1GB，这1GB是留给操作系统使用的
  var masters: Array[String] = null
  var workDir: String = null
  var propertiesFile: String = null

  // Check for settings in environment variables          检查环境变量中的设置
  if (System.getenv("SPARK_WORKER_PORT") != null) {
    port = System.getenv("SPARK_WORKER_PORT").toInt
  }
  if (System.getenv("SPARK_WORKER_CORES") != null) {
    cores = System.getenv("SPARK_WORKER_CORES").toInt
  }
  if (conf.getenv("SPARK_WORKER_MEMORY") != null) {
    memory = Utils.memoryStringToMb(conf.getenv("SPARK_WORKER_MEMORY"))
  }
  if (System.getenv("SPARK_WORKER_WEBUI_PORT") != null) {
    webUiPort = System.getenv("SPARK_WORKER_WEBUI_PORT").toInt
  }
  if (System.getenv("SPARK_WORKER_DIR") != null) {
    workDir = System.getenv("SPARK_WORKER_DIR")
  }

  parse(args.toList)

  // This mutates the SparkConf, so all accesses to it must be made after this line
  // 这将改变SparkConf，因此必须在这一行之后进行所有访问
  propertiesFile = Utils.loadDefaultSparkProperties(conf, propertiesFile)

  if (conf.contains("spark.worker.ui.port")) {
    webUiPort = conf.get("spark.worker.ui.port").toInt
  }

  // 检查worker节点的内存
  checkWorkerMemory()

  @tailrec
  private def parse(args: List[String]): Unit = args match {
    case ("--ip" | "-i") :: value :: tail =>
      Utils.checkHost(value, "ip no longer supported, please use hostname " + value)
      host = value
      parse(tail)

    case ("--host" | "-h") :: value :: tail =>
      Utils.checkHost(value, "Please use hostname " + value)
      host = value
      parse(tail)

    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)

    case ("--cores" | "-c") :: IntParam(value) :: tail =>
      cores = value
      parse(tail)

    case ("--memory" | "-m") :: MemoryParam(value) :: tail =>
      memory = value
      parse(tail)

    case ("--work-dir" | "-d") :: value :: tail =>
      workDir = value
      parse(tail)

    case "--webui-port" :: IntParam(value) :: tail =>
      webUiPort = value
      parse(tail)

    case ("--properties-file") :: value :: tail =>
      propertiesFile = value
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case value :: tail =>
      if (masters != null) {  // Two positional arguments were given
        printUsageAndExit(1)
      }
      masters = Utils.parseStandaloneMasterUrls(value)
      parse(tail)

    case Nil =>
      if (masters == null) {  // No positional argument was given
        printUsageAndExit(1)
      }

    case _ =>
      printUsageAndExit(1)
  }

  /**
    * Print usage and exit JVM with the given exit code.       使用给定的退出代码打印使用和退出JVM。
    */
  def printUsageAndExit(exitCode: Int) {
    // scalastyle:off println
    System.err.println(
      "Usage: Worker [options] <master>\n" +
        "\n" +
        "Master must be a URL of the form spark://hostname:port\n" +
        "\n" +
        "Options:\n" +
        "  -c CORES, --cores CORES  Number of cores to use\n" +
        "  -m MEM, --memory MEM     Amount of memory to use (e.g. 1000M, 2G)\n" +
        "  -d DIR, --work-dir DIR   Directory to run apps in (default: SPARK_HOME/work)\n" +
        "  -i HOST, --ip IP         Hostname to listen on (deprecated, please use --host or -h)\n" +
        "  -h HOST, --host HOST     Hostname to listen on\n" +
        "  -p PORT, --port PORT     Port to listen on (default: random)\n" +
        "  --webui-port PORT        Port for web UI (default: 8081)\n" +
        "  --properties-file FILE   Path to a custom Spark properties file.\n" +
        "                           Default is conf/spark-defaults.conf.")
    // scalastyle:on println
    System.exit(exitCode)
  }

  def inferDefaultCores(): Int = {
    Runtime.getRuntime.availableProcessors()
  }

  def inferDefaultMemory(): Int = {
    // 获取java的供应商
    val ibmVendor = System.getProperty("java.vendor").contains("IBM")
    var totalMb = 0
    try {
      // scalastyle:off classforname  返回Java虚拟机正在运行的操作系统的托管bean。
      val bean = ManagementFactory.getOperatingSystemMXBean()
      // 如果是IBM生产的java
      if (ibmVendor) {
        // OperatingSystemMXBean: 在JAVA 7中，在监控方面，可以监视了系统和CPU负载
        val beanClass = Class.forName("com.ibm.lang.management.OperatingSystemMXBean")
        val method = beanClass.getDeclaredMethod("getTotalPhysicalMemory")
        totalMb = (method.invoke(bean).asInstanceOf[Long] / 1024 / 1024).toInt
      } else {
        val beanClass = Class.forName("com.sun.management.OperatingSystemMXBean")
        val method = beanClass.getDeclaredMethod("getTotalPhysicalMemorySize")
        totalMb = (method.invoke(bean).asInstanceOf[Long] / 1024 / 1024).toInt
      }
      // scalastyle:on classforname
    } catch {
      case e: Exception =>
        // 这一点catah是因为try里面的语句，可能会报错，因为，OperatingSystemMXBean获取内存信息，在某些平台上是没有这些包的，会报错，
        // 这时候，我们就需要假设一个内存大小为2GB
        totalMb = 2*1024
        // scalastyle:off println
        System.out.println("Failed to get total physical memory. Using " + totalMb + " MB")
      // scalastyle:on println
    }
    // Leave out 1 GB for the operating system, but don't return a negative memory size
    // 注意：memory默认为操作系统最大内存减去1GB，这1GB是留给操作系统使用的。
    math.max(totalMb - 1024, Utils.DEFAULT_DRIVER_MEM_MB)
  }

  def checkWorkerMemory(): Unit = {
    if (memory <= 0) {
      val message = "Memory is below 1MB, or missing a M/G at the end of the memory specification?"
      throw new IllegalStateException(message)
    }
  }
}
