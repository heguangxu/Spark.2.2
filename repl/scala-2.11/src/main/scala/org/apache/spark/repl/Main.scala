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

package org.apache.spark.repl

import java.io.File
import java.util.Locale

import scala.tools.nsc.GenericRunnerSettings

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.util.Utils

object Main extends Logging {

  // 初始化日志
  initializeLogIfNecessary(true)
  // z注册一个信号事件：当我们按下ctrl+c键的时候，会调用对应的信号处理程序，先获取活动的SparkContext，然后取消全部的job
  Signaling.cancelOnInterrupt()

  val conf = new SparkConf()
  val rootDir = conf.getOption("spark.repl.classdir").getOrElse(Utils.getLocalDir(conf))
  val outputDir = Utils.createTempDir(root = rootDir, namePrefix = "repl")

  var sparkContext: SparkContext = _
  var sparkSession: SparkSession = _
  // this is a public var because tests reset it. 这是一个公共var，因为测试会重置它。
  var interp: SparkILoop = _

  private var hasErrors = false

  private def scalaOptionError(msg: String): Unit = {
    hasErrors = true
    Console.err.println(msg)
  }

  /**
    * main方法
    * @param args
    */
  def main(args: Array[String]) {
    // 这里先new  SparkILoop，然后才是调用doMain（）
    doMain(args, new SparkILoop)
  }

  // Visible for testing 可见测试
  private[repl] def doMain(args: Array[String], _interp: SparkILoop): Unit = {
    interp = _interp
    val jars = Utils.getUserJars(conf, isShell = true).mkString(File.pathSeparator)
    val interpArguments = List(
      "-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}",
      "-classpath", jars
    ) ++ args.toList

    val settings = new GenericRunnerSettings(scalaOptionError)
    // 一个可变对象设置。
    settings.processArguments(interpArguments, true)

    // 默认为false，这里为true
    if (!hasErrors) {
      /**  这里调用lLoop的process() --> SparkILoop.loadFiles --> SparkILoop.initializeSpark() */
      interp.process(settings) // Repl starts and goes in loop of R.E.P.L
      Option(sparkContext).foreach(_.stop)
    }
  }

  def createSparkSession(): SparkSession = {
    val execUri = System.getenv("SPARK_EXECUTOR_URI")
    conf.setIfMissing("spark.app.name", "Spark shell")
    // SparkContext will detect this configuration and register it with the RpcEnv's
    // file server, setting spark.repl.class.uri to the actual URI for executors to
    // use. This is sort of ugly but since executors are started as part of SparkContext
    // initialization in certain cases, there's an initialization order issue that prevents
    // this from being set after SparkContext is instantiated.

    // SparkContext将检测此配置并将其注册到RpcEnv的文件服务器上，设置setting spark.repl.class.uri
    // 的实际uri供执行程序使用。这有点不太好，但是因为在某些情况下，executor是SparkContext
    // 初始化的一部分，所以在SparkContext被实例化之后，有一个初始化顺序问题阻止了它的设置。
    conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath())
    if (execUri != null) {
      conf.set("spark.executor.uri", execUri)
    }
    if (System.getenv("SPARK_HOME") != null) {
      conf.setSparkHome(System.getenv("SPARK_HOME"))
    }

    val builder = SparkSession.builder.config(conf)
    if (conf.get(CATALOG_IMPLEMENTATION.key, "hive").toLowerCase(Locale.ROOT) == "hive") {
      // 如果hive的类能够加载就返回true，否则返回false
      if (SparkSession.hiveClassesArePresent) {
        // In the case that the property is not set at all, builder's config
        // does not have this value set to 'hive' yet. The original default
        // behavior is that when there are hive classes, we use hive catalog.

        // 在没有设置属性的情况下，构建器的配置没有将这个值设置为“hive”。原始的默认行为是，
        // 当有hive类时，我们使用hive catalog。
        sparkSession = builder.enableHiveSupport().getOrCreate()
        logInfo("Created Spark session with Hive support")
      } else {
        // Need to change it back to 'in-memory' if no hive classes are found
        // in the case that the property is set to hive in spark-defaults.conf
        // 如果没有发现在spark-default .conf中属性设置为hive的情况，则需要将其更改为“内存”。
        builder.config(CATALOG_IMPLEMENTATION.key, "in-memory")
        sparkSession = builder.getOrCreate()
        logInfo("Created Spark session")
      }
    } else {
      // In the case that the property is set but not to 'hive', the internal
      // default is 'in-memory'. So the sparkSession will use in-memory catalog.
      // 在属性设置而不是“hive”的情况下，内部默认为“内存中”。因此，sparkSession将使用内存目录。
      sparkSession = builder.getOrCreate()
      logInfo("Created Spark session")
    }
    sparkContext = sparkSession.sparkContext
    sparkSession
  }

}
