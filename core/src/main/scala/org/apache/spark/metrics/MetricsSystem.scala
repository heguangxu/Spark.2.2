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

package org.apache.spark.metrics

import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import com.codahale.metrics.{Metric, MetricFilter, MetricRegistry}
import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.config._
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.sink.{MetricsServlet, Sink}
import org.apache.spark.metrics.source.{Source, StaticSources}
import org.apache.spark.util.Utils

/**
 * Spark Metrics System, created by a specific "instance", combined by source,
 * sink, periodically polls source metrics data to sink destinations.
 * 创建被指定的实例
  *   spark的测量实例，指定一个"instance"实例去创建它，结合source，sink,periodically polls source metrics data to sink destinations.
  *
 * "instance" specifies "who" (the role) uses the metrics system. In Spark, there are several roles
 * like master, worker, executor, client driver. These roles will create metrics system
 * for monitoring. So, "instance" represents these roles. Currently in Spark, several instances
 * have already implemented: master, worker, executor, driver, applications.
 *
  * 实例："instance"代表的是谁在使用这个监测系统。在Spark中有以下几种实例。比如：master, worker, executor, client driver
  * 这些实例都会创建检测系统，为了监测各自的情况。所以，实例指的就是这些。在目前的spark版本中已经实现了几个实例的监控，分别是
  * master, worker, executor, driver, applications.
  *
  *
 * "source" specifies "where" (source) to collect metrics data from. In metrics system, there exists
 * two kinds of source:
 *   1. Spark internal source, like MasterSource, WorkerSource, etc, which will collect
 *   Spark component's internal state, these sources are related to instance and will be
 *   added after a specific metrics system is created.
 *   2. Common source, like JvmSource, which will collect low level state, is configured by
 *   configuration and loaded through reflection.
 *
  * 数据来源
  *   "source"值得是从哪里去收集这些被监控的数据。在监测系统中，已经存在了以下几种类别的数据来源：
  *   1.spark内在的数据来源，比如MasterSource, WorkerSource,等，他们会收集一些spark组件内在的状态，这些实例在监测系统创建的时候
  *     会自动关联。
  *   2.普通的数据来源，比如JvmSource，它将收集低级别状态，配置为配置，并通过反射加载。
  *
  *
 * "sink" specifies "where" (destination) to output metrics data to. Several sinks can
 * coexist and metrics can be flushed to all these sinks.
  *
  * 要把数据输出到哪里的sink：
  *   多个接收器可以共存，而指标可以被刷新到所有这些接收器。
  *
 *
 * Metrics configuration format is like below:
 * [instance].[sink|source].[name].[options] = xxxx
 *
 * [instance] can be "master", "worker", "executor", "driver", "applications" which means only
 * the specified instance has this property.
 * wild card "*" can be used to replace instance name, which means all the instances will have
 * this property.
 *
 * [sink|source] means this property belongs to source or sink. This field can only be
 * source or sink.
 *
 * [name] specify the name of sink or source, if it is custom defined.
 *
 * [options] represent the specific property of this source or sink.
  *
  *
  * 监测系统的配置格式如下：
  *
 */

/**
  * csdn博客：http://blog.csdn.net/allwefantasy/article/details/50449464
  * MetricsSystem 比较好理解，一般是为了衡量系统的各种指标的度量系统。算是一个key-value形态的东西。
  * 举个比较简单的例子，我怎么把当前JVM相关信息展示出去呢？做法自然很多，通过MetricsSystem就可以做的更标准化些
  *
  * MetricesSystem使用codahale提供的第三方测量仓库Metrics，它有3个概念：
  *   Instance:制定了谁在使用测量系统
  *   source:制定了从哪里手机测量数据
  *   Sink:指定从哪里输出测量数据
  *
  * Spark按照Instance的不同，区分为Master,Worker,Application,Driver和Executor.
  * Spark目前提供的Sink有ConsoleSink，csvSink,JmxSink,MetricsServlet,GraphiteSink等。
  * Spark中使用的MetricsServlet作为默认的Sink
  *
  */
private[spark] class MetricsSystem private (
    val instance: String,
    conf: SparkConf,
    securityMgr: SecurityManager)
  extends Logging {

  // 主要是加载监测系统的属性
  private[this] val metricsConfig = new MetricsConfig(conf)

  private val sinks = new mutable.ArrayBuffer[Sink]
  private val sources = new mutable.ArrayBuffer[Source]
  private val registry = new MetricRegistry()

  private var running: Boolean = false

  // Treat MetricsServlet as a special sink as it should be exposed to add handlers to web ui
  private var metricsServlet: Option[MetricsServlet] = None

  /**
   * Get any UI handlers used by this metrics system; can only be called after start().
    * 得到任何UI handlers，但是只能在start()方法调用后调用
    *
    * 为了能够在SparkUI（网页）访问到测量数据，所以需要给Sinks增加Jetty的servletContextHandler,这里
    * 主要用到了MetricsSystem的GetServletHandlers。
   */
  def getServletHandlers: Array[ServletContextHandler] = {
    require(running, "Can only call getServletHandlers on a running MetricsSystem")
    metricsServlet.map(_.getHandlers(conf)).getOrElse(Array())
  }

  // 主要是加载监测系统的属性，执行初始化方法
  metricsConfig.initialize()

  /**
    * 启动MetricsSystem貌似只能启动一次，否则会出问题 该方法在sparkContext中被_env.metricsSystem.start()这样调用
    * 启动过程包括以下步骤：
    *  1.注册Sources;
    *  2.注册Sinks;
    *  3.给Sinks增加Jetty的ServletContextHandler.
    */
  def start() {
    //require() 方法用在对参数的检验上，不通过则抛出 IllegalArgumentException，第一次为通过
    require(!running, "Attempting to start a MetricsSystem that is already running")
    // 立即设置这个实例的监控为正在运行，下次再启动这个实例的监控就会报错 Attempting to start a MetricsSystem that is already running
    running = true
    // 这句话的意思是得到所有的静态资源遍历它，并且为每个资源执行registerSource方法（该方法用于注册资源）
    StaticSources.allSources.foreach(registerSource)
    // 上面一句是把杂碎的source注册到Sources中，存储在一个mutable.ArrayBuffer[Source]中，这里是把sources。。。。。
    registerSources()
    // 注册Sinks
    registerSinks()
    // 这一点不知道执行的是哪个start方法
    sinks.foreach(_.start)
  }

  // 停止当前运行的实例对应的监测系统
  def stop() {
    if (running) {
      sinks.foreach(_.stop)
    } else {
      logWarning("Stopping a MetricsSystem that is not running")
    }
    running = false
  }

  def report() {
    sinks.foreach(_.report())
  }

  /**
   * Build a name that uniquely identifies each metric source.
   * The name is structured as follows: <app ID>.<executor ID (or "driver")>.<source name>.
   * If either ID is not available, this defaults to just using <source name>.
   *
    * 构建唯一标识每个度量源的名称。名称的结构如下:
    *     <app ID>.<executor ID (or "driver")>.<source name>.
    * 如果两个ID都得不到，那么格式如下：
    *     <source name>
   * @param source Metric source to be named by this method.
   * @return An unique metric name for each combination of
   *         application, executor/driver and metric source.
   */
  private[spark] def buildRegistryName(source: Source): String = {
    val metricsNamespace = conf.get(METRICS_NAMESPACE).orElse(conf.getOption("spark.app.id"))

    val executorId = conf.getOption("spark.executor.id")
    val defaultName = MetricRegistry.name(source.sourceName)

    if (instance == "driver" || instance == "executor") {
      if (metricsNamespace.isDefined && executorId.isDefined) {
        MetricRegistry.name(metricsNamespace.get, executorId.get, source.sourceName)
      } else {
        // Only Driver and Executor set spark.app.id and spark.executor.id.
        // Other instance types, e.g. Master and Worker, are not related to a specific application.
        if (metricsNamespace.isEmpty) {
          logWarning(s"Using default name $defaultName for source because neither " +
            s"${METRICS_NAMESPACE.key} nor spark.app.id is set.")
        }
        if (executorId.isEmpty) {
          logWarning(s"Using default name $defaultName for source because spark.executor.id is " +
            s"not set.")
        }
        defaultName
      }
    } else { defaultName }
  }

  def getSourcesByName(sourceName: String): Seq[Source] =
    sources.filter(_.sourceName == sourceName)

  /** 注册资源
    * 创建完ExecutorSource后，调用MetricsSysytem的registerSource方法将ExecutorSource注册到MetricsSystem.
    * registerSource方法使用MetricRegistry的register方法，将Source注册到MetricRegistry.
    */
  def registerSource(source: Source) {
    sources += source
    try {
      val regName = buildRegistryName(source)
      registry.register(regName, source.metricRegistry)
    } catch {
      case e: IllegalArgumentException => logInfo("Metrics already registered", e)
    }
  }

  def removeSource(source: Source) {
    sources -= source
    val regName = buildRegistryName(source)
    registry.removeMatching(new MetricFilter {
      def matches(name: String, metric: Metric): Boolean = name.startsWith(regName)
    })
  }

  /**
    registerSources方法用于注册Sources,高数测量系统从哪里收集测量数据。
    1.从metricsConfig获取Driver的Properties,默认为创建MetricsSystem的过程中解析的
      {sink.servlet.class=org.apache.spark.metrics.sink.MetricsServlet,sink.servlet.path=/metrics/json}
    2.用正则表达式匹配Driver的Properties中以source.开头的属性，然后将属性中的Source反射得到实例
      加入ArrayBuffer[Source].
    3.将每个source的metricRegistry(也是MetricSet的子类型)注册到ConcurrentMap<String,Metric>metrics.

    */
  private def registerSources() {
    // 得到一个实例的配置   比如driver的配置（或者executor的配置）
    val instConfig = metricsConfig.getInstance(instance)
    // 得到这个实例的资源配置
    val sourceConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SOURCE_REGEX)

    // Register all the sources related to instance
    // 注册所有与实例相关的资源
    sourceConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      try {
        val source = Utils.classForName(classPath).newInstance()
        registerSource(source.asInstanceOf[Source])
      } catch {
        case e: Exception => logError("Source class " + classPath + " cannot be instantiated", e)
      }
    }
  }

  /**
    registerSinks方法用于注册Sinks，既高数测量系统MetricsSystem往哪输出测量数据。
    1.从Driver的Properties中用正则匹配以sink.开头的属性，如{sink.servlet.class=org.apache.
      spark.metrics.sink.MetricsServlet，sink.servlet.path=/metrics/json},将其转换为
      Map(servlet->{class=org.apache.spark.metrics.sink.MetricsServlet,path=/metrics/json})
    2.将子属性class对应的类metricsServlet反射得到MetricsServlet实例，如果属性的key是Servlet,
      将其设置为metricsServlet；如果是Sink,则假如到ArrayBuffer[sink]中。
    */
  private def registerSinks() {
    val instConfig = metricsConfig.getInstance(instance)
    val sinkConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SINK_REGEX)

    sinkConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      if (null != classPath) {
        try {
          val sink = Utils.classForName(classPath)
            .getConstructor(classOf[Properties], classOf[MetricRegistry], classOf[SecurityManager])
            .newInstance(kv._2, registry, securityMgr)
          if (kv._1 == "servlet") {
            metricsServlet = Some(sink.asInstanceOf[MetricsServlet])
          } else {
            sinks += sink.asInstanceOf[Sink]
          }
        } catch {
          case e: Exception =>
            logError("Sink class " + classPath + " cannot be instantiated")
            throw e
        }
      }
    }
  }



}

private[spark] object MetricsSystem {
  val SINK_REGEX = "^sink\\.(.+)\\.(.+)".r
  val SOURCE_REGEX = "^source\\.(.+)\\.(.+)".r

  private[this] val MINIMAL_POLL_UNIT = TimeUnit.SECONDS
  private[this] val MINIMAL_POLL_PERIOD = 1

  def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Int) {
    val period = MINIMAL_POLL_UNIT.convert(pollPeriod, pollUnit)
    if (period < MINIMAL_POLL_PERIOD) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit +
        " below than minimal polling period ")
    }
  }

  def createMetricsSystem(
      instance: String, conf: SparkConf, securityMgr: SecurityManager): MetricsSystem = {
    new MetricsSystem(instance, conf, securityMgr)
  }
}
