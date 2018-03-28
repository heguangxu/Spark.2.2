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

package org.apache.spark.ui

import java.util.{Date, ServiceLoader}

import scala.collection.JavaConverters._

import org.apache.spark.{SecurityManager, SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.status.api.v1.{ApiRootResource, ApplicationAttemptInfo, ApplicationInfo,
  UIRoot}
import org.apache.spark.storage.StorageStatusListener
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.env.{EnvironmentListener, EnvironmentTab}
import org.apache.spark.ui.exec.{ExecutorsListener, ExecutorsTab}
import org.apache.spark.ui.jobs.{JobProgressListener, JobsTab, StagesTab}
import org.apache.spark.ui.scope.RDDOperationGraphListener
import org.apache.spark.ui.storage.{StorageListener, StorageTab}
import org.apache.spark.util.Utils

/**
 * Top level user interface for a Spark application.
 */
private[spark] class SparkUI private (
    val sc: Option[SparkContext],
    val conf: SparkConf,
    securityManager: SecurityManager,
    val environmentListener: EnvironmentListener,
    val storageStatusListener: StorageStatusListener,
    val executorsListener: ExecutorsListener,
    val jobProgressListener: JobProgressListener,
    val storageListener: StorageListener,
    val operationGraphListener: RDDOperationGraphListener,
    var appName: String,
    val basePath: String,
    val startTime: Long)
  extends WebUI(securityManager, securityManager.getSSLOptions("ui"), SparkUI.getUIPort(conf),
    conf, basePath, "SparkUI")
  with Logging
  with UIRoot {

  // Spark UI服务默认是可以被杀掉的，可以修改属性spark.ui.killEnabled为false可以保证不被杀死
  val killEnabled = sc.map(_.conf.getBoolean("spark.ui.killEnabled", true)).getOrElse(false)

  var appId: String = _

  private var streamingJobProgressListener: Option[SparkListener] = None

  /** Initialize all components of the server.
    * 初始化服务器的所有组件。
    * */
  def initialize() {
    val jobsTab = new JobsTab(this)
    attachTab(jobsTab)
    val stagesTab = new StagesTab(this)
    attachTab(stagesTab)
    attachTab(new StorageTab(this))
    attachTab(new EnvironmentTab(this))
    attachTab(new ExecutorsTab(this))
    attachHandler(createStaticHandler(SparkUI.STATIC_RESOURCE_DIR, "/static"))
    attachHandler(createRedirectHandler("/", "/jobs/", basePath = basePath))
    attachHandler(ApiRootResource.getServletHandler(this))
    // These should be POST only, but, the YARN AM proxy won't proxy POSTs
    attachHandler(createRedirectHandler(
      "/jobs/job/kill", "/jobs/", jobsTab.handleKillRequest, httpMethods = Set("GET", "POST")))
    attachHandler(createRedirectHandler(
      "/stages/stage/kill", "/stages/", stagesTab.handleKillRequest,
      httpMethods = Set("GET", "POST")))
  }

  // 调用改类的初始化方法
  initialize()

  def getSparkUser: String = {
    environmentListener.systemProperties.toMap.getOrElse("user.name", "<unknown>")
  }

  def getAppName: String = appName

  def setAppId(id: String): Unit = {
    appId = id
  }

  /** Stop the server behind this web interface. Only valid after bind().
    * 在这个web界面后面停止服务器。绑定后才有效()。
    * */
  override def stop() {
    super.stop()
    logInfo(s"Stopped Spark web UI at $webUrl")
  }

  def getSparkUI(appId: String): Option[SparkUI] = {
    if (appId == this.appId) Some(this) else None
  }

  def getApplicationInfoList: Iterator[ApplicationInfo] = {
    Iterator(new ApplicationInfo(
      id = appId,
      name = appName,
      coresGranted = None,
      maxCores = None,
      coresPerExecutor = None,
      memoryPerExecutorMB = None,
      attempts = Seq(new ApplicationAttemptInfo(
        attemptId = None,
        startTime = new Date(startTime),
        endTime = new Date(-1),
        duration = 0,
        lastUpdated = new Date(startTime),
        sparkUser = getSparkUser,
        completed = false
      ))
    ))
  }

  def getApplicationInfo(appId: String): Option[ApplicationInfo] = {
    getApplicationInfoList.find(_.id == appId)
  }

  def getStreamingJobProgressListener: Option[SparkListener] = streamingJobProgressListener

  def setStreamingJobProgressListener(sparkListener: SparkListener): Unit = {
    streamingJobProgressListener = Option(sparkListener)
  }
}

private[spark] abstract class SparkUITab(parent: SparkUI, prefix: String)
  extends WebUITab(parent, prefix) {

  def appName: String = parent.appName

}

private[spark] object SparkUI {
  val DEFAULT_PORT = 4040
  val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"
  val DEFAULT_POOL_NAME = "default"
  val DEFAULT_RETAINED_STAGES = 1000
  val DEFAULT_RETAINED_JOBS = 1000


  // 可以修改spark.ui.port，去修改Spark的界面端口
  def getUIPort(conf: SparkConf): Int = {
    conf.getInt("spark.ui.port", SparkUI.DEFAULT_PORT)
  }

  def createLiveUI(
      sc: SparkContext,
      conf: SparkConf,
      listenerBus: SparkListenerBus,
      jobProgressListener: JobProgressListener,
      securityManager: SecurityManager,
      appName: String,
      startTime: Long): SparkUI = {
    create(Some(sc), conf, listenerBus, securityManager, appName,
      jobProgressListener = Some(jobProgressListener), startTime = startTime)
  }

  def createHistoryUI(
      conf: SparkConf,
      listenerBus: SparkListenerBus,
      securityManager: SecurityManager,
      appName: String,
      basePath: String,
      startTime: Long): SparkUI = {
    val sparkUI = create(
      None, conf, listenerBus, securityManager, appName, basePath, startTime = startTime)

    val listenerFactories = ServiceLoader.load(classOf[SparkHistoryListenerFactory],
      Utils.getContextOrSparkClassLoader).asScala
    listenerFactories.foreach { listenerFactory =>
      val listeners = listenerFactory.createListeners(conf, sparkUI)
      listeners.foreach(listenerBus.addListener)
    }
    sparkUI
  }

  /**
   * Create a new Spark UI.       创建一个新的Spark UI.
   *
   * @param sc optional SparkContext; this can be None when reconstituting a UI from event logs.
    *           可选SparkContext;当从事件日志重新构造一个UI时，这是不可能的。
   * @param jobProgressListener if supplied, this JobProgressListener will be used; otherwise, the
   *                            web UI will create and register its own JobProgressListener.
    *                            如果提供，这个JobProgressListener将被使用;否则，web UI将创建并注册自己的JobProgressListener。
    *
    *  可以知道在create方法里除了JobProgressListener是外部传入的之外，又增加了一些SparkListener.例如
    *     1.用于对JVM参数， Spark属性，Java系统属性，classpath等进行监控的EnvironmentListener;
    *     2.用于维护Executor的存储状态的StorageStatusListener;
    *     3.用于准备将Executor的信息展示在ExecutorsTab的ExecutorsListener;
    *     4.用于准备将Executor相关存储信息展示在BlockManagerUI的StorgeListener等。
    *     5.最后创建的SparkUI，SparkUI服务默认是可以杀掉的，通过修改属性Spark.ui.killEnabled为false可以保证不被杀死
    *     6.initialize方法会组织前端页面各个Tab和Page的展示和布局。
    *
   */
  private def create(
      sc: Option[SparkContext],
      conf: SparkConf,
      listenerBus: SparkListenerBus,
      securityManager: SecurityManager,
      appName: String,
      basePath: String = "",
      jobProgressListener: Option[JobProgressListener] = None,
      startTime: Long): SparkUI = {

    //  这里先获取JobProgressListener，如果没有JobProgressListener就new一个添加进去
    val _jobProgressListener: JobProgressListener = jobProgressListener.getOrElse {
      val listener = new JobProgressListener(conf)
      listenerBus.addListener(listener)
      listener
    }

    //  一个SparkListener，它准备在environment选项卡上显示信息
    val environmentListener = new EnvironmentListener
    // 存储状态
    val storageStatusListener = new StorageStatusListener(conf)
    // executor监听器 在UI界面 ExecutorsTab显示的类
    val executorsListener = new ExecutorsListener(storageStatusListener, conf)
    //  BlockManagerUI按钮显示的信息
    val storageListener = new StorageListener(storageStatusListener)
    // 一个SparkListener，它构造一个RDD操作的DAG。
    val operationGraphListener = new RDDOperationGraphListener(conf)

    // 全部放在listenerBus中
    listenerBus.addListener(environmentListener)
    listenerBus.addListener(storageStatusListener)
    listenerBus.addListener(executorsListener)
    listenerBus.addListener(storageListener)
    listenerBus.addListener(operationGraphListener)

    new SparkUI(sc, conf, securityManager, environmentListener, storageStatusListener,
      executorsListener, _jobProgressListener, storageListener, operationGraphListener,
      appName, basePath, startTime)
  }
}
