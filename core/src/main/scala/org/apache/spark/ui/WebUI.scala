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

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.xml.Node

import org.eclipse.jetty.servlet.ServletContextHandler
import org.json4s.JsonAST.{JNothing, JValue}

import org.apache.spark.{SecurityManager, SparkConf, SSLOptions}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.Utils

/**
 * The top level component of the UI hierarchy that contains the server.
  * 包含服务器的UI层次结构的顶层组件。
 *
 * Each WebUI represents a collection of tabs, each of which in turn represents a collection of
 * pages. The use of tabs is optional, however; a WebUI may choose to include pages directly.
  *
  * 每个WebUI都表示一个选项卡的集合，每个选项卡依次表示一个页面集合。然而，选项卡的使用是可选的;
  * WebUI可以选择直接包含页面。
 */
private[spark] abstract class WebUI(
    val securityManager: SecurityManager,
    val sslOptions: SSLOptions,
    port: Int,
    conf: SparkConf,
    basePath: String = "",
    name: String = "")
  extends Logging {

  // WebUITab的tab页 是按钮对应的页面集合
  protected val tabs = ArrayBuffer[WebUITab]()
  // 页面按钮对应的动作ServletContextHandler
  protected val handlers = ArrayBuffer[ServletContextHandler]()
  // 将页面和对应的动作对应起来
  protected val pageToHandlers = new HashMap[WebUIPage, ArrayBuffer[ServletContextHandler]]
  protected var serverInfo: Option[ServerInfo] = None
  // DNS
  protected val publicHostName = Option(conf.getenv("SPARK_PUBLIC_DNS")).getOrElse(
    conf.get(DRIVER_HOST_ADDRESS))
  private val className = Utils.getFormattedClassName(this)

  def getBasePath: String = basePath
  def getTabs: Seq[WebUITab] = tabs
  def getHandlers: Seq[ServletContextHandler] = handlers
  def getSecurityManager: SecurityManager = securityManager   // 安全管理器

  /** Attach a tab to this UI, along with all of its attached pages.
    * 附加一个tab按钮到这个Spark UI界面 ，连同它的所有附属页面。
    * */
  def attachTab(tab: WebUITab) {
    tab.pages.foreach(attachPage)
    tabs += tab
  }

  // 去除某个界面
  def detachTab(tab: WebUITab) {
    tab.pages.foreach(detachPage)
    tabs -= tab
  }

  def detachPage(page: WebUIPage) {
    pageToHandlers.remove(page).foreach(_.foreach(detachHandler))
  }

  /** Attach a page to this UI.
    * 在这个UI上附加一个页面。
    * */
  def attachPage(page: WebUIPage) {
    val pagePath = "/" + page.prefix
    /**
        前spark 支持三种形态的http渲染结果：
          text/json
          text/html
          text/plain
        一般而言一个WebUIPage会对应两个Handler，
        在页面路径上，html和json的区别就是html的url path 多加了一个"/json"后缀。
        这里可以看到，一般一个page最好实现
        render
        renderJson
        两个方法，以方便使用。
        另外值得一提的是，上面的代码也展示了URL Path和对应的处理逻辑(Controller/Action)是如何关联起来的。
        其实就是pagePath -> Page的render函数。
      */
    val renderHandler = createServletHandler(pagePath,
      (request: HttpServletRequest) => page.render(request), securityManager, conf, basePath)
    val renderJsonHandler = createServletHandler(pagePath.stripSuffix("/") + "/json",
      (request: HttpServletRequest) => page.renderJson(request), securityManager, conf, basePath)

    attachHandler(renderHandler)
    attachHandler(renderJsonHandler)
    val handlers = pageToHandlers.getOrElseUpdate(page, ArrayBuffer[ServletContextHandler]())
    handlers += renderHandler
  }

  /** Attach a handler to this UI. */
  def attachHandler(handler: ServletContextHandler) {
    handlers += handler
    serverInfo.foreach(_.addHandler(handler))
  }

  /** Detach a handler from this UI. */
  def detachHandler(handler: ServletContextHandler) {
    handlers -= handler
    serverInfo.foreach(_.removeHandler(handler))
  }

  /**
   * Add a handler for static content.
   *
   * @param resourceBase Root of where to find resources to serve.
   * @param path Path in UI where to mount the resources.
   */
  def addStaticHandler(resourceBase: String, path: String): Unit = {
    attachHandler(JettyUtils.createStaticHandler(resourceBase, path))
  }

  /**
   * Remove a static content handler.
   *
   * @param path Path in UI to unmount.
   */
  def removeStaticHandler(path: String): Unit = {
    handlers.find(_.getContextPath() == path).foreach(detachHandler)
  }

  /** Initialize all components of the server. */
  def initialize(): Unit

  /** Bind to the HTTP server behind this web interface.
    * 绑定到这个web接口后面的HTTP服务器。
    *
    * SparkUI创建好了，需要调用父类的WebUI的bind方法，绑定host和端口，bind方法中主要的代码实现
    *  serverInfo = Some(startJettyServer(host, port, sslOptions, handlers, conf, name))
    * */
  def bind(): Unit = {
    assert(serverInfo.isEmpty, s"Attempted to bind $className more than once!")
    try {
      val host = Option(conf.getenv("SPARK_LOCAL_IP")).getOrElse("0.0.0.0")
      serverInfo = Some(startJettyServer(host, port, sslOptions, handlers, conf, name))
      logInfo(s"Bound $className to $host, and started at $webUrl")
    } catch {
      case e: Exception =>
        logError(s"Failed to bind $className", e)
        System.exit(1)
    }
  }

  /** Return the url of web interface. Only valid after bind(). */
  def webUrl: String = s"http://$publicHostName:$boundPort"

  /** Return the actual port to which this server is bound. Only valid after bind(). */
  def boundPort: Int = serverInfo.map(_.boundPort).getOrElse(-1)

  /** Stop the server behind this web interface. Only valid after bind().
    * 停止服务绑定web接口
    * */
  def stop(): Unit = {
    assert(serverInfo.isDefined,
      s"Attempted to stop $className before binding to a server!")
    serverInfo.get.stop()
  }
}


/**
 * A tab that represents a collection of pages.
 * The prefix is appended to the parent address to form a full path, and must not contain slashes.
  *
  * 表示页面集合的选项卡。
  * 前缀被追加到父地址以形成完整的路径，并且不能包含斜杠。
 */
private[spark] abstract class WebUITab(parent: WebUI, val prefix: String) {
  val pages = ArrayBuffer[WebUIPage]()
  val name = prefix.capitalize

  /** Attach a page to this tab. This prepends the page's prefix with the tab's own prefix. */
  def attachPage(page: WebUIPage) {
    page.prefix = (prefix + "/" + page.prefix).stripSuffix("/")
    pages += page
  }

  /** Get a list of header tabs from the parent UI. */
  def headerTabs: Seq[WebUITab] = parent.getTabs

  def basePath: String = parent.getBasePath
}


/**
 * A page that represents the leaf node in the UI hierarchy.
  * 表示UI层次结构中的叶节点的页面。
 *
 * The direct parent of a WebUIPage is not specified as it can be either a WebUI or a WebUITab.
 * If the parent is a WebUI, the prefix is appended to the parent's address to form a full path.
 * Else, if the parent is a WebUITab, the prefix is appended to the super prefix of the parent
 * to form a relative path. The prefix must not contain slashes.
  *
  * WebUIPage的直接父母没有指定，因为它可以是WebUI或WebUITab。如果父类是WebUI，则该前缀将附加到父地址以形成完整路径。
  * 否则，如果父类是WebUITab，则前缀将附加到父元素的超级前缀以形成相对路径。前缀不能包含斜杠。
 */
private[spark] abstract class WebUIPage(var prefix: String) {
  // render和renderJson用于相应页面请求
  def render(request: HttpServletRequest): Seq[Node]
  def renderJson(request: HttpServletRequest): JValue = JNothing
}
