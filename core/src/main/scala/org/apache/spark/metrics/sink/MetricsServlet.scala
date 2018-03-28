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

package org.apache.spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletRequest

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.json.MetricsModule
import com.fasterxml.jackson.databind.ObjectMapper
import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.ui.JettyUtils._

private[spark] class MetricsServlet(
    val property: Properties,
    val registry: MetricRegistry,
    securityMgr: SecurityManager)
  extends Sink {

  val SERVLET_KEY_PATH = "path"
  val SERVLET_KEY_SAMPLE = "sample"

  val SERVLET_DEFAULT_SAMPLE = false

  val servletPath = property.getProperty(SERVLET_KEY_PATH)

  val servletShowSample = Option(property.getProperty(SERVLET_KEY_SAMPLE)).map(_.toBoolean)
    .getOrElse(SERVLET_DEFAULT_SAMPLE)

  val mapper = new ObjectMapper().registerModule(
    new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, servletShowSample))

  /**
    可以看到metricsServlet调用了getHandlers，最终生成处理/metrics/json请求的ServletContextHandler,而请求的真正处理
    由MetricsSnapshot方法，利用fastjson解析。生成的ServletContextHandler通过SparkUI的attacheHandler方法。也被绑定到SparkUI
    （createServlethandler与attacheHandler方法），最终我们可以使用以下地址来访问测量数据：
        http:localhost:4040/metrics/applications/json
        http:localhost:4040/metrics/json
        http:localhost:4040/metrics/master/json
    */
  def getHandlers(conf: SparkConf): Array[ServletContextHandler] = {
    Array[ServletContextHandler](
      createServletHandler(servletPath,
        new ServletParams(request => getMetricsSnapshot(request), "text/json"), securityMgr, conf)
    )
  }

  def getMetricsSnapshot(request: HttpServletRequest): String = {
    mapper.writeValueAsString(registry)
  }

  override def start() { }

  override def stop() { }

  override def report() { }
}
