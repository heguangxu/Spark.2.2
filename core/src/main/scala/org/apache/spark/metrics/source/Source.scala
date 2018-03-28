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

package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry

//资源的存储方式
// 资源名称
// MetricRegistry
// Metrics可以为你的代码的运行提供无与伦比的洞察力。作为一款监控指标的度量类库，
// 它提供了很多模块可以为第三方库或者应用提供辅助统计信息，
// 比如Jetty, Logback, Log4j, Apache HttpClient, Ehcache, JDBI, Jersey, 它还可以将度量数据发送给Ganglia和Graphite以提供图形化的监控。
private[spark] trait Source {
  def sourceName: String
  def metricRegistry: MetricRegistry
}
