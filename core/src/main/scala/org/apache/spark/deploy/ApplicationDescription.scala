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

import java.net.URI

// app应用程序的描述
private[spark] case class ApplicationDescription(
    name: String,                           // application的名字，可以通过spark.app.name设置
    maxCores: Option[Int],                  // 最多可以使用的CPU  core的数量，可以通过spark.cores.max设置，每个executor可以占用的内存数，可以通过
                                            // spark.executor.momory,SPARK_EXECUTOR_MEMORY或者SPARK_MEM设置，默认值是512M
    memoryPerExecutorMB: Int,
    //  worker Node 拉起的ExecutorBackend进程的Command,在Worker街道Master LaunchExecutor，后会通过ExecutorRunner启动这个Command.Command包含了
    // 一个启动Java进程所需要的信息，包括启动的ClassName,所需要的参数，环境信息等
    command: Command,
    appUiUrl: String,
    //  Application 的web UI的hostname:port 如果spark.eventLog.enabled(默认为false)指定为true的话，eventLogFile就设置为Spark.eventLog.dir定义的目录
    eventLogDir: Option[URI] = None,
    // short name of compression codec used when writing event logs, if any (e.g. lzf)
    eventLogCodec: Option[String] = None,
    coresPerExecutor: Option[Int] = None,
    // number of executors this application wants to start with,
    // only used if dynamic allocation is enabled
    initialExecutorLimit: Option[Int] = None,
    user: String = System.getProperty("user.name", "<unknown>")) {

  override def toString: String = "ApplicationDescription(" + name + ")"
}
