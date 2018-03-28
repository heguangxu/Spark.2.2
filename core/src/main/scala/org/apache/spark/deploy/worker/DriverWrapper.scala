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

import java.io.File

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util.{ChildFirstURLClassLoader, MutableURLClassLoader, Utils}

/**
  * Utility object for launching driver programs such that they share fate with the Worker process.
  * This is used in standalone cluster mode only.
  * 用于启动驱动程序的实用对象，它们与工作进程共享命运。这仅用于独立集群模式。
  *
  */
object DriverWrapper {
  def main(args: Array[String]) {
    args.toList match {
      /*
       * IMPORTANT: Spark 1.3 provides a stable application submission gateway that is both
       * backward and forward compatible across future Spark versions. Because this gateway
       * uses this class to launch the driver, the ordering and semantics of the arguments
       * here must also remain consistent across versions.
       *
       * 重要:Spark 1.3提供了一个稳定的应用程序提交网关，它在未来的Spark版本中既向后兼容又向前兼容。
       * 因为这个网关使用这个类来启动驱动程序，所以这里的参数的排序和语义也必须在不同版本之间保持一致。
       *
       */
      case workerUrl :: userJar :: mainClass :: extraArgs =>
        val conf = new SparkConf()
        // 创建SecurityManager和RpcEnv
        val rpcEnv = RpcEnv.create("Driver",Utils.localHostName(), 0, conf, new SecurityManager(conf))
        // 创建WorkerWatcher：连接到工作进程的端点，如果连接断开，则终止JVM。
        rpcEnv.setupEndpoint("workerWatcher", new WorkerWatcher(rpcEnv, workerUrl))

        val currentLoader = Thread.currentThread.getContextClassLoader
        val userJarUrl = new File(userJar).toURI().toURL()
        val loader =
          if (sys.props.getOrElse("spark.driver.userClassPathFirst", "false").toBoolean) {
            new ChildFirstURLClassLoader(Array(userJarUrl), currentLoader)
          } else {
            new MutableURLClassLoader(Array(userJarUrl), currentLoader)
          }
        Thread.currentThread.setContextClassLoader(loader)

        // Delegate to supplied main class
        val clazz = Utils.classForName(mainClass)
        val mainMethod = clazz.getMethod("main", classOf[Array[String]])

        // 最终采用invoke方法调用，用户写的main()方法，我们在方法中创建了SparkContext
        mainMethod.invoke(null, extraArgs.toArray[String])

        rpcEnv.shutdown()

      case _ =>
        // scalastyle:off println
        System.err.println("Usage: DriverWrapper <workerUrl> <userJar> <driverMainClass> [options]")
        // scalastyle:on println
        System.exit(-1)
    }
  }
}
