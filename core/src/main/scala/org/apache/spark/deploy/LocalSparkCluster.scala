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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.deploy.master.Master
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util.Utils

/**
  * Testing class that creates a Spark standalone process in-cluster (that is, running the
  * spark.deploy.master.Master and spark.deploy.worker.Workers in the same JVMs). Executors launched
  * by the Workers still run in separate JVMs. This can be used to test distributed operation and
  * fault recovery without spinning up a lot of processes.
  *
  * 测试类,在集群中创建了一个Spark standalone线程。(也就是说,运行spark.deploy.master.Master和spark.deploy.worker.Workers
  * 在同一个jvm中）。由workers节点启动的Execuotrs仍然运行在分离的JVM中，这可以用来测试分布式操作和故障恢复不旋转的过程。
  *
  * local-cluster是一种伪集群部署模式，Driver,Master和Worker在同一个JVM进程内，可以存在多个Worker，每个Worker会有多个Executor,但这些Executor
  * 都独自存在于一个JVM进程内，除了这些，它与local部署，模式还有什么区别呢？
  *
  *
  *
  */
private[spark]
class LocalSparkCluster(
                         numWorkers: Int,
                         coresPerWorker: Int,
                         memoryPerWorker: Int,
                         conf: SparkConf)
  extends Logging {

  private val localHostname = Utils.localHostName()
  // 1.5版本中masterRpcEnvs貌似是masterActorSystems:用于缓存所有的Master的ActorSystem;
  private val masterRpcEnvs = ArrayBuffer[RpcEnv]()
  // 1.5版本中workerRpcEnvs貌似是workerActorSystems:维护所有的Worker的ActorSystem.
  private val workerRpcEnvs = ArrayBuffer[RpcEnv]()
  // exposed for testing   暴露出来仅仅用于测试
  var masterWebUIPort = -1

  // 启动
  def start(): Array[String] = {
    logInfo("Starting a local Spark cluster with " + numWorkers + " workers.")

    // Disable REST server on Master in this mode unless otherwise specified
    // 在此模式下禁用REST服务器，除非另有说明
    val _conf = conf.clone()
      .setIfMissing("spark.master.rest.enabled", "false")
      .set("spark.shuffle.service.enabled", "false")

    /** Start the Master  启动主节点*/
    val (rpcEnv, webUiPort, _) = Master.startRpcEnvAndEndpoint(localHostname, 0, 0, _conf)
    masterWebUIPort = webUiPort
    masterRpcEnvs += rpcEnv
    val masterUrl = "spark://" + Utils.localHostNameForURI() + ":" + rpcEnv.address.port
    val masters = Array(masterUrl)

    /* Start the Workers  启动Workers节点 */
    for (workerNum <- 1 to numWorkers) {
      // 启动worker节点相关信息
      val workerEnv = Worker.startRpcEnvAndEndpoint(localHostname, 0, 0, coresPerWorker,
        memoryPerWorker, masters, null, Some(workerNum), _conf)
      workerRpcEnvs += workerEnv
    }

    masters
  }


  // 停止
  def stop() {
    logInfo("Shutting down local Spark cluster.")
    // Stop the workers before the master so they don't get upset that it disconnected
    workerRpcEnvs.foreach(_.shutdown())
    masterRpcEnvs.foreach(_.shutdown())
    workerRpcEnvs.foreach(_.awaitTermination())
    masterRpcEnvs.foreach(_.awaitTermination())
    masterRpcEnvs.clear()
    workerRpcEnvs.clear()
  }
}
