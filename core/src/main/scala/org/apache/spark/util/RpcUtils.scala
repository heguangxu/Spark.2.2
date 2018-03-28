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

package org.apache.spark.util

import org.apache.spark.SparkConf
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcTimeout}

private[spark] object RpcUtils {

  /**
   * Retrieve a `RpcEndpointRef` which is located in the driver via its name.
    * 返回一个RpcEndpointRef
   */
  def makeDriverRef(name: String, conf: SparkConf, rpcEnv: RpcEnv): RpcEndpointRef = {
    val driverHost: String = conf.get("spark.driver.host", "localhost")
    val driverPort: Int = conf.getInt("spark.driver.port", 7077)
    Utils.checkHost(driverHost, "Expected hostname")
    rpcEnv.setupEndpointRef(RpcAddress(driverHost, driverPort), name)
  }

  /** Returns the configured number of times to retry connecting
    * 返回已配置的次数，以重试连接
    *
    * 当通信失败时会进行一定次数的重试，可以使用spark.rpc.numRetries属性设置重试次数，默认是三次：
    * */
  def numRetries(conf: SparkConf): Int = {
    conf.getInt("spark.rpc.numRetries", 3)
  }

  /** Returns the configured number of milliseconds to wait on each retry
    * 返回所配置的毫秒数，每次重试等待的毫秒数
    *
    * retryWaitMs代表每次重试需要间隔的时间，默认是3秒
    * */
  def retryWaitMs(conf: SparkConf): Long = {
    conf.getTimeAsMs("spark.rpc.retry.wait", "3s")
  }

  /** Returns the default Spark timeout to use for RPC ask operations.
    *
    * 请求超时的时间默认是120秒
    * */
  def askRpcTimeout(conf: SparkConf): RpcTimeout = {
    RpcTimeout(conf, Seq("spark.rpc.askTimeout", "spark.network.timeout"), "120s")
  }

  /** Returns the default Spark timeout to use for RPC remote endpoint lookup. */
  def lookupRpcTimeout(conf: SparkConf): RpcTimeout = {
    RpcTimeout(conf, Seq("spark.rpc.lookupTimeout", "spark.network.timeout"), "120s")
  }

  private val MAX_MESSAGE_SIZE_IN_MB = Int.MaxValue / 1024 / 1024

  /** Returns the configured max message size for messages in bytes. */
  def maxMessageSizeBytes(conf: SparkConf): Int = {
    val maxSizeInMB = conf.getInt("spark.rpc.message.maxSize", 128)
    if (maxSizeInMB > MAX_MESSAGE_SIZE_IN_MB) {
      throw new IllegalArgumentException(
        s"spark.rpc.message.maxSize should not be greater than $MAX_MESSAGE_SIZE_IN_MB MB")
    }
    maxSizeInMB * 1024 * 1024
  }
}
