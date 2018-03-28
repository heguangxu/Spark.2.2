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

package org.apache.spark.broadcast

import scala.reflect.ClassTag

import org.apache.spark.SecurityManager
import org.apache.spark.SparkConf

/**
  * An interface for all the broadcast implementations in Spark (to allow
  * multiple broadcast implementations). SparkContext uses a BroadcastFactory
  * implementation to instantiate a particular broadcast for the entire Spark job.
  *
  * 用于Spark中所有广播实现的接口（允许多个广播实现）。sparkcontext使用broadcastfactory为Spark job实现实例化一个特定的广播。
  *
  */
private[spark] trait BroadcastFactory {

  def initialize(isDriver: Boolean, conf: SparkConf, securityMgr: SecurityManager): Unit

  /**
    * Creates a new broadcast variable.   创建一个新的广播变量
    *
    * @param value value to broadcast     广播变量的值
    * @param isLocal whether we are in local mode (single JVM process)
    *                是否是本地模式（一个JVM进程内）
    * @param id unique id representing this broadcast variable
    *           广播变量独一无二的ID
    */
  def newBroadcast[T: ClassTag](value: T, isLocal: Boolean, id: Long): Broadcast[T]

  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit

  def stop(): Unit
}
