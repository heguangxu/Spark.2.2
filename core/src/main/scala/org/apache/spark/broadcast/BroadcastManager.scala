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

import java.util.concurrent.atomic.AtomicLong

import scala.reflect.ClassTag

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.broadcast.HttpBroadcastFactory;
/*
    BroadcastManager用于将配置信息和序列化后的RDD,job以及ShuffleDependency等信息在本地存储。 如果为了容灾，也会复制到其他节点上。

    BroadcastManager必须在其初始化方法initialize被调用之后，才能省自傲。initialize方法实际利用反射生成关闭工厂实例
    broadcastFactory（可以配置属性spark.broadcast.factory值定，默认为org.apache.spark.broadcast.TorrentbroadcastFactory）.
    BroadcastManager的广播方法newBroadcast实际代理了工厂broadcastFactory的newBroadcast方法来生成广播对象。unbroadcast方法实际
    代理了工厂broadcastFactory的unbroadcast方法生成非广播对象。
 */
private[spark] class BroadcastManager(
   val isDriver: Boolean,
   conf: SparkConf,
   securityManager: SecurityManager)
  extends Logging {

  private var initialized = false
  private var broadcastFactory: BroadcastFactory = null

  initialize()

  // Called by SparkContext or Executor before using Broadcast
  // 在使用广播变量之前被SparkContext 或者 Executor 调用
  private def initialize() {
    // 同步
    synchronized {
      // 如果没有初始化广播变量工厂，就初始化，广播变量工厂
      if (!initialized) {
        broadcastFactory = new TorrentBroadcastFactory
        broadcastFactory.initialize(isDriver, conf, securityManager)
        initialized = true
      }
    }
  }

  // 停止广播变量工厂
  def stop() {
    broadcastFactory.stop()
  }

  private val nextBroadcastId = new AtomicLong(0)


  def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean): Broadcast[T] = {
    // 这里直接调用特质BroadcastFactory实际上调用的是TorrentBroadcastFactory.newBroadcast()
    broadcastFactory.newBroadcast[T](value_, isLocal, nextBroadcastId.getAndIncrement())
  }

  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
    broadcastFactory.unbroadcast(id, removeFromDriver, blocking)
  }
}
