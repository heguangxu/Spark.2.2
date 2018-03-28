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

import scala.collection.JavaConverters._

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.KeeperException

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
  * spark的监护人；管理者 工具类
  *
  * Spark并不是Zookeeper的原生API，而是Apache Curator。Curator在Zookeeper上做了一层友好的封装。
  *
  * Curator Framework极大地简化了Zookeeper的使用，它提供了height-level的API.并且给予Zookeeper添加了很特性；
  *   1.自动连接管理：链接到Zookeeper的Client有可能会链接终端，Curator处理了这种情况，对于Client来说自动连接是透明的。
  *   2.简洁的API：简化了原生态的Zookeeper的方法，事件等，提供了一个简单易用的接口。
  *   3.Recipe的实现
  *     3.1 leader选举
  *     3.2 共享锁
  *     3.3 缓存和监控
  *     3.4 分布式队列
  *     3.5 分布式优先队列
  */
private[spark] object SparkCuratorUtil extends Logging {

  // zookeeper的连接超时时间
  private val ZK_CONNECTION_TIMEOUT_MILLIS = 15000
  // zookeeper session的超时时间
  private val ZK_SESSION_TIMEOUT_MILLIS = 60000
  // 重试等待时间
  private val RETRY_WAIT_MILLIS = 5000
  // 最大的尝试连接时间
  private val MAX_RECONNECT_ATTEMPTS = 3

  def newClient(
      conf: SparkConf,
      zkUrlConf: String = "spark.deploy.zookeeper.url"): CuratorFramework = {
    val ZK_URL = conf.get(zkUrlConf)
    // 创建Zookeeper的实例
    val zk = CuratorFrameworkFactory.newClient(ZK_URL,
      ZK_SESSION_TIMEOUT_MILLIS, ZK_CONNECTION_TIMEOUT_MILLIS,
      new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS))
    // 启动Zookeeper
    zk.start()
    // 返回启动的Zookeeper
    zk
  }

  def mkdir(zk: CuratorFramework, path: String) {
    if (zk.checkExists().forPath(path) == null) {
      try {
        // 创建一个路径
        zk.create().creatingParentsIfNeeded().forPath(path)
      } catch {
        case nodeExist: KeeperException.NodeExistsException =>
          // do nothing, ignore node existing exception.
        case e: Exception => throw e
      }
    }
  }

  def deleteRecursive(zk: CuratorFramework, path: String) {
    if (zk.checkExists().forPath(path) != null) {
      for (child <- zk.getChildren.forPath(path).asScala) {
        // 删除子节点
        zk.delete().forPath(path + "/" + child)
      }
      // 删除一条数据（路径为path）
      zk.delete().forPath(path)
    }
  }
}
