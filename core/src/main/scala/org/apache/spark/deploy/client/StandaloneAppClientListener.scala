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

package org.apache.spark.deploy.client

/**
  * Callbacks invoked by deploy client when various events happen. There are currently four events:
  * connecting to the cluster, disconnecting, being given an executor, and having an executor
  * removed (either due to failure or due to revocation).
  *
  * 当各种事件发生的时候，回调函数被部署客户端调用。目前有四个事件：连接到群集、断开连接、被授予执行器、删除执行器（由于故障或由于撤销）。
  *
  * Users of this API should *not* block inside the callback methods.
  * 此API的用户不应该在回调方法中阻塞。
  */
private[spark] trait StandaloneAppClientListener {
  def connected(appId: String): Unit

  /** Disconnection may be a temporary state, as we fail over to a new Master.
    * 断线可能是一种暂时的状态，比如我们连接一个新的主节点。
    * */
  def disconnected(): Unit

  /** An application death is an unrecoverable failure condition.
    * 一个应用程序是一个不可恢复的故障条件下的死亡。
    * */
  def dead(reason: String): Unit

  def executorAdded(
                     fullId: String, workerId: String, hostPort: String, cores: Int, memory: Int): Unit

  def executorRemoved(
                       fullId: String, message: String, exitStatus: Option[Int], workerLost: Boolean): Unit
}
