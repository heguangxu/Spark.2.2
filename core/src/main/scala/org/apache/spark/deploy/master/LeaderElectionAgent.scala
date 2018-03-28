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

package org.apache.spark.deploy.master

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 *
 * A LeaderElectionAgent tracks current master and is a common interface for all election Agents.
  * 选举代理人的主要接口（特质）
  *
  * 领导选举代理：
  *   领导选举机制（leader Election）可以保证集群虽然存在多个master，但是只有一个Master处于激活（active）状态，其他的master处于
  *  支持（Standby）状态。当Active状态的Master出现故障时，会选举出一个Standby状态的Master作为新的Active状态的Master，由于整个
  *  集群的Worker，Driver和Application的信息都已经持久化到文件系统，因此切换时只会影响新任务的提交，对于正在运行中的任务没有任何影响。
  *  Spark目前提供的两道选举代理有两种：
  *     1.ZookeeperLeaderElectionAgent:对Zookeeper提供的选举机制的代理；
  *     2.MonarchyLeaderAgent:默认的选举机制代理。
  *
 */
@DeveloperApi
trait LeaderElectionAgent {
  val masterInstance: LeaderElectable
  def stop() {} // to avoid noops in implementations.
}

@DeveloperApi
trait LeaderElectable {
  def electedLeader(): Unit
  def revokedLeadership(): Unit
}

/** Single-node implementation of LeaderElectionAgent -- we're initially and always the leader.
  * 领导选举代理的单节点实现——我们一开始就是领导者。
  * */
private[spark] class MonarchyLeaderAgent(val masterInstance: LeaderElectable)
  extends LeaderElectionAgent {
  // local-cluster模式：传入的是this，即master，直接参与选举 =====》 调用的是master.electedLeader()方法
  masterInstance.electedLeader()
}
