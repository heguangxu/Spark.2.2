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

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.internal.Logging
// zookeeper领导者的选举代理
private[master] class ZooKeeperLeaderElectionAgent(val masterInstance: LeaderElectable,
    conf: SparkConf) extends LeaderLatchListener with LeaderElectionAgent with Logging  {

  // zookeeper的工作目录
  val WORKING_DIR = conf.get("spark.deploy.zookeeper.dir", "/spark") + "/leader_election"

  private var zk: CuratorFramework = _
  private var leaderLatch: LeaderLatch = _
  private var status = LeadershipStatus.NOT_LEADER

  start()

  private def start() {
    logInfo("Starting ZooKeeper LeaderElection agent")
    // 在创建ZooKeeperLeaderElectionAgent的时候，会创建Zookeeper的实例，同时创建LeaderLatch;
    zk = SparkCuratorUtil.newClient(conf)
    leaderLatch = new LeaderLatch(zk, WORKING_DIR)
    leaderLatch.addListener(this)
    // 调用这个方法的时候，就启动了Leader的竞争与选举。主要逻辑在isleader和notLeader中
    // 状态变化时候，leaderLatch会调用Listener的这两个接口。
    leaderLatch.start()
  }

  override def stop() {
    leaderLatch.close()
    zk.close()
  }

  /**
    * 当节点被选举为Leader的时候，会调用这个
    */
  override def isLeader() {
    synchronized {
      // could have lost leadership by now.
      // 接口实现的时候，需要再次确认状态是否再次改变了。有可能状态已经再次改变，即Leader已经再次变化，因此需要再次确认。
      if (!leaderLatch.hasLeadership) {
        return
      }
      // 已经被选举为Leader
      logInfo("We have gained leadership")
      updateLeadershipStatus(true)
    }
  }

  /**
    * 当节点被剥夺Leader的时候，会调用这个
    */
  override def notLeader() {
    synchronized {
      // could have gained leadership by now.
      // 接口实现的时候，需要再次确认状态是否再次改变了。有可能状态已经再次改变，即Leader已经再次变化，因此需要再次确认。
      if (leaderLatch.hasLeadership) {
        return
      }

      // 已经剥夺leader的权限
      logInfo("We have lost leadership")
      updateLeadershipStatus(false)
    }
  }

  /**
    * 逻辑：主要是向master发送消息
    * @param isLeader
    */
  private def updateLeadershipStatus(isLeader: Boolean) {
    if (isLeader && status == LeadershipStatus.NOT_LEADER) {
      status = LeadershipStatus.LEADER
      // master已经被选举为Leader
      masterInstance.electedLeader() //通过此masterActor向master发送消息
    } else if (!isLeader && status == LeadershipStatus.LEADER) {
      status = LeadershipStatus.NOT_LEADER
      // master已经剥夺Leader
      masterInstance.revokedLeadership()
    }
  }

  private object LeadershipStatus extends Enumeration {
    type LeadershipStatus = Value
    val LEADER, NOT_LEADER = Value
  }
}
