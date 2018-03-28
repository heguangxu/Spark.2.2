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

package org.apache.spark.scheduler

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * An interface for schedulable entities.
 * there are two type of Schedulable entities(Pools and TaskSetManagers)
  *
  * 一个schedulable实体接口，这里有两种schedulable实体（pools和TaskSetManagers）
 */
private[spark] trait Schedulable {
  var parent: Pool
  // child queues
  def schedulableQueue: ConcurrentLinkedQueue[Schedulable]
  def schedulingMode: SchedulingMode

  //pool的weight和minShare是通过配置文件配置的，TaskSetManager的weight和minShare为1和0
  def weight: Int
  def minShare: Int
  def runningTasks: Int
  def priority: Int
  def stageId: Int
  def name: String

  //通过addSchedulable把TaskSetManager挂载到不同的pool里面
  def addSchedulable(schedulable: Schedulable): Unit
  def removeSchedulable(schedulable: Schedulable): Unit
  def getSchedulableByName(name: String): Schedulable
  def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit
  def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean

  // pool返回的是排序的TaskSetManager，TaskSetManager返回的是自己本身
  def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager]
}
