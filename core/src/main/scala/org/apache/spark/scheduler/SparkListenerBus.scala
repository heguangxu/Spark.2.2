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

import org.apache.spark.util.ListenerBus

/**
 * A [[SparkListenerEvent]] bus that relays [[SparkListenerEvent]]s to its listeners
 */
private[spark] trait SparkListenerBus
  extends ListenerBus[SparkListenerInterface, SparkListenerEvent] {

  // 真正的消息路由是由SparkListenerBus的onPostEvent函数完成的
  protected override def doPostEvent(
      listener: SparkListenerInterface,
      event: SparkListenerEvent): Unit = {
    event match {
      // 处理向listenerBus提交StageSubmitted事件，事件封装成SparkListenerStageSubmitted
      case stageSubmitted: SparkListenerStageSubmitted =>
        // 这里调用的是SparkListenerInterface接口，这里不知道调用哪个方法了？求大神指教
        listener.onStageSubmitted(stageSubmitted)

      case stageCompleted: SparkListenerStageCompleted =>
        listener.onStageCompleted(stageCompleted)
      case jobStart: SparkListenerJobStart =>
        listener.onJobStart(jobStart)
      case jobEnd: SparkListenerJobEnd =>
        listener.onJobEnd(jobEnd)
      case taskStart: SparkListenerTaskStart =>
        listener.onTaskStart(taskStart)

        // 由DAGSchedule的handleGetTaskResult(taskInfo: TaskInfo)方法调用
      case taskGettingResult: SparkListenerTaskGettingResult =>
        // SparkListenerInterface接口的onTaskGettingResult---》SparkListener.onTaskGettingResult
        // 继承的SparkListenerInterface--》
        listener.onTaskGettingResult(taskGettingResult)

      case taskEnd: SparkListenerTaskEnd =>
        listener.onTaskEnd(taskEnd)
      case environmentUpdate: SparkListenerEnvironmentUpdate =>
        listener.onEnvironmentUpdate(environmentUpdate)
      case blockManagerAdded: SparkListenerBlockManagerAdded =>
        listener.onBlockManagerAdded(blockManagerAdded)
      case blockManagerRemoved: SparkListenerBlockManagerRemoved =>
        listener.onBlockManagerRemoved(blockManagerRemoved)
        // 被SparkContext中调用 ---》 ？
      case unpersistRDD: SparkListenerUnpersistRDD =>
        listener.onUnpersistRDD(unpersistRDD)
      case applicationStart: SparkListenerApplicationStart =>
        listener.onApplicationStart(applicationStart)
      case applicationEnd: SparkListenerApplicationEnd =>
        listener.onApplicationEnd(applicationEnd)

        //更新metrics事件
      case metricsUpdate: SparkListenerExecutorMetricsUpdate =>
        listener.onExecutorMetricsUpdate(metricsUpdate)

      // excutor添加事件
      case executorAdded: SparkListenerExecutorAdded =>
        // 这里是谁处理呢？
        listener.onExecutorAdded(executorAdded)

      case executorRemoved: SparkListenerExecutorRemoved =>
        listener.onExecutorRemoved(executorRemoved)
      case executorBlacklisted: SparkListenerExecutorBlacklisted =>
        listener.onExecutorBlacklisted(executorBlacklisted)
      case executorUnblacklisted: SparkListenerExecutorUnblacklisted =>
        listener.onExecutorUnblacklisted(executorUnblacklisted)
      case nodeBlacklisted: SparkListenerNodeBlacklisted =>
        listener.onNodeBlacklisted(nodeBlacklisted)
      case nodeUnblacklisted: SparkListenerNodeUnblacklisted =>
        listener.onNodeUnblacklisted(nodeUnblacklisted)
      case blockUpdated: SparkListenerBlockUpdated =>
        listener.onBlockUpdated(blockUpdated)
      case logStart: SparkListenerLogStart => // ignore event log metadata
      case _ => listener.onOtherEvent(event)
    }
  }

}
