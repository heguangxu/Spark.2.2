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

/**
 * Interface used to listen for job completion or failure events after submitting a job to the
 * DAGScheduler. The listener is notified each time a task succeeds, as well as if the whole
 * job fails (and no further taskSucceeded events will happen).
  *
  * 接口用于在向DAGScheduler提交作业后侦听作业完成或故障事件。每当任务成功时，侦听器就会被通知，并且如果整个作业失败
  * (并且没有进一步的任务成功事件taskSucceeded将会发生)。
 */
private[spark] trait JobListener {
  //对Task执行结果进行处理的核心逻辑
  def taskSucceeded(index: Int, result: Any): Unit
  //对Task执行失败进行处理的而核心逻辑。
  def jobFailed(exception: Exception): Unit
}
