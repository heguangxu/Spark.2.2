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

package org.apache.spark.api.java

import org.apache.spark.{SparkContext, SparkJobInfo, SparkStageInfo}

/**
  * Low-level status reporting APIs for monitoring job and stage progress.
  *  spark的状态栈   低级别状态报告的API监测工作的阶段性进展。
  *
  * These APIs intentionally provide very weak consistency semantics; consumers of these APIs should
  * be prepared to handle empty / missing information.  For example, a job's stage ids may be known
  * but the status API may not have any information about the details of those stages, so
  * `getStageInfo` could potentially return `null` for a valid stage id.
  *
  * 这些API故意提供非常弱的一致性语义；这些API的消费者应该准备处理空/丢失的信息。例如，
  * 一个工作阶段job's stage的ID可以知道，但状态API可能没有关于这些阶段的细节的任何信息，所以使用`getStageInfo`
  * 去查询一个有效的stage id有可能返回`空`
  *
  *
  * To limit memory usage, these APIs only provide information on recent jobs / stages.  These APIs
  * will provide information for the last `spark.ui.retainedStages` stages and
  * `spark.ui.retainedJobs` jobs.
  *
  * 为了限制内存的使用，这些API只提供最近的jobs / stages的信息。这些API将为`spark.ui.retainedStages` stages 和
  * `spark.ui.retainedJobs` jobs.提供最新的信息。
  *
  * @note This class's constructor should be considered private and may be subject to change.
  *       这个类的构造函数应该是私有的，可能会有变化。
  */
class JavaSparkStatusTracker private[spark] (sc: SparkContext) {

  /**
    * Return a list of all known jobs in a particular job group.  If `jobGroup` is `null`, then
    * returns all known jobs that are not associated with a job group.
    *
    * 返回一个特定工作组的所有已知的jobs的列表。如果`jobGroup`是`null`，然后返回所有已知的job而别不与工作组相关的。
    * （还没有加入工作组的job）
    *
    * The returned list may contain running, failed, and completed jobs, and may vary across
    * invocations of this method.  This method does not guarantee the order of the elements in
    * its result.
    *
    * 返回的列表可以包含运行，失败，和已完成的工作，并且可以直接调用该方法。这种方法不能保证其结果的元素的有序性。
    *
    */
  def getJobIdsForGroup(jobGroup: String): Array[Int] = sc.statusTracker.getJobIdsForGroup(jobGroup)

  /**
    * Returns an array containing the ids of all active stages.
    *  返回一个数组包含所有活动stages的ids。
    * This method does not guarantee the order of the elements in its result.
    * 这种方法不能保证其结果的元素的有序性。
    */
  def getActiveStageIds(): Array[Int] = sc.statusTracker.getActiveStageIds()

  /**
    * Returns an array containing the ids of all active jobs.
    * 返回一个数组包含所有活动jobs的ids。
    * This method does not guarantee the order of the elements in its result.
    * 这种方法不能保证其结果的元素的有序性。
    */
  def getActiveJobIds(): Array[Int] = sc.statusTracker.getActiveJobIds()

  /**
    * Returns job information, or `null` if the job info could not be found or was garbage collected.
    * 如果找不到job的信息或job被垃圾收集，返回job信息或“null”。
    */
  def getJobInfo(jobId: Int): SparkJobInfo = sc.statusTracker.getJobInfo(jobId).orNull

  /**
    * Returns stage information, or `null` if the stage info could not be found or was
    * garbage collected.
    * 如果找不到stage的信息或stage被垃圾收集，返回stage信息或“null”。
    */
  def getStageInfo(stageId: Int): SparkStageInfo = sc.statusTracker.getStageInfo(stageId).orNull
}
