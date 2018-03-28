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

package org.apache.spark

import org.apache.spark.scheduler.TaskSchedulerImpl

/**
 * Low-level status reporting APIs for monitoring job and stage progress.
  * 监视job和stage进度的低级别状态报告API。
 *
 * These APIs intentionally provide very weak consistency semantics; consumers of these APIs should
 * be prepared to handle empty / missing information.  For example, a job's stage ids may be known
 * but the status API may not have any information about the details of those stages, so
 * `getStageInfo` could potentially return `None` for a valid stage id.
 *
  * 这些API故意提供非常弱的一致性语义；这些API的消费者应该准备处理空/丢失的信息。例如，
  * 一个工作阶段ID可以知道但状态API可能没有关于这些阶段的细节的任何信息，所以` getstageinfo `
  * 有可能返回`没有一个有效的阶段` ID。
  *
 * To limit memory usage, these APIs only provide information on recent jobs / stages.  These APIs
 * will provide information for the last `spark.ui.retainedStages` stages and
 * `spark.ui.retainedJobs` jobs.
  *
  * 为了限制内存使用，这些API只提供关于最近的jobs/stages的信息。这些API将提供`spark.ui.retainedStages`最新的
  * stages和job的`spark.ui.retainedJobs`的信息
  *
 *
 * NOTE: this class's constructor should be considered private and may be subject to change.
  * 注意：这个类的构造函数应该被认为是私有的，可能会发生变化。
 */
class SparkStatusTracker private[spark] (sc: SparkContext) {

  private val jobProgressListener = sc.jobProgressListener

  /**
   * Return a list of all known jobs in a particular job group.  If `jobGroup` is `null`, then
   * returns all known jobs that are not associated with a job group.
    * 返回一个特别工作组的所有已知的job，如果`jobGroup`是null,则返回所有已知的没有假如job group
    * 的所有job
   *
   * The returned list may contain running, failed, and completed jobs, and may vary across
   * invocations of this method.  This method does not guarantee the order of the elements in
   * its result.
    * 返回的列表可以包含运行，失败，和已完成的工作，可能会有所不同此方法调用。这种方法不能保证
    * 其结果的元素的顺序。
   */
  def getJobIdsForGroup(jobGroup: String): Array[Int] = {
    jobProgressListener.synchronized {
      jobProgressListener.jobGroupToJobIds.getOrElse(jobGroup, Seq.empty).toArray
    }
  }

  /**
   * Returns an array containing the ids of all active stages.
   *  返回一个数组包含所有活动的stages
   * This method does not guarantee the order of the elements in its result.
    * 返回的结果不保证结果的顺序
   */
  def getActiveStageIds(): Array[Int] = {
    jobProgressListener.synchronized {
      jobProgressListener.activeStages.values.map(_.stageId).toArray
    }
  }

  /**
   * Returns an array containing the ids of all active jobs.
   * 返回一个数组包含所有活动的jobs
   * This method does not guarantee the order of the elements in its result.
    * 返回的结果不保证结果的顺序
   */
  def getActiveJobIds(): Array[Int] = {
    jobProgressListener.synchronized {
      jobProgressListener.activeJobs.values.map(_.jobId).toArray
    }
  }

  /**
   * Returns job information, or `None` if the job info could not be found or was garbage collected.
    * 返回job的信息，或者 null 如果job的信息不能够找到或者垃圾收集器里面没有
   */
  def getJobInfo(jobId: Int): Option[SparkJobInfo] = {
    jobProgressListener.synchronized {
      jobProgressListener.jobIdToData.get(jobId).map { data =>
        new SparkJobInfoImpl(jobId, data.stageIds.toArray, data.status)
      }
    }
  }

  /**
   * Returns stage information, or `None` if the stage info could not be found or was
   * garbage collected.
    *
    * 返回stage的信息，或者 null 如果stage的信息不能够找到或者垃圾收集器里面没有
    *
   */
  def getStageInfo(stageId: Int): Option[SparkStageInfo] = {
    jobProgressListener.synchronized {
      for (
        info <- jobProgressListener.stageIdToInfo.get(stageId);
        data <- jobProgressListener.stageIdToData.get((stageId, info.attemptId))
      ) yield {
        new SparkStageInfoImpl(
          stageId,
          info.attemptId,
          info.submissionTime.getOrElse(0),
          info.name,
          info.numTasks,
          data.numActiveTasks,
          data.numCompleteTasks,
          data.numFailedTasks)
      }
    }
  }

  /**
   * Returns information of all known executors, including host, port, cacheSize, numRunningTasks.
    * 返回所有已知的executors信息，包括host,port,cacheSize（缓存的大小）,numRunningTasks(正在运行的Task数量)
   */
  def getExecutorInfos: Array[SparkExecutorInfo] = {
    val executorIdToRunningTasks: Map[String, Int] =
      sc.taskScheduler.asInstanceOf[TaskSchedulerImpl].runningTasksByExecutors

    sc.getExecutorStorageStatus.map { status =>
      val bmId = status.blockManagerId
      new SparkExecutorInfoImpl(
        bmId.host,
        bmId.port,
        status.cacheSize,
        executorIdToRunningTasks.getOrElse(bmId.executorId, 0)
      )
    }
  }
}
