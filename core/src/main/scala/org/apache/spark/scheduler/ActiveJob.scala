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

import java.util.Properties

import org.apache.spark.util.CallSite

/**
 * A running job in the DAGScheduler. Jobs can be of two types: a result job, which computes a
 * ResultStage to execute an action, or a map-stage job, which computes the map outputs for a
 * ShuffleMapStage before any downstream stages are submitted. The latter is used for adaptive
 * query planning, to look at map output statistics before submitting later stages. We distinguish
 * between these two types of jobs using the finalStage field of this class.
 *
 * Jobs are only tracked for "leaf" stages that clients directly submitted, through DAGScheduler's
 * submitJob or submitMapStage methods. However, either type of job may cause the execution of
 * other earlier stages (for RDDs in the DAG it depends on), and multiple jobs may share some of
 * these previous stages. These dependencies are managed inside DAGScheduler.
  *
  *   在DAGScheduler中运行的作业。作业可以是两种类型:result结果作业，它计算一个ResultStage来执行一个action操作，
  * 或者一个map-stage作业，它在提交下游阶段计算前一个ShuffleMapStage的映射输出。后者用于自适应查询计划，在提交后续
  * 阶段之前查看map输出统计信息。我们使用这个类的finalStage字段来区分这两种类型的作业。
  *
  *   只有通过DAGScheduler的submitJob或submitMapStage方法，才能跟踪客户直接提交的“leaf”阶段。然而，任何一种job
  * 都可能导致其他早期stages的执行(DAG中的RDDs依赖于此)，而多个jobs作业可能共享其中的一些前阶段stages。这些依赖项
  * 在DAGScheduler内进行管理。
  *
  *
 *
 * @param jobId A unique ID for this job.
 * @param finalStage The stage that this job computes (either a ResultStage for an action or a
 *   ShuffleMapStage for submitMapStage).
 * @param callSite Where this job was initiated in the user's program (shown on UI).
 * @param listener A listener to notify if tasks in this job finish or the job fails.
 * @param properties Scheduling properties attached to the job, such as fair scheduler pool name.
 */
private[spark] class ActiveJob(
    val jobId: Int,
    val finalStage: Stage,
    val callSite: CallSite,
    val listener: JobListener,
    val properties: Properties) {

  /**
   * Number of partitions we need to compute for this job. Note that result stages may not need
   * to compute all partitions in their target RDD, for actions like first() and lookup().
    *
    * 这个作业需要计算的分区数。注意，结果阶段(result stages)可能不需要计算目标RDD中的所有分区，比如first()和lookup()。
   */
  val numPartitions = finalStage match {
    case r: ResultStage => r.partitions.length
    case m: ShuffleMapStage => m.rdd.partitions.length
  }

  /** Which partitions of the stage have finished
    * 哪个分区已经完成了
    *
    *    一个Boolean类型的数组，初始值为false
    * 数组长度为partitions个数，哪个partition被计算了，则对应的
    * 值标记为true
    *
    * */
  val finished = Array.fill[Boolean](numPartitions)(false)

  // 处理完成的partition个数
  var numFinished = 0
}
