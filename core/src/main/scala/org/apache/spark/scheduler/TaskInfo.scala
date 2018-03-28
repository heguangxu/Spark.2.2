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

import org.apache.spark.TaskState
import org.apache.spark.TaskState.TaskState
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Information about a running task attempt inside a TaskSet.
 */
@DeveloperApi
class TaskInfo(
    val taskId: Long,
    /**
     * The index of this task within its task set. Not necessarily the same as the ID of the RDD
     * partition that the task is computing.
      *
      * 该任务在任务集内的索引，不一定与任务计算的RDD分区的ID相同。
     */
    val index: Int,
    val attemptNumber: Int,
    val launchTime: Long,
    val executorId: String,
    val host: String,
    val taskLocality: TaskLocality.TaskLocality,
    val speculative: Boolean) {

  /**
   * The time when the task started remotely getting the result. Will not be set if the
   * task result was sent immediately when the task finished (as opposed to sending an
   * IndirectTaskResult and later fetching the result from the block manager).
    *
    * 任务开始获取远程结果的时间。如果任务结果在任务完成时立即发送(而不是发送一个IndirectTaskResult并随后从块管理器获取结果)，将不会设置这个。
   */
  var gettingResultTime: Long = 0

  /**
   * Intermediate updates to accumulables during this task. Note that it is valid for the same
   * accumulable to be updated multiple times in a single task or for two accumulables with the
   * same name but different IDs to exist in a task.
    *
    * 在此任务期间，可累加的中间更新。请注意，在单个任务或两个具有相同名称但不同id在任务中存在的累加的情况下，相同的累计可以多次更新。
   */
  def accumulables: Seq[AccumulableInfo] = _accumulables

  private[this] var _accumulables: Seq[AccumulableInfo] = Nil

  private[spark] def setAccumulables(newAccumulables: Seq[AccumulableInfo]): Unit = {
    _accumulables = newAccumulables
  }

  /**
   * The time when the task has completed successfully (including the time to remotely fetch
   * results, if necessary).
    *
    * 任务成功完成的时间(包括在必要时远程获取结果的时间)。
   */
  var finishTime: Long = 0

  var failed = false

  var killed = false

  private[spark] def markGettingResult(time: Long) {
    gettingResultTime = time
  }

  private[spark] def markFinished(state: TaskState, time: Long) {
    // finishTime should be set larger than 0, otherwise "finished" below will return false.
    assert(time > 0)
    finishTime = time
    if (state == TaskState.FAILED) {
      failed = true
    } else if (state == TaskState.KILLED) {
      killed = true
    }
  }

  def gettingResult: Boolean = gettingResultTime != 0

  def finished: Boolean = finishTime != 0

  def successful: Boolean = finished && !failed && !killed

  def running: Boolean = !finished

  def status: String = {
    if (running) {
      if (gettingResult) {
        "GET RESULT"
      } else {
        "RUNNING"
      }
    } else if (failed) {
      "FAILED"
    } else if (killed) {
      "KILLED"
    } else if (successful) {
      "SUCCESS"
    } else {
      "UNKNOWN"
    }
  }

  def id: String = s"$index.$attemptNumber"

  def duration: Long = {
    if (!finished) {
      throw new UnsupportedOperationException("duration() called on unfinished task")
    } else {
      finishTime - launchTime
    }
  }

  private[spark] def timeRunning(currentTime: Long): Long = currentTime - launchTime
}
