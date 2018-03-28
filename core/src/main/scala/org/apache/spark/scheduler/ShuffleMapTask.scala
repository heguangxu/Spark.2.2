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

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
 * A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
 * specified in the ShuffleDependency).
  *
  * 一个ShuffleMapTask将一个RDD的元素划分为多个bucket(基于在ShuffleDependency中指定的一个分区器)。
 *
 * See [[org.apache.spark.scheduler.Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param taskBinary broadcast version of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    localProperties: Properties,
    serializedTaskMetrics: Array[Byte],
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None)
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, localProperties,
    serializedTaskMetrics, jobId, appId, appAttemptId)
  with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, 0, null, new Partition { override def index: Int = 0 }, null, new Properties, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  /**
    *
    * 运行的主要逻辑其实只有两步，如下：
        1、通过使用广播变量反序列化得到RDD和ShuffleDependency：
              1.1、获得反序列化的起始时间deserializeStartTime；
              1.2、通过SparkEnv获得反序列化器ser；
              1.3、调用反序列化器ser的deserialize()进行RDD和ShuffleDependency的反序列化，数据来源于taskBinary，得到rdd、dep；
              1.4、计算Executor进行反序列化的时间_executorDeserializeTime；
         2、利用shuffleManager的writer进行数据的写入：
               2.1、通过SparkEnv获得shuffleManager；
               2.2、通过shuffleManager的getWriter()方法，获得shuffle的writer，其中的partitionId表示的是当前RDD的某个partition,也就是说write操作作用于partition之上；
               2.3、针对RDD中的分区partition，调用rdd的iterator()方法后，再调用writer的write()方法，写数据；
               2.4、停止writer，并返回标志位。
    */
  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val threadMXBean = ManagementFactory.getThreadMXBean
    // 反序列化的起始时间
    val deserializeStartTime = System.currentTimeMillis()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L

    // 获得反序列化器closureSerializer
    val ser = SparkEnv.get.closureSerializer.newInstance()
    // 调用反序列化器closureSerializer的deserialize()进行RDD和ShuffleDependency的反序列化，数据来源于taskBinary
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    // 计算Executor进行反序列化的时间
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    var writer: ShuffleWriter[Any, Any] = null
    try {
      // 获得shuffleManager
      val manager = SparkEnv.get.shuffleManager
      // 通过shuffleManager的getWriter()方法，获得shuffle的writer
      // 启动的partitionId表示的是当前RDD的某个partition,也就是说write操作作用于partition之上
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      // 针对RDD中的分区<span style="font-family: Arial, Helvetica, sans-serif;">partition</span>
      // <span style="font-family: Arial, Helvetica, sans-serif;">，调用rdd的iterator()方法后，再调
      // 用writer的write()方法，写数据</span>
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      // 停止writer，并返回标志位
      writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
