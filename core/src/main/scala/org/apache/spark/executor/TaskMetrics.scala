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

package org.apache.spark.executor

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.storage.{BlockId, BlockStatus}
import org.apache.spark.util._


/**
 * :: DeveloperApi ::
 * Metrics tracked during the execution of a task.        在执行任务期间跟踪的指标。
 *
 * This class is wrapper around a collection of internal accumulators that represent metrics
 * associated with a task. The local values of these accumulators are sent from the executor
 * to the driver when the task completes. These values are then merged into the corresponding
 * accumulator previously registered on the driver.
  *
  * 这个类封装了一个内部累加器集合，它表示与一个任务相关的指标。当任务完成时，这些累加器的本地值将从执行器发送给驱动程序。
  * 然后将这些值合并到相应的值中之前在驱动程序上注册的累加器。
 *
 * The accumulator updates are also sent to the driver periodically (on executor heartbeat)
 * and when the task failed with an exception. The [[TaskMetrics]] object itself should never
 * be sent to the driver.
  *
  * 累加器更新也会定期发送给驱动程序(在执行器的心跳上)，当任务失败时，会有一个异常。[[TaskMetrics]]对象本身不应该被发送给驱动程序。
 */
@DeveloperApi
class TaskMetrics private[spark] () extends Serializable {
  // Each metric is internally represented as an accumulator          每个指标都在内部表示为累加器
  private val _executorDeserializeTime = new LongAccumulator
  private val _executorDeserializeCpuTime = new LongAccumulator
  private val _executorRunTime = new LongAccumulator
  private val _executorCpuTime = new LongAccumulator
  private val _resultSize = new LongAccumulator
  private val _jvmGCTime = new LongAccumulator
  private val _resultSerializationTime = new LongAccumulator
  private val _memoryBytesSpilled = new LongAccumulator
  private val _diskBytesSpilled = new LongAccumulator
  private val _peakExecutionMemory = new LongAccumulator
  private val _updatedBlockStatuses = new CollectionAccumulator[(BlockId, BlockStatus)]

  /**
   * Time taken on the executor to deserialize this task.       对执行程序进行反序列化的时间。
   */
  def executorDeserializeTime: Long = _executorDeserializeTime.sum

  /**
   * CPU Time taken on the executor to deserialize this task in nanoseconds.      执行程序在纳秒内将此任务反序列化的CPU时间。
   */
  def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime.sum

  /**
   * Time the executor spends actually running the task (including fetching shuffle data).
    * 执行器实际运行任务的时间(包括获取洗牌数据)。
   */
  def executorRunTime: Long = _executorRunTime.sum

  /**
   * CPU Time the executor spends actually running the task
   * (including fetching shuffle data) in nanoseconds.
    * 执行程序在纳秒内实际运行任务(包括获取洗牌数据)。
   */
  def executorCpuTime: Long = _executorCpuTime.sum

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult.
    * 这个任务向驱动程序传输任务结果的字节数。
   */
  def resultSize: Long = _resultSize.sum

  /**
   * Amount of time the JVM spent in garbage collection while executing this task.
    * 执行此任务时JVM在垃圾收集中花费的时间。
   */
  def jvmGCTime: Long = _jvmGCTime.sum

  /**
   * Amount of time spent serializing the task result.
    * 用于序列化任务结果的时间量。
   */
  def resultSerializationTime: Long = _resultSerializationTime.sum

  /**
   * The number of in-memory bytes spilled by this task.
    * 由该任务溢出的内存字节数。
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled.sum

  /**
   * The number of on-disk bytes spilled by this task.
    * 由这个任务溢出的磁盘字节数。
   */
  def diskBytesSpilled: Long = _diskBytesSpilled.sum

  /**
   * Peak memory used by internal data structures created during shuffles, aggregations and
   * joins. The value of this accumulator should be approximately the sum of the peak sizes
   * across all such data structures created in this task. For SQL jobs, this only tracks all
   * unsafe operators and ExternalSort.
    *
    * 在shuffles、聚合aggregations和连接joins期间创建的内部数据结构使用的峰值内存。
    * 这个累加器的值应该近似于这个任务中创建的所有数据结构的峰值大小之和。对于SQL作业，
    * 它只跟踪所有不安全的操作符和外部排序。
   */
  def peakExecutionMemory: Long = _peakExecutionMemory.sum

  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
    * 由于此任务而更新的任何块的存储状态。
   */
  def updatedBlockStatuses: Seq[(BlockId, BlockStatus)] = {
    // This is called on driver. All accumulator updates have a fixed value. So it's safe to use
    // `asScala` which accesses the internal values using `java.util.Iterator`.
    // 这被驱动程序driver调用。所有的累计更新都有一个固定值。所以使用“asScala”是安全的，它使用`java.util.Iterator`来访问内部值。
    _updatedBlockStatuses.value.asScala
  }

  // Setters and increment-ers
  private[spark] def setExecutorDeserializeTime(v: Long): Unit =
    _executorDeserializeTime.setValue(v)
  private[spark] def setExecutorDeserializeCpuTime(v: Long): Unit =
    _executorDeserializeCpuTime.setValue(v)
  private[spark] def setExecutorRunTime(v: Long): Unit = _executorRunTime.setValue(v)
  private[spark] def setExecutorCpuTime(v: Long): Unit = _executorCpuTime.setValue(v)
  private[spark] def setResultSize(v: Long): Unit = _resultSize.setValue(v)
  private[spark] def setJvmGCTime(v: Long): Unit = _jvmGCTime.setValue(v)
  private[spark] def setResultSerializationTime(v: Long): Unit =
    _resultSerializationTime.setValue(v)
  private[spark] def incMemoryBytesSpilled(v: Long): Unit = _memoryBytesSpilled.add(v)
  private[spark] def incDiskBytesSpilled(v: Long): Unit = _diskBytesSpilled.add(v)
  private[spark] def incPeakExecutionMemory(v: Long): Unit = _peakExecutionMemory.add(v)
  private[spark] def incUpdatedBlockStatuses(v: (BlockId, BlockStatus)): Unit =
    _updatedBlockStatuses.add(v)
  private[spark] def setUpdatedBlockStatuses(v: java.util.List[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.setValue(v)
  private[spark] def setUpdatedBlockStatuses(v: Seq[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.setValue(v.asJava)

  /**
   * Metrics related to reading data from a [[org.apache.spark.rdd.HadoopRDD]] or from persisted
   * data, defined only in tasks with input.
    *
    * 从一个[[org.apache.spark.rdd.HadoopRDD]] 读取数据的相关指标。或来自持久数据，只在有输入的任务中定义。
    *
   */
  val inputMetrics: InputMetrics = new InputMetrics()

  /**
   * Metrics related to writing data externally (e.g. to a distributed filesystem),
   * defined only in tasks with output.
    *
    * 与在外部(例如在分布式文件系统中)编写数据相关的指标，只在具有输出的任务中定义。
   */
  val outputMetrics: OutputMetrics = new OutputMetrics()

  /**
   * Metrics related to shuffle read aggregated across all shuffle dependencies.
   * This is defined only if there are shuffle dependencies in this task.
    *
    * 与shuffle相关的指标在所有洗牌依赖项之间进行聚合。只有在此任务中存在洗牌依赖项时才定义此功能。
   */
  val shuffleReadMetrics: ShuffleReadMetrics = new ShuffleReadMetrics()

  /**
   * Metrics related to shuffle write, defined only in shuffle map stages.
    * 与shuffle写入相关的度量，仅在shuffle map阶段中定义。
   */
  val shuffleWriteMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics()

  /**
   * A list of [[TempShuffleReadMetrics]], one per shuffle dependency.
   *
   * A task may have multiple shuffle readers for multiple dependencies. To avoid synchronization
   * issues from readers in different threads, in-progress tasks use a [[TempShuffleReadMetrics]]
   * for each dependency and merge these metrics before reporting them to the driver.
   */
  @transient private lazy val tempShuffleReadMetrics = new ArrayBuffer[TempShuffleReadMetrics]

  /**
   * Create a [[TempShuffleReadMetrics]] for a particular shuffle dependency.
   *
   * All usages are expected to be followed by a call to [[mergeShuffleReadMetrics]], which
   * merges the temporary values synchronously. Otherwise, all temporary data collected will
   * be lost.
   */
  private[spark] def createTempShuffleReadMetrics(): TempShuffleReadMetrics = synchronized {
    val readMetrics = new TempShuffleReadMetrics
    tempShuffleReadMetrics += readMetrics
    readMetrics
  }

  /**
   * Merge values across all temporary [[ShuffleReadMetrics]] into `_shuffleReadMetrics`.
   * This is expected to be called on executor heartbeat and at the end of a task.
    *
    * 将所有临时[[ShuffleReadMetrics]]中的值合并到' _shuffleReadMetrics '中。
    *  这将在执行器心跳和任务结束时调用。
   */
  private[spark] def mergeShuffleReadMetrics(): Unit = synchronized {
    if (tempShuffleReadMetrics.nonEmpty) {
      shuffleReadMetrics.setMergeValues(tempShuffleReadMetrics)
    }
  }

  // Only used for test
  private[spark] val testAccum = sys.props.get("spark.testing").map(_ => new LongAccumulator)


  import InternalAccumulator._
  @transient private[spark] lazy val nameToAccums = LinkedHashMap(
    EXECUTOR_DESERIALIZE_TIME -> _executorDeserializeTime,
    EXECUTOR_DESERIALIZE_CPU_TIME -> _executorDeserializeCpuTime,
    EXECUTOR_RUN_TIME -> _executorRunTime,
    EXECUTOR_CPU_TIME -> _executorCpuTime,
    RESULT_SIZE -> _resultSize,
    JVM_GC_TIME -> _jvmGCTime,
    RESULT_SERIALIZATION_TIME -> _resultSerializationTime,
    MEMORY_BYTES_SPILLED -> _memoryBytesSpilled,
    DISK_BYTES_SPILLED -> _diskBytesSpilled,
    PEAK_EXECUTION_MEMORY -> _peakExecutionMemory,
    UPDATED_BLOCK_STATUSES -> _updatedBlockStatuses,
    shuffleRead.REMOTE_BLOCKS_FETCHED -> shuffleReadMetrics._remoteBlocksFetched,
    shuffleRead.LOCAL_BLOCKS_FETCHED -> shuffleReadMetrics._localBlocksFetched,
    shuffleRead.REMOTE_BYTES_READ -> shuffleReadMetrics._remoteBytesRead,
    shuffleRead.LOCAL_BYTES_READ -> shuffleReadMetrics._localBytesRead,
    shuffleRead.FETCH_WAIT_TIME -> shuffleReadMetrics._fetchWaitTime,
    shuffleRead.RECORDS_READ -> shuffleReadMetrics._recordsRead,
    shuffleWrite.BYTES_WRITTEN -> shuffleWriteMetrics._bytesWritten,
    shuffleWrite.RECORDS_WRITTEN -> shuffleWriteMetrics._recordsWritten,
    shuffleWrite.WRITE_TIME -> shuffleWriteMetrics._writeTime,
    input.BYTES_READ -> inputMetrics._bytesRead,
    input.RECORDS_READ -> inputMetrics._recordsRead,
    output.BYTES_WRITTEN -> outputMetrics._bytesWritten,
    output.RECORDS_WRITTEN -> outputMetrics._recordsWritten
  ) ++ testAccum.map(TEST_ACCUM -> _)

  @transient private[spark] lazy val internalAccums: Seq[AccumulatorV2[_, _]] =
    nameToAccums.values.toIndexedSeq

  /* ========================== *
   |        OTHER THINGS        |
   * ========================== */

  private[spark] def register(sc: SparkContext): Unit = {
    nameToAccums.foreach {
      case (name, acc) => acc.register(sc, name = Some(name), countFailedValues = true)
    }
  }

  /**
   * External accumulators registered with this task.
   */
  @transient private[spark] lazy val externalAccums = new ArrayBuffer[AccumulatorV2[_, _]]

  private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
    externalAccums += a
  }

  private[spark] def accumulators(): Seq[AccumulatorV2[_, _]] = internalAccums ++ externalAccums

  private[spark] def nonZeroInternalAccums(): Seq[AccumulatorV2[_, _]] = {
    // RESULT_SIZE accumulator is always zero at executor, we need to send it back as its
    // value will be updated at driver side.
    internalAccums.filter(a => !a.isZero || a == _resultSize)
  }
}


private[spark] object TaskMetrics extends Logging {
  import InternalAccumulator._

  /**
   * Create an empty task metrics that doesn't register its accumulators.
   */
  def empty: TaskMetrics = {
    val tm = new TaskMetrics
    tm.nameToAccums.foreach { case (name, acc) =>
      acc.metadata = AccumulatorMetadata(AccumulatorContext.newId(), Some(name), true)
    }
    tm
  }

  def registered: TaskMetrics = {
    val tm = empty
    tm.internalAccums.foreach(AccumulatorContext.register)
    tm
  }

  /**
   * Construct a [[TaskMetrics]] object from a list of [[AccumulableInfo]], called on driver only.
   * The returned [[TaskMetrics]] is only used to get some internal metrics, we don't need to take
   * care of external accumulator info passed in.
   */
  def fromAccumulatorInfos(infos: Seq[AccumulableInfo]): TaskMetrics = {
    val tm = new TaskMetrics
    infos.filter(info => info.name.isDefined && info.update.isDefined).foreach { info =>
      val name = info.name.get
      val value = info.update.get
      if (name == UPDATED_BLOCK_STATUSES) {
        tm.setUpdatedBlockStatuses(value.asInstanceOf[java.util.List[(BlockId, BlockStatus)]])
      } else {
        tm.nameToAccums.get(name).foreach(
          _.asInstanceOf[LongAccumulator].setValue(value.asInstanceOf[Long])
        )
      }
    }
    tm
  }

  /**
   * Construct a [[TaskMetrics]] object from a list of accumulator updates, called on driver only.
   */
  def fromAccumulators(accums: Seq[AccumulatorV2[_, _]]): TaskMetrics = {
    val tm = new TaskMetrics
    for (acc <- accums) {
      val name = acc.name
      if (name.isDefined && tm.nameToAccums.contains(name.get)) {
        val tmAcc = tm.nameToAccums(name.get).asInstanceOf[AccumulatorV2[Any, Any]]
        tmAcc.metadata = acc.metadata
        tmAcc.merge(acc.asInstanceOf[AccumulatorV2[Any, Any]])
      } else {
        tm.externalAccums += acc
      }
    }
    tm
  }
}
