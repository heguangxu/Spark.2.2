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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.Utils

/**
  * An implementation of checkpointing implemented on top of Spark's caching layer.
  *
  *  Spark检查点的顶层缓存实现
  *
  * Local checkpointing trades off fault tolerance for performance by skipping the expensive
  * step of saving the RDD data to a reliable and fault-tolerant storage. Instead, the data
  * is written to the local, ephemeral block storage that lives in each executor. This is useful
  * for use cases where RDDs build up long lineages that need to be truncated often (e.g. GraphX).
  *
  * 本地检查点通过跳过 保存RDD和容错存储这些昂贵的步骤 来权衡性能，相反，数据写入每个执行器中的本地、临时块存储。
  * 这是非常有用的对于那些需要建立长RDDS血统的RDD，需要截断的往往是有用的（例如：GraphX）。
  *
  */
private[spark] class LocalRDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
  extends RDDCheckpointData[T](rdd) with Logging {

  /**
    * Ensure the RDD is fully cached so the partitions can be recovered later.
    * 确保RDD是完全缓存的，这样分区可以在以后恢复。
    */
  protected override def doCheckpoint(): CheckpointRDD[T] = {
    val level = rdd.getStorageLevel

    // Assume storage level uses disk; otherwise memory eviction may cause data loss
    // 假设存储层采用disk磁盘；否则内存回收可能导致数据丢失
    assume(level.useDisk, s"Storage level $level is not appropriate for local checkpointing")

    // Not all actions compute all partitions of the RDD (e.g. take). For correctness, we
    // must cache any missing partitions. TODO: avoid running another job here (SPARK-8582).
    // 不是所有的actions需要计算所有分区的RDD（如 take）。为了正确性，我们必须缓存任何丢失的分区。
    // 待办事项：避免另一个工作在这里运行（spark-8582）。
    val action = (tc: TaskContext, iterator: Iterator[T]) => Utils.getIteratorSize(iterator)
    val missingPartitionIndices = rdd.partitions.map(_.index).filter { i =>
      !SparkEnv.get.blockManager.master.contains(RDDBlockId(rdd.id, i))
    }
    if (missingPartitionIndices.nonEmpty) {
      rdd.sparkContext.runJob(rdd, action, missingPartitionIndices)
    }

    new LocalCheckpointRDD[T](rdd)
  }

}

private[spark] object LocalRDDCheckpointData {

  val DEFAULT_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK

  /**
    * Transform the specified storage level to one that uses disk.
    *
    * 根据一个指定的存储级别使用disk磁盘进行Transform转换
    *
    * This guarantees that the RDD can be recomputed multiple times correctly as long as
    * executors do not fail. Otherwise, if the RDD is cached in memory only, for instance,
    * the checkpoint data will be lost if the relevant block is evicted from memory.
    *
    * 这保证了RDD可以多次正确的计算，只要执行者不失败。否则，如果RDD是缓存在内存中，例如，如果相关的数据库在内存回收的时候
    * 可能造成checkpoint检查点的数据丢失
    *
    * This method is idempotent. 此方法是幂等的。
    */
  def transformStorageLevel(level: StorageLevel): StorageLevel = {
    StorageLevel(useDisk = true, level.useMemory, level.deserialized, level.replication)
  }
}
