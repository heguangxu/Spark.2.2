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

import org.apache.spark.{Partition, SparkContext, SparkException, TaskContext}
import org.apache.spark.storage.RDDBlockId

/**
  * A dummy CheckpointRDD that exists to provide informative error messages during failures.
  * 一个虚拟的CheckpointRDD 她的存在是为了在失败期间提供错误信息的
  *
  * This is simply a placeholder because the original checkpointed RDD is expected to be
  * fully cached. Only if an executor fails or if the user explicitly unpersists the original
  * RDD will Spark ever attempt to compute this CheckpointRDD. When this happens, however,
  * we must provide an informative error message.
  *
  * 这仅仅是一个占位符，因为最初的检查点RDD有望完全缓存。只有一个执行失败或如果用户明确使用unpersists不持久化最初的RDD，
  * Spark将尝试计算这个checkpointrdd。然而，当这种情况发生时，我们必须提供错误信息。
  *
  *
  * @param sc the active SparkContext
  * @param rddId the ID of the checkpointed RDD
  * @param numPartitions the number of partitions in the checkpointed RDD
  */
private[spark] class LocalCheckpointRDD[T: ClassTag](
                                                      sc: SparkContext,
                                                      rddId: Int,
                                                      numPartitions: Int)
  extends CheckpointRDD[T](sc) {

  def this(rdd: RDD[T]) {
    this(rdd.context, rdd.id, rdd.partitions.length)
  }

  protected override def getPartitions: Array[Partition] = {
    (0 until numPartitions).toArray.map { i => new CheckpointRDDPartition(i) }
  }

  /**
    * Throw an exception indicating that the relevant block is not found.
    * 抛出一个异常，表明相关的块没有被找到。
    *
    * This should only be called if the original RDD is explicitly unpersisted or if an
    * executor is lost. Under normal circumstances, however, the original RDD (our child)
    * is expected to be fully cached and so all partitions should already be computed and
    * available in the block storage.
    *
    *  当最初的RDD明确的使用unpersisted不进行持久化，或者执行者丢失了，那么应该调用这个函数。正常情况下，然而，
    *  最初的RDD（孩子）有望完全缓存，所以所有的分区应该已经被计算，而且块存储可用。
    *
    */
  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    throw new SparkException(
      s"Checkpoint block ${RDDBlockId(rddId, partition.index)} not found! Either the executor " +
        s"that originally checkpointed this partition is no longer alive, or the original RDD is " +
        s"unpersisted. If this problem persists, you may consider using `rdd.checkpoint()` " +
        s"instead, which is slower than local checkpointing but more fault-tolerant.")
  }

}
