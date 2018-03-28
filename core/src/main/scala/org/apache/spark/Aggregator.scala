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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.collection.ExternalAppendOnlyMap

/**
 * :: DeveloperApi ::
 * A set of functions used to aggregate data.     用于聚合数据的一组函数。
 *
 * @param createCombiner function to create the initial value of the aggregation.     函数来创建聚合的初始值。
 * @param mergeValue function to merge a new value into the aggregation result.       函数将新值合并到聚合结果中。
 * @param mergeCombiners function to merge outputs from multiple mergeValue function. 合并多个mergevalue输出功能函数。
 */
@DeveloperApi
case class Aggregator[K, V, C] (
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C) {

  def combineValuesByKey(
      iter: Iterator[_ <: Product2[K, V]],
      context: TaskContext): Iterator[(K, C)] = {
    val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners)
    combiners.insertAll(iter)
    updateMetrics(context, combiners)
    combiners.iterator
  }

  /**
    * reduce端在缓存中对中间结果执行聚合和排序
    *   reduce端获取map端任务计算中间结果后，将shuffleBlockFetcherIterator封装为InterruptibleIterator并聚合。聚合操作主要依赖a
    * aggregator的combineCombinersByKey方法。如果isSpillEnabled为false,会再次使用AppendOnlyMap的changeValue方法，这些在AppendOnlyMap的
    * 缓存局和算法时做过详细介绍。isSpillEnabled默认shitrue，此时会使用ExternaleAppendOnlyMap完成聚合。
    */
  def combineCombinersByKey(
      iter: Iterator[_ <: Product2[K, C]],
      context: TaskContext): Iterator[(K, C)] = {
    val combiners = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
    combiners.insertAll(iter)
    updateMetrics(context, combiners)
    combiners.iterator
  }

  /** Update task metrics after populating the external map.
    * 更新task指标后填充外部map。
    * */
  private def updateMetrics(context: TaskContext, map: ExternalAppendOnlyMap[_, _, _]): Unit = {
    Option(context).foreach { c =>
      c.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
      c.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
      c.taskMetrics().incPeakExecutionMemory(map.peakMemoryUsedBytes)
    }
  }
}
