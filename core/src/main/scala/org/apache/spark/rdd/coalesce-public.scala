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

import scala.collection.mutable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Partition

/**
  * ::DeveloperApi::
  * A PartitionCoalescer defines how to coalesce the partitions of a given RDD.
  *
  * 一个PartitionCoalescer（分区聚结器）定义了如何结合给定的RDD的分区。
  *
  */
@DeveloperApi
trait PartitionCoalescer {

  /**
    * Coalesce the partitions of the given RDD.      结合给定的RDD的分区。
    *
    * @param maxPartitions the maximum number of partitions to have after coalescing
    *                      聚结后的最大分区数
    * @param parent the parent RDD whose partitions to coalesce
    *               父RDD的分区合并
    * @return an array of [[PartitionGroup]]s, where each element is itself an array of
    * `Partition`s and represents a partition after coalescing is performed.
    * 返回一个[[PartitionGroup]]的数组，其中每个元素本身是一个数组`分区`和代表一个分区后合并进行。
    *
    */
  def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup]
}

/**
  * ::DeveloperApi::
  * A group of `Partition`s      “分区”的一组
  * @param prefLoc preferred location for the partition group
  */
@DeveloperApi
class PartitionGroup(val prefLoc: Option[String] = None) {
  val partitions = mutable.ArrayBuffer[Partition]()
  def numPartitions: Int = partitions.size
}
