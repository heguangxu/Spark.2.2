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

import org.apache.spark.{Partitioner, RangePartitioner}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging

/**
  * Extra functions available on RDDs of (key, value) pairs where the key is sortable through
  * an implicit conversion. They will work with any key type `K` that has an implicit `Ordering[K]`
  * in scope. Ordering objects already exist for all of the standard primitive types. Users can also
  * define their own orderings for custom types, or to override the default ordering. The implicit
  * ordering that is in the closest scope will be used.
  *
  * 额外的可用功能RDDS（键，值）对其中的key是可通过隐式转换。他们将与任何关键型` K `具有隐`Ordering[K]`范围工作。
  * 排序的对象已经存在的所有标准的原始类型。用户还可以定义自己的自定义类型排序，或重写默认的排序。隐式排序是在最近的范围将被使用。

  *
  * {{{
  *   import org.apache.spark.SparkContext._
  *
  *   val rdd: RDD[(String, Int)] = ...
  *   implicit val caseInsensitiveOrdering = new Ordering[String] {
  *     override def compare(a: String, b: String) = a.toLowerCase.compare(b.toLowerCase)
  *   }
  *
  *   // Sort by key, using the above case insensitive ordering.
  *   rdd.sortByKey()
  * }}}
  */
class OrderedRDDFunctions[K : Ordering : ClassTag,
V: ClassTag,
P <: Product2[K, V] : ClassTag] @DeveloperApi() (
                                                  self: RDD[P])
  extends Logging with Serializable {
  private val ordering = implicitly[Ordering[K]]

  /**
    * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
    * `collect` or `save` on the resulting RDD will return or output an ordered list of records
    * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
    * order of the keys).
    *
    * 通过key进行RDD排序，这样每个分区包含一个元素的排序范围。调用`collect`或`save` 的结果RDD将返回或输出的有序列表记录
    * （在`save`的情况，为了根据key进行排序，将会产生很多的`part-X`文件）。
    *
    */
  // TODO: this currently doesn't work on P other than Tuple2!
  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
  : RDD[(K, V)] = self.withScope
  {
    val part = new RangePartitioner(numPartitions, self, ascending)
    new ShuffledRDD[K, V, V](self, part)
      .setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }

  /**
    * Repartition the RDD according to the given partitioner and, within each resulting partition,
    * sort records by their keys.
    *
    * 根据给定的分区数对RDD进行重新分区，每个分区内，通过按key排序记录。
    *
    * This is more efficient than calling `repartition` and then sorting within each partition
    * because it can push the sorting down into the shuffle machinery.
    *
    * 这比调用`repartition`方法要效率高，然后在每个分区内排序，因为它可以推送排序后的结果到shuffle的机器。
    *
    */
  def repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)] = self.withScope {
    new ShuffledRDD[K, V, V](self, partitioner).setKeyOrdering(ordering)
  }

  /**
    * Returns an RDD containing only the elements in the inclusive range `lower` to `upper`.
    * If the RDD has been partitioned using a `RangePartitioner`, then this operation can be
    * performed efficiently by only scanning the partitions that might contain matching elements.
    * Otherwise, a standard `filter` is applied to all partitions.
    *
    * 返回一个RDD仅包含元素的范围`lower`到`upper`。如果RDD采用` rangepartitioner `分区过，
    * 那么这个操作可以通过扫描分区可能包含的匹配元素会更加有效。否则，一个标准的`filter`将应用到所有分区。
    *
    */
  def filterByRange(lower: K, upper: K): RDD[P] = self.withScope {

    def inRange(k: K): Boolean = ordering.gteq(k, lower) && ordering.lteq(k, upper)

    val rddToFilter: RDD[P] = self.partitioner match {
      case Some(rp: RangePartitioner[K, V]) =>
        val partitionIndicies = (rp.getPartition(lower), rp.getPartition(upper)) match {
          case (l, u) => Math.min(l, u) to Math.max(l, u)
        }
        PartitionPruningRDD.create(self, partitionIndicies.contains)
      case _ =>
        self
    }
    rddToFilter.filter { case (k, v) => inRange(k) }
  }

}
