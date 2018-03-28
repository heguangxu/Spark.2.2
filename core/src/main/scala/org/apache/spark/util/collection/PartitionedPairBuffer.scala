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

package org.apache.spark.util.collection

import java.util.Comparator

import org.apache.spark.util.collection.WritablePartitionedPairCollection._

/**
 * Append-only buffer of key-value pairs, each with a corresponding partition ID, that keeps track
 * of its estimated size in bytes.
 *
 * The buffer can support up to `1073741823 (2 ^ 30 - 1)` elements.
 */
private[spark] class PartitionedPairBuffer[K, V](initialCapacity: Int = 64)
  extends WritablePartitionedPairCollection[K, V] with SizeTracker
{
  import PartitionedPairBuffer._

  require(initialCapacity <= MAXIMUM_CAPACITY,
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")

  // Basic growable array data structure. We use a single array of AnyRef to hold both the keys
  // and the values, so that we can sort them efficiently with KVArraySortDataFormat.
  private var capacity = initialCapacity
  private var curSize = 0
  private var data = new Array[AnyRef](2 * initialCapacity)

  /** Add an element into the buffer
    * 把计算结果简单的缓存到数组中
    * */
  def insert(partition: Int, key: K, value: V): Unit = {
    if (curSize == capacity) {
      // SizeTrackingPairBuffer的容量增长是通过growArray方法实现的，growArray实现增长Data数组容量的方式非常简单，
      // 只是新建2倍大小的新数组，然后简单复制而已。
      growArray()
    }
    data(2 * curSize) = (partition, key.asInstanceOf[AnyRef])
    data(2 * curSize + 1) = value.asInstanceOf[AnyRef]
    curSize += 1
    afterUpdate()
  }

  /** Double the size of the array because we've reached capacity
    *
    * SizeTrackingPairBuffer的容量增长是通过growArray方法实现的。growArray实现增长data数组容量的方式，非常简单，只是
    * 新建2倍大小的新数组，然后简单的复制而已。
    *
    * 注意：新建的数组大小有可能超过Int类型的最大值，所以会抛出异常。
    *
    *
    * Spar使用SizeTrackingPairBuffer的过程中，也会调用maybeSplillCollection方法，来处理SizeTrackingPairBuffer溢出
    * （当SizeTrackingPairBuffer的大小超过myMemoryThreshold时，将集合中的数据写入到磁盘并新建SizeTrackingPairBuffer）
    * 这样做是为了防止内存溢出，解决了Spark早期版本的shuffle的内存撑爆问题。
    * */
  private def growArray(): Unit = {
    if (capacity >= MAXIMUM_CAPACITY) {
      throw new IllegalStateException(s"Can't insert more than ${MAXIMUM_CAPACITY} elements")
    }
    val newCapacity =
      if (capacity * 2 < 0 || capacity * 2 > MAXIMUM_CAPACITY) { // Overflow
        MAXIMUM_CAPACITY
      } else {
        capacity * 2
      }
    val newArray = new Array[AnyRef](2 * newCapacity)
    System.arraycopy(data, 0, newArray, 0, 2 * capacity)
    data = newArray
    capacity = newCapacity
    resetSamples()
  }

  /** Iterate through the data in a given order. For this class this is not really destructive. */
  override def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)] = {
    val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
    new Sorter(new KVArraySortDataFormat[(Int, K), AnyRef]).sort(data, 0, curSize, comparator)
    iterator
  }

  private def iterator(): Iterator[((Int, K), V)] = new Iterator[((Int, K), V)] {
    var pos = 0

    override def hasNext: Boolean = pos < curSize

    override def next(): ((Int, K), V) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val pair = (data(2 * pos).asInstanceOf[(Int, K)], data(2 * pos + 1).asInstanceOf[V])
      pos += 1
      pair
    }
  }
}

private object PartitionedPairBuffer {
  val MAXIMUM_CAPACITY = Int.MaxValue / 2 // 2 ^ 30 - 1
}
