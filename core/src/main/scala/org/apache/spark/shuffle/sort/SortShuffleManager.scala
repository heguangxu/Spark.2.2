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

package org.apache.spark.shuffle.sort

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._

/**
 * In sort-based shuffle, incoming records are sorted according to their target partition ids, then
 * written to a single map output file. Reducers fetch contiguous regions of this file in order to
 * read their portion of the map output. In cases where the map output data is too large to fit in
 * memory, sorted subsets of the output can are spilled to disk and those on-disk files are merged
 * to produce the final output file.
  *
  * 在基于排序的shuffle中，传入记录按照目标分区id排序，然后写入单个映射输出文件。还原器获取该文件的连续区域，
  * 以读取它们的部分映射输出。如果映射的输出数据太大，无法在内存中进行匹配，则将输出的排序子集溢出到磁盘，
  * 然后将这些磁盘文件合并，以生成最终的输出文件。
 *
 * Sort-based shuffle has two different write paths for producing its map output files:
  * 基于排序的shuffle有两种不同的生成地图输出文件的写路径:
 *
 *  - Serialized sorting: used when all three of the following conditions hold:
 *    1. The shuffle dependency specifies no aggregation or output ordering.
 *    2. The shuffle serializer supports relocation of serialized values (this is currently
 *       supported by KryoSerializer and Spark SQL's custom serializers).
 *    3. The shuffle produces fewer than 16777216 output partitions.
 *  - Deserialized sorting: used to handle all other cases.
  *
  *  序列化排序:当所有3个条件保持不变时使用:
  *  1 .工作shuffle依赖性指定没有聚合或输出排序。
  *  2。shuffle serializer支持序列化值的重新定位(目前由KryoSerializer和Spark SQL的自定义序列化器支持)。
  *  3 .项目shuffle产生的输出分区少于16777216。
  *  非序列化排序:用于处理所有其他情况。
 *
 * -----------------------
 * Serialized sorting mode  序列化的排序方式
 * -----------------------
 *
 * In the serialized sorting mode, incoming records are serialized as soon as they are passed to the
 * shuffle writer and are buffered in a serialized form during sorting. This write path implements
 * several optimizations:
  *
  * 在序列化的排序模式下，传入的记录在传递给shuffle writer时就被序列化，并且在排序过程中以序列化的形式缓冲。
  * 这个写路径实现了一些优化:
 *
 *  - Its sort operates on serialized binary data rather than Java objects, which reduces memory
 *    consumption and GC overheads. This optimization requires the record serializer to have certain
 *    properties to allow serialized records to be re-ordered without requiring deserialization.
 *    See SPARK-4550, where this optimization was first proposed and implemented, for more details.
  *
  *   它的排序作用于序列化的二进制数据而不是Java对象，这减少了内存消耗和GC开销。这种优化要求记录序列化程序具有一定的属性，
  *   允许序列化记录在不需要反序列化的情况下重新排序。参见spark - 4550，其中首次提出并实现了这个优化，以了解更多细节。
 *
 *  - It uses a specialized cache-efficient sorter ([[ShuffleExternalSorter]]) that sorts
 *    arrays of compressed record pointers and partition ids. By using only 8 bytes of space per
 *    record in the sorting array, this fits more of the array into cache.
  *
  *   它使用一种特殊的缓存高效排序器([[ShuffleExternalSorter]])，它可以对压缩的记录指针和分区id进行排序。
  *   通过在排序数组中只使用8个字节的空间，这更适合于数组的缓存。
 *
 *  - The spill merging procedure operates on blocks of serialized records that belong to the same
 *    partition and does not need to deserialize records during the merge.
  *
  *    spill合并过程运行在属于同一分区的序列化记录块上，在合并过程中不需要对记录进行反序列化。
 *
 *  - When the spill compression codec supports concatenation of compressed data, the spill merge
 *    simply concatenates the serialized and compressed spill partitions to produce the final output
 *    partition.  This allows efficient data copying methods, like NIO's `transferTo`, to be used
 *    and avoids the need to allocate decompression or copying buffers during the merge.
  *
  *   当spill压缩codec支持压缩数据的连接时，spill合并简单地连接序列化和压缩的溢出分区，以产生最终的输出分区。
  *   这允许高效的数据复制方法，比如NIO的“transferTo”，在合并过程中使用并避免了分配解压或复制缓冲区的需要。
 *
 * For more details on these optimizations, see SPARK-7081.
  * 有关这些优化的更多细节，请参见 SPARK-7081.
 */
private[spark] class SortShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  /**
    * 优化：spark.shuffle.spill
    *   如果为true，在shuffle期间通过溢出数据到磁盘来降低内存使用总量，溢出阈值是由spark.shuffle.memoryFraction指定的。
    */
  if (!conf.getBoolean("spark.shuffle.spill", true)) {
    // spark.shuffle.spill 被设置为false，但是这个配置被1.6版本忽略，因为Spark 1.6 Shuffle将在必要时继续泄漏到磁盘。
    logWarning(
      "spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
        " Shuffle will continue to spill to disk when necessary.")
  }

  /**
   * A mapping from shuffle ids to the number of mappers producing output for those shuffles.
    * 从shuffle id到为这些洗牌生产输出的mappers的数量的映射。
   */
  private[this] val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()

  override val shuffleBlockResolver = new IndexShuffleBlockResolver(conf)

  /**
   * Obtains a [[ShuffleHandle]] to pass to tasks.
    * 获得[[ShuffleHandle]]传递给任务。
    *
    * SortShuffleManager运行原理SortShuffleManager的运行机制主要分成两种，一种是普通运行机制，
    * 另一种是bypass运行机制。当shuffle read task的数量小于等于spark.shuffle.sort.bypassMergeThreshold
    * 参数的值时（默认为200），就会启用bypass机制。
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
      // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
      // need map-side aggregation, then write numPartitions files directly and just concatenate
      // them at the end. This avoids doing serialization and deserialization twice to merge
      // together the spilled files, which would happen with the normal code path. The downside is
      // having multiple files open at a time and thus more memory allocated to buffers.
      /** 如果有少于 spark.shuffle.sort.bypassMergeThreshold分区数，我们不需要map端的聚合，然后直接编写numPartitions文件，
        * 并在最后将它们连接起来。这避免了序列化和反序列化两次，以合并溢出的文件，这将发生在正常的代码路径上。
        * 缺点是在一个时间内打开多个文件，从而分配更多的内存到缓冲区。
        * */
      new BypassMergeSortShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
      //  否则，尝试以序列化形式缓冲映射输出，因为这样更有效:
      new SerializedShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // Otherwise, buffer map outputs in a deserialized form:
      // 否则，缓冲映射输出以反序列化形式输出:
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
    *
    * 为一系列减少分区获取一个阅读器(startPartition to endPartition-1, inclusive)。
    * 被运行在executors上的reduce任务调用
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    new BlockStoreShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  /** Get a writer for a given partition. Called on executors by map tasks.
    * 为给定的分区获取一个写入器。被运行在executors上的map任务调用
    * */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext): ShuffleWriter[K, V] = {
    numMapsForShuffle.putIfAbsent(
      handle.shuffleId, handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)
    val env = SparkEnv.get
    handle match {
      case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
        new UnsafeShuffleWriter(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf)
      case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
        new BypassMergeSortShuffleWriter(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
          bypassMergeSortHandle,
          mapId,
          context,
          env.conf)
      case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        new SortShuffleWriter(shuffleBlockResolver, other, mapId, context)
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager.
    * 从ShuffleManager移除shuffle的元数据
    * */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(numMapsForShuffle.remove(shuffleId)).foreach { numMaps =>
      (0 until numMaps).foreach { mapId =>
        shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
      }
    }
    true
  }

  /** Shut down this ShuffleManager.
    * 关闭ShuffleManager
    * */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}


private[spark] object SortShuffleManager extends Logging {

  /**
   * The maximum number of shuffle output partitions that SortShuffleManager supports when
   * buffering map outputs in a serialized form. This is an extreme defensive programming measure,
   * since it's extremely unlikely that a single shuffle produces over 16 million output partitions.
    *
    * 当以序列化形式缓冲映射输出时，SortShuffleManager支持的最大的洗牌输出分区数。这是一种极端的防御性编程措施，
    * 因为单次洗牌就不太可能产生超过1600万的输出分区。
    *
   * */
  val MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE =
    PackedRecordPointer.MAXIMUM_PARTITION_ID + 1

  /**
   * Helper method for determining whether a shuffle should use an optimized serialized shuffle
   * path or whether it should fall back to the original path that operates on deserialized objects.
    *
    * 帮助确定shuffle是否应该使用经过优化的序列化的洗牌路径，或者它是否应该回到运行在反序列化对象上的原始路径。
   */
  def canUseSerializedShuffle(dependency: ShuffleDependency[_, _, _]): Boolean = {
    val shufId = dependency.shuffleId
    val numPartitions = dependency.partitioner.numPartitions
    if (!dependency.serializer.supportsRelocationOfSerializedObjects) {
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because the serializer, " +
        s"${dependency.serializer.getClass.getName}, does not support object relocation")
      false
    } else if (dependency.aggregator.isDefined) {
      log.debug(
        s"Can't use serialized shuffle for shuffle $shufId because an aggregator is defined")
      false
    } else if (numPartitions > MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because it has more than " +
        s"$MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE partitions")
      false
    } else {
      log.debug(s"Can use serialized shuffle for shuffle $shufId")
      true
    }
  }
}

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * serialized shuffle.
  *
  * [[BaseShuffleHandle]的子类]，用于识别我们何时选择使用序列化的shuffle。
 */
private[spark] class SerializedShuffleHandle[K, V](
  shuffleId: Int,
  numMaps: Int,
  dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * bypass merge sort shuffle path.
  * [[BaseShuffleHandle]的子类]，用于识别我们选择何时使用旁路归并排序。
 */
private[spark] class BypassMergeSortShuffleHandle[K, V](
  shuffleId: Int,
  numMaps: Int,
  dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}
