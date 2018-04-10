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

import java.util.Random

import scala.collection.{mutable, Map}
import scala.collection.mutable.ArrayBuffer
import scala.io.Codec
import scala.language.implicitConversions
import scala.reflect.{classTag, ClassTag}

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.hadoop.io.{BytesWritable, NullWritable, Text}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.TextOutputFormat

import org.apache.spark._
import org.apache.spark.Partitioner._
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.partial.BoundedDouble
import org.apache.spark.partial.CountEvaluator
import org.apache.spark.partial.GroupedCountEvaluator
import org.apache.spark.partial.PartialResult
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.{BoundedPriorityQueue, Utils}
import org.apache.spark.util.collection.{OpenHashMap, Utils => collectionUtils}
import org.apache.spark.util.random.{BernoulliCellSampler, BernoulliSampler, PoissonSampler,
SamplingUtils}

/**
  * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,
  * partitioned collection of elements that can be operated on in parallel. This class contains the
  * basic operations available on all RDDs, such as `map`, `filter`, and `persist`. In addition,
  * [[org.apache.spark.rdd.PairRDDFunctions]] contains operations available only on RDDs of key-value
  * pairs, such as `groupByKey` and `join`;
  * [[org.apache.spark.rdd.DoubleRDDFunctions]] contains operations available only on RDDs of
  * Doubles; and
  * [[org.apache.spark.rdd.SequenceFileRDDFunctions]] contains operations available on RDDs that
  * can be saved as SequenceFiles.
  * All operations are automatically available on any RDD of the right type (e.g. RDD[(Int, Int)]
  * through implicit.
  *
  * 弹性分布式数据集（RDD），Spark的基本抽象。是不变的、是可以并行操作的分区的集合元素。这个类包含了基本操作的所有RDDS可用，
  * 如``map`, `filter`, 和`persist`。此外，[[org.apache.spark.rdd.PairRDDFunctions]] 包含操作只对键值对操作，
  * 如`groupByKey` and `join`；
  * [[org.apache.spark.rdd.DoubleRDDFunctions]] 包含操作只对RDDS Doubles可用；
  * 和[[org.apache.spark.rdd.SequenceFileRDDFunctions]] 可以操作RDD，并且可以保存为sequencefiles文件的可用的操作。
  * 所有的操作都是自动可用任何RDD的权利类型（如RDD [（int，Int）]通过内隐。
  *
  *
  * Internally, each RDD is characterized by five main properties:
  * 在内部，每个RDD具有五个主要特性：
  *
  *  - A list of partitions
  *                                         一个分区的集合
  *  - A function for computing each split
  *                                         计算每个分割的函数
  *  - A list of dependencies on other RDDs
  *                                         其他RDDS依赖的集合
  *  - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
  *                                         或者，一个key-value型的RDDS分区（例如说RDD是哈希分区）
  *  - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
  *    an HDFS file)
  *                                         或者，一个计算各分在优先位置列表（一个HDFS文件例如块位置）
  *
  * All of the scheduling and execution in Spark is done based on these methods, allowing each RDD
  * to implement its own way of computing itself. Indeed, users can implement custom RDDs (e.g. for
  * reading data from a new storage system) by overriding these functions. Please refer to the
  * <a href="http://people.csail.mit.edu/matei/papers/2012/nsdi_spark.pdf">Spark paper</a>
  * for more details on RDD internals.
  *
  * 在Spark所有的调度和执行这些方法的基础上，允许每个RDD实现自己的计算方式。事实上，用户可以实现自定义的RDD计算方式
  * （例如，从一个新的存储系统中读取数据）通过重写这些函数。请参阅http://people.csail.mit.edu/matei/papers/2012/nsdi_spark.pdf
  * 更多细节RDD内部。
  *
  *
  */
abstract class RDD[T: ClassTag](
                                 @transient private var _sc: SparkContext,
                                 @transient private var deps: Seq[Dependency[_]]
                               ) extends Serializable with Logging {

  if (classOf[RDD[_]].isAssignableFrom(elementClassTag.runtimeClass)) {
    // This is a warning instead of an exception in order to avoid breaking user programs that
    // might have defined nested RDDs without running jobs with them.
    // 这是一个warning而不是一个异常，以避免破坏用户程序可能定义嵌套RDDS不运行工作与他们的警告。
    logWarning("Spark does not support nested RDDs (see SPARK-5063)")
  }

  private def sc: SparkContext = {
    if (_sc == null) {
      // 这一个TDD缺乏sparkcontext。它可以在以下情况下发生：
      // （1）RDD变换和行动不是由驱动程序调用，但在其他转换；例如，rdd1.map(x => rdd2.values.count() * x) 是无效的
      //      因为转换值和计算总数不能在rdd1.map转换内部 。更多信息，见spark-5063。
      // （2）当一个Spark Streaming 的任务从检查点恢复，该异常将被打如果一个RDD不是由流的工作定义是用于dstream操作。
      //      更多信息，见spark-13758。
      throw new SparkException(
        "This RDD lacks a SparkContext. It could happen in the following cases: \n(1) RDD " +
          "transformations and actions are NOT invoked by the driver, but inside of other " +
          "transformations; for example, rdd1.map(x => rdd2.values.count() * x) is invalid " +
          "because the values transformation and count action cannot be performed inside of the " +
          "rdd1.map transformation. For more information, see SPARK-5063.\n(2) When a Spark " +
          "Streaming job recovers from checkpoint, this exception will be hit if a reference to " +
          "an RDD not defined by the streaming job is used in DStream operations. For more " +
          "information, See SPARK-13758.")
    }
    _sc
  }

  /** Construct an RDD with just a one-to-one dependency on one parent
    * 构建只是一对一的依赖一个父RDD
    * */
  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context, List(new OneToOneDependency(oneParent)))

  private[spark] def conf = sc.conf
  // =======================================================================
  // Methods that should be implemented by subclasses of RDD
  // 方法应该由RDD实现子类
  // =======================================================================

  /**
    * :: DeveloperApi ::
    * Implemented by subclasses to compute a given partition.
    * 由子类实现计算给定的分区。
    */
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[T]

  /**
    * Implemented by subclasses to return the set of partitions in this RDD. This method will only
    * be called once, so it is safe to implement a time-consuming computation in it.
    *
    * 通过子类在RDD返回分区的设置。此方法只调用一次，因此在其中实现耗时的计算是安全的。
    *
    * The partitions in this array must satisfy the following property:
    *   `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
    *
    *  这个数组中的分区必须满足以下属性：
    *   `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
    *
    */
  protected def getPartitions: Array[Partition]

  /**
    * Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
    * be called once, so it is safe to implement a time-consuming computation in it.
    *
    * 通过子类返回这个RDD取决于父母的RDD。此方法只调用一次，因此在其中实现耗时的计算是安全的。
    *
    */
  protected def getDependencies: Seq[Dependency[_]] = deps

  /**
    * Optionally overridden by subclasses to specify placement preferences.
    *
    * 可选地由子类重写以指定安置首选项。
    */
  protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  /** Optionally overridden by subclasses to specify how they are partitioned.
    * 可选地由子类重写以指定它们是如何分区的。
    * */
  @transient val partitioner: Option[Partitioner] = None

  // =======================================================================
  // Methods and fields available on all RDDs
  // 在所有的RDDS中方法和字段可用
  // =======================================================================

  /** The SparkContext that created this RDD.
    * 创建这个RDD的sparkcontext。
    * */
  def sparkContext: SparkContext = sc

  /** A unique ID for this RDD (within its SparkContext).
    * RDD唯一的ID（在其sparkcontext中）。
    * */
  val id: Int = sc.newRddId()

  /** A friendly name for this RDD
    * 为RDD起一个友好的名字
    * */
  @transient var name: String = null

  /** Assign a name to this RDD
    * 为RDD设置一个名称
    * */
  def setName(_name: String): this.type = {
    name = _name
    this
  }

  /**
    * Mark this RDD for persisting using the specified level.
    * 使用指定的级别去持久化一个RDD
    *
    * @param newLevel the target storage level     目标的持久化存储级别
    * @param allowOverride whether to override any existing level with the new one 否使用新的覆盖覆盖现有的级别
    */
  private def persist(newLevel: StorageLevel, allowOverride: Boolean): this.type = {
    // TODO: Handle changes of StorageLevel
    if (storageLevel != StorageLevel.NONE && newLevel != storageLevel && !allowOverride) {
      // 不能改变存储级别的RDD后已经分配了一个级别
      throw new UnsupportedOperationException(
        "Cannot change storage level of an RDD after it was already assigned a level")
    }
    // If this is the first time this RDD is marked for persisting, register it
    // with the SparkContext for cleanups and accounting. Do this only once.
    // 如果这是第一次，RDD标记为persisting持久化，SparkContext注册它为了以后的清理。只做一次。
    if (storageLevel == StorageLevel.NONE) {
      sc.cleaner.foreach(_.registerRDDForCleanup(this))
      sc.persistRDD(this)
    }
    storageLevel = newLevel
    this
  }

  /**
    * Set this RDD's storage level to persist its values across operations after the first time
    * it is computed. This can only be used to assign a new storage level if the RDD does not
    * have a storage level set yet. Local checkpointing is an exception.
    *
    * 设置这个RDD的存储级别，持久化其值在操作第一次计算后。这只能用于分配新的存储级别如果RDD存储级别平尚未确定。本地检查点是一个异常。
    *
    */
  def persist(newLevel: StorageLevel): this.type = {
    if (isLocallyCheckpointed) {
      // This means the user previously called localCheckpoint(), which should have already
      // marked this RDD for persisting. Here we should override the old storage level with
      // one that is explicitly requested by the user (after adapting it to use disk).
      // 这意味着用户以前调用localcheckpoint()，这应该已经标明RDD持久化。在这里，我们应该用用户明确要求的一个覆盖旧的存储级别
      // （在调整后使用磁盘）。
      persist(LocalRDDCheckpointData.transformStorageLevel(newLevel), allowOverride = true)
    } else {
      persist(newLevel, allowOverride = false)
    }
  }

  /**
    * Persist this RDD with the default storage level (`MEMORY_ONLY`).
    * 持久化一个RDD使用默认的存储级别(`MEMORY_ONLY`).仅内存
    */
  def persist(): this.type = persist(StorageLevel.MEMORY_ONLY)

  /**
    * Persist this RDD with the default storage level (`MEMORY_ONLY`).
    * 持久化（缓存）一个RDD使用默认的存储级别(`MEMORY_ONLY`).仅内存
    */
  def cache(): this.type = persist()

  /**
    * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
    * 取消持久化，而且从内存和磁盘都溢出RDD所有的块
    *
    * @param blocking Whether to block until all blocks are deleted.   是否阻塞直到所有的块都删除。
    * @return This RDD.
    */
  def unpersist(blocking: Boolean = true): this.type = {
    logInfo("Removing RDD " + id + " from persistence list")
    sc.unpersistRDD(id, blocking)
    storageLevel = StorageLevel.NONE
    this
  }

  /** Get the RDD's current storage level, or StorageLevel.NONE if none is set.
    * 得到RDD的当前存储级别，或storagelevel.none如果没有设置。
    * */
  def getStorageLevel: StorageLevel = storageLevel

  // Our dependencies and partitions will be gotten by calling subclass's methods below, and will
  // be overwritten when we're checkpointed
  // 我们的依赖和分区将通过调用子类的方法在下面了，并将被覆盖在我们的检查点
  private var dependencies_ : Seq[Dependency[_]] = null
  @transient private var partitions_ : Array[Partition] = null

  /** An Option holding our checkpoint RDD, if we are checkpointed
    * 一个拿着我们的检查点RDD的选择，如果我们设置了检查点
    * */
  private def checkpointRDD: Option[CheckpointRDD[T]] = checkpointData.flatMap(_.checkpointRDD)

  /**
    * Get the list of dependencies of this RDD, taking into account whether the
    * RDD is checkpointed or not.
    *
    * 得到了RDD的依赖列表，是否考虑到RDD是检查点或不。
    *
    */
  final def dependencies: Seq[Dependency[_]] = {
    checkpointRDD.map(r => List(new OneToOneDependency(r))).getOrElse {
      if (dependencies_ == null) {
        dependencies_ = getDependencies
      }
      dependencies_
    }
  }

  /**
    * Get the array of partitions of this RDD, taking into account whether the
    * RDD is checkpointed or not.
    *
    * 得到RDD的分区数组，，是否考虑到RDD是检查点或不。
    */
  final def partitions: Array[Partition] = {
    checkpointRDD.map(_.partitions).getOrElse {
      if (partitions_ == null) {
        partitions_ = getPartitions
        partitions_.zipWithIndex.foreach { case (partition, index) =>
          require(partition.index == index,
            s"partitions($index).partition == ${partition.index}, but it should equal $index")
        }
      }
      partitions_
    }
  }

  /**
    * Returns the number of partitions of this RDD.
    *
    * 返回一个RDD的分区数
    */
  @Since("1.6.0")
  final def getNumPartitions: Int = partitions.length

  /**
    * Get the preferred locations of a partition, taking into account whether the
    * RDD is checkpointed.
    *
    * 获取分区的首选位置，同时考虑到RDD是检查点。
    */
  final def preferredLocations(split: Partition): Seq[String] = {
    checkpointRDD.map(_.getPreferredLocations(split)).getOrElse {
      getPreferredLocations(split)
    }
  }

  /**
    * Internal method to this RDD; will read from cache if applicable, or otherwise compute it.
    * This should ''not'' be called by users directly, but is available for implementors of custom
    * subclasses of RDD.
    *
    * RDD的内部方法；将从缓存中读取如果适用，否则计算。这应该''not''被直接用户，但可用于RDD的自定义类实现。
    *
    *   MappedRDD的iterator方法实际上是父类RDD的iterator方法。如果分区任务初次执行，此时还没有缓存，所以会调用
    * computeOrReadCheckpoint方法。
    *   这里需要说明一下iterator方法的容错处理过程；如果某个分区任务执行失败，但是其他分区任务执行成功，可以利用DAG重新调度，
    * 失败的分区任务将从检查点恢复状态，而那些执行成功的分区任务由于其他执行结果已经缓存到存储体系，所以调用CacheManager的
    * getOrCompute方法获取即可，不需要再次执行。
    */
  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {
      // 如果存储级别不是NONE，那么先检查是否有缓存，没有缓存则需要进行计算
      getOrCompute(split, context)
    } else {
      // 如果有checkpoint,那么直接读取结果，否则直接进行计算
      computeOrReadCheckpoint(split, context)
    }
  }

  /**
    * Return the ancestors of the given RDD that are related to it only through a sequence of
    * narrow dependencies. This traverses the given RDD's dependency tree using DFS, but maintains
    * no ordering on the RDDs returned.
    *
    * 返回给定的RDD，只通过一个狭窄的依赖相关的祖先。这穿越了RDD的依存树使用DFS，但保持在RDD没有顺序返回。
    *
    */
  private[spark] def getNarrowAncestors: Seq[RDD[_]] = {
    val ancestors = new mutable.HashSet[RDD[_]]

    def visit(rdd: RDD[_]) {
      val narrowDependencies = rdd.dependencies.filter(_.isInstanceOf[NarrowDependency[_]])
      val narrowParents = narrowDependencies.map(_.rdd)
      val narrowParentsNotVisited = narrowParents.filterNot(ancestors.contains)
      narrowParentsNotVisited.foreach { parent =>
        ancestors.add(parent)
        visit(parent)
      }
    }

    visit(this)

    // In case there is a cycle, do not include the root itself
    // 如果有一个循环，不包括根本身。
    ancestors.filterNot(_ == this).toSeq
  }

  /**
    * Compute an RDD partition or read it from a checkpoint if the RDD is checkpointing.
    *
    * 计算一个RDD分区或从检查点读取一个RDD如果RDD是正在checkpointing中。
    *
    * computeOrReadCheckpoint在存在检查点时直接获取中间结果，否则需要调用compute继续计算
    */
  private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
  {
    // 判断这个RDD是否建立检查点和实现，是可靠的或局部的。
    if (isCheckpointedAndMaterialized) {
      firstParent[T].iterator(split, context)
    } else {
      // MappedRDD的compute
      compute(split, context)
    }
  }

  /**
    * Gets or computes an RDD partition. Used by RDD.iterator() when an RDD is cached.
    *
    * 得到或者计算一个RDD分区，使用 RDD.iterator() 当一个RDD是被缓存中
    *
    * 在任务迭代计算的过程中，当判断村粗级别使用了缓存，就会调用CacheManager的getOrCompute方法。
    *
    * 处理逻辑：
    *   1.从存储体系获取Block;
    *   2.如果确实获取到了Block，那么将它封装为InterruptibleIterator并且返回。如果还没有缓存Block，
    *   则重新计算或者从CheckPoint中获取数据，调用putInBlockManager方法将数据写入缓存后封装为InterruptibleIterator并且返回。
    *
    */
  private[spark] def getOrCompute(partition: Partition, context: TaskContext): Iterator[T] = {
    // 获取RDD的BlockId
    val blockId = RDDBlockId(id, partition.index)
    var readCachedBlock = true
    // This method is called on executors, so we need call SparkEnv.get instead of sc.env.
    // 这种方法被executors调用，所以我们需要调用sparkenv.get代替sc.env方法。
    SparkEnv.get.blockManager.getOrElseUpdate(blockId, storageLevel, elementClassTag, () => {
      readCachedBlock = false
      // 在存在检查点时直接获取中间结果，否则需要调用compute继续计算
      computeOrReadCheckpoint(partition, context)
    }) match {
      // 向BlockManager查询是否有缓存
      case Left(blockResult) =>
        if (readCachedBlock) {
          val existingMetrics = context.taskMetrics().inputMetrics
          existingMetrics.incBytesRead(blockResult.bytes)
          new InterruptibleIterator[T](context, blockResult.data.asInstanceOf[Iterator[T]]) {
            override def next(): T = {
              existingMetrics.incRecordsRead(1)
              delegate.next()
            }
          }
        } else {
          new InterruptibleIterator(context, blockResult.data.asInstanceOf[Iterator[T]])
        }
      case Right(iter) =>
        new InterruptibleIterator(context, iter.asInstanceOf[Iterator[T]])
    }
  }

  /**
    * Execute a block of code in a scope such that all new RDDs created in this body will
    * be part of the same scope. For more detail, see {{org.apache.spark.rdd.RDDOperationScope}}.
    *
    * 在一个范围，在这个范围中创建的所有新的RDDS将相同的范围执行一部分代码块。更多的细节，
    * 看{{org.apache.spark.rdd.RDDOperationScope}}.
    *
    * Note: Return statements are NOT allowed in the given body.
    * 注意：在给定的正文中不允许返回语句。
    */
  private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](sc)(body)






  // Transformations (return a new RDD)   转换（返回一个新的RDD）

  /**
    * Return a new RDD by applying a function to all elements of this RDD.
    * 将一个函数应用于这个RDD的所有元素，返回一个新的RDD。
    */
  def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    // 这里调用了SparkContext的clean方法
    // clean方法实际上调用了ClosureCleaner的clean方法，这里一再清除闭包中的不能序列化的变量，防止RDD在网络传输过程中反序列化失败。
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
  }

  /**
    *  Return a new RDD by first applying a function to all elements of this
    *  RDD, and then flattening the results.
    */
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.flatMap(cleanF))
  }

  /**
    * Return a new RDD containing only the elements that satisfy a predicate.
    */
  def filter(f: T => Boolean): RDD[T] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[T, T](
      this,
      (context, pid, iter) => iter.filter(cleanF),
      preservesPartitioning = true)
  }

  /**
    * Return a new RDD containing the distinct elements in this RDD.
    */
  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
  }

  /**
    * Return a new RDD containing the distinct elements in this RDD.
    */
  def distinct(): RDD[T] = withScope {
    distinct(partitions.length)
  }

  /**
    * Return a new RDD that has exactly numPartitions partitions.
    * 返回一个新的RDD，该RDD具有完全的分区。
    *
    * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
    * a shuffle to redistribute data.
    * 在此RDD中可以增加或减少并行度。在内部，这使用洗牌来重新分配数据。
    *
    * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
    * which can avoid performing a shuffle.
    *
    * 如果您正在减少这个RDD中的分区数量，可以考虑使用“合并coalesce”，这可以避免进行洗牌。
    *
    * 优化：
    * 当要对 rdd 进行重新分片时，如果目标片区数量小于当前片区数量，那么用 coalesce，不要用 repartition。
    * 因为这里默认shuffle=true是需要进行洗牌操作的。
    */
  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    coalesce(numPartitions, shuffle = true)
  }

  /**
    * Return a new RDD that is reduced into `numPartitions` partitions.
    * 返回一个新的RDD，该RDD被reduce为“numPartitions”个分区。（发生reduce就会发生洗牌操作）
    *
    * This results in a narrow dependency, e.g. if you go from 1000 partitions
    * to 100 partitions, there will not be a shuffle, instead each of the 100
    * new partitions will claim 10 of the current partitions. If a larger number
    * of partitions is requested, it will stay at the current number of partitions.
    *
    * 这将导致一个狭窄的依赖关系，例如，如果您从1000个分区到100个分区，那么将不会有一个洗牌，
    * 相反，如果您从10个分区到1000个分区。如果请求更多的分区，它将停留在当前分区的数量上。
    *
    * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
    * this may result in your computation taking place on fewer nodes than
    * you like (e.g. one node in the case of numPartitions = 1). To avoid this,
    * you can pass shuffle = true. This will add a shuffle step, but means the
    * current upstream partitions will be executed in parallel (per whatever
    * the current partitioning is).
    *
    * 但是，如果您正在进行一个激烈的合并，例如:to numPartitions = 1，这可能会导致您的计算
    * 发生在比您喜欢的节点更少的节点上(例如，numPartitions = 1中的一个节点)。为了避免这个问题，
    * 您可以通过shuffle = true。这将添加一个shuffle步骤，但是意味着当前的上游分区将并行执行
    * (无论当前分区是什么)。
    *
    * @note With shuffle = true, you can actually coalesce to a larger number
    * of partitions. This is useful if you have a small number of partitions,
    * say 100, potentially with a few partitions being abnormally large. Calling
    * coalesce(1000, shuffle = true) will result in 1000 partitions with the
    * data distributed using a hash partitioner. The optional partition coalescer
    * passed in must be serializable.
    *
    * 随着shuffle = true，您实际上可以合并到更多的分区。如果有一小部分分区，比如100，
    * 可能有几个分区异常大，这很有用。调用合并(1000,shuffle = true)将会导致1000个分区，
    * 数据分布使用散列分配程序。通过的可选分区合并器必须是可序列化的。
    *
    * 优化：
    * 当要对 rdd 进行重新分片时，如果目标片区数量小于当前片区数量，那么用 coalesce，不要用 repartition。
    * 因为这里默认shuffle=false是不需要进行洗牌操作的。
    */
  def coalesce(numPartitions: Int, shuffle: Boolean = false,
               partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
              (implicit ord: Ordering[T] = null)
  : RDD[T] = withScope {
    // 分区数必须大于0
    require(numPartitions > 0, s"Number of partitions ($numPartitions) must be positive.")
    // 不使用repartition(shuffle = true)  方法
    if (shuffle) {
      /** Distributes elements evenly across output partitions, starting from a random partition.
        * 将元素均匀地分布在输出分区上，从一个随机分区开始。
        * */
      val distributePartition = (index: Int, items: Iterator[T]) => {
        var position = (new Random(index)).nextInt(numPartitions)
        items.map { t =>
          // Note that the hash code of the key will just be the key itself. The HashPartitioner
          // will mod it with the number of total partitions.
          //
          // 注意，键的哈希代码本身就是关键字。hashpartiator将使用总分区的数量对其进行mod。
          position = position + 1
          (position, t)
        }
      } : Iterator[(Int, T)]

      // include a shuffle step so that our upstream tasks are still distributed
      new CoalescedRDD(
        new ShuffledRDD[Int, T, T](mapPartitionsWithIndex(distributePartition),
          new HashPartitioner(numPartitions)),
        numPartitions,
        partitionCoalescer).values
    } else {
      // 不使用repartition(shuffle = false)  方法,这里不洗牌，没有洗牌的默认操作
      new CoalescedRDD(this, numPartitions, partitionCoalescer)
    }
  }

  /**
    * Return a sampled subset of this RDD.
    *
    * @param withReplacement can elements be sampled multiple times (replaced when sampled out)
    * @param fraction expected size of the sample as a fraction of this RDD's size
    *  without replacement: probability that each element is chosen; fraction must be [0, 1]
    *  with replacement: expected number of times each element is chosen; fraction must be greater
    *  than or equal to 0
    * @param seed seed for the random number generator
    *
    * @note This is NOT guaranteed to provide exactly the fraction of the count
    * of the given [[RDD]].
    */
  def sample(
              withReplacement: Boolean,
              fraction: Double,
              seed: Long = Utils.random.nextLong): RDD[T] = {
    require(fraction >= 0,
      s"Fraction must be nonnegative, but got ${fraction}")

    withScope {
      require(fraction >= 0.0, "Negative fraction value: " + fraction)
      if (withReplacement) {
        new PartitionwiseSampledRDD[T, T](this, new PoissonSampler[T](fraction), true, seed)
      } else {
        new PartitionwiseSampledRDD[T, T](this, new BernoulliSampler[T](fraction), true, seed)
      }
    }
  }

  /**
    * Randomly splits this RDD with the provided weights.
    *
    * @param weights weights for splits, will be normalized if they don't sum to 1
    * @param seed random seed
    *
    * @return split RDDs in an array
    */
  def randomSplit(
                   weights: Array[Double],
                   seed: Long = Utils.random.nextLong): Array[RDD[T]] = {
    require(weights.forall(_ >= 0),
      s"Weights must be nonnegative, but got ${weights.mkString("[", ",", "]")}")
    require(weights.sum > 0,
      s"Sum of weights must be positive, but got ${weights.mkString("[", ",", "]")}")

    withScope {
      val sum = weights.sum
      val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
      normalizedCumWeights.sliding(2).map { x =>
        randomSampleWithRange(x(0), x(1), seed)
      }.toArray
    }
  }


  /**
    * Internal method exposed for Random Splits in DataFrames. Samples an RDD given a probability
    * range.
    * @param lb lower bound to use for the Bernoulli sampler
    * @param ub upper bound to use for the Bernoulli sampler
    * @param seed the seed for the Random number generator
    * @return A random sub-sample of the RDD without replacement.
    */
  private[spark] def randomSampleWithRange(lb: Double, ub: Double, seed: Long): RDD[T] = {
    this.mapPartitionsWithIndex( { (index, partition) =>
      val sampler = new BernoulliCellSampler[T](lb, ub)
      sampler.setSeed(seed + index)
      sampler.sample(partition)
    }, preservesPartitioning = true)
  }

  /**
    * Return a fixed-size sampled subset of this RDD in an array
    *
    * @param withReplacement whether sampling is done with replacement
    * @param num size of the returned sample
    * @param seed seed for the random number generator
    * @return sample of specified size in an array
    *
    * @note this method should only be used if the resulting array is expected to be small, as
    * all the data is loaded into the driver's memory.
    */
  def takeSample(
                  withReplacement: Boolean,
                  num: Int,
                  seed: Long = Utils.random.nextLong): Array[T] = withScope {
    val numStDev = 10.0

    require(num >= 0, "Negative number of elements requested")
    require(num <= (Int.MaxValue - (numStDev * math.sqrt(Int.MaxValue)).toInt),
      "Cannot support a sample size > Int.MaxValue - " +
        s"$numStDev * math.sqrt(Int.MaxValue)")

    if (num == 0) {
      new Array[T](0)
    } else {
      val initialCount = this.count()
      if (initialCount == 0) {
        new Array[T](0)
      } else {
        val rand = new Random(seed)
        if (!withReplacement && num >= initialCount) {
          Utils.randomizeInPlace(this.collect(), rand)
        } else {
          val fraction = SamplingUtils.computeFractionForSampleSize(num, initialCount,
            withReplacement)
          var samples = this.sample(withReplacement, fraction, rand.nextInt()).collect()

          // If the first sample didn't turn out large enough, keep trying to take samples;
          // this shouldn't happen often because we use a big multiplier for the initial size
          var numIters = 0
          while (samples.length < num) {
            logWarning(s"Needed to re-sample due to insufficient sample size. Repeat #$numIters")
            samples = this.sample(withReplacement, fraction, rand.nextInt()).collect()
            numIters += 1
          }
          Utils.randomizeInPlace(samples, rand).take(num)
        }
      }
    }
  }

  /**
    * Return the union of this RDD and another one. Any identical elements will appear multiple
    * times (use `.distinct()` to eliminate them).
    */
  def union(other: RDD[T]): RDD[T] = withScope {
    sc.union(this, other)
  }

  /**
    * Return the union of this RDD and another one. Any identical elements will appear multiple
    * times (use `.distinct()` to eliminate them).
    */
  def ++(other: RDD[T]): RDD[T] = withScope {
    this.union(other)
  }

  /**
    * Return this RDD sorted by the given key function.
    */
  def sortBy[K](
                 f: (T) => K,
                 ascending: Boolean = true,
                 numPartitions: Int = this.partitions.length)
               (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = withScope {
    this.keyBy[K](f)
      .sortByKey(ascending, numPartitions)
      .values
  }

  /**
    * Return the intersection of this RDD and another one. The output will not contain any duplicate
    * elements, even if the input RDDs did.
    *
    * @note This method performs a shuffle internally.
    */
  def intersection(other: RDD[T]): RDD[T] = withScope {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)))
      .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
      .keys
  }

  /**
    * Return the intersection of this RDD and another one. The output will not contain any duplicate
    * elements, even if the input RDDs did.
    *
    * @note This method performs a shuffle internally.
    *
    * @param partitioner Partitioner to use for the resulting RDD
    */
  def intersection(
                    other: RDD[T],
                    partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)), partitioner)
      .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
      .keys
  }

  /**
    * Return the intersection of this RDD and another one. The output will not contain any duplicate
    * elements, even if the input RDDs did.  Performs a hash partition across the cluster
    *
    * @note This method performs a shuffle internally.
    *
    * @param numPartitions How many partitions to use in the resulting RDD
    */
  def intersection(other: RDD[T], numPartitions: Int): RDD[T] = withScope {
    intersection(other, new HashPartitioner(numPartitions))
  }

  /**
    * Return an RDD created by coalescing all elements within each partition into an array.
    */
  def glom(): RDD[Array[T]] = withScope {
    new MapPartitionsRDD[Array[T], T](this, (context, pid, iter) => Iterator(iter.toArray))
  }

  /**
    * Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of
    * elements (a, b) where a is in `this` and b is in `other`.
    *
    * 返回这个RDD的笛卡尔积和另一个，也就是，所有对元素(a, b)的RDD，其中a在“这个”中，而b在“另一个”中。
    *
    * >>>  rdd = sc.parallelize([1, 2])
    * >>> sorted(rdd.cartesian(rdd).collect())
    * [(1, 1), (1, 2), (2, 1), (2, 2)]
    */
  def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)] = withScope {
    new CartesianRDD(sc, this, other)
  }

  /**
    * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
    * mapping to that key. The ordering of elements within each group is not guaranteed, and
    * may even differ each time the resulting RDD is evaluated.
    *
    * @note This operation may be very expensive. If you are grouping in order to perform an
    * aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
    * or `PairRDDFunctions.reduceByKey` will provide much better performance.
    */
  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope {
    groupBy[K](f, defaultPartitioner(this))
  }

  /**
    * Return an RDD of grouped elements. Each group consists of a key and a sequence of elements
    * mapping to that key. The ordering of elements within each group is not guaranteed, and
    * may even differ each time the resulting RDD is evaluated.
    *
    * @note This operation may be very expensive. If you are grouping in order to perform an
    * aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
    * or `PairRDDFunctions.reduceByKey` will provide much better performance.
    */
  def groupBy[K](
                  f: T => K,
                  numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope {
    groupBy(f, new HashPartitioner(numPartitions))
  }

  /**
    * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
    * mapping to that key. The ordering of elements within each group is not guaranteed, and
    * may even differ each time the resulting RDD is evaluated.
    *
    * @note This operation may be very expensive. If you are grouping in order to perform an
    * aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
    * or `PairRDDFunctions.reduceByKey` will provide much better performance.
    */
  def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null)
  : RDD[(K, Iterable[T])] = withScope {
    val cleanF = sc.clean(f)
    this.map(t => (cleanF(t), t)).groupByKey(p)
  }

  /**
    * Return an RDD created by piping elements to a forked external process.
    */
  def pipe(command: String): RDD[String] = withScope {
    // Similar to Runtime.exec(), if we are given a single string, split it into words
    // using a standard StringTokenizer (i.e. by spaces)
    pipe(PipedRDD.tokenize(command))
  }

  /**
    * Return an RDD created by piping elements to a forked external process.
    */
  def pipe(command: String, env: Map[String, String]): RDD[String] = withScope {
    // Similar to Runtime.exec(), if we are given a single string, split it into words
    // using a standard StringTokenizer (i.e. by spaces)
    pipe(PipedRDD.tokenize(command), env)
  }

  /**
    * Return an RDD created by piping elements to a forked external process. The resulting RDD
    * is computed by executing the given process once per partition. All elements
    * of each input partition are written to a process's stdin as lines of input separated
    * by a newline. The resulting partition consists of the process's stdout output, with
    * each line of stdout resulting in one element of the output partition. A process is invoked
    * even for empty partitions.
    *
    * The print behavior can be customized by providing two functions.
    *
    * @param command command to run in forked process.
    * @param env environment variables to set.
    * @param printPipeContext Before piping elements, this function is called as an opportunity
    *                         to pipe context data. Print line function (like out.println) will be
    *                         passed as printPipeContext's parameter.
    * @param printRDDElement Use this function to customize how to pipe elements. This function
    *                        will be called with each RDD element as the 1st parameter, and the
    *                        print line function (like out.println()) as the 2nd parameter.
    *                        An example of pipe the RDD data of groupBy() in a streaming way,
    *                        instead of constructing a huge String to concat all the elements:
    *                        {{{
    *                        def printRDDElement(record:(String, Seq[String]), f:String=>Unit) =
    *                          for (e <- record._2) {f(e)}
    *                        }}}
    * @param separateWorkingDir Use separate working directories for each task.
    * @param bufferSize Buffer size for the stdin writer for the piped process.
    * @param encoding Char encoding used for interacting (via stdin, stdout and stderr) with
    *                 the piped process
    * @return the result RDD
    */
  def pipe(
            command: Seq[String],
            env: Map[String, String] = Map(),
            printPipeContext: (String => Unit) => Unit = null,
            printRDDElement: (T, String => Unit) => Unit = null,
            separateWorkingDir: Boolean = false,
            bufferSize: Int = 8192,
            encoding: String = Codec.defaultCharsetCodec.name): RDD[String] = withScope {
    new PipedRDD(this, command, env,
      if (printPipeContext ne null) sc.clean(printPipeContext) else null,
      if (printRDDElement ne null) sc.clean(printRDDElement) else null,
      separateWorkingDir,
      bufferSize,
      encoding)
  }

  /**
    * Return a new RDD by applying a function to each partition of this RDD.
    *
    * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
    * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
    */
  def mapPartitions[U: ClassTag](
                                  f: Iterator[T] => Iterator[U],
                                  preservesPartitioning: Boolean = false): RDD[U] = withScope {
    val cleanedF = sc.clean(f)
    new MapPartitionsRDD(
      this,
      (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(iter),
      preservesPartitioning)
  }

  /**
    * [performance] Spark's internal mapPartitionsWithIndex method that skips closure cleaning.
    * It is a performance API to be used carefully only if we are sure that the RDD elements are
    * serializable and don't require closure cleaning.
    *
    * @param preservesPartitioning indicates whether the input function preserves the partitioner,
    * which should be `false` unless this is a pair RDD and the input function doesn't modify
    * the keys.
    */
  private[spark] def mapPartitionsWithIndexInternal[U: ClassTag](
                                                                  f: (Int, Iterator[T]) => Iterator[U],
                                                                  preservesPartitioning: Boolean = false): RDD[U] = withScope {
    new MapPartitionsRDD(
      this,
      (context: TaskContext, index: Int, iter: Iterator[T]) => f(index, iter),
      preservesPartitioning)
  }

  /**
    * [performance] Spark's internal mapPartitions method that skips closure cleaning.
    */
  private[spark] def mapPartitionsInternal[U: ClassTag](
                                                         f: Iterator[T] => Iterator[U],
                                                         preservesPartitioning: Boolean = false): RDD[U] = withScope {
    new MapPartitionsRDD(
      this,
      (context: TaskContext, index: Int, iter: Iterator[T]) => f(iter),
      preservesPartitioning)
  }

  /**
    * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
    * of the original partition.
    *
    * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
    * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
    */
  def mapPartitionsWithIndex[U: ClassTag](
                                           f: (Int, Iterator[T]) => Iterator[U],
                                           preservesPartitioning: Boolean = false): RDD[U] = withScope {
    val cleanedF = sc.clean(f)
    new MapPartitionsRDD(
      this,
      (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(index, iter),
      preservesPartitioning)
  }

  /**
    * Zips this RDD with another one, returning key-value pairs with the first element in each RDD,
    * second element in each RDD, etc. Assumes that the two RDDs have the *same number of
    * partitions* and the *same number of elements in each partition* (e.g. one was made through
    * a map on the other).
    */
  def zip[U: ClassTag](other: RDD[U]): RDD[(T, U)] = withScope {
    zipPartitions(other, preservesPartitioning = false) { (thisIter, otherIter) =>
      new Iterator[(T, U)] {
        def hasNext: Boolean = (thisIter.hasNext, otherIter.hasNext) match {
          case (true, true) => true
          case (false, false) => false
          case _ => throw new SparkException("Can only zip RDDs with " +
            "same number of elements in each partition")
        }
        def next(): (T, U) = (thisIter.next(), otherIter.next())
      }
    }
  }

  /**
    * Zip this RDD's partitions with one (or more) RDD(s) and return a new RDD by
    * applying a function to the zipped partitions. Assumes that all the RDDs have the
    * *same number of partitions*, but does *not* require them to have the same number
    * of elements in each partition.
    */
  def zipPartitions[B: ClassTag, V: ClassTag]
  (rdd2: RDD[B], preservesPartitioning: Boolean)
  (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD2(sc, sc.clean(f), this, rdd2, preservesPartitioning)
  }

  def zipPartitions[B: ClassTag, V: ClassTag]
  (rdd2: RDD[B])
  (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = withScope {
    zipPartitions(rdd2, preservesPartitioning = false)(f)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
  (rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)
  (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD3(sc, sc.clean(f), this, rdd2, rdd3, preservesPartitioning)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
  (rdd2: RDD[B], rdd3: RDD[C])
  (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = withScope {
    zipPartitions(rdd2, rdd3, preservesPartitioning = false)(f)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
  (rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D], preservesPartitioning: Boolean)
  (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD4(sc, sc.clean(f), this, rdd2, rdd3, rdd4, preservesPartitioning)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
  (rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])
  (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = withScope {
    zipPartitions(rdd2, rdd3, rdd4, preservesPartitioning = false)(f)
  }


  // Actions (launch a job to return a value to the user program)

  /**
    * Applies a function f to all elements of this RDD.
    */
  def foreach(f: T => Unit): Unit = withScope {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
  }

  /**
    * Applies a function f to each partition of this RDD.
    */
  def foreachPartition(f: Iterator[T] => Unit): Unit = withScope {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => cleanF(iter))
  }

  /**
    * Return an array that contains all of the elements in this RDD.
    *
    * @note This method should only be used if the resulting array is expected to be small, as
    * all the data is loaded into the driver's memory.
    */
  def collect(): Array[T] = withScope {
    // RDD的collect方法调用了SparkContext的runJob。
    // 调用过程RDD.runJob==>SparkContext.runJob()==>SparkContext.runJob()==>SparkContext.runJob()==>SparkContext.runJob()
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
  }

  /**
    * Return an iterator that contains all of the elements in this RDD.
    *
    * The iterator will consume as much memory as the largest partition in this RDD.
    *
    * @note This results in multiple Spark jobs, and if the input RDD is the result
    * of a wide transformation (e.g. join with different partitioners), to avoid
    * recomputing the input RDD should be cached first.
    */
  def toLocalIterator: Iterator[T] = withScope {
    def collectPartition(p: Int): Array[T] = {
      sc.runJob(this, (iter: Iterator[T]) => iter.toArray, Seq(p)).head
    }
    (0 until partitions.length).iterator.flatMap(i => collectPartition(i))
  }

  /**
    * Return an RDD that contains all matching values by applying `f`.
    */
  def collect[U: ClassTag](f: PartialFunction[T, U]): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    filter(cleanF.isDefinedAt).map(cleanF)
  }

  /**
    * Return an RDD with the elements from `this` that are not in `other`.
    *
    * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
    * RDD will be &lt;= us.
    */
  def subtract(other: RDD[T]): RDD[T] = withScope {
    subtract(other, partitioner.getOrElse(new HashPartitioner(partitions.length)))
  }

  /**
    * Return an RDD with the elements from `this` that are not in `other`.
    */
  def subtract(other: RDD[T], numPartitions: Int): RDD[T] = withScope {
    subtract(other, new HashPartitioner(numPartitions))
  }

  /**
    * Return an RDD with the elements from `this` that are not in `other`.
    */
  def subtract(
                other: RDD[T],
                p: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    if (partitioner == Some(p)) {
      // Our partitioner knows how to handle T (which, since we have a partitioner, is
      // really (K, V)) so make a new Partitioner that will de-tuple our fake tuples
      val p2 = new Partitioner() {
        override def numPartitions: Int = p.numPartitions
        override def getPartition(k: Any): Int = p.getPartition(k.asInstanceOf[(Any, _)]._1)
      }
      // Unfortunately, since we're making a new p2, we'll get ShuffleDependencies
      // anyway, and when calling .keys, will not have a partitioner set, even though
      // the SubtractedRDD will, thanks to p2's de-tupled partitioning, already be
      // partitioned by the right/real keys (e.g. p).
      this.map(x => (x, null)).subtractByKey(other.map((_, null)), p2).keys
    } else {
      this.map(x => (x, null)).subtractByKey(other.map((_, null)), p).keys
    }
  }

  /**
    * Reduces the elements of this RDD using the specified commutative and
    * associative binary operator.
    */
  def reduce(f: (T, T) => T): T = withScope {
    val cleanF = sc.clean(f)
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      } else {
        None
      }
    }
    var jobResult: Option[T] = None
    val mergeResult = (index: Int, taskResult: Option[T]) => {
      if (taskResult.isDefined) {
        jobResult = jobResult match {
          case Some(value) => Some(f(value, taskResult.get))
          case None => taskResult
        }
      }
    }
    sc.runJob(this, reducePartition, mergeResult)
    // Get the final result out of our Option, or throw an exception if the RDD was empty
    jobResult.getOrElse(throw new UnsupportedOperationException("empty collection"))
  }

  /**
    * Reduces the elements of this RDD in a multi-level tree pattern.
    *
    * 在多级树模式中减少这个RDD的元素。
    *
    * @param depth suggested depth of the tree (default: 2)
    * @see [[org.apache.spark.rdd.RDD#reduce]]
    *
    * 优化：
    * 尽量使用treeReduce而不使用reduce
    * 参考博客：https://blog.csdn.net/qq_21383435/article/details/79884129
    */
  def treeReduce(f: (T, T) => T, depth: Int = 2): T = withScope {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
    val cleanF = context.clean(f)
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      } else {
        None
      }
    }
    val partiallyReduced = mapPartitions(it => Iterator(reducePartition(it)))
    val op: (Option[T], Option[T]) => Option[T] = (c, x) => {
      if (c.isDefined && x.isDefined) {
        Some(cleanF(c.get, x.get))
      } else if (c.isDefined) {
        c
      } else if (x.isDefined) {
        x
      } else {
        None
      }
    }
    partiallyReduced.treeAggregate(Option.empty[T])(op, op, depth)
      .getOrElse(throw new UnsupportedOperationException("empty collection"))
  }

  /**
    * Aggregate the elements of each partition, and then the results for all the partitions, using a
    * given associative function and a neutral "zero value". The function
    * op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object
    * allocation; however, it should not modify t2.
    *
    * This behaves somewhat differently from fold operations implemented for non-distributed
    * collections in functional languages like Scala. This fold operation may be applied to
    * partitions individually, and then fold those results into the final result, rather than
    * apply the fold to each element sequentially in some defined ordering. For functions
    * that are not commutative, the result may differ from that of a fold applied to a
    * non-distributed collection.
    *
    * @param zeroValue the initial value for the accumulated result of each partition for the `op`
    *                  operator, and also the initial value for the combine results from different
    *                  partitions for the `op` operator - this will typically be the neutral
    *                  element (e.g. `Nil` for list concatenation or `0` for summation)
    * @param op an operator used to both accumulate results within a partition and combine results
    *                  from different partitions
    */
  def fold(zeroValue: T)(op: (T, T) => T): T = withScope {
    // Clone the zero value since we will also be serializing it as part of tasks
    var jobResult = Utils.clone(zeroValue, sc.env.closureSerializer.newInstance())
    val cleanOp = sc.clean(op)
    val foldPartition = (iter: Iterator[T]) => iter.fold(zeroValue)(cleanOp)
    val mergeResult = (index: Int, taskResult: T) => jobResult = op(jobResult, taskResult)
    sc.runJob(this, foldPartition, mergeResult)
    jobResult
  }

  /**
    * Aggregate the elements of each partition, and then the results for all the partitions, using
    * given combine functions and a neutral "zero value". This function can return a different result
    * type, U, than the type of this RDD, T. Thus, we need one operation for merging a T into an U
    * and one operation for merging two U's, as in scala.TraversableOnce. Both of these functions are
    * allowed to modify and return their first argument instead of creating a new U to avoid memory
    * allocation.
    *
    * @param zeroValue the initial value for the accumulated result of each partition for the
    *                  `seqOp` operator, and also the initial value for the combine results from
    *                  different partitions for the `combOp` operator - this will typically be the
    *                  neutral element (e.g. `Nil` for list concatenation or `0` for summation)
    * @param seqOp an operator used to accumulate results within a partition
    * @param combOp an associative operator used to combine results from different partitions
    */
  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = withScope {
    // Clone the zero value since we will also be serializing it as part of tasks
    var jobResult = Utils.clone(zeroValue, sc.env.serializer.newInstance())
    val cleanSeqOp = sc.clean(seqOp)
    val cleanCombOp = sc.clean(combOp)
    val aggregatePartition = (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp)
    val mergeResult = (index: Int, taskResult: U) => jobResult = combOp(jobResult, taskResult)
    sc.runJob(this, aggregatePartition, mergeResult)
    jobResult
  }

  /**
    * Aggregates the elements of this RDD in a multi-level tree pattern.
    *
    * @param depth suggested depth of the tree (default: 2)
    * @see [[org.apache.spark.rdd.RDD#aggregate]]
    */
  def treeAggregate[U: ClassTag](zeroValue: U)(
    seqOp: (U, T) => U,
    combOp: (U, U) => U,
    depth: Int = 2): U = withScope {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
    if (partitions.length == 0) {
      Utils.clone(zeroValue, context.env.closureSerializer.newInstance())
    } else {
      val cleanSeqOp = context.clean(seqOp)
      val cleanCombOp = context.clean(combOp)
      val aggregatePartition =
        (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp)
      var partiallyAggregated = mapPartitions(it => Iterator(aggregatePartition(it)))
      var numPartitions = partiallyAggregated.partitions.length
      val scale = math.max(math.ceil(math.pow(numPartitions, 1.0 / depth)).toInt, 2)
      // If creating an extra level doesn't help reduce
      // the wall-clock time, we stop tree aggregation.

      // Don't trigger TreeAggregation when it doesn't save wall-clock time
      while (numPartitions > scale + math.ceil(numPartitions.toDouble / scale)) {
        numPartitions /= scale
        val curNumPartitions = numPartitions
        partiallyAggregated = partiallyAggregated.mapPartitionsWithIndex {
          (i, iter) => iter.map((i % curNumPartitions, _))
        }.reduceByKey(new HashPartitioner(curNumPartitions), cleanCombOp).values
      }
      partiallyAggregated.reduce(cleanCombOp)
    }
  }

  /**
    * Return the number of elements in the RDD.
    * 返回RDD的元素数量
    */
  // 通过sc提交Job
  def count(): Long = sc.runJob(this, Utils.getIteratorSize _).sum

  /**
    * Approximate version of count() that returns a potentially incomplete result
    * within a timeout, even if not all tasks have finished.
    *
    * 对count()返回结果可能不完全在一个超时的近似版本，即使不是所有的任务都完成了。
    *
    * The confidence is the probability that the error bounds of the result will
    * contain the true value. That is, if countApprox were called repeatedly
    * with confidence 0.9, we would expect 90% of the results to contain the
    * true count. The confidence must be in the range [0,1] or an exception will
    * be thrown.
    *
    * 置信度是结果的误差范围包含真实值的概率。那就是，如果countapprox与信心0.9多次打电话，
    * 我们希望90%的结果包含真正的计数。信心必须在范围[0,1]或将抛出一个异常。
    *
    * @param timeout maximum time to wait for the job, in milliseconds
    * @param confidence the desired statistical confidence in the result
    * @return a potentially incomplete result, with error bounds
    */
  def countApprox(
                   timeout: Long,
                   confidence: Double = 0.95): PartialResult[BoundedDouble] = withScope {
    require(0.0 <= confidence && confidence <= 1.0, s"confidence ($confidence) must be in [0,1]")
    val countElements: (TaskContext, Iterator[T]) => Long = { (ctx, iter) =>
      var result = 0L
      while (iter.hasNext) {
        result += 1L
        iter.next()
      }
      result
    }
    val evaluator = new CountEvaluator(partitions.length, confidence)
    sc.runApproximateJob(this, countElements, evaluator, timeout)
  }

  /**
    * Return the count of each unique value in this RDD as a local map of (value, count) pairs.
    *
    * 返回每个RDD的不重复的value总数作为一个本地map集合（value、数量）对。
    *
    * @note This method should only be used if the resulting map is expected to be small, as
    * the whole thing is loaded into the driver's memory.
    * To handle very large results, consider using
    *
    * 注意，只有当整个结果集合map非常小可以加载到驱动程序内存中时，这个方法才应该被使用。
    * 如果要处理非常大的结果，考虑使用
    *
    * {{{
    * rdd.map(x => (x, 1L)).reduceByKey(_ + _)
    * }}}
    *
    * , which returns an RDD[T, Long] instead of a map.
    */
  def countByValue()(implicit ord: Ordering[T] = null): Map[T, Long] = withScope {
    map(value => (value, null)).countByKey()
  }

  /**
    * Approximate version of countByValue().
    * countbyvalue()近似版本的。
    *
    * @param timeout maximum time to wait for the job, in milliseconds  job的最大等待时间
    * @param confidence the desired statistical confidence in the result 所需的统计信心的结果
    * @return a potentially incomplete result, with error bounds  一个潜在的不完全的结果，误差范围
    */
  def countByValueApprox(timeout: Long, confidence: Double = 0.95)
                        (implicit ord: Ordering[T] = null)
  : PartialResult[Map[T, BoundedDouble]] = withScope {
    require(0.0 <= confidence && confidence <= 1.0, s"confidence ($confidence) must be in [0,1]")
    if (elementClassTag.runtimeClass.isArray) {
      throw new SparkException("countByValueApprox() does not support arrays")
    }
    val countPartition: (TaskContext, Iterator[T]) => OpenHashMap[T, Long] = { (ctx, iter) =>
      val map = new OpenHashMap[T, Long]
      iter.foreach {
        t => map.changeValue(t, 1L, _ + 1L)
      }
      map
    }
    val evaluator = new GroupedCountEvaluator[T](partitions.length, confidence)
    sc.runApproximateJob(this, countPartition, evaluator, timeout)
  }

  /**
    * Return approximate number of distinct elements in the RDD.
    *
    * 返回在RDD不同元素的近似值。
    *
    * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
    * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
    * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
    *
    * 该算法采用的是基于streamlib实施“HyperLogLog实践：一种艺术的基数估计算法”算法的工程，可从
    * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.得到答案
    *
    * The relative accuracy is approximately `1.054 / sqrt(2^p)`. Setting a nonzero (`sp` is greater
    * than `p`) would trigger sparse representation of registers, which may reduce the memory
    * consumption and increase accuracy when the cardinality is small.
    *
    * 相对精度约` 1.054 / sqrt（2 ^ P）`。设置一个非零（`sp`大于“p”）会触发寄存器的稀疏表示，
    * 当基数很小时，可以减少内存消耗并提高准确性。
    *
    * @param p The precision value for the normal set.
    *          `p` must be a value between 4 and `sp` if `sp` is not zero (32 max).
    * @param sp The precision value for the sparse set, between 0 and 32.
    *           If `sp` equals 0, the sparse representation is skipped.
    */
  def countApproxDistinct(p: Int, sp: Int): Long = withScope {
    require(p >= 4, s"p ($p) must be >= 4")
    require(sp <= 32, s"sp ($sp) must be <= 32")
    require(sp == 0 || p <= sp, s"p ($p) cannot be greater than sp ($sp)")
    val zeroCounter = new HyperLogLogPlus(p, sp)
    aggregate(zeroCounter)(
      (hll: HyperLogLogPlus, v: T) => {
        hll.offer(v)
        hll
      },
      (h1: HyperLogLogPlus, h2: HyperLogLogPlus) => {
        h1.addAll(h2)
        h1
      }).cardinality()
  }

  /**
    * Return approximate number of distinct elements in the RDD.
    *
    * 返回在RDD不同元素的近似值。
    *
    * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
    * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
    * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
    *
    * 相对精度约` 1.054 / sqrt（2 ^ P）`。设置一个非零（`sp`大于“p”）会触发寄存器的稀疏表示，
    * 当基数很小时，可以减少内存消耗并提高准确性。
    *
    * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
    *                   It must be greater than 0.000017.
    *                   相对精度。较小的值创建需要更多空间的计数器。它必须大于0.000017。
    */
  def countApproxDistinct(relativeSD: Double = 0.05): Long = withScope {
    require(relativeSD > 0.000017, s"accuracy ($relativeSD) must be greater than 0.000017")
    val p = math.ceil(2.0 * math.log(1.054 / relativeSD) / math.log(2)).toInt
    countApproxDistinct(if (p < 4) 4 else p, 0)
  }

  /**
    * Zips this RDD with its element indices. The ordering is first based on the partition index
    * and then the ordering of items within each partition. So the first item in the first
    * partition gets index 0, and the last item in the last partition receives the largest index.
    *
    * Zips this RDD with its element indices。第一顺序是根据分区的下表进行顺序排序然后每个项目中的分区中排序。
    * 所以第一项第一分区变指数0，最后一个分区中的最后一项收到最大的索引。
    *
    * This is similar to Scala's zipWithIndex but it uses Long instead of Int as the index type.
    * This method needs to trigger a spark job when this RDD contains more than one partitions.
    *
    * 这是类似于 Scala的zipwithindex但它使用long类型代替Int为下标数值类型。
    * 这种方法需要触发spark job当RDD包含多个分区。
    *
    * @note Some RDDs, such as those returned by groupBy(), do not guarantee order of
    * elements in a partition. The index assigned to each element is therefore not guaranteed,
    * and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee
    * the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
    *
    * @注意一些RDD，如那些由groupby()，不保证划分中的元素顺序。分配给每一个元素的索引是没有保证的，
    *         甚至如果RDD是重新评估的变化。如果一个固定的顺序必须保证相同的指标任务，
    *         你应该根据sortbykey()排序RDD或保存到一个文件。
    */
  def zipWithIndex(): RDD[(T, Long)] = withScope {
    new ZippedWithIndexRDD(this)
  }

  /**
    * Zips this RDD with generated unique Long ids. Items in the kth partition will get ids k, n+k,
    * 2*n+k, ..., where n is the number of partitions. So there may exist gaps, but this method
    * won't trigger a spark job, which is different from [[org.apache.spark.rdd.RDD#zipWithIndex]].
    *
    * Zips RDD保证了一个独一无二的Long类型的ID,Items在KTH partition get IDS（k，n + k，2×n + k，…，
    * n是partitions分区数。所以有可能存在差距，但这方法不会触发一个spark job，
    * 这是不同的从 [[org.apache.spark.rdd.RDD#zipWithIndex]].
    *
    * @note Some RDDs, such as those returned by groupBy(), do not guarantee order of
    * elements in a partition. The unique ID assigned to each element is therefore not guaranteed,
    * and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee
    * the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
    *
    * @注意一些RDD，如那些由groupby()，不保证划分中的元素顺序。分配给每一个元素的索引是没有保证的，
    *         甚至如果RDD是重新评估的变化。如果一个固定的顺序必须保证相同的指标任务，
    *         你应该根据sortbykey()排序RDD或保存到一个文件。
    */
  def zipWithUniqueId(): RDD[(T, Long)] = withScope {
    val n = this.partitions.length.toLong
    this.mapPartitionsWithIndex { case (k, iter) =>
      Utils.getIteratorZipWithIndex(iter, 0L).map { case (item, i) =>
        (item, i * n + k)
      }
    }
  }

  /**
    * Take the first num elements of the RDD. It works by first scanning one partition, and use the
    * results from that partition to estimate the number of additional partitions needed to satisfy
    * the limit.
    *
    * 获取RDD的第一个元素。它首先扫描一个分区，然后使用该分区的结果来估计需要满足的限制所需的额外分区的数量。
    *
    * @note This method should only be used if the resulting array is expected to be small, as
    * all the data is loaded into the driver's memory.
    *
    * @注意，如果结果数组很小可以把所有的数据被加载到驱动程序内存中时，可以使用此方法。
    *
    * @note Due to complications in the internal implementation, this method will raise
    * an exception if called on an RDD of `Nothing` or `Null`.
    *
    * @注意 由于在内部实现的并发症，这种方法将产生一个异常，如果调用一个RDD `Nothing`或`Null`.。
    *
    */
  def take(num: Int): Array[T] = withScope {
    val scaleUpFactor = Math.max(conf.getInt("spark.rdd.limit.scaleUpFactor", 4), 2)
    if (num == 0) {
      new Array[T](0)
    } else {
      val buf = new ArrayBuffer[T]
      val totalParts = this.partitions.length
      var partsScanned = 0
      while (buf.size < num && partsScanned < totalParts) {
        // The number of partitions to try in this iteration. It is ok for this number to be
        // greater than totalParts because we actually cap it at totalParts in runJob.
        // 在此迭代中要尝试的分区数。它是确定这个数是大于totalparts
        // 因为我们实际上限制在totalparts在runjob。
        var numPartsToTry = 1L
        if (partsScanned > 0) {
          // If we didn't find any rows after the previous iteration, quadruple and retry.
          // Otherwise, interpolate the number of partitions we need to try, but overestimate
          // it by 50%. We also cap the estimation in the end.
          // 如果我们在上一次迭代之后没有找到任何行，则四重重试。否则，我们需要尝试的分区数目，
          // 但估计值为50%。最后我们还进行了估算。
          if (buf.isEmpty) {
            numPartsToTry = partsScanned * scaleUpFactor
          } else {
            // the left side of max is >=1 whenever partsScanned >= 2
            numPartsToTry = Math.max((1.5 * num * partsScanned / buf.size).toInt - partsScanned, 1)
            numPartsToTry = Math.min(numPartsToTry, partsScanned * scaleUpFactor)
          }
        }

        val left = num - buf.size
        val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
        val res = sc.runJob(this, (it: Iterator[T]) => it.take(left).toArray, p)

        res.foreach(buf ++= _.take(num - buf.size))
        partsScanned += p.size
      }

      buf.toArray
    }
  }

  /**
    * Return the first element in this RDD.
    * 返回RDD的第一个元素
    */
  def first(): T = withScope {
    take(1) match {
      case Array(t) => t
      case _ => throw new UnsupportedOperationException("empty collection")
    }
  }

  /**
    * Returns the top k (largest) elements from this RDD as defined by the specified
    * implicit Ordering[T] and maintains the ordering. This does the opposite of
    *
    * 返回top K（最大）元素从RDD的指定的隐式排序[t]定义和维护有序。这正好相反。
    *
    * [[takeOrdered]]. For example:
    * {{{
    *   sc.parallelize(Seq(10, 4, 2, 12, 3)).top(1)
    *   // returns Array(12)
    *
    *   sc.parallelize(Seq(2, 3, 4, 5, 6)).top(2)
    *   // returns Array(6, 5)
    * }}}
    *
    * @note This method should only be used if the resulting array is expected to be small, as
    * all the data is loaded into the driver's memory.
    *
    * @注意，只有当所有的数据被加载到驱动程序内存中时，如果结果数组很小，只能使用此方法。
    *
    * @param num k, the number of top elements to return
    * @param ord the implicit ordering for T
    * @return an array of top elements
    */
  def top(num: Int)(implicit ord: Ordering[T]): Array[T] = withScope {
    takeOrdered(num)(ord.reverse)
  }

  /**
    * Returns the first k (smallest) elements from this RDD as defined by the specified
    * implicit Ordering[T] and maintains the ordering. This does the opposite of [[top]].
    *
    * 返回first K（最小）的元素从RDD的指定的隐式排序[t]定义和维护有序。这与[[top]]是相反。
    *
    * For example:
    * {{{
    *   sc.parallelize(Seq(10, 4, 2, 12, 3)).takeOrdered(1)
    *   // returns Array(2)
    *
    *   sc.parallelize(Seq(2, 3, 4, 5, 6)).takeOrdered(2)
    *   // returns Array(2, 3)
    * }}}
    *
    * @note This method should only be used if the resulting array is expected to be small, as
    * all the data is loaded into the driver's memory.
    *
    * @注意，只有当所有的数据被加载到驱动程序内存中时，如果结果数组很小，只能使用此方法。
    *
    * @param num k, the number of elements to return
    * @param ord the implicit ordering for T
    * @return an array of top elements
    */
  def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T] = withScope {
    if (num == 0) {
      Array.empty
    } else {
      val mapRDDs = mapPartitions { items =>
        // Priority keeps the largest elements, so let's reverse the ordering.
        val queue = new BoundedPriorityQueue[T](num)(ord.reverse)
        queue ++= collectionUtils.takeOrdered(items, num)(ord)
        Iterator.single(queue)
      }
      if (mapRDDs.partitions.length == 0) {
        Array.empty
      } else {
        mapRDDs.reduce { (queue1, queue2) =>
          queue1 ++= queue2
          queue1
        }.toArray.sorted(ord)
      }
    }
  }

  /**
    * Returns the max of this RDD as defined by the implicit Ordering[T].
    * 返回RDD最大的隐式Ordering[T].定义。
    * @return the maximum element of the RDD      返回RDD的最大结果
    * */
  def max()(implicit ord: Ordering[T]): T = withScope {
    this.reduce(ord.max)
  }

  /**
    * Returns the min of this RDD as defined by the implicit Ordering[T].
    * 返回RDD最小的隐式Ordering[T].定义。
    * @return the minimum element of the RDD    返回RDD的最小结果
    * */
  def min()(implicit ord: Ordering[T]): T = withScope {
    this.reduce(ord.min)
  }

  /**
    * @note Due to complications in the internal implementation, this method will raise an
    * exception if called on an RDD of `Nothing` or `Null`. This may be come up in practice
    * because, for example, the type of `parallelize(Seq())` is `RDD[Nothing]`.
    * (`parallelize(Seq())` should be avoided anyway in favor of `parallelize(Seq[T]())`.)
    *
    * @注意由于在内部实现的并发症，这种方法将产生一个异常，如果调用 `Nothing` 或 `Null`。
    *               这可能是因为在实践中，例如，类型 `parallelize(Seq())`是`RDD[Nothing]`.
    *               (`parallelize(Seq())`应该避免无论如何赞成`parallelize(Seq[T]())`
    *
    * @return true if and only if the RDD contains no elements at all. Note that an RDD
    *         may be empty even when it has at least 1 partition.
    *         返回真 若RDD没有包含任何元素。注意，RDD即使为空但是它至少有1个分区。
    */
  def isEmpty(): Boolean = withScope {
    partitions.length == 0 || take(1).length == 0
  }

  /**
    * Save this RDD as a text file, using string representations of elements.
    * 保存RDD为文本文件，使用元素的字符串表示。
    */
  def saveAsTextFile(path: String): Unit = withScope {
    // https://issues.apache.org/jira/browse/SPARK-2075
    //
    // NullWritable is a `Comparable` in Hadoop 1.+, so the compiler cannot find an implicit
    // Ordering for it and will use the default `null`. However, it's a `Comparable[NullWritable]`
    // in Hadoop 2.+, so the compiler will call the implicit `Ordering.ordered` method to create an
    // Ordering for `NullWritable`. That's why the compiler will generate different anonymous
    // classes for `saveAsTextFile` in Hadoop 1.+ and Hadoop 2.+.

    // NullWritable是`Comparable` Hadoop 1 +，所以编译器无法发现隐式排序，将使用默认的`null`。
    // 然而，这是一个`Comparable[NullWritable]`在 Hadoop 2 +，所以编译器会隐式`Ordering.ordered`
    // 方法创建一个排序 `NullWritable`.`。这就是为什么编译器会生成不同的 `saveAsTextFile`匿名类在
    //  Hadoop 1.+ 和 Hadoop 2.+.中
    //
    // Therefore, here we provide an explicit Ordering `null` to make sure the compiler generate
    // same bytecodes for `saveAsTextFile`.
    // 因此，在这里我们提供了一个明确的排序`null`确保编译器生成相同的字节码为了` saveastextfile `。
    val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
    val textClassTag = implicitly[ClassTag[Text]]
    val r = this.mapPartitions { iter =>
      val text = new Text()
      iter.map { x =>
        text.set(x.toString)
        (NullWritable.get(), text)
      }
    }
    RDD.rddToPairRDDFunctions(r)(nullWritableClassTag, textClassTag, null)
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
  }

  /**
    * Save this RDD as a compressed text file, using string representations of elements.
    * 保存该RDD作为压缩的文本文件，使用元素的字符串表示。
    */
  def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit = withScope {
    // https://issues.apache.org/jira/browse/SPARK-2075
    val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
    val textClassTag = implicitly[ClassTag[Text]]
    val r = this.mapPartitions { iter =>
      val text = new Text()
      iter.map { x =>
        text.set(x.toString)
        (NullWritable.get(), text)
      }
    }
    RDD.rddToPairRDDFunctions(r)(nullWritableClassTag, textClassTag, null)
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path, codec)
  }

  /**
    * Save this RDD as a SequenceFile of serialized objects.
    * 保存该RDD为经过序列化后对象的SequenceFile。
    */
  def saveAsObjectFile(path: String): Unit = withScope {
    this.mapPartitions(iter => iter.grouped(10).map(_.toArray))
      .map(x => (NullWritable.get(), new BytesWritable(Utils.serialize(x))))
      .saveAsSequenceFile(path)
  }

  /**
    * Creates tuples of the elements in this RDD by applying `f`.
    * 创建这个RDD的元素运用` F `元组。
    */
  def keyBy[K](f: T => K): RDD[(K, T)] = withScope {
    val cleanedF = sc.clean(f)
    map(x => (cleanedF(x), x))
  }

  /** A private method for tests, to look at the contents of each partition
    * 一种用于测试的私有方法，可以查看每个分区的内容
    * */
  private[spark] def collectPartitions(): Array[Array[T]] = withScope {
    sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
  }

  /**
    * Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint
    * directory set with `SparkContext#setCheckpointDir` and all references to its parent
    * RDDs will be removed. This function must be called before any job has been
    * executed on this RDD. It is strongly recommended that this RDD is persisted in
    * memory, otherwise saving it on a file will require recomputation.
    *
    * 标志着这个RDD为检查点。它将被保存为一个文件在检查点目录设置`SparkContext#setCheckpointDir`
    * 和所有引用它的父RDD将被移除。这个函数必须在任何job在executed执行之前调用。这是强烈建议这RDD
    * 保存在内存中，否则保存在一个文件需要重新计算。
    *
    */
  def checkpoint(): Unit = RDDCheckpointData.synchronized {
    // NOTE: we use a global lock here due to complexities downstream with ensuring
    // children RDD partitions point to the correct parent partitions. In the future
    // we should revisit this consideration.
    // 注意：我们使用全局锁在这里，由于复杂的下游保障儿童RDD分区指向正确的父分区。
    // 在未来，我们应该重新考虑。
    if (context.checkpointDir.isEmpty) {
      throw new SparkException("Checkpoint directory has not been set in the SparkContext")
    } else if (checkpointData.isEmpty) {
      checkpointData = Some(new ReliableRDDCheckpointData(this))
    }
  }

  /**
    * Mark this RDD for local checkpointing using Spark's existing caching layer.
    *
    * 标志这个RDD作为本地的检查点，使用spark已经存在的缓存层次。
    *
    * This method is for users who wish to truncate RDD lineages while skipping the expensive
    * step of replicating the materialized data in a reliable distributed file system. This is
    * useful for RDDs with long lineages that need to be truncated periodically (e.g. GraphX).
    *
    *  这种方法对于用户谁希望削去RDD血统而跳过一个可靠的分布式文件系统复制的物化数据的昂贵的步骤。
    *  这与长期的血统，需要截断定期RDDS是有用的（例如：GraphX）。
    *
    * Local checkpointing sacrifices fault-tolerance for performance. In particular, checkpointed
    * data is written to ephemeral local storage in the executors instead of to a reliable,
    * fault-tolerant storage. The effect is that if an executor fails during the computation,
    * the checkpointed data may no longer be accessible, causing an irrecoverable job failure.
    *
    * 局部检查点牺牲容错性能。特别是，检查点数据的executors而不是一个可靠的临时本地存储写入，存储容错。
    * 效果是：如果一个执行者，在计算过程中失败，检查点的数据可能不再被访问，造成不可挽回的工作失败。
    *
    *
    * This is NOT safe to use with dynamic allocation, which removes executors along
    * with their cached blocks. If you must use both features, you are advised to set
    * `spark.dynamicAllocation.cachedExecutorIdleTimeout` to a high value.
    *
    * 使用动态分配是不安全的，它会将执行器和缓存块一起移除。如果你必须使用的功能，建议您设置
    * `spark.dynamicAllocation.cachedExecutorIdleTimeout` 为一个很高的值。
    *
    * The checkpoint directory set through `SparkContext#setCheckpointDir` is not used.
    *
    * 通过`SparkContext#setCheckpointDir`来设置检查点的目录是没用的。
    */
  def localCheckpoint(): this.type = RDDCheckpointData.synchronized {
    if (conf.getBoolean("spark.dynamicAllocation.enabled", false) &&
      conf.contains("spark.dynamicAllocation.cachedExecutorIdleTimeout")) {
      logWarning("Local checkpointing is NOT safe to use with dynamic allocation, " +
        "which removes executors along with their cached blocks. If you must use both " +
        "features, you are advised to set `spark.dynamicAllocation.cachedExecutorIdleTimeout` " +
        "to a high value. E.g. If you plan to use the RDD for 1 hour, set the timeout to " +
        "at least 1 hour.")
    }

    // Note: At this point we do not actually know whether the user will call persist() on
    // this RDD later, so we must explicitly call it here ourselves to ensure the cached
    // blocks are registered for cleanup later in the SparkContext.

    // 注：在这一点上，我们不知道用户是否会调用persist()在这个RDD之后，所以我们必须显式地调用它，
    // 在这里我们保证缓存块是注册在sparkcontext后清理。

    //
    // If, however, the user has already called persist() on this RDD, then we must adapt
    // the storage level he/she specified to one that is appropriate for local checkpointing
    // (i.e. uses disk) to guarantee correctness.

    // 如果，然而，用户已经调用了RDD的persist()方法，那么我们必须适应存储级的他/她指定一个适合本地检查点
    // （即使用磁盘）来保证正确性。

    if (storageLevel == StorageLevel.NONE) {
      persist(LocalRDDCheckpointData.DEFAULT_STORAGE_LEVEL)
    } else {
      persist(LocalRDDCheckpointData.transformStorageLevel(storageLevel), allowOverride = true)
    }

    // If this RDD is already checkpointed and materialized, its lineage is already truncated.
    // We must not override our `checkpointData` in this case because it is needed to recover
    // the checkpointed data. If it is overridden, next time materializing on this RDD will
    // cause error.
    // 如果RDD已经建立检查点和持久化，其血统已经截断，我们不必要重写我们的`checkpointData`在这种情况下，
    // 因为它需要恢复的检查点数据。如果它被重写，下一次出现在这盘会导致错误。
    if (isCheckpointedAndMaterialized) {
      logWarning("Not marking RDD for local checkpoint because it was already " +
        "checkpointed and materialized")
    } else {
      // Lineage is not truncated yet, so just override any existing checkpoint data with ours
      checkpointData match {
        case Some(_: ReliableRDDCheckpointData[_]) => logWarning(
          "RDD was already marked for reliable checkpointing: overriding with local checkpoint.")
        case _ =>
      }
      checkpointData = Some(new LocalRDDCheckpointData(this))
    }
    this
  }

  /**
    * Return whether this RDD is checkpointed and materialized, either reliably or locally.
    * 判断这个RDD是否建立检查点和实现，是可靠的或局部。
    */
  def isCheckpointed: Boolean = isCheckpointedAndMaterialized

  /**
    * Return whether this RDD is checkpointed and materialized, either reliably or locally.
    * This is introduced as an alias for `isCheckpointed` to clarify the semantics of the
    * return value. Exposed for testing.
    *
    * 判断这个RDD是否建立检查点和实现，是可靠的或局部的。这是作为` ischeckpointed `返回值的语义的一个别名。用于测试。
    *
    */
  private[spark] def isCheckpointedAndMaterialized: Boolean =
    checkpointData.exists(_.isCheckpointed)

  /**
    * Return whether this RDD is marked for local checkpointing.
    * Exposed for testing.
    * 判断这个RDD标记为本地检查点，用于测试。
    */
  private[rdd] def isLocallyCheckpointed: Boolean = {
    checkpointData match {
      case Some(_: LocalRDDCheckpointData[T]) => true
      case _ => false
    }
  }

  /**
    * Gets the name of the directory to which this RDD was checkpointed.
    * This is not defined if the RDD is checkpointed locally.
    * 获取RDD检查点目录的名称。这是没有定义的如果RDD是建立在本地。
    */
  def getCheckpointFile: Option[String] = {
    checkpointData match {
      case Some(reliable: ReliableRDDCheckpointData[T]) => reliable.getCheckpointDir
      case _ => None
    }
  }

  // =======================================================================
  // Other internal methods and fields
  // 其他内部方法和字段
  // =======================================================================

  private var storageLevel: StorageLevel = StorageLevel.NONE

  /** User code that created this RDD (e.g. `textFile`, `parallelize`).
    * 用户代码创建rdd（例如,`textFile`, `parallelize`）
    * */
  @transient private[spark] val creationSite = sc.getCallSite()

  /**
    * The scope associated with the operation that created this RDD.
    * 与创建这个RDD操作相关的范围。
    *
    * This is more flexible than the call site and can be defined hierarchically. For more
    * detail, see the documentation of {{RDDOperationScope}}. This scope is not defined if the
    * user instantiates this RDD himself without using any Spark operations.
    *
    * 这比调用站点更灵活，可以分层定义。更多的细节，看文件{{RDDOperationScope}}。这个范围不确定如果
    * 用户实例化该RDD自己没有使用任何Spark操作。
    *
    *
    */
  @transient private[spark] val scope: Option[RDDOperationScope] = {
    Option(sc.getLocalProperty(SparkContext.RDD_SCOPE_KEY)).map(RDDOperationScope.fromJson)
  }

  private[spark] def getCreationSite: String = Option(creationSite).map(_.shortForm).getOrElse("")

  private[spark] def elementClassTag: ClassTag[T] = classTag[T]

  private[spark] var checkpointData: Option[RDDCheckpointData[T]] = None

  // Whether to checkpoint all ancestor RDDs that are marked for checkpointing. By default,
  // we stop as soon as we find the first such RDD, an optimization that allows us to write
  // less data but is not safe for all workloads. E.g. in streaming we may checkpoint both
  // an RDD and its parent in every batch, in which case the parent may never be checkpointed
  // and its lineage never truncated, leading to OOMs in the long run (SPARK-6847).

  // 是否检查点，检查点的所有祖先RDDS是显着的。默认情况下，只要我们找到的第一个这样的RDD停止我们的优化，可以让我们少写数据但不安全，
  // 为所有的工作负载。例如，我们可能都在流检查站RDD和每批其母公司，在这种情况下，父母可能永远无法建立其血统从未截断，
  // 导致从长远来看，OOMs（spark-6847）。

  private val checkpointAllMarkedAncestors =
  Option(sc.getLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS))
    .map(_.toBoolean).getOrElse(false)

  /** Returns the first parent RDD
    * 返回第一个父RDD
    * */
  protected[spark] def firstParent[U: ClassTag]: RDD[U] = {
    dependencies.head.rdd.asInstanceOf[RDD[U]]
  }

  /** Returns the jth parent RDD: e.g. rdd.parent[T](0) is equivalent to rdd.firstParent[T]
    * 返回jth的父RDD,例如 rdd.parent[T](0) 和 dd.firstParent[T]是相同的
    * */
  protected[spark] def parent[U: ClassTag](j: Int) = {
    dependencies(j).rdd.asInstanceOf[RDD[U]]
  }

  /** The [[org.apache.spark.SparkContext]] that this RDD was created on.
    *   这个RDD是由[[org.apache.spark.SparkContext]]这个类创建的
    * */
  def context: SparkContext = sc

  /**
    * Private API for changing an RDD's ClassTag.
    * Used for internal Java-Scala API compatibility.
    *
    * 改变一个RDD的classtag私有API。
    * 用于内部java Scala API兼容。
    */
  private[spark] def retag(cls: Class[T]): RDD[T] = {
    val classTag: ClassTag[T] = ClassTag.apply(cls)
    this.retag(classTag)
  }

  /**
    * Private API for changing an RDD's ClassTag.
    * Used for internal Java-Scala API compatibility.
    *
    * 改变一个RDD的classtag私有API。
    * 用于内部java Scala API兼容。
    *
    */
  private[spark] def retag(implicit classTag: ClassTag[T]): RDD[T] = {
    this.mapPartitions(identity, preservesPartitioning = true)(classTag)
  }

  // Avoid handling doCheckpoint multiple times to prevent excessive recursion
  // 避免多次处理docheckpoint以防止过多的递归
  @transient private var doCheckpointCalled = false

  /**
    * Performs the checkpointing of this RDD by saving this. It is called after a job using this RDD
    * has completed (therefore the RDD has been materialized and potentially stored in memory).
    * doCheckpoint() is called recursively on the parent RDDs.
    *
    * 执行这个RDD的检查点。在job完成后调用（因此称为RDD被物化了的和持久化在存储在内存中）。docheckpoint()在父RDDS被递归调用。
    *
    */
  private[spark] def doCheckpoint(): Unit = {
    RDDOperationScope.withScope(sc, "checkpoint", allowNesting = false, ignoreParent = true) {
      if (!doCheckpointCalled) {
        doCheckpointCalled = true
        if (checkpointData.isDefined) {
          if (checkpointAllMarkedAncestors) {
            // TODO We can collect all the RDDs that needs to be checkpointed, and then checkpoint
            // them in parallel.
            // Checkpoint parents first because our lineage will be truncated after we
            // checkpoint ourselves
            dependencies.foreach(_.rdd.doCheckpoint())
          }
          checkpointData.get.checkpoint()
        } else {
          dependencies.foreach(_.rdd.doCheckpoint())
        }
      }
    }
  }

  /**
    * Changes the dependencies of this RDD from its original parents to a new RDD (`newRDD`)
    * created from the checkpoint file, and forget its old dependencies and partitions.
    *
    * 从原来的父RDD到一个新的RDD,这个RDD的依赖关系的变化(`newRDD`)从检查点文件创建，忘记旧的依赖关系和分区。
    */
  private[spark] def markCheckpointed(): Unit = {
    clearDependencies()
    partitions_ = null
    deps = null    // Forget the constructor argument for dependencies too
  }

  /**
    * Clears the dependencies of this RDD. This method must ensure that all references
    * to the original parent RDDs are removed to enable the parent RDDs to be garbage
    * collected. Subclasses of RDD may override this method for implementing their own cleaning
    * logic. See [[org.apache.spark.rdd.UnionRDD]] for an example.
    *
    * 清除这个RDD的依赖关系。这种方法必须确保所有引用的原始母RDDS除去使父RDDS被垃圾收集。子RDD可以实施自己清洗逻辑重写这个方法。
    * 详情请看[[org.apache.spark.rdd.UnionRDD]] 为例。
    *
    */
  protected def clearDependencies() {
    dependencies_ = null
  }

  /** A description of this RDD and its recursive dependencies for debugging.
    * 调试说明这个RDD和递推关系。
    * */
  def toDebugString: String = {
    // Get a debug description of an rdd without its children
    // 得到一个RDDd的DEBUG描述，不包括他的子RDD信息
    def debugSelf(rdd: RDD[_]): Seq[String] = {
      import Utils.bytesToString

      val persistence = if (storageLevel != StorageLevel.NONE) storageLevel.description else ""
      val storageInfo = rdd.context.getRDDStorageInfo(_.id == rdd.id).map(info =>
        "    CachedPartitions: %d; MemorySize: %s; ExternalBlockStoreSize: %s; DiskSize: %s".format(
          info.numCachedPartitions, bytesToString(info.memSize),
          bytesToString(info.externalBlockStoreSize), bytesToString(info.diskSize)))

      s"$rdd [$persistence]" +: storageInfo
    }

    // Apply a different rule to the last child
    def debugChildren(rdd: RDD[_], prefix: String): Seq[String] = {
      val len = rdd.dependencies.length
      len match {
        case 0 => Seq.empty
        case 1 =>
          val d = rdd.dependencies.head
          debugString(d.rdd, prefix, d.isInstanceOf[ShuffleDependency[_, _, _]], true)
        case _ =>
          val frontDeps = rdd.dependencies.take(len - 1)
          val frontDepStrings = frontDeps.flatMap(
            d => debugString(d.rdd, prefix, d.isInstanceOf[ShuffleDependency[_, _, _]]))

          val lastDep = rdd.dependencies.last
          val lastDepStrings =
            debugString(lastDep.rdd, prefix, lastDep.isInstanceOf[ShuffleDependency[_, _, _]], true)

          (frontDepStrings ++ lastDepStrings)
      }
    }
    // The first RDD in the dependency stack has no parents, so no need for a +-
    def firstDebugString(rdd: RDD[_]): Seq[String] = {
      val partitionStr = "(" + rdd.partitions.length + ")"
      val leftOffset = (partitionStr.length - 1) / 2
      val nextPrefix = (" " * leftOffset) + "|" + (" " * (partitionStr.length - leftOffset))

      debugSelf(rdd).zipWithIndex.map{
        case (desc: String, 0) => s"$partitionStr $desc"
        case (desc: String, _) => s"$nextPrefix $desc"
      } ++ debugChildren(rdd, nextPrefix)
    }
    def shuffleDebugString(rdd: RDD[_], prefix: String = "", isLastChild: Boolean): Seq[String] = {
      val partitionStr = "(" + rdd.partitions.length + ")"
      val leftOffset = (partitionStr.length - 1) / 2
      val thisPrefix = prefix.replaceAll("\\|\\s+$", "")
      val nextPrefix = (
        thisPrefix
          + (if (isLastChild) "  " else "| ")
          + (" " * leftOffset) + "|" + (" " * (partitionStr.length - leftOffset)))

      debugSelf(rdd).zipWithIndex.map{
        case (desc: String, 0) => s"$thisPrefix+-$partitionStr $desc"
        case (desc: String, _) => s"$nextPrefix$desc"
      } ++ debugChildren(rdd, nextPrefix)
    }
    def debugString(
                     rdd: RDD[_],
                     prefix: String = "",
                     isShuffle: Boolean = true,
                     isLastChild: Boolean = false): Seq[String] = {
      if (isShuffle) {
        shuffleDebugString(rdd, prefix, isLastChild)
      } else {
        debugSelf(rdd).map(prefix + _) ++ debugChildren(rdd, prefix)
      }
    }
    firstDebugString(this).mkString("\n")
  }

  override def toString: String = "%s%s[%d] at %s".format(
    Option(name).map(_ + " ").getOrElse(""), getClass.getSimpleName, id, getCreationSite)

  def toJavaRDD() : JavaRDD[T] = {
    new JavaRDD(this)(elementClassTag)
  }
}


/**
  * Defines implicit functions that provide extra functionalities on RDDs of specific types.
  * 定义隐式函数，提供额外的功能，对特定类型的RDD。
  *
  * For example, [[RDD.rddToPairRDDFunctions]] converts an RDD into a [[PairRDDFunctions]] for
  * key-value-pair RDDs, and enabling extra functionalities such as `PairRDDFunctions.reduceByKey`.
  *
  * 例如， [[RDD.rddToPairRDDFunctions]] 转换成 [[PairRDDFunctions]]为键值对RDD，使额外的功能，
  * 如 `PairRDDFunctions.reduceByKey`.
  *
  */
object RDD {

  private[spark] val CHECKPOINT_ALL_MARKED_ANCESTORS =
    "spark.checkpoint.checkpointAllMarkedAncestors"

  // The following implicit functions were in SparkContext before 1.3 and users had to
  // `import SparkContext._` to enable them. Now we move them here to make the compiler find
  // them automatically. However, we still keep the old functions in SparkContext for backward
  // compatibility and forward to the following functions directly.

  // 以下是sparkcontext隐函数在1.3之前，用户必须使用`import SparkContext._`去启用他们。
  // 现在我们把它们移到这里，让编译器自动找到它们。然而，我们仍然保持旧的功能sparkcontext向后的兼容性，提出了以下功能直接。

  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
                                          (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }

  implicit def rddToAsyncRDDActions[T: ClassTag](rdd: RDD[T]): AsyncRDDActions[T] = {
    new AsyncRDDActions(rdd)
  }

  implicit def rddToSequenceFileRDDFunctions[K, V](rdd: RDD[(K, V)])
                                                  (implicit kt: ClassTag[K], vt: ClassTag[V],
                                                   keyWritableFactory: WritableFactory[K],
                                                   valueWritableFactory: WritableFactory[V])
  : SequenceFileRDDFunctions[K, V] = {
    implicit val keyConverter = keyWritableFactory.convert
    implicit val valueConverter = valueWritableFactory.convert
    new SequenceFileRDDFunctions(rdd,
      keyWritableFactory.writableClass(kt), valueWritableFactory.writableClass(vt))
  }

  implicit def rddToOrderedRDDFunctions[K : Ordering : ClassTag, V: ClassTag](rdd: RDD[(K, V)])
  : OrderedRDDFunctions[K, V, (K, V)] = {
    new OrderedRDDFunctions[K, V, (K, V)](rdd)
  }

  implicit def doubleRDDToDoubleRDDFunctions(rdd: RDD[Double]): DoubleRDDFunctions = {
    new DoubleRDDFunctions(rdd)
  }

  implicit def numericRDDToDoubleRDDFunctions[T](rdd: RDD[T])(implicit num: Numeric[T])
  : DoubleRDDFunctions = {
    new DoubleRDDFunctions(rdd.map(x => num.toDouble(x)))
  }
}
