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

package org.apache.spark.memory

import org.apache.spark.SparkConf
import org.apache.spark.storage.BlockId

/**
 * A [[MemoryManager]] that statically partitions the heap space into disjoint regions.
  * 一个[[MemoryManager]]，静态地将堆空间划分为不相交的区域。
 *
 * The sizes of the execution and storage regions are determined through
 * `spark.shuffle.memoryFraction` and `spark.storage.memoryFraction` respectively. The two
 * regions are cleanly separated such that neither usage can borrow memory from the other.
  *
  * 执行和存储区域的大小由“spark.shuffle.memoryFraction”和“spark.storage.memoryFraction”决定。
  * 这两个区域是干净的分开，这样两个使用都不能从另一个地方借用内存。
  *
  * 这个类就是Spark-1.6.0之前版本中主要使用的，对各部分内存静态划分好后便不可变化。
  *
  * StaticMemoryManager表示是静态的内存管理器，何谓静态，就是按照某种算法确定内存的分配后，其整体分布不会随便改变
 */
private[spark] class StaticMemoryManager(
    conf: SparkConf,
    maxOnHeapExecutionMemory: Long,
    override val maxOnHeapStorageMemory: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    maxOnHeapStorageMemory,
    maxOnHeapExecutionMemory) {

  def this(conf: SparkConf, numCores: Int) {
    this(
      conf,
      StaticMemoryManager.getMaxExecutionMemory(conf),
      StaticMemoryManager.getMaxStorageMemory(conf),
      numCores)
  }

  // The StaticMemoryManager does not support off-heap storage memory:
  offHeapExecutionMemoryPool.incrementPoolSize(offHeapStorageMemoryPool.poolSize)
  offHeapStorageMemoryPool.decrementPoolSize(offHeapStorageMemoryPool.poolSize)

  // Max number of bytes worth of blocks to evict when unrolling
  /** 当展开时，块的最大字节数被驱逐
    * 由maxStorageMemory(该方法在MemoryManager中被定义)乘以spark.storage.unrollFraction（默认值0.2）来确定。
    * 也就是说在storage内存中，有一部分会被用于unroll。由于Spark允许序列化和非序列化两种方式存储数据，对于序列化的数据，
    * 必须要先展开后才能使用。unroll部分空间就是用于展开序列化数据的。这部分空间是动态分配的
    */
  private val maxUnrollMemory: Long = {
    (maxOnHeapStorageMemory * conf.getDouble("spark.storage.unrollFraction", 0.2)).toLong
  }

  override def maxOffHeapStorageMemory: Long = 0L

  /**
    * 申请storage部分内存。在保证申请的内存数numBytes小于maxStorageMemory后，向storage内存池申请numBytes内存。
    * 进一步调用StorageMemoryPool的acquireMemory方法进行内存的申请。
    *
    *   就是这么简单。如果需要申请的内存超过Storage区域内存最大值的上限，则表明没有足够的内存进行存储，否则，
    * 调用storageMemoryPool的acquireMemory()方法分配内存，正是这里体现了static一词。至于具体分配内存的
    * storageMemoryPool，我们放到最后和Execution区域时的onHeapExecutionMemoryPool、offHeapExecutionMemoryPool
    * 一起讲，这里先了解下它的概念即可，它实际上是对应某种区域的内存池，是对内存总大小、可用内存、已用内存等内存
    * 使用情况的一种记账的专用对象。
    * */
  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    require(memoryMode != MemoryMode.OFF_HEAP,
      "StaticMemoryManager does not support off-heap storage memory")
    // 如果需要的大小numBytes超过Storage区域内存的上限，直接返回false，说明内存不够
    if (numBytes > maxOnHeapStorageMemory) {
      // Fail fast if the block simply won't fit
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
        s"memory limit ($maxOnHeapStorageMemory bytes)")
      false
    } else {
      // 否则，调用storageMemoryPool的acquireMemory()方法，申请内存
      onHeapStorageMemoryPool.acquireMemory(blockId, numBytes)
    }
  }

  /**
    *    根据传入numBytes，申请unroll部分内存。首先获取当前storage内存池中unroll部分使用的内存数currentUnrollMemory，
    * 以及当前storage内存池剩余内存数freeMemory。内存足够时，直接从storage内存池分配numBytes内存。如果内存不足，
    * 则会从storage内存池先释放出一部分内存。整个unroll部分使用的内存不能超过maxUnrollMemory。
    * */
  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    require(memoryMode != MemoryMode.OFF_HEAP,
      "StaticMemoryManager does not support off-heap unroll memory")
    val currentUnrollMemory = onHeapStorageMemoryPool.memoryStore.currentUnrollMemory
    val freeMemory = onHeapStorageMemoryPool.memoryFree
    // When unrolling, we will use all of the existing free memory, and, if necessary,
    // some extra space freed from evicting cached blocks. We must place a cap on the
    // amount of memory to be evicted by unrolling, however, otherwise unrolling one
    // big block can blow away the entire cache.
    /**
      * 当展开时，我们将使用所有现有的空闲内存，如果有必要的话，还可以腾出一些额外的空间来释放缓存块。
      * 我们必须对未滚动的内存数量设置一个上限，否则，打开一个大块就可以将整个缓存吹走。
      * */
    val maxNumBytesToFree = math.max(0, maxUnrollMemory - currentUnrollMemory - freeMemory)
    // Keep it within the range 0 <= X <= maxNumBytesToFree
    val numBytesToFree = math.max(0, math.min(maxNumBytesToFree, numBytes - freeMemory))
    onHeapStorageMemoryPool.acquireMemory(blockId, numBytes, numBytesToFree)
  }

  /**
    * 申请execution部分内存。根据传入的taskAttemptId，以及需要的内存数numBytes，和当前的MemoryMode是ON_HEAP还是OFF_HEAP，
    * 从对应的execution内存池中申请内存。这里进一步调用ExecutionMemoryPool的acquireMemory方法进行内存的申请。
    * */
  private[memory]
  override def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    // 根据MemoryMode的种类决定如何分配内存
    memoryMode match {
      // 如果是堆内，即ON_HEAP，则通过onHeapExecutionMemoryPool的acquireMemory对Task进行Execution区域内存分配
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId)
      // 如果是堆外，即OFF_HEAP，则通过offHeapExecutionMemoryPool的acquireMemory对Task进行Execution区域内存分配
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId)
    }
  }
}


private[spark] object StaticMemoryManager {

  private val MIN_MEMORY_BYTES = 32 * 1024 * 1024

  /**
   * Return the total amount of memory available for the storage region, in bytes.
    * 返回存储区域可用的内存总量，以字节为单位。
    * 返回为storage区域（即存储区域）分配的可用内存总大小，单位为bytes
    *
    * 伴生对象中的方法，用于获取storage部分内存大小，计算过程如下：
    *    systemMaxMemory是当前Executor的内存大小，虽然可以由参数spark.testing.memory来设定，但是这个参数一般用于做测试，
    * 在生产上不建议设置。
    *   memoryFraction是storage内存占整个systemMaxMemory内存的比例，由参数spark.storage.memoryFraction（默认值0.6）来设定。
    * 同时为了避免出现OOM的情况，会设定一个安全系数spark.storage.safetyFraction(默认值0.9）
   */
  private def getMaxStorageMemory(conf: SparkConf): Long = {
    // systemMaxMemory ：Runtime.getRuntime.maxMemory，即JVM能获得的最大内存空间。
    // 系统可用最大内存，取参数spark.testing.memory，未配置的话取运行时环境中的最大内存
    val systemMaxMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)

    // memoryFraction：由参数spark.storage.memoryFraction控制，默认0.6。
    // 取storage区域（即存储区域）在总内存中所占比重，由参数spark.storage.memoryFraction确定，默认为0.6
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)

    // safetyFraction：由参数spark.storage.safetyFraction控制，默认是0.9，因为cache block都是估算的，所以需要一个安全系数来保证安全。
    // 取storage区域（即存储区域）在系统为其可分配最大内存的安全系数，主要为了防止OOM，取参数spark.storage.safetyFraction，默认
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)

    // storageMemory能分到的内存是
    // 返回storage区域（即存储区域）分配的可用内存总大小，计算公式：
    // 系统可用最大内存*在系统可用最大内存中所占比重*安全系数
    (systemMaxMemory * memoryFraction * safetyFraction).toLong
  }

  /**
   * Return the total amount of memory available for the execution region, in bytes.
    * 以字节为单位，返回执行区域可用的内存总量。
    * 返回为Execution区域（即运行区域，为shuffle使用）分配的可用内存总大小，单位为bytes
    *
    * 伴生对象中的方法。用于获取execution部分内存大小。
    * memoryFraction即execution部分占所有能使用内存的百分比，由参数spark.shuffle.memoryFraction（默认值是0.2）来确定。
    * safetyFraction是execution部分的一个安全阈值，由参数spark.shuffle.safetyFraction（默认值是0.8）来确定。
   */
  private def getMaxExecutionMemory(conf: SparkConf): Long = {
    // 系统可用最大内存，取参数spark.testing.memory，未配置的话取运行时环境中的最大内存
    // 若设置了 spark.testing.memory 则以该配置的值作为 systemMaxMemory，否则使用 JVM 最大内存作为
    // systemMaxMemory。spark.testing.memory 仅用于测试，一般不设置，所以这里我们认为 systemMaxMemory
    // 的值就是 executor 的最大可用内存。
    val systemMaxMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)

    if (systemMaxMemory < MIN_MEMORY_BYTES) {
      throw new IllegalArgumentException(s"System memory $systemMaxMemory must " +
        s"be at least $MIN_MEMORY_BYTES. Please increase heap size using the --driver-memory " +
        s"option or spark.driver.memory in Spark configuration.")
    }
    if (conf.contains("spark.executor.memory")) {
      val executorMemory = conf.getSizeAsBytes("spark.executor.memory")
      if (executorMemory < MIN_MEMORY_BYTES) {
        throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
          s"$MIN_MEMORY_BYTES. Please increase executor memory using the " +
          s"--executor-memory option or spark.executor.memory in Spark configuration.")
      }
    }

    /**
      * 取Execution区域（即运行区域，为shuffle使用）在总内存中所占比重，由参数spark.shuffle.memoryFraction确定，默认为0.2
      *
      * 优化：
      *   spark.shuffle.memoryFraction
      *
      *   shuffle期间用于aggregation和cogroups的内存占executor运行的百分比，用小数表示。在任何时候，用于shuffle的
      *   内存总size不得超过这个限制，超出的部分会被spill到磁盘.如果经常spill，考虑调大spark.shuffle.memoryFraction.
      *
      * 默认值：0.2参数说明：该参数代表了Executor内存中，分配给shuffle read task进行聚合操作的内存比例，默认是20%。
      * 调优建议：在资源参数调优中讲解过这个参数。如果内存充足，而且很少使用持久化操作，建议调高这个比例，给shuffle read
      * 的聚合操作更多内存，以避免由于内存不足导致聚合过程中频繁读写磁盘。在实践中发现，合理调节该参数可以将性能提升10%左右。
      *
      * spark.shuffle.safetyFraction：为防止 OOM，不能把 systemMaxMemory * spark.shuffle.memoryFraction 全用了，
      * 需要有个安全百分比
      *
      * 所以最终用于 execution 的内存量为：executor 最大可用内存 * spark.shuffle.memoryFraction * spark.shuffle.safetyFraction，
      * 默认为 executor 最大可用内存 * 0.16
      *
      * 需要特别注意的是，即使用于 execution 的内存不够用了，但同时 executor 还有其他空余内存，也不能给 execution 用
      */
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)

    // 取Execution区域（即运行区域，为shuffle使用）在系统为其可分配最大内存的安全系数，主要为了防止OOM，
    // 取参数spark.shuffle.safetyFraction，默认为0.8
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)

    // 返回为Execution区域（即运行区域，为shuffle使用）分配的可用内存总大小，计算公式：系统可用最大内存 * 在系统可用最大内存中所占比重 * 安全系数
    (systemMaxMemory * memoryFraction * safetyFraction).toLong
  }

}
