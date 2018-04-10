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
 * A [[MemoryManager]] that enforces a soft boundary between execution and storage such that
 * either side can borrow memory from the other.
  *
  * 一个[[MemoryManager]]，它强制执行和存储之间的软边界，这样任何一方都可以从另一方借用内存。
 *
 * The region shared between execution and storage is a fraction of (the total heap space - 300MB)
 * configurable through `spark.memory.fraction` (default 0.6). The position of the boundary
 * within this space is further determined by `spark.memory.storageFraction` (default 0.5).
 * This means the size of the storage region is 0.6 * 0.5 = 0.3 of the heap space by default.
  *
  * 执行和存储之间共享的区域是通过“spark.memory.fraction”(默认0.6)配置的(总堆空间- 300MB)的一小部分。
  * 在这个空间内的边界位置由“spark.memory.storageFraction”进一步确定(默认0.5)。这意味着默认情况下，
  * 存储区域的大小为0.6 * 0.5 = 0.3。
 *
 * Storage can borrow as much execution memory as is free until execution reclaims its space.
 * When this happens, cached blocks will be evicted from memory until sufficient borrowed
 * memory is released to satisfy the execution memory request.
  *
  * Storage can borrow as much execution memory as is free，直到执行重新声明它的空间。当这种情况发生时，
  * 缓存的块将被从内存中删除，直到释放出足够的内存，以满足执行内存请求。
 *
 * Similarly, execution can borrow as much storage memory as is free. However, execution
 * memory is *never* evicted by storage due to the complexities involved in implementing this.
 * The implication is that attempts to cache blocks may fail if execution has already eaten
 * up most of the storage space, in which case the new blocks will be evicted immediately
 * according to their respective storage levels.
  *
  * 类似地，execution可以以自由的方式借用大量存储内存。然而，由于execution此操作的复杂性，执行内存永远不会被存储。
  * 其含义是，如果执行已经耗尽了大部分存储空间，那么尝试缓存块可能会失败，在这种情况下，新的块将根据它们各自的存储
  * 级别立即被驱逐。
 *
 * @param onHeapStorageRegionSize Size of the storage region, in bytes.
 *                          This region is not statically reserved; execution can borrow from
 *                          it if necessary. Cached blocks can be evicted only if actual
 *                          storage memory usage exceeds this region.
  *
  *                          存储区域的大小，以字节为单位。
  *                          这个区域不是静态保留的;如果有必要，execution可以向它借。
  *                          只有在实际的存储内存使用超过该区域时，缓存的块才可以被驱逐。
  *
  *  该memoryManager主要是使得execution部分和storage部分的内存不像之前由比例参数限定住，而是两者可以互相借用内存。
  *  execution和storage总的内存上限由参数｀spark.memory.fraction（默认0.75）来设定的，这个比例是相对于整个JVM heap来说的。
  *  Storage部分可以申请Execution部分的所有空闲内存，直到Execution内存不足时向Storage发出信号为止。当Execution需要
  *  更多内存时，Storage部分会向磁盘spill数据，直到把借用的内存都还上为止。同样的Execution部分也能向Storage部分借用内存，
  *  当Storage需要内存时，Execution中的数据不会马上spill到磁盘，因为Execution使用的内存发生在计算过程中，如果数据丢失就
  *  会到账task计算失败。Storage部分只能等待Execution部分主动释放占用的内存。
  *
  *
  *  UnifiedMemoryManager代表的是统一的内存管理器，统一么，是不是有共享和变动的意思。
  *
  *  参考博客：https://blog.csdn.net/qq_21383435/article/details/79108106
 */
private[spark] class UnifiedMemoryManager private[memory] (
    conf: SparkConf,
    val maxHeapMemory: Long,
    onHeapStorageRegionSize: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    onHeapStorageRegionSize,
    maxHeapMemory - onHeapStorageRegionSize) {

  /**
    * 这个函数传入的memoryMode可选择是使用堆内存还是直接使用本地内存,默认是使用堆内存.
    *
    * // 确保onHeapExecutionMemoryPool和storageMemoryPool大小之和等于二者共享内存区域maxMemory大小
    * */
  private def assertInvariants(): Unit = {
    assert(onHeapExecutionMemoryPool.poolSize + onHeapStorageMemoryPool.poolSize == maxHeapMemory)
    assert(
      offHeapExecutionMemoryPool.poolSize + offHeapStorageMemoryPool.poolSize == maxOffHeapMemory)
  }

  assertInvariants()

  /**
    * 以前的版本是：maxStorageMemory
    * maxOnHeapStorageMemory为execution和storage区域共享的最大内存减去Execution已用内存
     */
  override def maxOnHeapStorageMemory: Long = synchronized {
    maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed
  }

  override def maxOffHeapStorageMemory: Long = synchronized {
    maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
    *
    * 尝试获取当前任务的执行内存的“numBytes”，并返回所获得的字节数，如果没有可以分配的话，则返回0。
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
    *
    * 这个调用可能会阻塞，直到在某些情况下有足够的空闲内存，以确保每个任务都有机会到达总内存池的至少1 / 2N
    * (其中N是活动任务的#)，然后才会被迫溢写。如果任务的数量增加，但较老的任务有很多内存，这可能会发生。
    *
    * 为当前的taskAttemptId申请最多numBytes的内存，如果内存不足则返回0。
    * 由于这里涉及到的都是Executor JVM Heap中的内存，所以如果是OFF_HEAP模式，直接从offHeapExecution内存池分配。
    * 对memoryMode为ON_HEAP的进行如下处理。
   */
  override private[memory] def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    // 这个函数传入的memoryMode可选择是使用堆内存还是直接使用本地内存,默认是使用堆内存.
    // 确保onHeapExecutionMemoryPool和storageMemoryPool大小之和等于二者共享内存区域maxMemory大小
    assertInvariants()
    assert(numBytes >= 0)
    val (executionPool, storagePool, storageRegionSize, maxMemory) = memoryMode match {
      // 这里定义的这个函数,用于判断numBytes(需要申请的内存大小)减去当前内存池中可用的内存大小是否够用,
      // 如果不够用,这个函数的传入值是一个正数
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        onHeapStorageRegionSize,
        maxHeapMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        offHeapStorageMemory,
        maxOffHeapMemory)
    }

    /**
     * Grow the execution pool by evicting cached blocks, thereby shrinking the storage pool.
      * 通过驱逐缓存块来增加执行池，从而减少存储池。
     *
     * When acquiring memory for a task, the execution pool may need to make multiple
     * attempts. Each attempt must be able to evict storage in case another task jumps in
     * and caches a large block between the attempts. This is called once per attempt.
      *
      * 在为任务获取内存时，执行池可能需要多次尝试。每次尝试都必须能够驱逐存储，以防另一个任务跳跃，
      * 并在尝试之间缓存一个很大的块。这是每次尝试调用一次。
      *
      * 会释放storage中保存的数据，减小storage部分内存大小，从而增大Execution部分
      *
      * 首先，该方法何时被调用？
      *     当execution pool的空闲内存不够用时，则该方法会被调用。从该方法的名字就可以得知，该方法试图增长execution pool的大小。
      *
      * 那么，如何增长execution pool呢？
      *     通过阅读方法中的注释，我们可以看到有两种方式：
      *         1. storage pool中有空闲内存，则借用storage pool中的空闲内存
      *         2. storage pool的大小超过了storageRegionSize，则驱逐存储在storage pool中的blocks，
      *             来回收storage pool从execution pool中借走的内存。
     */
    def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {
      if (extraMemoryNeeded > 0) {
        // There is not enough free memory in the execution pool, so try to reclaim memory from
        // storage. We can reclaim any free memory from the storage pool. If the storage pool
        // has grown to become larger than `storageRegionSize`, we can evict blocks and reclaim
        // the memory that storage has borrowed from execution.
        //
        // 在执行池中没有足够的空闲内存，所以尝试从存储中回收内存。我们可以从存储池中回收任何空闲内存。
        // 如果存储池已经变得比“storageRegionSize”更大，那么我们就可以驱逐块并回收存储从执行中借用的内存。
        val memoryReclaimableFromStorage = math.max(
          storagePool.memoryFree,
          storagePool.poolSize - storageRegionSize)
        if (memoryReclaimableFromStorage > 0) {
          // Only reclaim as much space as is necessary and available: 只回收必要和可用的空间:
          val spaceToReclaim = storagePool.freeSpaceToShrinkPool(
            math.min(extraMemoryNeeded, memoryReclaimableFromStorage))
          storagePool.decrementPoolSize(spaceToReclaim)
          executionPool.incrementPoolSize(spaceToReclaim)
        }
      }
    }

    /**
     * The size the execution pool would have after evicting storage memory.
      * 执行池在清除存储内存后的大小。
     *
     * The execution memory pool divides this quantity among the active tasks evenly to cap
     * the execution memory allocation for each task. It is important to keep this greater
     * than the execution pool size, which doesn't take into account potential memory that
     * could be freed by evicting storage. Otherwise we may hit SPARK-12155.
      *
      * 执行内存池将此数量分配到活动任务中，平均分配每个任务的执行内存分配。保持这个大于执行池大小是很重要的，
      * 因为它没有考虑到可能通过驱逐存储而释放的潜在内存。否则，我们可能打到spark - 12155。
     *
     * Additionally, this quantity should be kept below `maxMemory` to arbitrate fairness
     * in execution memory allocation across tasks, Otherwise, a task may occupy more than
     * its fair share of execution memory, mistakenly thinking that other tasks can acquire
     * the portion of storage memory that cannot be evicted.
      *
      * 此外，这个数量应该保持在“maxMemory”之下，在执行内存分配中对任务进行仲裁，否则，一个任务可能会占用更多
      * 的执行内存，错误地认为其他任务可以获得无法被驱逐的存储内存部分。
      *
      * 计算在 storage 释放内存借给 execution 后，execution 部分的内存大小
     */
    def computeMaxExecutionPoolSize(): Long = {
      maxMemory - math.min(storagePool.memoryUsed, storageRegionSize)
    }

    executionPool.acquireMemory(
      numBytes, taskAttemptId, maybeGrowExecutionPool, computeMaxExecutionPoolSize)
  }







  /**
    * 首先申请的storage内存numBytes不能超过storage部分内存的最大值maxStorageMemory。
    * 然后当storage部分内存不足以满足此次申请时，尝试向execution内存池借用内存，借到的内存大小为min(execution内存池剩余
    * 内存，numBytes)，并且实时调整execution和storage内存池的大小，如下面的代码所描述的。
    *
    * 若申请的numBytes比两者总共的内存还大，直接返回false，说明申请失败。
    * 若numBytes比storage空闲的内存大，则需要向executionPool借用
    * 借用的大小为此时execution的空闲内存和numBytes的较小值（个人观点应该是和(numBytes-storage空闲内存)的较小值）
    * 减小execution的poolSize
    * 增加storage的poolSize
    *
    * 参考博客：https://blog.csdn.net/qq_21383435/article/details/79108106
    * */
  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    assertInvariants()
    assert(numBytes >= 0)
    val (executionPool, storagePool, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        maxOnHeapStorageMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        maxOffHeapStorageMemory)
    }
    // 申请的内存大于storage和execution内存之和
    // 如果需要申请的内存大小超过maxStorageMemory，即execution和storage区域共享的最大内存减去Execution已用内存，
    // 快速返回， 这里是将execution和storage区域一起考虑的
    if (numBytes > maxMemory) {
      // Fail fast if the block simply won't fit
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
        s"memory limit ($maxMemory bytes)")
      return false
    }
    // 大于storage空闲内存
    // 如果需要申请的内存大小超过预分配storage区域中可用大小memoryFree
    if (numBytes > storagePool.memoryFree) {
      // There is not enough free memory in the storage pool, so try to borrow free memory from
      // the execution pool.
      // 从Execution区域借调的内存大小，为需要申请内存大小和预分配的Execution区域可用大小memoryFree的较小者
      val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree,
        numBytes - storagePool.memoryFree)

      // Execution区域减小相应的值
      executionPool.decrementPoolSize(memoryBorrowedFromExecution)

      // Storage区域增大相应的值
      storagePool.incrementPoolSize(memoryBorrowedFromExecution)
    }
    // 通过storageMemoryPool完成内存分配
    storagePool.acquireMemory(blockId, numBytes)
  }

  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    acquireStorageMemory(blockId, numBytes, memoryMode)
  }
}

object UnifiedMemoryManager {

  // Set aside a fixed amount of memory for non-storage, non-execution purposes.
  // This serves a function similar to `spark.memory.fraction`, but guarantees that we reserve
  // sufficient memory for the system even for small heaps. E.g. if we have a 1GB JVM, then
  // the memory used for execution and storage will be (1024 - 300) * 0.6 = 434MB by default.
  /**
    * 为非存储、非执行的目的留出一定的内存。这与“spark.memory.fraction”类似，但保证我们为系统保留足够的内存，
    * 即使是小堆。如果我们有1GB的JVM，那么在默认情况下，用于执行和存储的内存将是(1024 - 300)* 0.6 = 434MB。
    *
    * 伴生对象的一个属性，值为300MB，是Execution和Storage之外的一部分内存，为系统保留。
    * */
  private val RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024

  /**
    * 使用apply方法进行初始化
    * */
  def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager = {
    // 获得execution和storage区域共享的最大内存
    val maxMemory = getMaxMemory(conf)
    // 构造UnifiedMemoryManager对象
    new UnifiedMemoryManager(
      conf,
      maxHeapMemory = maxMemory,
      // storage区域内存大小初始为execution和storage区域共享的最大内存的spark.memory.storageFraction，
      // 默认为0.5，即一半
      onHeapStorageRegionSize =
        (maxMemory * conf.getDouble("spark.memory.storageFraction", 0.5)).toLong,
      numCores = numCores)
  }

  /**
   * Return the total amount of memory shared between execution and storage, in bytes.
    *返回在执行和存储之间共享的内存总量，以字节为单位。
    * 返回execution和storage区域共享的最大内存
    *
    * 伴生对象的方法。获取execution和storage部分能够使用的总内存大小。
    *
    * systemMemory即Executor的内存大小。systemMemory要求最小为reservedMemory的1.5倍，否则直接抛出异常信息。
    * reservedMemory是为系统保留的内存大小，可以由参数spark.testing.reservedMemory确定，默认值为上面的300MB。
    * 如果为默认值的话，那么对应的会要求systemMemory最小为450MB。
    * memoryFraction是整个execution和storage共用的最大内存比例，由参数spark.memory.fraction（默认值0.75）来决定。
    * 那么还剩下0.25的内存作为User Memory部分使用。那么对一个1GB内存的Executor来说，在默认情况下，可使用的内存大小
    * 为（1024 - 300） * 0.75 = 543MB
    *
    * 处理流程大体如下：
    *     1、获取系统可用最大内存systemMemory，取参数spark.testing.memory，未配置的话取运行时环境中的最大内存；
    *     2、获取预留内存reservedMemory，取参数spark.testing.reservedMemory，未配置的话，根据参数spark.testing
    *        来确定默认值，参数spark.testing存在的话，默认为0，否则默认为300M；
    *     3、取最小的系统内存minSystemMemory，为预留内存reservedMemory的1.5倍；
    *     4、如果系统可用最大内存systemMemory小于最小的系统内存minSystemMemory，即预留内存reservedMemory的1.5倍
    *        的话，抛出异常，提醒用户调大JVM堆大小；
    *     5、计算可用内存usableMemory，即系统最大可用内存systemMemory减去预留内存reservedMemory；
    *     6、取可用内存所占比重，即参数spark.memory.fraction，默认为0.75；
    *     7、返回的execution和storage区域共享的最大内存为usableMemory * memoryFraction。
    *
    *   也就是说，UnifiedMemoryManager统一内存存储管理策略中，默认情况下，storage区域和execution区域默认都占其
    * 共享内存区域的一半，而execution和storage区域共享的最大内存为系统最大可用内存systemMemory减去预留内存
    * reservedMemory后的75%。至于在哪里体现的动态调整，则要到真正申请内存时再体现了。
   */
  private def getMaxMemory(conf: SparkConf): Long = {
    // 获取系统可用最大内存systemMemory，取参数spark.testing.memory，未配置的话取运行时环境中的最大内存
    // //< 生产环境中一般不会设置 spark.testing.memory，所以这里认为 systemMemory 大小为 Jvm 最大可用内存
    val systemMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)

    // 获取预留内存reservedMemory，取参数spark.testing.reservedMemory，
    // 未配置的话，根据参数spark.testing来确定默认值，参数spark.testing存在的话，默认为0，否则默认为300M
    //< 系统预留 300M
    val reservedMemory = conf.getLong("spark.testing.reservedMemory",
      if (conf.contains("spark.testing")) 0 else RESERVED_SYSTEM_MEMORY_BYTES)

    // 取最小的系统内存minSystemMemory，为预留内存reservedMemory的1.5倍
    val minSystemMemory = (reservedMemory * 1.5).ceil.toLong

    // 如果系统可用最大内存systemMemory小于最小的系统内存minSystemMemory，即预留内存reservedMemory的1.5倍的话，
    // 抛出异常 提醒用户调大JVM堆大小
    //< 如果 systemMemory 小于450M，则抛异常
    if (systemMemory < minSystemMemory) {
      throw new IllegalArgumentException(s"System memory $systemMemory must " +
        s"be at least $minSystemMemory. Please increase heap size using the --driver-memory " +
        s"option or spark.driver.memory in Spark configuration.")
    }

    // SPARK-12759 Check executor memory to fail fast if memory is insufficient
    if (conf.contains("spark.executor.memory")) {
      val executorMemory = conf.getSizeAsBytes("spark.executor.memory")
      if (executorMemory < minSystemMemory) {
        throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
          s"$minSystemMemory. Please increase executor memory using the " +
          s"--executor-memory option or spark.executor.memory in Spark configuration.")
      }
    }

    // 计算可用内存usableMemory，即系统最大可用内存systemMemory减去预留内存reservedMemory
    val usableMemory = systemMemory - reservedMemory

    // 取可用内存所占比重，即参数spark.memory.fraction，默认为0.6
    val memoryFraction = conf.getDouble("spark.memory.fraction", 0.6)

    // 返回的execution和storage区域共享的最大内存为usableMemory * memoryFraction
    //< 最终 execution 和 storage 的可用内存之和为 (JVM最大可用内存 - 系统预留内存) * spark.memory.fraction
    (usableMemory * memoryFraction).toLong
  }
}
