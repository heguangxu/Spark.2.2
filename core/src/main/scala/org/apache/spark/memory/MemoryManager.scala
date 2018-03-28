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

import javax.annotation.concurrent.GuardedBy

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.memory.MemoryStore
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.memory.MemoryAllocator

/**
 * An abstract memory manager that enforces how memory is shared between execution and storage.
 *
  *  一个抽象的内存管理器，用于执行内存如何在执行和存储之间共享。
  *
 * In this context, execution memory refers to that used for computation in shuffles, joins,
 * sorts and aggregations, while storage memory refers to that used for caching and propagating
 * internal data across the cluster. There exists one MemoryManager per JVM.
  *
  * 在这种情况下，执行内存指的是在shuffles、join、排序和聚合中进行计算，而存储内存则是用于在集群中
  * 缓存和传播内部数据。每个JVM都有一个MemoryManager。
  *
  * MemoryStore负责将没有序列化的java对象数组或者序列化的ByteBuffer存储到内存中。
  * 我们用一张图来说明MemoryStore的内存模型。
  *
  * =========================================================== ------------------------------------------------
  * ||               Thread1                              ||    >             >        >                >
  * ||----------------------------------------------------||    |             |        |                |
  * ||               Thread2                              ||    |             |   currentUnrollMemory   |
  * ||----------------------------------------------------||    |             |        |                |
  * ||               Thread3                              ||    |      freeMemory      |                |
  * ||----------------------------------------------------|| ---|-------------|-------->                |
  * ||                                                    ||    |             |                maxUnrollMemory
  * ||----------------------------------------------------||    |             |                         |
  * ||                                                    ||    maxMomory     |                         |
  * ||----------------------------------------------------|| ---|-------------|------------------------->
  * ||                                                    ||    |             |
  * ||                                                    ||    |             |
  * ||****************************************************||  --|------------->--------->
  * ||                                                    ||    |                       |
  * ||            MemoryEmtry                             ||    |                       |
  * ||                                                    ||    |                       |
  * ||****************************************************||    |               currentMemory
  * ||                *              *                    ||    |                       |
  * ||    MemoryEmtry *  MemoryEmtry *  MemoryEmtry       ||    |                       |
  * ||                *              *                    ||    >                       >
  * ||****************************************************||    -------------------------------------------------
  *
  *  上图说明：左边是MemoryStore的内存模型，右边是对应模块的说明
  *     从上图可以看到整个MemoryStore的存储分为两块：一块是被很多MemoryEntry占据的内存currentMemory,这些MemoryEntry实际上是
  *  通过entries（既LinkedHashMap[BlockId,MemoryEntry]）持有的；另外一块是unrollMemoryMap通过占座方式占用的内存
  *  current-UnrollMemory.所谓占座，好比教室里空着的作为，有人在座位上放上书本，以防止在需要坐的时候，却发现没有位置了。
  *  比起他人的行为，unrollMemoryMap占座的出发点却是“高尚的”，这样可以防止在向内存真正写入数据时候，内存不足发生溢出。
  *  每个线程实际占用的空间，其实是vetory（既SizeTrackingVector）占用的大小，但是unrollMemoryMap的大小会稍微大些。
  *
  *  概念解析：
  *   1.maxUnrollMemory:当前Driver或者Executor最多展开的Block所占用的内存，可以修改属性spark.storage.unrollFraction改变大小。
  *   2.maxMemory:当前Driver或者Executor的最大内存。
  *   3.currentMemory:当前Driver或者Executor已经使用的内存。
  *   4.freeMemory:当前Driver或者Executor未使用的内存，freeMemory = maxMemory-currentMemory
  *   5.currentUnrollMemory:unrollMemoryMap中所有展开的Block的内存之和，即当前Driver或者Executor中
  *     所有线程展开的Block的内存之和。
  *   6.unrollMemoryMap：当前Driver或者Executor所有线程展开的Block都存入此Map中，key为线程Id,value为线程展开的所有
  *     块的内存的大小总和。
 */
private[spark] abstract class MemoryManager(
    conf: SparkConf,
    numCores: Int,
    onHeapStorageMemory: Long,
    onHeapExecutionMemory: Long) extends Logging {

  // -- Methods related to memory allocation policies and bookkeeping ------------------------------

  @GuardedBy("this")
  protected val onHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.ON_HEAP)
  @GuardedBy("this")
  protected val offHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.OFF_HEAP)

  // 这两个都是ExecutionMemoryPool类型，只是在名称上有不同的识别，一个是“on-heap execution”，
  // 一个是”off-heap execution”。对应execution内存池。
  @GuardedBy("this")
  protected val onHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.ON_HEAP)
  @GuardedBy("this")
  protected val offHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.OFF_HEAP)

  onHeapStorageMemoryPool.incrementPoolSize(onHeapStorageMemory)
  onHeapExecutionMemoryPool.incrementPoolSize(onHeapExecutionMemory)

  protected[this] val maxOffHeapMemory = conf.getSizeAsBytes("spark.memory.offHeap.size", 0)
  protected[this] val offHeapStorageMemory =
    (maxOffHeapMemory * conf.getDouble("spark.memory.storageFraction", 0.5)).toLong

  offHeapExecutionMemoryPool.incrementPoolSize(maxOffHeapMemory - offHeapStorageMemory)
  offHeapStorageMemoryPool.incrementPoolSize(offHeapStorageMemory)

  /**
   * Total available on heap memory for storage, in bytes. This amount can vary over time,
   * depending on the MemoryManager implementation.
   * In this model, this is equivalent to the amount of memory not occupied by execution.
    *
    * 总可用堆内存存储，以字节为单位。根据MemoryManager实现的不同，这个数量会随时间而变化。在这个模型中，
    * 这相当于未被执行占用的内存的数量。
   */
  def maxOnHeapStorageMemory: Long

  /**
   * Total available off heap memory for storage, in bytes. This amount can vary over time,
   * depending on the MemoryManager implementation.
    *
    * 总可用堆内存以字节为单位存储。根据MemoryManager实现的不同，这个数量会随时间而变化。
   */
  def maxOffHeapStorageMemory: Long

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
    *
    * 设置此管理器使用的[[MemoryStore]]来驱逐缓存的块。这必须在构造之后设置，因为初始化的顺序约束。
    *
    * 设置storage内存池中存储对象
   */
  final def setMemoryStore(store: MemoryStore): Unit = synchronized {
    onHeapStorageMemoryPool.setMemoryStore(store)
    offHeapStorageMemoryPool.setMemoryStore(store)
  }

  /**
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
    * 获取N个字节的内存，以缓存给定的块，如果需要，将现有的块驱逐出去。
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireStorageMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean

  /**
   * Acquire N bytes of memory to unroll the given block, evicting existing ones if necessary.
    * 获取N字节的内存，以打开给定的块，如果需要，将现有的块驱逐出去。
   *
   * This extra method allows subclasses to differentiate behavior between acquiring storage
   * memory and acquiring unroll memory. For instance, the memory management model in Spark
   * 1.5 and before places a limit on the amount of space that can be freed from unrolling.
    *
    * 这种额外的方法允许子类区分获取存储内存和获取展开内存之间的行为。例如，Spark 1.5中的内存管理模型，
    * 然后对可以释放的空间进行限制。
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireUnrollMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean

  /**
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   */
  private[memory]
  def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long

  /**
   * Release numBytes of execution memory belonging to the given task.
    * 释放属于给定任务的执行内存的numBytes。
    *
    * 根据传入的taskAttemptId 以及numBytes，将对应task的execution部分内存释放出numBytes。
   */
  private[memory]
  def releaseExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Unit = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
    }
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
    * 为给定任务释放所有内存并将其标记为非活动(例如，当任务结束时)。
    *
    * 根据传入的taskAttemptId ，将对应的onHeap和offHeap的execution内存池内存全部释放。 releaseStorageMemory
   *
   * @return the number of bytes freed.
   */
  private[memory] def releaseAllExecutionMemoryForTask(taskAttemptId: Long): Long = synchronized {
    onHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId)
  }

  /**
   * Release N bytes of storage memory.
    * 释放N字节的存储内存。
    *
    * 根据传入的numBytes，将storage内存池的内存释放出numBytes。
   */
  def releaseStorageMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapStorageMemoryPool.releaseMemory(numBytes)
      case MemoryMode.OFF_HEAP => offHeapStorageMemoryPool.releaseMemory(numBytes)
    }
  }

  /**
   * Release all storage memory acquired.
    * 释放所有已获得的存储内存。
    *
    * 将所有的storage内存池的内存全部释放。
   */
  final def releaseAllStorageMemory(): Unit = synchronized {
    onHeapStorageMemoryPool.releaseAllMemory()
    offHeapStorageMemoryPool.releaseAllMemory()
  }

  /**
   * Release N bytes of unroll memory.
    * 释放N字节的unroll内存。
    *
    * 根据传入的numBytes，将unroll内存释放出numBytes。最后也是调用方法（releaseStorageMemory(numBytes: Long) ）。
   */
  final def releaseUnrollMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
    releaseStorageMemory(numBytes, memoryMode)
  }

  /**
   * Execution memory currently in use, in bytes.
    * 当前使用的执行内存，以字节为单位。
    *
    * 计算onHeap和offHeap的Execution内存池的总使用量。
   */
  final def executionMemoryUsed: Long = synchronized {
    onHeapExecutionMemoryPool.memoryUsed + offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * Storage memory currently in use, in bytes.
    * 当前使用的存储内存，以字节为单位。
    *
    * 计算当前storage内存池的使用量
   */
  final def storageMemoryUsed: Long = synchronized {
    onHeapStorageMemoryPool.memoryUsed + offHeapStorageMemoryPool.memoryUsed
  }

  /**
   * Returns the execution memory consumption, in bytes, for the given task.
    * 以字节为单位返回给定任务的执行内存消耗
    *
    * 根据传入的taskAttemptId，计算该task使用的onHeap和offHeap内存。
   */
  private[memory] def getExecutionMemoryUsageForTask(taskAttemptId: Long): Long = synchronized {
    onHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId)
  }

  // -- Fields related to Tungsten managed memory -------------------------------------------------

  /**
   * Tracks whether Tungsten memory will be allocated on the JVM heap or off-heap using
   * sun.misc.Unsafe.
    *
    * 跟踪Tungsten存储器是否会在JVM堆上或在堆上使用sun.misc.Unsafe.
    *
    * final变量，根据spark.memory.offHeap.enabled参数（默认为false）来决定是ON_HEAP还是OFF_HEAP。
    *
    * 如果设置为true，则为OFF_HEAP，但同时要求参数spark.memory.offHeap.size（默认为0），设置为大于0的值。
    * 设置为默认值false，则为ON_HEAP
   */
  final val tungstenMemoryMode: MemoryMode = {
    if (conf.getBoolean("spark.memory.offHeap.enabled", false)) {
      require(conf.getSizeAsBytes("spark.memory.offHeap.size", 0) > 0,
        "spark.memory.offHeap.size must be > 0 when spark.memory.offHeap.enabled == true")
      require(Platform.unaligned(),
        "No support for unaligned Unsafe. Set spark.memory.offHeap.enabled to false.")
      MemoryMode.OFF_HEAP
    } else {
      MemoryMode.ON_HEAP
    }
  }

  /**
   * The default page size, in bytes.
    * 默认的页面大小，以字节为单位。
   *
   * If user didn't explicitly set "spark.buffer.pageSize", we figure out the default value
   * by looking at the number of cores available to the process, and the total amount of memory,
   * and then divide it by a factor of safety.
    *
    * 如果用户没有显式设置“spark.buffer.pageSize”。在pageSize中，我们通过查看进程中可用的核心数量和内存总量来确定默认值，
    * 然后将其除以一个安全系数。
    *
    * 　大小由参数spark.buffer.pageSize决定，默认值有一个计算逻辑：default值由变量（onHeapExecutionMemoryPool和offHeapExecutionMemoryPool ）
    * 的大小除以核数，再除以safetyFactor(16)得到的最接近的2的幂值，然后限定在1M和64M之间。
   */
  val pageSizeBytes: Long = {
    val minPageSize = 1L * 1024 * 1024   // 1MB
    val maxPageSize = 64L * minPageSize  // 64MB
    val cores = if (numCores > 0) numCores else Runtime.getRuntime.availableProcessors()
    // Because of rounding to next power of 2, we may have safetyFactor as 8 in worst case
    val safetyFactor = 16
    val maxTungstenMemory: Long = tungstenMemoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.poolSize
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.poolSize
    }
    val size = ByteArrayMethods.nextPowerOf2(maxTungstenMemory / cores / safetyFactor)
    val default = math.min(maxPageSize, math.max(minPageSize, size))
    conf.getSizeAsBytes("spark.buffer.pageSize", default)
  }

  /**
   * Allocates memory for use by Unsafe/Tungsten code.
    * 分配内存用于不安全/Tungsten代码。
    *
    * final变量，根据上面变量模式来选择是使用HeapMemoryAllocator，还是使用UnsafeMemoryAllocator。
    * ON_HEAP对应HEAP，OFF_HEAP对应Unsafe。
   */
  private[memory] final val tungstenMemoryAllocator: MemoryAllocator = {
    tungstenMemoryMode match {
      case MemoryMode.ON_HEAP => MemoryAllocator.HEAP
      case MemoryMode.OFF_HEAP => MemoryAllocator.UNSAFE
    }
  }
}
