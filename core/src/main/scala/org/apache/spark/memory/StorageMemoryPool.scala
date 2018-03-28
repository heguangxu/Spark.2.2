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

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.memory.MemoryStore

/**
 * Performs bookkeeping for managing an adjustable-size pool of memory that is used for storage
 * (caching).
  * 执行bookkeeping，以管理用于存储(缓存)的可调整大小的内存池。
  *
  * 完成为管理一个可调整大小的用于存储（caching）的内存池的记账工作。
 *
 * @param lock a [[MemoryManager]] instance to synchronize on
 * @param memoryMode the type of memory tracked by this pool (on- or off-heap)
 */
private[memory] class StorageMemoryPool(
    lock: Object,
    memoryMode: MemoryMode
  ) extends MemoryPool(lock) with Logging {

  private[this] val poolName: String = memoryMode match {
    case MemoryMode.ON_HEAP => "on-heap storage"
    case MemoryMode.OFF_HEAP => "off-heap storage"
  }

  // 已使用内存大小
  @GuardedBy("lock")
  private[this] var _memoryUsed: Long = 0L

  // 获取已使用内存大小memoryUsed，需要使用对象lock的同步关键字synchronized，解决并发的问题，
  // 而这个lock就是StaticMemoryManager或UnifiedMemoryManager类类型的对象
  override def memoryUsed: Long = lock.synchronized {
    _memoryUsed
  }

  // MemoryStore内存存储
  private var _memoryStore: MemoryStore = _
  def memoryStore: MemoryStore = {
    if (_memoryStore == null) {
      throw new IllegalStateException("memory store not initialized yet")
    }
    _memoryStore
  }

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   */
  final def setMemoryStore(store: MemoryStore): Unit = {
    _memoryStore = store
  }

  /**
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
    * 获取N个字节的内存，以缓存给定的块，如果需要，将现有的块驱逐出去。
   *
   * @return whether all N bytes were successfully granted.
    *         是否成功地授予了所有N个字节。
    *
    *  numBytesToAcquire是申请内存的task传入的numBytes参数。
    *
    *  申请内存
    *  逻辑很简单，使用lock的synchronized，解决并发的问题，然后计算需要释放的内存大小
    *  numBytesToFree，需要申请的大小减去内存池目前可用内存大小，也就是看内存池中可用
    *  内存大小是否能满足申请分配的内存大小，然后调用多一个numBytesToFree参数的同名方法。
    *
    *
   */
  def acquireMemory(blockId: BlockId, numBytes: Long): Boolean = lock.synchronized {
    // 需要lock的synchronized，解决并发的问题
    // 需要释放的资源，为需要申请的大小减去内存池目前可用内存大小
    val numBytesToFree = math.max(0, numBytes - memoryFree)
    // 调用同名方法acquireMemory()
    acquireMemory(blockId, numBytes, numBytesToFree)
  }

  /**
   * Acquire N bytes of storage memory for the given block, evicting existing ones if necessary.
   *
   * @param blockId the ID of the block we are acquiring storage memory for
   * @param numBytesToAcquire the size of this block
   * @param numBytesToFree the amount of space to be freed through evicting blocks
   * @return whether all N bytes were successfully granted.
    *
    * numBytesToFree表示storage空闲内存与申请内存的差值，需要storage释放numBytesToFree的内存才能满足numBytes的申请。
    *
    *     在申请内存时，如果numBytes大于此刻storage内存池的剩余内存，则需要storage内存池释放一部分内存以满足申请需求。
    * 释放内存后如果memoryFree >= numBytes，就会把这部分内存分配给申请内存的task，并且更新storage内存池的使用情况。
    *
    *   释放内存部分的逻辑，调用MemoryStore#evictBlockToFreeSpace，在MemoryStore中有一个entries对象，它是一个LinkedHashMap
    * 结构，key为BlociId，value为记录了内存使用情况的一个对象。循环从最开始计算entries中每个Bokc使用的storage内存大小，
    * 取出一个就累加一下，直到累加内存大小达到前面的请求值numBytes，然后把这些BlockId对应的数据通过BlockManager充内存中
    * 直接清除，调用BlockManager#dropFromMemory把数据spill到磁盘上。
    *
    * 首先需要做一些内存大小的校验，确保内存的申请分配时合理的。校验的内容包含以下三个部分：
        1、申请分配的内存numBytesToAcquire必须大于等于0；
        2、需要释放的内存numBytesToFree必须大于等于0；
        3、已使用内存memoryUsed必须小于等于内存池大小poolSize；
        然后，如果需要释放部分内存，即numBytesToFree大于0，则调用MemoryStore的evictBlocksToFreeSpace(
     方法释放numBytesToFree大小内存，关于MemoryStore的内容我们在后续的存储管理模块再详细介绍，这里先有个概念即可。
        最后，判断是否有足够的内存，即申请分配的内存必须小于等于可用内存，如果有足够的内存，已使用内存_memoryUsed
     增加numBytesToAcquire，并返回ture，否则返回false。
   */
  def acquireMemory(
      blockId: BlockId,
      numBytesToAcquire: Long,
      numBytesToFree: Long): Boolean = lock.synchronized {

    // 申请分配的内存必须大于等于0
    assert(numBytesToAcquire >= 0)

    // 需要释放的内存必须大于等于0
    assert(numBytesToFree >= 0)

    // 已使用内存必须小于等于内存池大小
    assert(memoryUsed <= poolSize)

    if (numBytesToFree > 0) {
      // 调用MemoryStore的evictBlocksToFreeSpace()方法释放numBytesToFree大小内存
      memoryStore.evictBlocksToFreeSpace(Some(blockId), numBytesToFree, memoryMode)
    }
    // NOTE: If the memory store evicts blocks, then those evictions will synchronously call
    // back into this StorageMemoryPool in order to free memory. Therefore, these variables
    // should have been updated.
    // 判断是否有足够的内存，即申请分配的内存必须小于等于可用内存
    val enoughMemory = numBytesToAcquire <= memoryFree
    if (enoughMemory) {
      // 如果有足够的内存，已使用内存_memoryUsed增加numBytesToAcquire
      _memoryUsed += numBytesToAcquire
    }
    // 返回enoughMemory，标志内存是否分配成功，即存在可用内存的话就分配成功，否则分配不成功
    enoughMemory
  }

  // 释放size大小的内存，同样需要lock对象上的synchronized关键字，解决并发问题
  def releaseMemory(size: Long): Unit = lock.synchronized {
    // 如果size大于目前已使用内存_memoryUsed，记录Warning日志信息，且已使用内存_memoryUsed设置为0
    if (size > _memoryUsed) {
      logWarning(s"Attempted to release $size bytes of storage " +
        s"memory when we only have ${_memoryUsed} bytes")
      _memoryUsed = 0
    } else {
      // 否则，已使用内存_memoryUsed减去size大小
      _memoryUsed -= size
    }
  }

  // 释放所有的内存，同样需要lock对象上的synchronized关键字，解决并发问题，将目前已使用内存_memoryUsed设置为0
  def releaseAllMemory(): Unit = lock.synchronized {
    _memoryUsed = 0
  }

  /**
   * Free space to shrink the size of this storage memory pool by `spaceToFree` bytes.
   * Note: this method doesn't actually reduce the pool size but relies on the caller to do so.
   *
   * @return number of bytes to be removed from the pool's capacity.
   */
  def freeSpaceToShrinkPool(spaceToFree: Long): Long = lock.synchronized {
    val spaceFreedByReleasingUnusedMemory = math.min(spaceToFree, memoryFree)
    val remainingSpaceToFree = spaceToFree - spaceFreedByReleasingUnusedMemory
    if (remainingSpaceToFree > 0) {
      // If reclaiming free memory did not adequately shrink the pool, begin evicting blocks:
      val spaceFreedByEviction =
        memoryStore.evictBlocksToFreeSpace(None, remainingSpaceToFree, memoryMode)
      // When a block is released, BlockManager.dropFromMemory() calls releaseMemory(), so we do
      // not need to decrement _memoryUsed here. However, we do need to decrement the pool size.
      spaceFreedByReleasingUnusedMemory + spaceFreedByEviction
    } else {
      spaceFreedByReleasingUnusedMemory
    }
  }
}
