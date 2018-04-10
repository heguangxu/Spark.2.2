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

import scala.collection.mutable

import org.apache.spark.internal.Logging

/**
 * Implements policies and bookkeeping for sharing an adjustable-sized pool of memory between tasks.
  *
  * 实现策略和bookkeeping，以在任务之间共享可调整大小的内存池。
 *
 * Tries to ensure that each task gets a reasonable share of memory, instead of some task ramping up
 * to a large amount first and then causing others to spill to disk repeatedly.
  *
  * 试着确保每个任务得到合理的内存份额，而不是一些任务先增加到一个大的数量，然后再让其他任务重复地溢出到磁盘上。
 *
 * If there are N tasks, it ensures that each task can acquire at least 1 / 2N of the memory
 * before it has to spill, and at most 1 / N. Because N varies dynamically, we keep track of the
 * set of active tasks and redo the calculations of 1 / 2N and 1 / N in waiting tasks whenever this
 * set changes. This is all done by synchronizing access to mutable state and using wait() and
 * notifyAll() to signal changes to callers. Prior to Spark 1.6, this arbitration of memory across
 * tasks was performed by the ShuffleMemoryManager.
  *
  * 如果有N个任务,确保每个任务可以获得至少 1/2  N的内存，在task任务分隔之前,最多 1/N,因为N动态变化,
  * 我们跟踪活动任务的集合和重做1的计算/ 2 N和1 / N在等待任务只要这组发生变化。
  * 这一切都是通过同步访问可变状态和使用wait()和notifyAll()来对调用者进行信号更改来实现的。
  * 在Spark 1.6之前，这个关于任务的内存仲裁是由ShuffleMemoryManager执行的。
 *
 * @param lock a [[MemoryManager]] instance to synchronize on
 * @param memoryMode the type of memory tracked by this pool (on- or off-heap)
 */
private[memory] class ExecutionMemoryPool(
    lock: Object,
    memoryMode: MemoryMode
  ) extends MemoryPool(lock) with Logging {

  private[this] val poolName: String = memoryMode match {
    case MemoryMode.ON_HEAP => "on-heap execution"
    case MemoryMode.OFF_HEAP => "off-heap execution"
  }

  /**
   * Map from taskAttemptId -> memory consumption in bytes
    * 从taskAttemptId -> memory内存消耗的字节映射
    *
    * 这个变量的定义如下，是一个HashMap结构，用于存储每个Task所使用的Execution内存情况，key为taskAttemptId,
    * value为使用的内存数
    *
    * taskAttemptId到内存耗费的映射
    * 非常重要的数据结构memoryForTask，保存的是taskAttemptId到内存耗费的映射
   */
  @GuardedBy("lock")
  private val memoryForTask = new mutable.HashMap[Long, Long]()

  // 获取已使用内存，需要在对象lock上使用synchronized关键字，解决并发的问题
  override def memoryUsed: Long = lock.synchronized {
    memoryForTask.values.sum
  }

  /**
   * Returns the memory consumption, in bytes, for the given task.
    *
    * 返回给定Task的内存耗费
   */
  def getMemoryUsageForTask(taskAttemptId: Long): Long = lock.synchronized {
    memoryForTask.getOrElse(taskAttemptId, 0L)
  }

  /**
   * Try to acquire up to `numBytes` of memory for the given task and return the number of bytes
   * obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   *
   * @param numBytes number of bytes to acquire
   * @param taskAttemptId the task attempt acquiring memory
   * @param maybeGrowPool a callback that potentially grows the size of this pool. It takes in
   *                      one parameter (Long) that represents the desired amount of memory by
   *                      which this pool should be expanded.
   * @param computeMaxPoolSize a callback that returns the maximum allowable size of this pool
   *                           at this given moment. This is not a field because the max pool
   *                           size is variable in certain cases. For instance, in unified
   *                           memory management, the execution pool can be expanded by evicting
   *                           cached blocks, thereby shrinking the storage pool.
   *
   * @return the number of bytes granted to the task.
    *
    *@book
    *    此方法用于当前线程舱室获得numBytes大小的内存，并且返回实际获得的内存大小。
    *
    * 不确定的注释： 1.5版本
    *   ShuffleMemoryManager用于执行shuffle操作的线程分配内存次。每种磁盘溢出集合（如ExternalAppendOnlyMap和ExternalSorter）
    *   都能从这个内存次获得内存，当溢出集合的数据已经是维护处到存储系统，获得的内存会释放。当线程执行的任务结束，整个内存池都会被Executor
    *   释放。ShuffleMemoryManager会保证每个县城都能合理的共享内存，而不会使得一些线程获得了很大的内存，导致其他线程经常不得不将溢出的
    *   数据写入到磁盘。
    *
    *
    * acquireMemory在1.5版本名字叫tryToAcquire.
    * 此方法用于尝试获得numBytes大小的内存，并且返回实际获得的内存大小。
    * 根据ShuffleMemoryManager的实现，他的处理逻辑：假设当前有N个线程，必须保证每个线程在溢出前至少获得1/2N的内存，并且每个线程做多获得
    * 1/N的内存，由于N是动态变化的。所以要持续对这些线程进行跟踪，以便无论何时在这些线程发生变化的时候重新按照1/2N和1/N计算。
    *
    * 解释里面的while循环：
    *   程序一直处理该task的请求，直到系统判定无法满足该请求或者已经为该请求分配到足够的内存为止。如果当前execution内存池
    * 剩余内存不足以满足此次请求时，会向storage部分请求释放出被借走的内存以满足此次请求。
    *
    *    根据此刻execution内存池的总大小maxPoolSize，以及从memoryForTask中统计出的处于active状态的task的个数计算出每个
    * task能够得到的最大内存数maxMemoryPerTask = maxPoolSize / numActiveTasks。每个task能够得到的最少内存数
    * minMemoryPerTask = poolSize / (2 * numActiveTasks)。
    *
    *   根据申请内存的task当前使用的execution内存大小决定分配给该task多少内存，总的内存不能超过maxMemoryPerTask。但是如果
    * execution内存池能够分配的最大内存小于numBytes并且如果把能够分配的内存分配给当前task，但是该task最终得到的execution
    * 内存还是小于minMemoryPerTask时，该task进入等待状态，等其他task申请内存时将其唤醒。如果满足内存分配统计，就会返回能够
    * 分配的内存数，并且更新memoryForTask，将该task使用的内存调整为分配后的值。一个Task最少需要minMemoryPerTask才能开始
    * 执行。
    *
    * 1、校验，确保申请内存numBytes的大小必须大于0；
        2、如果memoryForTask中不包含该Task，加入该Task，初始化为0，并唤醒其它等待的对象；
        3、在一个循环体中：
              3.1、获取当前活跃Task的数目numActiveTasks；
              3.2、获取该Task对应的当前已耗费内存curMem；
              3.3、maybeGrowPool为传进来的UnifiedMemoryManager的maybeGrowExecutionPool()方法，其通过收回缓存的块扩充the execution pool，从而减少the storage pool；
              3.4、计算内存池的最大大小maxPoolSize；
              3.5、平均每个Task分配的最大内存大小maxMemoryPerTask；
              3.6、平均每个Task分配的最小内存大小minMemoryPerTask，为maxMemoryPerTask的一半；
              3.7、计算我们可以赋予该Task的最大大小maxToGrant，取numBytes和（maxMemoryPerTask - curMem与0较大者）中的较小者，也就是，如果当前已耗费内存大于maxMemoryPerTask，则为0，不再分配啦，否则取还可以分配的内存和申请分配的内存中的较小者；
              3.8、计算实际可以分配的最大大小toGrant，取maxToGrant和memoryFree中的较小者；
              3.9、如果实际分配的内存大小toGrant小于申请分配的内存大小numBytes，且当前已耗费内存加上马上就要分配的内存，小于Task需要的最小内存，记录日志信息，lock等待，即MemoryManager等待；否则memoryForTask中对应Task的已耗费内存增加toGrant，返回申请的内存大小toGrant，跳出循环。
    *
    *  参考博客：https://blog.csdn.net/qq_21383435/article/details/79108106
    */
  private[memory] def acquireMemory(
      numBytes: Long,
      taskAttemptId: Long,
      maybeGrowPool: Long => Unit = (additionalSpaceNeeded: Long) => Unit,
      computeMaxPoolSize: () => Long = () => poolSize): Long = lock.synchronized {

    // 申请内存numBytes的大小必须大于0
    assert(numBytes > 0, s"invalid number of bytes requested: $numBytes")

    // TODO: clean up this clunky method signature

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to `acquireMemory`
    // 如果memoryForTask中不包含该Task，加入该Task，初始化为0，并唤醒其它等待的对象
    if (!memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) = 0L
      // This will later cause waiting tasks to wake up and check numTasks again
      lock.notifyAll()
    }

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // task would have more than 1 / numActiveTasks of the memory) or we have enough free
    // memory to give it (we always let each task get at least 1 / (2 * numActiveTasks)).
    // TODO: simplify this to limit each task to its own slot
    while (true) {
      // 获取当前活跃Task的数目
      val numActiveTasks = memoryForTask.keys.size
      // 获取该Task对应的当前已耗费内存
      val curMem = memoryForTask(taskAttemptId)

      // In every iteration of this loop, we should first try to reclaim any borrowed execution
      // space from storage. This is necessary because of the potential race condition where new
      // storage blocks may steal the free execution memory that this task was waiting for.
      // 传进来的UnifiedMemoryManager的maybeGrowExecutionPool()方法
      // 通过收回缓存的块扩充the execution pool，从而减少the storage pool
      maybeGrowPool(numBytes - memoryFree)

      // Maximum size the pool would have after potentially growing the pool.
      // This is used to compute the upper bound of how much memory each task can occupy. This
      // must take into account potential free memory as well as the amount this pool currently
      // occupies. Otherwise, we may run into SPARK-12155 where, in unified memory management,
      // we did not take into account space that could have been freed by evicting cached blocks.
      // 计算内存池的最大大小maxPoolSize
      val maxPoolSize = computeMaxPoolSize()

      // 平均每个Task分配的最大内存大小maxMemoryPerTask
      val maxMemoryPerTask = maxPoolSize / numActiveTasks

      // 平均每个Task分配的最小内存大小minMemoryPerTask，为maxMemoryPerTask的一半
      val minMemoryPerTask = poolSize / (2 * numActiveTasks)

      // How much we can grant this task; keep its share within 0 <= X <= 1 / numActiveTasks
      // 我们可以赋予该Task的最大大小，取numBytes和（maxMemoryPerTask - curMem与0较大者）中的较小者
      // 如果当前已耗费内存大于maxMemoryPerTask，则为0，不再分配啦，否则取还可以分配的内存和申请分配的内存中的较小者
      val maxToGrant = math.min(numBytes, math.max(0, maxMemoryPerTask - curMem))

      // Only give it as much memory as is free, which might be none if it reached 1 / numTasks
      // 实际可以分配的最大大小，取maxToGrant和memoryFree中的较小者
      val toGrant = math.min(maxToGrant, memoryFree)

      // We want to let each task get at least 1 / (2 * numActiveTasks) before blocking;
      // if we can't give it this much now, wait for other tasks to free up memory
      // (this happens if older tasks allocated lots of memory before N grew)
      if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
        // 如果实际分配的内存大小toGrant小于申请分配的内存大小numBytes，且当前已耗费内存加上马上就要分配的内存，小于Task需要的最小内存
        // 记录日志信息
        logInfo(s"TID $taskAttemptId waiting for at least 1/2N of $poolName pool to be free")
        // lock等待，即MemoryManager等待
        lock.wait()
      } else {
        // 对应Task的已耗费内存增加toGrant
        memoryForTask(taskAttemptId) += toGrant
        // 返回申请的内存大小toGrant
        return toGrant
      }
    }
    0L  // Never reached
  }

  /**
   * Release `numBytes` of memory acquired by the given task.
    *
    * 释放给定Task申请的numBytes大小的内存
   */
  def releaseMemory(numBytes: Long, taskAttemptId: Long): Unit = lock.synchronized {
    // 根据Task获取当前已耗费内存
    val curMem = memoryForTask.getOrElse(taskAttemptId, 0L)

    // 如果当前已耗费内存小于需要释放的内存
    var memoryToFree = if (curMem < numBytes) {
      // 记录警告日志信息
      logWarning(
        s"Internal error: release called on $numBytes bytes but task only has $curMem bytes " +
          s"of memory from the $poolName pool")
      // 返回curMem
      curMem
    } else {
      // 否则直接返回numBytes
      numBytes
    }
    if (memoryForTask.contains(taskAttemptId)) {
      // memoryForTask中对应Task的已耗费内存减少memoryToFree
      memoryForTask(taskAttemptId) -= memoryToFree

      // 已耗费内存小于等于0的话，直接删除
      if (memoryForTask(taskAttemptId) <= 0) {
        memoryForTask.remove(taskAttemptId)
      }
    }

    // 唤醒所有等待的对象，比如acquireMemory()方法
    lock.notifyAll() // Notify waiters in acquireMemory() that memory has been freed
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   * @return the number of bytes freed.
    *
    * 释放给定Task的所有内存，并且标记其为不活跃
   */
  def releaseAllMemoryForTask(taskAttemptId: Long): Long = lock.synchronized {
    // 获取指定Task的内存使用情况
    val numBytesToFree = getMemoryUsageForTask(taskAttemptId)

    // 释放指定Task的numBytesToFree大小的内存
    releaseMemory(numBytesToFree, taskAttemptId)

    // 返回释放的大小numBytesToFree
    numBytesToFree
  }

}
