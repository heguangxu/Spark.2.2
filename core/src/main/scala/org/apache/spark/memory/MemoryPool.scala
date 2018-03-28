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

/**
 * Manages bookkeeping for an adjustable-sized region of memory. This class is internal to
 * the [[MemoryManager]]. See subclasses for more details.
  * 为一个可调整大小的内存区域管理记账工作。这个类在MemoryManager类中使用。更多详情请查看实现子类。
 *
 * @param lock a [[MemoryManager]] instance, used for synchronization. We purposely erase the type
 *             to `Object` to avoid programming errors, since this object should only be used for
 *             synchronization purposes.
  *
  *
 */
private[memory] abstract class MemoryPool(lock: Object) {

  // 内存池的大小
  @GuardedBy("lock")
  private[this] var _poolSize: Long = 0

  /**
   * Returns the current size of the pool, in bytes.
    *
    * 获取内存池大小，需要使用对象lock的同步关键字synchronized，解决并发的问题，
    * 而这个lock就是StaticMemoryManager或UnifiedMemoryManager类类型的对象
   */
  final def poolSize: Long = lock.synchronized {
    _poolSize
  }

  /**
   * Returns the amount of free memory in the pool, in bytes.
    *
    * 返回内存池可用内存，即内存池总大小减去已用内存，同样需要使用lock的同步关键字synchronized，解决并发的问题
   */
  final def memoryFree: Long = lock.synchronized {
    _poolSize - memoryUsed
  }

  /**
   * Expands the pool by `delta` bytes.
    *
    * 对内存池进行delta bytes的扩充，即完成_poolSize += delta，delta必须大于等于0
   */
  final def incrementPoolSize(delta: Long): Unit = lock.synchronized {
    require(delta >= 0)
    _poolSize += delta
  }

  /**
   * Shrinks the pool by `delta` bytes.
    *
    * 对内存池进行delta字节的收缩，即_poolSize -= delta，delta必须大于等于0，且小于内存池现在的大小，
    * 并且必须小于等于内存池现在
   */
  final def decrementPoolSize(delta: Long): Unit = lock.synchronized {
    require(delta >= 0)
    require(delta <= _poolSize)
    require(_poolSize - delta >= memoryUsed)
    _poolSize -= delta
  }

  /**
   * Returns the amount of used memory in this pool (in bytes).
    *
    * 返回内存池现在已使用的大小，由子类实现
   */
  def memoryUsed: Long
}
