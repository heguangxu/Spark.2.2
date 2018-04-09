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

package org.apache.spark.memory;

import java.io.IOException;

import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * A memory consumer of {@link TaskMemoryManager} that supports spilling.
 * 支持溢出的{@link TaskMemoryManager}的内存使用者。
 *
 * Note: this only supports allocation / spilling of Tungsten memory.
 * 注意：这只支持分配/溢出钨内存。
 */
public abstract class MemoryConsumer {

  protected final TaskMemoryManager taskMemoryManager;
  private final long pageSize;
  private final MemoryMode mode;
  protected long used;

  protected MemoryConsumer(TaskMemoryManager taskMemoryManager, long pageSize, MemoryMode mode) {
    this.taskMemoryManager = taskMemoryManager;
    this.pageSize = pageSize;
    this.mode = mode;
  }

  protected MemoryConsumer(TaskMemoryManager taskMemoryManager) {
    this(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.ON_HEAP);
  }

  /**
   * Returns the memory mode, {@link MemoryMode#ON_HEAP} or {@link MemoryMode#OFF_HEAP}.
   * 返回内存模式，{@link MemoryMode#ON_HEAP}或{@link MemoryMode#OFF_HEAP}。
   */
  public MemoryMode getMode() {
    return mode;
  }

  /**
   * Returns the size of used memory in bytes.
   * 返回以字节为单位的使用内存的大小。
   */
  protected long getUsed() {
    return used;
  }

  /**
   * Force spill during building.
   * 在building期间强制spill。
   */
  public void spill() throws IOException {
    spill(Long.MAX_VALUE, this);
  }

  /**
   * Spill some data to disk to release memory, which will be called by TaskMemoryManager
   * when there is not enough memory for the task.
   *
   * 将一些数据泄漏到磁盘以释放内存，当任务没有足够的内存时，TaskMemoryManager将调用它。
   *
   * This should be implemented by subclass.  这应该由子类来实现。
   *
   * Note: In order to avoid possible deadlock, should not call acquireMemory() from spill().
   * 注意:为了避免可能的死锁，不应调用泄漏()。
   *
   * Note: today, this only frees Tungsten-managed pages.
   * 注意:今天，这只释放了钨丝管理的页面。
   *
   * @param size the amount of memory should be released
   * @param trigger the MemoryConsumer that trigger this spilling
   * @return the amount of released memory in bytes
   * @throws IOException
   */
  public abstract long spill(long size, MemoryConsumer trigger) throws IOException;

  /**
   * Allocates a LongArray of `size`. 分配一个“大小”的长数组。
   */
  public LongArray allocateArray(long size) {
    long required = size * 8L;
    MemoryBlock page = taskMemoryManager.allocatePage(required, this);
    if (page == null || page.size() < required) {
      long got = 0;
      if (page != null) {
        got = page.size();
        taskMemoryManager.freePage(page, this);
      }
      taskMemoryManager.showMemoryUsage();
      throw new OutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " + got);
    }
    used += required;
    return new LongArray(page);
  }

  /**
   * Frees a LongArray. 释放一个LongArray。
   */
  public void freeArray(LongArray array) {
    freePage(array.memoryBlock());
  }

  /**
   * Allocate a memory block with at least `required` bytes. 用至少需要的字节来分配内存块。
   *
   * Throws IOException if there is not enough memory.  如果没有足够的内存，就会抛出IOException。
   *
   * @throws OutOfMemoryError
   */
  protected MemoryBlock allocatePage(long required) {
    MemoryBlock page = taskMemoryManager.allocatePage(Math.max(pageSize, required), this);
    if (page == null || page.size() < required) {
      long got = 0;
      if (page != null) {
        got = page.size();
        taskMemoryManager.freePage(page, this);
      }
      taskMemoryManager.showMemoryUsage();
      throw new OutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " + got);
    }
    used += page.size();
    return page;
  }

  /**
   * Free a memory block. 释放的一个内存块。
   */
  protected void freePage(MemoryBlock page) {
    used -= page.size();
    taskMemoryManager.freePage(page, this);
  }

  /**
   * Allocates memory of `size`.  分配内存的大小。
   */
  public long acquireMemory(long size) {
    long granted = taskMemoryManager.acquireExecutionMemory(size, this);
    used += granted;
    return granted;
  }

  /**
   * Release N bytes of memory.  释放N个字节的内存。
   */
  public void freeMemory(long size) {
    taskMemoryManager.releaseExecutionMemory(size, this);
    used -= size;
  }
}
