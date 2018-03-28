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

package org.apache.spark.executor

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.LongAccumulator


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about writing shuffle data.
 * Operations are not thread-safe.
  *
  * 一个累加器的集合，表示关于写入shuffle数据的指标。操作不是线程安全的。
 */
@DeveloperApi
class ShuffleWriteMetrics private[spark] () extends Serializable {
  private[executor] val _bytesWritten = new LongAccumulator
  private[executor] val _recordsWritten = new LongAccumulator
  private[executor] val _writeTime = new LongAccumulator

  /**
   * Number of bytes written for the shuffle by this task.
    * 这个任务总共写的的字节数。
   */
  def bytesWritten: Long = _bytesWritten.sum

  /**
   * Total number of records written to the shuffle by this task.
    * 这个任务总共写的的字记录数。
   */
  def recordsWritten: Long = _recordsWritten.sum

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds.
    * 在纳秒内，任务花费在写入磁盘或缓冲区缓存上的时间。
   */
  def writeTime: Long = _writeTime.sum

  private[spark] def incBytesWritten(v: Long): Unit = _bytesWritten.add(v)
  private[spark] def incRecordsWritten(v: Long): Unit = _recordsWritten.add(v)
  private[spark] def incWriteTime(v: Long): Unit = _writeTime.add(v)
  private[spark] def decBytesWritten(v: Long): Unit = {
    _bytesWritten.setValue(bytesWritten - v)
  }
  private[spark] def decRecordsWritten(v: Long): Unit = {
    _recordsWritten.setValue(recordsWritten - v)
  }

  // Legacy methods for backward compatibility.     用于向后兼容性的遗留方法。
  // TODO: remove these once we make this class private.
  @deprecated("use bytesWritten instead", "2.0.0")
  def shuffleBytesWritten: Long = bytesWritten
  @deprecated("use writeTime instead", "2.0.0")
  def shuffleWriteTime: Long = writeTime
  @deprecated("use recordsWritten instead", "2.0.0")
  def shuffleRecordsWritten: Long = recordsWritten

}
