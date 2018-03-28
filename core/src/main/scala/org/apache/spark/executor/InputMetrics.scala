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
 * Method by which input data was read. Network means that the data was read over the network
 * from a remote block manager (which may have stored the data on-disk or in-memory).
 * Operations are not thread-safe.
  *
  * 读取输入数据的方法。网络意味着数据是从远程块管理器(可能存储数据磁盘或内存中)的网络上读取的。
  * 操作不是线程安全的。
 */
@DeveloperApi
object DataReadMethod extends Enumeration with Serializable {
  type DataReadMethod = Value
  val Memory, Disk, Hadoop, Network = Value
}


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represents metrics about reading data from external systems.
  * 一组表示从外部系统读取数据的指标的累加器集合。
 */
@DeveloperApi
class InputMetrics private[spark] () extends Serializable {
  private[executor] val _bytesRead = new LongAccumulator
  private[executor] val _recordsRead = new LongAccumulator

  /**
   * Total number of bytes read.    总共读取的字节数
   */
  def bytesRead: Long = _bytesRead.sum

  /**
   * Total number of records read.  总共读取的记录数
   */
  def recordsRead: Long = _recordsRead.sum

  private[spark] def incBytesRead(v: Long): Unit = _bytesRead.add(v)
  private[spark] def incRecordsRead(v: Long): Unit = _recordsRead.add(v)
  private[spark] def setBytesRead(v: Long): Unit = _bytesRead.setValue(v)
}
