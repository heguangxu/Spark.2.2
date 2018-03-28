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

import org.apache.spark.unsafe.types.UTF8String

/**
  * This holds file names of the current Spark task. This is used in HadoopRDD,
  * FileScanRDD, NewHadoopRDD and InputFileName function in Spark SQL.
  *
  * 这是当前Spark任务文件名。这些在Spark SQL中的HadoopRDD，FileScanRDD, NewHadoopRDD 和 InputFileName 被使用
  *
  */
private[spark] object InputFileBlockHolder {
  /**
    * A wrapper around some input file information.
    * 在输入文件信息的包装。
    *
    * @param filePath path of the file read, or empty string if not available.
    *                 文件的读取路径，或如果文件不存在则为空字符串。
    * @param startOffset starting offset, in bytes, or -1 if not available.
    *                   开始偏移字节，或如果不可用则为-1。
    * @param length size of the block, in bytes, or -1 if not available.
    *               块的大小，字节，或如果不可用则为-1。
    */
  private class FileBlock(val filePath: UTF8String, val startOffset: Long, val length: Long) {
    def this() {
      this(UTF8String.fromString(""), -1, -1)
    }
  }

  /**
    * The thread variable for the name of the current file being read. This is used by
    * the InputFileName function in Spark SQL.
    *
    * 对于当前的文件被读取线程变量名称。这被用于 Spark SQL的InputFileName函数中。
    */
  private[this] val inputBlock: InheritableThreadLocal[FileBlock] =
  new InheritableThreadLocal[FileBlock] {
  override protected def initialValue(): FileBlock = new FileBlock
}

  /**
    * Returns the holding file name or empty string if it is unknown.
    * 如果它是未知的返回文件名或空字符串。
    */
  def getInputFilePath: UTF8String = inputBlock.get().filePath

  /**
    * Returns the starting offset of the block currently being read, or -1 if it is unknown.
    * 返回当前正在读取的块的起始偏移，或如果它是未知的那么就返回-1。
    */
  def getStartOffset: Long = inputBlock.get().startOffset

  /**
    * Returns the length of the block being read, or -1 if it is unknown.
    * 返回当前正在读取的块的长度，或如果它是未知的那么就返回-1。
    */
  def getLength: Long = inputBlock.get().length

  /**
    * Sets the thread-local input block.
    * 设置线程本地输入块。
    */
  def set(filePath: String, startOffset: Long, length: Long): Unit = {
  require(filePath != null, "filePath cannot be null")
  require(startOffset >= 0, s"startOffset ($startOffset) cannot be negative")
  require(length >= 0, s"length ($length) cannot be negative")
  inputBlock.set(new FileBlock(UTF8String.fromString(filePath), startOffset, length))
}

  /**
    * Clears the input file block to default value.
    * 清除输入文件块的默认值。
    */
  def unset(): Unit = inputBlock.remove()
}
