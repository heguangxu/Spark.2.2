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

package org.apache.spark.storage

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream}
import java.nio.channels.FileChannel

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.util.Utils

/**
 * A class for writing JVM objects directly to a file on disk. This class allows data to be appended
 * to an existing block. For efficiency, it retains the underlying file channel across
 * multiple commits. This channel is kept open until close() is called. In case of faults,
 * callers should instead close with revertPartialWritesAndClose() to atomically revert the
 * uncommitted partial writes.
  *
  * 将JVM对象直接写入到磁盘上的文件的类。这个类允许将数据追加到一个现有的块上。为了提高效率，
  * 它保留跨多个提交的底层文件通道。这个通道一直保持打开，直到调用close()。在错误的情况下,
  * 调用者应该靠近revertPartialWritesAndClose()来自动恢复theuncommitted部分写道。
 *
 * This class does not support concurrent writes. Also, once the writer has been opened it cannot be
 * reopened again.
  *
  * 这个类不支持并发写。而且，一旦作者被打开，它就不能再重新开放。
  *
  * DiskBlockObjectWriter被用于输出Spark任务的中间计算结果，DiskBlockObjectWriter的fileSegment方法用于创建
  * 文件分片FileSegment(FileSegment记录分片的其实，结束偏移量)。
  *
  *
  * 创建磁盘读写对象。
 */
private[spark] class DiskBlockObjectWriter(
    val file: File,
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    bufferSize: Int,
    syncWrites: Boolean,
    // These write metrics concurrently shared with other active DiskBlockObjectWriters who
    // are themselves performing writes. All updates must be relative.
    writeMetrics: ShuffleWriteMetrics,
    val blockId: BlockId = null)
  extends OutputStream
  with Logging {

  /**
   * Guards against close calls, e.g. from a wrapping stream.
   * Call manualClose to close the stream that was extended by this trait.
   * Commit uses this trait to close object streams without paying the
   * cost of closing and opening the underlying file.
   */
  private trait ManualCloseOutputStream extends OutputStream {
    abstract override def close(): Unit = {
      flush()
    }

    def manualClose(): Unit = {
      super.close()
    }
  }

  /** The file channel, used for repositioning / truncating the file. */
  private var channel: FileChannel = null
  private var mcs: ManualCloseOutputStream = null
  private var bs: OutputStream = null
  private var fos: FileOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null
  private var initialized = false
  private var streamOpen = false
  private var hasBeenClosed = false

  /**
   * Cursors used to represent positions in the file.
   *
   * xxxxxxxxxx|----------|-----|
   *           ^          ^     ^
   *           |          |    channel.position()
   *           |        reportedPosition
   *         committedPosition
   *
   * reportedPosition: Position at the time of the last update to the write metrics.
   * committedPosition: Offset after last committed write.
   * -----: Current writes to the underlying file.
   * xxxxx: Committed contents of the file.
   */
  private var committedPosition = file.length()
  private var reportedPosition = committedPosition

  /**
   * Keep track of number of records written and also use this to periodically
   * output bytes written since the latter is expensive to do for each record.
   */
  private var numRecordsWritten = 0

  private def initialize(): Unit = {
    fos = new FileOutputStream(file, true)
    channel = fos.getChannel()
    ts = new TimeTrackingOutputStream(writeMetrics, fos)
    class ManualCloseBufferedOutputStream
      extends BufferedOutputStream(ts, bufferSize) with ManualCloseOutputStream
    mcs = new ManualCloseBufferedOutputStream
  }

  /**
    * DiskBlockObjectWriter的open方法，利用NIO,压缩，缓存，序列化方式打开一个文件输出流。
    */
  def open(): DiskBlockObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    if (!initialized) {
      initialize()
      initialized = true
    }

    bs = serializerManager.wrapStream(blockId, mcs)
    objOut = serializerInstance.serializeStream(bs)
    streamOpen = true
    this
  }

  /**
   * Close and cleanup all resources.
   * Should call after committing or reverting partial writes.
   */
  private def closeResources(): Unit = {
    if (initialized) {
      Utils.tryWithSafeFinally {
        mcs.manualClose()
      } {
        channel = null
        mcs = null
        bs = null
        fos = null
        ts = null
        objOut = null
        initialized = false
        streamOpen = false
        hasBeenClosed = true
      }
    }
  }

  /**
   * Commits any remaining partial writes and closes resources.
    * 提交剩余部分写入和关闭资源。
    *
    * DiskBlockObjectWriter的close方法，利用close方法关闭文件输出流，并且更新测量信息。
   */
  override def close() {
    if (initialized) {
      Utils.tryWithSafeFinally {
        commitAndGet()
      } {
        closeResources()
      }
    }
  }

  /**
   * Flush the partial writes and commit them as a single atomic block.
   * A commit may write additional bytes to frame the atomic block.
   *
   * @return file segment with previous offset and length committed on this call.
   */
  def commitAndGet(): FileSegment = {
    if (streamOpen) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      bs.flush()
      objOut.close()
      streamOpen = false

      if (syncWrites) {
        // Force outstanding writes to disk and track how long it takes
        val start = System.nanoTime()
        fos.getFD.sync()
        writeMetrics.incWriteTime(System.nanoTime() - start)
      }

      val pos = channel.position()
      val fileSegment = new FileSegment(file, committedPosition, pos - committedPosition)
      committedPosition = pos
      // In certain compression codecs, more bytes are written after streams are closed
      writeMetrics.incBytesWritten(committedPosition - reportedPosition)
      reportedPosition = committedPosition
      fileSegment
    } else {
      new FileSegment(file, committedPosition, 0)
    }
  }


  /**
   * Reverts writes that haven't been committed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   *
   * @return the file that this DiskBlockObjectWriter wrote to.
   */
  def revertPartialWritesAndClose(): File = {
    // Discard current writes. We do this by flushing the outstanding writes and then
    // truncating the file to its initial position.
    Utils.tryWithSafeFinally {
      if (initialized) {
        writeMetrics.decBytesWritten(reportedPosition - committedPosition)
        writeMetrics.decRecordsWritten(numRecordsWritten)
        streamOpen = false
        closeResources()
      }
    } {
      var truncateStream: FileOutputStream = null
      try {
        truncateStream = new FileOutputStream(file, true)
        truncateStream.getChannel.truncate(committedPosition)
      } catch {
        case e: Exception =>
          logError("Uncaught exception while reverting partial writes to file " + file, e)
      } finally {
        if (truncateStream != null) {
          truncateStream.close()
          truncateStream = null
        }
      }
    }
    file
  }

  /**
   * Writes a key-value pair.
    *
    * DiskBlockObjectWriter的write方法，利用write方法将数据写入文件，并且更新测量信息。
   */
  def write(key: Any, value: Any) {
    if (!streamOpen) {
      open()
    }

    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
  }

  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (!streamOpen) {
      open()
    }

    bs.write(kvBytes, offs, len)
  }

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   */
  def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)

    if (numRecordsWritten % 16384 == 0) {
      updateBytesWritten()
    }
  }

  /**
   * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  private def updateBytesWritten() {
    val pos = channel.position()
    writeMetrics.incBytesWritten(pos - reportedPosition)
    reportedPosition = pos
  }

  // For testing
  private[spark] override def flush() {
    objOut.flush()
    bs.flush()
  }
}
