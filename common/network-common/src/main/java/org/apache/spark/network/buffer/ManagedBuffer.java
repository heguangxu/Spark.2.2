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

package org.apache.spark.network.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * This interface provides an immutable view for data in the form of bytes. The implementation
 * should specify how the data is provided:
 *
 * - {@link FileSegmentManagedBuffer}: data backed by part of a file
 * - {@link NioManagedBuffer}: data backed by a NIO ByteBuffer
 * - {@link NettyManagedBuffer}: data backed by a Netty ByteBuf
 *
 * The concrete buffer implementation might be managed outside the JVM garbage collector.
 * For example, in the case of {@link NettyManagedBuffer}, the buffers are reference counted.
 * In that case, if the buffer is going to be passed around to a different thread, retain/release
 * should be called.
 *
 * Spark为何使用Netty通信框架替代Akka：http://www.aboutyun.com/thread-21115-1-1.html
 *
 * Byte的表示：
 * 对于Network通信，不管传输的是序列化后的对象还是文件，在网络上表现的都是字节流。
 *      在传统IO中，字节流表示为Stream；
 *      在NIO中，字节流表示为ByteBuffer；
 *      在Netty中字节流表示为ByteBuff或FileRegion；
 *      在Spark中，针对Byte也做了一层包装，支持对Byte和文件流进行处理，即ManagedBuffer；
 *
 *  ManagedBuffer包含了三个函数createInputStream()，nioByteBuffer()，convertToNetty()来对Buffer进行“类型转换”，
 *  分别获取stream，ByteBuffer，ByteBuff或FileRegion；NioManagedBuffer/NettyManagedBuffer/FileSegmentManagedBuffer
 *  也是针对这ByteBuffer，ByteBuff或FileRegion提供了具体的实现。
 *
 *  更好的理解ManagedBuffer：比如Shuffle BlockManager模块需要在内存中维护本地executor生成的shuffle-map输出的文件引用，
 *  从而可以提供给shuffleFetch进行远程读取，此时文件表示为FileSegmentManagedBuffer，shuffleFetch远程调用
 *  FileSegmentManagedBuffer.nioByteBuffer/createInputStream函数从文件中读取为Bytes，并进行后面的网络传输。
 *  如果已经在内存中bytes就更好理解了，比如将一个字符数组表示为NettyManagedBuffer。
 *
 */
public abstract class ManagedBuffer {

  /** Number of bytes of the data. */
  public abstract long size();

  /**
   * Exposes this buffer's data as an NIO ByteBuffer. Changing the position and limit of the
   * returned ByteBuffer should not affect the content of this buffer.
   */
  // TODO: Deprecate this, usage may require expensive memory mapping or allocation.
  public abstract ByteBuffer nioByteBuffer() throws IOException;

  /**
   * Exposes this buffer's data as an InputStream. The underlying implementation does not
   * necessarily check for the length of bytes read, so the caller is responsible for making sure
   * it does not go over the limit.
   */
  public abstract InputStream createInputStream() throws IOException;

  /**
   * Increment the reference count by one if applicable.
   */
  public abstract ManagedBuffer retain();

  /**
   * If applicable, decrement the reference count by one and deallocates the buffer if the
   * reference count reaches zero.
   */
  public abstract ManagedBuffer release();

  /**
   * Convert the buffer into an Netty object, used to write the data out. The return value is either
   * a {@link io.netty.buffer.ByteBuf} or a {@link io.netty.channel.FileRegion}.
   *
   * If this method returns a ByteBuf, then that buffer's reference count will be incremented and
   * the caller will be responsible for releasing this new reference.
   */
  public abstract Object convertToNetty() throws IOException;
}
