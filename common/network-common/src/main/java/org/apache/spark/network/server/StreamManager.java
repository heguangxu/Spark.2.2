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

package org.apache.spark.network.server;

import io.netty.channel.Channel;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;

/**
 * The StreamManager is used to fetch individual chunks from a stream. This is used in
 * {@link TransportRequestHandler} in order to respond to fetchChunk() requests. Creation of the
 * stream is outside the scope of the transport layer, but a given stream is guaranteed to be read
 * by only one client connection, meaning that getChunk() for a particular stream will be called
 * serially and that once the connection associated with the stream is closed, that stream will
 * never be used again.
 *
 * StreamManager用于从流中获取单个块。为了响应fetchChunk()请求，在TransportRequestHandler中使用了这个命令。
 * 创建流在传输层的范围之外,但是给定流保证阅读只有一个客户端连接,这意味着getChunk()为特定流将被称为连续,
 * 一旦关闭连接的流,流将永远不会被再次使用。
 */
public abstract class StreamManager {
  /**
   * Called in response to a fetchChunk() request. The returned buffer will be passed as-is to the
   * client. A single stream will be associated with a single TCP connection, so this method
   * will not be called in parallel for a particular stream.
   * 调用以响应fetchChunk()请求。返回的缓冲区将按原样传递给客户端。单个流将与单个TCP连接相关联，
   * 因此这种方法不会在特定的流中并行调用。
   *
   * Chunks may be requested in any order, and requests may be repeated, but it is not required
   * that implementations support this behavior.
   * 可以按任何顺序请求块，请求可以重复，但不需要实现支持这种行为。
   *
   * The returned ManagedBuffer will be release()'d after being written to the network.
   *
   * 返回的ManagedBuffer将被释放()在被写进网络之后。
   * @param streamId id of a stream that has been previously registered with the StreamManager.
   * @param chunkIndex 0-indexed chunk of the stream that's requested
   */
  public abstract ManagedBuffer getChunk(long streamId, int chunkIndex);

  /**
   * Called in response to a stream() request. The returned data is streamed to the client
   * through a single TCP connection.
   *
   * Note the <code>streamId</code> argument is not related to the similarly named argument in the
   * {@link #getChunk(long, int)} method.
   *
   * 响应流()请求调用。返回的数据通过一个TCP连接流到客户端。
   * 请注意<代码> streamId参数与getChunk(long,int)方法中的类似命名参数无关。
   *
   * @param streamId id of a stream that has been previously registered with the StreamManager.
   * @return A managed buffer for the stream, or null if the stream was not found.
   */
  public ManagedBuffer openStream(String streamId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Associates a stream with a single client connection, which is guaranteed to be the only reader
   * of the stream. The getChunk() method will be called serially on this connection and once the
   * connection is closed, the stream will never be used again, enabling cleanup.
   *
   * This must be called before the first getChunk() on the stream, but it may be invoked multiple
   * times with the same channel and stream id.
   *
   * 将一个流与一个客户机连接关联起来，这将保证是流的惟一读取器。getChunk()方法将在此连接上连续被调用，
   * 一旦连接被关闭，流将不再被使用，从而可以进行清理。
   * 这必须在流上的第一个getChunk()之前调用，但是它可能会被同一个通道和流id多次调用。
   */
  public void registerChannel(Channel channel, long streamId) { }

  /**
   * Indicates that the given channel has been terminated. After this occurs, we are guaranteed not
   * to read from the associated streams again, so any state can be cleaned up.
   *
   * 指示给定的通道已被终止。在发生此事件之后，我们保证不再从关联的流中读取数据，因此任何状态都可以被清除。
   */
  public void connectionTerminated(Channel channel) { }

  /**
   * Verify that the client is authorized to read from the given stream.
   * 验证客户机是否被授权从给定的流中读取数据。
   *
   * @throws SecurityException If client is not authorized.
   */
  public void checkAuthorization(TransportClient client, long streamId) { }

}
