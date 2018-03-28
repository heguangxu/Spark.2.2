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

package org.apache.spark.network.client;

import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * Callback for the result of a single chunk result. For a single stream, the callbacks are
 * guaranteed to be called by the same thread in the same order as the requests for chunks were
 * made.
 *
 * Note that if a general stream failure occurs, all outstanding chunk requests may be failed.
 *
 * 为单个块结果的结果回调。对于单个流，在相同的线程中，回调被保证被相同的线程调用，就像对块的请求一样。
 * 注意，如果出现了一般的流故障，所有未处理的块请求都可能失败。
 */
public interface ChunkReceivedCallback {
  /**
   * Called upon receipt of a particular chunk. 在收到某一特定的块后调用。
   *
   * The given buffer will initially have a refcount of 1, but will be release()'d as soon as this
   * call returns. You must therefore either retain() the buffer or copy its contents before
   * returning.
   *
   * 给定的缓冲区最初将有一个refcount 1，但是回调函数返回将被释放()。因此，您必须保留()缓冲区或在
   * 返回之前复制其内容。
   */
  void onSuccess(int chunkIndex, ManagedBuffer buffer);

  /**
   * Called upon failure to fetch a particular chunk. Note that this may actually be called due
   * to failure to fetch a prior chunk in this stream.
   * 调用失败来获取特定的块。请注意，这实际上可能是由于未能在流中获取先前的块而被调用的。
   *
   * After receiving a failure, the stream may or may not be valid. The client should not assume
   * that the server's side of the stream has been closed.
   * 在接收失败后，流可能是有效的，也可能是无效的。客户端不应该假设服务器的服务器端已经关闭。
   */
  void onFailure(int chunkIndex, Throwable e);
}
