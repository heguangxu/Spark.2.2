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

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;

/**
 * Handler for sendRPC() messages sent by {@link org.apache.spark.network.client.TransportClient}s.
 *
 * 处理由程序org.apache.spark.network.client.TransportClient客户端sendRPC()发送的消息
 */
public abstract class RpcHandler {

  // 这个是receive回调函数，仅仅打印失败或者成功的日志 在这个文件最下面的内部类
  private static final RpcResponseCallback ONE_WAY_CALLBACK = new OneWayRpcCallback();

  /**
   * Receive a single RPC message. Any exception thrown while in this method will be sent back to
   * the client in string form as a standard RPC failure.
   * 接收单个RPC消息。在此方法中抛出的任何异常将以标准RPC失败的形式以字符串形式发送回客户端。
   *
   * This method will not be called in parallel for a single TransportClient (i.e., channel).
   * 这个方法不会被一个TransportClient并行的调用， （例如。channel）
   *
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   *               一个通道客户端，它允许处理程序将请求返回给这个RPC的发送方。对于特定的通道，这始终是完全相同的对象。
   * @param message The serialized bytes of the RPC.
   * @param callback Callback which should be invoked exactly once upon success or failure of the
   *                 RPC.
   *                 回调应该在RPC的成功或失败时准确地调用。
   */
  public abstract void receive(
      TransportClient client,
      ByteBuffer message,
      RpcResponseCallback callback);

  /**
   * Returns the StreamManager which contains the state about which streams are currently being
   * fetched by a TransportClient.
   * 返回流管理器，该流管理器包含传输客户机当前正在获取的流的状态。
   */
  public abstract StreamManager getStreamManager();

  /**
   * Receives an RPC message that does not expect a reply. The default implementation will
   * call "{@link #receive(TransportClient, ByteBuffer, RpcResponseCallback)}" and log a warning if
   * any of the callback methods are called.
   *
   * 接收不期望回复的RPC消息。就是接收客户端信息，但是不回复客户端，
   * 默认实现将调用“receive(TransportClient, ByteBuffer, RpcResponseCallback)”，
   * 如果调用了任何回调方法，则记录一个警告。
   * ONE_WAY_CALLBACK是一个仅仅记录日志的回调函数
   *
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param message The serialized bytes of the RPC.
   */
  public void receive(TransportClient client, ByteBuffer message) {
    receive(client, message, ONE_WAY_CALLBACK);
  }

  /**
   * Invoked when the channel associated with the given client is active.
   * 当与给定客户端关联的通道处于活动状态时调用。
   */
  public void channelActive(TransportClient client) { }

  /**
   * Invoked when the channel associated with the given client is inactive.
   * No further requests will come from this client.
   *
   * 当与给定客户端关联的通道不活动时调用。不会再有来自这个客户端的请求。
   * 客户端连接断了，或者客户端死掉的时候
   */
  public void channelInactive(TransportClient client) { }

  // 捕获异常
  public void exceptionCaught(Throwable cause, TransportClient client) { }


  // 仅仅记录日志的回调函数
  private static class OneWayRpcCallback implements RpcResponseCallback {

    private static final Logger logger = LoggerFactory.getLogger(OneWayRpcCallback.class);

    @Override
    public void onSuccess(ByteBuffer response) {
      logger.warn("Response provided for one-way RPC.");
    }

    @Override
    public void onFailure(Throwable e) {
      logger.error("Error response provided for one-way RPC.", e);
    }

  }

}
