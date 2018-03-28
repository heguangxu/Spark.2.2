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

package org.apache.spark.rpc

import org.apache.spark.SparkException

/**
 * A factory class to create the [[RpcEnv]]. It must have an empty constructor so that it can be
 * created using Reflection.
  *
  * 创建[[RpcEnv]]的工厂类。它必须有一个空的构造，以便可以使用反射创建它。
  *
  *  NettyRpcEnvFactory 继承 RpcEnvFactory
 */
private[spark] trait RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv
}

/**
 * An end point for the RPC that defines what functions to trigger given a message.
 *
 * It is guaranteed that `onStart`, `receive` and `onStop` will be called in sequence.
 *
 * The life-cycle of an endpoint is:
 *
 * {@code constructor -> onStart -> receive* -> onStop}
 *
 * Note: `receive` can be called concurrently. If you want `receive` to be thread-safe, please use
 * [[ThreadSafeRpcEndpoint]]
 *
 * If any error is thrown from one of [[RpcEndpoint]] methods except `onError`, `onError` will be
 * invoked with the cause. If `onError` throws an error, [[RpcEnv]] will ignore it.
  *
  *
  * 进程间调用的一个端点，当一个消息到来时，方法调用顺序为  onStart, receive, onStop
  * 它的生命周期为constructor -> onStart -> receive* -> onStop  .当然还有一些其他方法，都是间触发方法
  *
  * RpcEndpoint定义了由消息触发的一些函数，onStart，receive和onStop的调用时顺序发生的。生命周期是
  * constructor->onStart->receive*->onStop。receive能并发操作，如果想要receive是线程安全的
  * ，请使用ThreadSafeRpcEndpoint，如果RpcEndpoint抛出错误，它的onError方法将会触发。
  */
private[spark] trait RpcEndpoint {

  /**
   * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
   */
  val rpcEnv: RpcEnv

  /**
   * The [[RpcEndpointRef]] of this [[RpcEndpoint]]. `self` will become valid when `onStart` is
   * called. And `self` will become `null` when `onStop` is called.
    *
    * The [[RpcEndpointRef]] of this [[RpcEndpoint]]. `。
    * 当“onStart”被调用时，“self”将会变得有效。当“onStop”被调用时，“self”将变为“null”。
   *
   * Note: Because before `onStart`, [[RpcEndpoint]] has not yet been registered and there is not
   * valid [[RpcEndpointRef]] for it. So don't call `self` before `onStart` is called.
   */
  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  /**
   * Process messages from `RpcEndpointRef.send` or `RpcCallContext.reply`. If receiving a
   * unmatched message, `SparkException` will be thrown and sent to `onError`.
    * 从“RpcEndpointRef处理消息。发送”或“RpcCallContext.reply”。如果接收到未匹配的消息，
    * 将抛出“SparkException”并发送到“onError”。
    *
    * 处理RpcEndpointRef.send或RpcCallContext.reply方法，如果收到不匹配的消息，将抛出SparkException
    *
    * // 处理一个Ref调用send或者reply发送过过来的消息
   */
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new SparkException(self + " does not implement 'receive'")
  }

  /**
   * Process messages from `RpcEndpointRef.ask`. If receiving a unmatched message,
   * `SparkException` will be thrown and sent to `onError`.
    * 从“RpcEndpointRef.ask”处理消息。如果接收到未匹配的消息，将抛出“SparkException”并发送到“onError”。
    *
    * 处理RpcEndpointRef.ask方法，如果不匹配消息，将抛出SparkException
    *
    * // trait RpcEndpoint接收双向消息的偏函数
   */
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new SparkException(self + " won't reply anything"))
  }

  /**
   * Invoked when any exception is thrown during handling messages.
    *在处理消息期间抛出任何异常时调用。
    *
    * 当处理消息发生异常时
   */
  def onError(cause: Throwable): Unit = {
    // By default, throw e and let RpcEnv handle it
    throw cause
  }

  /**
   * Invoked when `remoteAddress` is connected to the current node.
    *
    * 当远程地址连接到当前的节点地址时触发
   */
  def onConnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when `remoteAddress` is lost.
    *
    * 当远程地址连接断开时触发
   */
  def onDisconnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when some network error happens in the connection between the current node and
   * `remoteAddress`.
    *
    * 当远程地址和当前节点的连接发生网络异常时触发
   */
  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked before [[RpcEndpoint]] starts to handle any message.
    * 开始处理任何消息之前调用[[RpcEndpoint]]。
    *
    * 启动RpcEndpoint处理任何消息
   */
  def onStart(): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when [[RpcEndpoint]] is stopping. `self` will be `null` in this method and you cannot
   * use it to send or ask messages.
    * 当[[RpcEndpoint]]停止时调用。“self”将在这个方法中为“null”，你不能用它来发送或询问消息。
    *
    * 停止RpcEndpoint
   */
  def onStop(): Unit = {
    // By default, do nothing.
  }

  /**
   * A convenient method to stop [[RpcEndpoint]].
   */
  final def stop(): Unit = {
    val _self = self
    if (_self != null) {
      rpcEnv.stop(_self)
    }
  }
}



/**
 * A trait that requires RpcEnv thread-safely sending messages to it.
 *
 * Thread-safety means processing of one message happens before processing of the next message by
 * the same [[ThreadSafeRpcEndpoint]]. In the other words, changes to internal fields of a
 * [[ThreadSafeRpcEndpoint]] are visible when processing the next message, and fields in the
 * [[ThreadSafeRpcEndpoint]] need not be volatile or equivalent.
 *
 * However, there is no guarantee that the same thread will be executing the same
 * [[ThreadSafeRpcEndpoint]] for different messages.
 */
private[spark] trait ThreadSafeRpcEndpoint extends RpcEndpoint
