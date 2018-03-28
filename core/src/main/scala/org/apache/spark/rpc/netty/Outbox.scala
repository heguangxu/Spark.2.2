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

package org.apache.spark.rpc.netty

import java.nio.ByteBuffer
import java.util.concurrent.Callable
import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.rpc.{RpcAddress, RpcEnvStoppedException}

private[netty] sealed trait OutboxMessage {

  def sendWith(client: TransportClient): Unit

  def onFailure(e: Throwable): Unit

}

// 单向消息体
private[netty] case class OneWayOutboxMessage(content: ByteBuffer) extends OutboxMessage
  with Logging {

  override def sendWith(client: TransportClient): Unit = {
    // 通过TransportClient发送消息
    // 底层则会调用 Netty的io.netty.channel.writeAndFlush
    // ==> TransportClient.send()
    client.send(content)
  }

  override def onFailure(e: Throwable): Unit = {
    e match {
      case e1: RpcEnvStoppedException => logWarning(e1.getMessage)
      case e1: Throwable => logWarning(s"Failed to send one-way RPC.", e1)
    }
  }

}

// 下面是双向消息体的实现 底层一样调用的是io.netty.channel.writeAndFlush，只是多个回调函数
private[netty] case class RpcOutboxMessage(
    content: ByteBuffer,
    _onFailure: (Throwable) => Unit,
    _onSuccess: (TransportClient, ByteBuffer) => Unit)
  extends OutboxMessage with RpcResponseCallback with Logging {

  private var client: TransportClient = _
  private var requestId: Long = _

  override def sendWith(client: TransportClient): Unit = {
    this.client = client
    // 底层也是调用Netty的io.netty.channel.writeAndFlush
    // 只是多了一个接收server端消息响应的回调函数RpcResponseCallback
    this.requestId = client.sendRpc(content, this)
  }

  def onTimeout(): Unit = {
    if (client != null) {
      client.removeRpcRequest(requestId)
    } else {
      logError("Ask timeout before connecting successfully")
    }
  }

  override def onFailure(e: Throwable): Unit = {
    _onFailure(e)
  }

  override def onSuccess(response: ByteBuffer): Unit = {
    _onSuccess(client, response)
  }

}

// Outbox:发件箱
private[netty] class Outbox(nettyEnv: NettyRpcEnv, val address: RpcAddress) {

  outbox => // Give this an alias so we can use it more clearly in closures.

  @GuardedBy("this")
  private val messages = new java.util.LinkedList[OutboxMessage]

  @GuardedBy("this")
  private var client: TransportClient = null

  /**
   * connectFuture points to the connect task. If there is no connect task, connectFuture will be
   * null.
   */
  @GuardedBy("this")
  private var connectFuture: java.util.concurrent.Future[Unit] = null

  @GuardedBy("this")
  private var stopped = false

  /**
   * If there is any thread draining the message queue
   */
  @GuardedBy("this")
  private var draining = false

  /**
   * Send a message. If there is no active connection, cache it and launch a new connection. If
   * [[Outbox]] is stopped, the sender will be notified with a [[SparkException]].
    *
    * 接下来是发现接收端没有启动TransportClient
   */
  def send(message: OutboxMessage): Unit = {
    //  检查状态
    val dropped = synchronized {
      // 判断Outbox是否关闭
      if (stopped) {
        true
      } else {
        // 如果Outbox启动了 则把message添加到自己的消息队列里
        // 加入 java.util.LinkedList[OutboxMessage] 的队列中，等待被线程消费
        messages.add(message)
        false
      }
    }
    if (dropped) {
      // 如果Outbox停止状态
      message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
    } else {
      // Outbox启动状态则调用drainOutbox处理消息
      drainOutbox()
    }
  }

  /**
   * Drain the message queue. If there is other draining thread, just exit. If the connection has
   * not been established, launch a task in the `nettyEnv.clientConnectionExecutor` to setup the
   * connection.
    *
    * 根据情况 最终还是可能会调用drainOutbox()，里面会再次判断接收端是否启动了TransportClient
    * 如果没有就去调用nettyEnv去执行远程创建TransportClient，然后会在同步锁里无限循环根据
    * 不同类型的message调用的sendWith实现
    *
    * //  outbox消费message队列
   */
  private def drainOutbox(): Unit = {
    var message: OutboxMessage = null
    // 附加同步锁
    synchronized {
      if (stopped) {
        // outbox停止状态直接返回
        return
      }
      if (connectFuture != null) {
        // We are connecting to the remote address, so just exit
        return
      }
      // 判断TransportClient对象是否为空
      if (client == null) {
        // There is no connect task but client is null, so we need to launch the connect task.
        // 如果TransportClient为空就会建立连接，然后再调用drainOutbox方法
        launchConnectTask()
        return
      }
      // 判断下琐里有没有其他线程在使用
      if (draining) {
        // There is some thread draining, so just exit
        return
      }
      // 没有的话就会从messages链表里poll移出第一个消息体
      message = messages.poll()
      if (message == null) {
        // 如果消息队列为空直接返回
        return
      }
      // 相当于强制锁，其他线程如果走到这里就说明能执行下面的while循环
      // 而上线的draining判断则会让其他过来的线程强制return
      // 在循环的最后消息处理完毕后就会把draining赋值为false，这样其他线程又能来使用了
      draining = true
    }
    while (true) {
      try {
        // 同步拿到client
        val _client = synchronized { client }
        if (_client != null) {
          // 调用sendWith核心方法
          // 提醒一下：这里所有的outbox里提取出的消息体都是实现了trait OutboxMessage
          // 所以不同类型的message调用的sendWith实现也不同
          // 也是分为单向和双向消息体
          message.sendWith(_client)
        } else {
          // 断言判断outbox
          assert(stopped == true)
        }
      } catch {
        case NonFatal(e) =>
          handleNetworkFailure(e)
          return
      }
      synchronized {
        if (stopped) {
          return
        }
        // 这个线程会在这个while无限循环中不停的poll出消息体并执行上面的动作
        // 直到消息队列里没有消息后会把draining赋值会false，以便下个线程使用
        message = messages.poll()
        if (message == null) {
          draining = false
          return
        }
      }
    }
  }

  private def launchConnectTask(): Unit = {
    // 调用ThreadUtils.newDaemonCachedThreadPool底层会创建一个java的ThreadPoolExecutor线程池
    connectFuture = nettyEnv.clientConnectionExecutor.submit(new Callable[Unit] {

      override def call(): Unit = {
        try {
          // 根据Rpc地址在接收者端启动一个TransportClient
          val _client = nettyEnv.createClient(address)
          // outbox给上同步锁
          outbox.synchronized {
            client = _client
            if (stopped) {
              // 如果是stop状态就关闭，会把client设置成null，但连接还在，方便重连
              closeClient()
            }
          }
        } catch {
          case ie: InterruptedException =>
            // exit
            return
          case NonFatal(e) =>
            outbox.synchronized { connectFuture = null }
            handleNetworkFailure(e)
            return
        }
        outbox.synchronized { connectFuture = null }
        // It's possible that no thread is draining now. If we don't drain here, we cannot send the
        // messages until the next message arrives.
        // 最后调用drainOutbox
        drainOutbox()
      }
    })
  }

  /**
   * Stop [[Inbox]] and notify the waiting messages with the cause.
   */
  private def handleNetworkFailure(e: Throwable): Unit = {
    synchronized {
      assert(connectFuture == null)
      if (stopped) {
        return
      }
      stopped = true
      closeClient()
    }
    // Remove this Outbox from nettyEnv so that the further messages will create a new Outbox along
    // with a new connection
    nettyEnv.removeOutbox(address)

    // Notify the connection failure for the remaining messages
    //
    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(e)
      message = messages.poll()
    }
    assert(messages.isEmpty)
  }

  private def closeClient(): Unit = synchronized {
    // Just set client to null. Don't close it in order to reuse the connection.
    client = null
  }

  /**
   * Stop [[Outbox]]. The remaining messages in the [[Outbox]] will be notified with a
   * [[SparkException]].
   */
  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
      if (connectFuture != null) {
        connectFuture.cancel(true)
      }
      closeClient()
    }

    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
      message = messages.poll()
    }
  }
}
