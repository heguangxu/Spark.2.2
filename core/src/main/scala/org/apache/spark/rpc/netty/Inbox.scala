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

import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, ThreadSafeRpcEndpoint}


private[netty] sealed trait InboxMessage

private[netty] case class OneWayMessage(
    senderAddress: RpcAddress,
    content: Any) extends InboxMessage

private[netty] case class RpcMessage(
    senderAddress: RpcAddress,
    content: Any,
    context: NettyRpcCallContext) extends InboxMessage

private[netty] case object OnStart extends InboxMessage

private[netty] case object OnStop extends InboxMessage

/** A message to tell all endpoints that a remote process has connected. 一个消息告诉所有端点一个远程进程已经连接 */
private[netty] case class RemoteProcessConnected(remoteAddress: RpcAddress) extends InboxMessage

/** A message to tell all endpoints that a remote process has disconnected. */
private[netty] case class RemoteProcessDisconnected(remoteAddress: RpcAddress) extends InboxMessage

/** A message to tell all endpoints that a network error has happened. */
private[netty] case class RemoteProcessConnectionError(cause: Throwable, remoteAddress: RpcAddress)
  extends InboxMessage

/**
 * An inbox that stores messages for an [[RpcEndpoint]] and posts messages to it thread-safely.
  *
  * Inbox:收件箱
 */
private[netty] class Inbox(
    val endpointRef: NettyRpcEndpointRef,
    val endpoint: RpcEndpoint)
  extends Logging {

  inbox =>  // Give this an alias so we can use it more clearly in closures.

  @GuardedBy("this")
  protected val messages = new java.util.LinkedList[InboxMessage]()

  /** True if the inbox (and its associated endpoint) is stopped. */
  @GuardedBy("this")
  private var stopped = false

  /** Allow multiple threads to process messages at the same time.
    * 允许多个线程同时处理消息。
    * */
  @GuardedBy("this")
  private var enableConcurrent = false

  /** The number of threads processing messages for this inbox.
    * 处理收件箱的线程数
    * */
  @GuardedBy("this")
  private var numActiveThreads = 0

  // OnStart should be the first message to process
  // 放置了空的EndpointData对象，那么是怎么从该对象的inbox里取数据处理的呢，原来是初始化EndpointData的inbox时，
  // 自动放置了一个OnStart消息，所以空的Endpoint对象对应于OnStart。
  inbox.synchronized {
    messages.add(OnStart)
  }

  /**
   * Process stored messages.
    *
    * 因为process里面涉及到一些组件真正调用单向和双向消息的具体实现,模式匹配+偏函数经典搭配；
    *  包括还有远程消息体的处理方式
   */
  def process(dispatcher: Dispatcher): Unit = {
    var message: InboxMessage = null
    // 这里是增加numActiveThreads 下面有减少的 应该是为了保证一个消息 ，只能发送一次（比如增加executor的消息，本来发送消息只是增加一个，
    // 但是不这样做，有可能这个消息，因为某些原因重试了好几次，那么就增加了很多的executor，造成浪费）
    inbox.synchronized {
      // 这个（真 && 假） 结果为false
      if (!enableConcurrent && numActiveThreads != 0) {
        return
      }
      // 先从messages链表里poll出一条消息数据
      message = messages.poll()
      // 这个numActiveThreads只要来的消息不为null就设置为1，但是返回后，又变成0了 在1和0之间转换
      if (message != null) {
        numActiveThreads += 1
      } else {
        return
      }
    }
    // message:"RpcMessage(192.168.2.89:59605,ExpireDeadHost,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@726c6a36)"
    // message:"RpcMessage(192.168.2.89:59605,CheckExistence(HeartbeatReceiver),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@288ca5f0)"
    // message:"RpcMessage(192.168.2.89:59605,RegisterBlockManager(BlockManagerId(driver,192.168.2.89,59296,none),915406848,NettyRpcEndpointRef(Spark://BlockManagerEndpoint1@192.168.2.89:59065)),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@48d293ee)"
    // message:"RpcMessage(192.168.2.89:59605,Heartbeat(driver[Lscala,Tuple2:@33a2cec3,nonullne),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@desg4543ew)"
    // message:"RpcMessage(192.168.2.89:59605,ExecutorRegistered(driver),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@48d293ee)"
    // message:"RpcMessage(192.168.2.89:59605,BlockManagerHeartbeat(driver(driver,192.168.2.89,59296,none),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@48d293ee)"
    // message:"RpcMessage(192.168.2.89:59605,UpdateBlockInfo(BlockManagerId(driver,192.168.2.89,59296,none),broadcast_0_piece0,StorageLevel(memory,1 replicas),29064,0),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@desg4543ew)"
    // message:"OneWayMessage(192.168.2.89:59605,StatusUpdate(1,RUNNING,java.nio,HeartByteBuffer[pos=0,lim=0,cap=0]))"
    // message:"RpcMessage(192.168.2.89:59605,StopMapOutTracker,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@desg4543ew)"
    // message:"OneWayMessage(192.168.2.89:59605,StopCoordinator)"
    while (true) {
      // 循环调用RpcEndpoint
      safelyCall(endpoint) {
        //  对poll出来的message做模式匹配，调用对应的处理机制
        message match {
          // message:"RpcMessage(192.168.2.89:59605,ExpireDeadHost,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@726c6a36)"
          // message:"RpcMessage(192.168.2.89:59605,CheckExistence(HeartbeatReceiver),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@288ca5f0)"
          // message:"RpcMessage(192.168.2.89:59605,RegisterBlockManager(BlockManagerId(driver,192.168.2.89,59296,none),915406848,NettyRpcEndpointRef(Spark://BlockManagerEndpoint1@192.168.2.89:59065)),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@48d293ee)"
          // message:"RpcMessage(192.168.2.89:59605,Heartbeat(driver[Lscala,Tuple2:@33a2cec3,nonullne),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@desg4543ew)"
          // message:"RpcMessage(192.168.2.89:59605,ExecutorRegistered(driver),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@48d293ee)"
          // message:"RpcMessage(192.168.2.89:59605,BlockManagerHeartbeat(driver(driver,192.168.2.89,59296,none),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@48d293ee)"
          // message:"RpcMessage(192.168.2.89:59605,UpdateBlockInfo(BlockManagerId(driver,192.168.2.89,59296,none),broadcast_0_piece0,StorageLevel(memory,1 replicas),29064,0),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@desg4543ew)"
          // message:"RpcMessage(192.168.2.89:59605,StopMapOutTracker,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@desg4543ew)"
          // 匹配到一条普通的Rpc消息
          // 这里说一下，一下所有匹配的消息类型都实现了trait InboxMessage，包括这条RpcMessage
          case RpcMessage(_sender, content, context) =>
            try {
              // 这个方法是接收并返回的双向消息体，是通过sender调用对应的Ref的ask方法触发的
              // 包括在下个章节会提及的blockmanager中的BlockManagerSlaveEndpoint组件在执行
              // RemoveBlock，GetBlockStatus等操作时都是调用receiveAndReply
              // 这里补充一下：receiveAndReply是一个PartialFunction（偏函数），当endpoint调用
              // receiveAndReply时会根据case 到的类型执行对应的操作
              // 如果是Heartbeat==》org.apache.spark.Heartbeat.receiveAndReply()
              // 如果是BlockManagerHeartbeat==》org.apache.spark.storage.BlockManagerMasterEndpoint.receiveAndReply()
              endpoint.receiveAndReply(context).applyOrElse[Any, Unit](content, { msg =>
                throw new SparkException(s"Unsupported message $message from ${_sender}")
              })
            } catch {
              case NonFatal(e) =>
                context.sendFailure(e)
                // Throw the exception -- this exception will be caught by the safelyCall function.
                // The endpoint's onError function will be called.
                throw e
            }

          // message:"OneWayMessage(192.168.2.89:59605,StatusUpdate(1,RUNNING,java.nio,HeartByteBuffer[pos=0,lim=0,cap=0]))"
          // 匹配一个单向的消息处理机制
          case OneWayMessage(_sender, content) =>
            // 这就是刚刚说到的单向消息体的具体实现
            // 调用偏函数receive处理一个Ref调用send或者reply发送过过来的消息
            endpoint.receive.applyOrElse[Any, Unit](content, { msg =>
              throw new SparkException(s"Unsupported message $message from ${_sender}")
            })

          // 匹配一个开启endpoint接收消息的方法
          case OnStart =>
            //  在endpoint接收任何消息之前调用，启动它的接收消息功能
            endpoint.onStart()
            //  如果它的实例不是ThreadSafeRpcEndpoint类型就强制关闭
            if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
              inbox.synchronized {
                if (!stopped) {
                  enableConcurrent = true
                }
              }
            }

          // 匹配一个停止endpoint接收消息的方法，当匹配到这个方法后，它的send和ask都不能用了
          case OnStop =>
            val activeThreads = inbox.synchronized { inbox.numActiveThreads }
            // 做个断言
            assert(activeThreads == 1,
              s"There should be only a single active thread but found $activeThreads threads.")
            // 移除掉RpcEndpointRef
            dispatcher.removeRpcEndpointRef(endpoint)
            //  停止接收消息
            endpoint.onStop()
            // 断言是否为空
            assert(isEmpty, "OnStop should be the last message")

          // 匹配到一条告诉所有节点的消息，一个远程进程已连接
          case RemoteProcessConnected(remoteAddress) =>
            endpoint.onConnected(remoteAddress)

          // 匹配到一条告诉所有节点的消息，一个远程进程已断开连接
          case RemoteProcessDisconnected(remoteAddress) =>
            endpoint.onDisconnected(remoteAddress)

          // 匹配到一条告诉所有节点的消息，一个远程进程连接发生错误状态
          case RemoteProcessConnectionError(cause, remoteAddress) =>
            endpoint.onNetworkError(cause, remoteAddress)
        }
      }

      // 这里减少numActiveThreads
      inbox.synchronized {
        // "enableConcurrent" will be set to false after `onStop` is called, so we should check it
        // every time.
        if (!enableConcurrent && numActiveThreads != 1) {
          // If we are not the only one worker, exit
          numActiveThreads -= 1
          return
        }
        message = messages.poll()
        if (message == null) {
          numActiveThreads -= 1
          return
        }
      }
    }
  }

  def post(message: InboxMessage): Unit = inbox.synchronized {
    // inbox停止了
    if (stopped) {
      // We already put "OnStop" into "messages", so we should drop further messages
      // 我们已经将“OnStop”放入“消息”中，因此我们应该删除更多的消息
      // 这里仅仅是报错warn
      onDrop(message)
    } else {
      // 把massage加入到java.util.LinkedList[InboxMessage]消息队列链表中
      // messsage:"RpcMessage(192.168.2.89:49901)"
      messages.add(message)
      false
    }
  }

  def stop(): Unit = inbox.synchronized {
    // The following codes should be in `synchronized` so that we can make sure "OnStop" is the last
    // message
    // 下面的代码应该是“同步”的，这样我们就可以确保“OnStop”是最后一条消息
    if (!stopped) {
      // We should disable concurrent here. Then when RpcEndpoint.onStop is called, it's the only
      // thread that is processing messages. So `RpcEndpoint.onStop` can release its resources
      // safely.
      // 我们应该在这里禁用并发。当RpcEndpoint。onStop被调用，它是唯一处理消息的线程。所以“RpcEndpoint。onStop可以安全地释放它的资源。
      enableConcurrent = false
      stopped = true
      messages.add(OnStop)
      // Note: The concurrent events in messages will be processed one by one.
    }
  }

  def isEmpty: Boolean = inbox.synchronized { messages.isEmpty }

  /**
   * Called when we are dropping a message. Test cases override this to test message dropping.
   * Exposed for testing.
    * 当我们删除一个消息的时候调用。测试用例覆盖了这一点以测试消息的下降。
    * 暴露进行测试。
   */
  protected def onDrop(message: InboxMessage): Unit = {
    logWarning(s"Drop $message because $endpointRef is stopped")
  }

  /**
   * Calls action closure, and calls the endpoint's onError function in the case of exceptions.
    * 调用操作闭包，并在异常情况下调用端点的onError函数。
   */
  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    try action catch {
      case NonFatal(e) =>
        try endpoint.onError(e) catch {
          case NonFatal(ee) => logError(s"Ignoring error", ee)
        }
    }
  }

}
