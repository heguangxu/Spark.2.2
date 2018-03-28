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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.concurrent.Promise
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.rpc._
import org.apache.spark.util.ThreadUtils

/**
 * A message dispatcher, responsible for routing RPC messages to the appropriate endpoint(s).
  *
  * Dispatcher类是一个消息分发器，负责将RPC消息发送到适当的端点。该类有一个内部类
  * Dispatcher作为一个分发器，内部存放了Inbox，Outbox的等相关句柄和存放了相关处理状态数据，结构大致如下
  *
  * 相当于路由器的功能,不仅仅能转发消息，还能记住两边的电脑ip
 */
private[netty] class Dispatcher(nettyEnv: NettyRpcEnv) extends Logging {

  // http://blog.csdn.net/lzy2014/article/details/71374478
  // 而Dispatcher内部有个核心部件是EndpointData，而它里面封装了用来存储接和处理本地消息的Inbox对象
  // （处理远程消息的交由在NettyRpcEnv生成的Outbox处理）
  //
  // 简单来说就是 接收到的messages都会封装成EndpointData 然后加入到receivers里，最后交由线程池消费
  // EndpointData里面封装了自己的信息和对应的Inbox
  private class EndpointData(
      val name: String,
      val endpoint: RpcEndpoint,
      val ref: NettyRpcEndpointRef) {
    // 用来存储本地messages和根据消息内容执行相关的操作，这里新创建一个Inbox收件箱，那么肯定会执行里面的代码块
    // inbox.synchronized {
    //   messages.add(OnStart)
    // }
    // 这里添加了第一个消息 收件箱
    val inbox = new Inbox(ref, endpoint)
  }

  // 数据存储结构：java.util.concurrent下的ConcurrentHashMap
  // key是endpoint名字，value是EndpointData(endpoint)
  // 0 BlockManagerEndpoint==>Dispatcher$EndpointData@
  // 1 endpoint-verifier==>Dispatcher$EndpointData@
  // 2 OutputCommitCoordinator==>Dispatcher$EndpointData@
  // 3 BlockManagerMaster==>Dispatcher$EndpointData@
  // 4 MapOutputTracker==>Dispatcher$EndpointData@
  private val endpoints: ConcurrentMap[String, EndpointData] =
    new ConcurrentHashMap[String, EndpointData]

  // 数据存储结构：java.util.concurrent下的ConcurrentHashMap
  // RpcEndpoint和RpcEndpointRef的对应关系
  // 1 OutputCommitCoordinator$OutputCommitCoordinatorEndpoint==>NettyRpcEndpointRef(spark://OutputCoordinator@192.168.2.89:60703)
  // 2 RpcEndpointVerfier@wekhr23==>NettyRpcEndpointRef(spark://endpoint-verifier@192.168.2.89:60703)
  // 3 MapOutputTrackerMasterEndpoint@28374stf7==>NettyRpcEndpointRef(spark://MapOutputTracker@192.168.2.89:60703)
  private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =
    new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]

  // Track the receivers whose inboxes may contain messages.
  // 跟踪收件箱中可能包含消息的接收方。
  // 数据存储结构：java.util.concurrent下的LinkedBlockingQueue
  // 里面维护着EndpointData的线程阻塞链表
  // receivers貌似始终为0，里面一放入数据，就会被MessageLoop取走
  private val receivers = new LinkedBlockingQueue[EndpointData]

  /**
   * True if the dispatcher has been stopped. Once stopped, all messages posted will be bounced
   * immediately.
    *
    * 如果dispatcher已停止，则为真。一旦停止，所有发布的消息将立即被反弹。
   */
  @GuardedBy("this")
  private var stopped = false

  /**
    * 注册自己到NettyRpcEnv上并发回自己的Ref的实现
    */
  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
    // 拿到nettyEnv地址
    // address格式:"spark://endpoint-verifier@192.168.2.89:49233" name:"endpoint-verifier"
    // 这一点nettyEnv.address是懒加载
    val addr = RpcEndpointAddress(nettyEnv.address, name)
    // 创建NettyRpcEndpointRef，集成与顶级超类RpcEndpointRef
    val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
    synchronized {
      // 如果dispatcher已停止 ,消息路由器，死掉了，肯定发不出消息了
      if (stopped) {
        throw new IllegalStateException("RpcEnv has been stopped")
      }
      // 因为是参数，先创建一个EndpointData，主要创建了收件箱Inbox，然后添加了第一条收件箱消息messages.add(OnStart)
      // putIfAbsent:如果指定的键未与某个值关联，则将其与给定值关联。相当于
      // if (!map.containsKey(key))
      //    return map.put(key, value);
      //  else
      //    return map.get(key);
      if (endpoints.putIfAbsent(name, new EndpointData(name, endpoint, endpointRef)) != null) {
        throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
      }
      // 根据endpoint的名字提取到对应的EndpointData
      val data = endpoints.get(name)
      // 放入endpoint和对应的ref，缓存RpcEndpoint关系
      endpointRefs.put(data.endpoint, data.ref)
      // 最后把EndpointData假如到receivers，调用offer塞入数据到尾部的时候，不会因为队列已满而报错或者阻塞，
      // 而是直接返回false（put会阻塞，add会报错）
      receivers.offer(data)  // for the OnStart message
    }
    // 返回endpointRef
    endpointRef
  }

  def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointRefs.get(endpoint)

  def removeRpcEndpointRef(endpoint: RpcEndpoint): Unit = endpointRefs.remove(endpoint)

  // Should be idempotent
  private def unregisterRpcEndpoint(name: String): Unit = {
    // 从ConcurrentMap中移除endpoint
    val data = endpoints.remove(name)
    if (data != null) {
      data.inbox.stop()
      receivers.offer(data)  // for the OnStop message
    }
    // Don't clean `endpointRefs` here because it's possible that some messages are being processed
    // now and they can use `getRpcEndpointRef`. So `endpointRefs` will be cleaned in Inbox via
    // `removeRpcEndpointRef`.
  }

  def stop(rpcEndpointRef: RpcEndpointRef): Unit = {
    synchronized {
      if (stopped) {
        // This endpoint will be stopped by Dispatcher.stop() method.
        return
      }
      unregisterRpcEndpoint(rpcEndpointRef.name)
    }
  }

  /**
   * Send a message to all registered [[RpcEndpoint]]s in this process.
    * 发送信息到所有已经注册的RpcEndpoint
   *
   * This can be used to make network events known to all end points (e.g. "a new node connected").
    * 这可以用于将已知的所有端点的网络事件(例如:“一个新节点连接”)。
   */
  def postToAll(message: InboxMessage): Unit = {
    // 获取所有endpoint的名称
    val iter = endpoints.keySet().iterator()
    // 然后遍历所有名称，一个一个的发送消息
    while (iter.hasNext) {
      val name = iter.next
      // 将消息发送到特定的endpoint。这里指定了名称
      postMessage(name, message, (e) => logWarning(s"Message $message dropped. ${e.getMessage}"))
    }
  }

  /** Posts a message sent by a remote endpoint.  发送一个信息给远程的endpoint */
  def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback): Unit = {
    // RpcCallContext将会调用RpcResponseCallback，发送一个响应信息
    val rpcCallContext =
      new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
    // 创建一个InboxMessage类型的RpcMessage
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    // 将消息发送到特定的endpoint。
    postMessage(message.receiver.name, rpcMessage, (e) => callback.onFailure(e))
  }

  /** Posts a message sent by a local endpoint. 发布由本地端点发送的消息。 */
  def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
    val rpcCallContext =
      new LocalNettyRpcCallContext(message.senderAddress, p)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
  }

  /** Posts a one-way message. 单向发送消息*/
  def postOneWayMessage(message: RequestMessage): Unit = {
    postMessage(message.receiver.name, OneWayMessage(message.senderAddress, message.content),
      (e) => throw e)
  }

  /**
   * Posts a message to a specific endpoint.
    * 将消息发送到特定的endpoint。
   *
   * @param endpointName name of the endpoint.
   * @param message the message to post
   * @param callbackIfStopped callback function if the endpoint is stopped.
   */
  private def postMessage(
      endpointName: String,
      message: InboxMessage,
      callbackIfStopped: (Exception) => Unit): Unit = {
    val error = synchronized {
      // 拿到对应的EndpointData,从收件箱拿出第一条消息onStart
      val data = endpoints.get(endpointName)
      // 判断dispatcher是否停止了，如果停止了 信息肯定发不出去了
      if (stopped) {
        // 抛出异常
        Some(new RpcEnvStoppedException())
      } else if (data == null) {
        // 如果data为空，那就是endpoint没有注册
        Some(new SparkException(s"Could not find $endpointName."))
      } else {
        // 调用inbox对象把massage加入到java.util.LinkedList[InboxMessage]消息队列链表中 放到自己的收件箱里
        // message:"RpcMessage(192.168.2.89:59605,ExpireDeadHost,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@23235wde)"
        // message:"RpcMessage(192.168.2.89:59605,TaskSchedulerIsSet,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@23235wde)"
        // message:"RpcMessage(192.168.2.89:59605,ExpireDeadHost,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@63a4a9fa)"
        // message:"RpcMessage(192.168.2.89:59605,ExpireDeadHost,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@19cdccll)"
        // message:"RpcMessage(192.168.2.89:59605,ExpireDeadHost,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@726c6a36)"
        // ..........
        // message:"RpcMessage(192.168.2.89:59605,CheckExistence(HeartbeatReceiver),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@288ca5f0)"
        // message:"RpcMessage(192.168.2.89:59605,RegisterBlockManager(BlockManagerId(driver,192.168.2.89,59296,none),915406848,NettyRpcEndpointRef(Spark://BlockManagerEndpoint1@192.168.2.89:59065)),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@48d293ee)"
        // message:"RpcMessage(192.168.2.89:59605,Heartbeat(driver[Lscala,Tuple2:@33a2cec3,nonullne),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@desg4543ew)"
        // message:"RpcMessage(192.168.2.89:59605,ExpireDeadHost,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@726c6a36)"
        // message:"RpcMessage(192.168.2.89:59605,ExpireDeadHost,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@726c6a36)"
        // message:"RpcMessage(192.168.2.89:59605,ExpireDeadHost,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@726c6a36)"
        // message:"RpcMessage(192.168.2.89:59605,ExpireDeadHost,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@726c6a36)"
        // message:"RpcMessage(192.168.2.89:59605,RegisterBlockManager(BlockManagerId(driver,192.168.2.89,59296,none),915406848,NettyRpcEndpointRef(Spark://BlockManagerEndpoint1@192.168.2.89:59065)),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@48d293ee)"
        // message:"RpcMessage(192.168.2.89:59605,ExecutorRegistered(driver),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@48d293ee)"
        // message:"RpcMessage(192.168.2.89:59605,ExpireDeadHost,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@726c6a36)"
        // message:"RpcMessage(192.168.2.89:59605,Heartbeat(driver[Lscala,Tuple2:@33a2cec3,nonullne),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@desg4543ew)"
        // message:"RpcMessage(192.168.2.89:59605,ExpireDeadHost,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@726c6a36)"
        // message:"RpcMessage(192.168.2.89:59605,BlockManagerHeartbeat(driver(driver,192.168.2.89,59296,none),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@48d293ee)"
        // message:"RpcMessage(192.168.2.89:59605,Heartbeat(driver[Lscala,Tuple2:@33a2cec3,nonullne),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@desg4543ew)"
        // message:"RpcMessage(192.168.2.89:59605,ExpireDeadHost,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@726c6a36)"
        // message:"RpcMessage(192.168.2.89:59605,BlockManagerHeartbeat(driver(driver,192.168.2.89,59296,none),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@48d293ee)"
        // message:"RpcMessage(192.168.2.89:59605,Heartbeat(driver[Lscala,Tuple2:@33a2cec3,nonullne),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@desg4543ew)"
        // message:"RpcMessage(192.168.2.89:59605,ExpireDeadHost,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@726c6a36)"
        // message:"RpcMessage(192.168.2.89:59605,BlockManagerHeartbeat(driver(driver,192.168.2.89,59296,none),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@48d293ee)"
        // message:"RpcMessage(192.168.2.89:59605,Heartbeat(driver[Lscala,Tuple2:@33a2cec3,nonullne),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@desg4543ew)"
        // ....
        // message:"RpcMessage(192.168.2.89:59605,UpdateBlockInfo(BlockManagerId(driver,192.168.2.89,59296,none),broadcast_0_piece0,StorageLevel(memory,1 replicas),29064,0),org.apache.spark.rpc.netty.LocalNettyRpcCallContext@desg4543ew)"
        // message:"OneWayMessage(192.168.2.89:59605,StatusUpdate(1,RUNNING,java.nio,HeartByteBuffer[pos=0,lim=0,cap=0]))"
        // message:"RpcMessage(192.168.2.89:59605,StopMapOutTracker,org.apache.spark.rpc.netty.LocalNettyRpcCallContext@desg4543ew)"
        // message:"OneWayMessage(192.168.2.89:59605,StopCoordinator)"
        data.inbox.post(message)
        // 把EndpointData加入到receivers链表中等待被消费
        receivers.offer(data)
        None
      }
    }
    // We don't need to call `onStop` in the `synchronized` block
    error.foreach(callbackIfStopped)
  }

  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
    }
    // Stop all endpoints. This will queue all endpoints for processing by the message loops.
    // 停止所有的endpoints
    endpoints.keySet().asScala.foreach(unregisterRpcEndpoint)
    // Enqueue a message that tells the message loops to stop.
    receivers.offer(PoisonPill)
    threadpool.shutdown()
  }

  def awaitTermination(): Unit = {
    threadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  /**
   * Return if the endpoint exists 判断endpoint是否存在
   */
  def verify(name: String): Boolean = {
    endpoints.containsKey(name)
  }

  /** Thread pool used for dispatching messages.
    *
    * //  dispatcher的线程池
    *   dispatcher会用java.util.concurrent.newFixedThreadPool创建一个属于自己的
    * ThreadPoolExecutor线程池，然后不停的会去拿去messages链表队列里的消息数据，
    * 并根据消息的类型执行message的模式匹配做对应的处理
    * */
  private val threadpool: ThreadPoolExecutor = {
    //  通过配置项拿到dispatcher的线程数量
    val numThreads = nettyEnv.conf.getInt("spark.rpc.netty.dispatcher.numThreads",
      math.max(2, Runtime.getRuntime.availableProcessors()))

    // 最后会生成Java的ThreadPoolExecutor线程池
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
    for (i <- 0 until numThreads) {
      // 直接调用execute执行线程MessageLoop  类型是Runnable
      // 这不是启动，而是把线程丢到  线程池队列，等待Thread触发start，注意：丢进去的不是线程，而是Runnable 的实现类MessageLoop
      // 放进去之后会在ThreadPoolExecutor线程池中并行的启动（虽然是队列，但是却是并行调用MessageLoop线程的start方法，不然线程池的优越性就没有了）
      // 这里相当于直接启动了MessageLoop线程
      pool.execute(new MessageLoop)
    }
    pool
  }

  /** Message loop used for dispatching messages. 用于发送消息的消息循环 */
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        // 线程会不停的去处理过来的messages
        while (true) {
          try {
            // 因为receivers是一个路由器，所以来的消息（Response）和去的消息（request）都在这里面，拿出来一个消息
            // OutputCommitCoordinator
            val data = receivers.take()
            // 如果取出的信息数据是空的
            if (data == PoisonPill) {
              // Put PoisonPill back so that other MessageLoops can see it.
              // 就放回去LinkedBlockingQueue
              receivers.offer(PoisonPill)
              return
            }
            // 调用inbox的process方法，根据需要处理的消息类型message的模式匹配来执行对应的处理方式
            // 调用收件箱的处理方法
            data.inbox.process(Dispatcher.this)
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }

  /** A poison endpoint that indicates MessageLoop should exit its message loop.
    * 一个空的EndpointData
    * */
  private val PoisonPill = new EndpointData(null, null, null)
}
