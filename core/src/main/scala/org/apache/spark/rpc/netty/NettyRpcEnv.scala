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

import java.io._
import java.net.{InetSocketAddress, URI}
import java.nio.ByteBuffer
import java.nio.channels.{Pipe, ReadableByteChannel, WritableByteChannel}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.Nullable

import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.{DynamicVariable, Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client._
import org.apache.spark.network.crypto.{AuthClientBootstrap, AuthServerBootstrap}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server._
import org.apache.spark.rpc._
import org.apache.spark.serializer.{JavaSerializer, JavaSerializerInstance, SerializationStream}
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, ThreadUtils, Utils}

private[netty] class NettyRpcEnv(
    val conf: SparkConf,
    javaSerializerInstance: JavaSerializerInstance,
    host: String,
    securityManager: SecurityManager) extends RpcEnv(conf) with Logging {

  // 提供一个内部的从Spark JVM(例如，执行器、驱动程序或独立的洗牌服务)中从SparkConf转换到传输conf的实用程序，其中包括分配给该JVM的内核的数量。
  private[netty] val transportConf = SparkTransportConf.fromSparkConf(
    conf.clone.set("spark.rpc.io.numConnectionsPerPeer", "1"),
    "rpc",
    conf.getInt("spark.rpc.io.threads", 0))

  // Dispatcher类是一个消息分发器，负责将RPC消息发送到适当的端点。该类有一个内部类
  // dispatcher负责把messages发送到相关的Endpoint上
  private val dispatcher: Dispatcher = new Dispatcher(this)

  // NettyStreamManager负责远程executor下载Driver端的jar或者其他格式的文件
  private val streamManager = new NettyStreamManager(this)

  // TransportContext主要用于创建TransportServer和TransportClientFactory
  /** 先创建NettyRpcHandler
      然后创建TransportContext
    */
  private val transportContext = new TransportContext(transportConf,
    new NettyRpcHandler(dispatcher, this, streamManager))

  private def createClientBootstraps(): java.util.List[TransportClientBootstrap] = {
    // 查是否启用了Spark通信协议的身份验证。
    if (securityManager.isAuthenticationEnabled()) {
      java.util.Arrays.asList(new AuthClientBootstrap(transportConf,
        securityManager.getSaslUser(), securityManager))
    } else {
      java.util.Collections.emptyList[TransportClientBootstrap]
    }
  }

  /** 创建clientFactory */
  private val clientFactory = transportContext.createClientFactory(createClientBootstraps())

  /**
   * A separate client factory for file downloads. This avoids using the same RPC handler as
   * the main RPC context, so that events caused by these clients are kept isolated from the
   * main RPC traffic.
    *
    * 另一个用于文件下载的客户端工厂。这避免使用与主RPC上下文相同的RPC处理程序，因此这些客户机导致的事件与主RPC通信隔离。
    *
   * It also allows for different configuration of certain properties, such as the number of
   * connections per peer.
    *
    * 它还允许不同的属性配置，例如每个对等点的连接数。
   */
  @volatile private var fileDownloadFactory: TransportClientFactory = _

  val timeoutScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("netty-rpc-env-timeout")

  // Because TransportClientFactory.createClient is blocking, we need to run it in this thread pool
  // to implement non-blocking send/ask.
  // TODO: a non-blocking TransportClientFactory.createClient in future
  // 因为TransportClientFactory。createClient阻塞了，我们需要在这个线程池中运行它来实现非阻塞发送/请求。
  // 待办事项:非阻塞TransportClientFactory。createClient在未来
  private[netty] val clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool(
    "netty-rpc-connection",
    conf.getInt("spark.rpc.connect.threads", 64))

  @volatile private var server: TransportServer = _

  private val stopped = new AtomicBoolean(false)

  /**
   * A map for [[RpcAddress]] and [[Outbox]]. When we are connecting to a remote [[RpcAddress]],
   * we just put messages to its [[Outbox]] to implement a non-blocking `send` method.
    *
    * 关于本地处理消息的机制解析完毕，接下来是远程消息体的处理机制解析
    * 这里的消息的底层完全基于Netty管道的writeAndFlush操作，当然也包括了单向和双向消息体，具体实现如下
    * 先了解下这个outboxes-----每个节点都有个outboxes用来存储各个节点对应的outbox
    * 如果接收者非本地地址就会直接发送给对方的outbox, 然后等待线程消费
    *
    * //  当调远程Ref的时候，仅需连接到远程对应的Rpc地址并把message放入它的Outbox等待消费而避免了线程阻塞
    * //  还是调用的Java的ConcurrentHashMap数据结构做的outboxes，里面存放的是Rpc地址和他对应的outbox对象
    * //  outbox里面封装的则是messages消息队列，TransportClient，消息的处理机制等逻辑
   */
  private val outboxes = new ConcurrentHashMap[RpcAddress, Outbox]()

  /**
   * Remove the address's Outbox and stop it.
   */
  private[netty] def removeOutbox(address: RpcAddress): Unit = {
    val outbox = outboxes.remove(address)
    if (outbox != null) {
      outbox.stop()
    }
  }

  def startServer(bindAddress: String, port: Int): Unit = {
    val bootstraps: java.util.List[TransportServerBootstrap] =
      // 检查是否启用了Spark通信协议的身份验证。
      if (securityManager.isAuthenticationEnabled()) {
        // Spark的auth协议进行身份验证
        java.util.Arrays.asList(new AuthServerBootstrap(transportConf, securityManager))
      } else {
        java.util.Collections.emptyList()
      }
    // 创建TransportServer
    server = transportContext.createServer(bindAddress, port, bootstraps)
    // 创建RpcEndpointVerifier,然后注册自己到NettyRpcEnv上并发回自己的Ref的实现
    dispatcher.registerRpcEndpoint(
      RpcEndpointVerifier.NAME, new RpcEndpointVerifier(this, dispatcher))
  }

  @Nullable
  override lazy val address: RpcAddress = {
    if (server != null) RpcAddress(host, server.getPort()) else null
  }

  /**
    * 根据RpcEndpoint的name注册到RpcEnv中并返回它的一个引用RpcEndpointRef
    */
  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    // Dispatcher类是一个消息分发器，负责将RPC消息发送到适当的端点。该类有一个内部类
    dispatcher.registerRpcEndpoint(name, endpoint)
  }

  /**
    * （1）通过url异步获取RpcEndpointRef
    *         def asyncSetupEndpointRefByURI(uri:String):Future[RpcEndpointRef]
    *     通过url同步获取RpcEndpointRef，这是一个阻塞操作
    *         def setupEndpointRefURI(uri:String):RpcEndpointRef = {
    *         defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))}
    */
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    val addr = RpcEndpointAddress(uri)
    val endpointRef = new NettyRpcEndpointRef(conf, addr, this)
    val verifier = new NettyRpcEndpointRef(
      conf, RpcEndpointAddress(addr.rpcAddress, RpcEndpointVerifier.NAME), this)
    verifier.ask[Boolean](RpcEndpointVerifier.CheckExistence(endpointRef.name)).flatMap { find =>
      if (find) {
        Future.successful(endpointRef)
      } else {
        Future.failed(new RpcEndpointNotFoundException(uri))
      }
    }(ThreadUtils.sameThread)
  }

  override def stop(endpointRef: RpcEndpointRef): Unit = {
    require(endpointRef.isInstanceOf[NettyRpcEndpointRef])
    dispatcher.stop(endpointRef)
  }

  private def postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage): Unit = {
    // 如果接收端的TransportClient启动了 就直接调用sendWith
    // 调用sendWith核心方法
    // 提醒一下：这里所有的outbox里提取出的消息体都是实现了trait OutboxMessage
    // 所以不同类型的message调用的sendWith实现也不同
    // 也是分为单向和双向消息体
    // message.receiver(receiver.client)==>NettyRpcEndpointRef==》比如workerRef
    // message的格式：
    // message:"RpcMessage(192.168.2.89:53298,TrackSchedulerIsSet,org.apache.spark.rpc.netty.localNettyRpcCallContext@67bds354)"
    if (receiver.client != null) {
      // 如果接受信息的客户端存在，直接发送信息过去
      message.sendWith(receiver.client)
    } else {
      // 如果接收端没有启动TransportClient就会先查询下是否包含接收者地址
      require(receiver.address != null,
        "Cannot send message to client endpoint with no listen address.")
      val targetOutbox = {
        // 通过Rpc地址从outboxes拿到接收者地址的对应的outbox
        // 数据结构：Java的ConcurrentHashMap[RpcAddress, Outbox]，根据要发送的远程地址获取本地发件箱
        // 有点拗口：我的本地地址是                                                       远程地址
        //        192.168.10.82                                                      192.168.10.83
        //          inbox(82的收件箱) <------------不断的从 右边 取消息放到 左边-------------- outBox(83的发件箱)
        //
        //
        //          outBox(82的发件箱) ------------不断的从 左边 取消息放到 右边--------------> inbox(83的收件箱)
        val outbox = outboxes.get(receiver.address)
        if (outbox == null) {
          // 如果该地址对应的outbox不存在就构建一个 (如果发件箱不存在，就创建一个)
          val newOutbox = new Outbox(this, receiver.address)
          // 并加入到outboxes里面
          val oldOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
          if (oldOutbox == null) {
            // 若为空就直接引用刚生成的newOutbox
            newOutbox
          } else {
            // 返回老的
            oldOutbox
          }
        } else {
          // 返回
          outbox
        }
      }

      // 判断NettyRpcEnv是否停止了
      if (stopped.get) {
        // It's possible that we put `targetOutbox` after stopping. So we need to clean it.
        outboxes.remove(receiver.address)
        // 关闭发件箱
        targetOutbox.stop()
      } else {
        // 最后生成的outbox对象会根据不同的状态执行send中不同的实现
        // 包括可能也会走drainOutbox方法（里面包含在接收者端启动一个TransportClient）
        // 把message添加到自己的消息队列里 ，加入 java.util.LinkedList[OutboxMessage] 的队列中，等待被线程消费
        targetOutbox.send(message)
      }
    }
  }

  /**
    * 我们来看下send这个经典的发送消息的方法，里面封装了不同类型消息体之间的通信的不同实现
    */
  private[netty] def send(message: RequestMessage): Unit = {
    // 拿到需要发送的endpoint地址
    val remoteAddr = message.receiver.address
    // 判断是否是远程地址,如果remoteAddr == address远程地址和自己所在的地址是一个地址，比如，我自己是address=192.168.10.12，要发送的地址
    // 是remoteAddr=192.268.10.12，这说明我是给自己发送消息，所以直接放到自己的邮箱里就可以了，不需要跑一圈
    if (remoteAddr == address) {
      // Message to a local RPC endpoint.
      try {
        // 如果消息接受者在本地就调用dispatcher来发送消息 消息放到自己的收件箱里
        // message的格式：
        // message:"RpcMessage(192.168.2.89:53298,TrackSchedulerIsSet,org.apache.spark.rpc.netty.localNettyRpcCallContext@67bds354)"
        // message:"RequestMessage(192.168.2.89:53298,NettyRpcEndpointRef(spark://LocalSchedulerBackendEndpoint@wer244)"
        dispatcher.postOneWayMessage(message)
      } catch {
        case e: RpcEnvStoppedException => logWarning(e.getMessage)
      }
    } else {
      // 否则就是我自己是address=192.168.10.12，要发送的地址
      // 是remoteAddr=192.268.10.56，这说明我是给别人发送消息，所以需要放到自己的发件箱里就可以了，然后被系统发送出去，别人来接收

      // Message to a remote RPC endpoint.
      // 如果消息接受者在远程节点就发送到对应节点的outbox
      // message.serialize(this) 这里消息要序列化，因为是要放到自己的发件箱，然后发送到别的机器上，要走网络的，因此要序列化
      // message.receiver==>NettyRpcEndpointRef==》比如workerRef
      // message的格式：
      // message:"RpcMessage(192.168.2.89:53298,TrackSchedulerIsSet,org.apache.spark.rpc.netty.localNettyRpcCallContext@67bds354)"
      postToOutbox(message.receiver, OneWayOutboxMessage(message.serialize(this)))
    }
  }

  private[netty] def createClient(address: RpcAddress): TransportClient = {
    clientFactory.createClient(address.host, address.port)
  }

  private[netty] def ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): Future[T] = {
    val promise = Promise[Any]()
    val remoteAddr = message.receiver.address

    def onFailure(e: Throwable): Unit = {
      if (!promise.tryFailure(e)) {
        logWarning(s"Ignored failure: $e")
      }
    }

    def onSuccess(reply: Any): Unit = reply match {
      case RpcFailure(e) => onFailure(e)
      case rpcReply =>
        if (!promise.trySuccess(rpcReply)) {
          logWarning(s"Ignored message: $reply")
        }
    }

    try {
      if (remoteAddr == address) {
        val p = Promise[Any]()
        p.future.onComplete {
          case Success(response) => onSuccess(response)
          case Failure(e) => onFailure(e)
        }(ThreadUtils.sameThread)
        dispatcher.postLocalMessage(message, p)
      } else {
        val rpcMessage = RpcOutboxMessage(message.serialize(this),
          onFailure,
          (client, response) => onSuccess(deserialize[Any](client, response)))
        postToOutbox(message.receiver, rpcMessage)
        promise.future.onFailure {
          case _: TimeoutException => rpcMessage.onTimeout()
          case _ =>
        }(ThreadUtils.sameThread)
      }

      val timeoutCancelable = timeoutScheduler.schedule(new Runnable {
        override def run(): Unit = {
          onFailure(new TimeoutException(s"Cannot receive any reply from ${remoteAddr} " +
            s"in ${timeout.duration}"))
        }
      }, timeout.duration.toNanos, TimeUnit.NANOSECONDS)
      promise.future.onComplete { v =>
        timeoutCancelable.cancel(true)
      }(ThreadUtils.sameThread)
    } catch {
      case NonFatal(e) =>
        onFailure(e)
    }
    promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread)
  }

  private[netty] def serialize(content: Any): ByteBuffer = {
    javaSerializerInstance.serialize(content)
  }

  /**
   * Returns [[SerializationStream]] that forwards the serialized bytes to `out`.
   */
  private[netty] def serializeStream(out: OutputStream): SerializationStream = {
    javaSerializerInstance.serializeStream(out)
  }

  private[netty] def deserialize[T: ClassTag](client: TransportClient, bytes: ByteBuffer): T = {
    NettyRpcEnv.currentClient.withValue(client) {
      deserialize { () =>
        javaSerializerInstance.deserialize[T](bytes)
      }
    }
  }

  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.getRpcEndpointRef(endpoint)
  }

  override def shutdown(): Unit = {
    cleanup()
  }

  override def awaitTermination(): Unit = {
    dispatcher.awaitTermination()
  }

  private def cleanup(): Unit = {
    if (!stopped.compareAndSet(false, true)) {
      return
    }

    val iter = outboxes.values().iterator()
    while (iter.hasNext()) {
      val outbox = iter.next()
      outboxes.remove(outbox.address)
      outbox.stop()
    }
    if (timeoutScheduler != null) {
      timeoutScheduler.shutdownNow()
    }
    if (dispatcher != null) {
      dispatcher.stop()
    }
    if (server != null) {
      server.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
    if (clientConnectionExecutor != null) {
      clientConnectionExecutor.shutdownNow()
    }
    if (fileDownloadFactory != null) {
      fileDownloadFactory.close()
    }
  }

  // RpcEndpointRef需要RpcEnv来反序列化，当反序列化RpcEndpointRefs的object时，需要通过该方法来操作
  override def deserialize[T](deserializationAction: () => T): T = {
    NettyRpcEnv.currentEnv.withValue(this) {
      deserializationAction()
    }
  }

  override def fileServer: RpcEnvFileServer = streamManager

  override def openChannel(uri: String): ReadableByteChannel = {
    val parsedUri = new URI(uri)
    require(parsedUri.getHost() != null, "Host name must be defined.")
    require(parsedUri.getPort() > 0, "Port must be defined.")
    require(parsedUri.getPath() != null && parsedUri.getPath().nonEmpty, "Path must be defined.")

    val pipe = Pipe.open()
    val source = new FileDownloadChannel(pipe.source())
    try {
      val client = downloadClient(parsedUri.getHost(), parsedUri.getPort())
      val callback = new FileDownloadCallback(pipe.sink(), source, client)
      client.stream(parsedUri.getPath(), callback)
    } catch {
      case e: Exception =>
        pipe.sink().close()
        source.close()
        throw e
    }

    source
  }

  private def downloadClient(host: String, port: Int): TransportClient = {
    if (fileDownloadFactory == null) synchronized {
      if (fileDownloadFactory == null) {
        val module = "files"
        val prefix = "spark.rpc.io."
        val clone = conf.clone()

        // Copy any RPC configuration that is not overridden in the spark.files namespace.
        conf.getAll.foreach { case (key, value) =>
          if (key.startsWith(prefix)) {
            val opt = key.substring(prefix.length())
            clone.setIfMissing(s"spark.$module.io.$opt", value)
          }
        }

        val ioThreads = clone.getInt("spark.files.io.threads", 1)
        val downloadConf = SparkTransportConf.fromSparkConf(clone, module, ioThreads)
        val downloadContext = new TransportContext(downloadConf, new NoOpRpcHandler(), true)
        fileDownloadFactory = downloadContext.createClientFactory(createClientBootstraps())
      }
    }
    fileDownloadFactory.createClient(host, port)
  }

  private class FileDownloadChannel(source: ReadableByteChannel) extends ReadableByteChannel {

    @volatile private var error: Throwable = _

    def setError(e: Throwable): Unit = {
      error = e
      source.close()
    }

    override def read(dst: ByteBuffer): Int = {
      Try(source.read(dst)) match {
        case Success(bytesRead) => bytesRead
        case Failure(readErr) =>
          if (error != null) {
            throw error
          } else {
            throw readErr
          }
      }
    }

    override def close(): Unit = source.close()

    override def isOpen(): Boolean = source.isOpen()

  }

  private class FileDownloadCallback(
      sink: WritableByteChannel,
      source: FileDownloadChannel,
      client: TransportClient) extends StreamCallback {

    override def onData(streamId: String, buf: ByteBuffer): Unit = {
      while (buf.remaining() > 0) {
        sink.write(buf)
      }
    }

    override def onComplete(streamId: String): Unit = {
      sink.close()
    }

    override def onFailure(streamId: String, cause: Throwable): Unit = {
      logDebug(s"Error downloading stream $streamId.", cause)
      source.setError(cause)
      sink.close()
    }

  }
}

/**
  * 我们再来研究下NettyRpcEnv类，该类继承RpcEnv，具有伴生对象。伴生对象仅维持两个对象currentEnv和currentClient
  * （在NettyRpcEndpointRef反序列化时使用，暂时不太明白什么意思）
  */
private[netty] object NettyRpcEnv extends Logging {
  /**
   * When deserializing the [[NettyRpcEndpointRef]], it needs a reference to [[NettyRpcEnv]].
   * Use `currentEnv` to wrap the deserialization codes. E.g.,
   *
   * {{{
   *   NettyRpcEnv.currentEnv.withValue(this) {
   *     your deserialization codes
   *   }
   * }}}
   */
  private[netty] val currentEnv = new DynamicVariable[NettyRpcEnv](null)

  /**
   * Similar to `currentEnv`, this variable references the client instance associated with an
   * RPC, in case it's needed to find out the remote address during deserialization.
   */
  private[netty] val currentClient = new DynamicVariable[TransportClient](null)

}

private[rpc] class NettyRpcEnvFactory extends RpcEnvFactory with Logging {

  /**
    * NettyRpcEnvFactory创建了NettyRpcEnv之后，如果clientMode为false，即服务端（Driver端Rpc通讯），则使用创建出
    * 的NettyRpcEnv的函数startServer定义一个函数变量startNettyRpcEnv（(nettyEnv, nettyEnv.address.port)为函
    * 数的返回值），将该函数作为参数传递给函数Utils.startServiceOnPort，即在Driver端启动服务。
    *
    * 这里可以进入Utils.startServiceOnPort这个函数看看源代码，可以看出为什么不直接调用nettyEnv.startServer，而要把它封装起来
    * 传递给工具类来调用：在这个端口启动服务不一定一次就能成功，工具类里对失败的情况做最大次数的尝试，直到启动成功并返回启
    * 动成功后的端口。
    */
  def create(config: RpcEnvConfig): RpcEnv = {
    val sparkConf = config.conf
    // Use JavaSerializerInstance in multiple threads is safe. However, if we plan to support
    // KryoSerializer in future, we have to use ThreadLocal to store SerializerInstance
    // 在多个线程中使用JavaSerializerInstance是安全的。然而，如果我们计划将来支持KryoSerializer，
    // 我们必须使用ThreadLocal来存储SerializerInstance
    // Netty的通讯都是基于Jav序列化，暂时不支持Kryo

    // a.初始化JavaSerializer，初始化NettyRpcEnv，如果是 非客户端模式就启动netty服务
    val javaSerializerInstance =
      new JavaSerializer(sparkConf).newInstance().asInstanceOf[JavaSerializerInstance]

    // 初始化NettyRpcEnv
    val nettyEnv =
      new NettyRpcEnv(sparkConf, javaSerializerInstance, config.advertiseAddress,
        config.securityManager)
    // 判断是否在Driver端
    if (!config.clientMode) {
      // startNettyRpcEnv作为一个函数变量将在下面的startServiceOnPort中被调用
      // 简单解释一下这段代码
      // 声明一个函数变量，参数是int(actuslPort)，=>后面是实现体，最终返回的是2元组（NettyRpcEnv,int）
      val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
        /** 主要是构建TransportServer和注册dispatcher */
        nettyEnv.startServer(config.bindAddress, actualPort)
        (nettyEnv, nettyEnv.address.port)
      }
      try {
        // 其实内部实现还是调用startNettyRpcEnv在指定的端口实例化，并且返回nettyEnv对象
        Utils.startServiceOnPort(config.port, startNettyRpcEnv, sparkConf, config.name)._1
      } catch {
        case NonFatal(e) =>
          nettyEnv.shutdown()
          throw e
      }
    }
    nettyEnv
  }
}

/**
 * The NettyRpcEnv version of RpcEndpointRef. NettyRpcEnv版本的RpcEndpointRef。
 *
 * This class behaves differently depending on where it's created. On the node that "owns" the
 * RpcEndpoint, it's a simple wrapper around the RpcEndpointAddress instance.
  * 这个类的行为取决于它在哪里创建。在“拥有”RpcEndpoint的节点上，它是一个围绕RpcEndpointAddress实例的简单包装器。
 *
 * On other machines that receive a serialized version of the reference, the behavior changes. The
 * instance will keep track of the TransportClient that sent the reference, so that messages
 * to the endpoint are sent over the client connection, instead of needing a new connection to
 * be opened.
  * 在其他接收到引用的序列化版本的机器上，行为发生了变化。实例将跟踪发送引用的传输客户机，以便将消息发送到客户端连接上，而不是需要打开新的连接。
 *
 * The RpcAddress of this ref can be null; what that means is that the ref can only be used through
 * a client connection, since the process hosting the endpoint is not listening for incoming
 * connections. These refs should not be shared with 3rd parties, since they will not be able to
 * send messages to the endpoint.
  *
  * 该ref的RpcAddress可以为空;这意味着ref只能通过客户端连接使用，因为托管端点的进程没有监听传入的连接。这些refs不应该与第三方共享，
  * 因为它们不能将消息发送到端点。
 *
 * @param conf Spark configuration.
 * @param endpointAddress The address where the endpoint is listening.
 * @param nettyEnv The RpcEnv associated with this ref.
  *
  *  RpcEndpointRef是一个对RpcEndpoint的远程引用对象，通过它可以向远程的RpcEndpoint端发送消息以进行通讯。
 */
private[netty] class NettyRpcEndpointRef(
    @transient private val conf: SparkConf,
    private val endpointAddress: RpcEndpointAddress,
    @transient @volatile private var nettyEnv: NettyRpcEnv) extends RpcEndpointRef(conf) {

  @transient @volatile var client: TransportClient = _

  override def address: RpcAddress =
    if (endpointAddress.rpcAddress != null) endpointAddress.rpcAddress else null

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    nettyEnv = NettyRpcEnv.currentEnv.value
    client = NettyRpcEnv.currentClient.value
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
  }

  override def name: String = endpointAddress.name

  /**
    * ask方法发送消息后需要等待通信对端给予响应，通过Future来异步获取响应结果，也是在NettyRpcEndpointRef中实现
    * */
  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    nettyEnv.ask(new RequestMessage(nettyEnv.address, this, message), timeout)
  }

  // send方法发送消息后不等待响应
  // 它是通过NettyRpcEnv来发送RequestMessage消息，并将当前NettyRpcEndpointRef封装到RequestMessage消息对象中发送出去，
  // 通信对端通过该NettyRpcEndpointRef能够识别出消息来源。
  override def send(message: Any): Unit = {
    require(message != null, "Message is null")
    nettyEnv.send(new RequestMessage(nettyEnv.address, this, message))
  }

  override def toString: String = s"NettyRpcEndpointRef(${endpointAddress})"

  final override def equals(that: Any): Boolean = that match {
    case other: NettyRpcEndpointRef => endpointAddress == other.endpointAddress
    case _ => false
  }

  final override def hashCode(): Int =
    if (endpointAddress == null) 0 else endpointAddress.hashCode()
}




/**
 * The message that is sent from the sender to the receiver.
  * 从发送方发送到接收方的消息。
 *
 * @param senderAddress the sender address. It's `null` if this message is from a client
 *                      `NettyRpcEnv`.
  *                      发送方地址。如果此消息来自客户的NettyRpcEnv，则为“null”。
 * @param receiver the receiver of this message.
  *                 这个消息的接收者。
 * @param content the message content.
  *                消息的内容
 */
private[netty] class RequestMessage(
    val senderAddress: RpcAddress,
    val receiver: NettyRpcEndpointRef,
    val content: Any) {

  /** Manually serialize [[RequestMessage]] to minimize the size.
    * 手工序列化[[RequestMessage]]以最小化大小。
    * */
  def serialize(nettyEnv: NettyRpcEnv): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    val out = new DataOutputStream(bos)
    try {
      writeRpcAddress(out, senderAddress)
      writeRpcAddress(out, receiver.address)
      out.writeUTF(receiver.name)
      val s = nettyEnv.serializeStream(out)
      try {
        s.writeObject(content)
      } finally {
        s.close()
      }
    } finally {
      out.close()
    }
    bos.toByteBuffer
  }

  private def writeRpcAddress(out: DataOutputStream, rpcAddress: RpcAddress): Unit = {
    if (rpcAddress == null) {
      out.writeBoolean(false)
    } else {
      out.writeBoolean(true)
      out.writeUTF(rpcAddress.host)
      out.writeInt(rpcAddress.port)
    }
  }

  override def toString: String = s"RequestMessage($senderAddress, $receiver, $content)"
}

private[netty] object RequestMessage {

  private def readRpcAddress(in: DataInputStream): RpcAddress = {
    val hasRpcAddress = in.readBoolean()
    if (hasRpcAddress) {
      RpcAddress(in.readUTF(), in.readInt())
    } else {
      null
    }
  }

  def apply(nettyEnv: NettyRpcEnv, client: TransportClient, bytes: ByteBuffer): RequestMessage = {
    val bis = new ByteBufferInputStream(bytes)
    val in = new DataInputStream(bis)
    try {
      val senderAddress = readRpcAddress(in)
      val endpointAddress = RpcEndpointAddress(readRpcAddress(in), in.readUTF())
      val ref = new NettyRpcEndpointRef(nettyEnv.conf, endpointAddress, nettyEnv)
      ref.client = client
      new RequestMessage(
        senderAddress,
        ref,
        // The remaining bytes in `bytes` are the message content.
        nettyEnv.deserialize(client, bytes))
    } finally {
      in.close()
    }
  }
}

/**
 * A response that indicates some failure happens in the receiver side.
 */
private[netty] case class RpcFailure(e: Throwable)

/**
 * Dispatches incoming RPCs to registered endpoints. 将输入的rpc发送到已注册的端点。
 *
 * The handler keeps track of all client instances that communicate with it, so that the RpcEnv
 * knows which `TransportClient` instance to use when sending RPCs to a client endpoint (i.e.,
 * one that is not listening for incoming connections, but rather needs to be contacted via the
 * client socket).
  *
  * 处理程序跟踪与它通信的所有客户端实例，以便RpcEnv知道在将rpc发送到客户端端点时要使用哪些“传输客户机TransportClient”
  * 实例(例如。，它不监听传入的连接，而是需要通过客户端套接字连接。
 *
 * Events are sent on a per-connection basis, so if a client opens multiple connections to the
 * RpcEnv, multiple connection / disconnection events will be created for that client (albeit
 * with different `RpcAddress` information).
  *
  * 事件以每个连接的方式发送，因此，如果客户端打开多个连接到RpcEnv，将为该客户机创建多个连接/断开连接事件
  * (尽管有不同的“RpcAddress”信息)。
 */
private[netty] class NettyRpcHandler(
    dispatcher: Dispatcher,
    nettyEnv: NettyRpcEnv,
    streamManager: StreamManager) extends RpcHandler with Logging {

  // A variable to track the remote RpcEnv addresses of all clients 跟踪所有客户端远程RpcEnv地址的变量
  // 相当于路由表
  private val remoteAddresses = new ConcurrentHashMap[RpcAddress, RpcAddress]()

  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    // 这个函数不是很清楚干嘛的
    val messageToDispatch = internalReceive(client, message)
    // 发送一个信息给远程的endpoint
    dispatcher.postRemoteMessage(messageToDispatch, callback)
  }

  override def receive(
      client: TransportClient,
      message: ByteBuffer): Unit = {
    val messageToDispatch = internalReceive(client, message)
    // 单向发送消息
    dispatcher.postOneWayMessage(messageToDispatch)
  }

  private def internalReceive(client: TransportClient, message: ByteBuffer): RequestMessage = {
    // InetSocketAddress 这个类实现了IP套接字地址(IP地址+端口号)，也可以是一对(主机名+端口号)，在这种情况下，
    // 将尝试解析主机名。如果解析失败，那么地址就被称为< I >未解析的< /I >，但仍然可以在某些情况下使用，
    // 比如通过代理连接。 它提供了一个用于套接字绑定、连接或返回值的不可变对象。 wildcard是一个特殊的本地IP地址。
    // 它通常表示“any”，只能用于bind操作。
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    // 组成一个RpcAddress，格式：spark://host:port
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    // 组成一个请求消息
    val requestMessage = RequestMessage(nettyEnv, client, message)
    if (requestMessage.senderAddress == null) {
      // Create a new message with the socket address of the client as the sender.
      // 用客户端的套接字地址创建一个新消息作为发送方。
      new RequestMessage(clientAddr, requestMessage.receiver, requestMessage.content)
    } else {
      // The remote RpcEnv listens to some port, we should also fire a RemoteProcessConnected for
      // the listening address
      // 远程RpcEnv监听某个端口，我们也应该为监听地址触发一个RemoteProcessConnected
      val remoteEnvAddress = requestMessage.senderAddress
      // 放到remoteAddresses中，remoteAddresses相当于路由表，如果没有就添加进去，先获取，后添加
      if (remoteAddresses.putIfAbsent(clientAddr, remoteEnvAddress) == null) {
        // RemoteProcessConnected:一个消息告诉所有端点一个远程进程已经连接
        // postToAll:发送信息到所有已经注册的RpcEndpoint
        dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
      }
      requestMessage
    }
  }

  override def getStreamManager: StreamManager = streamManager

  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      dispatcher.postToAll(RemoteProcessConnectionError(cause, clientAddr))
      // If the remove RpcEnv listens to some address, we should also fire a
      // RemoteProcessConnectionError for the remote RpcEnv listening address
      val remoteEnvAddress = remoteAddresses.get(clientAddr)
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(RemoteProcessConnectionError(cause, remoteEnvAddress))
      }
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null.
      // See java.net.Socket.getRemoteSocketAddress
      // Because we cannot get a RpcAddress, just log it
      logError("Exception before connecting to the client", cause)
    }
  }

  override def channelActive(client: TransportClient): Unit = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    dispatcher.postToAll(RemoteProcessConnected(clientAddr))
  }

  override def channelInactive(client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      nettyEnv.removeOutbox(clientAddr)
      dispatcher.postToAll(RemoteProcessDisconnected(clientAddr))
      val remoteEnvAddress = remoteAddresses.remove(clientAddr)
      // If the remove RpcEnv listens to some address, we should also  fire a
      // RemoteProcessDisconnected for the remote RpcEnv listening address
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(RemoteProcessDisconnected(remoteEnvAddress))
      }
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null. In this case,
      // we can ignore it since we don't fire "Associated".
      // See java.net.Socket.getRemoteSocketAddress
    }
  }
}
