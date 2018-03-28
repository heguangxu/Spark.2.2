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

package org.apache.spark.network;

import java.util.ArrayList;
import java.util.List;

import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.MessageDecoder;
import org.apache.spark.network.protocol.MessageEncoder;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.server.TransportRequestHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.util.TransportFrameDecoder;

/**
 * Contains the context to create a {@link TransportServer}, {@link TransportClientFactory}, and to
 * setup Netty Channel pipelines with a
 * {@link org.apache.spark.network.server.TransportChannelHandler}.
 *
 * 包含上下文创建一个{ @link TransportServer },{ @link TransportClientFactory },
 * 并设置网状的通道管道与{ @link org.apache.spark.network.server.TransportChannelHandler }。
 *
 * There are two communication protocols that the TransportClient provides, control-plane RPCs and
 * data-plane "chunk fetching". The handling of the RPCs is performed outside of the scope of the
 * TransportContext (i.e., by a user-provided handler), and it is responsible for setting up streams
 * which can be streamed through the data plane in chunks using zero-copy IO.
 *
 * 传输客户端提供了两个通信协议，control-plane RPCs 和data-plane "chunk fetching"。
 * RPCs的处理在传输上下文的范围之外执行。，由用户提供的处理程序)，它负责设置流，
 * 这些流可以用零拷贝IO进行数据块中的数据流。
 *
 * The TransportServer and TransportClientFactory both create a TransportChannelHandler for each
 * channel. As each TransportChannelHandler contains a TransportClient, this enables server
 * processes to send messages back to the client on an existing channel.
 *
 *   TransportServer和TransportClientFactory都为每个通道channel创建一个传输处理程序TransportChannelHandler。
 * 由于每个TransportChannelHandler都包含一个传输客户机TransportClient，因此它允许服务器进程通过一个存在的channel
 * 将消息发送给客户端。
 */
public class TransportContext {
  private static final Logger logger = LoggerFactory.getLogger(TransportContext.class);

  private final TransportConf conf;
  private final RpcHandler rpcHandler;
  private final boolean closeIdleConnections;

  /**
   * Force to create MessageEncoder and MessageDecoder so that we can make sure they will be created
   * before switching the current context class loader to ExecutorClassLoader.
   *
   * 强制创建MessageEncoder和MessageDecoder，这样我们可以确保在将当前上下文类加载器切换到
   * ExecutorClassLoader之前创建它们。
   *
   * Netty's MessageToMessageEncoder uses Javassist to generate a matcher class and the
   * implementation calls "Class.forName" to check if this calls is already generated. If the
   * following two objects are created in "ExecutorClassLoader.findClass", it will cause
   * "ClassCircularityError". This is because loading this Netty generated class will call
   * "ExecutorClassLoader.findClass" to search this class, and "ExecutorClassLoader" will try to use
   * RPC to load it and cause to load the non-exist matcher class again. JVM will report
   * `ClassCircularityError` to prevent such infinite recursion. (See SPARK-17714)
   *
   * Netty的MessageToMessageEncoder使用Javassist生成一个matcher类和实现调用"Class.forName"
   * 去检查是否已生成此调用。如果在"ExecutorClassLoader.findClass"中创建了以下两个对象。
   * 它会导致“ClassCircularityError”。这是因为加载这个Netty生成的类将调用"ExecutorClassLoader.findClass"
   * 去搜索这个类，而且"ExecutorClassLoader"将尝试使用RPC来加载它，并导致再次加载不存在的matcher类。
   * JVM将报告“ClassCircularityError”，以防止这种无限递归。(见火花- 17714)
   *
   *
   * 为什么需要MessageEncoder和MessageDecoder？
   *  因为在给予流的传输里（比如TCP/IP）,接收到的数据首先会被存储到一个socket接受缓冲里，不幸的是，基于流的传输
   *  并不是一个数据包队列，而是一个字节队列。即时发送了2个独立的数据包，操作系统也不会作为2个消息处理，而仅仅认为是
   *  一连串的字节。因此不能保证远程写入的数据被精准的读取，举个列子，假设操作系统的TCP/IP协议栈已经接受到了3个数据包。
   *  ABC,DEF，GHI,由于给予流产术的协议的这种统一的性质，在应用程序读取数据时很可能被分成以下的片段：AB,CDEFG，H,I
   *  .因此，接收方不管是客户端还是服务端，都应该把接收到的数据整理成一个或者多个更加有意义并且让程序的逻辑更好理解的
   *  数据。
   *
   *  public static final MessageEncoder INSTANCE = new MessageEncoder();
   *  public static final MessageDecoder INSTANCE = new MessageDecoder();
   *  这里居然是事先创建好的，为什么这样做呢？
   *  我觉得应该是编码解码是底层通讯的基础，应该提早设置好。
   */
  private static final MessageEncoder ENCODER = MessageEncoder.INSTANCE;
  private static final MessageDecoder DECODER = MessageDecoder.INSTANCE;

  public TransportContext(TransportConf conf, RpcHandler rpcHandler) {
    this(conf, rpcHandler, false);
  }

  /**
   *  1.5版本的构造器
   *   public TransportContext(
   *        TransportConf conf,
   *        RpcHandler rpcHandler) {
   *   this.conf = conf;
   *   this.rpcHandler = rpcHandler;
   *   this.encoder = new MessageEncoder;
   *   this.decoder = new MessageDecoder;
   *  }
   *
   *  TransportContext既可以创建Netty服务,也可以创建Netty访问客户端，TransportContext组成如下：
   *    1.TransportConf:主要控制Netty框架提供的shuffle的IO交互的客户端和服务器端线程数量。
   *    2.RpcHandler：负责shuffle的IO服务端在接受到客户端的RPC请求后，提供打开Block的RPC处理。
   *      此处即为NettyBlockRpcServer;
   *    3.encoder:在shuffle的IO客户端对消息内容进行编码，防止服务端丢包和解析错误。
   *    4.decoder：在shuffle的IO服务端对客户端传来的ByteBuf进行解析，防止服务端丢包和解析错误。
   */
  public TransportContext(
      TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections) {
    this.conf = conf;
    this.rpcHandler = rpcHandler;
    this.closeIdleConnections = closeIdleConnections;
  }

  /**
   * Initializes a ClientFactory which runs the given TransportClientBootstraps prior to returning
   * a new Client. Bootstraps will be executed synchronously, and must run successfully in order
   * to create a Client.
   *
   * 初始化一个ClientFactory它是运行给定的TransportClientBootstraps之前返回一个新客户。引导程序将同步执行，
   * 并且必须成功地运行以创建客户机。
   *
   * TeansportCilentFactory是创建Netty客户端TransportClient的工厂类，TransportClient用于向Netty端发送RPC请求，TransportContext的
   * createClientFactory方法用于创建TransportClientFactory。
   */
  public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
    return new TransportClientFactory(this, bootstraps);
  }

  public TransportClientFactory createClientFactory() {
    return createClientFactory(new ArrayList<>());
  }

  /** Create a server which will attempt to bind to a specific port.
   *  创建一个服务 该服务尝试绑定指定的端口
   * */
  public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, null, port, rpcHandler, bootstraps);
  }

  /** Create a server which will attempt to bind to a specific host and port.
   *  创建一个服务 该服务尝试绑定指定的主机名和端口
   * */
  public TransportServer createServer(
      String host, int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, host, port, rpcHandler, bootstraps);
  }

  /** Creates a new server, binding to any available ephemeral port.
   *  创建一个服务 该服务尝试绑定任何可以使用的随机短暂的接口（端口不确定）
   * */
  public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
    return createServer(0, bootstraps);
  }

  public TransportServer createServer() {
    return createServer(0, new ArrayList<>());
  }

  public TransportChannelHandler initializePipeline(SocketChannel channel) {
    return initializePipeline(channel, rpcHandler);
  }

  /**
   * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
   * has a {@link org.apache.spark.network.server.TransportChannelHandler} to handle request or
   * response messages.
   *
   * 初始化一个客户机或服务器网状的信道编码/解码消息的管道,org.apache.spark.network.server.TransportChannelHandler
   * 来处理请求或响应消息。
   *
   * @param channel The channel to initialize.
   * @param channelRpcHandler The RPC handler to use for the channel.
   *
   * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
   * be used to communicate on this channel. The TransportClient is directly associated with a
   * ChannelHandler to ensure all users of the same channel get the same TransportClient object.
   *
   *  返回创建的TransportChannelHandler，它包含一个可用于在该通道上通信的传输客户机TransportClient。传输客户机
   *  TransportClient与通道处理程序直接关联，以确保同一通道的所有用户都得到相同的传输客户机对象。
   */
  public TransportChannelHandler initializePipeline(
      SocketChannel channel,
      RpcHandler channelRpcHandler) {
    try {
      // 创建TransportChannelHandler
      TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
      channel.pipeline()
        .addLast("encoder", ENCODER)
        .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())
        .addLast("decoder", DECODER)
        .addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
        // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
        // would require more logic to guarantee if this were not part of the same event loop.
        .addLast("handler", channelHandler);
      return channelHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }

  /**
   * Creates the server- and client-side handler which is used to handle both RequestMessages and
   * ResponseMessages. The channel is expected to have been successfully created, though certain
   * properties (such as the remoteAddress()) may not be available yet.
   *
   * 创建用于处理RequestMessages和responsemessage的服务器和客户端处理程序。尽管某些属性(例如remoteAddress())
   * 可能还不能使用，但该通道有望被成功创建。
   */
  private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
    //  创建TransportResponseHandler 请求处理程序
    TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
    // 创建传输客户端
    TransportClient client = new TransportClient(channel, responseHandler);
    // ransportRequestHandler，它封装了对所有请求/响应的处理；
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client,
      rpcHandler);
    // 创建TransportChannelHandler
    return new TransportChannelHandler(client, responseHandler, requestHandler,
      conf.connectionTimeoutMs(), closeIdleConnections);
  }

  public TransportConf getConf() { return conf; }
}
