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

import java.io.File
import java.nio.channels.ReadableByteChannel

import scala.concurrent.Future

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.rpc.netty.NettyRpcEnvFactory
import org.apache.spark.util.RpcUtils


/**
 * A RpcEnv implementation must have a [[RpcEnvFactory]] implementation with an empty constructor
 * so that it can be created via Reflection.
  * RpcEnv的实现必须有一个[[RpcEnvFactory]]实现一个空的构造函数，这样它就可以通过反射创建。
  *
  * 这是一个RPC环境，所有的RpcEndpont需要注册到该对象中用于接收消息，注册时需要指定一个name,
  * RpcEnv将会处理从RpcEndpontRef和远程节点发送过来的消息（接口里面看不到这块逻辑），然后发
  * 送给相应的Endpoint处理，对于接收到的异常使用RpcCallContext来处理。
  *
  * 看RpcEnv像akka中的ActorSystem对象，所有的actor和acotorred都属于它，同时有一个根地址，
  * 所有RpcEnv有注册RpcEndpoint的方法，也有一个address返回根地址的方法，RpcEnv有几个方法用
  * 于获取RpcEndpointRef ,  这里说下Endpoint注册名会成为RpcEndpoint的地址,可以看uriof方法，
  * 还有停止和关闭的方法。RpcEnv的deserialize不明白具体用法，RpcEndpiontRef只能使用RpcEnv
  * 解码，当包含有RpcEndpointRef的对象解码时，解码代码将会被方法包装
  *
  * RpcEnv是各个组件之间通信的执行环境，每个节点之间（Driver或者Worker）组件的Endpoint和对应的EndpointRef之间
  * 的信息通信和方法调用都是通过RpcEnv作协调，而底层是通过Netty NIO框架实现（Spark早期版本通信是通过Akka，大的文
  * 件传输是通过Netty，在2.0.0版本后统一由Netty替换成了Akka，实现了通信传输统一化）
  *
  * netty的实现主要实现是：http://blog.csdn.net/ws0owws0ow/article/details/73088865
 */
private[spark] object RpcEnv {

  def create(
      name: String,
      host: String,
      port: Int,
      conf: SparkConf,
      securityManager: SecurityManager,
      clientMode: Boolean = false): RpcEnv = {
    create(name, host, host, port, conf, securityManager, clientMode)
  }

  def create(
      name: String,
      bindAddress: String,
      advertiseAddress: String,
      port: Int,
      conf: SparkConf,
      securityManager: SecurityManager,
      clientMode: Boolean): RpcEnv = {
    // 创建RpcEnv时通过创建RpcEnvFactory(默认为netty)然后使用工厂创建RpcEnv,如下代码所示
    val config = RpcEnvConfig(conf, name, bindAddress, advertiseAddress, port, securityManager,
      clientMode)
    new NettyRpcEnvFactory().create(config)
  }
}


/**
 * An RPC environment. [[RpcEndpoint]]s need to register itself with a name to [[RpcEnv]] to
 * receives messages. Then [[RpcEnv]] will process messages sent from [[RpcEndpointRef]] or remote
 * nodes, and deliver them to corresponding [[RpcEndpoint]]s. For uncaught exceptions caught by
 * [[RpcEnv]], [[RpcEnv]] will use [[RpcCallContext.sendFailure]] to send exceptions back to the
 * sender, or logging them if no such sender or `NotSerializableException`.
 *
 * [[RpcEnv]] also provides some methods to retrieve [[RpcEndpointRef]]s given name or uri.
 */
private[spark] abstract class RpcEnv(conf: SparkConf) {

  private[spark] val defaultLookupTimeout = RpcUtils.lookupRpcTimeout(conf)

  /**
   * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
   * [[RpcEndpoint.self]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
    *
    * 返回注册[[RpcEndpoint]]的RpcEndpointRef。将用于实现[[RpcEndpoint.self]]。
    * 如果对应的[[RpcEndpointRef]]不存在，则返回“null”。
    *
    * 根据RpcEndpoint返回RpcEndpointRef，如果RpcEndpointRef不存在，将返回null。
   */
  private[rpc] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  /**
   * Return the address that [[RpcEnv]] is listening to.
   */
  def address: RpcAddress

  /**
   * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
   * guarantee thread-safety.
    * 使用一个name名称注册一个[[RpcEndpoint]]，并返回其[[RpcEndpointRef]]。[[RpcEnv]]没有保证线程安全。
   */
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
    *
    * （1）通过url异步获取RpcEndpointRef
   */
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `uri`. This is a blocking action.
   */
  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
    defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))
  }

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `address` and `endpointName`.
   * This is a blocking action.
   */
  def setupEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByURI(RpcEndpointAddress(address, endpointName).toString)
  }

  /**
   * Stop [[RpcEndpoint]] specified by `endpoint`.
    * 停止[[RpcEndpoint]]根据指定的`endpoint`
    * 根据RpcEndpointRef停止RpcEndpoint
   */
  def stop(endpoint: RpcEndpointRef): Unit

  /**
   * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
   * call [[awaitTermination()]] straight after [[shutdown()]].
   */
  def shutdown(): Unit

  /**
   * Wait until [[RpcEnv]] exits.
   *
   * TODO do we need a timeout parameter?
    *
    * 等待直到RpcEnv退出
   */
  def awaitTermination(): Unit

  /**
   * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
   * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
    *
    * [[RpcEndpointRef]]没有[[RpcEnv]][[RpcEnv]][RpcEndpointRef]][[RpcEndpointRef]][[RpcEndpointRef]]
    * [[rp因此，当反序列化任何包含[[RpcEndpointRef]]s的对象时，反序列化代码应该用这种方法进行包装。
    *
    * RpcEndpointRef需要RpcEnv来反序列化，当反序列化RpcEndpointRefs的object时，需要通过该方法来操作
   */
  def deserialize[T](deserializationAction: () => T): T

  /**
   * Return the instance of the file server used to serve files. This may be `null` if the
   * RpcEnv is not operating in server mode.
   */
  def fileServer: RpcEnvFileServer

  /**
   * Open a channel to download a file from the given URI. If the URIs returned by the
   * RpcEnvFileServer use the "spark" scheme, this method will be called by the Utils class to
   * retrieve the files.
    *
    * 打开从给定URI下载文件的通道。如果由RpcEnvFileServer返回的uri使用“spark”方案，则Utils类将调用该方法来检索文件。
   *
   * @param uri URI with location of the file.
   */
  def openChannel(uri: String): ReadableByteChannel
}

/**
 * A server used by the RpcEnv to server files to other processes owned by the application.
 *
 * The file server can return URIs handled by common libraries (such as "http" or "hdfs"), or
 * it can return "spark" URIs which will be handled by `RpcEnv#fetchFile`.
 */
private[spark] trait RpcEnvFileServer {

  /**
   * Adds a file to be served by this RpcEnv. This is used to serve files from the driver
   * to executors when they're stored on the driver's local file system.
   *
   * @param file Local file to serve.
   * @return A URI for the location of the file.
   */
  def addFile(file: File): String

  /**
   * Adds a jar to be served by this RpcEnv. Similar to `addFile` but for jars added using
   * `SparkContext.addJar`.
   *
   * @param file Local file to serve.
   * @return A URI for the location of the file.
   */
  def addJar(file: File): String

  /**
   * Adds a local directory to be served via this file server.
   *
   * @param baseUri Leading URI path (files can be retrieved by appending their relative
   *                path to this base URI). This cannot be "files" nor "jars".
   * @param path Path to the local directory.
   * @return URI for the root of the directory in the file server.
   */
  def addDirectory(baseUri: String, path: File): String

  /** Validates and normalizes the base URI for directories. */
  protected def validateDirectoryUri(baseUri: String): String = {
    val fixedBaseUri = "/" + baseUri.stripPrefix("/").stripSuffix("/")
    require(fixedBaseUri != "/files" && fixedBaseUri != "/jars",
      "Directory URI cannot be /files nor /jars.")
    fixedBaseUri
  }

}

private[spark] case class RpcEnvConfig(
    conf: SparkConf,
    name: String,
    bindAddress: String,
    advertiseAddress: String,
    port: Int,
    securityManager: SecurityManager,
    clientMode: Boolean)
