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

package org.apache.spark.network.netty

import java.io.File
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.network._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, TransportClientBootstrap, TransportClientFactory}
import org.apache.spark.network.crypto.{AuthClientBootstrap, AuthServerBootstrap}
import org.apache.spark.network.server._
import org.apache.spark.network.shuffle.{BlockFetchingListener, OneForOneBlockFetcher, RetryingBlockFetcher}
import org.apache.spark.network.shuffle.protocol.UploadBlock
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.Utils

/**
 * A BlockTransferService that uses Netty to fetch a set of blocks at time.
  * 一个块传输服务，它使用Netty在同一时间内获取一组块的集合。
 */
private[spark] class NettyBlockTransferService(
    conf: SparkConf,
    securityManager: SecurityManager,
    bindAddress: String,
    override val hostName: String,
    _port: Int,
    numCores: Int)
  extends BlockTransferService {

  // TODO: Don't use Java serialization, use a more cross-version compatible serialization format.
  // 不要使用java的序列化 使用更跨版本的兼容序列化格式。
  private val serializer = new JavaSerializer(conf)
  // Spark负责安全的类。 检查是否启用了Spark通信协议的身份验证。
  private val authEnabled = securityManager.isAuthenticationEnabled()
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numCores)

  private[this] var transportContext: TransportContext = _
  private[this] var server: TransportServer = _
  private[this] var clientFactory: TransportClientFactory = _
  private[this] var appId: String = _

  /**
      1.创建RpcServer；
      2.构建TransportContext;
      3.创建RPC客户端工厂TransportClientFactory;
      4.创建Netty服务器TraansportServer,可以修改属性spark.blockManager.port
        (默认为0，表示随机选择)改变TransportServer的端口。
    */
  override def init(blockDataManager: BlockDataManager): Unit = {
    // 1.创建RpcServer；
    val rpcHandler = new NettyBlockRpcServer(conf.getAppId, serializer, blockDataManager)
    var serverBootstrap: Option[TransportServerBootstrap] = None
    var clientBootstrap: Option[TransportClientBootstrap] = None
    // 是否开启了权限认证spark.authenticate
    if (authEnabled) {
      serverBootstrap = Some(new AuthServerBootstrap(transportConf, securityManager))
      clientBootstrap = Some(new AuthClientBootstrap(transportConf, conf.getAppId, securityManager))
    }
    // 2.构建TransportContext;
    transportContext = new TransportContext(transportConf, rpcHandler)
    clientFactory = transportContext.createClientFactory(clientBootstrap.toSeq.asJava)
    server = createServer(serverBootstrap.toList)
    appId = conf.getAppId
    //26=> 17/12/05 11:56:51 INFO NettyBlockTransferService: Server created on 192.168.1.161:55290
    logInfo(s"Server created on ${hostName}:${server.getPort}")
  }



  /** Creates and binds the TransportServer, possibly trying multiple ports.
    * 创建并绑定传输服务器TransportServer，可能尝试多个端口。
    * */
  private def createServer(bootstraps: List[TransportServerBootstrap]): TransportServer = {
    def startService(port: Int): (TransportServer, Int) = {
      val server = transportContext.createServer(bindAddress, port, bootstraps.asJava)
      (server, server.getPort)
    }

    Utils.startServiceOnPort(_port, startService, conf, getClass.getName)._1
  }

  /**
    * NettyBlockTransferService的fetchBlocks方法用于获取远程shuffle文件，实际上是利用NettyBlockTransferService
    * 中创建的netty服务。
    *
    *  获取远程节点上的shuffle文件
    */
  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener,
      shuffleFiles: Array[File]): Unit = {
    logTrace(s"Fetch blocks from $host:$port (executor id $execId)")
    try {
      val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
        override def createAndStart(blockIds: Array[String], listener: BlockFetchingListener) {
          //通过C/S模式从远程进行通信，来拉去数据。
          val client = clientFactory.createClient(host, port)
          new OneForOneBlockFetcher(client, appId, execId, blockIds.toArray, listener,
            transportConf, shuffleFiles).start()
        }
      }

      val maxRetries = transportConf.maxIORetries()
      if (maxRetries > 0) {
        // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
        // a bug in this code. We should remove the if statement once we're sure of the stability.
        new RetryingBlockFetcher(transportConf, blockFetchStarter, blockIds, listener).start()
      } else {
        blockFetchStarter.createAndStart(blockIds, listener)
      }
    } catch {
      case e: Exception =>
        logError("Exception while beginning fetchBlocks", e)
        blockIds.foreach(listener.onBlockFetchFailure(_, e))
    }
  }

  override def port: Int = server.getPort

  /**
    * NettyBlockTransferService的uploadBlock方法用于上传shuffle文件到远程Executor，实际上也是利用NettyBlockTransferService
    * 中创建的Netty服务。其中步骤如下：
    *   1.创建Netty服务的客户端，客户端连接的hostname和port正式我们随机选择的BlockManager的hostname和port.
    *   2.将Block的存储级别Storagelevel序列化。
    *   3.将BlockByteBuffer转换为数组，便于序列化。
    *   4.将appId,execId,blockId，序列化的Storagelevel，转换为数组的Block封装为UploadBlock,并将UploadBlock序列化为字节数组。
    *   5.最终调用Netty客户端的sendRpc方法将字节数组上传，回调函数RpcResponseCallback根据RPC的结果更改上传状态。
    */
  override def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Future[Unit] = {
    val result = Promise[Unit]()
    val client = clientFactory.createClient(hostname, port)

    // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
    // Everything else is encoded using our binary protocol.
    val metadata = JavaUtils.bufferToArray(serializer.newInstance().serialize((level, classTag)))

    // Convert or copy nio buffer into array in order to serialize it.
    val array = JavaUtils.bufferToArray(blockData.nioByteBuffer())

    client.sendRpc(new UploadBlock(appId, execId, blockId.toString, metadata, array).toByteBuffer,
      new RpcResponseCallback {
        override def onSuccess(response: ByteBuffer): Unit = {
          logTrace(s"Successfully uploaded block $blockId")
          result.success((): Unit)
        }
        override def onFailure(e: Throwable): Unit = {
          logError(s"Error while uploading block $blockId", e)
          result.failure(e)
        }
      })

    result.future
  }

  override def close(): Unit = {
    if (server != null) {
      server.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
  }
}
