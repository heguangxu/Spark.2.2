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

package org.apache.spark.storage

import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.{MapOutputTracker, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * An RpcEndpoint to take commands from the master to execute options. For example,
 * this is used to remove blocks from the slave's BlockManager.
  *
  * 一个RpcEndpoint从主程序中获取命令来执行选项。例如，它用于从slave的BlockManager中移除块。
 */
private[storage]
class BlockManagerSlaveEndpoint(
    override val rpcEnv: RpcEnv,
    blockManager: BlockManager,
    mapOutputTracker: MapOutputTracker)
  extends ThreadSafeRpcEndpoint with Logging {

  //创建消息处理线程
  private val asyncThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("block-manager-slave-async-thread-pool")
  private implicit val asyncExecutionContext = ExecutionContext.fromExecutorService(asyncThreadPool)

  // 根据接收消息的类型（也就是case Class的类型）不同进行相应的处理
  // Operations that involve removing blocks may be slow and should be done asynchronously
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    //删除Block
    case RemoveBlock(blockId) =>
      doAsync[Boolean]("removing block " + blockId, context) {
        blockManager.removeBlock(blockId)
        true
      }

    //删除RDD
    case RemoveRdd(rddId) =>
      doAsync[Int]("removing RDD " + rddId, context) {
        blockManager.removeRdd(rddId)
      }

    //删除Shuffle数据
    case RemoveShuffle(shuffleId) =>
      doAsync[Boolean]("removing shuffle " + shuffleId, context) {
        if (mapOutputTracker != null) {
          mapOutputTracker.unregisterShuffle(shuffleId)
        }
        SparkEnv.get.shuffleManager.unregisterShuffle(shuffleId)
      }

    //删除广播数据
    case RemoveBroadcast(broadcastId, _) =>
      doAsync[Int]("removing broadcast " + broadcastId, context) {
        blockManager.removeBroadcast(broadcastId, tellMaster = true)
      }

    //获得广播变量
    case GetBlockStatus(blockId, _) =>
      context.reply(blockManager.getStatus(blockId))

    //获取Shuffle数据
    case GetMatchingBlockIds(filter, _) =>
      context.reply(blockManager.getMatchingBlockIds(filter))

    //获取线程Dump消息
    case TriggerThreadDump =>
      // EcecutorEndpoint很简单，当接收到SparkUI发来的消息时，将所有线程的栈信息发送回去
      context.reply(Utils.getThreadDump())

    case ReplicateBlock(blockId, replicas, maxReplicas) =>
      context.reply(blockManager.replicateBlock(blockId, replicas.toSet, maxReplicas))

  }

  private def doAsync[T](actionMessage: String, context: RpcCallContext)(body: => T) {
    val future = Future {
      logDebug(actionMessage)
      body
    }
    future.onSuccess { case response =>
      logDebug("Done " + actionMessage + ", response is " + response)
      context.reply(response)
      logDebug("Sent response: " + response + " to " + context.senderAddress)
    }
    future.onFailure { case t: Throwable =>
      logError("Error in " + actionMessage, t)
      context.sendFailure(t)
    }
  }

  override def onStop(): Unit = {
    asyncThreadPool.shutdownNow()
  }
}
