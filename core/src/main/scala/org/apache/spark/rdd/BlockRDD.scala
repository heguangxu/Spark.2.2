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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.storage.{BlockId, BlockManager}

private[spark] class BlockRDDPartition(val blockId: BlockId, idx: Int) extends Partition {
  val index = idx
}

private[spark]
class BlockRDD[T: ClassTag](sc: SparkContext, @transient val blockIds: Array[BlockId])
  extends RDD[T](sc, Nil) {

  @transient lazy val _locations = BlockManager.blockIdsToHosts(blockIds, SparkEnv.get)
  @volatile private var _isValid = true

  override def getPartitions: Array[Partition] = {
    assertValid()
    (0 until blockIds.length).map { i =>
      new BlockRDDPartition(blockIds(i), i).asInstanceOf[Partition]
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    assertValid()
    val blockManager = SparkEnv.get.blockManager
    val blockId = split.asInstanceOf[BlockRDDPartition].blockId
    blockManager.get[T](blockId) match {
      case Some(block) => block.data.asInstanceOf[Iterator[T]]
      case None =>
        throw new Exception(s"Could not compute split, block $blockId of RDD $id not found")
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    assertValid()
    _locations(split.asInstanceOf[BlockRDDPartition].blockId)
  }

  /**
    * Remove the data blocks that this BlockRDD is made from. NOTE: This is an
    * irreversible operation, as the data in the blocks cannot be recovered back
    * once removed. Use it with caution.
    *
    * 删除的数据块，这些数据块是由BlockRDD所产生的。注意：这是一个不可逆的操作，因为块中的数据一经删除就无法恢复。
    * 慎用。
    *
    */
  private[spark] def removeBlocks() {
    blockIds.foreach { blockId =>
      sparkContext.env.blockManager.master.removeBlock(blockId)
    }
    _isValid = false
  }

  /**
    * Whether this BlockRDD is actually usable. This will be false if the data blocks have been
    * removed using `this.removeBlocks`.
    * 这些BlockRDD是否实际上是可用的。这会如果数据块使用`this.removeBlocks`已被删除，将会返回false
    */
  private[spark] def isValid: Boolean = {
    _isValid
  }

  /** Check if this BlockRDD is valid. If not valid, exception is thrown.
    * 验证BlockRDD是否有效的。如果无效，则引发异常。
    * */
  private[spark] def assertValid() {
    if (!isValid) {
      throw new SparkException(
        "Attempted to use %s after its blocks have been removed!".format(toString))
    }
  }

  protected def getBlockIdLocations(): Map[BlockId, Seq[String]] = {
    _locations
  }
}

