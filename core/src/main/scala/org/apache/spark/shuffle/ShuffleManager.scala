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

package org.apache.spark.shuffle

import org.apache.spark.{ShuffleDependency, TaskContext}

/**
 * Pluggable interface for shuffle systems. A ShuffleManager is created in SparkEnv on the driver
 * and on each executor, based on the spark.shuffle.manager setting. The driver registers shuffles
 * with it, and executors (or tasks running locally in the driver) can ask to read and write data.
 *
 * NOTE: this will be instantiated by SparkEnv so its constructor can take a SparkConf and
 * boolean isDriver as parameters.
  *
  * shuffle系统的可插入接口。一个ShuffleManager是在SparkEnv的驱动程序和每个执行器上创建的，基于spark.shuffle.manager 设置。
  * 驱动程序用它注册shuffles，而执行器(或驱动程序在本地运行的任务)可以要求读取和写入数据。
  *
  * 注意:这将由SparkEnv实例化，因此它的构造函数可以使用SparkConf和布尔isDriver作为参数。
  *
  * ShuffleManager负责管理本地及远程的block数据的Shuffle操作。shuffleManager默认认为通过反射方式生成的SortShuffleManager的
  * 实例。可以修改属性Spark.shuffle.manager为hash来显示控制使用HashShuffleManager(HashShuffleManager在2.x版本已经取消，现在默认为SortShuffleManager通过持有的)。
  * SortShuffleManager通过持有的IndexShuffleBlockManager间接操作BlockManager中的DiskBlockManager将map结果写入本地，并且根据shuffleId，mapId写入索引文件，
  * 也能通过MapOutputTrackerMaster中维护的mapStatuses从本地或者其他远程节点读取文件。
  *
  * 无论是Hadoop还是spark，shuffle操作都是决定其性能的重要因素。在不能减少shuffle的情况下，使用一个好的shuffle管理器也是优化性能的重要手段。
  * ShuffleManager的主要功能是在task直接传递数据，所以getWriter和getReader是它的主要接口。
  * 大流程：
  *   1）需求方：当一个Stage依赖于一个shuffleMap的结果，那它在DAG分解的时候就能识别到这个依赖，并注册到shuffleManager；
  *   2）供应方：也就是shuffleMap，它在结束后，会将自己的结果注册到shuffleManager，并通知说自己已经结束了。
  *   3）这样，shuffleManager就将shuffle两段连接了起来。
  *
  *
  * Shuffle是MapReduce框架中的一个特定的phase，介于Map phase和Reduce phase之间，当Map的输出结果要被Reduce使用时，
  * 输出结果需要按key哈希，并且分发到每一个Reducer上去，这个过程就是shuffle。由于shuffle涉及到了磁盘的读写和网络的传输，
  * 因此shuffle性能的高低直接影响到了整个程序的运行效率。
  *
  *
  * 在Spark 1.2以前，默认的shuffle计算引擎是HashShuffleManager。该ShuffleManager而HashShuffleManager有着一个非常严重的弊端，
  * 就是会产生大量的中间磁盘文件，进而由大量的磁盘IO操作影响了性能。
  *
  * 因此在Spark 1.2以后的版本中，默认的ShuffleManager改成了SortShuffleManager。SortShuffleManager相较于HashShuffleManager来说，
  * 有了一定的改进。主要就在于，每个Task在进行shuffle操作时，虽然也会产生较多的临时磁盘文件，但是最后会将所有的临时文件合并（merge）
  * 成一个磁盘文件，因此每个Task就只有一个磁盘文件。在下一个stage的shuffle read task拉取自己的数据时，只要根据索引读取每个磁盘文件中
  * 的部分数据即可。
  *
 */
private[spark] trait ShuffleManager {

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
    * 向manager注册一次洗牌shuffle，并获得一个处理任务的句柄。
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  /** Get a writer for a given partition. Called on executors by map tasks.
    * 为给定的分区获取一个写入器。通过映射任务调用执行器。
    * */
  def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V]

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
    * 为一系列reduce分区获取一个阅读器(startPartition to endPartition-1, inclusive)。被运行在executors上的reduce任务调用
   */
  def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C]

  /**
   * Remove a shuffle's metadata from the ShuffleManager.
    * 从ShuffleManager删除一个shuffle的元数据。
   * @return true if the metadata removed successfully, otherwise false.
    *         如果移除成功就返回真，否则返回false
   */
  def unregisterShuffle(shuffleId: Int): Boolean

  /**
   * Return a resolver capable of retrieving shuffle block data based on block coordinates.
    * 返回一个能够根据块坐标来检索shuffle块数据的解析器。
   */
  def shuffleBlockResolver: ShuffleBlockResolver

  /** Shut down this ShuffleManager. 关闭这个ShuffleManager。*/
  def stop(): Unit
}
