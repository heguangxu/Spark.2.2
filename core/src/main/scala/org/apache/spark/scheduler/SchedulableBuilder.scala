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

package org.apache.spark.scheduler

import java.io.{FileInputStream, InputStream}
import java.util.{Locale, NoSuchElementException, Properties}

import scala.util.control.NonFatal
import scala.xml.{Node, XML}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.util.Utils

/**
 * An interface to build Schedulable tree
 * buildPools: build the tree nodes(pools)
 * addTaskSetManager: build the leaf nodes(TaskSetManagers)
  *
  * 构建可调度树
  * buildPools:构建树节点(pools)
  * addTaskSetManager:构建叶节点(TaskSetManagers)
 */
private[spark] trait SchedulableBuilder {
  def rootPool: Pool

  def buildPools(): Unit

  def addTaskSetManager(manager: Schedulable, properties: Properties): Unit
}

private[spark] class FIFOSchedulableBuilder(val rootPool: Pool)
  extends SchedulableBuilder with Logging {

  override def buildPools() {
    // nothing
  }

  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    rootPool.addSchedulable(manager)
  }
}

private[spark] class FairSchedulableBuilder(val rootPool: Pool, conf: SparkConf)
  extends SchedulableBuilder with Logging {

  val SCHEDULER_ALLOCATION_FILE_PROPERTY = "spark.scheduler.allocation.file"
  val schedulerAllocFile = conf.getOption(SCHEDULER_ALLOCATION_FILE_PROPERTY)
  //  调度的配置文件
  val DEFAULT_SCHEDULER_FILE = "fairscheduler.xml"
  // fair的调度properties
  val FAIR_SCHEDULER_PROPERTIES = "spark.scheduler.pool"
  // 默认的pool名称
  val DEFAULT_POOL_NAME = "default"
  val MINIMUM_SHARES_PROPERTY = "minShare"
  val SCHEDULING_MODE_PROPERTY = "schedulingMode"
  val WEIGHT_PROPERTY = "weight"
  val POOL_NAME_PROPERTY = "@name"
  val POOLS_PROPERTY = "pool"
  // 默认的调度模式
  val DEFAULT_SCHEDULING_MODE = SchedulingMode.FIFO
  val DEFAULT_MINIMUM_SHARE = 0
  val DEFAULT_WEIGHT = 1

  override def buildPools() {
    var fileData: Option[(InputStream, String)] = None
    try {
      fileData = schedulerAllocFile.map { f =>
        val fis = new FileInputStream(f)
        logInfo(s"Creating Fair Scheduler pools from $f")
        Some((fis, f))
      }.getOrElse {
        val is = Utils.getSparkClassLoader.getResourceAsStream(DEFAULT_SCHEDULER_FILE)
        if (is != null) {
          logInfo(s"Creating Fair Scheduler pools from default file: $DEFAULT_SCHEDULER_FILE")
          Some((is, DEFAULT_SCHEDULER_FILE))
        } else {
          logWarning("Fair Scheduler configuration file not found so jobs will be scheduled in " +
            s"FIFO order. To use fair scheduling, configure pools in $DEFAULT_SCHEDULER_FILE or " +
            s"set $SCHEDULER_ALLOCATION_FILE_PROPERTY to a file that contains the configuration.")
          None
        }
      }

      //加载配置文件，根据配置文件创建pool
      fileData.foreach { case (is, fileName) => buildFairSchedulerPool(is, fileName) }
    } catch {
      case NonFatal(t) =>
        val defaultMessage = "Error while building the fair scheduler pools"
        val message = fileData.map { case (is, fileName) => s"$defaultMessage from $fileName" }
          .getOrElse(defaultMessage)
        logError(message, t)
        throw t
    } finally {
      fileData.foreach { case (is, fileName) => is.close() }
    }

    // finally create "default" pool
    //创建default pool
    buildDefaultPool()
  }

  // 创建磨默认的pool
  private def buildDefaultPool() {
    if (rootPool.getSchedulableByName(DEFAULT_POOL_NAME) == null) {
      // default pool的名字为default，weight为1，minshare为0，schedule_mode为FIFO
      val pool = new Pool(DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE,
        DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
      //添加到root pool里面
      rootPool.addSchedulable(pool)
      logInfo("Created default pool: %s, schedulingMode: %s, minShare: %d, weight: %d".format(
        DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
    }
  }


  private def buildFairSchedulerPool(is: InputStream, fileName: String) {
    val xml = XML.load(is)
    for (poolNode <- (xml \\ POOLS_PROPERTY)) {

      val poolName = (poolNode \ POOL_NAME_PROPERTY).text

      val schedulingMode = getSchedulingModeValue(poolNode, poolName,
        DEFAULT_SCHEDULING_MODE, fileName)
      val minShare = getIntValue(poolNode, poolName, MINIMUM_SHARES_PROPERTY,
        DEFAULT_MINIMUM_SHARE, fileName)
      val weight = getIntValue(poolNode, poolName, WEIGHT_PROPERTY,
        DEFAULT_WEIGHT, fileName)

      //添加到root pool里面
      rootPool.addSchedulable(new Pool(poolName, schedulingMode, minShare, weight))

      logInfo("Created pool: %s, schedulingMode: %s, minShare: %d, weight: %d".format(
        poolName, schedulingMode, minShare, weight))
    }
  }

  private def getSchedulingModeValue(
      poolNode: Node,
      poolName: String,
      defaultValue: SchedulingMode,
      fileName: String): SchedulingMode = {

    val xmlSchedulingMode =
      (poolNode \ SCHEDULING_MODE_PROPERTY).text.trim.toUpperCase(Locale.ROOT)
    val warningMessage = s"Unsupported schedulingMode: $xmlSchedulingMode found in " +
      s"Fair Scheduler configuration file: $fileName, using " +
      s"the default schedulingMode: $defaultValue for pool: $poolName"
    try {
      if (SchedulingMode.withName(xmlSchedulingMode) != SchedulingMode.NONE) {
        SchedulingMode.withName(xmlSchedulingMode)
      } else {
        logWarning(warningMessage)
        defaultValue
      }
    } catch {
      case e: NoSuchElementException =>
        logWarning(warningMessage)
        defaultValue
    }
  }

  private def getIntValue(
      poolNode: Node,
      poolName: String,
      propertyName: String,
      defaultValue: Int,
      fileName: String): Int = {

    val data = (poolNode \ propertyName).text.trim
    try {
      data.toInt
    } catch {
      case e: NumberFormatException =>
        logWarning(s"Error while loading fair scheduler configuration from $fileName: " +
          s"$propertyName is blank or invalid: $data, using the default $propertyName: " +
          s"$defaultValue for pool: $poolName")
        defaultValue
    }
  }

  //FAIR模式下，addTaskSetManager
  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    // 如果fair的调度配置properties不为空，使用fair默认的名称，否则使用fifo默认的名称
    val poolName = if (properties != null) {
        properties.getProperty(FAIR_SCHEDULER_PROPERTIES, DEFAULT_POOL_NAME)
      } else {
        DEFAULT_POOL_NAME
      }
    //获得根节点的默认调度池的引用
    var parentPool = rootPool.getSchedulableByName(poolName)
    if (parentPool == null) {
      // we will create a new pool that user has configured in app
      // instead of being defined in xml file
      // 我们将创建一个用户在app中配置的新池，而不是在xml文件中定义
      //根据优先级获得父可调度对象pool的引用。
      parentPool = new Pool(poolName, DEFAULT_SCHEDULING_MODE,
        DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
      //最为根节点default pool的孩子加入default pool中
      rootPool.addSchedulable(parentPool)
      logWarning(s"A job was submitted with scheduler pool $poolName, which has not been " +
        "configured. This can happen when the file that pools are read from isn't set, or " +
        s"when that file doesn't contain $poolName. Created $poolName with default " +
        s"configuration (schedulingMode: $DEFAULT_SCHEDULING_MODE, " +
        s"minShare: $DEFAULT_MINIMUM_SHARE, weight: $DEFAULT_WEIGHT)")
    }
    //与FIFO类似，在每个父pool中采用队列形式，将TaskSetManager加入队尾。
    parentPool.addSchedulable(manager)
    logInfo("Added task set " + manager.name + " tasks to pool " + poolName)
  }
}
