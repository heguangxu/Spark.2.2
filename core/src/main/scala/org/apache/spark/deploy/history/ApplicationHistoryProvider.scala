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

package org.apache.spark.deploy.history

import java.util.zip.ZipOutputStream

import scala.xml.Node

import org.apache.spark.SparkException
import org.apache.spark.ui.SparkUI

private[spark] case class ApplicationAttemptInfo(
    attemptId: Option[String],
    startTime: Long,
    endTime: Long,
    lastUpdated: Long,
    sparkUser: String,
    completed: Boolean = false)

private[spark] case class ApplicationHistoryInfo(
    id: String,
    name: String,
    attempts: List[ApplicationAttemptInfo]) {

  /**
   * Has this application completed?                         应用程序是否已经完成？
   * @return true if the most recent attempt has completed  如果最近的一次完成了，那么就返回真
   */
  def completed: Boolean = {
    attempts.nonEmpty && attempts.head.completed
  }
}

/**
 *  A probe which can be invoked to see if a loaded Web UI has been updated.
 *  The probe is expected to be relative purely to that of the UI returned
 *  in the same [[LoadedAppUI]] instance. That is, whenever a new UI is loaded,
 *  the probe returned with it is the one that must be used to check for it
 *  being out of date; previous probes must be discarded.
  *
  *  一个probe，可以用来看看加载的Web用户界面是否已更新。The probe is expected to be relative purely to that of the UI returned
  *  in the same [[LoadedAppUI]] instance。也就是说，每当加载一个新的UI时，
  *  返回的探针必须被用来检查它是否过时了；以前的探针必须被丢弃。
  *
 */
private[history] abstract class HistoryUpdateProbe {
  /**
   * Return true if the history provider has a later version of the application
   * attempt than the one against this probe was constructed.
    * 如果历史提供者有一个应用程序尝试的后期版本，那么就返回true。
   * @return
   */
  def isUpdated(): Boolean
}

/**
 * All the information returned from a call to `getAppUI()`: the new UI
 * and any required update state.
  *   所有的信息从调用` getappui() `返回：新的用户界面和任何所需的更新状态。
 * @param ui Spark UI
 * @param updateProbe probe to call to check on the update state of this application attempt
 */
private[history] case class LoadedAppUI(
    ui: SparkUI,
    updateProbe: () => Boolean)

private[history] abstract class ApplicationHistoryProvider {

  /**
   * Returns the count of application event logs that the provider is currently still processing.
   * History Server UI can use this to indicate to a user that the application listing on the UI
   * can be expected to list additional known applications once the processing of these
   * application event logs completes.
    *
    * 返回应用程序事件日志数量，提供程序的数量目前仍在处理。历史服务器UI可以使用这一点向用户表明，
    * 一旦处理这些应用程序事件日志完成后，UI上列出的应用程序就可以列出其他已知的应用程序。
   *
   * A History Provider that does not have a notion of count of event logs that may be pending
   * for processing need not override this method.
    *
    * 一个历史提供者，它没有计算事件日志的概念，这些日志可能等待处理，不需要重写此方法。
   *
   * @return Count of application event logs that are currently under process
    *         应用程序事件日志，正在处理数
   */
  def getEventLogsUnderProcess(): Int = {
    0
  }

  /**
   * Returns the time the history provider last updated the application history information
    * 返回时间的历史提供的最后一次更新的应用历史信息
   *
   * @return 0 if this is undefined or unsupported, otherwise the last updated time in millis
    *         如果这是不确定的或不支持的返回0，否则最后更新时间毫秒数
   */
  def getLastUpdatedTime(): Long = {
    0
  }

  /**
   * Returns a list of applications available for the history server to show.
   *  返回一个列表，用于显示应用程序服务器历史。
   * @return List of all know applications.       所有已知应用程序的列表。
   */
  def getListing(): Iterator[ApplicationHistoryInfo]

  /**
   * Returns the Spark UI for a specific application.     为指定的应用程序返回Spark UI。
   *
   * @param appId The application ID.
   * @param attemptId The application attempt ID (or None if there is no attempt ID).
   * @return a [[LoadedAppUI]] instance containing the application's UI and any state information
   *         for update probes, or `None` if the application/attempt is not found.
   */
  def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI]

  /**
   * Called when the server is shutting down.
    *  当服务关闭的时候，调用
   */
  def stop(): Unit = { }

  /**
   * Returns configuration data to be shown in the History Server home page.
    *
    * 返回的配置数据是在服务器历史首页显示。
   *
   * @return A map with the configuration data. Data is show in the order returned by the map.
   */
  def getConfig(): Map[String, String] = Map()

  /**
   * Writes out the event logs to the output stream provided. The logs will be compressed into a
   * single zip file and written out.
    * 写入事件日志以提供输出流。日志将被压缩成一个zip文件写出来。
   * @throws SparkException if the logs for the app id cannot be found.
   */
  @throws(classOf[SparkException])
  def writeEventLogs(appId: String, attemptId: Option[String], zipStream: ZipOutputStream): Unit

  /**
   * @return the [[ApplicationHistoryInfo]] for the appId if it exists.
   */
  def getApplicationInfo(appId: String): Option[ApplicationHistoryInfo]

  /**
   * @return html text to display when the application list is empty
    *         应用列表为空时显示HTML文本
   */
  def getEmptyListingHtml(): Seq[Node] = Seq.empty
}
