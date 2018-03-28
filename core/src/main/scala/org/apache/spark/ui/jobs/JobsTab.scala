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

package org.apache.spark.ui.jobs

import javax.servlet.http.HttpServletRequest

import org.apache.spark.scheduler.SchedulingMode
import org.apache.spark.ui.{SparkUI, SparkUITab, UIUtils}

/** Web UI showing progress status of all jobs in the given SparkContext.
  * Web UI在给定的SparkContext中显示所有作业的进度状态。
  *
  *   SparkUI究竟是如何实现页面布局及其展示的？JobsTab展示所有的Job的进度，状态信息，
  *   这里我们以它为例来说明。JobsTab会复用SparkUI的killEnageled,SparkContext,jobProgressListener,
  *   包括AllJobsPage和JobPage两个页面。
  *
  *   AllJobsPage由Render方法渲染，利用jobProgressListener中统计监控数据生成，激活，完成，失败等状态
  *   的Job摘要信息，并且调用jobsTables方法生成表格等html元素，最终使用UIUtils的headerSparkPage分装好
  *   css,js，header及页面布局等。
  * */
private[ui] class JobsTab(parent: SparkUI) extends SparkUITab(parent, "jobs") {
  val sc = parent.sc
  val killEnabled = parent.killEnabled
  val jobProgresslistener = parent.jobProgressListener
  val executorListener = parent.executorsListener
  val operationGraphListener = parent.operationGraphListener

  def isFairScheduler: Boolean =
    jobProgresslistener.schedulingMode == Some(SchedulingMode.FAIR)

  def getSparkUser: String = parent.getSparkUser

  attachPage(new AllJobsPage(this))
  attachPage(new JobPage(this))

  def handleKillRequest(request: HttpServletRequest): Unit = {
    if (killEnabled && parent.securityManager.checkModifyPermissions(request.getRemoteUser)) {
      // stripXSS is called first to remove suspicious characters used in XSS attacks
      val jobId = Option(UIUtils.stripXSS(request.getParameter("id"))).map(_.toInt)
      jobId.foreach { id =>
        if (jobProgresslistener.activeJobs.contains(id)) {
          sc.foreach(_.cancelJob(id))
          // Do a quick pause here to give Spark time to kill the job so it shows up as
          // killed after the refresh. Note that this will block the serving thread so the
          // time should be limited in duration.
          Thread.sleep(100)
        }
      }
    }
  }
}
