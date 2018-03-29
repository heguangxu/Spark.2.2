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

import java.nio.ByteBuffer
import java.util.{Locale, Timer, TimerTask}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.Set
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, ThreadUtils, Utils}

/**
 * Schedules tasks for multiple types of clusters by acting through a SchedulerBackend.
 * It can also work with a local setup by using a `LocalSchedulerBackend` and setting
 * isLocal to true. It handles common logic, like determining a scheduling order across jobs, waking
 * up to launch speculative tasks, etc.
  *
  * 通过SchedulerBackend来调度多个类型集群的任务。
  * 它还可以通过使用“LocalSchedulerBackend”和设置isLocal为true来处理本地设置。它处理一般的逻辑，比如决定一个任务的调度顺序，唤醒来启动speculative任务等。
 *
 * Clients should first call initialize() and start(), then submit task sets through the
 * runTasks method.
  * 客户端应该首先调用initialize()和start()，然后通过runTasks方法提交任务集。
 *
 * THREADING: [[SchedulerBackend]]s and task-submitting clients can call this class from multiple
 * threads, so it needs locks in public API methods to maintain its state. In addition, some
 * [[SchedulerBackend]]s synchronize on themselves when they want to send events here, and then
 * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
 * we are holding a lock on ourselves.
  *
  *
  * TaskSchedulerImpl的构造过程如下：
  *   1.从SparkConf中读取配置信息，包括每个人物分配的CPU，调度模式（调度模式有FAIR和FIFO两种，默认为FIFO，可以
  *     修改属性spark.scheduler.mode来改变）等。
  *   2.创建TaskResultGetter，它的作用是通过线程池（Executors.newFixedThreadPoll创建的，默认4个线程，线程名字以
  *     task-result-getter开头，线程工厂默认是Executors.defaultThreadFactory)对Worker上的Executor发送给Task的
  *     执行结果进行处理。EXECUTOR
  *
  *  TaskSchedulerImpl的调度模式有FAIR和FIFO两种。任务的最终调度实际都是落实道接口SchedulerBackend的具体实现上。
  *  为了方便解析，我们先来看看local模式中SchedulerBackend的实现LocallBackend。LocalBackend依赖于LocalActor
  *  与ActorSystem进行消息通讯。
 */
private[spark] class TaskSchedulerImpl private[scheduler](
    val sc: SparkContext,
    val maxTaskFailures: Int,
    private[scheduler] val blacklistTrackerOpt: Option[BlacklistTracker],
    isLocal: Boolean = false)
  extends TaskScheduler with Logging {

  // BlacklistTracker:黑名单，一些尝试(或者失败)次数超过的task，executor,Stage列表

  import TaskSchedulerImpl._

  def this(sc: SparkContext) = {
    this(
      sc,
      sc.conf.get(config.MAX_TASK_FAILURES),
      TaskSchedulerImpl.maybeCreateBlacklistTracker(sc))
  }

  def this(sc: SparkContext, maxTaskFailures: Int, isLocal: Boolean) = {
    this(
      sc,
      maxTaskFailures,
      TaskSchedulerImpl.maybeCreateBlacklistTracker(sc),
      isLocal = isLocal)
  }

  val conf = sc.conf

  // How often to check for speculative tasks  多久检查一次推测任务
  val SPECULATION_INTERVAL_MS = conf.getTimeAsMs("spark.speculation.interval", "100ms")

  // Duplicate copies of a task will only be launched if the original copy has been running for
  // at least this amount of time. This is to avoid the overhead of launching speculative copies
  // of tasks that are very short.
  // 只有在原始副本至少运行了这么多时间的情况下，才会启动任务的副本。这是为了避免产生非常短的任务的推测性副本的开销。
  val MIN_TIME_TO_SPECULATION = 100

  private val speculationScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("task-scheduler-speculation")

  // Threshold above which we warn user initial TaskSet may be starved
  // 我们警告用户初始任务集的阈值可能会被饿死
  val STARVATION_TIMEOUT_MS = conf.getTimeAsMs("spark.starvation.timeout", "15s")

  // CPUs to request per task  每个task分配的CPU数目
  val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)

  // TaskSetManagers are not thread safe, so any access to one should be synchronized
  // on this class.
  // TaskSetManagers不是线程安全的，所以在这个类中访问应该同步
  private val taskSetsByStageIdAndAttempt = new HashMap[Int, HashMap[Int, TaskSetManager]]

  // Protected by `this`
  private[scheduler] val taskIdToTaskSetManager = new HashMap[Long, TaskSetManager]
  val taskIdToExecutorId = new HashMap[Long, String]

  @volatile private var hasReceivedTask = false
  @volatile private var hasLaunchedTask = false
  private val starvationTimer = new Timer(true)

  // Incrementing task IDs
  val nextTaskId = new AtomicLong(0)

  // IDs of the tasks running on each executor
  private val executorIdToRunningTaskIds = new HashMap[String, HashSet[Long]]

  def runningTasksByExecutors: Map[String, Int] = synchronized {
    executorIdToRunningTaskIds.toMap.mapValues(_.size)
  }

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  // 我们在每个主机上所设置的执行器集合;这用于计算hostsAlive，它反过来用于决定何时可以在给定主机上获得数据位置
  protected val hostToExecutors = new HashMap[String, HashSet[String]]

  protected val hostsByRack = new HashMap[String, HashSet[String]]

  protected val executorIdToHost = new HashMap[String, String]

  // Listener object to pass upcalls into 侦听器对象将向上调用传入。
  var dagScheduler: DAGScheduler = null

  var backend: SchedulerBackend = null

  // map输出跟踪器
  val mapOutputTracker = SparkEnv.get.mapOutputTracker

  // 构建可调度树
  private var schedulableBuilder: SchedulableBuilder = null

  // default scheduler is FIFO  scheduler默认的调度器是FIFO
  private val schedulingModeConf = conf.get(SCHEDULER_MODE_PROPERTY, SchedulingMode.FIFO.toString)
  // 调度模式
  val schedulingMode: SchedulingMode =
    try {
      SchedulingMode.withName(schedulingModeConf.toUpperCase(Locale.ROOT))
    } catch {
      case e: java.util.NoSuchElementException =>
        throw new SparkException(s"Unrecognized $SCHEDULER_MODE_PROPERTY: $schedulingModeConf")
    }

  // 一个可调度的实体，表示Pools或TaskSetManagers的集合,这里传入了调度模式
  val rootPool: Pool = new Pool("", schedulingMode, 0, 0)

  // This is a var so that we can reset it for testing purposes.
  // 这是一个var 变量，以便我们可以重置它用于测试。
  // 运行一个线程池，该线程池对任务结果进行反序列化和远程提取(如果需要)。
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  // 给taskScheduler设置DAGScheduler，在SparkContext中调用
  override def setDAGScheduler(dagScheduler: DAGScheduler) {
    this.dagScheduler = dagScheduler
  }

  /**
    * 由于FIFO调度比较简单，就是根据谁先进来谁先提交的顺序调度的，在此不过多阐述，这里主要阐述FAIR调度的原理。
      FAIR调度主要由1个default pool和用户配置的其它pool组成， 其中default pool里面的调度模式为FIFO，
      默认提交到default pool， 如果设置了spark.scheduler.pool的属性，则提交到对应的pool里面，
      pool和pool之间的调度模式为FAIR。按照之前的配置的pool1和pool2来说的话， 系统会存在3个pool，
      分别为：default pool，pool1，pool2， pool之间调度采用的是FAIR， pool内部采用的调度模式为：
      default为FIFO，pool1和pool2为FAIR。先看观察下TaskSchedulerImpl的初始化过程：

      创建完TaskSchedulerImpl和LocalBackend后，对TaskSchedulerImpl调用initialize进行初始化。以默认的FIFO
      调度为例，TaskSchedulerImpl的初始化过程如下：
        1.使用TaskSchedulerImpl持有LocalBackend的引用。
        2.创建Pool,Pool中缓存了调度队列，调度算法及TaskSetManager集合等信息。
        3.创建FIFISchedulableBuilder,FIFOSchedulableBuilder来操作Pool中的调度队列。

      问题：这个方法是谁调用的？什么时候调用的？
    */
  def initialize(backend: SchedulerBackend) {
    this.backend = backend
    // 构建可调度树 主节点和叶子节点（感觉像二叉树）
    schedulableBuilder = {
      schedulingMode match {
        //如果spark.scheduler.mode为FIFO，则就存在一个没有名字的rootPool，schedulingMode为FIFO，这里不多阐述
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          // 创建Fair调度
          new FairSchedulableBuilder(rootPool, conf)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported $SCHEDULER_MODE_PROPERTY: " +
          s"$schedulingMode")
      }
    }
    // buildPools: build the tree nodes(pools)
    schedulableBuilder.buildPools()
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  /**
    * 这个方法在SparkContext中TaskScheduler创建完成后就被SparkContext调用的
    */
  override def start() {

    // 这一点
    // local模式 ： 调用的是LocalSchedulerBackend.start()方法，里面没有
    // StandLone模式：调用的是StandaloneSchedulerBackend.start()方法，
    backend.start()

    /**
      * TaskScheduleImpl的初始化和启动是在SparkConext中，进行的，初始化的时候会
      * 传入SparkDeploySchedulerBackend对象。启动则直接调用start方法。在Start
      * 方法中，会判断是否启动任务的推测执行，由spark.speculation属性指定，默认不执行。
      * 问题：推测执行，是什么？
      *
      *     推测执行(Speculative Execution)是指在分布式集群环境下，因为程序BUG，
      *   负载不均衡或者资源分布不均等原因，造成同一个job的多个task运行速度不一致，
      *   有的task运行速度明显慢于其他task（比如：一个job的某个task进度只有10%，
      *   而其他所有task已经运行完毕），则这些task拖慢了作业的整体执行进度，为了避
      *   免这种情况发生，Hadoop会为该task启动备份任务，让该speculative task与
      *   原始task同时处理一份数据，哪个先运行完，则将谁的结果作为最终结果。
      *
      *     推测执行优化机制采用了典型的以空间换时间的优化策略，它同时启动多个相同task
      *   （备份任务）处理相同的数据块，哪个完成的早，则采用哪个task的结果，这样可防止
      *   拖后腿Task任务出现，进而提高作业计算速度，但是，这样却会占用更多的资源，在集
      *   群资源紧缺的情况下，设计合理的推测执行机制可在多用少量资源情况下，减少大作业的
      *   计算时间。
      *
      *   参考博客：https://blog.csdn.net/qq_21383435/article/details/79749459
      */
    if (!isLocal && conf.getBoolean("spark.speculation", false)) {
      logInfo("Starting speculative execution thread")
      speculationScheduler.scheduleWithFixedDelay(new Runnable {
        override def run(): Unit = Utils.tryOrStopSparkContext(sc) {
          // 检查我们所有活跃的job中是否有可推测的任务。
          checkSpeculatableTasks()
        }
      }, SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
  }

  override def postStartHook() {
    waitBackendReady()
  }

  /**
    *   submitTasks位于TaskSchedulerImpl：
    *   submitTasks主要分为以下几个步骤：
    *     1.构建任务集管理器。既将TaskScheduler，TaskSet及最大失败次数（maxTaskFailures）封装为TaskSetManager.
    *     2.设置任务集调度策略（调度模式有FAIR和FIFO两种，此处默认为FIFO为例）。将TaskSetManager添加到FIFOSchedulable中。
    *     3.资源分配。调用LocalBackend的reviveOffers方法，实际向localActor发送ReviveOffers消息。localActor对ReviveOffers消息的匹配
    *       执行reviveOffers方法。
    */
  override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    this.synchronized {
      // 生成一个TaskSetManager类型对象，
      // task最大重试次数，由参数spark.task.maxFailures设置，默认为4
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      val stage = taskSet.stageId
      // key为stageId，value为一个HashMap，这个HashMap中的key为stageAttemptId，value为TaskSetManager对象
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
      stageTaskSets(taskSet.stageAttemptId) = manager
      // 如果当前这个stageId对应的HashMap[Int, TaskSetManager]中存在某个taskSet
      // 使得当前的taskSet和这个taskSet不是同一个，并且当前这个TaskSetManager不是zombie进程
      // 即对于同一个stageId，如果当前这个TaskSetManager不是zombie进程，即其中的tasks需要运行，
      // 并且对当前stageId，有两个不同的taskSet在运行
      // 那么就应该抛出异常，确保同一个Stage在正常运行情况下不能有两个taskSet在运行
      val conflictingTaskSet = stageTaskSets.exists { case (_, ts) =>
        ts.taskSet != taskSet && !ts.isZombie
      }
      if (conflictingTaskSet) {
        throw new IllegalStateException(s"more than one active taskSet for stage $stage:" +
          s" ${stageTaskSets.toSeq.map{_._2.taskSet.id}.mkString(",")}")
      }

      //添加到资源池,task的调度下一节讲解
      // 根据调度模式生成FIFOSchedulableBuilder或者FairSchedulableBuilder，将当前的TaskSetManager提交到调度池中
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

      if (!isLocal && !hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run() {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
      }
      hasReceivedTask = true
    }
    //通过CoarseGrainedSchedulerBackend将task发往各个work节点的CoarseGrainedExecutorBackend进程
    // 向schedulerBackend申请资源
    // 这一点有问题，backend在哪里初始化的？又是在哪里被调用的？
    // 这里需要看你的backend是什么模式运行，如果是local就是LocalSchedulerBackend，但是最终调用的是LocalEndpoint.reviveOffers()里面的方法。
    /** 申请资源
      *  master("local") ===> LocalSchedulerBackend.reviveOffers() ===> LocalEndpoint.reviveOffers() ==> TaskSchedulerImpl.resourceOffers()
      */
    backend.reviveOffers()
  }

  // Label as private[scheduler] to allow tests to swap in different task set managers if necessary
  private[scheduler] def createTaskSetManager(
      taskSet: TaskSet,
      maxTaskFailures: Int): TaskSetManager = {
    new TaskSetManager(this, taskSet, maxTaskFailures, blacklistTrackerOpt)
  }

  /**
    * 取消task任务
    */
  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    // 获取这个Stage的Id，然后遍历他的task任务，逐个杀死
    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        // There are two possible cases here:
        // 1. The task set manager has been created and some tasks have been scheduled.
        //    In this case, send a kill signal to the executors to kill the task and then abort
        //    the stage.
        // 2. The task set manager has been created but no tasks has been scheduled. In this case,
        //    simply abort the stage.
        /**
          * 这里有两种情况：
          *   1.task任务的TaskSetManager已经创建了，而且一些task任务已经开始调度了。在这种情况下，
          *     发送一个kill信号给执行器executors，以杀死任务，然后中止stage。
          *   2.task任务的TaskSetManager已经创建了，但是task任务还没有调度。在这种情况下，中止stage就好了。
          */
        tsm.runningTasksSet.foreach { tid =>
          // 遍历这个Stage对应的TaskSet集合对应的执行器executor，获取ID
          val execId = taskIdToExecutorId(tid)
          // 逐个杀死每个任务
          backend.killTask(tid, execId, interruptThread, reason = "stage cancelled")
        }
        // 终止Stage
        tsm.abort("Stage %s cancelled".format(stageId))
        logInfo("Stage %d was cancelled".format(stageId))
      }
    }
  }

  override def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean = {
    logInfo(s"Killing task $taskId: $reason")
    val execId = taskIdToExecutorId.get(taskId)
    if (execId.isDefined) {
      backend.killTask(taskId, execId.get, interruptThread, reason)
      true
    } else {
      logWarning(s"Could not kill task $taskId because no task with that ID was found.")
      false
    }
  }

  /**
   * Called to indicate that all task attempts (including speculated tasks) associated with the
   * given TaskSetManager have completed, so state associated with the TaskSetManager should be
   * cleaned up.
   */
  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    taskSetsByStageIdAndAttempt.get(manager.taskSet.stageId).foreach { taskSetsForStage =>
      taskSetsForStage -= manager.taskSet.stageAttemptId
      if (taskSetsForStage.isEmpty) {
        taskSetsByStageIdAndAttempt -= manager.taskSet.stageId
      }
    }
    manager.parent.removeSchedulable(manager)
    logInfo(s"Removed TaskSet ${manager.taskSet.id}, whose tasks have all completed, from pool" +
      s" ${manager.parent.name}")
  }

  /**
    * 这个方法主要是在分配的executor资源上，执行taskSet中包含的所有task。首先遍历分配到的executor，
    * 如果当前executor中的cores个数满足配置的单个task需要的core数要求(该core数由参数spark.task.cpus
    * 确定，默认值为1)，才能在该executor上启动task。
    *
    * //为每一个TaskSetManager分配资源
    * */
  private def resourceOfferSingleTaskSet(
      taskSet: TaskSetManager,
      maxLocality: TaskLocality,
      shuffledOffers: Seq[WorkerOffer],
      availableCpus: Array[Int],
      tasks: IndexedSeq[ArrayBuffer[TaskDescription]]) : Boolean = {
    var launchedTask = false
    // nodes and executors that are blacklisted for the entire application have already been
    // filtered out by this point
    for (i <- 0 until shuffledOffers.size) {     // 顺序遍历当前存在的Executor
      val execId = shuffledOffers(i).executorId  // Executor ID
      val host = shuffledOffers(i).host          // Executor所在的host
      if (availableCpus(i) >= CPUS_PER_TASK) {   // 如果当前executor上的core数满足配置的单个task的core数要求
        try {
          // 真正调用的是TaskSetManager.resourceOffer方法
          for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {  // 为当前stage分配一个executor
            tasks(i) += task
            val tid = task.taskId
            taskIdToTaskSetManager(tid) = taskSet // 存储该task->taskSet的映射关系
            taskIdToExecutorId(tid) = execId      // 存储该task分配到的executorId
            executorIdToRunningTaskIds(execId).add(tid)
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            launchedTask = true
          }
        } catch {
          case e: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
            // Do not offer resources for this task, but don't throw an error to allow other
            // task sets to be submitted.
            return launchedTask
        }
      }
    }
    return launchedTask
  }

  /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
    *
    * 由集群管理器调用，从slaves节点上提供资源。我们的反应是，按优先顺序要求我们的活动任务设置为任务。我们以循环方式填充每个节点，
    * 以便任务在集群之间实现平衡。
    *
    * 由cluster manager来调用，为task分配节点上的资源。
    * 根据优先级为task分配资源，
    * 采用round-robin方式使task均匀分布到集群的各个节点上。
    *
    * // TaskSchedulerImpl.resourceOffers方法被cluster manager调用，传递的参数offers表示worker提供的资源，该方法根据资源情况，
    * 结合待执行任务的优先级，将任务平衡的分配给executors
    *
    * 处理步骤：
    *   1、标记Executor与host的关系，增加激活的Executor的id，按照host对executor分组。并且向DAGSchedulerEventProcessEcdpoint
    *     发送ExecutorAdded事件。
    *   2、计算资源的分配与计算，对所有WorkerOffer随机洗牌，避免将任务总是分配给同样的WorkerOffer。
    *   3.根据每个WorkerOffer的可用的CPU核数创建同等尺寸的任务描述（TaskDescription）数组。
    *   4.将每个Workeroffer的可用的CPU核数统计到可用CPU（availableCpus）数组咋哄。
    *   5.对rootPool中所有的TaskSetManager按照调度算法排序，比如FIFO先进先出排序
    *   6.调用每个TaskSetManager的resourceOffer方法，根据WorkerOffer的ExecutorId和host找到需要执行任务并且进一步进行资源处理。
    *   7.任务分配到响应的host和executor后，将taskId与TaskSetId的关系，taskId与ExecutorId的关系，executors与Host的分组关系等更新并且
    *     将availableCpus数目减去每个任务分配的CPU核数（CPU_PER_TASK）
    *   8.返回第三部生成TaskDescription列表。
    */
  def resourceOffers(offers: IndexedSeq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    // Mark each slave as alive and remember its hostname
    // Also track if new executor is added
    // 每个slave节点为alive并且记录其hostname
    // 如果有新的slave节点加入，对其进行追踪。 // 激活所有slave节点，记录其hostname，并检查是否有新的executor加入
    var newExecAvail = false
    for (o <- offers) {
      if (!hostToExecutors.contains(o.host)) {
        hostToExecutors(o.host) = new HashSet[String]()
      }
      if (!executorIdToRunningTaskIds.contains(o.executorId)) {
        hostToExecutors(o.host) += o.executorId
        executorAdded(o.executorId, o.host)
        executorIdToHost(o.executorId) = o.host
        executorIdToRunningTaskIds(o.executorId) = HashSet[Long]()
        newExecAvail = true
      }
      for (rack <- getRackForHost(o.host)) {
        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
      }
    }

    // Before making any offers, remove any nodes from the blacklist whose blacklist has expired. Do
    // this here to avoid a separate thread and added synchronization overhead, and also because
    // updating the blacklist is only relevant when task offers are being made.
    // 在发出任何offers之前，删除黑名单中已过期的任何节点。这样做是为了避免单独的线程和增加的同步开销，而且因为更新黑名单只在任务提供时才相关。
    blacklistTrackerOpt.foreach(_.applyBlacklistTimeout())

    val filteredOffers = blacklistTrackerOpt.map { blacklistTracker =>
      offers.filter { offer =>
        !blacklistTracker.isNodeBlacklisted(offer.host) &&
          !blacklistTracker.isExecutorBlacklisted(offer.executorId)
      }
    }.getOrElse(offers)

    // 为避免多个Task集中分配到某些机器上，对这些Task进行随机打散.
    val shuffledOffers = shuffleOffers(filteredOffers)

    // Build a list of tasks to assign to each worker.
    //存储分配好资源的task // 构建task序列，以分配到worker
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
    val availableCpus = shuffledOffers.map(o => o.cores).toArray

    // 从调度池中获取排好序的TaskSetManager，由调度池确定TaskSet的执行顺序
    // 按优先级排序的TaskSetManager序列，任务优先级是由Pool的调度模式(FIFO/FAIR)决定的
    val sortedTaskSets = rootPool.getSortedTaskSetQueue

    // 按顺序取出各taskSet
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      // 如果该executor是新分配来的
      if (newExecAvail) {
        // 重新计算TaskSetManager的就近原则
        taskSet.executorAdded()
      }
    }

    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    // 将每个任务集放到我们的调度顺序中，然后为每个节点提供递增位置级别的命令，这样它就有机会在所有这些节点上启动本地任务。
    //
    // 注意:preferredlocal命令:PROCESS_LOCAL,NODE_LOCAL,NO_PREF,RACK_LOCAL,ANY
    for (taskSet <- sortedTaskSets) {
      // 为从rootPool中获得的TaskSetManager列表分配资源。就近顺序是：
      // PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
      var launchedAnyTask = false
      var launchedTaskAtCurrentMaxLocality = false
      // 对每一个taskSet，按照就近顺序分配最近的executor来执行task
      for (currentMaxLocality <- taskSet.myLocalityLevels) {
        do {
          // 将前面随机打散的WorkOffers计算资源按照就近原则分配给taskSet，用于执行其中的task
          launchedTaskAtCurrentMaxLocality = resourceOfferSingleTaskSet(
            taskSet, currentMaxLocality, shuffledOffers, availableCpus, tasks)
          launchedAnyTask |= launchedTaskAtCurrentMaxLocality
        } while (launchedTaskAtCurrentMaxLocality)
      }
      if (!launchedAnyTask) {
        taskSet.abortIfCompletelyBlacklisted(hostToExecutors)
      }
    }

    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    return tasks
  }

  /**
   * Shuffle offers around to avoid always placing tasks on the same workers.  Exposed to allow
   * overriding in tests, so it can be deterministic.
   */
  protected def shuffleOffers(offers: IndexedSeq[WorkerOffer]): IndexedSeq[WorkerOffer] = {
    Random.shuffle(offers)
  }

  /**
    * 执行结果的处理
    *     任务完成的时候，会发送一次statusUpdate消息，LocalActor会先匹配执行TaskSchedulerImpl的statusUpdate
    *   方法，然后调用reveiveOffers方法调用其他任务。
    *     TaskSchedulerImpl的statusUpdate方法会从taskIdToTaskSetId,taskidToExecutorId中移除此任务，并且调用
    *   taskResultGetter的enqueueSuccessfulTask方法。
    *     taskResultGetter的enqueueSuccessfulTask和enqueueFailedTask方法，分别用于处理执行成功任务的返回结果
    *    和执行失败任务的返回结果
    *
    *
    * 在这个方法中，主要分析完成状态和失败状态的Task后续处理流程的入口。
    */
  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    var failedExecutor: Option[String] = None
    var reason: Option[ExecutorLossReason] = None
    synchronized {
      try {
        // taskIdToTaskSetManager在1.5版本中叫taskIdToTaskSetId
        taskIdToTaskSetManager.get(tid) match {
          case Some(taskSet) =>
            if (state == TaskState.LOST) {
              // TaskState.LOST is only used by the deprecated Mesos fine-grained scheduling mode,
              // where each executor corresponds to a single task, so mark the executor as failed.
              val execId = taskIdToExecutorId.getOrElse(tid, throw new IllegalStateException(
                "taskIdToTaskSetManager.contains(tid) <=> taskIdToExecutorId.contains(tid)"))
              if (executorIdToRunningTaskIds.contains(execId)) {
                reason = Some(
                  SlaveLost(s"Task $tid was lost, so marking the executor as lost as well."))
                removeExecutor(execId, reason.get)
                failedExecutor = Some(execId)
              }
            }

            /**
              * task任务完成处理
              */
            if (TaskState.isFinished(state)) {
              // 清理TaskScheduler的状态以跟踪给定的任务。
              cleanupTaskState(tid)
              // 如果给定的任务ID处于运行任务的集合中，则删除它。
              taskSet.removeRunningTask(tid)
              if (state == TaskState.FINISHED) {
                /**
                    taskResultGetter的enqueueSuccessfulTask和enqueueFailedTask方法，分别用于处理之子女给成功任务的返回结果和执行失败的
                    返回结果。
                  */
                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
              }
            }
          case None =>
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
                "likely the result of receiving duplicate task finished status updates) or its " +
                "executor has been marked as failed.")
                .format(state, tid))
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
    if (failedExecutor.isDefined) {
      assert(reason.isDefined)
      dagScheduler.executorLost(failedExecutor.get, reason.get)
      backend.reviveOffers()
    }
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
    *
    *
    * 更新正在执行的任务的度量标准，并让master知道BlockManager仍然是活的。如果驱动程序driver知道给定的块管理器block manager，
    * 则返回true。否则，返回false，指示区块管理器block manager应该重新注册。
    *
    * 这段代码通过遍历taskMetrics,一句taskIdToTaskSetId和activeTaskSets找到TaskSetManager，然后将taskId，TaskSetManager.stageId,
    * TaskSetManager.taskSet.attempt,TaskMetrics分装到类型Array[(Long,Int,Int,TaskMetrics)]的数组metricsWithStageIds中。
   */
  override def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId): Boolean = {
    // (taskId, stageId, stageAttemptId, accumUpdates)
    val accumUpdatesWithTaskIds: Array[(Long, Int, Int, Seq[AccumulableInfo])] = synchronized {
      accumUpdates.flatMap { case (id, updates) =>
        val accInfos = updates.map(acc => acc.toInfo(Some(acc.value), None))
        taskIdToTaskSetManager.get(id).map { taskSetMgr =>
          (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, accInfos)
        }
      }
    }
    // 最后调用了dagScheduler的executorHeartbeatReceived方法
    dagScheduler.executorHeartbeatReceived(execId, accumUpdatesWithTaskIds, blockManagerId)
  }

  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit = synchronized {
    taskSetManager.handleTaskGettingResult(tid)
  }

  def handleSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskResult: DirectTaskResult[_]): Unit = synchronized {
    // 对TaskSet中的任务信息进行成功状态标记，然后调用DAGScheduler的taskEnded方法
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  def handleFailedTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskState: TaskState,
      reason: TaskFailedReason): Unit = synchronized {
    // 调用TaskSetManager处理失败的情况。
    taskSetManager.handleFailedTask(tid, taskState, reason)
    if (!taskSetManager.isZombie && !taskSetManager.someAttemptSucceeded(tid)) {
      //这里需要重新进行资源调度来执行失败的Task，而失败的Task的状态(例如执行失败次数
      //等)已由TaskManager进行了更新，来反应该任务是失败后重新执行的Task
      // Need to revive offers again now that the task set manager state has been updated to
      // reflect failed tasks that need to be re-run.
      backend.reviveOffers()
    }
  }

  def error(message: String) {
    synchronized {
      if (taskSetsByStageIdAndAttempt.nonEmpty) {
        // Have each task set throw a SparkException with the error
        for {
          attempts <- taskSetsByStageIdAndAttempt.values
          manager <- attempts.values
        } {
          try {
            manager.abort(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        throw new SparkException(s"Exiting due to error from cluster scheduler: $message")
      }
    }
  }

  // SparkContext.stop() ==> DagScheduler.stop() ==> TaskSchedulerImpl.stop()
  override def stop() {
    speculationScheduler.shutdown()
    if (backend != null) {
      // org.apache.spark.executor.CoarseGrainedExecutorBackend.stop()
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
    starvationTimer.cancel()
  }

  override def defaultParallelism(): Int = backend.defaultParallelism()

  /** Check for speculatable tasks in all our active jobs.
      // 检查我们所有活跃的job中是否有可推测的任务。
      若开启则会启动一个线程每隔`SPECULATION_INTERVAL_MS`（默认100ms，可通过
      `spark.speculation.interval`属性设置）通过`checkSpeculatableTasks`
      方法检测是否有需要推测式执行的tasks：
    */
  def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      /**
        * 然后又通过rootPool的方法判断是否有需要推测式执行的tasks，若有则会调用
        * SchedulerBackend的reviveOffers去尝试拿资源运行推测任务。
        */
      shouldRevive = rootPool.checkSpeculatableTasks(MIN_TIME_TO_SPECULATION)
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    var failedExecutor: Option[String] = None

    synchronized {
      if (executorIdToRunningTaskIds.contains(executorId)) {
        val hostPort = executorIdToHost(executorId)
        logExecutorLoss(executorId, hostPort, reason)
        removeExecutor(executorId, reason)
        failedExecutor = Some(executorId)
      } else {
        executorIdToHost.get(executorId) match {
          case Some(hostPort) =>
            // If the host mapping still exists, it means we don't know the loss reason for the
            // executor. So call removeExecutor() to update tasks running on that executor when
            // the real loss reason is finally known.
            logExecutorLoss(executorId, hostPort, reason)
            removeExecutor(executorId, reason)

          case None =>
            // We may get multiple executorLost() calls with different loss reasons. For example,
            // one may be triggered by a dropped connection from the slave while another may be a
            // report of executor termination from Mesos. We produce log messages for both so we
            // eventually report the termination reason.
            logError(s"Lost an executor $executorId (already removed): $reason")
        }
      }
    }
    // Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get, reason)
      backend.reviveOffers()
    }
  }

  private def logExecutorLoss(
      executorId: String,
      hostPort: String,
      reason: ExecutorLossReason): Unit = reason match {
    case LossReasonPending =>
      logDebug(s"Executor $executorId on $hostPort lost, but reason not yet known.")
    case ExecutorKilled =>
      logInfo(s"Executor $executorId on $hostPort killed by driver.")
    case _ =>
      logError(s"Lost executor $executorId on $hostPort: $reason")
  }

  /**
   * Cleans up the TaskScheduler's state for tracking the given task.
    * 清理TaskScheduler的状态以跟踪给定的任务。
   */
  private def cleanupTaskState(tid: Long): Unit = {
    taskIdToTaskSetManager.remove(tid)
    taskIdToExecutorId.remove(tid).foreach { executorId =>
      executorIdToRunningTaskIds.get(executorId).foreach { _.remove(tid) }
    }
  }

  /**
   * Remove an executor from all our data structures and mark it as lost. If the executor's loss
   * reason is not yet known, do not yet remove its association with its host nor update the status
   * of any running tasks, since the loss reason defines whether we'll fail those tasks.
   */
  private def removeExecutor(executorId: String, reason: ExecutorLossReason) {
    // The tasks on the lost executor may not send any more status updates (because the executor
    // has been lost), so they should be cleaned up here.
    executorIdToRunningTaskIds.remove(executorId).foreach { taskIds =>
      logDebug("Cleaning up TaskScheduler state for tasks " +
        s"${taskIds.mkString("[", ",", "]")} on failed executor $executorId")
      // We do not notify the TaskSetManager of the task failures because that will
      // happen below in the rootPool.executorLost() call.
      taskIds.foreach(cleanupTaskState)
    }

    val host = executorIdToHost(executorId)
    val execs = hostToExecutors.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      hostToExecutors -= host
      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
        hosts -= host
        if (hosts.isEmpty) {
          hostsByRack -= rack
        }
      }
    }

    if (reason != LossReasonPending) {
      executorIdToHost -= executorId
      rootPool.executorLost(executorId, host, reason)
    }
    blacklistTrackerOpt.foreach(_.handleRemovedExecutor(executorId))
  }

  def executorAdded(execId: String, host: String) {
    dagScheduler.executorAdded(execId, host)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    hostToExecutors.get(host).map(_.toSet)
  }

  /*
  其中executorsByHost为 HashMap[String, HashSet[String]] 类型，key 为 host，value 为该 host 上的 active executors
   */
  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    hostToExecutors.contains(host)
  }

  /*
  其中hostsByRack: HashMap[String, HashSet[String]]的 key 为 rack，value 为该 rack 上所有作为 taskSetManager 优先位置的 hosts
   */
  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.contains(rack)
  }

  /**
    * isExecutorAlive(_)干了什么？
    * activeExecutorIds: HashSet[String]保存集群当前所有可用的 executor id（这里对 executor 的
    * free cores 个数并没有要求，可为0），每当 DAGScheduler 提交 taskSet 会触发 TaskScheduler 调用
    * resourceOffers 方法，该方法会更新当前可用的 executors 至 activeExecutorIds；当有 executor lost
    * 的时候，TaskSchedulerImpl也会调用 removeExecutor 来将 lost 的executor 从 activeExecutorIds 中去除
    */
  def isExecutorAlive(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.contains(execId)
  }

  def isExecutorBusy(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.get(execId).exists(_.nonEmpty)
  }

  /**
   * Get a snapshot of the currently blacklisted nodes for the entire application.  This is
   * thread-safe -- it can be called without a lock on the TaskScheduler.
   */
  def nodeBlacklist(): scala.collection.immutable.Set[String] = {
    blacklistTrackerOpt.map(_.nodeBlacklist()).getOrElse(scala.collection.immutable.Set())
  }

  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None

  private def waitBackendReady(): Unit = {
    if (backend.isReady) {
      return
    }
    while (!backend.isReady) {
      // Might take a while for backend to be ready if it is waiting on resources.
      if (sc.stopped.get) {
        // For example: the master removes the application for some reason
        throw new IllegalStateException("Spark context stopped while waiting for backend")
      }
      synchronized {
        this.wait(100)
      }
    }
  }

  override def applicationId(): String = backend.applicationId()

  override def applicationAttemptId(): Option[String] = backend.applicationAttemptId()

  private[scheduler] def taskSetManagerForAttempt(
      stageId: Int,
      stageAttemptId: Int): Option[TaskSetManager] = {
    for {
      attempts <- taskSetsByStageIdAndAttempt.get(stageId)
      manager <- attempts.get(stageAttemptId)
    } yield {
      manager
    }
  }

}


private[spark] object TaskSchedulerImpl {

  val SCHEDULER_MODE_PROPERTY = "spark.scheduler.mode"

  /**
   * Used to balance containers across hosts.
   *
   * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
   * resource offers representing the order in which the offers should be used. The resource
   * offers are ordered such that we'll allocate one container on each host before allocating a
   * second container on any host, and so on, in order to reduce the damage if a host fails.
   *
   * For example, given {@literal <h1, [o1, o2, o3]>}, {@literal <h2, [o4]>} and
   * {@literal <h3, [o5, o6]>}, returns {@literal [o1, o5, o4, o2, o6, o3]}.
   */
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.getOrElse(key, null)
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size) {
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }

  /**
    * BlacklistTracker：的意思是黑名单追踪
    * @param sc
    * @return
    */
  private def maybeCreateBlacklistTracker(sc: SparkContext): Option[BlacklistTracker] = {
    // 是否启用了黑名单追踪，有参数spark.blacklist.enabled控制
    if (BlacklistTracker.isBlacklistEnabled(sc.conf)) {
      val executorAllocClient: Option[ExecutorAllocationClient] = sc.schedulerBackend match {
        case b: ExecutorAllocationClient => Some(b)
        case _ => None
      }
      Some(new BlacklistTracker(sc, executorAllocClient))
    } else {
      None
    }
  }

}
