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

import java.io.NotSerializableException
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.math.max
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulingMode._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.util.{AccumulatorV2, Clock, SystemClock, Utils}
import org.apache.spark.util.collection.MedianHeap

/**
 * Schedules the tasks within a single TaskSet in the TaskSchedulerImpl. This class keeps track of
 * each task, retries tasks if they fail (up to a limited number of times), and
 * handles locality-aware scheduling for this TaskSet via delay scheduling. The main interfaces
 * to it are resourceOffer, which asks the TaskSet whether it wants to run a task on one node,
 * and statusUpdate, which tells it that one of its tasks changed state (e.g. finished).
  *
  * 在TaskSchedulerImpl中的一个TaskSet中安排任务。这个类跟踪每一个任务，如果它们失败(次数有限)，重试任务，并通过延迟调度
  * 处理这个任务集的本地感知调度。它的主要接口是resourceOffer，它询问TaskSet是否想在一个节点上运行任务，以及statusUpdate，
  * 它告诉它它的一个任务改变了状态(如完成)。
  *
 *
 * THREADING: This class is designed to only be called from code with a lock on the
 * TaskScheduler (e.g. its event handlers). It should not be called from other threads.
  *
  * 线程化:这个类只被设计为从代码中调用，并锁在TaskScheduler上(例如，它的事件处理程序)。它不应该从其他线程调用。
 *
 * @param sched           the TaskSchedulerImpl associated with the TaskSetManager
 * @param taskSet         the TaskSet to manage scheduling for
 * @param maxTaskFailures if any particular task fails this number of times, the entire
 *                        task set will be aborted
  *
  *
  *    在 DAGScheduler 向 TaskScheduler 提交了 taskSet 之后，TaskSchedulerImpl 会为每个 taskSet 创建一个
  * TaskSetManager 对象，该对象包含taskSet 所有 tasks，并管理这些 tasks 的执行，其中就包括计算 taskSetManager
  * 中的 tasks 都有哪些locality levels，以便在调度和延迟调度 tasks 时发挥作用。
  *
  *
  * TaskSetManager的主要接口包括：
  *     ResourceOffer：根据TaskScheduler所提供的单个Resource资源包括host，executor和locality的要求返回一个合适的Task，
  *                   TaskSetManager内部会根据上一个任务的成功提交的时间，自动调整自身的Locality匹配策略，如果上一次成功
  *                   提交任务的时间间隔很长，则降低对Locality的要求（例如从最差要求Process Local降低为最差要求Node Local），
  *                   反之则提高对Locality的要求。这一动态调整Locality的策略为了提高任务在最佳Locality的情况下得到运行的机会
  *                   ，因为Resource资源是在短期内分批提供给TaskSetManager的，动态调整Locality门槛有助于改善整体的Locality
  *                   分布情况。
  * HandleSuccessfulTask/handleFailedTask/handleTaskGettingResult：用于更新任务的运行状态，TaskSetManager在这些函数中除了
  *                   更新自身维护的任务状态列表等信息，用于剩余的任务的调度以外，也会进一步调用DAGScheduler的函数接口将结果
  *                   通知给它。
  * 此外，TaskSetManager在调度任务时还可能进一步考虑Speculattion的情况，当某个任务的运行时间超过其他任务的运行时间的一个特定
  * 比例值时，该任务可能被重复调度。目的是为了防止某个运行中的Task由于有些特殊原因（例如所在节点CPU负载过高，IO带宽被占等）运
  * 行缓慢拖延了整个Stage的完成时间，Speculation同样需要根据集群和作业的实际情况合理配置。
  *
  * TaskSetManager负责当前TaskSet中所有的Task的启动，失败后的重试，本地化处理等。
 */
private[spark] class TaskSetManager(
    sched: TaskSchedulerImpl,  // sched：TaskScheduler实现类,可以有不同的实现类，这里默认为TaskSchedulerImpl
    val taskSet: TaskSet,  // DAGScheduler提交的任务集，一般和一个stage对应着，每一个task对应着一个RDD分区
    val maxTaskFailures: Int,   // 所允许 的最大失败次数
    blacklistTracker: Option[BlacklistTracker] = None,
    clock: Clock = new SystemClock()) extends Schedulable with Logging {

  private val conf = sched.sc.conf

  // Quantile of tasks at which to start speculation
  // 对于一些特殊的stage，task要完成多少百分比，才可以进行推测
  val SPECULATION_QUANTILE = conf.getDouble("spark.speculation.quantile", 0.75)
  // 一个任务比预测的中间值要慢多少倍才会推测
  val SPECULATION_MULTIPLIER = conf.getDouble("spark.speculation.multiplier", 1.5)

  // Limit of bytes for total size of results (default is 1GB)
  // task返回的最大的结果限制，默认是1G
  val maxResultSize = Utils.getMaxResultSize(conf)

  val speculationEnabled = conf.getBoolean("spark.speculation", false)

  // Serializer for closures and tasks. 用于闭包和任务的序列化器。
  val env = SparkEnv.get
  val ser = env.closureSerializer.newInstance()

  val tasks = taskSet.tasks
  val numTasks = tasks.length
  // 拷贝的正在运行的一个task数组
  val copiesRunning = new Array[Int](numTasks)

  // For each task, tracks whether a copy of the task has succeeded. A task will also be
  // marked as "succeeded" if it failed with a fetch failure, in which case it should not
  // be re-run because the missing map data needs to be regenerated first.
  // 存放成功的任务
  val successful = new Array[Boolean](numTasks)
  // 存放任务失败次数
  private val numFailures = new Array[Int](numTasks)

  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)
  // 成功任务数量
  private[scheduler] var tasksSuccessful = 0

  // 权重
  val weight = 1
  val minShare = 0
  // 任务集的优先级
  var priority = taskSet.priority
  // 任务集所在的stage
  var stageId = taskSet.stageId
  // TaskSetManager的名字
  val name = "TaskSet_" + taskSet.id
  var parent: Pool = null
  // 总结果大小
  private var totalResultSize = 0L
  // 计算过的任务数量
  private var calculatedTasks = 0

  private[scheduler] val taskSetBlacklistHelperOpt: Option[TaskSetBlacklist] = {
    blacklistTracker.map { _ =>
      new TaskSetBlacklist(conf, stageId, clock)
    }
  }

  // 正在运行的任务集 HashSet
  private[scheduler] val runningTasksSet = new HashSet[Long]

  // 正在运行的任务集的大小
  override def runningTasks: Int = runningTasksSet.size

  def someAttemptSucceeded(tid: Long): Boolean = {
    successful(taskInfos(tid).index)
  }

  // True once no more tasks should be launched for this task set manager. TaskSetManagers enter
  // the zombie state once at least one attempt of each task has completed successfully, or if the
  // task set is aborted (for example, because it was killed).  TaskSetManagers remain in the zombie
  // state until all tasks have finished running; we keep TaskSetManagers that are in the zombie
  // state in order to continue to track and account for the running tasks.
  // TODO: We should kill any running task attempts when the task set manager becomes a zombie.
  /**
    * isZombie：首先，定义TaskSetManager进入zombie状态——TaskSet中至少有一个task运行完成或者整个taskset被抛弃；
    *           zombie状态会一直保持到所有的task都执行完成；之所以让TaskSetManager处于zombie状态，是因为这时可以
    *           跟踪所有正在运行的task。
    */
  private[scheduler] var isZombie = false

  // Set of pending tasks for each executor. These collections are actually
  // treated as stacks, in which new tasks are added to the end of the
  // ArrayBuffer and removed from the end. This makes it faster to detect
  // tasks that repeatedly fail because whenever a task failed, it is put
  // back at the head of the stack. These collections may contain duplicates
  // for two reasons:
  // (1): Tasks are only removed lazily; when a task is launched, it remains
  // in all the pending lists except the one that it was launched from.
  // (2): Tasks may be re-added to these lists multiple times as a result
  // of failures.
  // Duplicates are handled in dequeueTaskFromList, which ensures that a
  // task hasn't already started running before launching it.
  /**
    *
    * 为每个执行器设置未决任务。这些集合实际上被当作栈来处理，其中新任务被添加到ArrayBuffer的末尾，并从末尾删除。
    * 这使得检测多次失败的任务变得更快，因为每当任务失败时，它就被放回堆栈的头部。这些收藏可能有两个原因:
    * (1):任务只被延迟删除;当一个任务启动时，它仍然保留在所有未决的列表中，除了它被启动的列表之外。
    * (2):由于失败，任务可能多次被重新添加到这些列表中。
    * 副本在dequeueTaskFromList中处理，这确保任务在启动之前还没有开始运行。
    *
    * pendingTasksForExecutor是怎么来的，什么含义？
    *   pendingTasksForExecutor 在 TaskSetManager 构造函数中被创建，如下
    *     其中，key 为executoroId，value 为task index 数组。在 TaskSetManager 的构造函数中如下调用
    *     for (i <- (0 until numTasks).reverse) {
    *       addPendingTask(i)
    *     }
    *
    *  这段调用为 taskSetManager 中的优先位置类型为 ExecutorCacheTaskLocation（这里通过 toString 返回的格式进行匹配）
    *  的 tasks 调用 addPendingTask，addPendingTask 获取 task 的优先位置，即一个 Seq[String]；再获得这组优先位置对应的
    *  executors，从来反过来获得了 executor 对应 partition 缓存在其上内存的 tasks，即pendingTasksForExecutor
    *
    *  pendingTasksForExecutor：Executor的未确定队列中的所有task。这些task实际上以堆的形式存放，新来的task存放在堆尾，
    *       而执行错误的任务则会放置在堆头，以此可以察觉重复执行失败的任务。并且这些task还只是简单的清除，当它确定分发给
    *       executor时，它只是从这个executor的未确定队列中清除，其他的executor的未确定队列中稍后才清除。类似的参数有
    *       pendingTasksForHost和pendingTasksForRack。PS. 在向pending list中插入task时一般是按照逆序，这样序号小的
    *       任务就先launch。
    */
  private val pendingTasksForExecutor = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each host. Similar to pendingTasksForExecutor,
  // but at host level.
  // 每一个主机上即将发生的task集合，也是栈结构
  private val pendingTasksForHost = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each rack -- similar to the above.
  // 每一个机架上即将发生的task集合，也是栈结构
  private val pendingTasksForRack = new HashMap[String, ArrayBuffer[Int]]

  // Set containing pending tasks with no locality preferences.
  // 没有本地首选项的未决任务。
  private[scheduler] var pendingTasksWithNoPrefs = new ArrayBuffer[Int]

  // Set containing all pending tasks (also used as a stack, as above).
  // 包含所有未决任务的集合
  private val allPendingTasks = new ArrayBuffer[Int]

  // Tasks that can be speculated. Since these will be a small fraction of total
  // tasks, we'll just hold them in a HashSet.
  // 推测执行任务集合
  private[scheduler] val speculatableTasks = new HashSet[Int]

  // Task index, start and finish time for each task attempt (indexed by task ID)
  // 映射列表
  private val taskInfos = new HashMap[Long, TaskInfo]

  // Use a MedianHeap to record durations of successful tasks so we know when to launch
  // speculative tasks. This is only used when speculation is enabled, to avoid the overhead
  // of inserting into the heap when the heap won't be used.
  // 使用MedianHeap记录成功任务的持续时间，这样我们就能知道何时启动猜测性任务。这只在启用猜测时使用，
  // 以避免在堆不被使用时插入堆的开销。
  val successfulTaskDurations = new MedianHeap()

  // How frequently to reprint duplicate exceptions in full, in milliseconds
  //  如何频繁地以毫秒为单位重印重复的异常
  val EXCEPTION_PRINT_INTERVAL =
    conf.getLong("spark.logging.exceptionPrintInterval", 10000)

  // Map of recent exceptions (identified by string representation and top stack frame) to
  // duplicate count (how many times the same exception has appeared) and time the full exception
  // was printed. This should ideally be an LRU map that can drop old exceptions automatically.
  // 最近的异常(通过字符串表示和顶部堆栈帧识别)来重复计数(有多少次出现了相同的异常)，并将完整的异常打印出来。
  // 这应该是一个可以自动删除旧异常的LRU映射。
  private val recentExceptions = HashMap[String, (Int, Long)]()

  // Figure out the current map output tracker epoch and set it on all tasks
  // 找出当前map输出跟踪器时代，并将其设置在所有任务上
  val epoch = sched.mapOutputTracker.getEpoch
  logDebug("Epoch for " + taskSet + ": " + epoch)
  for (t <- tasks) {
    t.epoch = epoch
  }

  // Add all our tasks to the pending lists. We do this in reverse order
  // of task index so that tasks with low indices get launched first.
  /**
    * key 为executoroId，value 为task index 数组。
    * 这段调用为 taskSetManager 中的优先位置类型为 ExecutorCacheTaskLocation（这里通过 toString 返回的格式进行匹配）
    * 的 tasks 调用 addPendingTask，addPendingTask 获取 task 的优先位置，即一个 Seq[String]；再获得这组优先位置对应的
    * executors，从来反过来获得了 executor 对应 partition 缓存在其上内存的 tasks，即pendingTasksForExecutor
    */
  for (i <- (0 until numTasks).reverse) {
    addPendingTask(i)
  }

  /**
   * Track the set of locality levels which are valid given the tasks locality preferences and
   * the set of currently available executors.  This is updated as executors are added and removed.
   * This allows a performance optimization, of skipping levels that aren't relevant (eg., skip
   * PROCESS_LOCAL if no tasks could be run PROCESS_LOCAL for the current set of executors).
    *
    *
    * 在构造 TaskSetManager 对象时，会调用var myLocalityLevels = computeValidLocalityLevels()来确定locality levels
   */
  private[scheduler] var myLocalityLevels = computeValidLocalityLevels()

  // Time to wait at each level
  // localityWaits实际上是对myLocalityLevels应用getLocalityWait方法获得。
  // 每一个级别的等待时间
  private[scheduler] var localityWaits = myLocalityLevels.map(getLocalityWait)

  // Delay scheduling variables: we keep track of our current locality level and the time we
  // last launched a task at that level, and move up a level when localityWaits[curLevel] expires.
  // We then move down if we manage to launch a "more local" task.
  /**
    *
    * currentLocalityIndex和lastLaunchTime：用来定义当前的分配等级（根据数据本地性定义的优先级）以及当前等级下的任务
    * launch的最迟时间。
    */
  private var currentLocalityIndex = 0 // Index of our current locality level in validLocalityLevels
  private var lastLaunchTime = clock.getTimeMillis()  // Time we last launched a task at this level

  // 调度队列
  override def schedulableQueue: ConcurrentLinkedQueue[Schedulable] = null

  // 调度的模式
  override def schedulingMode: SchedulingMode = SchedulingMode.NONE

  private[scheduler] var emittedTaskSizeWarning = false

  /** Add a task to all the pending-task lists that it should be on.
    *
    * 将一个task任务添加到它应该在的所有task任务列表中。
    *
    * 该方法依靠本地性优先级将task悬挂到对应的pending list中。
    * 添加一个任务到pending 任务列表
    * */
  private def addPendingTask(index: Int) {
    for (loc <- tasks(index).preferredLocations) {
      loc match {
        // 代表数据存储在 executor 的内存中，也就是这个 partition 被 cache到内存了
        case e: ExecutorCacheTaskLocation =>
          pendingTasksForExecutor.getOrElseUpdate(e.executorId, new ArrayBuffer) += index
          // 代表数据存储在 hdfs 上
        case e: HDFSCacheTaskLocation =>
          // 这一句不知道啥意思
          val exe = sched.getExecutorsAliveOnHost(loc.host)
          exe match {
            case Some(set) =>
              for (e <- set) {
                // 未完成的任务，它要计算数据的位置上面(内存，任务所在主机，hdfs上)没有executor
                pendingTasksForExecutor.getOrElseUpdate(e, new ArrayBuffer) += index
              }
              logInfo(s"Pending task $index has a cached location at ${e.host} " +
                ", where there are executors " + set.mkString(","))
            // 未完成的任务，它要计算数据的位置上面(内存，任务所在主机，hdfs上)没有executor
            case None => logDebug(s"Pending task $index has a cached location at ${e.host} " +
                ", but there are no executors alive there.")
          }
        case _ =>
      }
      pendingTasksForHost.getOrElseUpdate(loc.host, new ArrayBuffer) += index
      for (rack <- sched.getRackForHost(loc.host)) {
        pendingTasksForRack.getOrElseUpdate(rack, new ArrayBuffer) += index
      }
    }

    if (tasks(index).preferredLocations == Nil) {
      // 没有本地首选项的未决任务。
      pendingTasksWithNoPrefs += index
    }

    // 不需要扫描整个列表来找到旧的任务
    allPendingTasks += index  // No point scanning this whole list to find the old task there
  }

  /**
   * Return the pending tasks list for a given executor ID, or an empty list if
   * there is no map entry for that host
    * 返回给定executor ID的待定任务列表，或如果该主机没有映射executor实例，则返回空列表
   */
  private def getPendingTasksForExecutor(executorId: String): ArrayBuffer[Int] = {
    pendingTasksForExecutor.getOrElse(executorId, ArrayBuffer())
  }

  /**
   * Return the pending tasks list for a given host, or an empty list if
   * there is no map entry for that host
    * 返回给定host的待定任务列表，或如果该主机没有映射host实例，则返回空列表
   */
  private def getPendingTasksForHost(host: String): ArrayBuffer[Int] = {
    pendingTasksForHost.getOrElse(host, ArrayBuffer())
  }

  /**
   * Return the pending rack-local task list for a given rack, or an empty list if
   * there is no map entry for that rack
    * 返回给定rack（机架）的待定任务列表，或如果该主机没有映射rack（机架）实例，则返回空列表
   */
  private def getPendingTasksForRack(rack: String): ArrayBuffer[Int] = {
    pendingTasksForRack.getOrElse(rack, ArrayBuffer())
  }

  /**
   * Dequeue a pending task from the given list and return its index.
   * Return None if the list is empty.
   * This method also cleans up any tasks in the list that have already
   * been launched, since we want that to happen lazily.
    *
    * 从给定的列表中取消一个挂起的任务并返回它的索引。如果列表为空，则返回None。
    * 这种方法还可以清理已经启动的列表中的任何任务，因为我们希望这种情况可以延迟发生。
   */
  private def dequeueTaskFromList(
      execId: String,
      host: String,
      list: ArrayBuffer[Int]): Option[Int] = {
    var indexOffset = list.size
    // 循环取出list最后一个元素
    while (indexOffset > 0) {
      indexOffset -= 1
      // 取出list最后一个元素
      val index = list(indexOffset)
      // 这一句不知道干嘛的？
      if (!isTaskBlacklistedOnExecOrNode(index, execId, host)) {
        // This should almost always be list.trimEnd(1) to remove tail
        // 这应该几乎总是列表。trimend(1)删除尾部
        list.remove(indexOffset)
        if (copiesRunning(index) == 0 && !successful(index)) {
          return Some(index)
        }
      }
    }
    None
  }

  /** Check whether a task is currently running an attempt on a given host
    * 检查当前任务是否正在对给定主机进行尝试
    * */
  private def hasAttemptOnHost(taskIndex: Int, host: String): Boolean = {
    taskAttempts(taskIndex).exists(_.host == host)
  }

  private def isTaskBlacklistedOnExecOrNode(index: Int, execId: String, host: String): Boolean = {
    taskSetBlacklistHelperOpt.exists { blacklist =>
      blacklist.isNodeBlacklistedForTask(host, index) ||
        blacklist.isExecutorBlacklistedForTask(execId, index)
    }
  }

  /**
   * Return a speculative task for a given executor if any are available. The task should not have
   * an attempt running on this host, in case the host is slow. In addition, the task should meet
   * the given locality constraint.
    *
    * 如果有任何可用的执行器，返回一个推测任务。任务不应该在主机上运行，以防主机运行缓慢。此外，
    * 该任务还应满足给定的局部约束条件。
   */
  // Labeled as protected to allow tests to override providing speculative tasks if necessary
  protected def dequeueSpeculativeTask(execId: String, host: String, locality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value)] =
  {
    // 从推测式执行任务列表中移除已经成功完成的task，因为从检测到调度之间还有一段时间，
    // 某些task已经成功执行
    // 从set集合中删除完成的任务
    speculatableTasks.retain(index => !successful(index)) // Remove finished tasks from set


    // 判断task是否可以在该executor对应的Host上执行，判断条件是：
    // task没有在该host上运行；
    // 该executor没有在task的黑名单里面（task在这个executor上失败过，并还在'黑暗'时间内）
    def canRunOnHost(index: Int): Boolean = {
      !hasAttemptOnHost(index, host) &&
        !isTaskBlacklistedOnExecOrNode(index, execId, host)
    }

    // // 推测执行任务集合是否为空
    if (!speculatableTasks.isEmpty) {
      // Check for process-local tasks; note that tasks can be process-local
      // on multiple nodes when we replicate cached blocks, as in Spark Streaming
      // 获取能在该executor上启动的taskIndex
      for (index <- speculatableTasks if canRunOnHost(index)) {
        // 获取task的优先位置
        val prefs = tasks(index).preferredLocations
        val executors = prefs.flatMap(_ match {
          case e: ExecutorCacheTaskLocation => Some(e.executorId)
          case _ => None
        });

        // 优先位置若为ExecutorCacheTaskLocation并且数据所在executor包含当前executor，
        // 则返回其task在taskSet的index和Locality Levels
        if (executors.contains(execId)) {
          speculatableTasks -= index
          return Some((index, TaskLocality.PROCESS_LOCAL))
        }
      }

      // Check for node-local tasks
      // 这里的判断是延迟调度的作用，即使是推测式任务也尽量以最好的本地性级别来启动
      if (TaskLocality.isAllowed(locality, TaskLocality.NODE_LOCAL)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          val locations = tasks(index).preferredLocations.map(_.host)
          if (locations.contains(host)) {
            speculatableTasks -= index
            return Some((index, TaskLocality.NODE_LOCAL))
          }
        }
      }

      // Check for no-preference tasks
      // NO_PREF: 数据从哪里访问都一样快，不需要位置优先
      if (TaskLocality.isAllowed(locality, TaskLocality.NO_PREF)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          val locations = tasks(index).preferredLocations
          if (locations.size == 0) {
            speculatableTasks -= index
            return Some((index, TaskLocality.PROCESS_LOCAL))
          }
        }
      }

      // Check for rack-local tasks
      // RACK_LOCAL: 数据在同一机架的不同节点上。需要通过网络传输数据及文件 IO，比 NODE_LOCAL 慢
      if (TaskLocality.isAllowed(locality, TaskLocality.RACK_LOCAL)) {
        for (rack <- sched.getRackForHost(host)) {
          for (index <- speculatableTasks if canRunOnHost(index)) {
            val racks = tasks(index).preferredLocations.map(_.host).flatMap(sched.getRackForHost)
            if (racks.contains(rack)) {
              speculatableTasks -= index
              return Some((index, TaskLocality.RACK_LOCAL))
            }
          }
        }
      }

      // Check for non-local tasks A
      // NY: 数据在非同一机架的网络上，速度最慢
      if (TaskLocality.isAllowed(locality, TaskLocality.ANY)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          speculatableTasks -= index
          return Some((index, TaskLocality.ANY))
        }
      }
    }

    None
  }

  /**
   * Dequeue a pending task for a given node and return its index and locality level.
   * Only search for tasks matching the given locality constraint.
    *
    * 将给定节点的挂起任务删除，并返回其索引和位置级别。只搜索与给定区域约束匹配的任务。
   *
   * @return An option containing (task index within the task set, locality, is speculative?)
    *         包含(任务集中的任务索引，地点，是推测的?)
   */
  private def dequeueTask(execId: String, host: String, maxLocality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value, Boolean)] =
  {
    // dequeueTaskFromList()方法：从给定的列表中取消一个挂起的任务并返回它的索引。如果列表为空，则返回None。
    // PROCESS_LOCAL: 数据在同一个 JVM 中，即同一个 executor 上。这是最佳数据 locality。
    for (index <- dequeueTaskFromList(execId, host, getPendingTasksForExecutor(execId))) {
      return Some((index, TaskLocality.PROCESS_LOCAL, false))
    }

    // NODE_LOCAL: 数据在同一个节点上。比如数据在同一个节点的另一个 executor上；或在 HDFS 上，
    // 恰好有 block 在同一个节点上。速度比 PROCESS_LOCAL 稍慢，因为数据需要在不同进程之间传递或从文件中读取
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {
      for (index <- dequeueTaskFromList(execId, host, getPendingTasksForHost(host))) {
        return Some((index, TaskLocality.NODE_LOCAL, false))
      }
    }

    // NO_PREF: 数据从哪里访问都一样快，不需要位置优先
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NO_PREF)) {
      // Look for noPref tasks after NODE_LOCAL for minimize cross-rack traffic
      for (index <- dequeueTaskFromList(execId, host, pendingTasksWithNoPrefs)) {
        return Some((index, TaskLocality.PROCESS_LOCAL, false))
      }
    }

    // RACK_LOCAL: 数据在同一机架的不同节点上。需要通过网络传输数据及文件 IO，比 NODE_LOCAL 慢
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.RACK_LOCAL)) {
      for {
        rack <- sched.getRackForHost(host)
        index <- dequeueTaskFromList(execId, host, getPendingTasksForRack(rack))
      } {
        return Some((index, TaskLocality.RACK_LOCAL, false))
      }
    }

    // ANY: 数据在非同一机架的网络上，速度最慢
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.ANY)) {
      for (index <- dequeueTaskFromList(execId, host, allPendingTasks)) {
        return Some((index, TaskLocality.ANY, false))
      }
    }

    // find a speculative task if all others tasks have been scheduled
    // 如果所有其他任务都安排好了，就去找一个推测的任务。
    dequeueSpeculativeTask(execId, host, maxLocality).map {
      case (taskIndex, allowedLocality) => (taskIndex, allowedLocality, true)}
  }

  /**
   * Respond to an offer of a single executor from the scheduler by finding a task
   *
   * NOTE: this function is either called with a maxLocality which
   * would be adjusted by delay scheduling algorithm or it will be with a special
   * NO_PREF locality which will be not modified
   *
   * @param execId the executor Id of the offered resource
   * @param host  the host Id of the offered resource
   * @param maxLocality the maximum locality we want to schedule the tasks at
    *
    *
    * ResourceOffer：根据TaskScheduler所提供的单个Resource资源包括host，executor和locality的要求返回一个合适的Task，
    *               TaskSetManager内部会根据上一个任务的成功提交的时间，自动调整自身的Locality匹配策略，如果上一次成功
    *               提交任务的时间间隔很长，则降低对Locality的要求（例如从最差要求Process Local降低为最差要求Node Local），
    *               反之则提高对Locality的要求。这一动态调整Locality的策略为了提高任务在最佳Locality的情况下得到运行的机会
    *                ，因为Resource资源是在短期内分批提供给TaskSetManager的，动态调整Locality门槛有助于改善整体的Locality
    *                分布情况。
   */
  @throws[TaskNotSerializableException]
  def resourceOffer(
      execId: String,
      host: String,
      maxLocality: TaskLocality.TaskLocality)
    : Option[TaskDescription] =
  {
    val offerBlacklisted = taskSetBlacklistHelperOpt.exists { blacklist =>
      blacklist.isNodeBlacklistedForTaskSet(host) ||
        blacklist.isExecutorBlacklistedForTaskSet(execId)
    }
    if (!isZombie && !offerBlacklisted) {
      val curTime = clock.getTimeMillis()

      var allowedLocality = maxLocality

      if (maxLocality != TaskLocality.NO_PREF) {
        allowedLocality = getAllowedLocalityLevel(curTime)
        if (allowedLocality > maxLocality) {
          // We're not allowed to search for farther-away tasks
          allowedLocality = maxLocality
        }
      }

      // 顺序取出TaskSetManager中未执行的task
      dequeueTask(execId, host, allowedLocality).map { case ((index, taskLocality, speculative)) =>
        // Found a task; do some bookkeeping and return a task description
        // 取出一个task
        val task = tasks(index)
        // 分配一个新的taskId
        val taskId = sched.newTaskId()
        // Do various bookkeeping
        copiesRunning(index) += 1
        val attemptNum = taskAttempts(index).size
        // 生成taskInfo对象存储当前task的相关信息
        val info = new TaskInfo(taskId, index, attemptNum, curTime,
          execId, host, taskLocality, speculative)
        taskInfos(taskId) = info
        // 记录taskId->taskInfo映射关系
        taskAttempts(index) = info :: taskAttempts(index)
        // Update our locality level for delay scheduling
        // NO_PREF will not affect the variables related to delay scheduling
        if (maxLocality != TaskLocality.NO_PREF) {
          currentLocalityIndex = getLocalityIndex(taskLocality)
          lastLaunchTime = curTime
        }
        // Serialize and return the task
        val serializedTask: ByteBuffer = try {
          ser.serialize(task)
        } catch {
          // If the task cannot be serialized, then there's no point to re-attempt the task,
          // as it will always fail. So just abort the whole task-set.
          case NonFatal(e) =>
            val msg = s"Failed to serialize task $taskId, not attempting to retry it."
            logError(msg, e)
            abort(s"$msg Exception during serialization: $e")
            throw new TaskNotSerializableException(e)
        }
        // 如果序列化后的task大小超过100KB时,直接抛出异常
        if (serializedTask.limit > TaskSetManager.TASK_SIZE_TO_WARN_KB * 1024 &&
          !emittedTaskSizeWarning) {
          emittedTaskSizeWarning = true
          logWarning(s"Stage ${task.stageId} contains a task of very large size " +
            s"(${serializedTask.limit / 1024} KB). The maximum recommended task size is " +
            s"${TaskSetManager.TASK_SIZE_TO_WARN_KB} KB.")
        }

        // 添加到正在运行的Task任务集中
        addRunningTask(taskId)

        // We used to log the time it takes to serialize the task, but task size is already
        // a good proxy to task serialization time.
        // val timeTaken = clock.getTime() - startTime
        // 我们曾经记录了序列化任务所需的时间，但是任务大小已经成为任务序列化时间的好代理。
        // val timeTaken = clock. gettime()- startTime
        val taskName = s"task ${info.id} in stage ${taskSet.id}"
        logInfo(s"Starting $taskName (TID $taskId, $host, executor ${info.executorId}, " +
          s"partition ${task.partitionId}, $taskLocality, ${serializedTask.limit} bytes)")

        // 为该task准备好执行环境后，开始执行task
        sched.dagScheduler.taskStarted(task, info)
        // 返回一个TaskDescription信息
        new TaskDescription(
          taskId,
          attemptNum,
          execId,
          taskName,
          index,
          sched.sc.addedFiles,
          sched.sc.addedJars,
          task.localProperties,
          serializedTask)
      }
    } else {
      None
    }
  }

  private def maybeFinishTaskSet() {
    if (isZombie && runningTasks == 0) {
      sched.taskSetFinished(this)
      if (tasksSuccessful == numTasks) {
        blacklistTracker.foreach(_.updateBlacklistForSuccessfulTaskSet(
          taskSet.stageId,
          taskSet.stageAttemptId,
          taskSetBlacklistHelperOpt.get.execToFailures))
      }
    }
  }

  /**
   * Get the level we can launch tasks according to delay scheduling, based on current wait time.
    *
    * 1.5版本代码如下：
    *  private def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality = {
    *     while(curTime - lastLanuchTime >= localityWaits(currentLocalityIndex) &&
    *       currentLocalityIndex < myLocalityLevels.length -1 ){
    *           lastLaunchTime += localityWaits(currentLocalityIndex)
    *           currentLocalityIndex += 1
    *       }
    *       myLocalityLevels(currentLocalityIndex)
    *  }
    *
    *  1.5版本实现的功能是：  经过对Spark任务本地化的分析后，读者可能觉得这样的代码实现过于复杂，并且在获取本地化级别的
    *  时候竟然每次都要等待一段本地化级别的等待时间，这种事先未免太过奇怪。正如刚开始说的，任何任务都希望被分配到可以从本地
    *  读取数据的节点上以获得最大的性能提升。然而每个任务的运行时长都不是事先可以预料到的，当一个任务在分配的时，如果没有满足
    *  最佳本地化（PROCESS_LOCAL）的资源时，如果一直固执地期盼的到最佳的资源，很有可能会被已经占用最佳资源但是运行时间很长的
    *  任务单个，所以这些代码实现了当没有最佳本地化时，退而求其次选择稍微差一点的资源。
    *
    *   获取当前时间curTime，并且结合上次执行Task分配的时间，从currentLocalityIndex的下标开始，取出locality对应的
    *   task分配等待时间，如果时间超过这个设置，就把下标加1，直至找到一个符合条件的下标值，然后计算此下标值对应的locality
    *   级别。
    *
   */
  private def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality = {
    // Remove the scheduled or finished tasks lazily 懒加载地删除计划或完成的任务
    def tasksNeedToBeScheduledFrom(pendingTaskIds: ArrayBuffer[Int]): Boolean = {
      // 获取准备执行任务数组的大小
      var indexOffset = pendingTaskIds.size
      // 倒叙循环
      while (indexOffset > 0) {
        indexOffset -= 1
        // 获取数组最后一个下标
        val index = pendingTaskIds(indexOffset)
        // 如果拷贝正在运行的一个task数组为0，而且这个task运行失败，说明这个task还没开始执行啊
        if (copiesRunning(index) == 0 && !successful(index)) {
          return true
        } else {
          // 如果task已经开始执行了，那么就从准备运行的task数组中删除
          pendingTaskIds.remove(indexOffset)
        }
      }
      false
    }

    // Walk through the list of tasks that can be scheduled at each location and returns true
    // if there are any tasks that still need to be scheduled. Lazily cleans up tasks that have
    // already been scheduled.
    //
    // 遍历可在每个位置安排的任务列表，并返回true，如果仍有需要调度的任务。懒洋洋地清理已经安排好的任务。
    def moreTasksToRunIn(pendingTasks: HashMap[String, ArrayBuffer[Int]]): Boolean = {
      val emptyKeys = new ArrayBuffer[String]
      val hasTasks = pendingTasks.exists {
        case (id: String, tasks: ArrayBuffer[Int]) =>
          if (tasksNeedToBeScheduledFrom(tasks)) {
            true
          } else {
            emptyKeys += id
            false
          }
      }
      // The key could be executorId, host or rackId
      emptyKeys.foreach(id => pendingTasks.remove(id))
      hasTasks
    }

    // 当前的分配等级（根据数据本地性定义的优先级） <  在构造 TaskSetManager 对象时，确定的locality levels -1
    while (currentLocalityIndex < myLocalityLevels.length - 1) {
      val moreTasks = myLocalityLevels(currentLocalityIndex) match {
        case TaskLocality.PROCESS_LOCAL => moreTasksToRunIn(pendingTasksForExecutor)
        case TaskLocality.NODE_LOCAL => moreTasksToRunIn(pendingTasksForHost)
        case TaskLocality.NO_PREF => pendingTasksWithNoPrefs.nonEmpty
        case TaskLocality.RACK_LOCAL => moreTasksToRunIn(pendingTasksForRack)
      }

      if (!moreTasks) {
        // This is a performance optimization: if there are no more tasks that can
        // be scheduled at a particular locality level, there is no point in waiting
        // for the locality wait timeout (SPARK-4939).
        //
        // 这是一个性能优化:如果没有更多的任务可以在一个特定的局部性级别调度，那么等待本地等待超时是没有意义的(SPARK-4939)
        lastLaunchTime = curTime
        logDebug(s"No tasks for locality level ${myLocalityLevels(currentLocalityIndex)}, " +
          s"so moving to locality level ${myLocalityLevels(currentLocalityIndex + 1)}")
        currentLocalityIndex += 1

        // curTime - 当前等级下的任务launch的最迟时间。 >=  每一个级别的等待时间
      } else if (curTime - lastLaunchTime >= localityWaits(currentLocalityIndex)) {
        // Jump to the next locality level, and reset lastLaunchTime so that the next locality
        // wait timer doesn't immediately expire
        //
        // 跳转到下一个本地级别，并重置lastLaunchTime，以便下一个本地等待计时器不会立即过期。
        lastLaunchTime += localityWaits(currentLocalityIndex)
        logDebug(s"Moving to ${myLocalityLevels(currentLocalityIndex + 1)} after waiting for " +
          s"${localityWaits(currentLocalityIndex)}ms")
        currentLocalityIndex += 1
      } else {
        return myLocalityLevels(currentLocalityIndex)
      }
    }
    myLocalityLevels(currentLocalityIndex)
  }

  /**
   * Find the index in myLocalityLevels for a given locality. This is also designed to work with
   * localities that are not in myLocalityLevels (in case we somehow get those) by returning the
   * next-biggest level we have. Uses the fact that the last value in myLocalityLevels is ANY.
   */
  def getLocalityIndex(locality: TaskLocality.TaskLocality): Int = {
    var index = 0
    while (locality > myLocalityLevels(index)) {
      index += 1
    }
    index
  }

  /**
   * Check whether the given task set has been blacklisted to the point that it can't run anywhere.
   *
   * It is possible that this taskset has become impossible to schedule *anywhere* due to the
   * blacklist.  The most common scenario would be if there are fewer executors than
   * spark.task.maxFailures. We need to detect this so we can fail the task set, otherwise the job
   * will hang.
   *
   * There's a tradeoff here: we could make sure all tasks in the task set are schedulable, but that
   * would add extra time to each iteration of the scheduling loop. Here, we take the approach of
   * making sure at least one of the unscheduled tasks is schedulable. This means we may not detect
   * the hang as quickly as we could have, but we'll always detect the hang eventually, and the
   * method is faster in the typical case. In the worst case, this method can take
   * O(maxTaskFailures + numTasks) time, but it will be faster when there haven't been any task
   * failures (this is because the method picks one unscheduled task, and then iterates through each
   * executor until it finds one that the task isn't blacklisted on).
   */
  private[scheduler] def abortIfCompletelyBlacklisted(
      hostToExecutors: HashMap[String, HashSet[String]]): Unit = {
    taskSetBlacklistHelperOpt.foreach { taskSetBlacklist =>
      val appBlacklist = blacklistTracker.get
      // Only look for unschedulable tasks when at least one executor has registered. Otherwise,
      // task sets will be (unnecessarily) aborted in cases when no executors have registered yet.
      if (hostToExecutors.nonEmpty) {
        // find any task that needs to be scheduled
        val pendingTask: Option[Int] = {
          // usually this will just take the last pending task, but because of the lazy removal
          // from each list, we may need to go deeper in the list.  We poll from the end because
          // failed tasks are put back at the end of allPendingTasks, so we're more likely to find
          // an unschedulable task this way.
          val indexOffset = allPendingTasks.lastIndexWhere { indexInTaskSet =>
            copiesRunning(indexInTaskSet) == 0 && !successful(indexInTaskSet)
          }
          if (indexOffset == -1) {
            None
          } else {
            Some(allPendingTasks(indexOffset))
          }
        }

        pendingTask.foreach { indexInTaskSet =>
          // try to find some executor this task can run on.  Its possible that some *other*
          // task isn't schedulable anywhere, but we will discover that in some later call,
          // when that unschedulable task is the last task remaining.
          val blacklistedEverywhere = hostToExecutors.forall { case (host, execsOnHost) =>
            // Check if the task can run on the node
            val nodeBlacklisted =
              appBlacklist.isNodeBlacklisted(host) ||
                taskSetBlacklist.isNodeBlacklistedForTaskSet(host) ||
                taskSetBlacklist.isNodeBlacklistedForTask(host, indexInTaskSet)
            if (nodeBlacklisted) {
              true
            } else {
              // Check if the task can run on any of the executors
              execsOnHost.forall { exec =>
                appBlacklist.isExecutorBlacklisted(exec) ||
                  taskSetBlacklist.isExecutorBlacklistedForTaskSet(exec) ||
                  taskSetBlacklist.isExecutorBlacklistedForTask(exec, indexInTaskSet)
              }
            }
          }
          if (blacklistedEverywhere) {
            val partition = tasks(indexInTaskSet).partitionId
            abort(s"Aborting $taskSet because task $indexInTaskSet (partition $partition) " +
              s"cannot run anywhere due to node and executor blacklist.  Blacklisting behavior " +
              s"can be configured via spark.blacklist.*.")
          }
        }
      }
    }
  }

  /**
   * Marks the task as getting result and notifies the DAG Scheduler
    * 将任务标记为获取结果并通知DAG调度器
   */
  def handleTaskGettingResult(tid: Long): Unit = {
    val info = taskInfos(tid)
    info.markGettingResult(clock.getTimeMillis())
    sched.dagScheduler.taskGettingResult(info)
  }

  /**
   * Check whether has enough quota to fetch the result with `size` bytes
   */
  def canFetchMoreResults(size: Long): Boolean = sched.synchronized {
    totalResultSize += size
    calculatedTasks += 1
    if (maxResultSize > 0 && totalResultSize > maxResultSize) {
      val msg = s"Total size of serialized results of ${calculatedTasks} tasks " +
        s"(${Utils.bytesToString(totalResultSize)}) is bigger than spark.driver.maxResultSize " +
        s"(${Utils.bytesToString(maxResultSize)})"
      logError(msg)
      abort(msg)
      false
    } else {
      true
    }
  }

  /**
   * Marks a task as successful and notifies the DAGScheduler that the task has ended.
    * 将任务标记为成功，并通知DAGScheduler，任务已经结束。
   */
  def handleSuccessfulTask(tid: Long, result: DirectTaskResult[_]): Unit = {
    val info = taskInfos(tid)
    val index = info.index
    info.markFinished(TaskState.FINISHED, clock.getTimeMillis())
    if (speculationEnabled) {
      successfulTaskDurations.insert(info.duration)
    }
    removeRunningTask(tid)

    // Kill any other attempts for the same task (since those are unnecessary now that one
    // attempt completed successfully).
    for (attemptInfo <- taskAttempts(index) if attemptInfo.running) {
      logInfo(s"Killing attempt ${attemptInfo.attemptNumber} for task ${attemptInfo.id} " +
        s"in stage ${taskSet.id} (TID ${attemptInfo.taskId}) on ${attemptInfo.host} " +
        s"as the attempt ${info.attemptNumber} succeeded on ${info.host}")
      sched.backend.killTask(
        attemptInfo.taskId,
        attemptInfo.executorId,
        interruptThread = true,
        reason = "another attempt succeeded")
    }

    //判断该TaskSet中Task是否已全部执行成功
    if (!successful(index)) {
      //该Task还未标记为成功执行
      //增加执行成功的Task
      tasksSuccessful += 1
      // 17/12/05 10:39:40 INFO TaskSetManager: Finished task 6.0 in stage 0.0 (TID 6) in 26147 ms on localhost (executor driver) (8/10)
      // 17/12/05 10:39:45 INFO TaskSetManager: Finished task 8.0 in stage 0.0 (TID 8) in 24170 ms on localhost (executor driver) (9/10)
      logInfo(s"Finished task ${info.id} in stage ${taskSet.id} (TID ${info.taskId}) in" +
        s" ${info.duration} ms on ${info.host} (executor ${info.executorId})" +
        s" ($tasksSuccessful/$numTasks)")
      // Mark successful and stop if all the tasks have succeeded.
      // 标记执行成功的Task，如果TaskSet中的所有Task执行成功则停止该TaskSetManager
      successful(index) = true
      if (tasksSuccessful == numTasks) {
        isZombie = true
      }
    } else {
      logInfo("Ignoring task-finished event for " + info.id + " in stage " + taskSet.id +
        " because task " + index + " has already completed successfully")
    }
    // This method is called by "TaskSchedulerImpl.handleSuccessfulTask" which holds the
    // "TaskSchedulerImpl" lock until exiting. To avoid the SPARK-7655 issue, we should not
    // "deserialize" the value when holding a lock to avoid blocking other threads. So we call
    // "result.value()" in "TaskResultGetter.enqueueSuccessfulTask" before reaching here.
    // Note: "result.value()" only deserializes the value when it's called at the first time, so
    // here "result.value()" just returns the value and won't block other threads.
    sched.dagScheduler.taskEnded(tasks(index), Success, result.value(), result.accumUpdates, info)
    //判断TaskSet中Task是否已全部执行完成，是则说明该TaskSet已执行完成，相应的对该
    //TaskSetManager的调度结束，从调度池中删除该TaskSetManager
    maybeFinishTaskSet()
  }

  /**
   * Marks the task as failed, re-adds it to the list of pending tasks, and notifies the
   * DAG Scheduler.
    *
    * TaskSetManager#handlerFailTask方法主要是将任务标记为失败，并将它重新添加到待处理任务列表，同时通知高层调度器DAG Scheduler。
   */
  def handleFailedTask(tid: Long, state: TaskState, reason: TaskFailedReason) {
    val info = taskInfos(tid)
    if (info.failed || info.killed) {
      return
    }
    removeRunningTask(tid)
    info.markFinished(state, clock.getTimeMillis())
    val index = info.index
    copiesRunning(index) -= 1
    var accumUpdates: Seq[AccumulatorV2[_, _]] = Seq.empty
    val failureReason = s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid, ${info.host}," +
      s" executor ${info.executorId}): ${reason.toErrorString}"
    val failureException: Option[Throwable] = reason match {
      case fetchFailed: FetchFailed =>
        logWarning(failureReason)
        if (!successful(index)) {
          successful(index) = true
          tasksSuccessful += 1
        }
        isZombie = true
        None

      case ef: ExceptionFailure =>
        // ExceptionFailure's might have accumulator updates
        accumUpdates = ef.accums
        if (ef.className == classOf[NotSerializableException].getName) {
          // If the task result wasn't serializable, there's no point in trying to re-execute it.
          logError("Task %s in stage %s (TID %d) had a not serializable result: %s; not retrying"
            .format(info.id, taskSet.id, tid, ef.description))
          abort("Task %s in stage %s (TID %d) had a not serializable result: %s".format(
            info.id, taskSet.id, tid, ef.description))
          return
        }
        val key = ef.description
        val now = clock.getTimeMillis()
        val (printFull, dupCount) = {
          if (recentExceptions.contains(key)) {
            val (dupCount, printTime) = recentExceptions(key)
            if (now - printTime > EXCEPTION_PRINT_INTERVAL) {
              recentExceptions(key) = (0, now)
              (true, 0)
            } else {
              recentExceptions(key) = (dupCount + 1, printTime)
              (false, dupCount + 1)
            }
          } else {
            recentExceptions(key) = (0, now)
            (true, 0)
          }
        }
        if (printFull) {
          logWarning(failureReason)
        } else {
          logInfo(
            s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid) on ${info.host}, executor" +
              s" ${info.executorId}: ${ef.className} (${ef.description}) [duplicate $dupCount]")
        }
        ef.exception

      case e: ExecutorLostFailure if !e.exitCausedByApp =>
        logInfo(s"Task $tid failed because while it was being computed, its executor " +
          "exited for a reason unrelated to the task. Not counting this failure towards the " +
          "maximum number of failures for the task.")
        None

      case e: TaskFailedReason =>  // TaskResultLost, TaskKilled, and others
        logWarning(failureReason)
        None
    }

    //调用高层调度器（DAGScheduler）进行容错
    sched.dagScheduler.taskEnded(tasks(index), reason, null, accumUpdates, info)

    if (successful(index)) {
      logInfo(s"Task ${info.id} in stage ${taskSet.id} (TID $tid) failed, but the task will not" +
        s" be re-executed (either because the task failed with a shuffle data fetch failure," +
        s" so the previous stage needs to be re-run, or because a different copy of the task" +
        s" has already succeeded).")
    } else {
      //将Task加入到待处理任务列表
      addPendingTask(index)
    }

    if (!isZombie && reason.countTowardsTaskFailures) {
      taskSetBlacklistHelperOpt.foreach(_.updateBlacklistForFailedTask(
        info.host, info.executorId, index))
      assert (null != failureReason)
      numFailures(index) += 1
      //如果失败次数大于最大失败次数，则将Task丢弃。
      if (numFailures(index) >= maxTaskFailures) {
        logError("Task %d in stage %s failed %d times; aborting job".format(
          index, taskSet.id, maxTaskFailures))
        abort("Task %d in stage %s failed %d times, most recent failure: %s\nDriver stacktrace:"
          .format(index, taskSet.id, maxTaskFailures, failureReason), failureException)
        return
      }
    }
    //判断TaskSet中Task是否已全部执行完成，是则说明该TaskSet已执行完成，相应的对该
    //TaskSetManager的调度结束，从调度池中删除该TaskSetManager
    maybeFinishTaskSet()
  }

  def abort(message: String, exception: Option[Throwable] = None): Unit = sched.synchronized {
    // TODO: Kill running tasks if we were not terminated due to a Mesos error
    sched.dagScheduler.taskSetFailed(taskSet, message, exception)
    isZombie = true
    maybeFinishTaskSet()
  }

  /** If the given task ID is not in the set of running tasks, adds it.
    * 如果给定的TaskID没有设置的正在运行的Task集合中，那么就添加他
   *
   * Used to keep track of the number of running tasks, for enforcing scheduling policies.
    * 用于跟踪运行任务的数量，用于执行调度策略。
   */
  def addRunningTask(tid: Long) {
    // runningTasksSet 正在运行的任务集 HashSet  parent是一个 Pool
    if (runningTasksSet.add(tid) && parent != null) {
      parent.increaseRunningTasks(1)
    }
  }

  /** If the given task ID is in the set of running tasks, removes it.
    * 如果给定的任务ID处于运行任务的集合中，则删除它。
    * */
  def removeRunningTask(tid: Long) {
    if (runningTasksSet.remove(tid) && parent != null) {
      parent.decreaseRunningTasks(1)
    }
  }

  override def getSchedulableByName(name: String): Schedulable = {
    null
  }

  override def addSchedulable(schedulable: Schedulable) {}

  override def removeSchedulable(schedulable: Schedulable) {}

  override def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]()
    sortedTaskSetQueue += this
    sortedTaskSetQueue
  }

  /** Called by TaskScheduler when an executor is lost so we can re-enqueue our tasks */
  override def executorLost(execId: String, host: String, reason: ExecutorLossReason) {
    // Re-enqueue any tasks that ran on the failed executor if this is a shuffle map stage,
    // and we are not using an external shuffle server which could serve the shuffle outputs.
    // The reason is the next stage wouldn't be able to fetch the data from this dead executor
    // so we would need to rerun these tasks on other executors.
    if (tasks(0).isInstanceOf[ShuffleMapTask] && !env.blockManager.externalShuffleServiceEnabled
        && !isZombie) {
      for ((tid, info) <- taskInfos if info.executorId == execId) {
        val index = taskInfos(tid).index
        if (successful(index)) {
          successful(index) = false
          copiesRunning(index) -= 1
          tasksSuccessful -= 1
          addPendingTask(index)
          // Tell the DAGScheduler that this task was resubmitted so that it doesn't think our
          // stage finishes when a total of tasks.size tasks finish.
          sched.dagScheduler.taskEnded(
            tasks(index), Resubmitted, null, Seq.empty, info)
        }
      }
    }
    for ((tid, info) <- taskInfos if info.running && info.executorId == execId) {
      val exitCausedByApp: Boolean = reason match {
        case exited: ExecutorExited => exited.exitCausedByApp
        case ExecutorKilled => false
        case _ => true
      }
      handleFailedTask(tid, TaskState.FAILED, ExecutorLostFailure(info.executorId, exitCausedByApp,
        Some(reason.toString)))
    }
    // recalculate valid locality levels and waits when executor is lost
    recomputeLocality()
  }

  /**
   * Check for tasks to be speculated and return true if there are any. This is called periodically
   * by the TaskScheduler.
   *
   */
  override def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean = {
    // Can't speculate if we only have one task, and no need to speculate if the task set is a
    // zombie.
    // 如果task只有一个或者所有task都不需要再执行了就没有必要再检测
    if (isZombie || numTasks == 1) {
      return false
    }
    var foundTasks = false
    // 所有task数 * SPECULATION_QUANTILE（默认0.75，可通过spark.speculation.quantile设置）
    val minFinishedForSpeculation = (SPECULATION_QUANTILE * numTasks).floor.toInt
    logDebug("Checking for speculative tasks: minFinished = " + minFinishedForSpeculation)

    // 成功的task数是否超过总数的75%，并且成功的task是否大于0
    if (tasksSuccessful >= minFinishedForSpeculation && tasksSuccessful > 0) {
      val time = clock.getTimeMillis()
      // 取这多个task任务执行成功时间的中位数
      var medianDuration = successfulTaskDurations.median
      // 中位数 * SPECULATION_MULTIPLIER （默认1.5，可通过spark.speculation.multiplier设置）
      val threshold = max(SPECULATION_MULTIPLIER * medianDuration, minTimeToSpeculation)
      // TODO: Threshold should also look at standard deviation of task durations and have a lower
      // bound based on that.
      logDebug("Task length threshold for speculation: " + threshold)

      // 遍历该TaskSet中的task，取未成功执行、正在执行、执行时间已经大于threshold 、
      // 推测式执行task列表中未包括的task放进需要推测式执行的列表中speculatableTasks
      for (tid <- runningTasksSet) {
        val info = taskInfos(tid)
        val index = info.index
        if (!successful(index) && copiesRunning(index) == 1 && info.timeRunning(time) > threshold &&
          !speculatableTasks.contains(index)) {
          logInfo(
            "Marking task %d in stage %s (on %s) as speculatable because it ran more than %.0f ms"
              .format(index, taskSet.id, info.host, threshold))
          speculatableTasks += index
          foundTasks = true
        }
      }
    }
    foundTasks
  }

  /**
    * 用于获取各个本地化级别的等待时间。
    *
    *     变量名称                       描述                           默认值
    *   spark.locality.wait             本地化级别的默认等待时间          3000
    *   spark.locality.wait.process     本地进程的等待时间               3000
    *   spark.locality.wait.node        本地节点的等待时间               3000
    *   spark.locality.wait.rack        本地机架的等待时间               3000
    *
    *
    *   提示：在运行的任务时间很长而且数量较多的情况下，适当调高这些参数可以显著提高性能。
    *   然而当这些参数值都已经超过任务运行的时长时，需要调小这些参数。
    *
    */
  private def getLocalityWait(level: TaskLocality.TaskLocality): Long = {
    val defaultWait = conf.get("spark.locality.wait", "3s")
    val localityWaitKey = level match {
      case TaskLocality.PROCESS_LOCAL => "spark.locality.wait.process"
      case TaskLocality.NODE_LOCAL => "spark.locality.wait.node"
      case TaskLocality.RACK_LOCAL => "spark.locality.wait.rack"
      case _ => null
    }

    if (localityWaitKey != null) {
      conf.getTimeAsMs(localityWaitKey, defaultWait)
    } else {
      0L
    }
  }

  /**
   * Compute the locality levels used in this TaskSet. Assumes that all tasks have already been
   * added to queues using addPendingTask.
    *
    * 计算这个任务集中使用的局部性级别。假设所有的任务都已经被添加到使用addPendingTask的队列中了。
    *
    * computeValidLocalityLevels方法用于计算有效的本地化级别。以PROCESS_LOCAL为例。如果存在Executor中有待执行的任务。
    * （pengingTaskForExecutor不为空）且PROCESS_LOCAL本地化的等待时间不为0（调用getLocalityWait方法获得）且存在Executor
    * 已经被激活（pendingTasksforExecutor中的ExecutorId有存在于TaskScheduler的activeExecutorIds中的，那么允许的本地化级别里包括
    * PROCESS_LOCAL）
   *
    *
    * 这个函数是在解决4个问题：
    * taskSetManager 的 locality levels是否包含 PROCESS_LOCAL
    * taskSetManager 的 locality levels是否包含 NODE_LOCAL
    * taskSetManager 的 locality levels是否包含 NO_PREF
    * taskSetManager 的 locality levels是否包含 RACK_LOCAL
   */
  private def computeValidLocalityLevels(): Array[TaskLocality.TaskLocality] = {
    import TaskLocality.{PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY}
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]

    // taskSetManager 的 locality levels是否包含 PROCESS_LOCAL
    /**
      * 要搞懂这段代码，首先要搞明白下面两个问题
      *     pendingTasksForExecutor是怎么来的，什么含义？
      *     sched.isExecutorAlive(_)干了什么？
      *
      * 这行代码pendingTasksForExecutor.keySet.exists(sched.isExecutorAlive(_))的含义：
      *     taskSetManager 的所有对应 partition 数据缓存在 executor 内存中的 tasks 对应的所有 executor，
      *     是否有任一 active，若有则返回 true；否则返回 false
      */
    if (!pendingTasksForExecutor.isEmpty &&
        pendingTasksForExecutor.keySet.exists(sched.isExecutorAlive(_))) {
      levels += PROCESS_LOCAL
    }

    /**
      * taskSetManager 的 locality levels是否包含 NODE_LOCAL
      *
      * pendingTasksForHost: HashMap[String, ArrayBuffer[Int]]类型，key 为 host，value 为 preferredLocations 包含该 host 的 tasks indexs 数组
      *
      * sched.hasExecutorsAliveOnHost(_):
      * 其中executorsByHost为 HashMap[String, HashSet[String]] 类型，key 为 host，value 为该 host 上的 active executors
      *
      * taskSetManager 的所有 tasks 对应的所有 hosts，是否有任一是 tasks 的优先位置 hosts，若有返回 true；否则返回 false
      */
    if (!pendingTasksForHost.isEmpty &&
        pendingTasksForHost.keySet.exists(sched.hasExecutorsAliveOnHost(_))) {
      levels += NODE_LOCAL
    }


    /**
      *   如果一个 RDD 的某些 partitions 没有优先位置（如是以内存集合作为数据源且 executors 和 driver不在同一个节点），那么这个 RDD action 产生的 taskSetManagers
      * 的 locality levels 就包含 NO_PREF
     */
    if (!pendingTasksWithNoPrefs.isEmpty) {
      levels += NO_PREF
    }


    /**
      *     pendingTasksForRack：HashMap[String, ArrayBuffer[Int]]类型，key为 rack，value 为优先位置所在的 host
      * 属于该机架的 tasks
      *
      *   判断 taskSetManager 的 locality levels 是否包含RACK_LOCAL的规则为：taskSetManager 的所有 tasks 的优先位置 host 所在的所有 racks 与当前 active executors
      * 所在的机架是否有交集，若有则返回 true，否则返回 false
     */
    if (!pendingTasksForRack.isEmpty &&
        pendingTasksForRack.keySet.exists(sched.hasHostAliveOnRack(_))) {
      levels += RACK_LOCAL
    }

    // 对于所有的 taskSetManager 均包含 ANY
    levels += ANY
    logDebug("Valid locality levels for " + taskSet + ": " + levels.mkString(", "))
    levels.toArray
  }

  def recomputeLocality() {
    val previousLocalityLevel = myLocalityLevels(currentLocalityIndex)
    myLocalityLevels = computeValidLocalityLevels()
    localityWaits = myLocalityLevels.map(getLocalityWait)
    currentLocalityIndex = getLocalityIndex(previousLocalityLevel)
  }

  def executorAdded() {
    recomputeLocality()
  }
}

private[spark] object TaskSetManager {
  // The user will be warned if any stages contain a task that has a serialized size greater than
  // this.
  val TASK_SIZE_TO_WARN_KB = 100
}
