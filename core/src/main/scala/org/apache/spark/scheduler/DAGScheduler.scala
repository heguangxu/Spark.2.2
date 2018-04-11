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
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.collection.Map
import scala.collection.mutable.{HashMap, HashSet, Stack}
import scala.concurrent.duration._
import scala.language.existentials
import scala.language.postfixOps
import scala.util.control.NonFatal

import org.apache.commons.lang3.SerializationUtils

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.storage._
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat
import org.apache.spark.util._

/**
 * The high-level scheduling layer that implements stage-oriented scheduling. It computes a DAG of
 * stages for each job, keeps track of which RDDs and stage outputs are materialized, and finds a
 * minimal schedule to run the job. It then submits stages as TaskSets to an underlying
 * TaskScheduler implementation that runs them on the cluster. A TaskSet contains fully independent
 * tasks that can run right away based on the data that's already on the cluster (e.g. map output
 * files from previous stages), though it may fail if this data becomes unavailable.
  *
  *     高级调度层实现面向stage-oriented  scheduling。它为每个作业计算一个DAG阶段，跟踪哪些RDDs和stage
  * 输出被具体化，并找到一个最小的调度来运行该作业。然后，它将stages作为任务集提交给在集群上运行
  * 它们的底层任务调度程序实现。TaskSet包含了完全独立的任务，可以根据已经在集群中的数据
  * (例如从以前的stages映射出输出文件)来立即运行，尽管如果这些数据不可用，它可能会失败。
  *
 *
 * Spark stages are created by breaking the RDD graph at shuffle boundaries. RDD operations with
 * "narrow" dependencies, like map() and filter(), are pipelined together into one set of tasks
 * in each stage, but operations with shuffle dependencies require multiple stages (one to write a
 * set of map output files, and another to read those files after a barrier). In the end, every
 * stage will have only shuffle dependencies on other stages, and may compute multiple operations
 * inside it. The actual pipelining of these operations happens in the RDD.compute() functions of
 * various RDDs (MappedRDD, FilteredRDD, etc).
  *
  * Spark stage是通过在shuffle边界上打破RDD图创建的。RDD操作具有“窄”的依赖关系，如map()和filter()，
  * 它们在每个stage都被连接到一组任务中，但是处理带有shuffle依赖性的操作需要多个stage(一个要编写一组映射
  * 输出文件，另一个要在一个barrier后读取这些文件)。最后，每个stage只会对其他stage的依赖进行调整，
  * 并可能计算其中的多个操作。这些操作的实际流水线发生在各种RDDs(MappedRDD、FilteredRDD等)的RDD.compute()函数中。
  *
 *
 * In addition to coming up with a DAG of stages, the DAGScheduler also determines the preferred
 * locations to run each task on, based on the current cache status, and passes these to the
 * low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being
 * lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are
 * not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task
 * a small number of times before cancelling the whole stage.
  *
  * 除了有一个stages的DAG之外，DAGScheduler还根据当前的缓存状态确定首选的位置来运行每个任务，
  * 并将这些任务传递给低级的TaskScheduler。此外，它处理由于拖拽输出文件丢失而导致的故障，在这种情况下，
  * 旧阶段可能需要重新提交。在一个不是由shuffle文件丢失引起的阶段中的失败由TaskScheduler处理，
  * 它将在取消整个阶段之前对每个任务进行多次重新尝试。
 *
 * When looking through this code, there are several key concepts:
  * 在查看这段代码时，有几个关键的概念:
 *
 *  - Jobs (represented by [[ActiveJob]]) are the top-level work items submitted to the scheduler.
 *    For example, when the user calls an action, like count(), a job will be submitted through
 *    submitJob. Each Job may require the execution of multiple stages to build intermediate data.
  *
  *    Jobs(由[[ActiveJob]]代表)是提交给调度器的顶级工作项。例如，当用户调用一个动作，如count()时，
  *    将通过submitJob提交作业。每个作业可能需要执行多个阶段来构建中间数据。
 *
 *  - Stages ([[Stage]]) are sets of tasks that compute intermediate results in jobs, where each
 *    task computes the same function on partitions of the same RDD. Stages are separated at shuffle
 *    boundaries, which introduce a barrier (where we must wait for the previous stage to finish to
 *    fetch outputs). There are two types of stages: [[ResultStage]], for the final stage that
 *    executes an action, and [[ShuffleMapStage]], which writes map output files for a shuffle.
 *    Stages are often shared across multiple jobs, if these jobs reuse the same RDDs.
  *
  *    Stages([[Stage]])是在作业中计算中间结果的任务集，每个任务在同一个RDD的分区上计算同一个函数。
  *    stage在shuffl边界被分开，它引入了一个barrier(在这里我们必须等待上一个stage来完成获取输出)。
  *    有两种类型的阶段Stages:[[ResultStage]]，用于执行一个操作的最后阶段，以及[[ShuffleMapStage]]，
  *    它为一个shuffle编写映射输出文件。如果这些作业重用相同的RDDs，则通常会在多个作业之间共享阶段。
 *
 *  - Tasks are individual units of work, each sent to one machine.
  *  v 任务是单个的工作单元，每个单元都发送到一台机器上。
 *
 *  - Cache tracking: the DAGScheduler figures out which RDDs are cached to avoid recomputing them
 *    and likewise remembers which shuffle map stages have already produced output files to avoid
 *    redoing the map side of a shuffle.
  *
  *    缓存跟踪:DAGScheduler指明了那些RDDs被缓存，以避免重新计算它们，同样的，shuffle map stages已经产生了输出文件，
  *    以避免重做shuffle的map side。
 *
 *  - Preferred locations: the DAGScheduler also computes where to run each task in a stage based
 *    on the preferred locations of its underlying RDDs, or the location of cached or shuffle data.
  *    首选位置:DAGScheduler还根据底层RDDs的首选位置，或缓存或洗牌数据的位置，计算在一个阶段中运行每个任务的位置。
 *
 *  - Cleanup: all data structures are cleared when the running jobs that depend on them finish,
 *    to prevent memory leaks in a long-running application.
  *    清理:所有数据结构在运行依赖于它们的运行作业完成时被清除，以防止长时间运行的应用程序中的内存泄漏。
 *
 * To recover from failures, the same stage might need to run multiple times, which are called
 * "attempts". If the TaskScheduler reports that a task failed because a map output file from a
 * previous stage was lost, the DAGScheduler resubmits that lost stage. This is detected through a
 * CompletionEvent with FetchFailed, or an ExecutorLost event. The DAGScheduler will wait a small
 * amount of time to see whether other nodes or tasks fail, then resubmit TaskSets for any lost
 * stage(s) that compute the missing tasks. As part of this process, we might also have to create
 * Stage objects for old (finished) stages where we previously cleaned up the Stage object. Since
 * tasks from the old attempt of a stage could still be running, care must be taken to map any
 * events received in the correct Stage object.
  *
  * 为了从失败中恢复，同样的阶段stage可能需要运行多次，这被称为“尝试”"attempts"。如果TaskScheduler报告一个任务失败了，
  * 因为之前一个阶段的映射输出文件丢失了，那么DAGScheduler就会重新提交丢失的阶段。这是通过一个与FetchFailed或ExecutorLost事件的
  * CompletionEvent来检测的。DAGScheduler将等待一个小的查看其他节点或任务是否失败的时间，然后重新提交任务集，
  * 用于计算丢失任务的任何丢失阶段。作为这个过程的一部分，我们还可能需要为旧的(已完成的)阶段创建阶段对象，
  * 之前我们已经清理了阶段对象。由于一个阶段的旧尝试的任务仍然可以运行，所以必须小心地将接收到的任何事件映射到
  * 正确的阶段对象中。
 *
 * Here's a checklist to use when making or reviewing changes to this class:
  * 这里有一个检查表，用来制作或审查这个类的变化:
 *
 *  - All data structures should be cleared when the jobs involving them end to avoid indefinite
 *    accumulation of state in long-running programs.
  *    当涉及到的工作结束时，所有数据结构都应该被清除，以避免在长时间运行的程序中无限期地积累状态
 *
 *  - When adding a new data structure, update `DAGSchedulerSuite.assertDataStructuresEmpty` to
 *    include the new structure. This will help to catch memory leaks.
  *
  *    在添加新的数据结构时，更新“dagupdatersuite”。assertDataStructuresEmpty”包括新结构。
  *    这将有助于捕获内存泄漏。
  *
  *
  *   DAGScheduler是主要产生各类SparkListenterEvent的源头，它将各种SparkListenterEvent发送到listenterBus的
  *   事件的源头，它将各种SparkListenterEvent发送到listenerBus的事件队列中，listenerBus通过定时器将
  *   SparkListenerEvent事件匹配到具体的SparkListener,改变SparkListener中的统计监控数据，最终由SparkUI的界面
  *   进行展示。
  *
  *     DAGScheduler的数据结构主要是维护jobId和stageId的关系，Stage,ActiveJob,以及缓存的RDD的partitions的
  *  位置信息。
  *
  *   DAGScheduler负责将Task拆分成不同Stage的具有依赖关系（包含RDD的依赖关系）的多批任务，然后提交给TaskScheduler
  *  进行具体处理。DAG全称 Directed Acyclic Graph，有向无环图。简单的来说，就是一个由顶点和有方向性的边构成的图，从任
  *  意一个顶点出发，没有任何一条路径会将其带回到出发的顶点。
  *
  *  在作业调度系统中，调度的基础就在于判断多个作业任务的依赖关系，这些任务之间可能存在多重的依赖关系，也就是说有些任务必须
  *  先获得执行，然后另外的相关依赖任务才能执行，但是任务之间显然不应该出现任何直接或间接的循环依赖关系，所以本质上这种关系
  *  适合用DAG有向无环图来表示。
  *
  *  ====>
  *  作业调度核心——DAGScheduler
  *     用户代码都是基于RDD的一系列计算操作，实际运行时，这些计算操作是Lazy执行的，并不是所有的RDD操作都会触发Spark往
  *   Cluster上提交实际作业，基本上只有一些需要返回数据或者向外部输出的操作才会触发实际计算工作（Action算子），其它的
  *   变换操作基本上只是生成对应的RDD记录依赖关系（Transformation算子）。
  *
  *   在这些RDD.Action操作中（如count,collect）会自动触发runJob提交作业，不需要用户显式的提交作业（这一部分可以看
  *   下Spark DAGSheduler生成Stage过程分析实验）
  *
  *   作业调度的两个主要入口是submitJob 和 runJob，两者的区别在于前者返回一个Jobwaiter对象，可以用在异步调用中，
  *   用来判断作业完成或者取消作业，runJob在内部调用submitJob，阻塞等待直到作业完成（或失败）
  *
 */
private[spark]
class DAGScheduler(
    private[scheduler] val sc: SparkContext,  // 获得当前SparkContext对象
    private[scheduler] val taskScheduler: TaskScheduler, // 获得当前saprkContext内置的taskScheduler
    listenerBus: LiveListenerBus,              // 异步处理事件的对象，从sc中获取
    mapOutputTracker: MapOutputTrackerMaster,  //运行在Driver端管理shuffle map task的输出，从sc中获取
    blockManagerMaster: BlockManagerMaster,    //运行在driver端，管理整个Job的Block信息，从sc中获取
    env: SparkEnv,                             // 从sc中获取
    clock: Clock = new SystemClock())
  extends Logging {

  def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
    this(
      sc,
      taskScheduler,
      sc.listenerBus,
      sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      sc.env.blockManager.master,
      sc.env)
  }

  def this(sc: SparkContext) = this(sc, sc.taskScheduler)

  // 在DAGScheduler的源代码中，定义了很多变量，在刚构造出来时，仅仅只是初始化这些变量，具体使用是在后面Job提交的过程中了。

  // DAGSchedulerSource的测量信息是job和Satge相关的信息
  private[spark] val metricsSource: DAGSchedulerSource = new DAGSchedulerSource(this)

  // 生成JobId
  private[scheduler] val nextJobId = new AtomicInteger(0)
  // 总的Job数
  private[scheduler] def numTotalJobs: Int = nextJobId.get()
  // 下一个StageId
  private val nextStageId = new AtomicInteger(0)

  // 记录某个job对应的包含的所有stage
  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  // 记录StageId对应的Stage
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage]
  /**
   * Mapping from shuffle dependency ID to the ShuffleMapStage that will generate the data for
   * that dependency. Only includes stages that are part of currently running job (when the job(s)
   * that require the shuffle stage complete, the mapping will be removed, and the only record of
   * the shuffle data will be in the MapOutputTracker).
    *
    *   从shuffle依赖ID映射到ShuffleMapStage，它将生成该依赖项的数据。只包括当前正在运行的工作的一部分(当需要进行
    * shuffle阶段完成的作业时，映射将被删除，而shuffle数据的唯一记录将在MapOutputTracker中)。
    *
    * // 记录每一个shuffle对应的ShuffleMapStage，key为shuffleId
   */
  private[scheduler] val shuffleIdToMapStage = new HashMap[Int, ShuffleMapStage]

  // 记录处于Active状态的job，key为jobId, value为ActiveJob类型对象
  private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]

  // Stages we need to run whose parents aren't done
  // 等待运行的Stage，一般这些是在等待Parent Stage运行完成才能开始
  private[scheduler] val waitingStages = new HashSet[Stage]

  // Stages we are running right now
  // 处于Running状态的Stage
  private[scheduler] val runningStages = new HashSet[Stage]

  // Stages that must be resubmitted due to fetch failures
  // 失败原因为fetch failures的Stage，并等待重新提交
  private[scheduler] val failedStages = new HashSet[Stage]

  // active状态的Job列表
  private[scheduler] val activeJobs = new HashSet[ActiveJob]

  /**
   * Contains the locations that each RDD's partitions are cached on.  This map's keys are RDD ids
   * and its values are arrays indexed by partition numbers. Each array value is the set of
   * locations where that RDD partition is cached.
    *
    * 包含每个RDD分区被缓存的位置。该映射的键是RDD id，它的值是由分区号索引的数组。每个数组值都是缓存RDD分区的位置集。
   *
   * All accesses to this map should be guarded by synchronizing on it (see SPARK-4454).
    * 所有对这张map的访问都应该通过同步进行保护(见spark - 4454)。
   */
  private val cacheLocs = new HashMap[Int, IndexedSeq[Seq[TaskLocation]]]

  // For tracking failed nodes, we use the MapOutputTracker's epoch number, which is sent with
  // every task. When we detect a node failing, we note the current epoch number and failed
  // executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask results.
  //
  // TODO: Garbage collect information about failure epochs when we know there are no more
  //       stray messages to detect.
  /**
    * 对于跟踪失败的节点，我们使用MapOutputTracker的纪元号(epoch number)，它是由每个任务发送的。当我们发现一个节点失败时，
    * 我们注意到当前的纪元编号(epoch number)和失败的执行器，将它增加到新的任务，并使用它忽略掉杂的ShuffleMapTask结果。
    */
  private val failedEpoch = new HashMap[String, Long]

  // 输出提交控制
  private [scheduler] val outputCommitCoordinator = env.outputCommitCoordinator

  // A closure serializer that we reuse.
  // This is only safe because DAGScheduler runs in a single thread.
  /**
    * 我们重用的闭包序列化器。
    * 这是唯一安全的，因为DAGScheduler在一个线程中运行。
    */
  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()

  /** If enabled, FetchFailed will not cause stage retry, in order to surface the problem.
    * 如果启用，FetchFailed将不会导致stage重试，以解决问题。
    * */
  private val disallowStageRetryForTest = sc.getConf.getBoolean("spark.test.noStageRetry", false)

  /**
   * Number of consecutive stage attempts allowed before a stage is aborted.
    * 在舞台被中止之前，连续的阶段尝试的次数。
   */
  private[scheduler] val maxConsecutiveStageAttempts =
    sc.getConf.getInt("spark.stage.maxConsecutiveAttempts",
      DAGScheduler.DEFAULT_MAX_CONSECUTIVE_STAGE_ATTEMPTS)

  private val messageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("dag-scheduler-message")

  // 这里创建 DAGSchedulerEvent事件处理对象（相当于一个路由器，比方说他是现实中的指引者，你post各种事件过去，就是问，我去张家界去哪？
  // 他告诉你石家庄，你问我去九寨沟，他告诉你去四川省阿坝藏族），这个DAGSchedulerEventProcessLoop类就在这个文件中
  private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)

  /** 给taskScheduler设置一个DAGScheduler，为什么这样做呢？
    * 因为task是先生成DAG有向无环图，然后运行task,taskScheduler调度需要参考DAG图，所以必须把这两个联系在一起
    * 在TaskSchedulerImpl实现类中setDAGScheduler内容为this.dagScheduler = dagScheduler
    */
  taskScheduler.setDAGScheduler(this)

  /**
   * Called by the TaskSetManager to report task's starting.
    * 由TaskSetManager调用来报告任务的开始。
   */
  def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    // DAGSchedulerEventProcessLoop的doOnReceive(event: DAGSchedulerEvent)处理
    eventProcessLoop.post(BeginEvent(task, taskInfo))
  }

  /**
   * Called by the TaskSetManager to report that a task has completed
   * and results are being fetched remotely.
    *
    * 由TaskSetManager调用，报告任务已经完成，结果被远程提取。
   */
  def taskGettingResult(taskInfo: TaskInfo) {
    eventProcessLoop.post(GettingResultEvent(taskInfo))
  }

  /**
   * Called by the TaskSetManager to report task completions or failures.
    *
    * 被TaskSetManager调用，报告task任务完成还是失败了
   */
  def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Seq[AccumulatorV2[_, _]],
      taskInfo: TaskInfo): Unit = {
    //加入DAGScheduler的消息队列，等待处理
    eventProcessLoop.post(
      CompletionEvent(task, reason, result, accumUpdates, taskInfo))
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
    *
    * 更新正在执行的任务的度量标准，并让master知道BlockManager还活着。如果驱动程序知道给定的块管理器（block manager），
    * 则返回true。否则，返回false，指示区块管理器（block manager）应该重新注册。
   */
  def executorHeartbeatReceived(
      execId: String,
      // (taskId, stageId, stageAttemptId, accumUpdates)
      accumUpdates: Array[(Long, Int, Int, Seq[AccumulableInfo])],
      blockManagerId: BlockManagerId): Boolean = {
    //dagScheduler将execID,accumUpdates封装成SparkListenerExecutorMetricsUpdate事件，并且post到ListenerBus中，此事件用于更新Satge
    //的各种测量数据，由SparkListenerBus的doPostEvent方法执行
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, accumUpdates))

    // 最后给BlockManagerManagerMaster持有的BlockMasterEndpoint发送BlockManagerHeartbeat消息。BlockManagerMasterEndpoint在
    // 收到消息后会匹配执行heartbeatReceived方法，由BlockManagerMasterEndpoint类中的receiveAndReply方法执行
    blockManagerMaster.driverEndpoint.askSync[Boolean](
      BlockManagerHeartbeat(blockManagerId), new RpcTimeout(600 seconds, "BlockManagerHeartbeat"))
  }

  /**
   * Called by TaskScheduler implementation when an executor fails.
    * 当执行程序失败时，由TaskScheduler执行调用。
   */
  def executorLost(execId: String, reason: ExecutorLossReason): Unit = {
    eventProcessLoop.post(ExecutorLost(execId, reason))
  }

  /**
   * Called by TaskScheduler implementation when a host is added.
    * 在添加主机时由TaskScheduler实现调用。
   */
  def executorAdded(execId: String, host: String): Unit = {
    eventProcessLoop.post(ExecutorAdded(execId, host))
  }

  /**
   * Called by the TaskSetManager to cancel an entire TaskSet due to either repeated failures or
   * cancellation of the job itself.
    * 由TaskSetManager调用来取消整个任务集，原因是重复的失败或作业本身的取消。
   */
  def taskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable]): Unit = {
    eventProcessLoop.post(TaskSetFailed(taskSet, reason, exception))
  }

  private[scheduler]
  def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]] = cacheLocs.synchronized {
    // Note: this doesn't use `getOrElse()` because this method is called O(num tasks) times
    if (!cacheLocs.contains(rdd.id)) {
      // Note: if the storage level is NONE, we don't need to get locations from block manager.
      val locs: IndexedSeq[Seq[TaskLocation]] = if (rdd.getStorageLevel == StorageLevel.NONE) {
        IndexedSeq.fill(rdd.partitions.length)(Nil)
      } else {
        val blockIds =
          rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
        blockManagerMaster.getLocations(blockIds).map { bms =>
          bms.map(bm => TaskLocation(bm.host, bm.executorId))
        }
      }
      cacheLocs(rdd.id) = locs
    }
    cacheLocs(rdd.id)
  }

  private def clearCacheLocs(): Unit = cacheLocs.synchronized {
    cacheLocs.clear()
  }

  /**
   * Gets a shuffle map stage if one exists in shuffleIdToMapStage. Otherwise, if the
   * shuffle map stage doesn't already exist, this method will create the shuffle map stage in
   * addition to any missing ancestor shuffle map stages.
    *
    *   如果在shuffleIdToMapStage中存在一个shuffle map stage。否则，如果shuffle map stage不存在，则该方法将创建
    * shuffle map stage，并添加到任何缺失的祖先shuffle map stage。
   */
  private def getOrCreateShuffleMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    // shuffleIdToMapStage存储的是HashMap[Int, ShuffleMapStage]
    shuffleIdToMapStage.get(shuffleDep.shuffleId) match {
      //如果原本shuffleIdToMapStage中就有ShuffleMapStage ，直接返回
      case Some(stage) =>
        stage

      // 如果没有，调用getMissingAncestorShuffleDependencies找到祖先的宽依赖
      case None =>
        // Create stages for all missing ancestor shuffle dependencies. 为所有丢失的祖先shuffle依赖项创建stages。
        getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
          // Even though getMissingAncestorShuffleDependencies only returns shuffle dependencies
          // that were not already in shuffleIdToMapStage, it's possible that by the time we
          // get to a particular dependency in the foreach loop, it's been added to
          // shuffleIdToMapStage by the stage creation process for an earlier dependency. See
          // SPARK-13902 for more information.
          /**
            * 尽管getMissingAncestorShuffleDependencies只返回在shuffle依赖（dependencies），这些不在shuffleIdToMapStage中,
            * 有可能的时候我们到达一个特定的依赖在foreach循环中,它被添加到shuffleIdToMapStage早期阶段创建过程的依赖。更多信息见spark - 13902。
            *
            */
          if (!shuffleIdToMapStage.contains(dep.shuffleId)) {
            createShuffleMapStage(dep, firstJobId)
          }
        }
        // Finally, create a stage for the given shuffle dependency.
        /**
          * 这里存在递归调用：createResultStage（）--> getOrCreateParentStages()-->  getOrCreateShuffleMapStage（）
          *                                                 |                      |
          *                                                 |                      |
          *                                                 |                      |
          *                                                 >                      |
          *                                            createShuffleMapStage（） <--
          */
        createShuffleMapStage(shuffleDep, firstJobId)
    }
  }

  /**
   * Creates a ShuffleMapStage that generates the given shuffle dependency's partitions. If a
   * previously run stage generated the same shuffle data, this function will copy the output
   * locations that are still available from the previous shuffle to avoid unnecessarily
   * regenerating data.
    *
    *   创建一个ShuffleMapStage，生成给定的洗牌依赖项的分区。如果先前运行的阶段生成相同的shuffle数据，此函数将
    * 复制之前的洗牌中仍然可用的输出位置，以避免不必要的重新生成数据。
    *
    * 这个方法以前是：newOrUsedShuffleStage
   */
  def createShuffleMapStage(shuffleDep: ShuffleDependency[_, _, _], jobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    // 根据当前rdd的paritions个数，计算出当前Stage的task个数。
    val numTasks = rdd.partitions.length
    /**
      * 这里存在递归调用：createResultStage（）--> getOrCreateParentStages()-->  getOrCreateShuffleMapStage（）
      *                                                 |                      |
      *                                                 |                      |
      *                                                 |                      |
      *                                                 >                      |
      *                                            createShuffleMapStage（） <--
      *
      */
    val parents = getOrCreateParentStages(rdd, jobId)
    // 生成当前stage的stageId。同一Application中Stage初始编号为0
    val id = nextStageId.getAndIncrement()
    // 为当前rdd生成ShuffleMapStage
    val stage = new ShuffleMapStage(id, rdd, numTasks, parents, jobId, rdd.creationSite, shuffleDep)

    stageIdToStage(id) = stage
    shuffleIdToMapStage(shuffleDep.shuffleId) = stage
    updateJobIdStageIdMaps(jobId, stage)

    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      // A previously run stage generated partitions for this shuffle, so for each output
      // that's still available, copy information about that output location to the new stage
      // (so we don't unnecessarily re-compute that data).
      // 如果当前shuffle已经在MapOutputTracker中注册过
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      // 更新Shuffle的Shuffle Write路径
      (0 until locs.length).foreach { i =>
        if (locs(i) ne null) {
          // locs(i) will be null if missing
          stage.addOutputLoc(i, locs(i))
        }
      }
    } else {
      // 如果当前Shuffle没有在MapOutputTracker中注册过
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
      // 注册
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
    }
    stage
  }

  /**
   * Create a ResultStage associated with the provided jobId.
    * 创建与提供的jobId相关的结果阶段。
    *
    * 1.5版本中函数名为newStage
    * 5.4.2 finalStage的创建与Stage的划分
    *   在Spark中，一个Job可能被划分为一个多个Stage，各个之间存在依赖关系，其中最下游的Stage也成为最终的Stage，
    *   用来处理Job最后阶段的工作。
    *
    *  处理步骤如下：
    *   1.调用getOrCreateParentStages获取所有的父Stage的列表，父Stage主要是宽依赖（如ShuffleDependency）对应的Stage,此列表内的Stage
    *     包含以下几种：
    *         (1).当前RDD的直接或者间接的依赖是ShuffleDependency且已经注册过的Stage.
    *         (2).当前RDD的直接或者间接的依赖是ShuffleDependency且没有注册过Stage的。则根据ShuffleDependency本身的RDD,
    *             找到它的直接或者间接的依赖是ShuffleDependency且没有注册过Stage的所有ShuffleDependency，为他们生成Stage并且注册。
    *         (3).当前RDD的直接或者间接的依赖是ShuffleDependency且没有注册过Stage的。为它们生成Stage并且注册，最后也添加到Stage到List.
    *   2.生成Stage的Id,并且创建Stage
    *   3.将Satge注册到stageIdToSatge = new HashMap[Int,Satge]中。
    *   4.调用updateJobIdSatgeIdMaps方法Satge及其祖先Stage与jobId的对应关系。
   */
  private def createResultStage(
      rdd: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      jobId: Int,
      callSite: CallSite): ResultStage = {
    // 调用getOrCreateParentStages获取所有的父Stage的列表，父Stage主要是宽依赖（如ShuffleDependency）对应的Stage
    /**
      * 这里存在递归调用：createResultStage（）--> getOrCreateParentStages()-->  getOrCreateShuffleMapStage（）
      *                                                 |                      |
      *                                                 |                      |
      *                                                 |                      |
      *                                                 >                      |
      *                                            createShuffleMapStage（） <--
      */
    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    // ResultStage继承Stage 但是里面感觉好空，没啥方法
    val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
    // 记录StageId对应的Stage
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }

  /**
   * Get or create the list of parent stages for a given RDD.  The new Stages will be created with
   * the provided firstJobId.
    *
    * 获取或创建给定RDD的父级列表。新阶段将由提供的firstJobId创建。
   */
  private def getOrCreateParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
    /**
      *
      * 首先是getOrCreateParentStages，以当前的rdd和jobid作为参数，返回一个List(parentStage,id),前面的代表这个当前的
      * resultStage所依赖的全部stage，后面的就是返回当前stage的id
      *
      * getShuffleDependencies方法只会返回直接父依赖（意思是只返回你爸爸，不返回你爷爷），而这个getOrCreateParentStages()是被createResultStage（）
      * 方法调用的。也就是说传入的是最后一个Stage,举个例子：
      *
      */
    getShuffleDependencies(rdd).map { shuffleDep =>
      getOrCreateShuffleMapStage(shuffleDep, firstJobId)
    }.toList
  }

  /** Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet
    * 找到祖先shuffle的依赖，这些依赖还没有注册到shuffleToMapStage中
    * */
  private def getMissingAncestorShuffleDependencies(
      rdd: RDD[_]): Stack[ShuffleDependency[_, _, _]] = {
    //尚未提交的父stages
    val ancestors = new Stack[ShuffleDependency[_, _, _]]
    //已经处理过的RDD
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    // 未处理的将存入这个栈
    val waitingForVisit = new Stack[RDD[_]]
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.pop()
      if (!visited(toVisit)) {
        visited += toVisit
        getShuffleDependencies(toVisit).foreach { shuffleDep =>
          if (!shuffleIdToMapStage.contains(shuffleDep.shuffleId)) {
            ancestors.push(shuffleDep)
            waitingForVisit.push(shuffleDep.rdd)
          } // Otherwise, the dependency and its ancestors have already been registered.
        }
      }
    }
    ancestors
  }

  /**
   * Returns shuffle dependencies that are immediate parents of the given RDD.
   *
   * This function will not return more distant ancestors.  For example, if C has a shuffle
   * dependency on B which has a shuffle dependency on A:
   *
   * A <-- B <-- C
   *
   * calling this function with rdd C will only return the B <-- C dependency.
   *
   * This function is scheduler-visible for the purpose of unit testing.
    *
    * 返回给给定RDD的直接父类的重组依赖项。
    * 这个函数不会返回更远的祖先。例如，如果C对B有一个shuffle依赖，而且B对a有一个洗牌依赖:
    *     A <-- B <-- C
    * 使用rdd C调用这个函数只会返回B <- C依赖项。
    * 这个函数是用于单元测试的调度程序。
   */
  private[scheduler] def getShuffleDependencies(
      rdd: RDD[_]): HashSet[ShuffleDependency[_, _, _]] = {
    val parents = new HashSet[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // 新建一个栈
    val waitingForVisit = new Stack[RDD[_]]
    // 将传入的RDD入栈
    waitingForVisit.push(rdd)
    // 判断栈是否为空，只要不为空就循环处理
    while (waitingForVisit.nonEmpty) {
      // 取出站定的元素
      val toVisit = waitingForVisit.pop()
      // 如果visited里面不包括这个toVisit
      if (!visited(toVisit)) {
        // 加入到visited
        visited += toVisit
        // 这里又不懂了，等待以后
        toVisit.dependencies.foreach {
          case shuffleDep: ShuffleDependency[_, _, _] =>
            parents += shuffleDep
          case dependency =>
            waitingForVisit.push(dependency.rdd)
        }
      }
    }
    parents
  }

  /**
    * getMissingParentStages方法用来找到Stage所有不可用的祖先Stage.
    *
    *   如何判断Stage可用？他的判断十分简单：如果Stage不是Map任务，那么它是可用的；
    *  否则它的已经输出计算结果的分区任务数量要和分区数一样，即所有分区上的子任务都要完成。
    *
    *  寻找父Stage的方法。首先根据当前Stage中的RDD，得到RDD的一阿里关系。如果依赖关系是宽依赖，则生成一个
    *  mapSrtage来作为当前Stage的父Stage（因为有个多父亲，肯定要用map来存储）。如果依赖关系是窄依赖，不会
    *  生成新的Stage.
    *
    *  对于不需要Shuffle操作的Job，他只需要一个finalStage,对于需要Shuffle操作dejob，会生成mapStage和
    *  finalStage这两种Stage.
    */
  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    // 我们手动维护一个栈来防止由于递归访问堆栈溢出错误
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)
        if (rddHasUncachedPartitions) {
          for (dep <- rdd.dependencies) {
            dep match {
              //如果是ShuffleDependency，则生成ShuffleMapStage，将其加入到missing
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }

              //如果是NarrowDependency，则检查这个rdd依赖的上一个rdd
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.push(narrowDep.rdd)
            }
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }

  /**
   * Registers the given jobId among the jobs that need the given stage and
   * all of that stage's ancestors.
    * 在需要给定阶段和所有这些阶段的祖先的工作中注册给定的jobId。
   */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {
    @tailrec
    def updateJobIdStageIdMapsList(stages: List[Stage]) {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parentsWithoutThisJobId = s.parents.filter { ! _.jobIds.contains(jobId) }
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    }
    updateJobIdStageIdMapsList(List(stage))
  }

  /**
   * Removes state for job and any stages that are not needed by any other job.  Does not
   * handle cancelling tasks or notifying the SparkListener about finished jobs/stages/tasks.
    *
    * 删除工作状态和任何其他工作不需要的阶段。不处理取消任务或通知SparkListener关于完成的工作/阶段/任务。
   *
   * @param job The job whose state to cleanup.
   */
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob): Unit = {
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    } else {
      stageIdToStage.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds
          if (!jobSet.contains(job.jobId)) {
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
              .format(job.jobId, stageId))
          } else {
            def removeStage(stageId: Int) {
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                for ((k, v) <- shuffleIdToMapStage.find(_._2 == stage)) {
                  shuffleIdToMapStage.remove(k)
                }
                if (waitingStages.contains(stage)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waitingStages -= stage
                }
                if (failedStages.contains(stage)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failedStages -= stage
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId
              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }

            jobSet -= job.jobId
            if (jobSet.isEmpty) { // no other job needs this stage
              removeStage(stageId)
            }
          }
      }
    }
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    job.finalStage match {
      case r: ResultStage => r.removeActiveJob()
      case m: ShuffleMapStage => m.removeActiveJob(job)
    }
  }

  /**
   * Submit an action job to the scheduler.
    *
    * submitJob方法用来将一个Job提交到Job scheduler.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @return a JobWaiter object that can be used to block until the job finishes executing
   *         or can be used to cancel the job.
   *
   * @throws IllegalArgumentException when partitions ids are illegal
    *
    * submitJob的处理步骤如下：
    *   1.调用RDD的partitions函数来获取当前Job的最大分区数，即maxPartitions。根据maxPartitions，确认我们没有在一个不存在的
    *     partition上运行任务。
    *   2.生成当前的Job的jobId;
    *   3.创建JobWaiter，望文生义，即Job的服务员。
    *   4.向eventProcessLoop发送JobSubmitted事件（这里的eventProcessLoop就是DAGSchedulerEventProcessLoop）
    *   5.返回JobWaiter.
    *
    *
    *     进入submitJob方法，首先会去检查rdd的分区信息，在确保rdd分区信息正确的情况下，给当前job生成一个jobId，nexJobId在刚构造出来时是从0开始编号的，
    *   在同一个SparkContext中，jobId会逐渐顺延。然后构造出一个JobWaiter对象返回给上一级调用函数。通过上面提到的eventProcessLoop提交该任务，
    *   最终会调用到DAGScheduler.handleJobSubmitted来处理这次提交的Job。handleJobSubmitted在下面的Stage划分部分会有提到。
    *
   */
  def submitJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    // 检查确保我们没有在不存在的分区上启动任务。
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    // 2.生成当前的Job的jobId;
    val jobId = nextJobId.getAndIncrement()
    // 因为nexJobId在刚构造出来时是从0开始编号的，所以如果为0，就说明这个job还没运行，可以直接返回JobWaiter
    if (partitions.size == 0) {
      // Return immediately if the job is running 0 tasks
      // 如果作业运行为0，则立即返回
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    // 3.创建JobWaiter，望文生义，即Job的服务员。
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)

    // 4.向eventProcessLoop发送JobSubmitted事件（这里的eventProcessLoop就是DAGSchedulerEventProcessLoop）
    //   这里开始运行job吧，这里先生成一个DAGSchedulerEvent类型的JobSubmitted（这是一个类，里面啥都没有）的事件对象
    //   然后传递给eventProcessLoop去处理
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))

    // 5.返回JobWaiter.
    waiter
  }





  /**
   * Run an action job on the given RDD and pass all the results to the resultHandler function as
   * they arrive.
    *
    * 在给定的RDD上运行一个操作任务，并将所有结果传递给resultHandler函数。
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @note Throws `Exception` when the job fails
    *
    *
    *  这个在SparkContext中被调用
   */
  def runJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): Unit = {
    val start = System.nanoTime
    /**
      * 调用DAGScheduler.submitJob方法后会得到一个JobWaiter实例来监听Job的执行情况。针对Job的Succeeded状态和Failed状态，
      * 在接下来代码中都有不同的处理方式。
      * */
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
    // 线程阻塞，不释放锁，时间到了，会继续运行。等待job提交完成后，异步返回的waiter（submit是异步提交）
    // 阻塞住 jobWaiter  直到jobWaiter 知道 submitJob运行 失败还是成功  也就是说 等到completionFuture 有状态值
    ThreadUtils.awaitReady(waiter.completionFuture, Duration.Inf)
    // 如果成功就打印成功日志，否则打印失败日志
    waiter.completionFuture.value.get match {
      case scala.util.Success(_) =>
        logInfo("Job %d finished: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case scala.util.Failure(exception) =>
        logInfo("Job %d failed: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }

  /**
   * Run an approximate job on the given RDD and pass all the results to an ApproximateEvaluator
   * as they arrive. Returns a partial result object from the evaluator.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param evaluator `ApproximateEvaluator` to receive the partial results
   * @param callSite where in the user program this job was called
   * @param timeout maximum time to wait for the job, in milliseconds
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   */
  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      callSite: CallSite,
      timeout: Long,
      properties: Properties): PartialResult[R] = {
    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val partitions = (0 until rdd.partitions.length).toArray
    val jobId = nextJobId.getAndIncrement()
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions, callSite, listener, SerializationUtils.clone(properties)))
    listener.awaitResult()    // Will throw an exception if the job fails
  }

  /**
   * Submit a shuffle map stage to run independently and get a JobWaiter object back. The waiter
   * can be used to block until the job finishes executing or can be used to cancel the job.
   * This method is used for adaptive query planning, to run map stages and look at statistics
   * about their outputs before submitting downstream stages.
   *
   * @param dependency the ShuffleDependency to run a map stage for
   * @param callback function called with the result of the job, which in this case will be a
   *   single MapOutputStatistics object showing how much data was produced for each partition
   * @param callSite where in the user program this job was submitted
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   */
  def submitMapStage[K, V, C](
      dependency: ShuffleDependency[K, V, C],
      callback: MapOutputStatistics => Unit,
      callSite: CallSite,
      properties: Properties): JobWaiter[MapOutputStatistics] = {

    val rdd = dependency.rdd
    val jobId = nextJobId.getAndIncrement()
    if (rdd.partitions.length == 0) {
      throw new SparkException("Can't run submitMapStage on RDD with 0 partitions")
    }

    // We create a JobWaiter with only one "task", which will be marked as complete when the whole
    // map stage has completed, and will be passed the MapOutputStatistics for that stage.
    // This makes it easier to avoid race conditions between the user code and the map output
    // tracker that might result if we told the user the stage had finished, but then they queries
    // the map output tracker and some node failures had caused the output statistics to be lost.
    val waiter = new JobWaiter(this, jobId, 1, (i: Int, r: MapOutputStatistics) => callback(r))
    eventProcessLoop.post(MapStageSubmitted(
      jobId, dependency, callSite, waiter, SerializationUtils.clone(properties)))
    waiter
  }

  /**
   * Cancel a job that is running or waiting in the queue.
   */
  def cancelJob(jobId: Int, reason: Option[String]): Unit = {
    logInfo("Asked to cancel job " + jobId)
    eventProcessLoop.post(JobCancelled(jobId, reason))
  }

  /**
   * Cancel all jobs in the given job group ID.
   */
  def cancelJobGroup(groupId: String): Unit = {
    logInfo("Asked to cancel job group " + groupId)
    eventProcessLoop.post(JobGroupCancelled(groupId))
  }

  /**
   * Cancel all jobs that are running or waiting in the queue.
    * 取消正在运行或在队列中等待的所有作业。
   */
  def cancelAllJobs(): Unit = {
    eventProcessLoop.post(AllJobsCancelled)
  }

  private[scheduler] def doCancelAllJobs() {
    // Cancel all running jobs.
    runningStages.map(_.firstJobId).foreach(handleJobCancellation(_,
      Option("as part of cancellation of all jobs")))
    activeJobs.clear() // These should already be empty by this point,
    jobIdToActiveJob.clear() // but just in case we lost track of some jobs...
  }

  /**
   * Cancel all jobs associated with a running or scheduled stage.
   */
  def cancelStage(stageId: Int, reason: Option[String]) {
    eventProcessLoop.post(StageCancelled(stageId, reason))
  }

  /**
   * Kill a given task. It will be retried.
   *
   * @return Whether the task was successfully killed.
   */
  def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean = {
    taskScheduler.killTaskAttempt(taskId, interruptThread, reason)
  }

  /**
   * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
   * the last fetch failure.
   */
  private[scheduler] def resubmitFailedStages() {
    if (failedStages.size > 0) {
      // Failed stages may be removed by job cancellation, so failed might be empty even if
      // the ResubmitFailedStages event has been scheduled.
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.firstJobId)) {
        submitStage(stage)
      }
    }
  }

  /**
   * Check for waiting stages which are now eligible for resubmission.
   * Submits stages that depend on the given parent stage. Called when the parent stage completes
   * successfully.
    *
    * 检查现在有资格重新提交的等待stages。
    * 提交依赖于给定父阶段的stages。调用父stages成功完成。
   */
  private def submitWaitingChildStages(parent: Stage) {
    logTrace(s"Checking if any dependencies of $parent are now runnable")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    val childStages = waitingStages.filter(_.parents.contains(parent)).toArray
    waitingStages --= childStages
    for (stage <- childStages.sortBy(_.firstJobId)) {
      submitStage(stage)
    }
  }

  /** Finds the earliest-created active job that needs the stage */
  // TODO: Probably should actually find among the active jobs that need this
  // stage the one with the highest priority (highest-priority pool, earliest created).
  // That should take care of at least part of the priority inversion problem with
  // cross-job dependencies.
  private def activeJobForStage(stage: Stage): Option[Int] = {
    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted
    jobsThatUseStage.find(jobIdToActiveJob.contains)
  }

  private[scheduler] def handleJobGroupCancelled(groupId: String) {
    // Cancel all jobs belonging to this job group.
    // First finds all active jobs with this group id, and then kill stages for them.
    val activeInGroup = activeJobs.filter { activeJob =>
      Option(activeJob.properties).exists {
        _.getProperty(SparkContext.SPARK_JOB_GROUP_ID) == groupId
      }
    }
    val jobIds = activeInGroup.map(_.jobId)
    jobIds.foreach(handleJobCancellation(_,
        Option("part of cancelled job group %s".format(groupId))))
  }


  // 由doOnReceive(event: DAGSchedulerEvent)方法调用，处理报告任务开始
  private[scheduler] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo) {
    // Note that there is a chance that this task is launched after the stage is cancelled.
    // In that case, we wouldn't have the stage anymore in stageIdToStage.
    // 请注意，这个任务在被取消后才有可能启动。在这种情况下，我们在stageIdToStage上就不会有stage了。
    val stageAttemptId = stageIdToStage.get(task.stageId).map(_.latestInfo.attemptId).getOrElse(-1)
    // 调用LiveListenerBus的post方法，将SparkListenerTaskStart添加到队列队尾
    listenerBus.post(SparkListenerTaskStart(task.stageId, stageAttemptId, taskInfo))
  }

  private[scheduler] def handleTaskSetFailed(
      taskSet: TaskSet,
      reason: String,
      exception: Option[Throwable]): Unit = {
    stageIdToStage.get(taskSet.stageId).foreach { abortStage(_, reason, exception) }
  }

  private[scheduler] def cleanUpAfterSchedulerStop() {
    for (job <- activeJobs) {
      val error =
        new SparkException(s"Job ${job.jobId} cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      // Tell the listeners that all of the running stages have ended.  Don't bother
      // cancelling the stages because if the DAG scheduler is stopped, the entire application
      // is in the process of getting stopped.
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      // The `toArray` here is necessary so that we don't iterate over `runningStages` while
      // mutating it.
      runningStages.toArray.foreach { stage =>
        markStageAsFinished(stage, Some(stageFailedMessage))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  private[scheduler] def handleGetTaskResult(taskInfo: TaskInfo) {
    // 调用SparkListenerBus的doPostEvent方法
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
  }

  /**
    * 5.4-2 任务的提交-处理Job
    *
    *   DAGSchedulerEventProcessLoop收到JobSubmitted事件，会调用dagScheduler的handleJobSubmitted方法。
    *   执行过程如下：
    *     1.创建finalStage及Stage的划分。创建Stage的过程可能发生异常，比如，运行在HadoopRDD上的任务所以来的底层HDFS文件被删除了。
    *       所以当异常发生时需要主动调用JobWaiter的jobFailed方法。
    *     2.创建ActiveJob并且更新jobIdToActiveJob = new HashMap[Int,ActiveJob]，activeJobs = new HashSet[ActiveJob]
    *       和finalStage.resultOfJob.
    *     3.向listenerBus发送SparkListenerJobSatrt事件。
    *     4.提交finalStage.
    *     5.提交等待中的Stage。
    */
  private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    var finalStage: ResultStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      // Stage划分过程是从最后一个Stage开始往前执行的，最后一个Stage的类型是ResultStage
      /**
        * 这里存在递归调用：createResultStage（）--> getOrCreateParentStages()-->  getOrCreateShuffleMapStage（）
        *                                                 |                      |
        *                                                 |                      |
        *                                                 |                      |
        *                                                 >                      |
        *                                            createShuffleMapStage（） <--
        *
        * 获取最后一个stages
        */
      finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }

    //为该Job生成一个ActiveJob对象，并准备计算这个finalStage
    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    // 删除Hash表中的所有条目 这个是什么鬼？这个有什么用
    clearCacheLocs()
    logInfo("Got job %s (%s) with %d output partitions".format(
      job.jobId, callSite.shortForm, partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    // 该job进入active状态 记录处于Active状态的job，key为jobId, value为ActiveJob类型对象
    jobIdToActiveJob(jobId) = job
    // active状态的Job列表
    activeJobs += job
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    // 向LiveListenerBus发送Job提交事件
    listenerBus.post(SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))

    //提交当前Stage
    submitStage(finalStage)
  }

  private[scheduler] def handleMapStageSubmitted(jobId: Int,
      dependency: ShuffleDependency[_, _, _],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    // Submitting this map stage might still require the creation of some parent stages, so make
    // sure that happens.
    var finalStage: ShuffleMapStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = getOrCreateShuffleMapStage(dependency, jobId)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }

    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got map stage job %s (%s) with %d output partitions".format(
      jobId, callSite.shortForm, dependency.rdd.partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.addActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage)

    // If the whole stage has already finished, tell the listener and remove it
    if (finalStage.isAvailable) {
      markMapStageJobAsFinished(job, mapOutputTracker.getStatistics(dependency))
    }
  }

  /** Submits stage, but first recursively submits any missing parents.
    * 提交阶段，但首先递归提交任何丢失的父Stage。
    *
    * submitStage提交Stage，它会把一个job中的第一个stage提交上去
    *
    *    在提交finalStage之前，如果存在没有提交的祖先Stage,则需要先提交所有没有提交的祖先Stage.每个Stage提交之前，
    * 如果存在没有提交的祖先Stage,都会先提交祖先Stage,并且将子Satge放入waitingStages = new HashSet[Stage]
    * 中等待。如果不存在没有提交的祖先Stage，则提交、所有未提交的Task。
    *
    *
    * =====>
    *
    *   提交Job的提交，是从最后那个Stage开始的。如果当前stage已经被提交过，处于waiting或者waiting状态，或者当前
    * stage已经处于failed状态则不作任何处理，否则继续提交该stage。
    *
    *   在提交时，需要当前Stage需要满足依赖关系，其前置的Parent Stage都运行完成后才能轮得到当前Stage运行。如果还有
    * Parent Stage未运行完成，则优先提交Parent Stage。通过调用方法DAGScheduler.getMissingParentStages方法获
    * 取未执行的Parent Stage。
    *
    * 如果当前Stage满足上述两个条件后，调用DAGScheduler.submitMissingTasks方法，提交当前Stage。
    *
    *
    * ===>
    *   这一步主要是计算stage之间的依赖关系（Stage DAG）并且对依赖关系进行处理，首先寻找父Stage,如果没有，说明
    * 当前的Stage的所有依赖都已经准备完毕，则提交Task，病将当前的Stage放入runningStages中，进入下一步。如果有父
    * Stage，则需要先提交父Stage,并且把当前的Stage放入等待队列中，因为当前Stage的执行时依赖前面的Stage的，可见，
    * Stage调度室从后往前找所以来的各个Stage。
    *
    * 打个比方：
    *   我想找一个 从你十八代祖宗到你的一条血缘线
    *   你爷爷 --》 你爸爸 --》你
    *   类似于把这个关系放到队列结构中
    *
    *
    * */
  private def submitStage(stage: Stage) {
    // 获取当前提交Stage所属的Job
    val jobId = activeJobForStage(stage)
    // jobId不为空
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      // 首先判断当前stage的状态，如果当前Stage不是处于waiting, running以及failed状态
      // 则提交该stage
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        // getMissingParentStages方法用来找到Stage所有不可用的祖先Stage.
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing: " + missing)
        //如果所有的parent stage都以及完成，那么就会提交该stage所包含的task
        if (missing.isEmpty) {  ////找到了第一个Stage，其ParentStages为Empty，则提交这个Stage的task
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          /** 任务的提交  */
          submitMissingTasks(stage, jobId.get)
        } else {
          //否则递归的去提交未完成的parent stage
          for (parent <- missing) {
            submitStage(parent)  ////没有找到的话，继续往上找，这里使用递归调用自己的这个方法了
          }
          //当前stage进入等待队列
          waitingStages += stage  ////并把中间的Stage记录下来
        }
      }
    } else {
      //如果jobId没被定义，即无效的stage则直接停止
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
  }

  /** Called when stage's parents are available and we can now do its task.
    * 当一个上stage的父stage存在的时候，我们现在就可以运行他的任务了。
    *
    * 当找到了第一个Stage之后，会开始提交这个Stage的task
    *
    * 5.4.5 提交Task
    *   提交Task的入口是submitMissingTasks函数，此函数在Stage没有不可用的祖先Stage时候，
    *   被调用处理当前Stage未提交的任务。
    *
    *   它主要根据当前Stage所以来的RDD的partition的分布，产生和partition数量相等的Task,
    *   如果当前的Stage是MapStage类型，则产生ShuffleMapTask，否则产生ResultTask，Task
    *   的位置都是经过getPreferedLocs方法计算得到的最佳位置。
    *
    *   1.提交还未计算的任务
    *
    *     submitMissingTasks用于提交还未计算的任务。
    *     pendingTasks:类型是HashSet[Task[_]],存储有待处理的Task。
    *     MapStatus:包括执行Task的BlockManager的地址和要传给reduce任务的Block的估算大小。
    *     outputLocs:如果Stage是map任务，则outputLocs记录每个Partition的MapStatus。
    *
    *     submitMissingTasks执行过程总结如下：
    *     （1）.清空pendingTasks，由于当前Stage的任务刚开始提交，所以需要清空，便于记录需要计算的任务。
    *     （2）.找出还未计算的partition(如果Stage是map任务，那么需要获取Stage的finalJob，并且调用
    *           finished方法判断每个partition
    *           的任务是否完成)
    *     （3）.将当前Stage加入运行中的Stage集合（runningStages:HashSet[stage]）中。
    *     （4）.使用StageInfo。fromStage方法创建当前Stage的latestInfo(StageInfo)
    *     （5）.向listenerBus发送SparkListenerStageSubmitted事件。
    *     （6）.如果Stage是map任务，那么序列化Stage的RDD及ShuffleDependency,如果Stage不是Map任务，
    *           那么序列化Stage的RDD及resultOfJob
    *           的处理函数，这些序列化得到的字节数组最后需要使用sc.broadcast进行广播。
    *     （7）.如果Stage是map任务，则创建ShuffleMapTask，否则创建ResultTask，还未计算的partition
    *           个数决定了最终创建的Task
    *           个数。并将创建的所有Task都添加到Stage的pendingTasks中。
    *     （8）.利用上一步创建的所有Task，当前Stage的id，jobId等信息创建TaskSet，并调用taskScheduler
    *           的submitTasks，批量提交Stage及其所有的Task.
    *
    * */
  private def submitMissingTasks(stage: Stage, jobId: Int) {
    logDebug("submitMissingTasks(" + stage + ")")

    // First figure out the indexes of partition ids to compute.
    // 首先要计算的分区索引ID。 取得当前Stage需要计算的partition 返回丢失的分区id序列(即需要计算)
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()

    // Use the scheduling pool, job group, description, etc. from an ActiveJob associated
    // with this Stage
    // 使用调度池，工作组，描述，等从一个与这个Stage相关的activejob
    val properties = jobIdToActiveJob(jobId).properties

    // 将当前stage存入running状态的stage列表中
    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    // sparklistenerstagesubmitted应该被posted提交在检验任务无论tasks任务是否序列化之前，如果任务不可序列化的，
    // 一个sparklistenerstagecompleted事件将posted，这应该是一个相应的sparklistenerstagesubmitted事件后。

    // 判断当前stage是ShuffleMapStage还是ResultStage，（猜测的）
    stage match {
      case s: ShuffleMapStage =>
        outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
    }

    // 这一点不知道是干嘛的？ 在DAGScheudler的submitMissingTasks方法中利用RDD的本地性来得到Task的本地性， 获取Stage内部Task的最佳位置。
    //  dagscheduler 初步判断划分的task 跑在那个executer上  是根据RDD的getPreferredLocs 来确定 数据在哪里  就近分配
    /**
      * DAGScheduler 通过调用 submitStage 来提交一个 stage 对应的 tasks，submitStage 会调用submitMissingTasks，
      * submitMissingTasks 会以下代码来确定每个需要计算的 task 的preferredLocations，这里调用到了 RDD#getPreferredLocs，
      * getPreferredLocs返回的 partition 的优先位置，就是这个 partition 对应的 task 的优先位置
      */
    val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
      stage match {
        case s: ShuffleMapStage =>
          // getPreferredLocs:获取与特定RDD分区相关联的本地信息。
          // 取得当前Stage需要计算的partition 返回丢失的分区id序列(即需要计算)
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
        case s: ResultStage =>
          partitionsToCompute.map { id =>
            val p = s.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      // 非致命的错误，如VirtualMachineError，OutOfMemoryError，StackOverflowError等
      case NonFatal(e) =>
        // 通过使用新的尝试ID创建一个新的StageInfo，为这个stage创建一个新的尝试。
        stage.makeNewStageAttempt(partitionsToCompute.size)
        // 重新提交这个stage,这里listenerBus.post(SparkListenerStageSubmitted))) 这个被谁消费了？
        // SparkListenerStageSubmitted事件是SparkListenerBus的doPostEvent（）方法处理的
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    // 通过使用新的尝试ID创建一个新的StageInfo，为这个stage创建一个新的尝试。
    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)
    // 向listenerBus提交StageSubmitted事件
    // SparkListenerStageSubmitted事件是SparkListenerBus的doPostEvent（）方法处理的，
    // 这里有个问题没解决，不知道最后谁使用的？
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    /**
      * TODO:也许我们可以把任务二进制文件(taskBinary)放在Stage上，以避免多次序列化。
      * 用于任务的广播二进制文件，用于将任务分派给执行程序executors。请注意，我们广播了RDD的序列化副本，对于每个任务，我们将对其进行反序列化，
      * 这意味着每个任务得到RDD的不同副本。这为可能修改闭包中引用的对象状态的任务提供了更强的隔离。在Hadoop中，JobConf / Configuration对
      * 象不是线程安全的。
      *
      */
    var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep). 对于ShuffleMapTask，序列化和广播
      // For ResultTask, serialize and broadcast (rdd, func). 对于ResultTask，序列化和广播
      //注意：我们broadcast RDD的拷贝并且对于每一个task我们将要反序列化，这意味着每个task得到一个不同的RDD 拷贝
      val taskBinaryBytes: Array[Byte] = stage match {
        case stage: ShuffleMapStage =>
          JavaUtils.bufferToArray(
            closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
        case stage: ResultStage =>
          JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
      }

      //将序列化后的task广播出去
      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      // 在序列化失败的情况下，中止stage。
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution 中止执行
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    // 根据stage的类型获取其中包含的task
    //根据stage生成tasks
    /**
      * 这段调用返回的 taskIdToLocations: Seq[ taskId -> Seq[hosts] ] 会在submitMissingTasks生成要提交给
      * TaskScheduler 调度的 taskSet: Seq[Task[_]]时用到
      *
      */
    val tasks: Seq[Task[_]] = try {
      val serializedTaskMetrics = closureSerializer.serialize(stage.latestInfo.taskMetrics).array()
      stage match {
        // ShuffleMapStage中对应的是ShuffleMapTask   //对于ShuffleMapStages生成ShuffleMapTask
        case stage: ShuffleMapStage =>
          // 清空stage的PendingTasks
          stage.pendingPartitions.clear()
          // 每个分区对应一个ShuffleMapTask（这样更加高效）
          partitionsToCompute.map { id =>
            val locs = taskIdToLocations(id)
            val part = stage.rdd.partitions(id)
            stage.pendingPartitions += id
            //< 使用上述获得的 task 对应的优先位置，即 locs 来构造ShuffleMapTask
            // 生成ShuffleMapTask
            //可见一个partition，一个task，一个位置信息
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, properties, serializedTaskMetrics, Option(jobId),
              Option(sc.applicationId), sc.applicationAttemptId)
          }

        // ResultStage中对应的是ResultTask //对于ResultStage生成ResultTask
        case stage: ResultStage =>
          // 每个分区对应一个ResultTask
          partitionsToCompute.map { id =>
            val p: Int = stage.partitions(id)
            val part = stage.rdd.partitions(p)
            val locs = taskIdToLocations(id)
            //< 使用上述获得的 task 对应的优先位置，即 locs 来构造ResultTask
            new ResultTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, id, properties, serializedTaskMetrics,
              Option(jobId), Option(sc.applicationId), sc.applicationAttemptId)
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    /**
      * taskIdToLocations 和 tasks: Seq[Task[_]] =这两个总结：
      *   简而言之，在 DAGScheduler 为 stage 创建要提交给 TaskScheduler 调度执行的 taskSet 时，对于 taskSet
      *  中的每一个 task，其优先位置与其对应的 partition 对应的优先位置一致
      */


    // 如果当前Stege中有task
    if (tasks.size > 0) {
      logInfo(s"Submitting ${tasks.size} missing tasks from $stage (${stage.rdd}) (first 15 " +
        s"tasks are for partitions ${tasks.take(15).map(_.partitionId)})")
      // 要么是ShuffleMapTask或者是ResultTask，其TaskSet的priority为stage的jobid，而jobid是递增的，在submitTasks函数里面会
      // 创建TaskSetManager，然后把TaskSetManager添加到以上的pool中
      // 根据tasks生成TaskSet，然后通过TaskScheduler.submitTasks方法提交TaskSet
      // TODO:最后所有的Stage都转换为TaskSet任务集去提交，最后开始执行任务
      // 这里调用的是TaskScheduler的接口方法submitTasks（）提交一系列要运行的任务。所以要看其实现类TaskSchedulerImpl。
      // 调用了里面的方法submitTasks
      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties))

      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
      // 如果当前Stege中不包含task
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      // 由于前面已经向listenerBus中提交了StageSubmitted事件，现在这个Stege中没有task运行
      // 则正常流程时，该stage不会被标记为结束。那么需要手动指定该stege为finish状态。
      //因为我们之前就已经发送了事件SparkListenerStageSubmitted，所以我们标记Stage为completed防止没有任务提交
      markStageAsFinished(stage, None)

      // log中的显示信息 //将debugString记录到日志中
      val debugString = stage match {
        case stage: ShuffleMapStage =>
          s"Stage ${stage} is actually done; " +
            s"(available: ${stage.isAvailable}," +
            s"available outputs: ${stage.numAvailableOutputs}," +
            s"partitions: ${stage.numPartitions})"
        case stage : ResultStage =>
          s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})"
      }
      logDebug(debugString)

      submitWaitingChildStages(stage)
    }
  }




  /**
   * Merge local values from a task into the corresponding accumulators previously registered
   * here on the driver.
   *
   * Although accumulators themselves are not thread-safe, this method is called only from one
   * thread, the one that runs the scheduling loop. This means we only handle one task
   * completion event at a time so we don't need to worry about locking the accumulators.
   * This still doesn't stop the caller from updating the accumulator outside the scheduler,
   * but that's not our problem since there's nothing we can do about that.
   */
  private def updateAccumulators(event: CompletionEvent): Unit = {
    val task = event.task
    val stage = stageIdToStage(task.stageId)
    try {
      event.accumUpdates.foreach { updates =>
        val id = updates.id
        // Find the corresponding accumulator on the driver and update it
        val acc: AccumulatorV2[Any, Any] = AccumulatorContext.get(id) match {
          case Some(accum) => accum.asInstanceOf[AccumulatorV2[Any, Any]]
          case None =>
            throw new SparkException(s"attempted to access non-existent accumulator $id")
        }
        acc.merge(updates.asInstanceOf[AccumulatorV2[Any, Any]])
        // To avoid UI cruft, ignore cases where value wasn't updated
        if (acc.name.isDefined && !updates.isZero) {
          stage.latestInfo.accumulables(id) = acc.toInfo(None, Some(acc.value))
          event.taskInfo.setAccumulables(
            acc.toInfo(Some(updates.value), Some(acc.value)) +: event.taskInfo.accumulables)
        }
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Failed to update accumulators for task ${task.partitionId}", e)
    }
  }

  /**
   * Responds to a task finishing. This is called inside the event loop so it assumes that it can
   * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
    *
    * 完成任务的响应。这在事件循环中被调用，因此它假设它可以修改调度器的内部状态。使用taskended()去post一个外部的task。
   */
  private[scheduler] def handleTaskCompletion(event: CompletionEvent) {
    val task = event.task
    val taskId = event.taskInfo.id
    val stageId = task.stageId
    val taskType = Utils.getFormattedClassName(task)

    outputCommitCoordinator.taskCompleted(
      stageId,
      task.partitionId,
      event.taskInfo.attemptNumber, // this is a task attempt number
      event.reason)

    // Reconstruct task metrics. Note: this may be null if the task has failed.
    val taskMetrics: TaskMetrics =
      if (event.accumUpdates.nonEmpty) {
        try {
          TaskMetrics.fromAccumulators(event.accumUpdates)
        } catch {
          case NonFatal(e) =>
            logError(s"Error when attempting to reconstruct metrics for task $taskId", e)
            null
        }
      } else {
        null
      }

    // The stage may have already finished when we get this event -- eg. maybe it was a
    // speculative task. It is important that we send the TaskEnd event in any case, so listeners
    // are properly notified and can chose to handle it. For instance, some listeners are
    // doing their own accounting and if they don't get the task end event they think
    // tasks are still running when they really aren't.
    listenerBus.post(SparkListenerTaskEnd(
       stageId, task.stageAttemptId, taskType, event.reason, event.taskInfo, taskMetrics))

    if (!stageIdToStage.contains(task.stageId)) {
      // Skip all the actions if the stage has been cancelled.
      return
    }

    val stage = stageIdToStage(task.stageId)
    event.reason match {
      case Success =>
        task match {
          case rt: ResultTask[_, _]   //如果是ResultTask，仅仅是更新下Task的完成情况
            // Cast to ResultStage here because it's part of the ResultTask
            // TODO Refactor this out to a function that accepts a ResultStage
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.activeJob match {
              case Some(job) =>
                // 如果这个Task未执行完成
                if (!job.finished(rt.outputId)) {
                  // 更新当前状态
                  updateAccumulators(event)
                  // 将Task所对应的Partition标记为计算完成
                  job.finished(rt.outputId) = true
                  // 当前作业中Partition完成数增加
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  // 如果当前的Job所有Partition对已计算完成，就将这个Stage remove掉
                  if (job.numFinished == job.numPartitions) {
                    markStageAsFinished(resultStage)
                    cleanupStateForJobAndIndependentStages(job)
                    listenerBus.post(
                      SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
                  }

                  // taskSucceeded runs some user code that might throw an exception. Make sure
                  // we are resilient against that.
                  // 在处理SucceedTask时，会调用一些用户定义的函数，可能会产生异常，为
                  // 了确保程序的健壮性，需要进行异常处理。
                  try {
                    job.listener.taskSucceeded(rt.outputId, event.result)
                  } catch {
                    case e: Exception =>
                      //当异常发生时，有时需要标记ResultStage失败。
                      // TODO: Perhaps we want to mark the resultStage as failed?
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                }
              case None =>
                //在任务进行推测执行时，可能有多个Task的执行结果，对于对于的结果，系统
                //会进行忽略处理。
                logInfo("Ignoring result from " + rt + " because its job has finished")
            }

          //如果是ShuffleMapTask，则它之后必定还存在待提交的ShuffleMapStage或者ResultStage，有则提交
          /**
            * 处理步骤如下：
            *   1.将Task的partitionId和MapStatus追加到Stage的outputLocs中。
            *   2.将当前Stage标记为完成，然后将当前Stage的shuffleId和outputLocs中的MapStatus注册到mapOutputTracker。
            *     这里注册的map任务状态将最终被reduce任务所用。
            *   3.如果Stage的outputLocs中某个分区的输出为Nil，那么说明有任务失败了，这时需要再次提交此Stage.
            *   4.如果不存在Stage的outputLocs中某个分区的输出为Nil，那么说明所有任务执行成功了，这时需要遍历waitingStages
            *     中的Stage并且将它放入running Stages，最后调用submitMissingTask方法逐个提交这些准备运行的STage的任务。
            *
            */
          case smt: ShuffleMapTask =>
            //如果是ShuffleMapTask
            //实例化一个shuffleStage实例，用来保存TaskSet结果
            val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
            // 跟新本地的状态信息
            updateAccumulators(event)
            val status = event.result.asInstanceOf[MapStatus]
            val execId = status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            if (stageIdToStage(task.stageId).latestInfo.attemptId == task.stageAttemptId) {
              // This task was for the currently running attempt of the stage. Since the task
              // completed successfully from the perspective of the TaskSetManager, mark it as
              // no longer pending (the TaskSetManager may consider the task complete even
              // when the output needs to be ignored because the task's epoch is too small below.
              // In this case, when pending partitions is empty, there will still be missing
              // output locations, which will cause the DAGScheduler to resubmit the stage below.)
              shuffleStage.pendingPartitions -= task.partitionId
            }

            // 忽略在集群中游走的ShuffleMapTask（来自一个失效的节点的Task结果）。
            if (failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
              logInfo(s"Ignoring possibly bogus $smt completion from executor $execId")
            } else {
              // The epoch of the task is acceptable (i.e., the task was launched after the most
              // recent failure we're aware of for the executor), so mark the task's output as
              // available.
              shuffleStage.addOutputLoc(smt.partitionId, status)
              // Remove the task's partition from pending partitions. This may have already been
              // done above, but will not have been done yet in cases where the task attempt was
              // from an earlier attempt of the stage (i.e., not the attempt that's currently
              // running).  This allows the DAGScheduler to mark the stage as complete when one
              // copy of each task has finished successfully, even if the currently active stage
              // still has tasks running.
              // 将结果保存到Stage中。即将Task结果的输出位置放到Stage的数据结构中。
              shuffleStage.pendingPartitions -= task.partitionId
            }

            // 如果当前Stage运行完毕
            if (runningStages.contains(shuffleStage) && shuffleStage.pendingPartitions.isEmpty) {
              // 标记当前Stage为Finished，并将其从运行中Stage列表中删除
              markStageAsFinished(shuffleStage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("failed: " + failedStages)

              // We supply true to increment the epoch number here in case this is a
              // recomputation of the map outputs. In that case, some nodes may have cached
              // locations with holes (from when we detected the error) and will need the
              // epoch incremented to refetch them.
              // TODO: Only increment the epoch number if this is not the first time
              //       we registered these map outputs.
              // 将整体结果注册到MapOutputTrackerMaster；
              mapOutputTracker.registerMapOutputs(
                shuffleStage.shuffleDep.shuffleId,
                shuffleStage.outputLocInMapOutputTrackerFormat(),
                changeEpoch = true)

              // 清除本地缓存
              clearCacheLocs()

              // 如果shuffleMapStage中有一些tasks运行失败，没有结果。
              if (!shuffleStage.isAvailable) {
                //则需要重新提交这个shuffleMapStage,并且需要告知顶层调度器TaskScheduler
                //进行处理。
                // Some tasks had failed; let's resubmit this shuffleStage.
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + shuffleStage + " (" + shuffleStage.name +
                  ") because some of its tasks had failed: " +
                  shuffleStage.findMissingPartitions().mkString(", "))
                submitStage(shuffleStage)
              } else {
                //否则提交依赖此shuffleStage的，且位于waitingStages中的Stage，此例子即为ResultStage
                // Mark any map-stage jobs waiting on this stage as finished
                // 标记所有等待这个Stage结束的Map-Stage Job为结束状态
                //这里会将这个Job记为Finished状态，并统计输出结果，报告给监听器
                if (shuffleStage.mapStageJobs.nonEmpty) {
                  val stats = mapOutputTracker.getStatistics(shuffleStage.shuffleDep)
                  //遍历waitingStages，且其没有ParentStage
                  for (job <- shuffleStage.mapStageJobs) {
                    markMapStageJobAsFinished(job, stats)
                  }
                }
                submitWaitingChildStages(shuffleStage)
              }
            }
        }

      case Resubmitted =>
        logInfo("Resubmitted " + task + ", so marking it as still running")
        stage match {
          case sms: ShuffleMapStage =>
            sms.pendingPartitions += task.partitionId

          case _ =>
            assert(false, "TaskSetManagers should only send Resubmitted task statuses for " +
              "tasks in ShuffleMapStages.")
        }

      case FetchFailed(bmAddress, shuffleId, mapId, reduceId, failureMessage) =>
        val failedStage = stageIdToStage(task.stageId)
        val mapStage = shuffleIdToMapStage(shuffleId)

        if (failedStage.latestInfo.attemptId != task.stageAttemptId) {
          logInfo(s"Ignoring fetch failure from $task as it's from $failedStage attempt" +
            s" ${task.stageAttemptId} and there is a more recent attempt for that stage " +
            s"(attempt ID ${failedStage.latestInfo.attemptId}) running")
        } else {
          // It is likely that we receive multiple FetchFailed for a single stage (because we have
          // multiple tasks running concurrently on different executors). In that case, it is
          // possible the fetch failure has already been handled by the scheduler.
          if (runningStages.contains(failedStage)) {
            logInfo(s"Marking $failedStage (${failedStage.name}) as failed " +
              s"due to a fetch failure from $mapStage (${mapStage.name})")
            markStageAsFinished(failedStage, Some(failureMessage))
          } else {
            logDebug(s"Received fetch failure from $task, but its from $failedStage which is no " +
              s"longer running")
          }

          failedStage.fetchFailedAttemptIds.add(task.stageAttemptId)
          val shouldAbortStage =
            failedStage.fetchFailedAttemptIds.size >= maxConsecutiveStageAttempts ||
            disallowStageRetryForTest

          if (shouldAbortStage) {
            val abortMessage = if (disallowStageRetryForTest) {
              "Fetch failure will not retry stage due to testing config"
            } else {
              s"""$failedStage (${failedStage.name})
                 |has failed the maximum allowable number of
                 |times: $maxConsecutiveStageAttempts.
                 |Most recent failure reason: $failureMessage""".stripMargin.replaceAll("\n", " ")
            }
            abortStage(failedStage, abortMessage, None)
          } else { // update failedStages and make sure a ResubmitFailedStages event is enqueued
            // TODO: Cancel running tasks in the failed stage -- cf. SPARK-17064
            val noResubmitEnqueued = !failedStages.contains(failedStage)
            failedStages += failedStage
            failedStages += mapStage
            if (noResubmitEnqueued) {
              // We expect one executor failure to trigger many FetchFailures in rapid succession,
              // but all of those task failures can typically be handled by a single resubmission of
              // the failed stage.  We avoid flooding the scheduler's event queue with resubmit
              // messages by checking whether a resubmit is already in the event queue for the
              // failed stage.  If there is already a resubmit enqueued for a different failed
              // stage, that event would also be sufficient to handle the current failed stage, but
              // producing a resubmit for each failed stage makes debugging and logging a little
              // simpler while not producing an overwhelming number of scheduler events.
              logInfo(
                s"Resubmitting $mapStage (${mapStage.name}) and " +
                s"$failedStage (${failedStage.name}) due to fetch failure"
              )
              messageScheduler.schedule(
                new Runnable {
                  override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
                },
                DAGScheduler.RESUBMIT_TIMEOUT,
                TimeUnit.MILLISECONDS
              )
            }
          }
          // Mark the map whose fetch failed as broken in the map stage
          if (mapId != -1) {
            mapStage.removeOutputLoc(mapId, bmAddress)
            mapOutputTracker.unregisterMapOutput(shuffleId, mapId, bmAddress)
          }

          // TODO: mark the executor as failed only if there were lots of fetch failures on it
          if (bmAddress != null) {
            handleExecutorLost(bmAddress.executorId, filesLost = true, Some(task.epoch))
          }
        }

      case commitDenied: TaskCommitDenied =>
        // Do nothing here, left up to the TaskScheduler to decide how to handle denied commits

      case exceptionFailure: ExceptionFailure =>
        // Tasks failed with exceptions might still have accumulator updates.
        updateAccumulators(event)

      case TaskResultLost =>
        // Do nothing here; the TaskScheduler handles these failures and resubmits the task.

      case _: ExecutorLostFailure | _: TaskKilled | UnknownReason =>
        // Unrecognized failure - also do nothing. If the task fails repeatedly, the TaskScheduler
        // will abort the job.
    }
  }

  /**
   * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
   *
   * We will also assume that we've lost all shuffle blocks associated with the executor if the
   * executor serves its own blocks (i.e., we're not using external shuffle), the entire slave
   * is lost (likely including the shuffle service), or a FetchFailed occurred, in which case we
   * presume all shuffle data related to this executor to be lost.
   *
   * Optionally the epoch during which the failure was caught can be passed to avoid allowing
   * stray fetch failures from possibly retriggering the detection of a node as lost.
   */
  private[scheduler] def handleExecutorLost(
      execId: String,
      filesLost: Boolean,
      maybeEpoch: Option[Long] = None) {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    if (!failedEpoch.contains(execId) || failedEpoch(execId) < currentEpoch) {
      failedEpoch(execId) = currentEpoch
      logInfo("Executor lost: %s (epoch %d)".format(execId, currentEpoch))
      blockManagerMaster.removeExecutor(execId)

      if (filesLost || !env.blockManager.externalShuffleServiceEnabled) {
        logInfo("Shuffle files lost for executor: %s (epoch %d)".format(execId, currentEpoch))
        // TODO: This will be really slow if we keep accumulating shuffle map stages
        for ((shuffleId, stage) <- shuffleIdToMapStage) {
          stage.removeOutputsOnExecutor(execId)
          mapOutputTracker.registerMapOutputs(
            shuffleId,
            stage.outputLocInMapOutputTrackerFormat(),
            changeEpoch = true)
        }
        if (shuffleIdToMapStage.isEmpty) {
          mapOutputTracker.incrementEpoch()
        }
        clearCacheLocs()
      }
    } else {
      logDebug("Additional executor lost message for " + execId +
               "(epoch " + currentEpoch + ")")
    }
  }

  private[scheduler] def handleExecutorAdded(execId: String, host: String) {
    // remove from failedEpoch(execId) ?
    if (failedEpoch.contains(execId)) {
      logInfo("Host added was in lost list earlier: " + host)
      failedEpoch -= execId
    }
  }

  private[scheduler] def handleStageCancellation(stageId: Int, reason: Option[String]) {
    stageIdToStage.get(stageId) match {
      case Some(stage) =>
        val jobsThatUseStage: Array[Int] = stage.jobIds.toArray
        jobsThatUseStage.foreach { jobId =>
          val reasonStr = reason match {
            case Some(originalReason) =>
              s"because $originalReason"
            case None =>
              s"because Stage $stageId was cancelled"
          }
          handleJobCancellation(jobId, Option(reasonStr))
        }
      case None =>
        logInfo("No active jobs to kill for Stage " + stageId)
    }
  }

  private[scheduler] def handleJobCancellation(jobId: Int, reason: Option[String]) {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to cancel unregistered job " + jobId)
    } else {
      failJobAndIndependentStages(
        jobIdToActiveJob(jobId), "Job %d cancelled %s".format(jobId, reason.getOrElse("")))
    }
  }

  /**
   * Marks a stage as finished and removes it from the list of running stages.
   */
  private def markStageAsFinished(stage: Stage, errorMessage: Option[String] = None): Unit = {
    val serviceTime = stage.latestInfo.submissionTime match {
      case Some(t) => "%.03f".format((clock.getTimeMillis() - t) / 1000.0)
      case _ => "Unknown"
    }
    if (errorMessage.isEmpty) {
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
      stage.latestInfo.completionTime = Some(clock.getTimeMillis())

      // Clear failure count for this stage, now that it's succeeded.
      // We only limit consecutive failures of stage attempts,so that if a stage is
      // re-used many times in a long-running job, unrelated failures don't eventually cause the
      // stage to be aborted.
      stage.clearFailures()
    } else {
      stage.latestInfo.stageFailed(errorMessage.get)
      logInfo(s"$stage (${stage.name}) failed in $serviceTime s due to ${errorMessage.get}")
    }

    outputCommitCoordinator.stageEnd(stage.id)
    listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
    runningStages -= stage
  }

  /**
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
    *
    * 根据特定的阶段中止所有作业。这是响应由TaskScheduler取消的任务集的响应。使用taskSetFailed()从外部注入该事件。
   */
  private[scheduler] def abortStage(
      failedStage: Stage,
      reason: String,
      exception: Option[Throwable]): Unit = {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      return
    }
    val dependentJobs: Seq[ActiveJob] =
      activeJobs.filter(job => stageDependsOn(job.finalStage, failedStage)).toSeq
    failedStage.latestInfo.completionTime = Some(clock.getTimeMillis())
    for (job <- dependentJobs) {
      failJobAndIndependentStages(job, s"Job aborted due to stage failure: $reason", exception)
    }
    if (dependentJobs.isEmpty) {
      logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
    }
  }

  /** Fails a job and all stages that are only used by that job, and cleans up relevant state. */
  private def failJobAndIndependentStages(
      job: ActiveJob,
      failureReason: String,
      exception: Option[Throwable] = None): Unit = {
    val error = new SparkException(failureReason, exception.getOrElse(null))
    var ableToCancelStages = true

    val shouldInterruptThread =
      if (job.properties == null) false
      else job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false").toBoolean

    // Cancel all independent, running stages.
    val stages = jobIdToStageIds(job.jobId)
    if (stages.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    }
    stages.foreach { stageId =>
      val jobsForStage: Option[HashSet[Int]] = stageIdToStage.get(stageId).map(_.jobIds)
      if (jobsForStage.isEmpty || !jobsForStage.get.contains(job.jobId)) {
        logError(
          "Job %d not registered for stage %d even though that stage was registered for the job"
            .format(job.jobId, stageId))
      } else if (jobsForStage.get.size == 1) {
        if (!stageIdToStage.contains(stageId)) {
          logError(s"Missing Stage for stage with id $stageId")
        } else {
          // This is the only job that uses this stage, so fail the stage if it is running.
          val stage = stageIdToStage(stageId)
          if (runningStages.contains(stage)) {
            try { // cancelTasks will fail if a SchedulerBackend does not implement killTask
              taskScheduler.cancelTasks(stageId, shouldInterruptThread)
              markStageAsFinished(stage, Some(failureReason))
            } catch {
              case e: UnsupportedOperationException =>
                logInfo(s"Could not cancel tasks for stage $stageId", e)
              ableToCancelStages = false
            }
          }
        }
      }
    }

    if (ableToCancelStages) {
      // SPARK-15783 important to cleanup state first, just for tests where we have some asserts
      // against the state.  Otherwise we have a *little* bit of flakiness in the tests.
      cleanupStateForJobAndIndependentStages(job)
      job.listener.jobFailed(error)
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  /** Return true if one of stage's ancestors is target. */
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {
    if (stage == target) {
      return true
    }
    val visitedRdds = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
              if (!mapStage.isAvailable) {
                waitingForVisit.push(mapStage.rdd)
              }  // Otherwise there's no need to follow the dependency back
            case narrowDep: NarrowDependency[_] =>
              waitingForVisit.push(narrowDep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    visitedRdds.contains(target.rdd)
  }

  /**
   * Gets the locality information associated with a partition of a particular RDD.
    * 获取与特定RDD分区相关联的本地信息。
   *
   * This method is thread-safe and is called from both DAGScheduler and SparkContext.
   *该方法是线程安全的，并由DAGScheduler和SparkContext调用。
    *
   * @param rdd whose partitions are to be looked at
   * @param partition to lookup locality information for
   * @return list of machines that are preferred by the partition
   */
  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    getPreferredLocsInternal(rdd, partition, new HashSet)
  }

  /**
   * Recursive implementation for getPreferredLocs.
   *
   * This method is thread-safe because it only accesses DAGScheduler state through thread-safe
   * methods (getCacheLocs()); please be careful when modifying this method, because any new
   * DAGScheduler state accessed by it may require additional synchronization.
   */
  private def getPreferredLocsInternal(
      rdd: RDD[_],
      partition: Int,
      visited: HashSet[(RDD[_], Int)]): Seq[TaskLocation] = {
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    if (!visited.add((rdd, partition))) {
      // Nil has already been returned for previously visited partitions.
      return Nil
    }
    // If the partition is cached, return the cache locations
    // 如果RDD分区被缓存了，就返回缓存的位置
    val cached = getCacheLocs(rdd)(partition)
    if (cached.nonEmpty) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    // 如果RDD被checkpoint过
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (rddPrefs.nonEmpty) {
      return rddPrefs.map(TaskLocation(_))
    }

    // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    // 递归查找具有依赖关系的RDD分区位置信息，仅当窄依赖时
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return locs
          }
        }

      case _ =>
    }

    Nil
  }

  /** Mark a map stage job as finished with the given output stats, and report to its listener. */
  def markMapStageJobAsFinished(job: ActiveJob, stats: MapOutputStatistics): Unit = {
    // In map stage jobs, we only create a single "task", which is to finish all of the stage
    // (including reusing any previous map outputs, etc); so we just mark task 0 as done
    job.finished(0) = true
    job.numFinished += 1
    job.listener.taskSucceeded(0, stats)
    cleanupStateForJobAndIndependentStages(job)
    listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
  }

  def stop() {
    messageScheduler.shutdownNow()
    eventProcessLoop.stop()
    //  TaskSchedulerImpl.stop()方法
    taskScheduler.stop()
  }

  eventProcessLoop.start()
}

private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  // 计时器记录在DAGScheduler的事件循环中处理消息的时间
  private[this] val timer = dagScheduler.metricsSource.messageProcessingTimer

  /**
   * The main event loop of the DAG scheduler.
    * DAG调度器的主事件循环。
    *
    * 这个方法在EventLoop抽象类中被循环调用
   */
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      doOnReceive(event)
    } finally {
      timerContext.stop()
    }
  }

  /**
    * 在该方法中，根据事件类别分别匹配不同的方法进一步处理。本次传入的是JobSubmitted方法，那么进一步调用的方法是
    * DAGScheduler.handleJobSubmitted。这部分的逻辑，以及还可以处理的其他事件，都在下面的源代码中。
    *
    * @param event
    */
  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    // 处理Job提交事件
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      // 开始处理Job，并执行Stage的划分。
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)

    // 处理Map Stage提交事件
    case MapStageSubmitted(jobId, dependency, callSite, listener, properties) =>
      dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, properties)

    // 处理Stage取消事件
    case StageCancelled(stageId, reason) =>
      dagScheduler.handleStageCancellation(stageId, reason)

    // 处理Job取消事件
    case JobCancelled(jobId, reason) =>
      dagScheduler.handleJobCancellation(jobId, reason)

    // 处理Job组取消事件
    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    // 处理所有Job取消事件
    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    // 处理Executor分配事件
    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    // 处理Executor丢失事件
    case ExecutorLost(execId, reason) =>
      val filesLost = reason match {
        case SlaveLost(_, true) => true
        case _ => false
      }
      dagScheduler.handleExecutorLost(execId, filesLost)

    // 由TaskSetManager调用来报告任务的开始。
    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    // GettingResultEvent事件
    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    // 处理完成事件
    case completion: CompletionEvent =>
      dagScheduler.handleTaskCompletion(completion)

    // 处理task集失败事件
    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    // 处理重新提交失败Stage事件
    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }

  override def onError(e: Throwable): Unit = {
    logError("DAGSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      dagScheduler.doCancelAllJobs()
    } catch {
      case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
    }
    dagScheduler.sc.stopInNewThread()
  }

  override def onStop(): Unit = {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}

private[spark] object DAGScheduler {
  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 200

  // Number of consecutive stage attempts allowed before a stage is aborted
  val DEFAULT_MAX_CONSECUTIVE_STAGE_ATTEMPTS = 4
}
