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

package org.apache.spark

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.ControlThrowable

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{DYN_ALLOCATION_MAX_EXECUTORS, DYN_ALLOCATION_MIN_EXECUTORS}
import org.apache.spark.metrics.source.Source
import org.apache.spark.scheduler._
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * An agent that dynamically allocates and removes executors based on the workload.
  * 根据工作负载动态分配和删除执行器的代理。
 *
 * The ExecutorAllocationManager maintains a moving target number of executors which is periodically
 * synced to the cluster manager. The target starts at a configured initial value and changes with
 * the number of pending and running tasks.
 *
  * executorallocationmanager保持一直移动目标数量的执行者可定期同步到集群管理。目标以配置的初始值开始，
  * 并随挂起和正在运行的任务的数量而改变。
  *
 * Decreasing the target number of executors happens when the current target is more than needed to
 * handle the current load. The target number of executors is always truncated to the number of
 * executors that could run all current running and pending tasks at once.
  *
  * 当当前目标超过需要处理当前负载时，减少执行器的目标数量。执行器的目标数目总是被截断为执行器的数目，
  * 该执行器可以同时运行所有当前运行和挂起的任务。
 *
 * Increasing the target number of executors happens in response to backlogged tasks waiting to be
 * scheduled. If the scheduler queue is not drained in N seconds, then new executors are added. If
 * the queue persists for another M seconds, then more executors are added and so on. The number
 * added in each round increases exponentially from the previous round until an upper bound has been
 * reached. The upper bound is based both on a configured property and on the current number of
 * running and pending tasks, as described above.
  *
  * 增加执行器的目标数量以响应等待调度的积压任务。如果调度器队列在N秒内不被抽干，则会添加新的executor。
  * 如果队列持续了另一个M秒，则会添加更多的executor。在每一轮中加入的数字指数从上一轮增加到上界。运行和等待任务，
  * 如上所述。
 *
 * The rationale for the exponential increase is twofold: (1) Executors should be added slowly
 * in the beginning in case the number of extra executors needed turns out to be small. Otherwise,
 * we may add more executors than we need just to remove them later. (2) Executors should be added
 * quickly over time in case the maximum number of executors is very high. Otherwise, it will take
 * a long time to ramp up under heavy workloads.
 *
  * 指数增长的基本原理是双重的:
  * (1)在开始时应该缓慢地增加执行者，以防需要的额外执行者数目很小。否则，我们可能会添加更多的executor，
  *   而不是仅仅在以后删除它们。
  * (2)遗Executors应快速的增加，以防Executors的最大数目非常高。否则，将需要很长时间才能在繁重的工作负载下增加。
  *
 * The remove policy is simpler: If an executor has been idle for K seconds, meaning it has not
 * been scheduled to run any tasks, then it is removed.
  *
  * 删除策略更简单:如果执行器空闲时间为K秒，意味着它没有被调度执行任何任务，那么它将被删除。
 *
 * There is no retry logic in either case because we make the assumption that the cluster manager
 * will eventually fulfill all requests it receives asynchronously.
 *
  * 在这两种情况下，都没有重试逻辑，因为我们假定集群管理器最终将实现它异步接收的所有请求。
  *
 * The relevant Spark properties include the following:
  * 相关的Spark属性包括以下内容:
 *
 *   spark.dynamicAllocation.enabled - Whether this feature is enabled
 *   spark.dynamicAllocation.minExecutors - Lower bound on the number of executors
 *   spark.dynamicAllocation.maxExecutors - Upper bound on the number of executors
 *   spark.dynamicAllocation.initialExecutors - Number of executors to start with
  *
  *   spark.dynamicAllocation.enabled - 是否启用动态分配这个策略
  *   spark.dynamicAllocation.minExecutors - 最低分配的executor数目
  *   spark.dynamicAllocation.maxExecutors - 最高分配的executor数目
  *   spark.dynamicAllocation.initialExecutors - 开始分配executor的起始点数目
 *
 *   spark.dynamicAllocation.schedulerBacklogTimeout (M) -
 *     If there are backlogged tasks for this duration, add new executors
  *     默认值1s,如果未分配的task等待分配的时间超过了这个配置的时间,表示需要新启动executor.
 *
 *   spark.dynamicAllocation.sustainedSchedulerBacklogTimeout (N) -
 *     If the backlog is sustained for this duration, add more executors
 *     This is used only after the initial backlog timeout is exceeded
  *    默认是4,1,配置项的值,这个配置用于设置在初始调度的executor调度延时后,每次的等待超时时间.
 *
 *   spark.dynamicAllocation.executorIdleTimeout (K) -
 *     If an executor has been idle for this duration, remove it
  *     默认值60s,executor的空闲回收时间.
  *
  *
  *    默认情况下不会创建ExecutorAllocationManager,可以修改属性spark.dynamicAllocation.enable为true来创建。
  *  ExecutorAllocationManager可以设置动态分配的最小Executor数量，动态分配的最大Executor数量，每个Executor可以运行
  *  的task数量等配置信息，并且对配置信息进行校验。start方法将ExecutorAllocationListener加入listenerBus中，
  *  ExecutorAllocationListener通过监听listenerBus里的时间，动态的添加，删除Executor.并且通过Thread不断地添加Executor,
  *  遍历Executor，将超时的Executor杀掉并且移除。ExecutorAllocationListener的实现与SparkListener类似。
 */
private[spark] class ExecutorAllocationManager(
    client: ExecutorAllocationClient,
    listenerBus: LiveListenerBus,
    conf: SparkConf)
  extends Logging {

  allocationManager =>

  import ExecutorAllocationManager._

  // Lower and upper bounds on the number of executors.  添加executor的最低最高的限制
  private val minNumExecutors = conf.get(DYN_ALLOCATION_MIN_EXECUTORS) // 默认值0,最少分配的executor的个数.
  private val maxNumExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS) // 默认值int.maxvalue.最大可分配的executor的个数.
  // 返回执行器的初始数量，用于动态分配。
  private val initialNumExecutors = Utils.getDynamicAllocationInitialExecutors(conf)  // initialNumExecutors:0

  // How long there must be backlogged tasks for before an addition is triggered (seconds)
  // 需要多长时间才能完成添加的任务(秒)
  // 默认值1s,如果未分配的task等待分配的时间超过了这个配置的时间,表示需要新启动executor.
  private val schedulerBacklogTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.schedulerBacklogTimeout", "1s")

  // Same as above, but used only after `schedulerBacklogTimeoutS` is exceeded
  // 与上面相同，但仅在超出“调度日志超时schedulerBacklogTimeoutS”之后才使用
  // 默认是4,1,配置项的值,这个配置用于设置在初始调度的executor调度延时后,每次的等待超时时间.
  private val sustainedSchedulerBacklogTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", s"${schedulerBacklogTimeoutS}s")    // cachedExecutorIdleTimeouts:2147483647

  // How long an executor must be idle for before it is removed (seconds) 执行器在删除前必须空闲多长时间(秒)
  // 默认值60s,executor的空闲回收时间. 删除Woker的触发条件是 一定时间内(默认60s)没有task运行的Executor
  /**
    * 这里实际是60*1000大约16个小时才会被删除
    */
  private val executorIdleTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.executorIdleTimeout", "60s")

  /**
    * Cache问题。如果需要移除的Executor含有RDD cache该如何办？
    * Cache去掉了重算即可。为了防止数据抖动，默认包含有Cache的Executor是不会被删除的，因为默认的Idle时间设置的非常大：
    */
  private val cachedExecutorIdleTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.cachedExecutorIdleTimeout", s"${Integer.MAX_VALUE}s")

  // During testing, the methods to actually kill and add executors are mocked out
  // 在测试过程中，实际上会显示杀死和添加执行器的方法
  private val testing = conf.getBoolean("spark.dynamicAllocation.testing", false) // 如果我们设置了spark.dynamicAllocation.testing为true,那么这里就是true

  // TODO: The default value of 1 for spark.executor.cores works right now because dynamic
  // allocation is only supported for YARN and the default number of cores per executor in YARN is
  // 1, but it might need to be attained differently for different cluster managers
  //  TODO:spark.executor.cores的默认值为1。核心之所以有效，是因为动态
  //  在YARN中，分配只支持YARN和每个执行者的默认核数，但对于不同的集群管理器，可能需要不同的分配
  // spark.shuffle.service.port配置项配置的shuffle的端口,同时对应BlockManager的shuffleClient不在是默认的BlockTransferService实例,而是ExternalShuffleClient实例.
  /** 一个cpu分配了几个核数，这些核数组成一个executor */
  private val tasksPerExecutor =
    conf.getInt("spark.executor.cores", 1) / conf.getInt("spark.task.cpus", 1)

  // 验证配置是否都正确，如果不正确就抛出异常
  validateSettings()

  // Number of executors to add in the next round 在下一个循环中增加的executor个数（数字越大增加的速度越快）
  // 1的时候是1 2 3 4 5 6 。。。
  // 2的时候是1 3 5 7 9 11 。。。
  private var numExecutorsToAdd = 1

  // The desired number of executors at this moment in time. If all our executors were to die, this
  // is the number of executors we would immediately want from the cluster manager.
  // 在这个时刻，期望的执行者数量。如果我们所有的执行者都死了，那么我们将立即从集群管理器中得到执行器的数量。
  // 假设我们stage需要20个executor 第一次分配了4个，不够，继续5个不够，一直增加到20个够了，但是突然这些分配的
  // executor死掉了，我们难道重新一个一个的加？如果这里记住，那么可一次增加到20个
  private var numExecutorsTarget = initialNumExecutors

  // Executors that have been requested to be removed but have not been killed yet
  // 被要求移除的执行者Executors，但还没有被杀死
  private val executorsPendingToRemove = new mutable.HashSet[String]

  // All known executors  所有已知的Executor的ID
  private val executorIds = new mutable.HashSet[String]

  // A timestamp of when an addition should be triggered, or NOT_SET if it is not set
  // This is set when pending tasks are added but not scheduled yet
  private var addTime: Long = NOT_SET   // addTime:9223372036854775807

  // A timestamp for each executor of when the executor should be removed, indexed by the ID
  // This is set when an executor is no longer running a task, or when it first registers
  // 当执行器被删除时，每个执行器的时间戳，由ID索引。
  // 这是在执行器不再运行任务或第一次注册时设置的
  /**（executor不在运行任务的时间，这样我们可以计算比如现在时间是12点，我们定期计划是没过20分钟如果这个executor还没有执行，
    * 我们就可以认定这个executor可以移除了）
    * 例如：
    *
    *   假设删除间隔executorIdleTimeoutS是16分钟（源代码是：60*1000=16个小时）
    *
    *   executor的字符串标志  未来要删除的时间    现在时间    是否运行任务        不运行时间  计算时间是否大于未来要删除的时间
    *   executor:@3bb87d36    12:16             12:16     开始运行一个任务     0        false
    *   executor:@3bb87d36    12:18             12:18     正在运行一个任务     0        false
    *   executor:@3bb87d36    12:20             12:20     停止任务            0        false
    *   此时executor:@3bb87d36处于空闲状态调用onExecutorIdle(executorId: String)，修改未来超时时间（没缓存块是16个小时）
    *   executor:@3bb87d36    28:20             12:20         无             0        false
    *   executor:@3bb87d36    28:20             18:21         无             1        false
    *   executor:@3bb87d36    28:20             22:28         无             8        false
    *   executor:@3bb87d36    28:20             28:20         无             21       true           这时候我们就可以移除这个executor了
    *
    */
  private val removeTimes = new mutable.HashMap[String, Long]

  // Polling loop interval (ms) 循环轮询时间间隔(ms)
  private val intervalMillis: Long = 100

  // Clock used to schedule when executors should be added and removed 当执行器应该被添加和删除时的时钟用于调度
  private var clock: Clock = new SystemClock()

  // Listener for Spark events that impact the allocation policy
  // 侦听影响分配策略的Spark事件 这个类在本文件中
  private val listener = new ExecutorAllocationListener

  // Executor that handles the scheduling task. 执行调度任务的执行程序。
  // Standlone调试：executor:"java.util.concurrent.ScheduleThreadPoolExecutor@3bb87d36[
  //    Running,pool size=0,active threads =0,queued tasks=0,complemeted task =0
  // ]"
  private val executor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("spark-dynamic-executor-allocation")

  // Metric source for ExecutorAllocationManager to expose internal status to MetricsSystem.
  // ExecutorAllocationManager的度量源，用于向MetricsSystem公开内部状态。
  val executorAllocationManagerSource = new ExecutorAllocationManagerSource

  // Whether we are still waiting for the initial set of executors to be allocated.
  // While this is true, we will not cancel outstanding executor requests. This is
  // set to false when:
  //   (1) a stage is submitted, or
  //   (2) an executor idle timeout has elapsed.
  // 不管我们是否还在等待分配的初始遗嘱。
  // 虽然这是真的，但我们不会取消未执行的执行请求。这是
  // 设置为false时:
  //   (1)提交一个阶段，或
  //   (2)一个executor空闲超时已经过去了。
  @volatile private var initializing: Boolean = true

  // Number of locality aware tasks, used for executor placement. 位置感知任务的数量，用于执行位置。
  private var localityAwareTasks = 0

  // Host to possible task running on it, used for executor placement.
  // 在Host上面运行可能的任务，用于执行器的放置。
  private var hostToLocalTaskCount: Map[String, Int] = Map.empty

  /**
   * Verify that the settings specified through the config are valid.
   * If not, throw an appropriate exception.
    *
    * 验证配置是否正确
   */
  private def validateSettings(): Unit = {
    if (minNumExecutors < 0 || maxNumExecutors < 0) {
      throw new SparkException("spark.dynamicAllocation.{min/max}Executors must be positive!")
    }
    if (maxNumExecutors == 0) {
      throw new SparkException("spark.dynamicAllocation.maxExecutors cannot be 0!")
    }
    if (minNumExecutors > maxNumExecutors) {
      throw new SparkException(s"spark.dynamicAllocation.minExecutors ($minNumExecutors) must " +
        s"be less than or equal to spark.dynamicAllocation.maxExecutors ($maxNumExecutors)!")
    }
    if (schedulerBacklogTimeoutS <= 0) {
      throw new SparkException("spark.dynamicAllocation.schedulerBacklogTimeout must be > 0!")
    }
    if (sustainedSchedulerBacklogTimeoutS <= 0) {
      throw new SparkException(
        "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout must be > 0!")
    }
    if (executorIdleTimeoutS <= 0) {
      throw new SparkException("spark.dynamicAllocation.executorIdleTimeout must be > 0!")
    }
    // Require external shuffle service for dynamic allocation
    // Otherwise, we may lose shuffle files when killing executors
    if (!conf.getBoolean("spark.shuffle.service.enabled", false) && !testing) {
      throw new SparkException("Dynamic allocation of executors requires the external " +
        "shuffle service. You may enable this through spark.shuffle.service.enabled.")
    }
    if (tasksPerExecutor == 0) {
      throw new SparkException("spark.executor.cores must not be less than spark.task.cpus.")
    }
  }

  /**
   * Use a different clock for this allocation manager. This is mainly used for testing.
    * 为这个分配管理器使用一个不同的时钟。这主要用于测试。
   */
  def setClock(newClock: Clock): Unit = {
    clock = newClock
  }

  /**
   * Register for scheduler callbacks to decide when to add and remove executors, and start
   * the scheduling task.
    *
    * 注册调度器回调，以决定何时添加和删除执行器，并启动调度任务。
   */
  def start(): Unit = {
    // 这里把ExecutorAllocationListener实例(内部实现类)添加到sparkContext中的listenerBus中,
    // 用于监听stage,task的启动与完成,并做对应的操作.
    listenerBus.addListener(listener)

    val scheduleTask = new Runnable() {
      override def run(): Unit = {
        try {
          schedule()
        } catch {
          case ct: ControlThrowable =>
            throw ct
          case t: Throwable =>
            logWarning(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        }
      }
    }
    // 实例化的executor 定时100ms执行一次schedule（）的调度函数,来进行task的分析.
    // 动态资源分配，是一个定时发起的任务，采用周期性触发的方式来发起。
    executor.scheduleWithFixedDelay(scheduleTask, 0, intervalMillis, TimeUnit.MILLISECONDS)

    // 这个方法还没解析
    client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
  }

  /**
   * Stop the allocation manager.
   */
  def stop(): Unit = {
    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
  }

  /**
   * Reset the allocation manager to the initial state. Currently this will only be called in
   * yarn-client mode when AM re-registers after a failure.
    *
    *
    * 将分配管理器（llocation manager ）重置为初始状态。目前只有yarn - client模式下当失败后重新注册时才会被调用。
   */
  def reset(): Unit = synchronized {
    initializing = true
    // 初始化numExecutorsTarget为0
    numExecutorsTarget = initialNumExecutors
    // numExecutorsToAdd因为每个executor的这个参数是会增肌的，所以要重置
    numExecutorsToAdd = 1

    // 把将要被移除的executor列表清空
    executorsPendingToRemove.clear()
    // 清空executor不在运行的时间列表
    removeTimes.clear()
  }

  /**
   * The maximum number of executors we would need under the current load to satisfy all running
   * and pending tasks, rounded up.
    *
    * 在当前负载下，我们需要执行的最大执行器数量，以满足所有运行和未决任务。
    * 系统会根据下面的公式计算出实际需要的Executors数目：
    *
    * 问题1：一个cpu分配一个核,组成一个executor(现在一个executor里面有一个核)，总共有30个task,不排队，直接一起运行（同时）
    * 是不是最多需要30个executor?

    * 问题2：4个cpu分配20个核(一个cpu里面5个核),组成一个executor(现在一个executor里面有5个核)，总共有30个task,不排队，
    * 直接一起运行（同时）是不是最多需要（30+5-1）/5=6个executor?
   */
  private def maxNumExecutorsNeeded(): Int = {
    // numRunningOrPendingTasks = 目前正在运行的阶段剩余任务总数的估计数 + 所有阶段Stages中运行的任务的数量。
    val numRunningOrPendingTasks = listener.totalPendingTasks + listener.totalRunningTasks
    //    （numRunningOrPendingTasks +  一个cpu分配了几个核数(是一个executor)  - 1 ）/ 一个cpu分配了几个核数(是一个executor)
    (numRunningOrPendingTasks + tasksPerExecutor - 1) / tasksPerExecutor
  }

  /**
   * This is called at a fixed interval to regulate the number of pending executor requests
   * and number of executors running.
    *
    * 这是在一个固定的时间间隔内调用，来管理正在准备运行的executor请求的数量和正在运行的executor的数量。
   *
   * First, adjust our requested executors based on the add time and our current needs.
   * Then, if the remove time for an existing executor has expired, kill the executor.
    *
    * 首先，根据添加时间和我们当前的需要调整我们请求的executors。
    * 然后，如果现有executor的删除时间过期，则杀死executor。
   *
   * This is factored out into its own method for testing.
    *
    * 针对task的调度主要由一个定时器每100ms进行一次schedule函数的调用.
   */
  private def schedule(): Unit = synchronized {
    val now = clock.getTimeMillis  // 在这个函数中,首先得到当前的时间,

    // 在调用这个函数时,初始情况下,initializing的属性值为true,这个时候,这个函数什么也不做.
    // 这个函数的内容,后面在进行分析.
    /** 增加Executor */
    updateAndSyncNumExecutorsTarget(now)

    val executorIdsToBeRemoved = ArrayBuffer[String]()
    // 这个removeTimes集合中记录有每一个executor没有被task占用后的时间,如果这个时间超过了上面配置的idle的时间,
    // 会移出掉这个executor,同时设置initializing属性为false,表示可以继续进行task的调度.retain函数只保留未超时
    // 的executor.(retain是保留的意思)
    removeTimes.retain { case (executorId, expireTime) =>
      //  executorId--->expireTime(未来要删除的时间)
      val expired = now >= expireTime     // 说明当前时间大于未来时间 可以删除了
      if (expired) {
        initializing = false
        executorIdsToBeRemoved += executorId   //将这个executor加入到要删除的数组中
      }
      !expired
    }

    // 如果要删除的数组中不为空 开始删除
    if (executorIdsToBeRemoved.nonEmpty) {
      /** 减少Executor */
      removeExecutors(executorIdsToBeRemoved)
    }
  }

  /**
   * Updates our target number of executors and syncs the result with the cluster manager.
   *
   * Check to see whether our existing allocation and the requests we've made previously exceed our
   * current needs. If so, truncate our target and let the cluster manager know so that it can
   * cancel pending requests that are unneeded.
   *
   * If not, and the add time has expired, see if we can request new executors and refresh the add
   * time.
   *
   * @return the delta in the target number of executors.
    *
    *    示例说明:
    *     假定这次的stage需要的executor的个数为5,numExecutorsTarget的配置保持默认值0,
    *     如果是第一次调度启动时,在updateAndSyncNumExecutorsTarget函数中:
   */
  private def updateAndSyncNumExecutorsTarget(now: Long): Int = synchronized {
    // 1,先计算出这个stage需要的executor的个数,
    val maxNeeded = maxNumExecutorsNeeded

    if (initializing) {
      // 如果函数进行这里,表示还没有stage提交,也就是没有job被执行.不进行调度.
      // Do not change our target while we are still initializing,
      // Otherwise the first job may have to ramp up unnecessarily
      0

    // stage实际计算得到需要的executor个数, 小于  执行器executor的初始数量
      /** 如果发现 maxNumExecutorsNeeded < numExecutorsTarget 则会发出取消还有没有执行的Container申请。
        * 并且重置每次申请的容器数为1,也就是numExecutorsToAdd=1
        * */
    } else if (maxNeeded < numExecutorsTarget) {
      // The target number exceeds the number we actually need, so stop adding new
      // executors and inform the cluster manager to cancel the extra pending requests
      val oldNumExecutorsTarget = numExecutorsTarget
      numExecutorsTarget = math.max(maxNeeded, minNumExecutors)
      numExecutorsToAdd = 1

      // If the new target has not changed, avoid sending a message to the cluster manager
      // 如果新目标没有更改，则避免向集群管理器发送消息  ，（将要分配的executor个数 小于 原来有的 那么就说明原来分配的已经够用了）
      if (numExecutorsTarget < oldNumExecutorsTarget) {
        client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
        logDebug(s"Lowering target number of executors to $numExecutorsTarget (previously " +
          s"$oldNumExecutorsTarget) because not all requested executors are actually needed")
      }

      numExecutorsTarget - oldNumExecutorsTarget

      // 2,进入的流程为else if (addTime != NOT_SET && now >= addTime)部分.这个时候执行addExecutors函数,
      // (这里假定时间已经达到了addTime的超时时间)
      // 这种情况下默认的初始executor的个数为0的情况下,在当前时间超过了等待超时时间后,会进入,第一次时需要等待一秒钟,
      // 每次执行会更新等待时间.这里根据要stage对应的task需要的executor的个数,并执行addExecutors的函数.

      // 3,调度定时器开始执行第二次调度启动,这个时候执行updateAndSyncNumExecutorsTarget函数时,
      // numExecutorsTarget的值为1,需要的executor的个数为3,因此,还是会执行时间超时的流程.
      /** 否则如果发现当前时间now >= addTime(addTime 每次会增加一个sustainedSchedulerBacklogTimeoutS ，
        * 避免申请容器过于频繁)，则会进行新容器的申请，如果是第一次，则增加一个(numExecutorsToAdd)，如果是第二次
        * 则增加2个以此按倍数类推。直到maxNumExecutorsNeeded <= numExecutorsTarget ,然后就会重置numExecutorsToAdd。
        *
        * 所以我们会发现，我们并不是一次性就申请足够的资源，而是每隔sustainedSchedulerBacklogTimeoutS次时间，
        * 按[1,2,4,8]这种节奏去申请资源的。因为在某个sustainedSchedulerBacklogTimeoutS期间，可能已经有很多任务完成了，
        * 其实不需要那么多资源了。而按倍数上升的原因是，防止为了申请到足够的资源时间花费过长。这是一种权衡。
        *
        * */
    } else if (addTime != NOT_SET && now >= addTime) {
      val delta = addExecutors(maxNeeded)
      logDebug(s"Starting timer to add more executors (to " +
        s"expire in $sustainedSchedulerBacklogTimeoutS seconds)")
      addTime = now + (sustainedSchedulerBacklogTimeoutS * 1000)
      delta
    } else {
      0
    }
  }

  /**
   * Request a number of executors from the cluster manager.
   * If the cap on the number of executors is reached, give up and reset the
   * number of executors to add next round instead of continuing to double it.
   *
   * @param maxNumExecutorsNeeded the maximum number of executors all currently running or pending
   *                              tasks could fill
   * @return the number of additional executors actually requested.
   */
  private def addExecutors(maxNumExecutorsNeeded: Int): Int = {
    // Do not request more executors if it would put our target over the upper bound
    // 在addExecutors函数中,先计算出目标的executor的个数(属性numExecutorsTarget),
    // 目标数量 大于  最大分配的数量
    if (numExecutorsTarget >= maxNumExecutors) {
      logDebug(s"Not adding executors because our current target total " +
        s"is already $numExecutorsTarget (limit $maxNumExecutors)")
      numExecutorsToAdd = 1
      return 0
    }

    val oldNumExecutorsTarget = numExecutorsTarget
    // There's no point in wasting time ramping up to the number of executors we already have, so
    // make sure our target is at least as much as our current allocation:
    numExecutorsTarget = math.max(numExecutorsTarget, executorIds.size)
    // Boost our target with the number to add for this round:
    numExecutorsTarget += numExecutorsToAdd
    // Ensure that our target doesn't exceed what we need at the present moment:
    numExecutorsTarget = math.min(numExecutorsTarget, maxNumExecutorsNeeded)
    // Ensure that our target fits within configured bounds:
    numExecutorsTarget = math.max(math.min(numExecutorsTarget, maxNumExecutors), minNumExecutors)

    // 此时executorIds的长度为0,集合是个空集合,这个时候numExecutorsToAdd的值为默认的1,根据上面的代码计算
    // 完成后(maxNumExecutorsNeeded为5就是tasks需要的executor的个数),numExecutorsTarget的值为1.接下来
    // 计算出来一个值,如果这次任务的目标executor的个数高于上次tasks启动的目标executor的个数,delta的值是
    // 一个大于0的值.根据上面的说明,下面代码中这个delta的值为1,
    val delta = numExecutorsTarget - oldNumExecutorsTarget

    // If our target has not changed, do not send a message
    // to the cluster manager and reset our exponential growth
    if (delta == 0) {
      // 如果delta等于0,表示这次的目标executor的个数,与上次任务的executor的个数相同,重置增量的个数为1.
      numExecutorsToAdd = 1
      return 0
    }

    // 接下来,通过下面的代码通过SparkContext发起numExecutorsTarget的executor的启动,并在executor中加载对应的
    // task的个数.
    val addRequestAcknowledged = testing ||
      client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
    if (addRequestAcknowledged) {
      val executorsString = "executor" + { if (delta > 1) "s" else "" }
      logInfo(s"Requesting $delta new $executorsString because tasks are backlogged" +
        s" (new desired total will be $numExecutorsTarget)")

      // 接下来,由于我们的任务执行还需要的executor的个数还需要4个(共需要),同时这个时候,delta的值为1,与numExecutorsToAdd的属性值相同,
      // 因此numExecutorsToAdd的值会*2.
      numExecutorsToAdd = if (delta == numExecutorsToAdd) {
        numExecutorsToAdd * 2
      } else {
        1
      }
      delta
    } else {
      logWarning(
        s"Unable to reach the cluster manager to request $numExecutorsTarget total executors!")
      numExecutorsTarget = oldNumExecutorsTarget
      0
    }
  }

  /**
   * Request the cluster manager to remove the given executors.
   * Returns the list of executors which are removed.
    *
    * 请求集群管理器删除给定的executors。
    * 返回删除的executor列表。（可能有没法删除的？）
   */
  private def removeExecutors(executors: Seq[String]): Seq[String] = synchronized {
    val executorIdsToBeRemoved = new ArrayBuffer[String]

    logInfo("Request to remove executorIds: " + executors.mkString(", "))
    // 所有已知的Executor的ID(不包括还没注册的)- 正准备要移除的 = 剩下的executor
    val numExistingExecutors = allocationManager.executorIds.size - executorsPendingToRemove.size

    var newExecutorTotal = numExistingExecutors
    executors.foreach { executorIdToBeRemoved =>
      // 如果移除这些后剩下的executor小于最低限度的executor 那么就报错（比方说我集群里面最少要有3个executor，无论这三个是否空闲，都要保留三个）
      if (newExecutorTotal - 1 < minNumExecutors) {
        logDebug(s"Not removing idle executor $executorIdToBeRemoved because there are only " +
          s"$newExecutorTotal executor(s) left (limit $minNumExecutors)")
        // 如果已知的executor列表中不包括这个executor和正准备移除的列表不包括这个executor
      } else if (canBeKilled(executorIdToBeRemoved)) {
        executorIdsToBeRemoved += executorIdToBeRemoved  // 加入到要移除列表
        newExecutorTotal -= 1   // 总的executor数目现存的 不被删除的 减去1
      }
    }

    if (executorIdsToBeRemoved.isEmpty) {
      return Seq.empty[String]
    }

    // Send a request to the backend to kill this executor(s)
    val executorsRemoved = if (testing) {
      executorIdsToBeRemoved  // 如果设置了spark.dynamicAllocation.testing这个executor只会增加不会被删除
    } else {
      // 真正删除executor 调用DummyLocalSchedulerBackend的killExecutors（）--->sparkContext.killExecutors()-->CoarseGrainedSchedulerBackend.killExecutors()方法
      client.killExecutors(executorIdsToBeRemoved)
    }
    // reset the newExecutorTotal to the existing number of executors
    newExecutorTotal = numExistingExecutors
    // 如果上面给了5个executor 但是删除了4个，还有有个没有被删除，那么就添加到顽固要删除的列表中
    if (testing || executorsRemoved.nonEmpty) {
      executorsRemoved.foreach { removedExecutorId =>
        newExecutorTotal -= 1
        logInfo(s"Removing executor $removedExecutorId because it has been idle for " +
          s"$executorIdleTimeoutS seconds (new desired total will be $newExecutorTotal)")
        executorsPendingToRemove.add(removedExecutorId)
      }
      executorsRemoved
    } else {
      logWarning(s"Unable to reach the cluster manager to kill executor/s " +
        s"${executorIdsToBeRemoved.mkString(",")} or no executor eligible to kill!")
      Seq.empty[String]
    }
  }

  /**
   * Request the cluster manager to remove the given executor.
   * Return whether the request is acknowledged.
   */
  private def removeExecutor(executorId: String): Boolean = synchronized {
    val executorsRemoved = removeExecutors(Seq(executorId))
    executorsRemoved.nonEmpty && executorsRemoved(0) == executorId
  }

  /**
   * Determine if the given executor can be killed.
    *
    * 决定给定的executor是否可以被删除
   */
  private def canBeKilled(executorId: String): Boolean = synchronized {
    // Do not kill the executor if we are not aware of it (should never happen)
    if (!executorIds.contains(executorId)) {
      logWarning(s"Attempted to remove unknown executor $executorId!")
      return false
    }

    // Do not kill the executor again if it is already pending to be killed (should never happen)
    if (executorsPendingToRemove.contains(executorId)) {
      logWarning(s"Attempted to remove executor $executorId " +
        s"when it is already pending to be removed!")
      return false
    }

    true
  }

  /**
   * Callback invoked when the specified executor has been added.
   */
  private def onExecutorAdded(executorId: String): Unit = synchronized {
    if (!executorIds.contains(executorId)) {
      // 添加到所有已知的Executor的ID列表中
      executorIds.add(executorId)
      // If an executor (call this executor X) is not removed because the lower bound
      // has been reached, it will no longer be marked as idle. When new executors join,
      // however, we are no longer at the lower bound, and so we must mark executor X
      // as idle again so as not to forget that it is a candidate for removal. (see SPARK-4951)
      executorIds.filter(listener.isExecutorIdle).foreach(onExecutorIdle)
      logInfo(s"New executor $executorId has registered (new total is ${executorIds.size})")
    } else {
      logWarning(s"Duplicate executor $executorId has registered")
    }
  }

  /**
   * Callback invoked when the specified executor has been removed.
    * 在删除指定的executor时调用回调函数。
   */
  private def onExecutorRemoved(executorId: String): Unit = synchronized {
    if (executorIds.contains(executorId)) {
      executorIds.remove(executorId)  // 所有已知的Executor的ID 要删除这个executor
      removeTimes.remove(executorId)  // 时间executor要删除
      logInfo(s"Existing executor $executorId has been removed (new total is ${executorIds.size})")
      // 要准备删除的也移除
      if (executorsPendingToRemove.contains(executorId)) {
        executorsPendingToRemove.remove(executorId)
        logDebug(s"Executor $executorId is no longer pending to " +
          s"be removed (${executorsPendingToRemove.size} left)")
      }
    } else {
      logWarning(s"Unknown executor $executorId has been removed!")
    }
  }

  /**
   * Callback invoked when the scheduler receives new pending tasks.
   * This sets a time in the future that decides when executors should be added
   * if it is not already set.
   */
  private def onSchedulerBacklogged(): Unit = synchronized {
    if (addTime == NOT_SET) {
      logDebug(s"Starting timer to add executors because pending tasks " +
        s"are building up (to expire in $schedulerBacklogTimeoutS seconds)")
      addTime = clock.getTimeMillis + schedulerBacklogTimeoutS * 1000
    }
  }

  /**
   * Callback invoked when the scheduler queue is drained.
   * This resets all variables used for adding executors.
   */
  private def onSchedulerQueueEmpty(): Unit = synchronized {
    logDebug("Clearing timer to add executors because there are no more pending tasks")
    addTime = NOT_SET
    numExecutorsToAdd = 1
  }

  /**
   * Callback invoked when the specified executor is no longer running any tasks.
   * This sets a time in the future that decides when this executor should be removed if
   * the executor is not already marked as idle.
    *
    * 当指定executor不再运行任何任务时调用回调函数。这将在将来设置一个时间，以决定当executor未标记为空闲时，
    * 该executor应该被删除。
    * （只要executor一空闲，就调用这个方法）
   */
  private def onExecutorIdle(executorId: String): Unit = synchronized {
    // 所有已知的executor 不包括那些刚刚创建的但是还没开始运行的executor(不然刚刚创建因为他没有执行任何任务，会被删除，造成没有executor的现象)
    if (executorIds.contains(executorId)) {
      //  removeTimes和executorsPendingToRemove准备移除的 里面都没有这个executor的时候
      if (!removeTimes.contains(executorId) && !executorsPendingToRemove.contains(executorId)) {
        // Note that it is not necessary to query the executors since all the cached
        // blocks we are concerned with are reported to the driver. Note that this
        // does not include broadcast blocks.
        // 查看executor是否缓存了块
        val hasCachedBlocks = SparkEnv.get.blockManager.master.hasCachedBlocks(executorId)
        val now = clock.getTimeMillis()   // 得到当掐时间
        val timeout = {
          if (hasCachedBlocks) {
            // Use a different timeout if the executor has cached blocks.
            // 如果这个executor缓存了块，现在处于空闲状态，肯定不能删除啊，所以我们给它设置一个超级大的超时时间，比如2000年后
            now + cachedExecutorIdleTimeoutS * 1000
          } else {
            // 如果这个executor没有缓存了块，现在处于空闲状态，所以我们给它设置 现在时间+超时时间60秒乘以1000=16个小时
            // 也就是说现在到16个小时以后 如果这个executor还是处于空闲状态，那么这个executor就会被删除
            now + executorIdleTimeoutS * 1000
          }
        }
        // 真正的超时时间
        val realTimeout = if (timeout <= 0) Long.MaxValue else timeout // overflow
        // removeTimes现在存储格式  executorId ---> 一个未来时间
        removeTimes(executorId) = realTimeout
        logDebug(s"Starting idle timer for $executorId because there are no more tasks " +
          s"scheduled to run on the executor (to expire in ${(realTimeout - now)/1000} seconds)")
      }
    } else {
      logWarning(s"Attempted to mark unknown executor $executorId idle")
    }
  }

  /**
   * Callback invoked when the specified executor is now running a task.
   * This resets all variables used for removing this executor.
   */
  private def onExecutorBusy(executorId: String): Unit = synchronized {
    logDebug(s"Clearing idle timer for $executorId because it is now running a task")
    // 当这个executor开始繁忙的时候（运行task就从removeTimes删除）
    removeTimes.remove(executorId)
  }















  /**
   * A listener that notifies the given allocation manager of when to add and remove executors.
    * 一个侦听器，它通知给定的分配管理器何时添加和删除执行器。
   *
   * This class is intentionally conservative in its assumptions about the relative ordering
   * and consistency of events returned by the listener. For simplicity, it does not account
   * for speculated tasks.
    *
    * 这个类对于侦听器返回的事件的相对顺序和一致性的假设是故意保守的。为了简单起见，它不考虑推测的任务。
   */
  private class ExecutorAllocationListener extends SparkListener {

    /**
        stageIdToNumTasks:Executor上是否为空,如果为空，就可以标记为Idle.只要超过一定的时间，就可以删除掉这个Executor.
        stageIdToTaskIndices:正在跑的Task有多少
        executorIdToTaskIds:等待调度的Task有多少
      */
    private val stageIdToNumTasks = new mutable.HashMap[Int, Int]
    private val stageIdToTaskIndices = new mutable.HashMap[Int, mutable.HashSet[Int]]
    private val executorIdToTaskIds = new mutable.HashMap[String, mutable.HashSet[Long]]
    // Number of tasks currently running on the cluster.  Should be 0 when no stages are active.
    private var numRunningTasks: Int = _

    // stageId to tuple (the number of task with locality preferences, a map where each pair is a
    // node and the number of tasks that would like to be scheduled on that node) map,
    // maintain the executor placement hints for each stage Id used by resource framework to better
    // place the executors.
    // stageId到tuple(位置首选项的数量，每个对都是一个节点的映射，以及希望在该节点上调度的任务的数量)，
    // 保持对资源框架使用的每个阶段Id的执行位置提示，以便更好地放置执行器。
    private val stageIdToExecutorPlacementHints = new mutable.HashMap[Int, (Int, Map[String, Int])]

    /**
        如何知道stage被提交?看下面,
        在SparkContext中,执行runJob命令时,针对一个stage进行submit操作时,会调用listenerBus中所有的listener对应的onStageSubmitted函数.
        而在ExecutorAllocationManager进行start操作时,生成了一个listener,实例为ExecutorAllocationListener,并把这个listener添加到了listenerBus中.
        接下来看看ExecutorAllocationListener中对应stage提交的监听处理:
     */
    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      // 这里首先把initializing的属性值设置为false,表示下次定时调度时,需要执行executor的分配操作.
      initializing = false
      // 得到进行submit操作对应的stage的id与stage中对应的task的个数.
      val stageId = stageSubmitted.stageInfo.stageId
      // 通过对应的stageId设置这个stage的task的个数,存储到stageIdToNumTasks集合中.
      val numTasks = stageSubmitted.stageInfo.numTasks
      // 这里更新allocationManager中的addTime的时间,
      // 由当前时间加上配置spark.dynamicAllocation.schedulerBacklogTimeout的超时时间.
      allocationManager.synchronized {
        stageIdToNumTasks(stageId) = numTasks
        allocationManager.onSchedulerBacklogged()

        // 这里根据每个task对应的host,计算出每个host对应的task的个数,numTasksPending的个数原则上应该与stage
        // 中numTask的个数相同.
        // Compute the number of tasks requested by the stage on each host
        var numTasksPending = 0
        val hostToLocalTaskCountPerStage = new mutable.HashMap[String, Int]()
        stageSubmitted.stageInfo.taskLocalityPreferences.foreach { locality =>
          if (!locality.isEmpty) {
            numTasksPending += 1
            locality.foreach { location =>
              val count = hostToLocalTaskCountPerStage.getOrElse(location.host, 0) + 1
              hostToLocalTaskCountPerStage(location.host) = count
            }
          }
        }

        // 在对应的集合中,根据stageid与pending的task的个数,对应的host与host对应的task的个数进行存储.
        stageIdToExecutorPlacementHints.put(stageId,
          (numTasksPending, hostToLocalTaskCountPerStage.toMap))


        // 下面的函数迭代stageIdToExecutorPlacementHints集合中的values,并更新allocationManager中
        // localityAwareTasks属性(存储待启动的task的个数)与hostToLocalTaskCount集合属性(存储host
        // 对应的task的个数)的值.添加到这里,主要是executor启动时对应的调度启动task
        // Update the executor placement hints
        updateExecutorPlacementHints()
      }
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val stageId = stageCompleted.stageInfo.stageId
      allocationManager.synchronized {
        stageIdToNumTasks -= stageId
        stageIdToTaskIndices -= stageId
        stageIdToExecutorPlacementHints -= stageId

        // Update the executor placement hints
        updateExecutorPlacementHints()

        // If this is the last stage with pending tasks, mark the scheduler queue as empty
        // This is needed in case the stage is aborted for any reason
        if (stageIdToNumTasks.isEmpty) {
          allocationManager.onSchedulerQueueEmpty()
          if (numRunningTasks != 0) {
            logWarning("No stages are running, but numRunningTasks != 0")
            numRunningTasks = 0
          }
        }
      }
    }

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
      val stageId = taskStart.stageId
      val taskId = taskStart.taskInfo.taskId
      val taskIndex = taskStart.taskInfo.index
      val executorId = taskStart.taskInfo.executorId

      allocationManager.synchronized {
        numRunningTasks += 1
        // This guards against the race condition in which the `SparkListenerTaskStart`
        // event is posted before the `SparkListenerBlockManagerAdded` event, which is
        // possible because these events are posted in different threads. (see SPARK-4951)
        if (!allocationManager.executorIds.contains(executorId)) {
          allocationManager.onExecutorAdded(executorId)
        }

        // If this is the last pending task, mark the scheduler queue as empty
        stageIdToTaskIndices.getOrElseUpdate(stageId, new mutable.HashSet[Int]) += taskIndex
        if (totalPendingTasks() == 0) {
          allocationManager.onSchedulerQueueEmpty()
        }

        // Mark the executor on which this task is scheduled as busy
        executorIdToTaskIds.getOrElseUpdate(executorId, new mutable.HashSet[Long]) += taskId
        allocationManager.onExecutorBusy(executorId)
      }
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      val executorId = taskEnd.taskInfo.executorId
      val taskId = taskEnd.taskInfo.taskId
      val taskIndex = taskEnd.taskInfo.index
      val stageId = taskEnd.stageId
      allocationManager.synchronized {
        numRunningTasks -= 1
        // If the executor is no longer running any scheduled tasks, mark it as idle
        if (executorIdToTaskIds.contains(executorId)) {
          executorIdToTaskIds(executorId) -= taskId
          if (executorIdToTaskIds(executorId).isEmpty) {
            executorIdToTaskIds -= executorId
            allocationManager.onExecutorIdle(executorId)
          }
        }

        // If the task failed, we expect it to be resubmitted later. To ensure we have
        // enough resources to run the resubmitted task, we need to mark the scheduler
        // as backlogged again if it's not already marked as such (SPARK-8366)
        if (taskEnd.reason != Success) {
          if (totalPendingTasks() == 0) {
            allocationManager.onSchedulerBacklogged()
          }
          stageIdToTaskIndices.get(stageId).foreach { _.remove(taskIndex) }
        }
      }
    }

    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
      val executorId = executorAdded.executorId
      if (executorId != SparkContext.DRIVER_IDENTIFIER) {
        // This guards against the race condition in which the `SparkListenerTaskStart`
        // event is posted before the `SparkListenerBlockManagerAdded` event, which is
        // possible because these events are posted in different threads. (see SPARK-4951)
        if (!allocationManager.executorIds.contains(executorId)) {
          allocationManager.onExecutorAdded(executorId)
        }
      }
    }

    override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
      allocationManager.onExecutorRemoved(executorRemoved.executorId)
    }

    /**
     * An estimate of the total number of pending tasks remaining for currently running stages. Does
     * not account for tasks which may have failed and been resubmitted.
      *
      * 准备运行的task任务数量。不包括那些可能已经失败或者被重新提交的任务。
     *
     * Note: This is not thread-safe without the caller owning the `allocationManager` lock.
      * 注意:如果没有调用“allocationManager”的锁，这将不是线程安全的。
     */
    def totalPendingTasks(): Int = {
      stageIdToNumTasks.map { case (stageId, numTasks) =>
        numTasks - stageIdToTaskIndices.get(stageId).map(_.size).getOrElse(0)
      }.sum
    }

    /**
     * The number of tasks currently running across all stages.
      *
      * 当前在所有阶段Stages中运行的任务的数量。
     */
    def totalRunningTasks(): Int = numRunningTasks

    /**
     * Return true if an executor is not currently running a task, and false otherwise.
     *
     * Note: This is not thread-safe without the caller owning the `allocationManager` lock.
     */
    def isExecutorIdle(executorId: String): Boolean = {
      !executorIdToTaskIds.contains(executorId)
    }

    /**
     * Update the Executor placement hints (the number of tasks with locality preferences,
     * a map where each pair is a node and the number of tasks that would like to be scheduled
     * on that node).
     *
     * These hints are updated when stages arrive and complete, so are not up-to-date at task
     * granularity within stages.
     */
    def updateExecutorPlacementHints(): Unit = {
      var localityAwareTasks = 0
      val localityToCount = new mutable.HashMap[String, Int]()
      stageIdToExecutorPlacementHints.values.foreach { case (numTasksPending, localities) =>
        localityAwareTasks += numTasksPending
        localities.foreach { case (hostname, count) =>
          val updatedCount = localityToCount.getOrElse(hostname, 0) + count
          localityToCount(hostname) = updatedCount
        }
      }

      allocationManager.localityAwareTasks = localityAwareTasks
      allocationManager.hostToLocalTaskCount = localityToCount.toMap
    }
  }

  /**
   * Metric source for ExecutorAllocationManager to expose its internal executor allocation
   * status to MetricsSystem.
   * Note: These metrics heavily rely on the internal implementation of
   * ExecutorAllocationManager, metrics or value of metrics will be changed when internal
   * implementation is changed, so these metrics are not stable across Spark version.
    *
    * ExecutorAllocationManager的度量源，用于向MetricsSystem公开内部状态。
    *
    * 注意:这些指标严重依赖于执行程序管理器ExecutorAllocationManager的内部实现，当内部实现更改时，
    * 度量指标或值的值将被更改，因此这些指标在Spark版本之间不稳定。
   */
  private[spark] class ExecutorAllocationManagerSource extends Source {
    val sourceName = "ExecutorAllocationManager"
    val metricRegistry = new MetricRegistry()

    private def registerGauge[T](name: String, value: => T, defaultValue: T): Unit = {
      metricRegistry.register(MetricRegistry.name("executors", name), new Gauge[T] {
        override def getValue: T = synchronized { Option(value).getOrElse(defaultValue) }
      })
    }

    registerGauge("numberExecutorsToAdd", numExecutorsToAdd, 0)
    registerGauge("numberExecutorsPendingToRemove", executorsPendingToRemove.size, 0)
    registerGauge("numberAllExecutors", executorIds.size, 0)
    registerGauge("numberTargetExecutors", numExecutorsTarget, 0)
    registerGauge("numberMaxNeededExecutors", maxNumExecutorsNeeded(), 0)
  }






}

private object ExecutorAllocationManager {
  val NOT_SET = Long.MaxValue
}
