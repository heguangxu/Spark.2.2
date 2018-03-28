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

package org.apache.spark.executor

import java.io.{File, NotSerializableException}
import java.lang.Thread.UncaughtExceptionHandler
import java.lang.management.ManagementFactory
import java.net.{URI, URL}
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent._
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, Map}
import scala.util.control.NonFatal

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.scheduler.{DirectTaskResult, IndirectTaskResult, Task, TaskDescription}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer

/**
  * Spark executor, backed by a threadpool to run tasks.
  *
  * Spark的执行者，由一个线程池来运行任务。
  *
  * This can be used with Mesos, YARN, and the standalone scheduler.
  * An internal RPC interface is used for communication with the driver,
  * except in the case of Mesos fine-grained mode.
  *
  * 这可以在Mesos，YARN，和standalone的调度模式下使用。内部RPC接口用于与driver沟通，
  * 除了在Mesos的细粒度的模式下。
  *
  * Executor的构建如下：
  *   1.创建并且注册ExecutorSource。
  *   2.获取SparkEnv。如果是非local莫斯，Worker上的CoarseGrainedExecutorBackend向Driver上的
  *     CoarseGrainedExecutorBackend注册Executor时，则需要新建SparkEnv.可以修改属性spark.executor.port
  *     (默认为0，表示随机生成)来配置Executor中的ActorSystem的端口号。
  *   3.创建并且注册ExecotorActor。ExecutorActor负责接收发送给Execotor的消息。
  *   4.urlClassLoader的创建。为什么需要创建这个ClassLoader?在非local模式下，Driver或者Worker上会有很多个
  *     Executor，每个Executor都设置自身的urlClassLoader，用于加载任务上传的jar包中的类，有效对任务的类加载环境进行隔离。
  *   5.创建Executor执行Task的线程池。此线程池用于执行任务。
  *   6.启动Executor的心跳线程池，用于向Driver发送心跳。
  *
  */
private[spark] class Executor(
                               executorId: String,
                               executorHostname: String,
                               env: SparkEnv,
                               userClassPath: Seq[URL] = Nil,
                               isLocal: Boolean = false,
                               uncaughtExceptionHandler: UncaughtExceptionHandler = SparkUncaughtExceptionHandler)
  extends Logging {

  //24=> 17/12/05 11:56:51 INFO Executor: Starting executor ID driver on host localhost
  logInfo(s"Starting executor ID $executorId on host $executorHostname")

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  // 应用程序依赖关系（通过sparkcontext添加），我们把到目前为止在这个节点。每一个map都保存了我们所得到的文件或JAR版本的主时间戳。
  private val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  private val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  private val conf = env.conf

  // No ip or host:port - just hostname         不要IP或者 host:port  只要hostname主机名
  Utils.checkHost(executorHostname, "Expected executed slave to be a hostname")
  // must not have port specified.              不能指定端口。
  assert (0 == Utils.parseHostPort(executorHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  // 确保本地主机名与我们在集群调度的名字相同
  Utils.setCustomHostname(executorHostname)

  // 默认为false ，这里为true
  if (!isLocal) {
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions kill the entire
    // executor process to avoid surprising stalls.

    // 设置为非本地模式未捕获的异常处理程序。任何线程终端由于未捕获的异常杀死整个执行过程中避免意外的摊位。
    Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler)
  }

  // Start worker thread pool   启动worker线程池
  private val threadPool = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("Executor task launch worker-%d")
      .setThreadFactory(new ThreadFactory {
        override def newThread(r: Runnable): Thread =
        // Use UninterruptibleThread to run tasks so that we can allow running codes without being
        // interrupted by `Thread.interrupt()`. Some issues, such as KAFKA-1894, HADOOP-10622,
        // will hang forever if some methods are interrupted.

        // 使用uninterruptiblethread运行任务，这样我们可以允许运行代码不受`Thread.interrupt()`的影响。一些问题，
        // 如kafka-1894，hadoop-10622，如果一些方法被中断了会永远挂着。
          new UninterruptibleThread(r, "unused") // thread name will be set by ThreadFactoryBuilder
      })
      .build()
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /** ExecutorSource用于测量系统。
    * */
  private val executorSource = new ExecutorSource(threadPool, executorId)


  // Pool used for threads that supervise task killing / cancellation
  // 用于监视任务killing/取消的线程池
  private val taskReaperPool = ThreadUtils.newDaemonCachedThreadPool("Task reaper")
  // For tasks which are in the process of being killed, this map holds the most recently created
  // TaskReaper. All accesses to this map should be synchronized on the map itself (this isn't
  // a ConcurrentHashMap because we use the synchronization for purposes other than simply guarding
  // the integrity of the map's internal state). The purpose of this map is to prevent the creation
  // of a separate TaskReaper for every killTask() of a given task. Instead, this map allows us to
  // track whether an existing TaskReaper fulfills the role of a TaskReaper that we would otherwise
  // create. The map key is a task id.

  // tasks任务进程被杀死的时候，这个map保存了最近创建的taskreaper。所有访问该map应该同步在map上
  // （这并不是因为我们使用ConcurrentHashMap同步而不是简单地守着map的内部状态的完整性）。
  // 这个map的目的是为了防止对一个给定的任务，每一killtask()单独taskreaper创作。
  // 相反，这张地图让我们跟踪是否现有的taskreaper满足一taskreaper的作用，我们将创造。map键是一个任务ID。
  private val taskReaperForTask: HashMap[Long, TaskReaper] = HashMap[Long, TaskReaper]()

  // 这里为true
  if (!isLocal) {
    env.metricsSystem.registerSource(executorSource)
    /** 初始化metricsSystem */
    env.blockManager.initialize(conf.getAppId)
  }

  // Whether to load classes in user jars before those in Spark jars
  // 是否在Spark的jars加载之前加载用户自己的jars
  private val userClassPathFirst = conf.getBoolean("spark.executor.userClassPathFirst", false)

  // Whether to monitor killed / interrupted tasks
  // 是否监视被杀/中断的任务
  private val taskReaperEnabled = conf.getBoolean("spark.task.reaper.enabled", false)

  // Create our ClassLoader
  // do this after SparkEnv creation so can access the SecurityManager
  // 在创建SparkEnv之后创造我们的类加载器这样做可以访问SecurityManager
  /**
    * 为什么创建这个ClassLoader?在非local模式中，Driver或者Woeker上都会有多个Executor,每个Executor都设置自身的
    * urlClassLoader,用于加载任务上传的jar包中的类，有效对任务的类加载环境进行隔离。
    * */
  private val urlClassLoader = createClassLoader()
  private val replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader)

  // Set the classloader for serializer
  // 对于序列化程序集的类加载器
  env.serializer.setDefaultClassLoader(replClassLoader)

  // Max size of direct result. If task result is bigger than this, we use the block manager
  // to send the result back.
  // 直接结果的最大大小。如果任务结果大于此，我们使用块管理器将结果返回。
  private val maxDirectResultSize = Math.min(
    conf.getSizeAsBytes("spark.task.maxDirectResultSize", 1L << 20),
    RpcUtils.maxMessageSizeBytes(conf))

  // Limit of bytes for total size of results (default is 1GB)
  // 对结果的总大小限制的字节数（默认为1GB）
  private val maxResultSize = Utils.getMaxResultSize(conf)

  // Maintains the list of running tasks.
  // 维护正在运行的任务列表。
  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  // Executor for the heartbeat task.
  // 心跳任务执行器。
  private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-heartbeater")

  // must be initialized before running startDriverHeartbeat()
  // 在运行startDriverHeartbeat()之前必须初始化
  private val heartbeatReceiverRef =
  RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)

  /**
    * When an executor is unable to send heartbeats to the driver more than `HEARTBEAT_MAX_FAILURES`
    * times, it should kill itself. The default value is 60. It means we will retry to send
    * heartbeats about 10 minutes because the heartbeat interval is 10s.
    *
    * 当一个执行者是无法发送心跳给driver超过 `HEARTBEAT_MAX_FAILURES`设置的时间，它会杀死自己。默认值是60。
    * 这意味着我们将重试发送约10分钟的心跳，因为心跳间隔10s。
    *
    */
  private val HEARTBEAT_MAX_FAILURES = conf.getInt("spark.executor.heartbeat.maxFailures", 60)

  /**
    * Count the failure times of heartbeat. It should only be accessed in the heartbeat thread. Each
    * successful heartbeat will reset it to 0.
    *
    * 计算心跳失败次数。它只应在心跳线程访问。每一个成功的心跳将会重置为0。
    *
    */
  private var heartbeatFailures = 0

  // 执行发送心跳的线程
  startDriverHeartbeater()

  private[executor] def numRunningTasks: Int = runningTasks.size()

  /**
    * 调用Executor的launchTask方法时，标志着任务执行阶段的开始。执行过程如下：
    *
    * 1.创建TaskRunner，并且将其与taskId,taskName及serializedTask(这三个都被TaskDescription对象包含了)
    *   添加到runningTasks = new ConcurrentHashMap[Long,TaskRuuner]中。
    * 2.TaskRunner实现了Runnable接口（Scala中成为继承Runnable特质），最后使用线程池执行TaskRunner.
    *
    */
  def launchTask(context: ExecutorBackend, taskDescription: TaskDescription): Unit = {
    //实例化一个TaskRunner对象来执行Task
    val tr = new TaskRunner(context, taskDescription)
    //将Task加入到正在运行的Task队列
    runningTasks.put(taskDescription.taskId, tr)

    /**
      * 任务最终运行的地方
      * 在worker的线程池中,
      * 线程池调用org.apache.spark.executor.Executor.TaskRunner.run（）方法运行任务
      * 就是这个文件中的内部类
      */
    threadPool.execute(tr)
  }


  // 杀死任务
  def killTask(taskId: Long, interruptThread: Boolean, reason: String): Unit = {
    // 得到正在运行的任务
    val taskRunner = runningTasks.get(taskId)
    if (taskRunner != null) {
      if (taskReaperEnabled) {
        val maybeNewTaskReaper: Option[TaskReaper] = taskReaperForTask.synchronized {
          val shouldCreateReaper = taskReaperForTask.get(taskId) match {
            case None => true
            case Some(existingReaper) => interruptThread && !existingReaper.interruptThread
          }
          if (shouldCreateReaper) {
            val taskReaper = new TaskReaper(
              taskRunner, interruptThread = interruptThread, reason = reason)
            taskReaperForTask(taskId) = taskReaper
            Some(taskReaper)
          } else {
            None
          }
        }
        // Execute the TaskReaper from outside of the synchronized block.
        maybeNewTaskReaper.foreach(taskReaperPool.execute)
      } else {
        taskRunner.kill(interruptThread = interruptThread, reason = reason)
      }
    }
  }

  /**
    * Function to kill the running tasks in an executor.
    * This can be called by executor back-ends to kill the
    * tasks instead of taking the JVM down.
    *
    * 用来杀死执行器中正在运行的任务函数。这可以由执行器后端调用来杀死任务，而不是将JVM删除。
    *
    * @param interruptThread whether to interrupt the task thread  是否中断任务线程
    */
  def killAllTasks(interruptThread: Boolean, reason: String) : Unit = {
    runningTasks.keys().asScala.foreach(t =>
      killTask(t, interruptThread = interruptThread, reason = reason))
  }

  def stop(): Unit = {
    env.metricsSystem.report()
    heartbeater.shutdown()
    heartbeater.awaitTermination(10, TimeUnit.SECONDS)
    threadPool.shutdown()
    if (!isLocal) {
      env.stop()
    }
  }

  /** Returns the total amount of time this JVM process has spent in garbage collection.
    * 返回此JVM进程花费在垃圾收集中的总时间。
    * */
  private def computeTotalGcTime(): Long = {
    ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum
  }













  /**
    *  内部类
    *
    *  Runnable：
    *
    *  The <code>Runnable</code> interface should be implemented by any
    *  class whose instances are intended to be executed by a thread. The
    *  class must define a method of no arguments called <code>run</code>.
    *  <p>
    *  This interface is designed to provide a common protocol for objects that
    *  wish to execute code while they are active. For example,
    *  <code>Runnable</code> is implemented by class <code>Thread</code>.
    *  Being active simply means that a thread has been started and has not
    *  yet been stopped.
    *  <p>
    *  In addition, <code>Runnable</code> provides the means for a class to be
    *  active while not subclassing <code>Thread</code>. A class that implements
    *  <code>Runnable</code> can run without subclassing <code>Thread</code>
    *  by instantiating a <code>Thread</code> instance and passing itself in
    *  as the target.  In most cases, the <code>Runnable</code> interface should
    *  be used if you are only planning to override the <code>run()</code>
    *  method and no other <code>Thread</code> methods.
    *  This is important because classes should not be subclassed
    *  unless the programmer intends on modifying or enhancing the fundamental
    *  behavior of the class.
    *
    *    接口应该由任何一个要被线程执行的类来实现。这个类必须定义一个名为 run 的无参方法。
    *
    *    这个接口被设计用来为希望在执行代码的对象提供一个通用的协议。例如，Runnable由Thread类实现。active仅仅意味着线程已经启动并没有停止。
    *
    *   此外，Runnable还提供了一种方法，用于在不子类化线程的情况下进行活动。一个实现Runnable的类可以
    *   通过实例化一个线程实例并将自己作为目标传递，而无需子类化线程。在大多数情况下，如果您只打算重写run()方法，
    *   而没有其他线程方法，则应该使用Runnable接口。这很重要，因为类不应该被子类化，除非程序员打算修改或增强类的基本行为。
    *
    */

  class TaskRunner(
                    execBackend: ExecutorBackend,
                    private val taskDescription: TaskDescription)
    extends Runnable {

    val taskId = taskDescription.taskId
    val threadName = s"Executor task launch worker for task $taskId"
    private val taskName = taskDescription.name

    /** If specified, this task has been killed and this option contains the reason.
      * 如果指定，此任务已被杀死，此选项包含原因。
      * */
    @volatile private var reasonIfKilled: Option[String] = None

    @volatile private var threadId: Long = -1

    def getThreadId: Long = threadId

    /** Whether this task has been finished.
      * 这个任务是否完成了。
      * */
    @GuardedBy("TaskRunner.this")
    private var finished = false

    def isFinished: Boolean = synchronized { finished }

    /** How much the JVM process has spent in GC when the task starts to run.
      * 当任务开始运行时，JVM进程在GC中花费了多少。
      * */
    @volatile var startGCTime: Long = _

    /**
      * The task to run. This will be set in run() by deserializing the task binary coming
      * from the driver. Once it is set, it will never be changed.
      *
      * 要运行的任务。这将通过序列化任务二来自驱动设置在run()。一旦设置，它将永远不会改变。
      *
      */
    @volatile var task: Task[Any] = _

    def kill(interruptThread: Boolean, reason: String): Unit = {
      logInfo(s"Executor is trying to kill $taskName (TID $taskId), reason: $reason")
      reasonIfKilled = Some(reason)
      if (task != null) {
        synchronized {
          if (!finished) {
            task.kill(interruptThread, reason)
          }
        }
      }
    }

    /**
      * Set the finished flag to true and clear the current thread's interrupt status
      * 将已完成的标志设置为true，并清除当前线程的中断状态。
      */
    private def setTaskFinishedAndClearInterruptStatus(): Unit = synchronized {
      this.finished = true
      // SPARK-14234 - Reset the interrupted status of the thread to avoid the
      // ClosedByInterruptException during execBackend.statusUpdate which causes
      // Executor to crash
      // spark-14234复位中断状态的线程来避免execbackend.statusupdate导致执行者在closedbyinterruptexception崩溃

      Thread.interrupted()
      // Notify any waiting TaskReapers. Generally there will only be one reaper per task but there
      // is a rare corner-case where one task can have two reapers in case cancel(interrupt=False)
      // is followed by cancel(interrupt=True). Thus we use notifyAll() to avoid a lost wakeup:

      // 通知任何等待taskreapers。通常每个任务只会有一个reaper(收割者)但有一个罕见的情况下，
      // 一个任务可以有2个reapers在那种情况下取消（中断interrupt= false）其次是取消（中断interrupt= true）。
      // 因此，我们使用notifyall()避免失去唤醒：
      notifyAll()
    }

    /**
      * 　　这个方法是Executor执行task的主要方法。Task在Executor中执行完成后，会通过向Driver发送StatusUpdate的消息来
      * 通知Driver任务的状态更新为TaskState.FINISHED。
      *
      * 在Executor运行Task时，得到计算结果会存入org.apache.spark.scheduler.DirectTaskResult。在将结果传回Driver时，
      * 会根据结果的大小有不同的策略：对于较大的结果，将其以taskId为key存入org.apache.storage.BlockManager，如果结果不大，
      * 则直接回传给Driver。回传是通过AKKA来实现的，所以能够回传的值会有一个由AKKA限制的大小，这里涉及到一个参数
      * spark.akka.frameSize，默认为128，单位为Byte，在源码中最终转换成了128MB。表示AKKA最大能传递的消息大小为128MB，
      * 但是同时AKKA会保留一部分空间用于存储其他数据，这部分的大小为200KB，那么结果如果小于128MB - 200KB的话就可以直接返回该值，
      * 否则的话，在不大于1G的情况下（可以通过参数spark.driver.maxResultSize来修改，默认为1g），会通过BlockManager来传递。
      * 详细信息会在Executor模块中描述。完整情况如下：
      * （1）如果结果大于1G，直接丢弃
      * （2）如果结果小于等于1G，大于128MB - 200KB，通过BlockManager记录结果的tid和其他信息
      * （3）如果结果小于128MB - 200 KB，直接返回该值
      * */
    override def run(): Unit = {
      threadId = Thread.currentThread.getId
      Thread.currentThread.setName(threadName)
      // 返回Java虚拟机的线程系统的托管bean。
      val threadMXBean = ManagementFactory.getThreadMXBean
      //为我们的Task创建内存管理器 ==》 管理单个任务分配的内存。
      val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)
      //记录反序列化时间
      val deserializeStartTime = System.currentTimeMillis()
      //测试如果Java虚拟机支持当前线程的CPU时间度量。这个貌似很高深，不理解先放着？
      val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
        threadMXBean.getCurrentThreadCpuTime
      } else 0L
      //加载具体类时需要用到ClassLoader
      Thread.currentThread.setContextClassLoader(replClassLoader)
      //创建序列化器
      val ser = env.closureSerializer.newInstance()
      logInfo(s"Running $taskName (TID $taskId)")
      // 更新任务的 状态
      // 开始执行Task，
      // yarn-client模式下，调用CoarseGrainedExecutorBackend的statusUpdate方法
      // 将该Task的运行状态置为RUNNING
      // 调用ExecutorBackend#statusUpdate向Driver发信息汇报当前状态
      // 以local为例：那么这里调用的是LocalSchedulerBackend中的statusUpdate方法
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      //记录运行时间和GC信息
      var taskStart: Long = 0
      var taskStartCpu: Long = 0
      startGCTime = computeTotalGcTime()

      try {
        // Must be set before updateDependencies() is called, in case fetching dependencies
        // requires access to properties contained within (e.g. for access control).
        // 必须在updateDependencies()调用之前设置，以防获取依赖关系需要访问包含在(例如访问控制)中的属性。
        Executor.taskDeserializationProps.set(taskDescription.properties)

        //下载Task运行缺少的依赖。
        updateDependencies(taskDescription.addedFiles, taskDescription.addedJars)
        //反序列化Task
        task = ser.deserialize[Task[Any]](
          taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)
        task.localProperties = taskDescription.properties
        //设置Task运行时的MemoryManager
        task.setTaskMemoryManager(taskMemoryManager)

        // If this task has been killed before we deserialized it, let's quit now. Otherwise,
        // continue executing the task.
        // 如果在序列化之前杀掉任务了，那么我们退出，否则继续执行任务
        // 要运行的任务，是不是指定被杀死了，比如，我提交了一个任务，刚刚提交发现错了，直接ctrl+c终止程序了，这时候任务还没运行，相当于指定这个任务被杀死
        val killReason = reasonIfKilled
        // 判断如果该task被kill了，直接抛出异常
        if (killReason.isDefined) {
          // Throw an exception rather than returning, because returning within a try{} block
          // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
          // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
          // for the task.
          throw new TaskKilledException(killReason.get)
        }

        logDebug("Task " + taskId + "'s epoch is " + task.epoch)
        env.mapOutputTracker.updateEpoch(task.epoch)

        // Run the actual task and measure its runtime.
        //运行的实际任务，并测量它的运行时间。
        taskStart = System.currentTimeMillis()
        taskStartCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L
        var threwException = true
        val value = try {
          /** 调用Task.run方法，开始运行task */
          val res = task.run(
            taskAttemptId = taskId,
            attemptNumber = taskDescription.attemptNumber,
            metricsSystem = env.metricsSystem)
          threwException = false
          res
        } finally {
          //清理所有分配的内存和分页,并检测是否有内存泄漏
          val releasedLocks = env.blockManager.releaseAllLocksForTask(taskId)
          val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()

          if (freedMemory > 0 && !threwException) {
            val errMsg = s"Managed memory leak detected; size = $freedMemory bytes, TID = $taskId"
            if (conf.getBoolean("spark.unsafe.exceptionOnMemoryLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logWarning(errMsg)
            }
          }

          if (releasedLocks.nonEmpty && !threwException) {
            val errMsg =
              s"${releasedLocks.size} block locks were not released by TID = $taskId:\n" +
                releasedLocks.mkString("[", ", ", "]")
            if (conf.getBoolean("spark.storage.exceptionOnPinLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logInfo(errMsg)
            }
          }
        }
        task.context.fetchFailed.foreach { fetchFailure =>
          // uh-oh.  it appears the user code has caught the fetch-failure without throwing any
          // other exceptions.  Its *possible* this is what the user meant to do (though highly
          // unlikely).  So we will log an error and keep going.
          logError(s"TID ${taskId} completed successfully though internally it encountered " +
            s"unrecoverable fetch failures!  Most likely this means user code is incorrectly " +
            s"swallowing Spark's internal ${classOf[FetchFailedException]}", fetchFailure)
        }

        //记录Task完成时间
        val taskFinish = System.currentTimeMillis()
        val taskFinishCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L

        // If the task has been killed, let's fail it.
        task.context.killTaskIfInterrupted()

        //否则序列化得到的Task执行的结果
        val resultSer = env.serializer.newInstance()
        val beforeSerialization = System.currentTimeMillis()
        val valueBytes = resultSer.serialize(value)
        val afterSerialization = System.currentTimeMillis()


        //记录相关的metrics
        // Deserialization happens in two parts: first, we deserialize a Task object, which
        // includes the Partition. Second, Task.run() deserializes the RDD and function to be run.
        task.metrics.setExecutorDeserializeTime(
          (taskStart - deserializeStartTime) + task.executorDeserializeTime)
        task.metrics.setExecutorDeserializeCpuTime(
          (taskStartCpu - deserializeStartCpuTime) + task.executorDeserializeCpuTime)
        // We need to subtract Task.run()'s deserialization time to avoid double-counting
        task.metrics.setExecutorRunTime((taskFinish - taskStart) - task.executorDeserializeTime)
        task.metrics.setExecutorCpuTime(
          (taskFinishCpu - taskStartCpu) - task.executorDeserializeCpuTime)
        task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
        task.metrics.setResultSerializationTime(afterSerialization - beforeSerialization)

        // Note: accumulator updates must be collected after TaskMetrics is updated
        val accumUpdates = task.collectAccumulatorUpdates()

        //创建直接返回给Driver的结果对象DirectTaskResult
        // 生成DirectTaskResult对象，并序列化Task的运行结果
        // TODO: do not serialize value twice
        val directResult = new DirectTaskResult(valueBytes, accumUpdates)
        val serializedDirectResult = ser.serialize(directResult)
        val resultSize = serializedDirectResult.limit

        // directSend = sending directly back to the driver
        // 如果序列化后的结果比spark.driver.maxResultSize配置的还大，直接丢弃该结果
        val serializedResult: ByteBuffer = {
          //对直接返回的结果对象大小进行判断
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            // 大于最大限制1G，直接丢弃ResultTask
            logWarning(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize " +
              s"(${Utils.bytesToString(resultSize)} > ${Utils.bytesToString(maxResultSize)}), " +
              s"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
            // 如果序列化后的结果小于上面的配置，而大于spark.akka.frameSize - 200KB
            // 结果通过BlockManager回传
          } else if (resultSize > maxDirectResultSize) {
            // 结果大小大于设定的阀值，则放入BlockManager中
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId,
              new ChunkedByteBuffer(serializedDirectResult.duplicate()),
              StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(
              s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
            // 返回非直接返回给Driver的对象TaskResultTask
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            // 结果不大，直接传回给Driver
            // 如果结果小于spark.akka.frameSize - 200KB，则可通过AKKA直接返回Task的该执行结果
            logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver")
            serializedDirectResult
          }
        }

        setTaskFinishedAndClearInterruptStatus()

        /**
          * 更新当前Task的状态为finished    //通知Driver Task已完成
          * 调用ExecutorBackend.statusUpdate（） ==》 org.apache.spark.executor.CoarseGrainedExecutorBackend.statusUpdate（）
          */
        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)

      } catch {
        case t: Throwable if hasFetchFailure && !Utils.isFatalError(t) =>
          val reason = task.context.fetchFailed.get.toTaskFailedReason
          if (!t.isInstanceOf[FetchFailedException]) {
            // there was a fetch failure in the task, but some user code wrapped that exception
            // and threw something else.  Regardless, we treat it as a fetch failure.
            val fetchFailedCls = classOf[FetchFailedException].getName
            logWarning(s"TID ${taskId} encountered a ${fetchFailedCls} and " +
              s"failed, but the ${fetchFailedCls} was hidden by another " +
              s"exception.  Spark is handling this like a fetch failure and ignoring the " +
              s"other exception: $t")
          }
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case t: TaskKilledException =>
          logInfo(s"Executor killed $taskName (TID $taskId), reason: ${t.reason}")
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(TaskKilled(t.reason)))

        case _: InterruptedException | NonFatal(_) if
        task != null && task.reasonIfKilled.isDefined =>
          val killReason = task.reasonIfKilled.getOrElse("unknown reason")
          logInfo(s"Executor interrupted and killed $taskName (TID $taskId), reason: $killReason")
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(
            taskId, TaskState.KILLED, ser.serialize(TaskKilled(killReason)))

        case CausedBy(cDE: CommitDeniedException) =>
          val reason = cDE.toTaskFailedReason
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case t: Throwable =>
          // Attempt to exit cleanly by informing the driver of our failure.
          // If anything goes wrong (or this was a fatal exception), we will delegate to
          // the default uncaught exception handler, which will terminate the Executor.
          logError(s"Exception in $taskName (TID $taskId)", t)

          // Collect latest accumulator values to report back to the driver
          val accums: Seq[AccumulatorV2[_, _]] =
            if (task != null) {
              task.metrics.setExecutorRunTime(System.currentTimeMillis() - taskStart)
              task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
              task.collectAccumulatorUpdates(taskFailed = true)
            } else {
              Seq.empty
            }

          val accUpdates = accums.map(acc => acc.toInfo(Some(acc.value), None))

          val serializedTaskEndReason = {
            try {
              ser.serialize(new ExceptionFailure(t, accUpdates).withAccums(accums))
            } catch {
              case _: NotSerializableException =>
                // t is not serializable so just send the stacktrace
                ser.serialize(new ExceptionFailure(t, accUpdates, false).withAccums(accums))
            }
          }
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskEndReason)

          // Don't forcibly exit unless the exception was inherently fatal, to avoid
          // stopping other tasks unnecessarily.
          if (Utils.isFatalError(t)) {
            uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), t)
          }

      } finally {
        // 总runnint状态的task列表中将该task移除 //将Task从运行队列中去除
        runningTasks.remove(taskId)
      }
    }

    private def hasFetchFailure: Boolean = {
      task != null && task.context != null && task.context.fetchFailed.isDefined
    }
  }





























  /**
    * Supervises the killing / cancellation of a task by sending the interrupted flag, optionally
    * sending a Thread.interrupt(), and monitoring the task until it finishes.
    *
    * 监督 杀killing/取消的任务通过发送中断标志，随意发送Thread.interrupt()，而且监测任务，直到它完成。
    *
    * Spark's current task cancellation / task killing mechanism is "best effort" because some tasks
    * may not be interruptable or may not respond to their "killed" flags being set. If a significant
    * fraction of a cluster's task slots are occupied by tasks that have been marked as killed but
    * remain running then this can lead to a situation where new jobs and tasks are starved of
    * resources that are being used by these zombie tasks.
    *
    * Spark的当前任务取消/任务killing 机制是"best effort（尽力而为）”，因为某些任务可能不会中断或不回应他们的“killed”标志被设置。
    * 如果一个集群的任务槽中的一个很大部分被那些被标记为被杀死但仍在运行的任务所占用，那么这些僵尸任务正在使用的资源 将导致一个新的工作
    * 和任务缺乏资源的情况。
    *
    *
    * The TaskReaper was introduced in SPARK-18761 as a mechanism to monitor and clean up zombie
    * tasks. For backwards-compatibility / backportability this component is disabled by default
    * and must be explicitly enabled by setting `spark.task.reaper.enabled=true`.
    *
    * TaskReaper在spark-18761介绍说，它作为一种机制来监控和清理僵尸任务。为了向后兼容性/backportability，这个组件默认是禁用的，
    * 必须通过设置`spark.task.reaper.enabled=true`来启用它。
    *
    *
    * A TaskReaper is created for a particular task when that task is killed / cancelled. Typically
    * a task will have only one TaskReaper, but it's possible for a task to have up to two reapers
    * in case kill is called twice with different values for the `interrupt` parameter.
    *
    * 一个taskreaper是一个特定的任务当任务被创建/取消的时候。一个典型的任务只有一个taskreaper，但有可能一个task有两个reapers
    * 通过调用两次kill的使用不同的 `interrupt`参数
    *
    *
    * Once created, a TaskReaper will run until its supervised task has finished running. If the
    * TaskReaper has not been configured to kill the JVM after a timeout (i.e. if
    * `spark.task.reaper.killTimeout < 0`) then this implies that the TaskReaper may run indefinitely
    * if the supervised task never exits.
    *
    * 一旦创建，taskreaper将运行到其监督的任务完成。如果taskreaper尚未配置杀JVM暂停之后（即如果`spark.task.reaper.killTimeout < 0`）
    * 那么这意味着taskreaper可能如果监督任务不退出运行下去。
    *
    *
    */
  private class TaskReaper(
                            taskRunner: TaskRunner,
                            val interruptThread: Boolean,
                            val reason: String)
    extends Runnable {

    private[this] val taskId: Long = taskRunner.taskId

    private[this] val killPollingIntervalMs: Long =
      conf.getTimeAsMs("spark.task.reaper.pollingInterval", "10s")

    private[this] val killTimeoutMs: Long = conf.getTimeAsMs("spark.task.reaper.killTimeout", "-1")

    private[this] val takeThreadDump: Boolean =
      conf.getBoolean("spark.task.reaper.threadDump", true)

    override def run(): Unit = {
      val startTimeMs = System.currentTimeMillis()
      def elapsedTimeMs = System.currentTimeMillis() - startTimeMs
      def timeoutExceeded(): Boolean = killTimeoutMs > 0 && elapsedTimeMs > killTimeoutMs
      try {
        // Only attempt to kill the task once. If interruptThread = false then a second kill
        // attempt would be a no-op and if interruptThread = true then it may not be safe or
        // effective to interrupt multiple times:

        // 只想一次性杀死任务。如果interruptThread = false 然后第二次kill将会是一个no-op而且如果interruptThread = true
        // 那么它就有可能不是安全的或多次有效的中断：
        taskRunner.kill(interruptThread = interruptThread, reason = reason)
        // Monitor the killed task until it exits. The synchronization logic here is complicated
        // because we don't want to synchronize on the taskRunner while possibly taking a thread
        // dump, but we also need to be careful to avoid races between checking whether the task
        // has finished and wait()ing for it to finish.

        // 监视被杀死的任务，直到它退出。这里的同步逻辑很复杂，因为我们不想对taskrunner而可能采取一个线程转储，
        // 但我们也需要小心避免之间检查任务是否完成，wait()为它完成比赛。
        var finished: Boolean = false
        while (!finished && !timeoutExceeded()) {
          taskRunner.synchronized {
            // We need to synchronize on the TaskRunner while checking whether the task has
            // finished in order to avoid a race where the task is marked as finished right after
            // we check and before we call wait().

            // 我们需要对taskrunner进行同步，在检查任务是否已经完成为了避免种族的任务标记为权后，我们检查完毕，在我们调用wait()。
            if (taskRunner.isFinished) {
              finished = true
            } else {
              taskRunner.wait(killPollingIntervalMs)
            }
          }
          if (taskRunner.isFinished) {
            finished = true
          } else {
            logWarning(s"Killed task $taskId is still running after $elapsedTimeMs ms")
            if (takeThreadDump) {
              try {
                Utils.getThreadDumpForThread(taskRunner.getThreadId).foreach { thread =>
                  if (thread.threadName == taskRunner.threadName) {
                    logWarning(s"Thread dump from task $taskId:\n${thread.stackTrace}")
                  }
                }
              } catch {
                case NonFatal(e) =>
                  logWarning("Exception thrown while obtaining thread dump: ", e)
              }
            }
          }
        }

        if (!taskRunner.isFinished && timeoutExceeded()) {
          if (isLocal) {
            logError(s"Killed task $taskId could not be stopped within $killTimeoutMs ms; " +
              "not killing JVM because we are running in local mode.")
          } else {
            // In non-local-mode, the exception thrown here will bubble up to the uncaught exception
            // handler and cause the executor JVM to exit.
            throw new SparkException(
              s"Killing executor JVM because killed task $taskId could not be stopped within " +
                s"$killTimeoutMs ms.")
          }
        }
      } finally {
        // Clean up entries in the taskReaperForTask map.
        taskReaperForTask.synchronized {
          taskReaperForTask.get(taskId).foreach { taskReaperInMap =>
            if (taskReaperInMap eq this) {
              taskReaperForTask.remove(taskId)
            } else {
              // This must have been a TaskReaper where interruptThread == false where a subsequent
              // killTask() call for the same task had interruptThread == true and overwrote the
              // map entry.
            }
          }
        }
      }
    }
  }

  /**
    * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
    * created by the interpreter to the search path
    *
    * 创建一个用于任务的类装载器，添加任何JARs由用户指定或由解释器创建搜索路径指定的任何类
    *
    * Spark自身ClassLoader的创建
    *   获取要创建的ClassLoader的父类加载器currentLoader，然后根据currentJars生成URL数组，
    *   spark.files.userClassPathFirst属性指定加载类时是否先从用户的classpath下加载，最后创建
    *   ExecutorURLClassLoader或者ChildExecutorURLClassLoader。
    *
    *
    */
  private def createClassLoader(): MutableURLClassLoader = {
    // Bootstrap the list of jars with the user class path.
    // 根据类路径加载jars
    val now = System.currentTimeMillis()
    userClassPath.foreach { url =>
      currentJars(url.getPath().split("/").last) = now
    }

    val currentLoader = Utils.getContextOrSparkClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    // 在jarSet为每个jars设置，将它们添加到类加载器中。我们假设每个文件都已经被获取到。
    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }
    if (userClassPathFirst) {
      new ChildFirstURLClassLoader(urls, currentLoader)
    } else {
      new MutableURLClassLoader(urls, currentLoader)
    }
  }

  /**
    * If the REPL is in use, add another ClassLoader that will read
    * new classes defined by the REPL as the user types code
    *
    * 如果REPL正在使用中，添加一个类加载器会读REPL作为用户定义的类型代码
    *
    */
  private def addReplClassLoaderIfNeeded(parent: ClassLoader): ClassLoader = {
    val classUri = conf.get("spark.repl.class.uri", null)
    if (classUri != null) {
      logInfo("Using REPL class URI: " + classUri)
      try {
        val _userClassPathFirst: java.lang.Boolean = userClassPathFirst
        val klass = Utils.classForName("org.apache.spark.repl.ExecutorClassLoader")
          .asInstanceOf[Class[_ <: ClassLoader]]
        val constructor = klass.getConstructor(classOf[SparkConf], classOf[SparkEnv],
          classOf[String], classOf[ClassLoader], classOf[Boolean])
        constructor.newInstance(conf, env, classUri, parent, _userClassPathFirst)
      } catch {
        case _: ClassNotFoundException =>
          logError("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
          System.exit(1)
          null
      }
    } else {
      parent
    }
  }

  /**
    * Download any missing dependencies if we receive a new set of files and JARs from the
    * SparkContext. Also adds any new JARs we fetched to the class loader.
    *
    * 下载任何缺失的依赖，如果我们从SparkContext收到一套新的文件和JARS。添加所有我们获取的JARS到我们的类加载器中。
    *
    */
  private def updateDependencies(newFiles: Map[String, Long], newJars: Map[String, Long]) {
    lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    synchronized {
      // Fetch missing dependencies    获取缺失的以来
      for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        // Fetch file with useCache mode, close cache for local mode.
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
          env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
        currentFiles(name) = timestamp
      }
      for ((name, timestamp) <- newJars) {
        val localName = new URI(name).getPath.split("/").last
        val currentTimeStamp = currentJars.get(name)
          .orElse(currentJars.get(localName))
          .getOrElse(-1L)
        if (currentTimeStamp < timestamp) {
          logInfo("Fetching " + name + " with timestamp " + timestamp)
          // Fetch file with useCache mode, close cache for local mode.
          Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
            env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
          currentJars(name) = timestamp
          // Add it to our class loader
          val url = new File(SparkFiles.getRootDirectory(), localName).toURI.toURL
          if (!urlClassLoader.getURLs().contains(url)) {
            logInfo("Adding " + url + " to class loader")
            urlClassLoader.addURL(url)
          }
        }
      }
    }
  }

  /** Reports heartbeat and metrics for active tasks to the driver.
    * 活动的tasK向driver报告心跳和度量。
    * */
  private def reportHeartBeat(): Unit = {
    // list of (task id, accumUpdates) to send back to the driver  一个list集合格式为(task id, accumUpdates)这个要发送给driver
    val accumUpdates = new ArrayBuffer[(Long, Seq[AccumulatorV2[_, _]])]()
    // 返回此JVM进程花费在垃圾收集中的总时间。
    val curGCTime = computeTotalGcTime()

    // 循环每个正在运行的任务，把任务id和对应的metrics信息封装起来
    for (taskRunner <- runningTasks.values().asScala) {
      if (taskRunner.task != null) {
        // 将所有临时[[ShuffleReadMetrics]]中的值合并到' _shuffleReadMetrics '中。
        taskRunner.task.metrics.mergeShuffleReadMetrics()
        // GC的差值时间
        taskRunner.task.metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime)
        accumUpdates += ((taskRunner.taskId, taskRunner.task.metrics.accumulators()))
      }
    }

    // 组成一个Heartbeat事件
    val message = Heartbeat(executorId, accumUpdates.toArray, env.blockManager.blockManagerId)
    try {
      // 将HeartbeatResponse事件发送给HeartbeatReceiver的receiveAndReply（）方法处理，并且等待结果
      val response = heartbeatReceiverRef.askSync[HeartbeatResponse](
        message, RpcTimeout(conf, "spark.executor.heartbeatInterval", "10s"))

      //心跳成功
      if (response.reregisterBlockManager) {
        logInfo("Told to re-register on heartbeat")
        env.blockManager.reregister()
      }
      // 心跳成功，重试失败次数清零
      heartbeatFailures = 0
    } catch {
      // 心跳失败，重试失败次数加一
      case NonFatal(e) =>
        logWarning("Issue communicating with driver in heartbeater", e)
        heartbeatFailures += 1
        if (heartbeatFailures >= HEARTBEAT_MAX_FAILURES) {
          logError(s"Exit as unable to send heartbeats to driver " +
            s"more than $HEARTBEAT_MAX_FAILURES times")
          System.exit(ExecutorExitCode.HEARTBEAT_FAILURE)
        }
    }
  }

  /**
    * Schedules a task to report heartbeat and partial metrics for active tasks to driver.
    *
    * 调度一个任务，向驱动程序报告活动任务的心跳和部分度量。
    *
    * Executor心跳线程的间隔由属性spark.executor.heartbeatInterval配置，默认是10s，此外超时时间是30秒，
    * 超时重试测试是3次，重试时间间隔是3000毫秒，使用actorSystem.actorSelection(url)方法查找到匹配的Actor
    * 引用，url是akka.tcp://sparkDZriver@$driverHost:$driverPort/user/heartbeatReceiver，最终创建一个运行
    * 过程中，每次回休眠1 0000-2 0000毫秒的线程。次现场从runningTasks获取最新有关的Task的测量信息，将其与executorId,
    * blockManagerId分装成Heartbeat消息，向HeartbeatReceiver发送Heartbeat消息。
    *
    * 这个心跳线程的作用是什么呢？其作用有两个：
    * 1.更新正在处理的任务的测量信息；
    * 2.通知BlockManagerMaster，此Executor上的BlockManager依然活着。
    *
    */
  private def startDriverHeartbeater(): Unit = {
    val intervalMs = conf.getTimeAsMs("spark.executor.heartbeatInterval", "10s")

    // Wait a random interval so the heartbeats don't end up in sync
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    val heartbeatTask = new Runnable() {
      // reportHeartBeat()方法是活动的tasK向driver报告心跳和度量。
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartBeat())
    }
    // 这是一个定时执行的任务，没10秒执行一次
    heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
  }



}

private[spark] object Executor {
  // This is reserved for internal use by components that need to read task properties before a
  // task is fully deserialized. When possible, the TaskContext.getLocalProperty call should be
  // used instead.

  // 这是内部保留的，需要在一个任务是完全反序列化读取任务属性组件内部使用。
  // 如果可能，TaskContext.getLocalProperty在内部调用。
  val taskDeserializationProps: ThreadLocal[Properties] = new ThreadLocal[Properties]
}
