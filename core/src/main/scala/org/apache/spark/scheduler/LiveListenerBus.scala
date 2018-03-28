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

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.util.DynamicVariable

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

/**
 * Asynchronously passes SparkListenerEvents to registered SparkListeners.
 *
 * Until `start()` is called, all posted events are only buffered. Only after this listener bus
 * has started will events be actually propagated to all attached listeners. This listener bus
 * is stopped when `stop()` is called, and it will drop further events after stopping.
  *
  * LiveListenerBus以异步的方式把SparkListenerEvents传递给注册的SparkListeners。
  *
  * 在Start() 方法被调用之前，所有提交的事件仅仅放到缓冲区里。 直到这个Listenerbus启动之后，
  * 这些事件才会被真正的广播到所有注册的监听器中。 当stop方法被调用后，listener bus会停止运行，在此之后
  * ，会丢掉所有到来的事件。
  *
  * LiveListenerBus由以下部分组成：
  *     1.事件阻塞队列：类型为LinkedBlockingQueue[SparkListenerEvent],固定大小是10000；
  *     2.监听器组件：类型为ArrayBuffer[SparkListener],存放各类监听器SparkListener.
  *     3.事件匹配监听器的线程：此Thread不断地拉去LinkedBlockingQueue中的事件，遍历监听器，点用监听器的方法
  *       。任何事件都会在LinkedBlockingQueue中存在一段时间，然后Thread处理了此事件后，会将其清除，因此使用listenerBus
  *       这个名字再射和不过，到站就下车。
  *
  *  Spark 事件体系的中枢是ListenerBus，由该类接受Event并且分发给各个Listener。MetricsSystem 则是一个为了衡量系统
  *  的各种指标的度量系统。Listener可以是MetricsSystem的信息来源之一。他们之间总体是一个互相补充的关系。
  *
  *  在SparkContext中, 首先会创建LiveListenerBus实例,这个类主要功能如下:
  *     保存有消息队列,负责消息的缓存
  *     保存有注册过的listener,负责消息的分发
  *
 */
private[spark] class LiveListenerBus(val sparkContext: SparkContext) extends SparkListenerBus {

  self =>

  import LiveListenerBus._

  // Cap the capacity of the event queue so we get an explicit error (rather than
  // an OOM exception) if it's perpetually being added to more quickly than it's being drained.
  // 限制事件队列的容量，这样我们就得到了一个显式的错误(而不是OOM异常)，
  // 如果它总是被不断地添加到比它正在被消耗的更快的速度。
  // 1.5的版本是private vall EVENT_QUEUE_CAPACITY=10000 现在一直追加到最后，任然是默认值为1000
  private lazy val EVENT_QUEUE_CAPACITY = validateAndGetQueueSize()
  // 事件阻塞队列 一个队列还是懒加载的 消息队列实际上是保存在类liveListenerBus中的:
  private lazy val eventQueue = new LinkedBlockingQueue[SparkListenerEvent](EVENT_QUEUE_CAPACITY)

  // EVENT_QUEUE_CAPACITY=10000 现在一直追加到最后，任然是默认值为1000
  private def validateAndGetQueueSize(): Int = {
    val queueSize = sparkContext.conf.get(LISTENER_BUS_EVENT_QUEUE_SIZE)
    if (queueSize <= 0) {
      throw new SparkException("spark.scheduler.listenerbus.eventqueue.size must be > 0!")
    }
    queueSize
  }

  // Indicate if `start()` is called  是否启动
  private val started = new AtomicBoolean(false)
  // Indicate if `stop()` is called   是否停止
  private val stopped = new AtomicBoolean(false)

  /** A counter for dropped events. It will be reset every time we log it.
    * 一个撤销事件的计数器。每次我们记录它的时候它都会被重置。
    * */
  private val droppedEventsCounter = new AtomicLong(0L)

  /** When `droppedEventsCounter` was logged last time in milliseconds.
    *
    * 上一次“droppedEventsCounter”被记录在毫秒中
    * */
  @volatile private var lastReportTimestamp = 0L

  // Indicate if we are processing some event
  // Guarded by `self`
  // 指示我们是否正在处理由“self”守护的事件
  private var processingEvent = false

  private val logDroppedEvent = new AtomicBoolean(false)

  // A counter that represents the number of events produced and consumed in the queue
  // 表示队列中产生和使用的事件数量的计数器 //这个信号量是为了避免消费者线程空跑
  private val eventLock = new Semaphore(0)



  // TODO:消费者：
  /**
    * 整个思想就是典型的生产者消费者思想.为了保证生产者和消费者对消息队列的并发访问,在每次需要获取消息的时候,
    * 调用eventLock.acquire()来获取信号量, 信号量的值就是当前队列中所含有的事件数量.如果正常获取到事件,
    * 就调用postToAll将事件分发给所有listener, 继续下一次循环. 如果获取到null值, 则有下面两种情况:
    *     整个application正常结束, 此时stopped值已经被设置为true
    *     系统发生了错误, 立即终止运行
    *
    *
    *  这个方法就是消费者，每次从队列首拿出来一个元素，然后，事件发布到所有已注册的侦听器。
    *  你是什么类型的事件，就由什么侦听器去处理
    * */
  private val listenerThread = new Thread(name) {
    setDaemon(true) //线程本身设为守护线程 （thread.setDaemon(true)必须在thread.start()之前设置，否则会跑出一个IllegalThreadStateException异常。）
    // tryOrStopSparkContext把一个代码块当做执行的单元，如果有任何未捕获的异常，那么就停止SparkContext
    override def run(): Unit = Utils.tryOrStopSparkContext(sparkContext) {
      LiveListenerBus.withinListenerThread.withValue(true) {
        while (true) {
          // 只要为真，那么就一直接受新的event
          eventLock.acquire()
          self.synchronized {
            processingEvent = true
          }
          try {
            val event = eventQueue.poll  // remove() 和 poll() 方法都是从队列中删除第一个元素（head）。
            if (event == null) {
              // Get out of the while loop and shutdown the daemon thread
              // 跳出while循环，关闭守护进程线程
              if (!stopped.get) {
                throw new IllegalStateException("Polling `null` from eventQueue means" +
                  " the listener bus has been stopped. So `stopped` must be true")
              }
              return
            }
            // 调用ListenerBus的postToAll(event: E)方法
            postToAll(event)
          } finally {
            self.synchronized {
              processingEvent = false
            }
          }
        }
      }
    }
  }

  /**
   * Start sending events to attached listeners.
    * 开始发送事件给附加的监听器。
   *
   * This first sends out all buffered events posted before this listener bus has started, then
   * listens for any additional events asynchronously while the listener bus is still running.
   * This should only be called once.
    *
    * 在此侦听器总线启动之前，它首先发出所有的缓冲事件，然后在侦听器总线仍在运行的情况下，异步侦听任何其他事件。这只需要调用一次。
   *
   */
  def start(): Unit = {
    // CAS有3个操作数，内存值V，旧的预期值A，要修改的新值B。当且仅当预期值A和内存值V相同时，将内存值V修改为B，否则什么都不做
    if (started.compareAndSet(false, true)) { // 这一点涉及并发编程，反正这一点是循环设置为true
      listenerThread.start() //启动消费者线程
    } else {
      throw new IllegalStateException(s"$name already started!")
    }
  }



  // TODO:生产者：
  /**
    * 产生很多SparkListenerEvent的事件，比如SparkListenerStageSubmitted，SparkListenerStageCompleted，SparkListenerTaskStart等等
    * @param event
    */
  def post(event: SparkListenerEvent): Unit = {
    if (stopped.get) {
      // Drop further events to make `listenerThread` exit ASAP
      logError(s"$name has already stopped! Dropping event $event")
      return
    }
    // 在事件队列队尾添加事件
    // add（）和offer（）区别：两者都是往队列尾部插入元素，不同的时候，当超出队列界限的时候，add（）方法是抛出异常让你处理，而offer（）方法是直接返回false
    val eventAdded = eventQueue.offer(event)
    if (eventAdded) {
      //如果成功加入队列，则在信号量中加一，与之对应，消费者线程就可以消费这个元素
      /** 当成功放入一个事件到事件队列时，就调用eventLock.release()，信号量值加1，listener线程取事件
        * 之前通过eventLock.acquire()，阻塞直到有一个信号（信号量大于0），然后信号量减1，取出一个事件
        * 处理。另外，LiveListenerBus的stop函数里面也调用了eventLock.release()，意思就是通过让
        * listener线程最后可以多取一个空事件，通过对空事件的处理
        * */
      eventLock.release()
    } else {
      // 如果事件队列超过其容量，则将删除新的事件，这些子类将被通知到删除事件。（
      // （直接丢弃肯定不行啊，不然程序都连不在一起了，所以还要通知子类），但是貌似没看到通知子类的代码
      onDropEvent(event)
      droppedEventsCounter.incrementAndGet()
    }

    val droppedEvents = droppedEventsCounter.get
    if (droppedEvents > 0) {
      // Don't log too frequently   日志不要太频繁
      // 如果上一次，队列满了EVENT_QUEUE_CAPACITY=1000设置的值，就丢掉，然后记录一个时间，如果一直持续丢掉，那么每过60秒记录一次日志，不然日志会爆满的
      if (System.currentTimeMillis() - lastReportTimestamp >= 60 * 1000) {
        // There may be multiple threads trying to decrease droppedEventsCounter.
        // Use "compareAndSet" to make sure only one thread can win.
        // And if another thread is increasing droppedEventsCounter, "compareAndSet" will fail and
        // then that thread will update it.
        // 可能有多个线程试图减少droppedEventsCounter。
        // 使用“compareAndSet”，确保只有一个线程能够获
        // 如果另一个线程正在增加droppedEventsCounter，“compareAndSet”将会失败
        // 然后线程会更新它。
        if (droppedEventsCounter.compareAndSet(droppedEvents, 0)) {
          val prevLastReportTimestamp = lastReportTimestamp
          lastReportTimestamp = System.currentTimeMillis()
          // 记录一个warn日志，表示这个事件，被丢弃了
          logWarning(s"Dropped $droppedEvents SparkListenerEvents since " +
            new java.util.Date(prevLastReportTimestamp))
        }
      }
    }
  }

  /**
   * For testing only. Wait until there are no more events in the queue, or until the specified
   * time has elapsed. Throw `TimeoutException` if the specified time elapsed before the queue
   * emptied.
   * Exposed for testing.
    *
    * 仅供测试。等待直到队列中没有更多事件，或者直到指定的时间过去为止。如果在队列清空之前经过指定的时间，则抛出“TimeoutException”。
    * 暴露进行测试。
   */
  @throws(classOf[TimeoutException])
  def waitUntilEmpty(timeoutMillis: Long): Unit = {
    val finishTime = System.currentTimeMillis + timeoutMillis
    // 队列不为空的时候，才抛出异常
    while (!queueIsEmpty) {
      if (System.currentTimeMillis > finishTime) {
        throw new TimeoutException(
          s"The event queue is not empty after $timeoutMillis milliseconds")
      }
      /* Sleep rather than using wait/notify, because this is used only for testing and
       * wait/notify add overhead in the general case. */
      Thread.sleep(10)
    }
  }

  /**
   * For testing only. Return whether the listener daemon thread is still alive.
   * Exposed for testing.
    *
    * 仅供测试。返回侦听器守护线程是否还活着。
   */
  def listenerThreadIsAlive: Boolean = listenerThread.isAlive

  /**
   * Return whether the event queue is empty.
    *
    * 返回事件队列是否为空
   *
   * The use of synchronized here guarantees that all events that once belonged to this queue
   * have already been processed by all attached listeners, if this returns true.
    *
    * 在这里使用synchronized保证了曾经属于这个队列的所有事件都已经被所有附加的侦听器处理过，如果它返回true。
   */
  private def queueIsEmpty: Boolean = synchronized { eventQueue.isEmpty && !processingEvent }

  /**
   * Stop the listener bus. It will wait until the queued events have been processed, but drop the
   * new events after stopping.
    *
    * 停止监听总线。它将等待队列事件被处理，但在停止后删除新的事件。
   */
  def stop(): Unit = {
    if (!started.get()) {
      throw new IllegalStateException(s"Attempted to stop $name that has not yet started!")
    }
    if (stopped.compareAndSet(false, true)) {
      // Call eventLock.release() so that listenerThread will poll `null` from `eventQueue` and know
      // `stop` is called.
      eventLock.release()
      //然后等待消费者线程自动关闭
      listenerThread.join()
    } else {
      // Keep quiet
    }
  }

  /**
   * If the event queue exceeds its capacity, the new events will be dropped. The subclasses will be
   * notified with the dropped events.
    *
    * 如果事件队列超过其容量，则将删除新的事件。这些子类将被通知到删除事件。但是没看到哪里通知子类的代码
   *
   * Note: `onDropEvent` can be called in any thread.  这个方法可以再任何线程中调用
   */
  def onDropEvent(event: SparkListenerEvent): Unit = {
    if (logDroppedEvent.compareAndSet(false, true)) {
      // Only log the following message once to avoid duplicated annoying logs.
      logError("Dropping SparkListenerEvent because no remaining room in event queue. " +
        "This likely means one of the SparkListeners is too slow and cannot keep up with " +
        "the rate at which tasks are being started by the scheduler.")
    }
  }
}

private[spark] object LiveListenerBus {
  // Allows for Context to check whether stop() call is made within listener thread
  // 允许Context来检查侦听器线程是否发出stop()调用
  val withinListenerThread: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false) // 默认为false

  /** The thread name of Spark listener bus
    * Spark侦听器总线的线程名称
    * */
  val name = "SparkListenerBus"
}

