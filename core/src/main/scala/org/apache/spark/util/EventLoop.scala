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

package org.apache.spark.util

import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

/**
 * An event loop to receive events from the caller and process all events in the event thread. It
 * will start an exclusive event thread to process all events.
  *
  * 事件循环从调用者接收事件并处理事件线程中的所有事件。它将启动一个单独的事件线程来处理所有事件。
 *
 * Note: The event queue will grow indefinitely. So subclasses should make sure `onReceive` can
 * handle events in time to avoid the potential OOM.
  *
  * 注意:事件队列将无限增长。因此子类应该确保“onReceive”能够及时处理事件，以避免潜在的OOM。
 */
private[spark] abstract class EventLoop[E](name: String) extends Logging {

  private val eventQueue: BlockingQueue[E] = new LinkedBlockingDeque[E]() // 事件队列

  private val stopped = new AtomicBoolean(false)

  private val eventThread = new Thread(name) {
    // 设置为守护进程
    setDaemon(true)

    override def run(): Unit = {
      try {
        while (!stopped.get) {
          // 检索并删除该队列的头部，如果需要，则等待元素变为可用。(就是拿出队列的第一个元素，然后删除)
          val event = eventQueue.take()
          try {
            // 这里调用这个方法，但是EventLoop是一个抽象方法，谁继承这个类，就调用子类的这个方法，也就是
            // DAGSchedulerEventProcessLoop的  onReceive()方法，而且是循环调用，不停地在一个线程里调用
            onReceive(event)
          } catch {
            case NonFatal(e) =>
              try {
                onError(e)
              } catch {
                case NonFatal(e) => logError("Unexpected error in " + name, e)
              }
          }
        }
      } catch {
        case ie: InterruptedException => // exit even if eventQueue is not empty
        case NonFatal(e) => logError("Unexpected error in " + name, e)
      }
    }

  }

  def start(): Unit = {
    if (stopped.get) {
      throw new IllegalStateException(name + " has already been stopped")
    }
    // Call onStart before starting the event thread to make sure it happens before onReceive
    onStart()
    eventThread.start()
  }

  def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      eventThread.interrupt()
      var onStopCalled = false
      try {
        eventThread.join()
        // Call onStop after the event thread exits to make sure onReceive happens before onStop
        onStopCalled = true
        onStop()
      } catch {
        case ie: InterruptedException =>
          Thread.currentThread().interrupt()
          if (!onStopCalled) {
            // ie is thrown from `eventThread.join()`. Otherwise, we should not call `onStop` since
            // it's already called.
            onStop()
          }
      }
    } else {
      // Keep quiet to allow calling `stop` multiple times.
    }
  }

  /**
   * Put the event into the event queue. The event thread will process it later.
    * 将事件放入事件队列中。事件线程稍后将处理它。
   */
  def post(event: E): Unit = {
    // 将JobSubmitted，Job提交事件存入该队列中
    eventQueue.put(event)
  }

  /**
   * Return if the event thread has already been started but not yet stopped.
   */
  def isActive: Boolean = eventThread.isAlive

  /**
   * Invoked when `start()` is called but before the event thread starts.
   */
  protected def onStart(): Unit = {}

  /**
   * Invoked when `stop()` is called and the event thread exits.
   */
  protected def onStop(): Unit = {}

  /**
   * Invoked in the event thread when polling events from the event queue.
    * 当事件队列中的轮询事件时在事件线程中调用。
   *
   * Note: Should avoid calling blocking actions in `onReceive`, or the event thread will be blocked
   * and cannot process events in time. If you want to call some blocking actions, run them in
   * another thread.
    *
    * 注意:应避免在“onReceive”中调用阻塞动作，否则事件线程将被阻塞，无法及时处理事件。如果您想调用一些阻塞动作，在另一个线程中运行它们。
   */
  protected def onReceive(event: E): Unit

  /**
   * Invoked if `onReceive` throws any non fatal error. Any non fatal error thrown from `onError`
   * will be ignored.
   */
  protected def onError(e: Throwable): Unit

}
