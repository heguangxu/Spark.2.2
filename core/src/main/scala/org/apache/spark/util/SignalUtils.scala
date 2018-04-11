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

import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.commons.lang3.SystemUtils
import org.slf4j.Logger
import sun.misc.{Signal, SignalHandler}

import org.apache.spark.internal.Logging

/**
 * Contains utilities for working with posix signals.
 */
private[spark] object SignalUtils extends Logging {

  /** A flag to make sure we only register the logger once.
    * 一个标志，确保我们只登记一次记录器。
    * */
  private var loggerRegistered = false

  /** Register a signal handler to log signals on UNIX-like systems.
    * 在类unix系统上注册一个信号处理程序来记录信号。
    * */
  def registerLogger(log: Logger): Unit = synchronized {
    if (!loggerRegistered) {
      Seq("TERM", "HUP", "INT").foreach { sig =>
        SignalUtils.register(sig) {
          log.error("RECEIVED SIGNAL " + sig)
          false
        }
      }
      loggerRegistered = true
    }
  }

  /**
   * Adds an action to be run when a given signal is received by this process.
    * 当这个过程接收到给定的信号时，添加一个操作。
   *
   * Note that signals are only supported on unix-like operating systems and work on a best-effort
   * basis: if a signal is not available or cannot be intercepted, only a warning is emitted.
    *
    * 请注意，信号仅在类unix操作系统上支持，并在最佳工作基础上工作:如果一个信号不可用或不能被截获，那么只会发出警告。
   *
   * All actions for a given signal are run in a separate thread.
    *
    * 给定信号的所有操作都在一个单独的线程中运行。
   */
  def register(signal: String)(action: => Boolean): Unit = synchronized {
    // 判断系统是不是linux或者Unix系统
    if (SystemUtils.IS_OS_UNIX) {
      try {
        // handlers:从信号映射到它们各自的处理程序。这就是一个可变的HashMap
        val handler = handlers.getOrElseUpdate(signal, {
          logInfo("Registered signal handler for " + signal)
          // ActionHandler: 用于运行一个操作集合的给定信号的处理程序。
          new ActionHandler(new Signal(signal))
        })
        handler.register(action)
      } catch {
        case ex: Exception => logWarning(s"Failed to register signal handler for " + signal, ex)
      }
    }
  }

  /**
   * A handler for the given signal that runs a collection of actions.
    *
    * 用于运行一个操作集合的给定信号的处理程序。
   */
  private class ActionHandler(signal: Signal) extends SignalHandler {

    /**
     * List of actions upon the signal; the callbacks should return true if the signal is "handled",
     * i.e. should not escalate to the next callback.
      *
      * 在信号上的actions列表;如果信号是“处理”的，回调应该返回true，即不应该升级到下一个回调。
     */
    private val actions = Collections.synchronizedList(new java.util.LinkedList[() => Boolean])

    // original signal handler, before this handler was attached
    // 原始信号处理程序，在此处理器被连接之前。
    private val prevHandler: SignalHandler = Signal.handle(signal, this)

    /**
     * Called when this handler's signal is received. Note that if the same signal is received
     * before this method returns, it is escalated to the previous handler.
      *
      * 当接收到该处理程序的信号时调用。注意，如果在此方法返回之前收到相同的信号，则会将其升级到之前的处理程序。
     */
    override def handle(sig: Signal): Unit = {
      // register old handler, will receive incoming signals while this handler is running
      // 注册旧的处理程序，当这个处理程序运行时将接收传入的信号。
      Signal.handle(signal, prevHandler)

      // Run all actions, escalate to parent handler if no action catches the signal
      // (i.e. all actions return false). Note that calling `map` is to ensure that
      // all actions are run, `forall` is short-circuited and will stop evaluating
      // after reaching a first false predicate.

      // 如果没有动作捕捉到信号(即所有操作返回false)，运行所有操作，升级到父处理程序。注意，
      // 调用“map”是为了确保所有操作都是运行的，“forall”是短路的，在到达第一个错误谓词后将停止计算。
      val escalate = actions.asScala.map(action => action()).forall(_ == false)
      if (escalate) {
        prevHandler.handle(sig)
      }

      // re-register this handler
      Signal.handle(signal, this)
    }

    /**
     * Adds an action to be run by this handler.
     * @param action An action to be run when a signal is received. Return true if the signal
     *               should be stopped with this handler, false if it should be escalated.
     */
    def register(action: => Boolean): Unit = actions.add(() => action)
  }

  /** Mapping from signal to their respective handlers.
    * 从信号映射到它们各自的处理程序。这就是一个可变的HashMap
    * */
  private val handlers = new scala.collection.mutable.HashMap[String, ActionHandler]
}
