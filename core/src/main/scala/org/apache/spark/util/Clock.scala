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

/**
 * An interface to represent clocks, so that they can be mocked out in unit tests.
  * 一个代表时钟的接口，这样它们就可以在单元测试中被模拟出来。
 */
private[spark] trait Clock {
  def getTimeMillis(): Long
  def waitTillTime(targetTime: Long): Long
}

/**
 * A clock backed by the actual time from the OS as reported by the `System` API.
  * 由“系统”API所报告的操作系统的实际时间支持的时钟。
 */
private[spark] class SystemClock extends Clock {

  val minPollTime = 25L

  /**
    * 系统当前时间的毫秒数
   * @return the same time (milliseconds since the epoch)
   *         as is reported by `System.currentTimeMillis()`
   */
  def getTimeMillis(): Long = System.currentTimeMillis()

  /**
    * 线程等待阻塞
   * @param targetTime block until the current time is at least this value 阻塞直到当前时间至少是这个值。
   * @return current system time when wait has completed 当前系统时间等待已完成。
   */
  def waitTillTime(targetTime: Long): Long = {
    var currentTime = 0L
    currentTime = System.currentTimeMillis()

    var waitTime = targetTime - currentTime
    if (waitTime <= 0) {
      return currentTime
    }

    val pollTime = math.max(waitTime / 10.0, minPollTime).toLong

    while (true) {
      currentTime = System.currentTimeMillis()
      waitTime = targetTime - currentTime
      if (waitTime <= 0) {
        return currentTime
      }
      val sleepTime = math.min(waitTime, pollTime)
      Thread.sleep(sleepTime)
    }
    -1
  }
}
