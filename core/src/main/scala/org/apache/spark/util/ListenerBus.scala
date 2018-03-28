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

import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

/**
 * An event bus which posts events to its listeners.
  * 事件总线将事件发布到它的监听器。
  *
 */
private[spark] trait ListenerBus[L <: AnyRef, E] extends Logging {

  // Marked `private[spark]` for access in tests.
  /**
    * listener链表保存在ListenerBus类中,为了保证并发访问的安全性,此处采用Java的CopyOnWriteArrayList
    * 类来存储listener. 当需要对listener链表进行更改时,CopyOnWriteArrayList的特性使得会先复制整个链表,\
    * 然后在复制的链表上面进行修改.当一旦获得链表的迭代器,在迭代器的生命周期中,可以保证数据的一致性.
    * */
  private[spark] val listeners = new CopyOnWriteArrayList[L]

  /**
   * Add a listener to listen events. This method is thread-safe and can be called in any thread.
    * 添加一个监听器来监听事件。这种方法是线程安全的，可以在任何线程中调用。
   */
  final def addListener(listener: L): Unit = {
    listeners.add(listener)
  }

  /**
   * Remove a listener and it won't receive any events. This method is thread-safe and can be called
   * in any thread.
    * 删除一个监听器，它不会收到任何事件。这种方法是线程安全的，可以在任何线程中调用。
   */
  final def removeListener(listener: L): Unit = {
    listeners.remove(listener)
  }

  /**
   * Post the event to all registered listeners. The `postToAll` caller should guarantee calling
   * `postToAll` in the same thread for all events.
    *
    * 将事件发布到所有已注册的侦听器。“postToAll”调用者应该保证在所有事件的同一线程中调用“postToAll”。
   */
  def postToAll(event: E): Unit = {
    // JavaConverters can create a JIterableWrapper if we use asScala.
    // However, this method will be called frequently. To avoid the wrapper cost, here we use
    // Java Iterator directly.
    val iter = listeners.iterator   //得到遍历所有添加到ListenerBus中的侦听器，比如SparkListener等等
    while (iter.hasNext) {
      val listener = iter.next()
      try {
        // 每次遍历一个监听器，就把监听器的类型和事件发过去，这一点是无差别攻击，只要是监听器，我就发给你，管你是不是你的，都发给你
        // 然后由每个监听器去鉴定是不是我的啊，是的话就处理，不是的话就扔到，
        // （就像老师给张三布置作业，老师站在讲台上说，张三你写篇作文，然后全班都听到了，但是只有张三去写作文）
        doPostEvent(listener, event)
      } catch {
        case NonFatal(e) =>
          logError(s"Listener ${Utils.getFormattedClassName(listener)} threw an exception", e)
      }
    }
  }

  /**
   * Post an event to the specified listener. `onPostEvent` is guaranteed to be called in the same
   * thread for all listeners.
    *
    * 将事件发送到指定的侦听器。“onPostEvent”保证在同一个线程中为所有侦听器调用。
    *
    * 只要继承这个ListenerBus这个特质的，都要用这个方法去处理
   */
  protected def doPostEvent(listener: L, event: E): Unit

  private[spark] def findListenersByClass[T <: L : ClassTag](): Seq[T] = {
    val c = implicitly[ClassTag[T]].runtimeClass
    listeners.asScala.filter(_.getClass == c).map(_.asInstanceOf[T]).toSeq
  }

}
