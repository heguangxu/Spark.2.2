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

package org.apache.spark.broadcast

import java.io.Serializable

import scala.reflect.ClassTag

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
  * A broadcast variable. Broadcast variables allow the programmer to keep a read-only variable
  * cached on each machine rather than shipping a copy of it with tasks. They can be used, for
  * example, to give every node a copy of a large input dataset in an efficient manner. Spark also
  * attempts to distribute broadcast variables using efficient broadcast algorithms to reduce
  * communication cost.
  *
  * 广播变量。广播变量允许我们在每台机器缓存一个只读变量，而不是使用tasks去拷贝它。它们可以被使用。例如，
  * 给每个节点拷贝一个大的输入数据集是非常有效的方式。spark允许一高校的广播方式分发广播变量以减少通讯的花销。
  *
  *
  * Broadcast variables are created from a variable `v` by calling
  * [[org.apache.spark.SparkContext#broadcast]].
  * The broadcast variable is a wrapper around `v`, and its value can be accessed by calling the
  * `value` method. The interpreter session below shows this:
  *
  * 广播变量创建一个变量` V `通过调用[[org.apache.spark.SparkContext#broadcast]]。广播变量是围绕“V”的包装器，
  * 它的值可以通过调用`value`方法来访问。下面的解释器会话显示了这一点：
  *
  *
  * {{{
  * scala> val broadcastVar = sc.broadcast(Array(1, 2, 3))
  * broadcastVar: org.apache.spark.broadcast.Broadcast[Array[Int]] = Broadcast(0)
  *
  * scala> broadcastVar.value
  * res0: Array[Int] = Array(1, 2, 3)
  * }}}
  *
  * After the broadcast variable is created, it should be used instead of the value `v` in any
  * functions run on the cluster so that `v` is not shipped to the nodes more than once.
  * In addition, the object `v` should not be modified after it is broadcast in order to ensure
  * that all nodes get the same value of the broadcast variable (e.g. if the variable is shipped
  * to a new node later).
  *
  * 在创建广播变量之后，应该使用它在集群上运行的任何函数中的值“V”，这样“V”不会不止一次地发送到节点。
  * 此外，对象“V”在广播之后不应该被修改，以确保所有节点获得相同的广播变量值（例如，如果变量稍后被运送到新节点）。
  *
  *
  * @param id A unique identifier for the broadcast variable.        为广播变的唯一标识符。
  * @tparam T Type of the data contained in the broadcast variable.  包含在广播中变量的数据类型。
  */
abstract class Broadcast[T: ClassTag](val id: Long) extends Serializable with Logging {

  /**
    * Flag signifying whether the broadcast variable is valid
    * (that is, not already destroyed) or not.
    * 象征着广播变量是否有效的标志(that is, not already destroyed) or not.
    */
  @volatile private var _isValid = true

  private var _destroySite = ""

  /** Get the broadcasted value.
    * 得到广播变量的所有值
    * */
  def value: T = {
    assertValid()
    getValue()
  }

  /**
    * Asynchronously delete cached copies of this broadcast on the executors.
    * If the broadcast is used after this is called, it will need to be re-sent to each executor.
    *
    * 在执行器上异步删除此广播的缓存副本。如果调用后又使用这个广播变量，则需要重新发送给每个执行器。
    */
  def unpersist() {
    unpersist(blocking = false)
  }

  /**
    * Delete cached copies of this broadcast on the executors. If the broadcast is used after
    * this is called, it will need to be re-sent to each executor.
    * 在执行器上异步删除此广播的缓存副本。如果调用后又使用这个广播变量，则需要重新发送给每个执行器。
    * @param blocking Whether to block until unpersisting has completed
    *                 是否要阻塞块直到unpersisting完成
    */
  def unpersist(blocking: Boolean) {
    assertValid()
    doUnpersist(blocking)
  }


  /**
    * Destroy all data and metadata related to this broadcast variable. Use this with caution;
    * once a broadcast variable has been destroyed, it cannot be used again.
    * This method blocks until destroy has completed
    *
    * 破坏所有的数据，这个广播变量相关的元数据。谨慎使用；一旦广播变量被破坏，就不能再次使用它。此方法阻塞直到销毁完成为止。
    */
  def destroy() {
    destroy(blocking = true)
  }

  /**
    * Destroy all data and metadata related to this broadcast variable. Use this with caution;
    * once a broadcast variable has been destroyed, it cannot be used again.
    * 破坏所有的数据，这个广播变量相关的元数据。谨慎使用；一旦广播变量被破坏，就不能再次使用它。
    * @param blocking Whether to block until destroy has completed
    *                 是否阻塞直到销毁完成为止。
    */
  private[spark] def destroy(blocking: Boolean) {
    assertValid()
    _isValid = false
    _destroySite = Utils.getCallSite().shortForm
    logInfo("Destroying %s (from %s)".format(toString, _destroySite))
    doDestroy(blocking)
  }

  /**
    * Whether this Broadcast is actually usable. This should be false once persisted state is
    * removed from the driver.
    *
    * 这是否是真的可用广播。一旦持久化后的state从驱动driver上移除的时候，这里应该是false
    */
  private[spark] def isValid: Boolean = {
    _isValid
  }

  /**
    * Actually get the broadcasted value. Concrete implementations of Broadcast class must
    * define their own way to get the value.
    *
    * 实际获得广播变量的值。广播类的具体实现必须定义自己的方式来获得广播变量的值。
    */
  protected def getValue(): T

  /**
    * Actually unpersist the broadcasted value on the executors. Concrete implementations of
    * Broadcast class must define their own logic to unpersist their own data.
    *
    * 在executors执行者上取消持久化的广播变量的值，广播类的具体实现都必须定义自己的逻辑unpersist取消持久化自己的数据的方法。
    */
  protected def doUnpersist(blocking: Boolean)

  /**
    * Actually destroy all data and metadata related to this broadcast variable.
    * Implementation of Broadcast class must define their own logic to destroy their own
    * state.
    *
    * 实际上，销毁与此广播变量相关的所有数据和元数据。广播类的实现必须定义自己的逻辑来摧毁自己的状态。
    *
    */
  protected def doDestroy(blocking: Boolean)

  /** Check if this broadcast is valid. If not valid, exception is thrown.
    * 检查此广播是否有效。如果无效，则引发异常。
    * */
  protected def assertValid() {
    if (!_isValid) {
      throw new SparkException(
        "Attempted to use %s after it was destroyed (%s) ".format(toString, _destroySite))
    }
  }

  override def toString: String = "Broadcast(" + id + ")"
}
