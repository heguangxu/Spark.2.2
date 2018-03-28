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

import java.io.Serializable

import scala.collection.generic.Growable
import scala.reflect.ClassTag

import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{AccumulatorContext, AccumulatorMetadata, LegacyAccumulatorWrapper}


/**
 * A data type that can be accumulated, i.e. has a commutative and associative "add" operation,
 * but where the result type, `R`, may be different from the element type being added, `T`.
  *
  * 可累加的数据类型，i.e. 即具有交换和关联的“add”操作，但是，结果类型“r”可能与所添加的元素类型不同，“t”。
 *
 * You must define how to add data, and how to merge two of these together.  For some data types,
 * such as a counter, these might be the same operation. In that case, you can use the simpler
 * [[org.apache.spark.Accumulator]]. They won't always be the same, though -- e.g., imagine you are
 * accumulating a set. You will add items to the set, and you will union two sets together.
  *
  * 您必须定义如何添加数据，以及如何将两个数据合并在一起。对于某些数据类型，比如计数器，这些操作可能是相同的。
  * 在这种情况下，可以使用更简单的方法[[org.apache.spark.Accumulator]].。但它们并不总是一样的——比如，
  * 想象你正在累加一个set集合。您将向集合中添加项，并将两个集合合并在一起。
 *
 * Operations are not thread-safe.    操作不是线程安全的
 *
 * @param id ID of this accumulator; for internal use only.       此累加器的id；仅用于内部使用。
 * @param initialValue initial value of accumulator               初始化累加器
 * @param param helper object defining how to add elements of type `R` and `T`    帮助对象定义如何添加类型“r”和“T”的元素
 * @param name human-readable name for use in Spark's web UI    用于Spark Web用户界面的可读名称
 * @param countFailedValues whether to accumulate values from failed tasks. This is set to true
 *                          for system and time metrics like serialization time or bytes spilled,
 *                          and false for things with absolute values like number of input rows.
 *                          This should be used for internal metrics only.
  *
  *                          是否从失败的任务中增加累加器的值。对于序列化时间或字节溢出的系统和时间度量来说，
  *                          这是对的，对于绝对值，比如输入行数，这是错误的。
  *
 * @tparam R the full accumulated data (result type)    完整的累积数据（结果类型）
 * @tparam T partial data that can be added in          可以添加的部分数据
 */
@deprecated("use AccumulatorV2", "2.0.0")
class Accumulable[R, T] private (
    val id: Long,
    // SI-8813: This must explicitly be a private val, or else scala 2.11 doesn't compile
    // SI-8813:这里必须明确的标明属性是val类型，否则scala 2.11 将不会编译
    @transient private val initialValue: R,
    param: AccumulableParam[R, T],
    val name: Option[String],
    private[spark] val countFailedValues: Boolean)
  extends Serializable {

  private[spark] def this(
      initialValue: R,
      param: AccumulableParam[R, T],
      name: Option[String],
      countFailedValues: Boolean) = {
    this(AccumulatorContext.newId(), initialValue, param, name, countFailedValues)
  }

  private[spark] def this(initialValue: R, param: AccumulableParam[R, T], name: Option[String]) = {
    this(initialValue, param, name, false /* countFailedValues */)
  }

  def this(initialValue: R, param: AccumulableParam[R, T]) = this(initialValue, param, None)

  val zero = param.zero(initialValue)
  private[spark] val newAcc = new LegacyAccumulatorWrapper(initialValue, param)
  newAcc.metadata = AccumulatorMetadata(id, name, countFailedValues)
  // Register the new accumulator in ctor, to follow the previous behaviour.
  AccumulatorContext.register(newAcc)

  /**
   * Add more data to this accumulator / accumulable    给accumulator / accumulable添加更多的数据
   * @param term the data to add
   */
  def += (term: T) { newAcc.add(term) }

  /**
   * Add more data to this accumulator / accumulable   给accumulator / accumulable添加更多的数据
   * @param term the data to add
   */
  def add(term: T) { newAcc.add(term) }

  /**
   * Merge two accumulable objects together    合并两个累加器对象
   *
   * Normally, a user will not want to use this version, but will instead call `+=`.
    * 正常的，用户将不会使用这个版本，但是会调用`+=`.
   * @param term the other `R` that will get merged with this         另一个' R '将合并与此
   */
  def ++= (term: R) { newAcc._value = param.addInPlace(newAcc._value, term) }

  /**
   * Merge two accumulable objects together   合并两个累加器对象
   *
   * Normally, a user will not want to use this version, but will instead call `add`.
   * @param term the other `R` that will get merged with this
   */
  def merge(term: R) { newAcc._value = param.addInPlace(newAcc._value, term) }

  /**
   * Access the accumulator's current value; only allowed on driver.
    *
    *  访问累加器当前的值，仅仅在driver中可以使用
   */
  def value: R = {
    if (newAcc.isAtDriverSide) {
      newAcc.value
    } else {
      throw new UnsupportedOperationException("Can't read accumulator value in task")
    }
  }

  /**
   * Get the current value of this accumulator from within a task.
    * 从一个task中获取这个累加器的当前值。
   *
   * This is NOT the global value of the accumulator.  To get the global value after a
   * completed operation on the dataset, call `value`.
    *
    * 这不是累加器的全局值。要在数据集上完成操作后获得全局值，请调用“value”。
   *
   * The typical use of this method is to directly mutate the local value, eg., to add
   * an element to a Set.
    *
    * 这种方法的典型用途是直接改变局部值，例如，将元素添加到集合中。
   */
  def localValue: R = newAcc.value

  /**
   * Set the accumulator's value; only allowed on driver.
    *
    * 设置累加器的值，仅仅在driver中允许
   */
  def value_= (newValue: R) {
    if (newAcc.isAtDriverSide) {
      newAcc._value = newValue
    } else {
      throw new UnsupportedOperationException("Can't assign accumulator value in task")
    }
  }

  /**
   * Set the accumulator's value. For internal use only.
    * 设置累加器的值，仅仅在内部中使用
   */
  def setValue(newValue: R): Unit = { newAcc._value = newValue }

  /**
   * Set the accumulator's value. For internal use only.
    * 设置累加器的值，仅仅内部使用
   */
  private[spark] def setValueAny(newValue: Any): Unit = { setValue(newValue.asInstanceOf[R]) }

  /**
   * Create an [[AccumulableInfo]] representation of this [[Accumulable]] with the provided values.
    * 创建一个[ ] [ ]表示accumulableinfo这[ [ ] ]可累积与提供的值。
    */
  private[spark] def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    val isInternal = name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX))
    new AccumulableInfo(id, name, update, value, isInternal, countFailedValues)
  }

  override def toString: String = if (newAcc._value == null) "null" else newAcc._value.toString
}


/**
 * Helper object defining how to accumulate values of a particular type. An implicit
 * AccumulableParam needs to be available when you create [[Accumulable]]s of a specific type.
 *
  *   帮助对象定义如何积累特定类型的值。一个隐含的accumulableparam需要当你创建[ ] [ ]可累积的一种特定类型的。
  *
 * @tparam R the full accumulated data (result type)
 * @tparam T partial data that can be added in
 */
@deprecated("use AccumulatorV2", "2.0.0")
trait AccumulableParam[R, T] extends Serializable {
  /**
   * Add additional data to the accumulator value. Is allowed to modify and return `r`
   * for efficiency (to avoid allocating objects).
    *
    * 添加额外的数据的累加值。允许修改和返回“r”（以避免分配对象）。
   *
   * @param r the current value of the accumulator      累加器当前的值
   * @param t the data to be added to the accumulator   要添加进累加器的值
   * @return the new value of the accumulator           累加器新的值
   */
  def addAccumulator(r: R, t: T): R

  /**
   * Merge two accumulated values together. Is allowed to modify and return the first value
   * for efficiency (to avoid allocating objects).
    *
    * 合并两个累加器的值。允许修改和返回效率的第一个值（避免分配对象）。
   *
   * @param r1 one set of accumulated data
   * @param r2 another set of accumulated data
   * @return both data sets merged together
   */
  def addInPlace(r1: R, r2: R): R

  /**
   * Return the "zero" (identity) value for an accumulator type, given its initial value. For
   * example, if R was a vector of N dimensions, this would return a vector of N zeroes.
    *
    *   返回累加器类型的“zero”（标识）值，给定其初始值。例如，如果R是一个N维向量，这将返回一个N个零向量。
    *
   */
  def zero(initialValue: R): R
}


@deprecated("use AccumulatorV2", "2.0.0")
private[spark] class
GrowableAccumulableParam[R <% Growable[T] with TraversableOnce[T] with Serializable: ClassTag, T]
  extends AccumulableParam[R, T] {

  def addAccumulator(growable: R, elem: T): R = {
    growable += elem
    growable
  }

  def addInPlace(t1: R, t2: R): R = {
    t1 ++= t2
    t1
  }

  def zero(initialValue: R): R = {
    // We need to clone initialValue, but it's hard to specify that R should also be Cloneable.
    // Instead we'll serialize it to a buffer and load it back.
    // 我们需要克隆的初始值，但很难指定R也应该是可复制的。而我们将序列化到一个缓冲和负载回来。
    val ser = new JavaSerializer(new SparkConf(false)).newInstance()
    val copy = ser.deserialize[R](ser.serialize(initialValue))
    copy.clear()   // In case it contained stuff
    copy
  }
}
