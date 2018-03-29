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

package org.apache.spark.rdd

import java.util.concurrent.atomic.AtomicInteger

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude, JsonPropertyOrder}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.base.Objects

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

/**
 * A general, named code block representing an operation that instantiates RDDs.
 *
 * All RDDs instantiated in the corresponding code block will store a pointer to this object.
 * Examples include, but will not be limited to, existing RDD operations, such as textFile,
 * reduceByKey, and treeAggregate.
 *
 * An operation scope may be nested in other scopes. For instance, a SQL query may enclose
 * scopes associated with the public RDD APIs it uses under the hood.
 *
 * There is no particular relationship between an operation scope and a stage or a job.
 * A scope may live inside one stage (e.g. map) or span across multiple jobs (e.g. take).
  *
  * 一个通用的、命名的代码块，表示实例化RDDs的操作。
  *
  * 在相应代码块中实例化的所有RDDs将存储指向该对象的指针。示例包括但不限于现有的RDD操作，比如textFile、reduceByKey和treeAggregate。
  *
  * 操作范围可以嵌套在其他范围内。例如，一个SQL查询可以包含与它在hood中使用的公共RDD api相关联的范围。
  *
  * 操作范围和阶段或作业之间没有特定的关系。范围可以在一个阶段(如map)或跨多个作业(例如:take)的范围内。
  *
  *
  * 它是用来做DAG可视化的（DAG visualization on SparkUI）
  *
  * 以前的sparkUI中只有stage的执行情况，也就是说我们不可以看到上个RDD到下个RDD的具体信息。于是为了在
  * sparkUI中能展示更多的信息。所以把所有创建的RDD的方法都包裹起来，同时用RDDOperationScope 记录 RDD 的操作历史和关联，就能达成目标。
  *
  * 参考博客：https://blog.csdn.net/qq_21383435/article/details/79666170
 */
@JsonInclude(Include.NON_NULL)
@JsonPropertyOrder(Array("id", "name", "parent"))
private[spark] class RDDOperationScope(
    val name: String,
    val parent: Option[RDDOperationScope] = None,
    val id: String = RDDOperationScope.nextScopeId().toString) {

  def toJson: String = {
    RDDOperationScope.jsonMapper.writeValueAsString(this)
  }

  /**
   * Return a list of scopes that this scope is a part of, including this scope itself.
   * The result is ordered from the outermost scope (eldest ancestor) to this scope.
    * 返回这个范围是其中一部分的作用域列表，包括这个范围本身。结果从最外层(最年长的祖先)到这个范围。
   */
  @JsonIgnore
  def getAllScopes: Seq[RDDOperationScope] = {
    parent.map(_.getAllScopes).getOrElse(Seq.empty) ++ Seq(this)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case s: RDDOperationScope =>
        id == s.id && name == s.name && parent == s.parent
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(id, name, parent)

  override def toString: String = toJson
}

/**
 * A collection of utility methods to construct a hierarchical representation of RDD scopes.
 * An RDD scope tracks the series of operations that created a given RDD.
  *
  * 一种实用方法的集合，用于构造RDD范围的分层表示。RDD范围跟踪创建给定RDD的一系列操作。
 */
private[spark] object RDDOperationScope extends Logging {
  private val jsonMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val scopeCounter = new AtomicInteger(0)

  def fromJson(s: String): RDDOperationScope = {
    jsonMapper.readValue(s, classOf[RDDOperationScope])
  }

  /** Return a globally unique operation scope ID.
    *
    * //返回一个全局独一无二的scopeID
    * */
  def nextScopeId(): Int = scopeCounter.getAndIncrement

  /**
   * Execute the given body such that all RDDs created in this body will have the same scope.
   * The name of the scope will be the first method name in the stack trace that is not the
   * same as this method's.
   *
   * Note: Return statements are NOT allowed in body.
    *
    * 执行给定的主体，这样在这个主体中创建的所有RDDs将具有相同的范围。作用域的名称将是堆栈跟踪中的第一个方法名，与此方法的名称不同。
    * 注意:返回语句在正文中是不允许的。
   */
  private[spark] def withScope[T](
      sc: SparkContext,
      allowNesting: Boolean = false)(body: => T): T = {
    // 设置跟踪堆的轨迹的scope名字
    val ourMethodName = "withScope"
    val callerMethodName = Thread.currentThread.getStackTrace()
      .dropWhile(_.getMethodName != ourMethodName)
      .find(_.getMethodName != ourMethodName)
      .map(_.getMethodName)
      .getOrElse {
        // Log a warning just in case, but this should almost certainly never happen
        logWarning("No valid method name for this RDD operation scope!")
        "N/A"
      }
    withScope[T](sc, callerMethodName, allowNesting, ignoreParent = false)(body)
  }

  /**
   * Execute the given body such that all RDDs created in this body will have the same scope.
   *
   * If nesting is allowed, any subsequent calls to this method in the given body will instantiate
   * child scopes that are nested within our scope. Otherwise, these calls will take no effect.
   *
   * Additionally, the caller of this method may optionally ignore the configurations and scopes
   * set by the higher level caller. In this case, this method will ignore the parent caller's
   * intention to disallow nesting, and the new scope instantiated will not have a parent. This
   * is useful for scoping physical operations in Spark SQL, for instance.
   *
   * Note: Return statements are NOT allowed in body.
    *
    *
    * 执行给定的主体，这样在这个主体中创建的所有RDDs将具有相同的范围。
    *
    * 如果允许嵌套，那么在给定的主体中对该方法的任何后续调用都会实例化嵌套在我们范围内的子范围。否则，这些调用将不起作用。
    *
    * 此外，该方法的调用者可以选择性地忽略高级调用者设置的配置和范围。在这种情况下，此方法将忽略父调用方不允许嵌套的意图，
    * 并且新范围实例化将不会有父对象。这对于在Spark SQL中进行物理操作是很有用的，例如。
    *
    * 注意:返回语句在正文中是不允许的。
    *
   */
  private[spark] def withScope[T](
      sc: SparkContext,
      name: String,
      allowNesting: Boolean,
      ignoreParent: Boolean)(body: => T): T = {
    // Save the old scope to restore it later
    // 先保存老的scope，之后恢复它
    val scopeKey = SparkContext.RDD_SCOPE_KEY
    val noOverrideKey = SparkContext.RDD_SCOPE_NO_OVERRIDE_KEY
    val oldScopeJson = sc.getLocalProperty(scopeKey)
    val oldScope = Option(oldScopeJson).map(RDDOperationScope.fromJson)
    val oldNoOverride = sc.getLocalProperty(noOverrideKey)
    try {
      if (ignoreParent) {
        // Ignore all parent settings and scopes and start afresh with our own root scope
        // ignoreParent: Boolean：当ignorePatent设置为true的时候，那么回忽略之前的全部设置和scope
        // 从新我们自己的scope
        sc.setLocalProperty(scopeKey, new RDDOperationScope(name).toJson)
      } else if (sc.getLocalProperty(noOverrideKey) == null) {
        // Otherwise, set the scope only if the higher level caller allows us to do so
        sc.setLocalProperty(scopeKey, new RDDOperationScope(name, oldScope).toJson)
      }
      // Optionally disallow the child body to override our scope
      // 可选：不让我们的新的子RDD放入我们的scope中
      if (!allowNesting) {
        sc.setLocalProperty(noOverrideKey, "true")
      }
      body
    } finally {
      // Remember to restore any state that was modified before exiting
      // 把所有的新状态恢复放在一起
      sc.setLocalProperty(scopeKey, oldScopeJson)
      sc.setLocalProperty(noOverrideKey, oldNoOverride)
    }
  }
}
