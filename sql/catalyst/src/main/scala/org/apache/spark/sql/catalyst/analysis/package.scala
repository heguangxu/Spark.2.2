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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.trees.TreeNode

/**
 * Provides a logical query plan [[Analyzer]] and supporting classes for performing analysis.
 * Analysis consists of translating [[UnresolvedAttribute]]s and [[UnresolvedRelation]]s
 * into fully typed objects using information in a schema [[Catalog]].
  *
  * 提供一个逻辑查询计划[[Analyzer]]和支持类进行分析。分析包括将[[UnresolvedAttribute]]
  * [[UnresolvedAttribute]]和[[unresolvedconnection] [[UnresolvedRelation]]转换为在模式
  * [[目录]]中使用信息的完全类型化对象。
 */
package object analysis {

  /**
   * Resolver should return true if the first string refers to the same entity as the second string.
   * For example, by using case insensitive equality.
    *
    * 如果第一个字符串引用与第二个字符串相同的实体，则解析器应该返回true。例如，通过用例不敏感的等式。
   */
  type Resolver = (String, String) => Boolean

  val caseInsensitiveResolution = (a: String, b: String) => a.equalsIgnoreCase(b)
  val caseSensitiveResolution = (a: String, b: String) => a == b

  implicit class AnalysisErrorAt(t: TreeNode[_]) {
    /** Fails the analysis at the point where a specific tree node was parsed.
      * 在解析特定的树节点时失败。
      * */
    def failAnalysis(msg: String): Nothing = {
      throw new AnalysisException(msg, t.origin.line, t.origin.startPosition)
    }
  }

  /** Catches any AnalysisExceptions thrown by `f` and attaches `t`'s position if any.
    * 捕获任何被“f”抛出的分析结果，并附加“t”的位置。
    * */
  def withPosition[A](t: TreeNode[_])(f: => A): A = {
    try f catch {
      case a: AnalysisException =>
        throw a.withPosition(t.origin.line, t.origin.startPosition)
    }
  }
}
