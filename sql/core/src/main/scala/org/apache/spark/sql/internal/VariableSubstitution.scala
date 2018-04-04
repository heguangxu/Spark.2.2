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

package org.apache.spark.sql.internal

import org.apache.spark.internal.config._

/**
 * A helper class that enables substitution using syntax like
 * `${var}`, `${system:var}` and `${env:var}`.
 *
 * Variable substitution is controlled by `SQLConf.variableSubstituteEnabled`.
 */
class VariableSubstitution(conf: SQLConf) {

  private val provider = new ConfigProvider {
    override def get(key: String): Option[String] = Option(conf.getConfString(key, ""))
  }

  private val reader = new ConfigReader(provider)
    .bind("spark", provider)
    .bind("sparkconf", provider)
    .bind("hivevar", provider)
    .bind("hiveconf", provider)

  /**
   * Given a query, does variable substitution and return the result.
    *
    * 给定一个查询，进行变量替换并返回结果。
   */
  def substitute(input: String): String = {
    // 默认进行变量替换
    if (conf.variableSubstituteEnabled) {
      // 在给定的输入字符串上执行变量替换。那种带可变参数的SQL
      reader.substitute(input)
    } else {
      input
    }
  }
}
