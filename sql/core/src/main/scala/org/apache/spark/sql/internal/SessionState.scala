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

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.streaming.StreamingQueryManager
import org.apache.spark.sql.util.{ExecutionListenerManager, QueryExecutionListener}

/**
 * A class that holds all session-specific state in a given [[SparkSession]].
  *
  * 在给定的SparkSession中保存所有会话特定状态的类。
 *
 * @param sharedState The state shared across sessions, e.g. global view manager, external catalog.   在会话中共享状态，例如全局视图管理器、外部目录。
 * @param conf SQL-specific key-value configurations.                                                 sql特定键值key-value配置。
 * @param experimentalMethods Interface to add custom planning strategies and optimizers.             添加自定义规划策略和优化器的接口。
 * @param functionRegistry Internal catalog for managing functions registered by the user.            管理用户注册的函数的内部目录。
 * @param udfRegistration Interface exposed to the user for registering user-defined functions.       面向用户公开用户定义函数的接口。
 * @param catalog Internal catalog for managing table and database states.                            管理表和数据库状态的内部目录。
 * @param sqlParser Parser that extracts expressions, plans, table identifiers etc. from SQL texts.   从SQL文本中提取表达式、计划、表标识符等的解析器。
 * @param analyzer Logical query plan analyzer for resolving unresolved attributes and relations.     逻辑查询计划分析器来解决未解决的属性和关系。
 * @param optimizer Logical query plan optimizer.                                                     逻辑查询优化器计划。
 * @param planner Planner that converts optimized logical plans to physical plans.                    将优化的逻辑计划转换成物理计划的计划者。
 * @param streamingQueryManager Interface to start and stop streaming queries.                        启动和停止流查询的接口。
 * @param listenerManager Interface to register custom [[QueryExecutionListener]]s.                   用于注册自定义[[QueryExecutionListener]]的接口。
 * @param resourceLoader Session shared resource loader to load JARs, files, etc.                     会话共享资源加载器加载jar、文件等。
 * @param createQueryExecution Function used to create QueryExecution objects.                        用于创建QueryExecution对象的函数。
 * @param createClone Function used to create clones of the session state.                            用于创建会话状态的克隆的函数
 */
private[sql] class SessionState(
    sharedState: SharedState,
    val conf: SQLConf,
    val experimentalMethods: ExperimentalMethods,
    val functionRegistry: FunctionRegistry,
    val udfRegistration: UDFRegistration,
    val catalog: SessionCatalog,
    val sqlParser: ParserInterface,
    val analyzer: Analyzer,
    val optimizer: Optimizer,
    val planner: SparkPlanner,
    val streamingQueryManager: StreamingQueryManager,
    val listenerManager: ExecutionListenerManager,
    val resourceLoader: SessionResourceLoader,
    createQueryExecution: LogicalPlan => QueryExecution,
    createClone: (SparkSession, SessionState) => SessionState) {

  def newHadoopConf(): Configuration = SessionState.newHadoopConf(
    sharedState.sparkContext.hadoopConfiguration,
    conf)

  def newHadoopConfWithOptions(options: Map[String, String]): Configuration = {
    val hadoopConf = newHadoopConf()
    options.foreach { case (k, v) =>
      if ((v ne null) && k != "path" && k != "paths") {
        hadoopConf.set(k, v)
      }
    }
    hadoopConf
  }

  /**
   * Get an identical copy of the `SessionState` and associate it with the given `SparkSession`
   */
  def clone(newSparkSession: SparkSession): SessionState = createClone(newSparkSession, this)

  // ------------------------------------------------------
  //  Helper methods, partially leftover from pre-2.0 days
  // ------------------------------------------------------

  def executePlan(plan: LogicalPlan): QueryExecution = createQueryExecution(plan)

  def refreshTable(tableName: String): Unit = {
    catalog.refreshTable(sqlParser.parseTableIdentifier(tableName))
  }
}

private[sql] object SessionState {
  def newHadoopConf(hadoopConf: Configuration, sqlConf: SQLConf): Configuration = {
    val newHadoopConf = new Configuration(hadoopConf)
    sqlConf.getAllConfs.foreach { case (k, v) => if (v ne null) newHadoopConf.set(k, v) }
    newHadoopConf
  }
}

/**
 * Concrete implementation of a [[SessionStateBuilder]].
 */
@Experimental
@InterfaceStability.Unstable
class SessionStateBuilder(
    session: SparkSession,
    parentState: Option[SessionState] = None)
  extends BaseSessionStateBuilder(session, parentState) {
  override protected def newBuilder: NewBuilder = new SessionStateBuilder(_, _)
}

/**
 * Session shared [[FunctionResourceLoader]].
 */
@InterfaceStability.Unstable
class SessionResourceLoader(session: SparkSession) extends FunctionResourceLoader {
  override def loadResource(resource: FunctionResource): Unit = {
    resource.resourceType match {
      case JarResource => addJar(resource.uri)
      case FileResource => session.sparkContext.addFile(resource.uri)
      case ArchiveResource =>
        throw new AnalysisException(
          "Archive is not allowed to be loaded. If YARN mode is used, " +
            "please use --archives options while calling spark-submit.")
    }
  }

  /**
   * Add a jar path to [[SparkContext]] and the classloader.
   *
   * Note: this method seems not access any session state, but a Hive based `SessionState` needs
   * to add the jar to its hive client for the current session. Hence, it still needs to be in
   * [[SessionState]].
   */
  def addJar(path: String): Unit = {
    session.sparkContext.addJar(path)
    val uri = new Path(path).toUri
    val jarURL = if (uri.getScheme == null) {
      // `path` is a local file path without a URL scheme
      new File(path).toURI.toURL
    } else {
      // `path` is a URL with a scheme
      uri.toURL
    }
    session.sharedState.jarClassLoader.addURL(jarURL)
    Thread.currentThread().setContextClassLoader(session.sharedState.jarClassLoader)
  }
}
