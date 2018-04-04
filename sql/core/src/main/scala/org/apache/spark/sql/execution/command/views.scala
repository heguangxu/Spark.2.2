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

package org.apache.spark.sql.execution.command

import scala.collection.mutable

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.{Alias, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, View}
import org.apache.spark.sql.types.MetadataBuilder


/**
 * ViewType is used to specify the expected view type when we want to create or replace a view in
 * [[CreateViewCommand]].
 */
sealed trait ViewType {
  override def toString: String = getClass.getSimpleName.stripSuffix("$")
}

/**
 * LocalTempView means session-scoped local temporary views. Its lifetime is the lifetime of the
 * session that created it, i.e. it will be automatically dropped when the session terminates. It's
 * not tied to any databases, i.e. we can't use `db1.view1` to reference a local temporary view.
  *
  * LocalTempView的意思是会话范围的本地临时视图。它的生命周期是创建它的会话的生命周期，也就是说，
  * 当会话终止时它将被自动删除。它没有绑定到任何数据库，即我们不能使用db1.view1引用一个本地临时视图。
  *
  * // 两个view的区别：https://blog.csdn.net/qq_21383435/article/details/79805772
 */
object LocalTempView extends ViewType

/**
 * GlobalTempView means cross-session global temporary views. Its lifetime is the lifetime of the
 * Spark application, i.e. it will be automatically dropped when the application terminates. It's
 * tied to a system preserved database `global_temp`, and we must use the qualified name to refer a
 * global temp view, e.g. SELECT * FROM global_temp.view1.
  *
  * GlobalTempView意味着跨会话全局临时视图。它的生命周期是Spark应用程序的生命周期，即当应用程序终止时，
  * 它将自动删除。它绑定到一个保存数据库“global_temp”的系统，我们必须使用限定名来引用全局临时视图，
  * 例如 SELECT * FROM global_temp.view1。
  *
  * // 两个view的区别：https://blog.csdn.net/qq_21383435/article/details/79805772
 */
object GlobalTempView extends ViewType

/**
 * PersistedView means cross-session persisted views. Persisted views stay until they are
 * explicitly dropped by user command. It's always tied to a database, default to the current
 * database if not specified.
  *
  * PersistedView意味着跨会话持久化视图。持久化视图的生命周期是直到用户命令显式地删除它们为止。它总是绑定到数据库，
  * 如果没有指定，则默认为当前数据库。
 *
 * Note that, Existing persisted view with the same name are not visible to the current session
 * while the local temporary view exists, unless the view name is qualified by database.
  *
  * 注意，当前会话中不可见具有相同名称的现有持久化视图，而本地临时视图存在，除非数据库对视图名称进行了限定。
 */
object PersistedView extends ViewType


/**
 * Create or replace a view with given query plan. This command will generate some view-specific
 * properties(e.g. view default database, view query output column names) and store them as
 * properties in metastore, if we need to create a permanent view.
  *
  * 用给定的查询计划创建或替换一个视图。这个命令将生成一些特定于视图的属性（例如：查看默认数据库，查看输出的列）和
  * 把他们作为属性存储，如果我们需要创建一个永久的视图。
 *
 * @param name the name of this view.   视图名称
 * @param userSpecifiedColumns the output column names and optional comments specified by users,
 *                             can be Nil if not specified.
  *                             用户指定的输出列名称和可选注释，如果没有指定，可以为Nil。
 * @param comment the comment of this view.
 * @param properties the properties of this view.
 * @param originalText the original SQL text of this view, can be None if this view is created via
 *                     Dataset API.
  *                     如果这个视图是通过Dataset API创建的，那么这个视图的原始SQL文本就不存在了。
 * @param child the logical plan that represents the view; this is used to generate the logical
 *              plan for temporary view and the view schema.
  *              代表视图的逻辑计划;这用于生成临时视图和视图模式的逻辑计划。
 * @param allowExisting if true, and if the view already exists, noop; if false, and if the view
 *                already exists, throws analysis exception.
  *                如果视图已经存在，是true noop; 如果视图已经存在，抛出分析异常。
 * @param replace if true, and if the view already exists, updates it; if false, and if the view
 *                already exists, throws analysis exception.
  *                如果为true，而且视图已经存在，就更新它，如果为false，而且视图已经存在，就抛出异常
 * @param viewType the expected view type to be created with this command.
  *                 使用此命令创建的预期视图类型。
 */
case class CreateViewCommand(
    name: TableIdentifier,
    userSpecifiedColumns: Seq[(String, Option[String])],
    comment: Option[String],
    properties: Map[String, String],
    originalText: Option[String],
    child: LogicalPlan,
    allowExisting: Boolean,
    replace: Boolean,
    viewType: ViewType)
  extends RunnableCommand {

  import ViewHelper._

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(child)

  if (viewType == PersistedView) {
    require(originalText.isDefined, "'originalText' must be provided to create permanent view")
  }

  if (allowExisting && replace) {
    throw new AnalysisException("CREATE VIEW with both IF NOT EXISTS and REPLACE is not allowed.")
  }

  // 貌似只能为false
  private def isTemporary = viewType == LocalTempView || viewType == GlobalTempView

  // Disallows 'CREATE TEMPORARY VIEW IF NOT EXISTS' to be consistent with 'CREATE TEMPORARY TABLE'
  // 如果允许已经存在  和 临时表
  if (allowExisting && isTemporary) {
    throw new AnalysisException(
      "It is not allowed to define a TEMPORARY view with IF NOT EXISTS.")
  }

  // Temporary view names should NOT contain database prefix like "database.table"
  // 临时视图名称不应包含数据库前缀，如“database.table”。
  if (isTemporary && name.database.isDefined) {
    val database = name.database.get
    throw new AnalysisException(
      s"It is not allowed to add database prefix `$database` for the TEMPORARY view name.")
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    // 如果计划不能被分析，抛出一个异常，不要继续。
    val qe = sparkSession.sessionState.executePlan(child)
    // 大概是检查一个逻辑计划是否能运行，遇到第一个解析错误就停止
    qe.assertAnalyzed()
    // 调用analyzer解析器,重点
    val analyzedPlan = qe.analyzed

    // 如果用户使用 CREATE VIEW (）指定的column和SELECT * 方法分析出来的column列数量不一致，抛出异常
    if (userSpecifiedColumns.nonEmpty &&
        userSpecifiedColumns.length != analyzedPlan.output.length) {
      throw new AnalysisException(s"The number of columns produced by the SELECT clause " +
        s"(num: `${analyzedPlan.output.length}`) does not match the number of column names " +
        s"specified by CREATE VIEW (num: `${userSpecifiedColumns.length}`).")
    }

    // When creating a permanent view, not allowed to reference temporary objects.
    // This should be called after `qe.assertAnalyzed()` (i.e., `child` can be resolved)
    //
    // 在创建永久视图时，不允许引用临时对象。这应该在“qe. assertanalysis()”之后调用。“孩子”可以解决
    verifyTemporaryObjectsNotExists(sparkSession)

    val catalog = sparkSession.sessionState.catalog
    // 如果是本地临时视图
    if (viewType == LocalTempView) {
      //
      val aliasedPlan = aliasPlan(sparkSession, analyzedPlan)
      // 主要是将表名和对应的逻辑计划关系 放到map
      catalog.createTempView(name.table, aliasedPlan, overrideIfExists = replace)

      // 如果是全局视图
    } else if (viewType == GlobalTempView) {
      val aliasedPlan = aliasPlan(sparkSession, analyzedPlan)
      catalog.createGlobalTempView(name.table, aliasedPlan, overrideIfExists = replace)

      // 如果该视图已经在数据库中存在
    } else if (catalog.tableExists(name)) {
      val tableMetadata = catalog.getTableMetadata(name)

      // 如果已经存在，则不做处理
      if (allowExisting) {
        // Handles `CREATE VIEW IF NOT EXISTS v0 AS SELECT ...`. Does nothing when the target view
        // already exists.

        // 不是一个视图
      } else if (tableMetadata.tableType != CatalogTableType.VIEW) {
        throw new AnalysisException(s"$name is not a view")

        // 如果允许视图替换
      } else if (replace) {
        // Detect cyclic view reference on CREATE OR REPLACE VIEW.
        val viewIdent = tableMetadata.identifier
        checkCyclicViewReference(analyzedPlan, Seq(viewIdent), viewIdent)

        // Handles `CREATE OR REPLACE VIEW v0 AS SELECT ...`
        // Nothing we need to retain from the old view, so just drop and create a new one
        catalog.dropTable(viewIdent, ignoreIfNotExists = false, purge = false)
        catalog.createTable(prepareTable(sparkSession, analyzedPlan), ignoreIfExists = false)
      } else {
        // Handles `CREATE VIEW v0 AS SELECT ...`. Throws exception when the target view already
        // exists.
        throw new AnalysisException(
          s"View $name already exists. If you want to update the view definition, " +
            "please use ALTER VIEW AS or CREATE OR REPLACE VIEW AS")
      }
    } else {
      // Create the view if it doesn't exist.  如果视图不存在，就创建它
      catalog.createTable(prepareTable(sparkSession, analyzedPlan), ignoreIfExists = false)
    }

    //
    Seq.empty[Row]
  }

  /**
   * Permanent views are not allowed to reference temp objects, including temp function and views
    *
    * 永久视图不允许引用temp对象，包括temp函数和视图。
   */
  private def verifyTemporaryObjectsNotExists(sparkSession: SparkSession): Unit = {
    if (!isTemporary) {
      // This func traverses the unresolved plan `child`. Below are the reasons:
      // 1) Analyzer replaces unresolved temporary views by a SubqueryAlias with the corresponding
      // logical plan. After replacement, it is impossible to detect whether the SubqueryAlias is
      // added/generated from a temporary view.
      // 2) The temp functions are represented by multiple classes. Most are inaccessible from this
      // package (e.g., HiveGenericUDF).
      child.collect {
        // Disallow creating permanent views based on temporary views.
        case s: UnresolvedRelation
          if sparkSession.sessionState.catalog.isTemporaryTable(s.tableIdentifier) =>
          throw new AnalysisException(s"Not allowed to create a permanent view $name by " +
            s"referencing a temporary view ${s.tableIdentifier}")
        case other if !other.resolved => other.expressions.flatMap(_.collect {
          // Disallow creating permanent views based on temporary UDFs.
          case e: UnresolvedFunction
            if sparkSession.sessionState.catalog.isTemporaryFunction(e.name) =>
            throw new AnalysisException(s"Not allowed to create a permanent view $name by " +
              s"referencing a temporary function `${e.name}`")
        })
      }
    }
  }

  /**
   * If `userSpecifiedColumns` is defined, alias the analyzed plan to the user specified columns,
   * else return the analyzed plan directly.
    *
    * 如果定义了“userSpecifiedColumns”，将分析后的计划别名为用户指定的列，否则直接返回分析的计划。
    *
    * 大概就是指定列为空，进行那个plan操作，如果不为空，将指定列绑定到plan，并修改列名等操作
   */
  private def aliasPlan(session: SparkSession, analyzedPlan: LogicalPlan): LogicalPlan = {
    // userSpecifiedColumns:用户指定的输出列名称和可选注释，如果没有指定，可以为Nil。
    if (userSpecifiedColumns.isEmpty) {
      // 当用户只传入表名的时候，比如people，这里因为没有指明列名，所以会直接返回
      analyzedPlan
    } else {
      val projectList = analyzedPlan.output.zip(userSpecifiedColumns).map {
        case (attr, (colName, None)) => Alias(attr, colName)()
        case (attr, (colName, Some(colComment))) =>
          val meta = new MetadataBuilder().putString("comment", colComment).build()
          Alias(attr, colName)(explicitMetadata = Some(meta))
      }
      session.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
    }
  }

  /**
   * Returns a [[CatalogTable]] that can be used to save in the catalog. Generate the view-specific
   * properties(e.g. view default database, view query output column names) and store them as
   * properties in the CatalogTable, and also creates the proper schema for the view.
   */
  private def prepareTable(session: SparkSession, analyzedPlan: LogicalPlan): CatalogTable = {
    if (originalText.isEmpty) {
      throw new AnalysisException(
        "It is not allowed to create a persisted view from the Dataset API")
    }

    val newProperties = generateViewProperties(properties, session, analyzedPlan)

    CatalogTable(
      identifier = name,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = aliasPlan(session, analyzedPlan).schema,
      properties = newProperties,
      viewText = originalText,
      comment = comment
    )
  }
}

/**
 * Alter a view with given query plan. If the view name contains database prefix, this command will
 * alter a permanent view matching the given name, or throw an exception if view not exist. Else,
 * this command will try to alter a temporary view first, if view not exist, try permanent view
 * next, if still not exist, throw an exception.
 *
 * @param name the name of this view.
 * @param originalText the original SQL text of this view. Note that we can only alter a view by
 *                     SQL API, which means we always have originalText.
 * @param query the logical plan that represents the view; this is used to generate the new view
 *              schema.
 */
case class AlterViewAsCommand(
    name: TableIdentifier,
    originalText: String,
    query: LogicalPlan) extends RunnableCommand {

  import ViewHelper._

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(session: SparkSession): Seq[Row] = {
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    val qe = session.sessionState.executePlan(query)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    if (session.sessionState.catalog.alterTempViewDefinition(name, analyzedPlan)) {
      // a local/global temp view has been altered, we are done.
    } else {
      alterPermanentView(session, analyzedPlan)
    }

    Seq.empty[Row]
  }

  private def alterPermanentView(session: SparkSession, analyzedPlan: LogicalPlan): Unit = {
    val viewMeta = session.sessionState.catalog.getTableMetadata(name)
    if (viewMeta.tableType != CatalogTableType.VIEW) {
      throw new AnalysisException(s"${viewMeta.identifier} is not a view.")
    }

    // Detect cyclic view reference on ALTER VIEW.
    val viewIdent = viewMeta.identifier
    checkCyclicViewReference(analyzedPlan, Seq(viewIdent), viewIdent)

    val newProperties = generateViewProperties(viewMeta.properties, session, analyzedPlan)

    val updatedViewMeta = viewMeta.copy(
      schema = analyzedPlan.schema,
      properties = newProperties,
      viewText = Some(originalText))

    session.sessionState.catalog.alterTable(updatedViewMeta)
  }
}

object ViewHelper {

  import CatalogTable._

  /**
   * Generate the view default database in `properties`.
   */
  private def generateViewDefaultDatabase(databaseName: String): Map[String, String] = {
    Map(VIEW_DEFAULT_DATABASE -> databaseName)
  }

  /**
   * Generate the view query output column names in `properties`.
   */
  private def generateQueryColumnNames(columns: Seq[String]): Map[String, String] = {
    val props = new mutable.HashMap[String, String]
    if (columns.nonEmpty) {
      props.put(VIEW_QUERY_OUTPUT_NUM_COLUMNS, columns.length.toString)
      columns.zipWithIndex.foreach { case (colName, index) =>
        props.put(s"$VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX$index", colName)
      }
    }
    props.toMap
  }

  /**
   * Remove the view query output column names in `properties`.
   */
  private def removeQueryColumnNames(properties: Map[String, String]): Map[String, String] = {
    // We can't use `filterKeys` here, as the map returned by `filterKeys` is not serializable,
    // while `CatalogTable` should be serializable.
    properties.filterNot { case (key, _) =>
      key.startsWith(VIEW_QUERY_OUTPUT_PREFIX)
    }
  }

  /**
   * Generate the view properties in CatalogTable, including:
   * 1. view default database that is used to provide the default database name on view resolution.
   * 2. the output column names of the query that creates a view, this is used to map the output of
   *    the view child to the view output during view resolution.
   *
   * @param properties the `properties` in CatalogTable.
   * @param session the spark session.
   * @param analyzedPlan the analyzed logical plan that represents the child of a view.
   * @return new view properties including view default database and query column names properties.
   */
  def generateViewProperties(
      properties: Map[String, String],
      session: SparkSession,
      analyzedPlan: LogicalPlan): Map[String, String] = {
    // Generate the query column names, throw an AnalysisException if there exists duplicate column
    // names.
    val queryOutput = analyzedPlan.schema.fieldNames
    assert(queryOutput.distinct.size == queryOutput.size,
      s"The view output ${queryOutput.mkString("(", ",", ")")} contains duplicate column name.")

    // Generate the view default database name.
    val viewDefaultDatabase = session.sessionState.catalog.getCurrentDatabase

    removeQueryColumnNames(properties) ++
      generateViewDefaultDatabase(viewDefaultDatabase) ++
      generateQueryColumnNames(queryOutput)
  }

  /**
   * Recursively search the logical plan to detect cyclic view references, throw an
   * AnalysisException if cycle detected.
   *
   * A cyclic view reference is a cycle of reference dependencies, for example, if the following
   * statements are executed:
   * CREATE VIEW testView AS SELECT id FROM tbl
   * CREATE VIEW testView2 AS SELECT id FROM testView
   * ALTER VIEW testView AS SELECT * FROM testView2
   * The view `testView` references `testView2`, and `testView2` also references `testView`,
   * therefore a reference cycle (testView -> testView2 -> testView) exists.
   *
   * @param plan the logical plan we detect cyclic view references from.
   * @param path the path between the altered view and current node.
   * @param viewIdent the table identifier of the altered view, we compare two views by the
   *                  `desc.identifier`.
   */
  def checkCyclicViewReference(
      plan: LogicalPlan,
      path: Seq[TableIdentifier],
      viewIdent: TableIdentifier): Unit = {
    plan match {
      case v: View =>
        val ident = v.desc.identifier
        val newPath = path :+ ident
        // If the table identifier equals to the `viewIdent`, current view node is the same with
        // the altered view. We detect a view reference cycle, should throw an AnalysisException.
        if (ident == viewIdent) {
          throw new AnalysisException(s"Recursive view $viewIdent detected " +
            s"(cycle: ${newPath.mkString(" -> ")})")
        } else {
          v.children.foreach { child =>
            checkCyclicViewReference(child, newPath, viewIdent)
          }
        }
      case _ =>
        plan.children.foreach(child => checkCyclicViewReference(child, path, viewIdent))
    }

    // Detect cyclic references from subqueries.
    plan.expressions.foreach { expr =>
      expr match {
        case s: SubqueryExpression =>
          checkCyclicViewReference(s.plan, path, viewIdent)
        case _ => // Do nothing.
      }
    }
  }
}
