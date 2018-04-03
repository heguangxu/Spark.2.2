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

package org.apache.spark.sql.catalog

import scala.collection.JavaConverters._

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset}
import org.apache.spark.sql.types.StructType


/**
 * Catalog interface for Spark. To access this, use `SparkSession.catalog`.
  *
  * Catalog:字典表，用于注册表，对表缓存后便于查询
  * catalog:的意思是目录
 *
 * @since 2.0.0
 */
@InterfaceStability.Stable
abstract class Catalog {

  /**
   * Returns the current default database in this session.
    * 在这个会话中返回当前的默认数据库。
   *
   * @since 2.0.0
   */
  def currentDatabase: String

  /**
   * Sets the current default database in this session.
   * 在这个会话中设置当前的默认数据库。
   * @since 2.0.0
   */
  def setCurrentDatabase(dbName: String): Unit

  /**
   * Returns a list of databases available across all sessions.
    * 返回所有会话中可用的数据库列表。
   *
   * @since 2.0.0
   */
  def listDatabases(): Dataset[Database]

  /**
   * Returns a list of tables/views in the current database.
   * This includes all temporary views.
    *
    * 返回当前数据库的表列表，包括临时视图
   *
   * @since 2.0.0
   */
  def listTables(): Dataset[Table]

  /**
   * Returns a list of tables/views in the specified database.
   * This includes all temporary views.
    *
    * 返回一个指定数据库的table和视图
   *
   * @since 2.0.0
   */
  @throws[AnalysisException]("database does not exist")
  def listTables(dbName: String): Dataset[Table]

  /**
   * Returns a list of functions registered in the current database.
   * This includes all temporary functions
    *
    * 返回当前  数据库的函数列表，包括临时函数
   *
   * @since 2.0.0
   */
  def listFunctions(): Dataset[Function]

  /**
   * Returns a list of functions registered in the specified database.
   * This includes all temporary functions
   *
    * 返回指定数据库的函数列表，包括临时函数
    *
   * @since 2.0.0
   */
  @throws[AnalysisException]("database does not exist")
  def listFunctions(dbName: String): Dataset[Function]

  /**
   * Returns a list of columns for the given table/view or temporary view.
    *
    * 返回给定表/视图或临时视图的 列 列表。
   *
   * @param tableName is either a qualified or unqualified name that designates a table/view.
   *                  If no database identifier is provided, it refers to a temporary view or
   *                  a table/view in the current database.
   * @since 2.0.0
   */
  @throws[AnalysisException]("table does not exist")
  def listColumns(tableName: String): Dataset[Column]

  /**
   * Returns a list of columns for the given table/view in the specified database.
   *
    * 返回指定数据库中给定表/视图的列列表。
    *
   * @param dbName is a name that designates a database.
   * @param tableName is an unqualified name that designates a table/view.
   * @since 2.0.0
   */
  @throws[AnalysisException]("database or table does not exist")
  def listColumns(dbName: String, tableName: String): Dataset[Column]

  /**
   * Get the database with the specified name. This throws an AnalysisException when the database
   * cannot be found.
    *
    * 获取具有指定名称的数据库。当无法找到数据库时，就会抛出一个AnalysisException。
   *
   * @since 2.1.0
   */
  @throws[AnalysisException]("database does not exist")
  def getDatabase(dbName: String): Database

  /**
   * Get the table or view with the specified name. This table can be a temporary view or a
   * table/view. This throws an AnalysisException when no Table can be found.
    *
    * 使用指定的名称获取表或视图。此表可以是临时视图或表/视图。当无法找到表时，就会抛出一个AnalysisException。
   *
   * @param tableName is either a qualified or unqualified name that designates a table/view.
   *                  If no database identifier is provided, it refers to a table/view in
   *                  the current database.
   * @since 2.1.0
   */
  @throws[AnalysisException]("table does not exist")
  def getTable(tableName: String): Table

  /**
   * Get the table or view with the specified name in the specified database. This throws an
   * AnalysisException when no Table can be found.
    *
    * 在指定的数据库中使用指定的名称获取表或视图。当无法找到表时，就会抛出一个AnalysisException。
   *
   * @since 2.1.0
   */
  @throws[AnalysisException]("database or table does not exist")
  def getTable(dbName: String, tableName: String): Table

  /**
   * Get the function with the specified name. This function can be a temporary function or a
   * function. This throws an AnalysisException when the function cannot be found.
    *
    * 获取具有指定名称的函数。这个函数可以是一个临时函数，也可以是一个函数。当无法找到函数时，就会抛出一个AnalysisException。
   *
   * @param functionName is either a qualified or unqualified name that designates a function.
   *                     If no database identifier is provided, it refers to a temporary function
   *                     or a function in the current database.
   * @since 2.1.0
   */
  @throws[AnalysisException]("function does not exist")
  def getFunction(functionName: String): Function

  /**
   * Get the function with the specified name. This throws an AnalysisException when the function
   * cannot be found.
   *
   * @param dbName is a name that designates a database.
   * @param functionName is an unqualified name that designates a function in the specified database
   * @since 2.1.0
   */
  @throws[AnalysisException]("database or function does not exist")
  def getFunction(dbName: String, functionName: String): Function

  /**
   * Check if the database with the specified name exists.
    *
    * 检查具有指定名称的数据库是否存在。
   *
   * @since 2.1.0
   */
  def databaseExists(dbName: String): Boolean

  /**
   * Check if the table or view with the specified name exists. This can either be a temporary
   * view or a table/view.
    * 检查具有指定名称的表或视图是否存在。这可以是临时视图，也可以是表/视图。
    *
   *
   * @param tableName is either a qualified or unqualified name that designates a table/view.
   *                  If no database identifier is provided, it refers to a table/view in
   *                  the current database.
   * @since 2.1.0
   */
  def tableExists(tableName: String): Boolean

  /**
   * Check if the table or view with the specified name exists in the specified database.
    *
    * 检查指定数据库中是否存在指定名称的表或视图。
   *
   * @param dbName is a name that designates a database.
   * @param tableName is an unqualified name that designates a table.
   * @since 2.1.0
   */
  def tableExists(dbName: String, tableName: String): Boolean

  /**
   * Check if the function with the specified name exists. This can either be a temporary function
   * or a function.
   *
    * 检查具有指定名称的函数是否存在。这可以是一个临时函数，也可以是一个函数。
    *
   * @param functionName is either a qualified or unqualified name that designates a function.
   *                     If no database identifier is provided, it refers to a function in
   *                     the current database.
   * @since 2.1.0
   */
  def functionExists(functionName: String): Boolean

  /**
   * Check if the function with the specified name exists in the specified database.
    *
    * 检查指定数据库中是否具有指定名称的函数。
   *
   * @param dbName is a name that designates a database.
   * @param functionName is an unqualified name that designates a function.
   * @since 2.1.0
   */
  def functionExists(dbName: String, functionName: String): Boolean

  /**
   * Creates a table from the given path and returns the corresponding DataFrame.
   * It will use the default data source configured by spark.sql.sources.default.
    *
    * 从给定的路径创建一个表并返回相应的DataFrame。它将使用sparksql .sources.default配置的默认数据源。
   *
   * @param tableName is either a qualified or unqualified name that designates a table.
   *                  If no database identifier is provided, it refers to a table in
   *                  the current database.
   * @since 2.0.0
   */
  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(tableName: String, path: String): DataFrame = {
    createTable(tableName, path)
  }

  /**
   * :: Experimental ::
   * Creates a table from the given path and returns the corresponding DataFrame.
   * It will use the default data source configured by spark.sql.sources.default.
    *
    * 从给定的路径创建一个表并返回相应的DataFrame。它将使用sparksql .sources.default配置的默认数据源。
   *
   * @param tableName is either a qualified or unqualified name that designates a table.
   *                  If no database identifier is provided, it refers to a table in
   *                  the current database.
   * @since 2.2.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def createTable(tableName: String, path: String): DataFrame

  /**
   * Creates a table from the given path based on a data source and returns the corresponding
   * DataFrame.
    * 根据数据源从给定的路径创建一个表，并返回相应的dataframe。
   *
   * @param tableName is either a qualified or unqualified name that designates a table.
   *                  If no database identifier is provided, it refers to a table in
   *                  the current database.
   * @since 2.0.0
   */
  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(tableName: String, path: String, source: String): DataFrame = {
    createTable(tableName, path, source)
  }

  /**
   * :: Experimental ::
   * Creates a table from the given path based on a data source and returns the corresponding
   * DataFrame.
   *
   * @param tableName is either a qualified or unqualified name that designates a table.
   *                  If no database identifier is provided, it refers to a table in
   *                  the current database.
   * @since 2.2.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def createTable(tableName: String, path: String, source: String): DataFrame

  /**
   * Creates a table from the given path based on a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @param tableName is either a qualified or unqualified name that designates a table.
   *                  If no database identifier is provided, it refers to a table in
   *                  the current database.
   * @since 2.0.0
   */
  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      source: String,
      options: java.util.Map[String, String]): DataFrame = {
    createTable(tableName, source, options)
  }

  /**
   * :: Experimental ::
   * Creates a table based on the dataset in a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @param tableName is either a qualified or unqualified name that designates a table.
   *                  If no database identifier is provided, it refers to a table in
   *                  the current database.
   * @since 2.2.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def createTable(
      tableName: String,
      source: String,
      options: java.util.Map[String, String]): DataFrame = {
    createTable(tableName, source, options.asScala.toMap)
  }

  /**
   * (Scala-specific)
   * Creates a table from the given path based on a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @param tableName is either a qualified or unqualified name that designates a table.
   *                  If no database identifier is provided, it refers to a table in
   *                  the current database.
   * @since 2.0.0
   */
  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      source: String,
      options: Map[String, String]): DataFrame = {
    createTable(tableName, source, options)
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Creates a table based on the dataset in a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @param tableName is either a qualified or unqualified name that designates a table.
   *                  If no database identifier is provided, it refers to a table in
   *                  the current database.
   * @since 2.2.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def createTable(
      tableName: String,
      source: String,
      options: Map[String, String]): DataFrame

  /**
   * :: Experimental ::
   * Create a table from the given path based on a data source, a schema and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @param tableName is either a qualified or unqualified name that designates a table.
   *                  If no database identifier is provided, it refers to a table in
   *                  the current database.
   * @since 2.0.0
   */
  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: java.util.Map[String, String]): DataFrame = {
    createTable(tableName, source, schema, options)
  }

  /**
   * :: Experimental ::
   * Create a table based on the dataset in a data source, a schema and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @param tableName is either a qualified or unqualified name that designates a table.
   *                  If no database identifier is provided, it refers to a table in
   *                  the current database.
   * @since 2.2.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: java.util.Map[String, String]): DataFrame = {
    createTable(tableName, source, schema, options.asScala.toMap)
  }

  /**
   * (Scala-specific)
   * Create a table from the given path based on a data source, a schema and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @param tableName is either a qualified or unqualified name that designates a table.
   *                  If no database identifier is provided, it refers to a table in
   *                  the current database.
   * @since 2.0.0
   */
  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame = {
    createTable(tableName, source, schema, options)
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Create a table based on the dataset in a data source, a schema and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @param tableName is either a qualified or unqualified name that designates a table.
   *                  If no database identifier is provided, it refers to a table in
   *                  the current database.
   * @since 2.2.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame

  /**
   * Drops the local temporary view with the given view name in the catalog.
   * If the view has been cached before, then it will also be uncached.
   *
   * Local temporary view is session-scoped. Its lifetime is the lifetime of the session that
   * created it, i.e. it will be automatically dropped when the session terminates. It's not
   * tied to any databases, i.e. we can't use `db1.view1` to reference a local temporary view.
   *
   * Note that, the return type of this method was Unit in Spark 2.0, but changed to Boolean
   * in Spark 2.1.
   *
   * @param viewName the name of the temporary view to be dropped.
   * @return true if the view is dropped successfully, false otherwise.
   * @since 2.0.0
   */
  def dropTempView(viewName: String): Boolean

  /**
   * Drops the global temporary view with the given view name in the catalog.
   * If the view has been cached before, then it will also be uncached.
   *
   * Global temporary view is cross-session. Its lifetime is the lifetime of the Spark application,
   * i.e. it will be automatically dropped when the application terminates. It's tied to a system
   * preserved database `global_temp`, and we must use the qualified name to refer a global temp
   * view, e.g. `SELECT * FROM global_temp.view1`.
   *
   * @param viewName the unqualified name of the temporary view to be dropped.
   * @return true if the view is dropped successfully, false otherwise.
   * @since 2.1.0
   */
  def dropGlobalTempView(viewName: String): Boolean

  /**
   * Recovers all the partitions in the directory of a table and update the catalog.
   * Only works with a partitioned table, and not a view.
   *
   * @param tableName is either a qualified or unqualified name that designates a table.
   *                  If no database identifier is provided, it refers to a table in the
   *                  current database.
   * @since 2.1.1
   */
  def recoverPartitions(tableName: String): Unit

  /**
   * Returns true if the table is currently cached in-memory.
    *
    * 返回true如果当前表被缓存到内存中
   *
   * @param tableName is either a qualified or unqualified name that designates a table/view.
   *                  If no database identifier is provided, it refers to a temporary view or
   *                  a table/view in the current database.
   * @since 2.0.0
   */
  def isCached(tableName: String): Boolean

  /**
   * Caches the specified table in-memory.
    *
    * 缓存指定的表到内存中
   *
   * @param tableName is either a qualified or unqualified name that designates a table/view.
   *                  If no database identifier is provided, it refers to a temporary view or
   *                  a table/view in the current database.
   * @since 2.0.0
   */
  def cacheTable(tableName: String): Unit

  /**
   * Removes the specified table from the in-memory cache.
    *
    * 从内存中移除缓存的表
   *
   * @param tableName is either a qualified or unqualified name that designates a table/view.
   *                  If no database identifier is provided, it refers to a temporary view or
   *                  a table/view in the current database.
   * @since 2.0.0
   */
  def uncacheTable(tableName: String): Unit

  /**
   * Removes all cached tables from the in-memory cache.
    *
    * 移除所有在内存缓存的表
   *
   * @since 2.0.0
   */
  def clearCache(): Unit

  /**
   * Invalidates and refreshes all the cached data and metadata of the given table. For performance
   * reasons, Spark SQL or the external data source library it uses might cache certain metadata
   * about a table, such as the location of blocks. When those change outside of Spark SQL, users
   * should call this function to invalidate the cache.
    *
    * 无效并刷新给定表的所有缓存数据和元数据。出于性能原因，Spark SQL或它使用的外部数据源库可能会缓存一些
    * 关于表的元数据，比如块的位置。在Spark SQL之外的更改时，用户应该调用该函数来使缓存无效。
   *
   * If this table is cached as an InMemoryRelation, drop the original cached version and make the
   * new version cached lazily.
    *
    * 如果将此表作为内存关系缓存，则删除原始缓存版本，并使新版本缓存在缓存。
   *
   * @param tableName is either a qualified or unqualified name that designates a table/view.
   *                  If no database identifier is provided, it refers to a temporary view or
   *                  a table/view in the current database.
   * @since 2.0.0
   */
  def refreshTable(tableName: String): Unit

  /**
   * Invalidates and refreshes all the cached data (and the associated metadata) for any `Dataset`
   * that contains the given data source path. Path matching is by prefix, i.e. "/" would invalidate
   * everything that is cached.
    *
    * 对包含给定数据源路径的任何“数据集”的所有缓存数据(以及相关的元数据)进行无效和刷新。路径匹配是通过前缀，即。
    * “/”将使所有缓存的内容失效。
   *
   * @since 2.0.0
   */
  def refreshByPath(path: String): Unit
}
