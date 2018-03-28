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

package org.apache.spark.metrics

import java.io.{FileInputStream, InputStream}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.matching.Regex

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[spark] class MetricsConfig(conf: SparkConf) extends Logging {

  private val DEFAULT_PREFIX = "*"
  private val INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)".r
  private val DEFAULT_METRICS_CONF_FILENAME = "metrics.properties"

  private[metrics] val properties = new Properties()
  private[metrics] var perInstanceSubProperties: mutable.HashMap[String, Properties] = null

  private def setDefaultProperties(prop: Properties) {
    prop.setProperty("*.sink.servlet.class", "org.apache.spark.metrics.sink.MetricsServlet")
    prop.setProperty("*.sink.servlet.path", "/metrics/json")
    prop.setProperty("master.sink.servlet.path", "/metrics/master/json")
    prop.setProperty("applications.sink.servlet.path", "/metrics/applications/json")
  }

  /**
   * Load properties from various places, based on precedence
   * If the same property is set again latter on in the method, it overwrites the previous value
    *
    * 从不同位置加载属性，如果在方法中再次设置相同的属性，则会重写前面的值
    *
    *  MetricsConfig的initialize（）方法主要负责加载metrics.properties文件中的属性配置，并且对属性进行初始化转换
   */
  def initialize() {
    // Add default properties in case there's no properties file
    // 在没有属性文件的情况下添加默认属性
    setDefaultProperties(properties)

    loadPropertiesFromFile(conf.getOption("spark.metrics.conf"))

    // Also look for the properties in provided Spark configuration
    // 还可以在提供的Spark配置中查找属性
    val prefix = "spark.metrics.conf."
    conf.getAll.foreach {
      case (k, v) if k.startsWith(prefix) =>
        properties.setProperty(k.substring(prefix.length()), v)
      case _ =>
    }

    // Now, let's populate a list of sub-properties per instance, instance being the prefix that
    // appears before the first dot in the property name.
    // Add to the sub-properties per instance, the default properties (those with prefix "*"), if
    // they don't have that exact same sub-property already defined.
    //
    // For example, if properties has ("*.class"->"default_class", "*.path"->"default_path,
    // "driver.path"->"driver_path"), for driver specific sub-properties, we'd like the output to be
    // ("driver"->Map("path"->"driver_path", "class"->"default_class")
    // Note how class got added to based on the default property, but path remained the same
    // since "driver.path" already existed and took precedence over "*.path"
    //
    /**
            现在，让我们填充每个实例的子属性列表，例如，在属性名中出现在第一个点之前的前缀。添加到每个实例的子属性，
        默认属性(带有前缀“*”的属性)，如果它们没有已经定义的相同子属性。

          例如，如果属性有("*.class"->"default_class", "*.path"->"default_path,"driver.path"->"driver_path")，
        对于驱动特定子属性，我们希望输出为(("driver"->Map("path"->"driver_path", "class"->"default_class")

        注意，在默认属性的基础上如何添加类，但自“驱动程序”以来，路径仍然是相同的。路径"已经存在并优先于" *.path"
    */
      perInstanceSubProperties = subProperties(properties, INSTANCE_REGEX)
    if (perInstanceSubProperties.contains(DEFAULT_PREFIX)) {
      val defaultSubProperties = perInstanceSubProperties(DEFAULT_PREFIX).asScala
      for ((instance, prop) <- perInstanceSubProperties if (instance != DEFAULT_PREFIX);
           (k, v) <- defaultSubProperties if (prop.get(k) == null)) {
        prop.put(k, v)
      }
    }
  }

  /**
   * Take a simple set of properties and a regex that the instance names (part before the first dot)
   * have to conform to. And, return a map of the first order prefix (before the first dot) to the
   * sub-properties under that prefix.
   *
   * For example, if the properties sent were Properties("*.sink.servlet.class"->"class1",
   * "*.sink.servlet.path"->"path1"), the returned map would be
   * Map("*" -> Properties("sink.servlet.class" -> "class1", "sink.servlet.path" -> "path1"))
   * Note in the subProperties (value of the returned Map), only the suffixes are used as property
   * keys.
   * If, in the passed properties, there is only one property with a given prefix, it is still
   * "unflattened". For example, if the input was Properties("*.sink.servlet.class" -> "class1"
   * the returned Map would contain one key-value pair
   * Map("*" -> Properties("sink.servlet.class" -> "class1"))
   * Any passed in properties, not complying with the regex are ignored.
   *
   * @param prop the flat list of properties to "unflatten" based on prefixes
   * @param regex the regex that the prefix has to comply with
   * @return an unflatted map, mapping prefix with sub-properties under that prefix
   */
  def subProperties(prop: Properties, regex: Regex): mutable.HashMap[String, Properties] = {
    val subProperties = new mutable.HashMap[String, Properties]
    prop.asScala.foreach { kv =>
      if (regex.findPrefixOf(kv._1.toString).isDefined) {
        val regex(prefix, suffix) = kv._1.toString
        subProperties.getOrElseUpdate(prefix, new Properties).setProperty(suffix, kv._2.toString)
      }
    }
    subProperties
  }

  def getInstance(inst: String): Properties = {
    perInstanceSubProperties.get(inst) match {
      case Some(s) => s
      case None => perInstanceSubProperties.getOrElse(DEFAULT_PREFIX, new Properties)
    }
  }

  /**
   * Loads configuration from a config file. If no config file is provided, try to get file
   * in class path.
   */
  private[this] def loadPropertiesFromFile(path: Option[String]): Unit = {
    var is: InputStream = null
    try {
      is = path match {
        case Some(f) => new FileInputStream(f)
        case None => Utils.getSparkClassLoader.getResourceAsStream(DEFAULT_METRICS_CONF_FILENAME)
      }

      if (is != null) {
        properties.load(is)
      }
    } catch {
      case e: Exception =>
        val file = path.getOrElse(DEFAULT_METRICS_CONF_FILENAME)
        logError(s"Error loading configuration file $file", e)
    } finally {
      if (is != null) {
        is.close()
      }
    }
  }

}
