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

package org.apache.spark.deploy.rest

import com.fasterxml.jackson.annotation._
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.util.Utils

/**
  * An abstract message exchanged in the REST application submission protocol.
  * 在REST应用程序提交协议中交换的一个抽象消息。
  *
  * This message is intended to be serialized to and deserialized from JSON in the exchange.
  * Each message can either be a request or a response and consists of three common fields:
  *   (1) the action, which fully specifies the type of the message
  *   (2) the Spark version of the client / server
  *   (3) an optional message
  *
  *  该消息的目的是要序列化并在交换中从JSON中反序列化。
  *  每个消息可以是请求或响应，由三个常见字段组成:
  *  (1)行为，它完全指定消息的类型
  *  (2)客户端/服务器的Spark版本
  *  (3)一个可选的消息
  */
@JsonInclude(Include.NON_NULL)
@JsonAutoDetect(getterVisibility = Visibility.ANY, setterVisibility = Visibility.ANY)
@JsonPropertyOrder(alphabetic = true)
private[rest] abstract class SubmitRestProtocolMessage {
  @JsonIgnore
  val messageType = Utils.getFormattedClassName(this)

  val action: String = messageType
  var message: String = null

  // For JSON deserialization       为JSON反序列化
  private def setAction(a: String): Unit = { }

  /**
    * Serialize the message to JSON.
    * This also ensures that the message is valid and its fields are in the expected format.
    * 将消息序列化为JSON。
    * 这也确保消息是有效的，并且它的字段是预期格式。
    */
  def toJson: String = {
    validate()
    SubmitRestProtocolMessage.mapper.writeValueAsString(this)
  }

  /**
    * Assert the validity of the message.
    * If the validation fails, throw a [[SubmitRestProtocolException]].
    *
    * 断言消息的有效性。
    * 如果验证失败,抛出一个[[SubmitRestProtocolException]]。
    */
  final def validate(): Unit = {
    try {
      doValidate()
    } catch {
      case e: Exception =>
        throw new SubmitRestProtocolException(s"Validation of message $messageType failed!", e)
    }
  }

  /** Assert the validity of the message
    * 断言消息的有效性
    * */
  protected def doValidate(): Unit = {
    if (action == null) {
      throw new SubmitRestMissingFieldException(s"The action field is missing in $messageType")
    }
  }

  /** Assert that the specified field is set in this message.
    * 断言指定的字段是否设置在此消息中。
    * */
  protected def assertFieldIsSet[T](value: T, name: String): Unit = {
    if (value == null) {
      throw new SubmitRestMissingFieldException(s"'$name' is missing in message $messageType.")
    }
  }

  /**
    * Assert a condition when validating this message.
    * If the assertion fails, throw a [[SubmitRestProtocolException]].
    *
    * 在验证此消息时确定一个条件。
    * 如果断言失败,抛出一个[[SubmitRestProtocolException]]。
    */
  protected def assert(condition: Boolean, failMessage: String): Unit = {
    if (!condition) { throw new SubmitRestProtocolException(failMessage) }
  }
}

/**
  * Helper methods to process serialized [[SubmitRestProtocolMessage]]s.
  *
  * 助手方法来处理序列化的[[SubmitRestProtocolMessage]]。
  */
private[spark] object SubmitRestProtocolMessage {
  private val packagePrefix = this.getClass.getPackage.getName
  private val mapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .enable(SerializationFeature.INDENT_OUTPUT)
    .registerModule(DefaultScalaModule)

  /**
    * Parse the value of the action field from the given JSON.
    * If the action field is not found, throw a [[SubmitRestMissingFieldException]].
    *
    * 从给定的JSON解析动作字段的值。
    * 如果没有找到行动领域,抛出一个[[SubmitRestMissingFieldException]]。
    */
  def parseAction(json: String): String = {
    val value: Option[String] = parse(json) match {
      case JObject(fields) =>
        fields.collectFirst { case ("action", v) => v }.collect { case JString(s) => s }
      case _ => None
    }
    value.getOrElse {
      throw new SubmitRestMissingFieldException(s"Action field not found in JSON:\n$json")
    }
  }

  /**
    * Construct a [[SubmitRestProtocolMessage]] from its JSON representation.
    *
    * 构造一个JSON表示的[[SubmitRestProtocolMessage]]。
    *
    * This method first parses the action from the JSON and uses it to infer the message type.
    * Note that the action must represent one of the [[SubmitRestProtocolMessage]]s defined in
    * this package. Otherwise, a [[ClassNotFoundException]] will be thrown.
    *
    * 该方法首先从JSON解析操作，并使用它来推断消息类型。注意,行动必须代表一个[[SubmitRestProtocolMessage]]在这个包中定义。
    * 否则，将抛出[[ClassNotFoundException]]。
    */
  def fromJson(json: String): SubmitRestProtocolMessage = {
    val className = parseAction(json)
    val clazz = Utils.classForName(packagePrefix + "." + className)
      .asSubclass[SubmitRestProtocolMessage](classOf[SubmitRestProtocolMessage])
    fromJson(json, clazz)
  }

  /**
    * Construct a [[SubmitRestProtocolMessage]] from its JSON representation.
    *
    * 构造一个JSON表示的[[SubmitRestProtocolMessage]]。
    *
    * This method determines the type of the message from the class provided instead of
    * inferring it from the action field. This is useful for deserializing JSON that
    * represents custom user-defined messages.
    *
    * 此方法决定从类提供的消息的类型，而不是从action字段推断它。这对于表示自定义用户定义消息的JSON是有用的。
    */
  def fromJson[T <: SubmitRestProtocolMessage](json: String, clazz: Class[T]): T = {
    mapper.readValue(json, clazz)
  }
}
