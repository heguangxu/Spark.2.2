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

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import scala.io.Source

import com.fasterxml.jackson.core.JsonProcessingException
import org.eclipse.jetty.server.{HttpConnectionFactory, Server, ServerConnector}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.{QueuedThreadPool, ScheduledExecutorScheduler}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
  * A server that responds to requests submitted by the [[RestSubmissionClient]].
  * 响应[[RestSubmissionClient]]提交的请求的服务器。
  *
  *
  * This server responds with different HTTP codes depending on the situation:
  *   200 OK - Request was processed successfully
  *   400 BAD REQUEST - Request was malformed, not successfully validated, or of unexpected type
  *   468 UNKNOWN PROTOCOL VERSION - Request specified a protocol this server does not understand
  *   500 INTERNAL SERVER ERROR - Server throws an exception internally while processing the request
  *
  * 该服务器根据情况响应不同的HTTP代码:
  *   200 OK           - Request被成功处理
  *   400 不良请求      -请求是畸形的，没有成功验证，或意想不到的类型
  *   468 未知协议版本   -请求指定该服务器不懂的协议
  *   500 内部服务器错误  ——服务器在处理请求时在内部抛出一个异常
  *
  * The server always includes a JSON representation of the relevant [[SubmitRestProtocolResponse]]
  * in the HTTP body. If an error occurs, however, the server will include an [[ErrorResponse]]
  * instead of the one expected by the client. If the construction of this error response itself
  * fails, the response will consist of an empty body with a response code that indicates internal
  * server error.
  *
  * 服务器总是包含一个JSON表示相关的[[SubmitRestProtocolResponse]]在HTTP的body中。然而，如果出现错误，
  * 服务器将包括[[ErrorResponse]]而不是客户机所期望的。如果该错误响应本身的构建失败，那么响应将由一个空的主体组成，
  * 其响应代码指示内部服务器错误。
  *
  */
private[spark] abstract class RestSubmissionServer(
                                                    val host: String,
                                                    val requestedPort: Int,
                                                    val masterConf: SparkConf) extends Logging {
  protected val submitRequestServlet: SubmitRequestServlet
  protected val killRequestServlet: KillRequestServlet
  protected val statusRequestServlet: StatusRequestServlet

  private var _server: Option[Server] = None

  // A mapping from URL prefixes to servlets that serve them. Exposed for testing.
  // 从URL前缀到服务它们的servlet的映射。暴露进行测试。
  protected val baseContext = s"/${RestSubmissionServer.PROTOCOL_VERSION}/submissions"
  protected lazy val contextToServlet = Map[String, RestServlet](
    s"$baseContext/create/*" -> submitRequestServlet,
    s"$baseContext/kill/*" -> killRequestServlet,
    s"$baseContext/status/*" -> statusRequestServlet,
    "/*" -> new ErrorServlet // default handler
  )

  /** Start the server and return the bound port.
    * 启动程序返回绑定的端口
    * */
  def start(): Int = {
    val (server, boundPort) = Utils.startServiceOnPort[Server](requestedPort, doStart, masterConf)
    _server = Some(server)
    logInfo(s"Started REST server for submitting applications on port $boundPort")
    boundPort
  }

  /**
    * Map the servlets to their corresponding contexts and attach them to a server.
    * Return a 2-tuple of the started server and the bound port.
    *
    * 将servlet映射到相应的上下文，并将它们附加到服务器上。返回一个2-tuple包含启动的服务器和绑定端口。
    */
  private def doStart(startPort: Int): (Server, Int) = {
    val threadPool = new QueuedThreadPool
    threadPool.setDaemon(true)
    val server = new Server(threadPool)

    val connector = new ServerConnector(
      server,
      null,
      // Call this full constructor to set this, which forces daemon threads:
      new ScheduledExecutorScheduler("RestSubmissionServer-JettyScheduler", true),
      null,
      -1,
      -1,
      new HttpConnectionFactory())
    connector.setHost(host)
    connector.setPort(startPort)
    server.addConnector(connector)

    val mainHandler = new ServletContextHandler
    mainHandler.setServer(server)
    mainHandler.setContextPath("/")
    contextToServlet.foreach { case (prefix, servlet) =>
      mainHandler.addServlet(new ServletHolder(servlet), prefix)
    }
    server.setHandler(mainHandler)
    server.start()
    val boundPort = connector.getLocalPort
    (server, boundPort)
  }

  def stop(): Unit = {
    _server.foreach(_.stop())
  }
}

private[rest] object RestSubmissionServer {
  val PROTOCOL_VERSION = RestSubmissionClient.PROTOCOL_VERSION
  val SC_UNKNOWN_PROTOCOL_VERSION = 468
}

/**
  * An abstract servlet for handling requests passed to the [[RestSubmissionServer]].
  *
  * 一个抽象的servlet，用于处理传递到[[RestSubmissionServer]]的请求。
  */
private[rest] abstract class RestServlet extends HttpServlet with Logging {

  /**
    * Serialize the given response message to JSON and send it through the response servlet.
    * This validates the response before sending it to ensure it is properly constructed.
    *
    * 序列化将给定的响应消息为JSON格式，并通过响应servlet发送。这在发送之前对响应进行验证，以确保其构造正确。
    */
  protected def sendResponse(
                              responseMessage: SubmitRestProtocolResponse,
                              responseServlet: HttpServletResponse): Unit = {
    val message = validateResponse(responseMessage, responseServlet)
    responseServlet.setContentType("application/json")
    responseServlet.setCharacterEncoding("utf-8")
    responseServlet.getWriter.write(message.toJson)
  }

  /**
    * Return any fields in the client request message that the server does not know about.
    *
    * 在客户端请求消息中返回服务器不知道的任何字段。
    *
    * The mechanism for this is to reconstruct the JSON on the server side and compare the
    * diff between this JSON and the one generated on the client side. Any fields that are
    * only in the client JSON are treated as unexpected.
    *
    * 其机制是在服务器端重构JSON，并比较JSON与客户端生成的数据之间的差异。任何仅在客户机JSON中出现的字段都被视为意外。
    */
  protected def findUnknownFields(
                                   requestJson: String,
                                   requestMessage: SubmitRestProtocolMessage): Array[String] = {
    val clientSideJson = parse(requestJson)
    val serverSideJson = parse(requestMessage.toJson)
    val Diff(_, _, unknown) = clientSideJson.diff(serverSideJson)
    unknown match {
      case j: JObject => j.obj.map { case (k, _) => k }.toArray
      case _ => Array.empty[String] // No difference
    }
  }

  /** Return a human readable String representation of the exception.
    * 返回一个可读的异常的字符串表示。
    * */
  protected def formatException(e: Throwable): String = {
    val stackTraceString = e.getStackTrace.map { "\t" + _ }.mkString("\n")
    s"$e\n$stackTraceString"
  }

  /** Construct an error message to signal the fact that an exception has been thrown.
    * 构造一个错误消息，以表明抛出异常的事实。
    * */
  protected def handleError(message: String): ErrorResponse = {
    val e = new ErrorResponse
    e.serverSparkVersion = sparkVersion
    e.message = message
    e
  }

  /**
    * Parse a submission ID from the relative path, assuming it is the first part of the path.
    * For instance, we expect the path to take the form /[submission ID]/maybe/something/else.
    * The returned submission ID cannot be empty. If the path is unexpected, return None.
    *
    * 从相对路径解析提交ID，假设它是路径的第一部分。例如，我们期望路径采用 /[submission ID]//something/else这样的方式
    * 返回的提交ID不能为空。如果路径是错误的，返回None。
    */
  protected def parseSubmissionId(path: String): Option[String] = {
    if (path == null || path.isEmpty) {
      None
    } else {
      path.stripPrefix("/").split("/").headOption.filter(_.nonEmpty)
    }
  }

  /**
    * Validate the response to ensure that it is correctly constructed.
    * 验证响应，以确保其构造正确。
    *
    * If it is, simply return the message as is. Otherwise, return an error response instead
    * to propagate the exception back to the client and set the appropriate error code.
    *
    * 如果是，则简单地返回消息。否则，返回一个错误响应，而不是将异常传播回客户机并设置适当的错误代码。
    */
  private def validateResponse(
                                responseMessage: SubmitRestProtocolResponse,
                                responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
    try {
      responseMessage.validate()
      responseMessage
    } catch {
      case e: Exception =>
        responseServlet.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        handleError("Internal server error: " + formatException(e))
    }
  }
}

/**
  * A servlet for handling kill requests passed to the [[RestSubmissionServer]].
  * 一个用来处理杀死请求的servlet被传递给[[RestSubmissionServer]]。
  */
private[rest] abstract class KillRequestServlet extends RestServlet {

  /**
    * If a submission ID is specified in the URL, have the Master kill the corresponding
    * driver and return an appropriate response to the client. Otherwise, return error.
    *
    * 如果在URL中指定了提交ID，那么主服务器将杀死相应的驱动程序并向客户机返回适当的响应。否则,返回错误。
    */
  protected override def doPost(
                                 request: HttpServletRequest,
                                 response: HttpServletResponse): Unit = {
    val submissionId = parseSubmissionId(request.getPathInfo)
    val responseMessage = submissionId.map(handleKill).getOrElse {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
      handleError("Submission ID is missing in kill request.")
    }
    sendResponse(responseMessage, response)
  }

  protected def handleKill(submissionId: String): KillSubmissionResponse
}

/**
  * A servlet for handling status requests passed to the [[RestSubmissionServer]].
  * 处理传递到[[RestSubmissionServer]]的状态请求的servlet。
  */
private[rest] abstract class StatusRequestServlet extends RestServlet {

  /**
    * If a submission ID is specified in the URL, request the status of the corresponding
    * driver from the Master and include it in the response. Otherwise, return error.
    *
    * 如果在URL中指定了提交ID，请从主服务器请求相应的驱动程序的状态，并将其包含在响应中。否则,返回错误。
    */
  protected override def doGet(
                                request: HttpServletRequest,
                                response: HttpServletResponse): Unit = {
    val submissionId = parseSubmissionId(request.getPathInfo)
    val responseMessage = submissionId.map(handleStatus).getOrElse {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
      handleError("Submission ID is missing in status request.")
    }
    sendResponse(responseMessage, response)
  }

  protected def handleStatus(submissionId: String): SubmissionStatusResponse
}

/**
  * A servlet for handling submit requests passed to the [[RestSubmissionServer]].
  * 一个处理提交请求的servlet被传递给[[RestSubmissionServer]]。
  */
private[rest] abstract class SubmitRequestServlet extends RestServlet {

  /**
    * Submit an application to the Master with parameters specified in the request.
    * 向Master提交请求中指定参数的应用程序。
    *
    * The request is assumed to be a [[SubmitRestProtocolRequest]] in the form of JSON.
    * If the request is successfully processed, return an appropriate response to the
    * client indicating so. Otherwise, return error instead.
    *
    * 请求被认为是[[SubmitRestProtocolRequest]]的形式JSON。如果请求成功处理，则返回一个适当的响应给客户端。否则,返回错误。
    */
  protected override def doPost(
                                 requestServlet: HttpServletRequest,
                                 responseServlet: HttpServletResponse): Unit = {
    val responseMessage =
      try {
        val requestMessageJson = Source.fromInputStream(requestServlet.getInputStream).mkString
        val requestMessage = SubmitRestProtocolMessage.fromJson(requestMessageJson)
        // The response should have already been validated on the client.
        // In case this is not true, validate it ourselves to avoid potential NPEs.
        requestMessage.validate()
        handleSubmit(requestMessageJson, requestMessage, responseServlet)
      } catch {
        // The client failed to provide a valid JSON, so this is not our fault
        case e @ (_: JsonProcessingException | _: SubmitRestProtocolException) =>
          responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
          handleError("Malformed request: " + formatException(e))
      }
    sendResponse(responseMessage, responseServlet)
  }

  protected def handleSubmit(
                              requestMessageJson: String,
                              requestMessage: SubmitRestProtocolMessage,
                              responseServlet: HttpServletResponse): SubmitRestProtocolResponse
}

/**
  * A default servlet that handles error cases that are not captured by other servlets.
  * 一个默认的servlet，它处理不被其他servlet捕获的错误。
  */
private class ErrorServlet extends RestServlet {
  private val serverVersion = RestSubmissionServer.PROTOCOL_VERSION

  /** Service a faulty request by returning an appropriate error message to the client.
    *  一个向客户端返回适当的错误消息的服务
    * */
  protected override def service(
                                  request: HttpServletRequest,
                                  response: HttpServletResponse): Unit = {
    val path = request.getPathInfo
    val parts = path.stripPrefix("/").split("/").filter(_.nonEmpty).toList
    var versionMismatch = false
    var msg =
      parts match {
        case Nil =>
          // http://host:port/
          "Missing protocol version."
        case `serverVersion` :: Nil =>
          // http://host:port/correct-version
          "Missing the /submissions prefix."
        case `serverVersion` :: "submissions" :: tail =>
          // http://host:port/correct-version/submissions/*
          "Missing an action: please specify one of /create, /kill, or /status."
        case unknownVersion :: tail =>
          // http://host:port/unknown-version/*
          versionMismatch = true
          s"Unknown protocol version '$unknownVersion'."
        case _ =>
          // never reached
          s"Malformed path $path."
      }
    msg += s" Please submit requests through http://[host]:[port]/$serverVersion/submissions/..."
    val error = handleError(msg)
    // If there is a version mismatch, include the highest protocol version that
    // this server supports in case the client wants to retry with our version
    if (versionMismatch) {
      error.highestProtocolVersion = serverVersion
      response.setStatus(RestSubmissionServer.SC_UNKNOWN_PROTOCOL_VERSION)
    } else {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
    }
    sendResponse(error, response)
  }
}
