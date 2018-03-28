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

import java.io.File
import javax.servlet.http.HttpServletResponse

import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf}
import org.apache.spark.deploy.{Command, DeployMessages, DriverDescription}
import org.apache.spark.deploy.ClientArguments._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

/**
  * A server that responds to requests submitted by the [[RestSubmissionClient]].
  * This is intended to be embedded in the standalone Master and used in cluster mode only.
  *
  * 响应[[RestSubmissionClient]]提交的请求的服务器。它的目的是嵌入到独立的主程序中，并只在集群模式中使用。
  *
  * This server responds with different HTTP codes depending on the situation:
  *   200 OK - Request was processed successfully
  *   400 BAD REQUEST - Request was malformed, not successfully validated, or of unexpected type
  *   468 UNKNOWN PROTOCOL VERSION - Request specified a protocol this server does not understand
  *   500 INTERNAL SERVER ERROR - Server throws an exception internally while processing the request
  *
  *   该服务器根据情况响应不同的HTTP代码:
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
  * 服务器总是包含一个JSON表示相关的[[SubmitRestProtocolResponse]]在HTTP的body中。
  * 然而，如果出现错误，服务器将包括[[ErrorResponse]]而不是客户机所期望的。如果该错误响应本身的构建失败，
  * 那么响应将由一个空的主体组成，其响应代码指示内部服务器错误。
  *
  *
  * @param host the address this server should bind to
  * @param requestedPort the port this server will attempt to bind to
  * @param masterConf the conf used by the Master
  * @param masterEndpoint reference to the Master endpoint to which requests can be sent
  * @param masterUrl the URL of the Master new drivers will attempt to connect to
  */
private[deploy] class StandaloneRestServer(
                                            host: String,
                                            requestedPort: Int,
                                            masterConf: SparkConf,
                                            masterEndpoint: RpcEndpointRef,
                                            masterUrl: String)
  extends RestSubmissionServer(host, requestedPort, masterConf) {

  protected override val submitRequestServlet =
    new StandaloneSubmitRequestServlet(masterEndpoint, masterUrl, masterConf)
  protected override val killRequestServlet =
    new StandaloneKillRequestServlet(masterEndpoint, masterConf)
  protected override val statusRequestServlet =
    new StandaloneStatusRequestServlet(masterEndpoint, masterConf)
}

/**
  * A servlet for handling kill requests passed to the [[StandaloneRestServer]].
  * 一个用于处理传递给[[StandaloneRestServer]]的servlet。
  */
private[rest] class StandaloneKillRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends KillRequestServlet {

  protected def handleKill(submissionId: String): KillSubmissionResponse = {
    val response = masterEndpoint.askSync[DeployMessages.KillDriverResponse](
      DeployMessages.RequestKillDriver(submissionId))
    val k = new KillSubmissionResponse
    k.serverSparkVersion = sparkVersion
    k.message = response.message
    k.submissionId = submissionId
    k.success = response.success
    k
  }
}

/**
  * A servlet for handling status requests passed to the [[StandaloneRestServer]].
  * 一个处理状态请求的servlet被传递给[[StandaloneRestServer]]。
  */
private[rest] class StandaloneStatusRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends StatusRequestServlet {

  protected def handleStatus(submissionId: String): SubmissionStatusResponse = {
    val response = masterEndpoint.askSync[DeployMessages.DriverStatusResponse](
      DeployMessages.RequestDriverStatus(submissionId))
    val message = response.exception.map { s"Exception from the cluster:\n" + formatException(_) }
    val d = new SubmissionStatusResponse
    d.serverSparkVersion = sparkVersion
    d.submissionId = submissionId
    d.success = response.found
    d.driverState = response.state.map(_.toString).orNull
    d.workerId = response.workerId.orNull
    d.workerHostPort = response.workerHostPort.orNull
    d.message = message.orNull
    d
  }
}

/**
  * A servlet for handling submit requests passed to the [[StandaloneRestServer]].
  * 一个处理提交请求的servlet被传递给[[StandaloneRestServer]]。
  */
private[rest] class StandaloneSubmitRequestServlet(
                                                    masterEndpoint: RpcEndpointRef,
                                                    masterUrl: String,
                                                    conf: SparkConf)
  extends SubmitRequestServlet {

  /**
    * Build a driver description from the fields specified in the submit request.
    * 从提交请求中指定的字段中构建驱动描述。
    *
    * This involves constructing a command that takes into account memory, java options,
    * classpath and other settings to launch the driver. This does not currently consider
    * fields used by python applications since python is not supported in standalone
    * cluster mode yet.
    *
    * 这包括构造一个命令，它考虑到内存、java选项、类路径和其他设置来启动驱动程序。
    * 目前还没有考虑python应用程序使用的字段，因为python在独立集群模式下还不支持。
    */
  private def buildDriverDescription(request: CreateSubmissionRequest): DriverDescription = {
    // Required fields, including the main class because python is not yet supported
    val appResource = Option(request.appResource).getOrElse {
      throw new SubmitRestMissingFieldException("Application jar is missing.")
    }
    val mainClass = Option(request.mainClass).getOrElse {
      throw new SubmitRestMissingFieldException("Main class is missing.")
    }

    // Optional fields
    val sparkProperties = request.sparkProperties
    val driverMemory = sparkProperties.get("spark.driver.memory")
    val driverCores = sparkProperties.get("spark.driver.cores")
    val driverExtraJavaOptions = sparkProperties.get("spark.driver.extraJavaOptions")
    val driverExtraClassPath = sparkProperties.get("spark.driver.extraClassPath")
    val driverExtraLibraryPath = sparkProperties.get("spark.driver.extraLibraryPath")
    val superviseDriver = sparkProperties.get("spark.driver.supervise")
    val appArgs = request.appArgs
    val environmentVariables = request.environmentVariables

    // Construct driver description
    val conf = new SparkConf(false)
      .setAll(sparkProperties)
      .set("spark.master", masterUrl)
    val extraClassPath = driverExtraClassPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraLibraryPath = driverExtraLibraryPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraJavaOpts = driverExtraJavaOptions.map(Utils.splitCommandString).getOrElse(Seq.empty)
    val sparkJavaOpts = Utils.sparkJavaOpts(conf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    val command = new Command(
      "org.apache.spark.deploy.worker.DriverWrapper",
      Seq("{{WORKER_URL}}", "{{USER_JAR}}", mainClass) ++ appArgs, // args to the DriverWrapper
      environmentVariables, extraClassPath, extraLibraryPath, javaOpts)
    val actualDriverMemory = driverMemory.map(Utils.memoryStringToMb).getOrElse(DEFAULT_MEMORY)
    val actualDriverCores = driverCores.map(_.toInt).getOrElse(DEFAULT_CORES)
    val actualSuperviseDriver = superviseDriver.map(_.toBoolean).getOrElse(DEFAULT_SUPERVISE)
    new DriverDescription(
      appResource, actualDriverMemory, actualDriverCores, actualSuperviseDriver, command)
  }

  /**
    * Handle the submit request and construct an appropriate response to return to the client.
    *
    * 处理提交请求，并构造相应的响应返回给客户端。
    *
    * This assumes that the request message is already successfully validated.
    * If the request message is not of the expected type, return error to the client.
    *
    * 这假定请求消息已经成功验证。如果请求消息不属于预期类型，则将错误返回给客户端。
    */
  protected override def handleSubmit(
                                       requestMessageJson: String,
                                       requestMessage: SubmitRestProtocolMessage,
                                       responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
    requestMessage match {
      case submitRequest: CreateSubmissionRequest =>
        val driverDescription = buildDriverDescription(submitRequest)
        val response = masterEndpoint.askSync[DeployMessages.SubmitDriverResponse](
          DeployMessages.RequestSubmitDriver(driverDescription))
        val submitResponse = new CreateSubmissionResponse
        submitResponse.serverSparkVersion = sparkVersion
        submitResponse.message = response.message
        submitResponse.success = response.success
        submitResponse.submissionId = response.driverId.orNull
        val unknownFields = findUnknownFields(requestMessageJson, requestMessage)
        if (unknownFields.nonEmpty) {
          // If there are fields that the server does not know about, warn the client
          submitResponse.unknownFields = unknownFields
        }
        submitResponse
      case unexpected =>
        responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        handleError(s"Received message of unexpected type ${unexpected.messageType}.")
    }
  }
}
