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

package org.apache.spark.deploy.worker

import java.io.{File, FileOutputStream, InputStream, IOException}

import scala.collection.JavaConverters._
import scala.collection.Map

import org.apache.spark.SecurityManager
import org.apache.spark.deploy.Command
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.WorkerCommandBuilder
import org.apache.spark.util.Utils

/**
  * Utilities for running commands with the spark classpath.
  * 使用spark类路径运行命令的实用程序。
  */
private[deploy]
object CommandUtils extends Logging {

  /**
    * Build a ProcessBuilder based on the given parameters.
    * The `env` argument is exposed for testing.
    *
    * 根据给定的参数构建一个ProcessBuilder。
    * “env”的参数暴露在测试中。
    *
    */
  def buildProcessBuilder(
                           command: Command,
                           securityMgr: SecurityManager,
                           memory: Int,
                           sparkHome: String,
                           substituteArguments: String => String,
                           classPaths: Seq[String] = Seq[String](),
                           env: Map[String, String] = sys.env): ProcessBuilder = {

    // 在给定的基础上构建一个命令，考虑到该命令运行的本地环境，替换任何占位符，并附加任何额外的类路径。
    val localCommand = buildLocalCommand(
      command, securityMgr, substituteArguments, classPaths, env)

    val commandSeq = buildCommandSeq(localCommand, memory, sparkHome)

    val builder = new ProcessBuilder(commandSeq: _*)
    val environment = builder.environment()
    for ((key, value) <- localCommand.environment) {
      environment.put(key, value)
    }
    builder
  }

  private def buildCommandSeq(command: Command, memory: Int, sparkHome: String): Seq[String] = {
    // SPARK-698: do not call the run.cmd script, as process.destroy()
    // fails to kill a process tree on Windows
    // spark - 698:不要调用run.cmd脚本，作为process.destroy()不能在Windows上杀死进程树
    val cmd = new WorkerCommandBuilder(sparkHome, memory, command).buildCommand()
    cmd.asScala ++ Seq(command.mainClass) ++ command.arguments
  }

  /**
    * Build a command based on the given one, taking into account the local environment
    * of where this command is expected to run, substitute any placeholders, and append
    * any extra class paths.
    *
    * 在给定的基础上构建一个命令，考虑到该命令运行的本地环境，替换任何占位符，并附加任何额外的类路径。
    */
  private def buildLocalCommand(
                                 command: Command,
                                 securityMgr: SecurityManager,
                                 substituteArguments: String => String,
                                 classPath: Seq[String] = Seq[String](),
                                 env: Map[String, String]): Command = {
    // 返回当前系统LD_LIBRARY_PATH名称。
    val libraryPathName = Utils.libraryPathEnvName
    val libraryPathEntries = command.libraryPathEntries
    val cmdLibraryPath = command.environment.get(libraryPathName)

    var newEnvironment = if (libraryPathEntries.nonEmpty && libraryPathName.nonEmpty) {
      val libraryPaths = libraryPathEntries ++ cmdLibraryPath ++ env.get(libraryPathName)
      command.environment + ((libraryPathName, libraryPaths.mkString(File.pathSeparator)))
    } else {
      command.environment
    }

    // set auth secret to env variable if needed
    // 如果需要，将auth秘密设置为env变量。
    if (securityMgr.isAuthenticationEnabled) {
      newEnvironment += (SecurityManager.ENV_AUTH_SECRET -> securityMgr.getSecretKey)
    }

    Command(
      command.mainClass,
      command.arguments.map(substituteArguments),
      newEnvironment,
      command.classPathEntries ++ classPath,
      Seq[String](), // library path already captured in environment variable
      // filter out auth secret from java options
      command.javaOpts.filterNot(_.startsWith("-D" + SecurityManager.SPARK_AUTH_SECRET_CONF)))
  }

  /** Spawn a thread that will redirect a given stream to a file
    * 生成一个线程，该线程将一个给定的流重定向到文件
    * */
  def redirectStream(in: InputStream, file: File) {
    val out = new FileOutputStream(file, true)
    // TODO: It would be nice to add a shutdown hook here that explains why the output is
    //       terminating. Otherwise if the worker dies the executor logs will silently stop.
    new Thread("redirect output to " + file) {
      override def run() {
        try {
          Utils.copyStream(in, out, true)
        } catch {
          case e: IOException =>
            logInfo("Redirection to " + file + " closed: " + e.getMessage)
        }
      }
    }.start()
  }
}
