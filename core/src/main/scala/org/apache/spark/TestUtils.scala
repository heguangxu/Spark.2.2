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

package org.apache.spark

import java.io.{ByteArrayInputStream, File, FileInputStream, FileOutputStream}
import java.net.{HttpURLConnection, URI, URL}
import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.util.Arrays
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.jar.{JarEntry, JarOutputStream}
import javax.net.ssl._
import javax.tools.{JavaFileObject, SimpleJavaFileObject, ToolProvider}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Try

import com.google.common.io.{ByteStreams, Files}

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.util.Utils

/**
 * Utilities for tests. Included in main codebase since it's used by multiple
 * projects.
  * 测试工具。因为它是由多个项目，包括在主代码库。
 *
 * TODO: See if we can move this to the test codebase by specifying
 * test dependencies between projects.
  *
  * 待办事项：看看我们是否可以将这个测试代码通过指定测试项目之间的依赖关系。
 */
private[spark] object TestUtils {

  /**
   * Create a jar that defines classes with the given names.
    * 创建一个定义给定名称的类的JAR。
   *
   * Note: if this is used during class loader tests, class names should be unique
   * in order to avoid interference between tests.
    *
    * 注意：如果在类装入器测试中使用此项，类名应是唯一的，以避免测试之间的干扰。
    *
   */
  def createJarWithClasses(
      classNames: Seq[String],
      toStringValue: String = "",
      classNamesWithBase: Seq[(String, String)] = Seq(),
      classpathUrls: Seq[URL] = Seq()): URL = {
    val tempDir = Utils.createTempDir()
    val files1 = for (name <- classNames) yield {
      createCompiledClass(name, tempDir, toStringValue, classpathUrls = classpathUrls)
    }
    val files2 = for ((childName, baseName) <- classNamesWithBase) yield {
      createCompiledClass(childName, tempDir, toStringValue, baseName, classpathUrls)
    }
    val jarFile = new File(tempDir, "testJar-%s.jar".format(System.currentTimeMillis()))
    createJar(files1 ++ files2, jarFile)
  }

  /**
   * Create a jar file containing multiple files. The `files` map contains a mapping of
   * file names in the jar file to their contents.
    * 创建一个包含讯多文件的jar包，这个文件包含一个文件名的映射
   */
  def createJarWithFiles(files: Map[String, String], dir: File = null): URL = {
    val tempDir = Option(dir).getOrElse(Utils.createTempDir())
    val jarFile = File.createTempFile("testJar", ".jar", tempDir)
    val jarStream = new JarOutputStream(new FileOutputStream(jarFile))
    files.foreach { case (k, v) =>
      val entry = new JarEntry(k)
      jarStream.putNextEntry(entry)
      ByteStreams.copy(new ByteArrayInputStream(v.getBytes(StandardCharsets.UTF_8)), jarStream)
    }
    jarStream.close()
    jarFile.toURI.toURL
  }

  /**
   * Create a jar file that contains this set of files. All files will be located in the specified
   * directory or at the root of the jar.
    * 包含一个jar文件包含一个set集合文件，所有文件将位于指定目录或JAR的根目录中。
   */
  def createJar(files: Seq[File], jarFile: File, directoryPrefix: Option[String] = None): URL = {
    val jarFileStream = new FileOutputStream(jarFile)
    val jarStream = new JarOutputStream(jarFileStream, new java.util.jar.Manifest())

    for (file <- files) {
      // The `name` for the argument in `JarEntry` should use / for its separator. This is
      // ZIP specification.
      // JarEntry的名称应该使用/分割 这是一个ZIP规范
      val prefix = directoryPrefix.map(d => s"$d/").getOrElse("")
      val jarEntry = new JarEntry(prefix + file.getName)
      jarStream.putNextEntry(jarEntry)

      val in = new FileInputStream(file)
      ByteStreams.copy(in, jarStream)
      in.close()
    }
    jarStream.close()
    jarFileStream.close()

    jarFile.toURI.toURL
  }

  // Adapted from the JavaCompiler.java doc examples    改编自JavaCompiler.java 的例子
  private val SOURCE = JavaFileObject.Kind.SOURCE
  private def createURI(name: String) = {
    URI.create(s"string:///${name.replace(".", "/")}${SOURCE.extension}")
  }

  private[spark] class JavaSourceFromString(val name: String, val code: String)
    extends SimpleJavaFileObject(createURI(name), SOURCE) {
    override def getCharContent(ignoreEncodingErrors: Boolean): String = code
  }

  /** Creates a compiled class with the source file. Class file will be placed in destDir.
    * 用源文件创建编译的类。类文件将被放置在DestDir。
    * */
  def createCompiledClass(
      className: String,
      destDir: File,
      sourceFile: JavaSourceFromString,
      classpathUrls: Seq[URL]): File = {
    val compiler = ToolProvider.getSystemJavaCompiler

    // Calling this outputs a class file in pwd. It's easier to just rename the files than
    // build a custom FileManager that controls the output location.
    // 调用此输出密码类文件。它只是重命名文件比建立一个自定义的文件管理器，控制输出的位置更容易。
    val options = if (classpathUrls.nonEmpty) {
      Seq("-classpath", classpathUrls.map { _.getFile }.mkString(File.pathSeparator))
    } else {
      Seq()
    }
    compiler.getTask(null, null, null, options.asJava, null, Arrays.asList(sourceFile)).call()

    val fileName = className + ".class"
    val result = new File(fileName)
    assert(result.exists(), "Compiled file not found: " + result.getAbsolutePath())
    val out = new File(destDir, fileName)

    // renameTo cannot handle in and out files in different filesystems
    // use google's Files.move instead
    // renameTo不能在不同的文件系统中处理在现在使用谷歌的files.move代替
    Files.move(result, out)

    assert(out.exists(), "Destination file not moved: " + out.getAbsolutePath())
    out
  }

  /** Creates a compiled class with the given name. Class file will be placed in destDir.
    * 创建具有给定名称的编译类。类文件将被放置在DestDir。
    * */
  def createCompiledClass(
      className: String,
      destDir: File,
      toStringValue: String = "",
      baseClass: String = null,
      classpathUrls: Seq[URL] = Seq()): File = {
    val extendsText = Option(baseClass).map { c => s" extends ${c}" }.getOrElse("")
    val sourceFile = new JavaSourceFromString(className,
      "public class " + className + extendsText + " implements java.io.Serializable {" +
      "  @Override public String toString() { return \"" + toStringValue + "\"; }}")
    createCompiledClass(className, destDir, sourceFile, classpathUrls)
  }

  /**
   * Run some code involving jobs submitted to the given context and assert that the jobs spilled.
    * 运行一些代码，其中包括提交到给定上下文的jobs，并断言jobs溢出。
   */
  def assertSpilled[T](sc: SparkContext, identifier: String)(body: => T): Unit = {
    val spillListener = new SpillListener
    sc.addSparkListener(spillListener)
    body
    assert(spillListener.numSpilledStages > 0, s"expected $identifier to spill, but did not")
  }

  /**
   * Run some code involving jobs submitted to the given context and assert that the jobs
   * did not spill.
    *
    * 运行一些代码，其中包括提交到给定上下文的jobs，并断言jobs溢出。
   */
  def assertNotSpilled[T](sc: SparkContext, identifier: String)(body: => T): Unit = {
    val spillListener = new SpillListener
    sc.addSparkListener(spillListener)
    body
    assert(spillListener.numSpilledStages == 0, s"expected $identifier to not spill, but did")
  }

  /**
   * Test if a command is available.    测试是否有可用的命令。
   */
  def testCommandAvailable(command: String): Boolean = {
    val attempt = Try(Process(command).run(ProcessLogger(_ => ())).exitValue())
    attempt.isSuccess && attempt.get == 0
  }

  /**
   * Returns the response code from an HTTP(S) URL.     从HTTP（URL）返回响应代码。
   */
  def httpResponseCode(
      url: URL,
      method: String = "GET",
      headers: Seq[(String, String)] = Nil): Int = {
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod(method)
    headers.foreach { case (k, v) => connection.setRequestProperty(k, v) }

    // Disable cert and host name validation for HTTPS tests.
    if (connection.isInstanceOf[HttpsURLConnection]) {
      val sslCtx = SSLContext.getInstance("SSL")
      val trustManager = new X509TrustManager {
        override def getAcceptedIssuers(): Array[X509Certificate] = null
        override def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String) {}
        override def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String) {}
      }
      val verifier = new HostnameVerifier() {
        override def verify(hostname: String, session: SSLSession): Boolean = true
      }
      sslCtx.init(null, Array(trustManager), new SecureRandom())
      connection.asInstanceOf[HttpsURLConnection].setSSLSocketFactory(sslCtx.getSocketFactory())
      connection.asInstanceOf[HttpsURLConnection].setHostnameVerifier(verifier)
    }

    try {
      connection.connect()
      connection.getResponseCode()
    } finally {
      connection.disconnect()
    }
  }

}


/**
 * A `SparkListener` that detects whether spills have occurred in Spark jobs.
  *  一个 `SparkListener`检测Spark jobs的泄露和发生
 */
private class SpillListener extends SparkListener {
  private val stageIdToTaskMetrics = new mutable.HashMap[Int, ArrayBuffer[TaskMetrics]]
  private val spilledStageIds = new mutable.HashSet[Int]
  private val stagesDone = new CountDownLatch(1)

  def numSpilledStages: Int = {
    // Long timeout, just in case somehow the job end isn't notified.
    // Fails if a timeout occurs
    // 长时间超时，以防万一没有通知job结束 如果超时发生失败。
    assert(stagesDone.await(10, TimeUnit.SECONDS))
    spilledStageIds.size
  }

  // 当Task结束的时候
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    stageIdToTaskMetrics.getOrElseUpdate(
      taskEnd.stageId, new ArrayBuffer[TaskMetrics]) += taskEnd.taskMetrics
  }

  // 当Stage完成的时候
  override def onStageCompleted(stageComplete: SparkListenerStageCompleted): Unit = {
    val stageId = stageComplete.stageInfo.stageId
    val metrics = stageIdToTaskMetrics.remove(stageId).toSeq.flatten
    val spilled = metrics.map(_.memoryBytesSpilled).sum > 0
    if (spilled) {
      spilledStageIds += stageId
    }
  }

  //  当job完成的时候
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    stagesDone.countDown()
  }
}
