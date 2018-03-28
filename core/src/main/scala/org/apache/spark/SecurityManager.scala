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

import java.lang.{Byte => JByte}
import java.net.{Authenticator, PasswordAuthentication}
import java.security.{KeyStore, SecureRandom}
import java.security.cert.X509Certificate
import javax.net.ssl._

import com.google.common.hash.HashCodes
import com.google.common.io.Files
import org.apache.hadoop.io.Text

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.network.sasl.SecretKeyHolder
import org.apache.spark.util.Utils

/** Spark class responsible for security.
  * Spark负责安全的类。
  *
  * In general this class should be instantiated by the SparkEnv and most components
  * should access it from that. There are some cases where the SparkEnv hasn't been
  * initialized yet and this class must be instantiated directly.
  *
  * 一般这个类应该由sparkenv大多数组件实例化应该从访问它。也有一些情况下，sparkenv没有初始化的然而这个类必须直接实例化。
  *
  * Spark currently supports authentication via a shared secret.
  * Authentication can be configured to be on via the 'spark.authenticate' configuration
  * parameter. This parameter controls whether the Spark communication protocols do
  * authentication using the shared secret. This authentication is a basic handshake to
  * make sure both sides have the same shared secret and are allowed to communicate.
  * If the shared secret is not identical they will not be allowed to communicate.
  *
  * Spark目前支持通过共享密钥进行身份验证。'spark.authenticate' 此参数控制spark通信协议是否使用共享密钥进行身份验证。
  * 这种身份验证是一种基本的握手，以确保双方具有相同的共享秘密并允许通信。如果共享秘密不相同，则不允许进行通信。
  *
  * The Spark UI can also be secured by using javax servlet filters. A user may want to
  * secure the UI if it has data that other users should not be allowed to see. The javax
  * servlet filter specified by the user can authenticate the user and then once the user
  * is logged in, Spark can compare that user versus the view acls to make sure they are
  * authorized to view the UI. The configs 'spark.acls.enable', 'spark.ui.view.acls' and
  * 'spark.ui.view.acls.groups' control the behavior of the acls. Note that the person who
  * started the application always has view access to the UI.
  *
  * Spark的UI也可以用javax servlet过滤器进行安全过滤。如果用户有其他用户不允许看到的数据，则用户可能希望保护该UI。
  * 由用户指定：Servlet过滤器可以对用户进行身份认证，然后一旦用户登录，Spark可以比较用户与视图的ACL来确保他们有权查看UI。
  * 配置的Spark的'spark.acls.enable', 'spark.ui.view.acls'和'spark.ui.view.acls.groups' 控制的ACL的行为。
  * 注意，启动应用程序的人总是有对UI的视图有访问权限。
  *
  *
  * Spark has a set of individual and group modify acls (`spark.modify.acls`) and
  * (`spark.modify.acls.groups`) that controls which users and groups have permission to
  * modify a single application. This would include things like killing the application.
  * By default the person who started the application has modify access. For modify access
  * through the UI, you must have a filter that does authentication in place for the modify
  * acls to work properly.
  *
  * Spark有一套个人和小组修改ACL的配置(`spark.modify.acls`)和(`spark.modify.acls.groups`)
  * 这些控制哪些用户和组权限修改一个单一的应用。这将包括诸如杀死应用程序之类的事情。默认情况下，启动应用程序的人已经拥有修改访问的权限。
  * 通过用户界面修改访问，你必须有一个过滤器，用于修改ACL正常工作并认证的地方。
  *
  * Spark also has a set of individual and group admin acls (`spark.admin.acls`) and
  * (`spark.admin.acls.groups`) which is a set of users/administrators and admin groups
  * who always have permission to view or modify the Spark application.
  *
  * Spark有一套个人和小组修改ACL的配置(`spark.modify.acls`)和(`spark.modify.acls.groups`)
  * 是一组用户/管理员和管理员组总是有权限查看或修改Spark应用
  *
  * Starting from version 1.3, Spark has partial support for encrypted connections with SSL.
  * 从版本1.3开始，Spark对SSL加密连接有部分支持。
  *
  * At this point spark has multiple communication protocols that need to be secured and
  * different underlying mechanisms are used depending on the protocol:
  *
  * 在这一点上，spark有多个通信协议需要安全，根据协议使用不同的底层机制：
  *
  *
  *  - HTTP for broadcast and file server (via HttpServer) ->  Spark currently uses Jetty
  *            for the HttpServer. Jetty supports multiple authentication mechanisms -
  *            Basic, Digest, Form, Spnego, etc. It also supports multiple different login
  *            services - Hash, JAAS, Spnego, JDBC, etc.  Spark currently uses the HashLoginService
  *            to authenticate using DIGEST-MD5 via a single user and the shared secret.
  *            Since we are using DIGEST-MD5, the shared secret is not passed on the wire
  *            in plaintext.
  *
  *            We currently support SSL (https) for this communication protocol (see the details
  *            below).
  *
  *            The Spark HttpServer installs the HashLoginServer and configures it to DIGEST-MD5.
  *            Any clients must specify the user and password. There is a default
  *            Authenticator installed in the SecurityManager to how it does the authentication
  *            and in this case gets the user name and password from the request.
  *
  *           - HTTP广播和文件服务器（通过HTTP服务器）->Spark目前使用Jetty的HTTP服务器。Jetty支持多种认证机制-Basic,
  *           Digest, Form, Spnego,等等。它还支持多个不同的登录服务 - Hash, JAAS, Spnego, JDBC,等。Spark目前使用的
  *           hashloginservice验证用DIGEST-MD5通过单一用户和共享秘密。由于我们使用的是DIGEST-MD5，秘密不传中明文的线。
  *
  *           我们目前支持此通信协议的SSL（HTTPS）（请参阅下面的详细信息）。
  *
  *           Spar的HttpServer安装在hashloginserver和配置它digest-md5.any客户必须指定用户名和密码。
  *           有一个defaultb认证安装在要如何认证，在这种情况下，从请求获取用户名和密码。
  *
  *
  *  - BlockTransferService -> The Spark BlockTransferServices uses java nio to asynchronously
  *            exchange messages.  For this we use the Java SASL
  *            (Simple Authentication and Security Layer) API and again use DIGEST-MD5
  *            as the authentication mechanism. This means the shared secret is not passed
  *            over the wire in plaintext.
  *            Note that SASL is pluggable as to what mechanism it uses.  We currently use
  *            DIGEST-MD5 but this could be changed to use Kerberos or other in the future.
  *            Spark currently supports "auth" for the quality of protection, which means
  *            the connection does not support integrity or privacy protection (encryption)
  *            after authentication. SASL also supports "auth-int" and "auth-conf" which
  *            SPARK could support in the future to allow the user to specify the quality
  *            of protection they want. If we support those, the messages will also have to
  *            be wrapped and unwrapped via the SaslServer/SaslClient.wrap/unwrap API's.
  *
  *            Since the NioBlockTransferService does asynchronous messages passing, the SASL
  *            authentication is a bit more complex. A ConnectionManager can be both a client
  *            and a Server, so for a particular connection it has to determine what to do.
  *            A ConnectionId was added to be able to track connections and is used to
  *            match up incoming messages with connections waiting for authentication.
  *            The ConnectionManager tracks all the sendingConnections using the ConnectionId,
  *            waits for the response from the server, and does the handshake before sending
  *            the real message.
  *
  *            The NettyBlockTransferService ensures that SASL authentication is performed
  *            synchronously prior to any other communication on a connection. This is done in
  *            SaslClientBootstrap on the client side and SaslRpcHandler on the server side.
  *
  *           - blocktransferservice ->Spark的blocktransferservices使用java NIO异步消息交换。为此我们使用java SASL
  *            （简单身份验证和安全层）的API和再次使用DIGEST-MD5作为身份验证机制。这意味着共享的秘密不会通过明文传输到网上。
  *            请注意，SASL是可插拔为它使用什么样的机制。我们目前使用的DIGEST-MD5但这可以改变，未来使用Kerberos或其他。
  *            目前支持“auth”的品质保障，这意味着连接不支持完整性或隐私保护（加密）认证后。SASL也支持“auth-int”和“auth-conf”。
  *            SPARK可能在未来支持允许用户指定他们想要的品质保障。如果我们支持这些信息也将被打开通过SaslServer/SaslClient.wrap/unwrap的
  *            API。
  *
  *            由于nioblocktransferservice是异步消息传递，SASL认证是一个更复杂。一个连接管理器可以是一个客户端和一个服务器，
  *            所以对于一个特定的连接已经确定要做什么。一个connectionid添加能够跟踪连接，用于连接等待验证传入消息匹配。
  *            该连接管理器跟踪所有的sendingconnections使用ConnectionID，等待来自服务器的响应，并握手前发送真实信息。
  *
  *            nettyblocktransferservice确保SASL认证同步进行在连接任何其他沟通前。这是在客户端的SaslClientBootstrap
  *            和服务器端的SaslRpcHandler中完成的。
  *
  *  - HTTP for the Spark UI -> the UI was changed to use servlets so that javax servlet filters
  *            can be used. Yarn requires a specific AmIpFilter be installed for security to work
  *            properly. For non-Yarn deployments, users can write a filter to go through their
  *            organization's normal login service. If an authentication filter is in place then the
  *            SparkUI can be configured to check the logged in user against the list of users who
  *            have view acls to see if that user is authorized.
  *            The filters can also be used for many different purposes. For instance filters
  *            could be used for logging, encryption, or compression.
  *
  *           - HTTP for the Spark UI ->  UI改为使用servlet以便于servlet过滤器可以使用。Yarn需要特定的amipfilter安装为了能在work
  *           节点上安全的运行。对于non-Yarn部署，用户可以编写过滤器以通过组织的正常登录服务。
  *           如果认证过滤器是在解放军sparkui可以被配置为检查登录用户对列表视图是否有ACL，用户授权，过滤器也可用于许多不同的目的。
  *           例如文件可用于logging、加密或压缩。
  *
  *  The exact mechanisms used to generate/distribute the shared secret are deployment-specific.
  *  用于生成/分发共享秘密的确切机制是部署特定的。
  *
  *  For YARN deployments, the secret is automatically generated. The secret is placed in the Hadoop
  *  UGI which gets passed around via the Hadoop RPC mechanism. Hadoop RPC can be configured to
  *  support different levels of protection. See the Hadoop documentation for more details. Each
  *  Spark application on YARN gets a different shared secret.
  *
  *  对于YARN部署模式，验证秘钥是自动生成的。验证秘钥被放在Hadoop UGI中，它是通过Hadoop RPC机制传递的。
  *  Hadoop RPC可以配置为支持不同级别的保护。有关详细信息，请参见Hadoop文档。Spark上的每个Spark应用程序获得不同的共享秘密。
  *
  *
  *  On YARN, the Spark UI gets configured to use the Hadoop YARN AmIpFilter which requires the user
  *  to go through the ResourceManager Proxy. That proxy is there to reduce the possibility of web
  *  based attacks through YARN. Hadoop can be configured to use filters to do authentication. That
  *  authentication then happens via the ResourceManager Proxy and Spark will use that to do
  *  authorization against the view acls.
  *
  *  对YARN，Spark的UI被配置为使用Hadoop的YARN amipfilter要求用户通过ResourceManager代理。该代理可以减少通过YARN
  *  进行基于Web的攻击的可能性。Hadoop可以配置为使用筛选器进行身份验证。然后，
  *  认证是通过ResourceManager代理和Spark会用它来做授权对视图的ACL。
  *
  *
  *  For other Spark deployments, the shared secret must be specified via the
  *  spark.authenticate.secret config.
  *  All the nodes (Master and Workers) and the applications need to have the same shared secret.
  *  This again is not ideal as one user could potentially affect another users application.
  *  This should be enhanced in the future to provide better protection.
  *  If the UI needs to be secure, the user needs to install a javax servlet filter to do the
  *  authentication. Spark will then use that user to compare against the view acls to do
  *  authorization. If not filter is in place the user is generally null and no authorization
  *  can take place.
  *
  * 其他Spark的部署，共享密钥必须通过指定的spark.authenticate.secret配置。所有的节点（Master和Workers）
  * 和应用程序都需要具有相同的共享密钥。这又不理想，因为一个用户可能会影响另一个用户应用程序。今后应加强这方面的工作，以提供更好的保护。
  * 如果用户需要的是UI安全的，用户需要安装：Servlet过滤器做认证。Spark将使用的用户比较，查看ACL做授权。
  * 如果没有筛选器，则用户通常是空的，不能进行授权。
  *
  *
  *  When authentication is being used, encryption can also be enabled by setting the option
  *  spark.authenticate.enableSaslEncryption to true. This is only supported by communication
  *  channels that use the network-common library, and can be used as an alternative to SSL in those
  *  cases.
  *
  *  当正在使用的加密认证，也可以通过设置选项spark.authenticate.enablesaslencryption真正启用。
  *  只有在使用网络公共库的通信通道的支持下，才能在这些情况下作为SSL的替代品。
  *
  *
  *  SSL can be used for encryption for certain communication channels. The user can configure the
  *  default SSL settings which will be used for all the supported communication protocols unless
  *  they are overwritten by protocol specific settings. This way the user can easily provide the
  *  common settings for all the protocols without disabling the ability to configure each one
  *  individually.
  *
  *  SSL可以用于某些通信信道的加密。用户可以配置默认的SSL设置，用于所有支持的通信协议，除非它们被协议特定的设置覆盖。
  *  通过这种方式，用户可以轻松地为所有协议提供公共设置，而不禁用单独配置每个协议的能力。
  *
  *
  *  All the SSL settings like `spark.ssl.xxx` where `xxx` is a particular configuration property,
  *  denote the global configuration for all the supported protocols. In order to override the global
  *  configuration for the particular protocol, the properties must be overwritten in the
  *  protocol-specific namespace. Use `spark.ssl.yyy.xxx` settings to overwrite the global
  *  configuration for particular protocol denoted by `yyy`. Currently `yyy` can be only`fs` for
  *  broadcast and file server.
  *
  *  所有的SSL设置，比如`spark.ssl.xxx` ，其中xxx是一个特定的配置属性，表示所有支持的协议的全局配置。为了覆盖特定协议的全局配置，
  *  必须在特定于协议的命名空间中重写属性。使用`spark.ssl.yyy.xxx` 设置覆盖全球配置为特定的协议表示` YYY `。
  *  目前` YYY `只能` FS `广播和文件服务器。
  *
  *
  *  Refer to [[org.apache.spark.SSLOptions]] documentation for the list of
  *  options that can be specified.
  *
  * 参考[[org.apache.spark.SSLOptions]]文件的选项，可以指定列表。
  *
  *  SecurityManager initializes SSLOptions objects for different protocols separately. SSLOptions
  *  object parses Spark configuration at a given namespace and builds the common representation
  *  of SSL settings. SSLOptions is then used to provide protocol-specific SSLContextFactory for
  *  Jetty.
  *
  *  SecurityManager要初始化SSLOptions对象为了支持不同协议。ssloptions对象分析Spark配置在一个给定的命名空间并建立SSL
  *  设置。ssloptions共同表示然后用来提供特定协议的sslcontextfactory  Jetty。
  *
  *
  *  SSL must be configured on each node and configured for each component involved in
  *  communication using the particular protocol. In YARN clusters, the key-store can be prepared on
  *  the client side then distributed and used by the executors as the part of the application
  *  (YARN allows the user to deploy files before the application is started).
  *  In standalone deployment, the user needs to provide key-stores and configuration
  *  options for master and workers. In this mode, the user may allow the executors to use the SSL
  *  settings inherited from the worker which spawned that executor. It can be accomplished by
  *  setting `spark.ssl.useNodeLocalConf` to `true`.
  *
  *  必须在每个节点上配置SSL，并使用特定协议为通信中涉及的每个组件进行配置。在YARN集群下，key-store可以在客户端准备好，然后分发出去
  *  并且作为应用程序的一部分被executors使用，（YARN允许用户配置文件在启动应用程序之前）。
  *
  *  独立部署，用户需要提供 key-stores以及master和workers的配置选项。在这种模式下，用户可以允许执行器使用从生成该执行器的人员继承的SSL设置。
  *  它可以通过设置 `spark.ssl.useNodeLocalConf`为 `true`来实现。
  *
  *
  *
  *  安全管理器SecurityManager：
  *     SecurityManager主要对权限，账号进行设置，如果使用Hadoop Yarn作为集群管理器，则需要使用证书生成secret key进行登录，
  *   最后给当前系统设置默认的口令认证实例，此实例采用内部类实现。
  *
  *   官网请查看：http://spark.apache.org/docs/latest/security.html
  *
  */

private[spark] class SecurityManager(
                                      sparkConf: SparkConf,
                                      val ioEncryptionKey: Option[Array[Byte]] = None)
  extends Logging with SecretKeyHolder {

  import SecurityManager._

  // allow all users/groups to have view/modify permissions
  // 允许用户/组拥有查看/修改的权限
  private val WILDCARD_ACL = "*" // ACL权限通配符（wildcard的意思通配符）

  // 这里配置的是这个属性spark.authenticate，默认为false
  private val authOn = sparkConf.get(NETWORK_AUTH_ENABLED)
  // keep spark.ui.acls.enable for backwards compatibility with 1.0
  private var aclsOn =
    sparkConf.getBoolean("spark.acls.enable", sparkConf.getBoolean("spark.ui.acls.enable", false))

  // admin acls should be set before view or modify acls
  // Admin ACL前应设置查看或修改ACL
  private var adminAcls: Set[String] =
  stringToSet(sparkConf.get("spark.admin.acls", ""))

  // admin group acls should be set before view or modify group acls
  // DMIN组ACL前应设置查看或修改组的ACL
  private var adminAclsGroups : Set[String] =
  stringToSet(sparkConf.get("spark.admin.acls.groups", ""))

  private var viewAcls: Set[String] = _

  private var viewAclsGroups: Set[String] = _

  // list of users who have permission to modify the application. This should
  // apply to both UI and CLI for things like killing the application.
  // 具有修改应用程序权限的用户列表。这应该适用于UI和CLI等用于杀死应用程序的东西。
  private var modifyAcls: Set[String] = _

  private var modifyAclsGroups: Set[String] = _

  // always add the current user and SPARK_USER to the viewAcls
  // 随时添加当前用户和spark_user的viewacls
  private val defaultAclUsers = Set[String](System.getProperty("user.name", ""),
    Utils.getCurrentUserName()) // TODO:调试注释  defaultAclUsers:"Set$Set2" size =2  ,这里获取了 0=“hzjs” 1="root"

  setViewAcls(defaultAclUsers, sparkConf.get("spark.ui.view.acls", ""))
  setModifyAcls(defaultAclUsers, sparkConf.get("spark.modify.acls", ""))

  setViewAclsGroups(sparkConf.get("spark.ui.view.acls.groups", ""));
  setModifyAclsGroups(sparkConf.get("spark.modify.acls.groups", ""));

  // 想生成secretKey必须spark.authenticate=true，而且要在yarn模式下运行
  private val secretKey = generateSecretKey()
  // 17/12/05 11:56:50 INFO SecurityManager: SecurityManager: authentication disabled;
  // ui acls disabled;
  // users  with view permissions: Set(hzjs, root);
  // groups with view permissions: Set();
  // users  with modify permissions: Set(hzjs, root);
  // groups with modify permissions: Set()
  logInfo("SecurityManager: authentication " + (if (authOn) "enabled" else "disabled") +
    "; ui acls " + (if (aclsOn) "enabled" else "disabled") +
    "; users  with view permissions: " + viewAcls.toString() +
    "; groups with view permissions: " + viewAclsGroups.toString() +
    "; users  with modify permissions: " + modifyAcls.toString() +
    "; groups with modify permissions: " + modifyAclsGroups.toString())

  // Set our own authenticator to properly negotiate user/password for HTTP connections.
  // This is needed by the HTTP client fetching from the HttpServer. Put here so its
  // only set once.
  // 使用HTTP连接设置口令认证
  // 设定自己的验证器妥善协商HTTP连接/密码的用户。这是从HTTP服务器获取HTTP客户端的需要。把它放在这里，它只被设置一次。
  // 注意这一段话，必须设置spark.authenticate为true，但是设置了这个，如果不是yarn模式运行会报错
  if (authOn) {
    Authenticator.setDefault(
      new Authenticator() {
        override def getPasswordAuthentication(): PasswordAuthentication = {
          var passAuth: PasswordAuthentication = null
          val userInfo = getRequestingURL().getUserInfo()
          if (userInfo != null) {
            val  parts = userInfo.split(":", 2)
            passAuth = new PasswordAuthentication(parts(0), parts(1).toCharArray())
          }
          return passAuth
        }
      }
    )
  }

  // the default SSL configuration - it will be used by all communication layers unless overwritten
  private val defaultSSLOptions = SSLOptions.parse(sparkConf, "spark.ssl", defaults = None)

  // SSL configuration for the file server. This is used by Utils.setupSecureURLConnection().
  val fileServerSSLOptions = getSSLOptions("fs")
  val (sslSocketFactory, hostnameVerifier) = if (fileServerSSLOptions.enabled) {
    val trustStoreManagers =
      for (trustStore <- fileServerSSLOptions.trustStore) yield {
        val input = Files.asByteSource(fileServerSSLOptions.trustStore.get).openStream()

        try {
          val ks = KeyStore.getInstance(KeyStore.getDefaultType)
          ks.load(input, fileServerSSLOptions.trustStorePassword.get.toCharArray)

          val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
          tmf.init(ks)
          tmf.getTrustManagers
        } finally {
          input.close()
        }
      }

    lazy val credulousTrustStoreManagers = Array({
      logWarning("Using 'accept-all' trust manager for SSL connections.")
      new X509TrustManager {
        override def getAcceptedIssuers: Array[X509Certificate] = null

        override def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String) {}

        override def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String) {}
      }: TrustManager
    })

    require(fileServerSSLOptions.protocol.isDefined,
      "spark.ssl.protocol is required when enabling SSL connections.")

    val sslContext = SSLContext.getInstance(fileServerSSLOptions.protocol.get)
    sslContext.init(null, trustStoreManagers.getOrElse(credulousTrustStoreManagers), null)

    val hostVerifier = new HostnameVerifier {
      override def verify(s: String, sslSession: SSLSession): Boolean = true
    }

    (Some(sslContext.getSocketFactory), Some(hostVerifier))
  } else {
    (None, None)
  }

  def getSSLOptions(module: String): SSLOptions = {
    val opts = SSLOptions.parse(sparkConf, s"spark.ssl.$module", Some(defaultSSLOptions))
    logDebug(s"Created SSL options for $module: $opts")
    opts
  }

  /**
    * Split a comma separated String, filter out any empty items, and return a Set of strings
    * 拆分一个逗号分隔的字符串，过滤掉任何空项，并返回一组字符串。
    */
  private def stringToSet(list: String): Set[String] = {
    list.split(',').map(_.trim).filter(!_.isEmpty).toSet
  }

  /**
    * Admin acls should be set before the view or modify acls.  If you modify the admin
    * acls you should also set the view and modify acls again to pick up the changes.
    *
    * Admin ACL前应设置查看或修改ACL。如果你修改你还应设置查看和修改ACL再次拿起变化管理ACL。
    *
    */
  def setViewAcls(defaultUsers: Set[String], allowedUsers: String) {
    viewAcls = (adminAcls ++ defaultUsers ++ stringToSet(allowedUsers))
    // 2/05 11:56:50 INFO SecurityManager: Changing view acls to: hzjs,root
    logInfo("Changing view acls to: " + viewAcls.mkString(","))
  }

  def setViewAcls(defaultUser: String, allowedUsers: String) {
    setViewAcls(Set[String](defaultUser), allowedUsers)
  }

  /**
    * Admin acls groups should be set before the view or modify acls groups. If you modify the admin
    * acls groups you should also set the view and modify acls groups again to pick up the changes.
    *
    * Admin ACL组前应设置查看或修改ACL组。如果你修改admin ACL组还应该设置查看和修改ACL组再次拿起了变化。
    *
    *
    */
  def setViewAclsGroups(allowedUserGroups: String) {
    viewAclsGroups = (adminAclsGroups ++ stringToSet(allowedUserGroups));
    // 17/12/05 11:56:50 INFO SecurityManager: Changing view acls groups to:
    logInfo("Changing view acls groups to: " + viewAclsGroups.mkString(","))
  }

  /**
    * Checking the existence of "*" is necessary as YARN can't recognize the "*" in "defaultuser,*"
    *
    * 检查是否存在“*”是必要的YARN不承认“*”中的"defaultuser,*"
    */
  def getViewAcls: String = {
    if (viewAcls.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      viewAcls.mkString(",")
    }
  }

  def getViewAclsGroups: String = {
    if (viewAclsGroups.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      viewAclsGroups.mkString(",")
    }
  }

  /**
    * Admin acls should be set before the view or modify acls.  If you modify the admin
    * acls you should also set the view and modify acls again to pick up the changes.
    *
    * Admin ACL前应设置查看或修改ACL。如果你修改你还应设置查看和修改ACL再次拿起变化管理ACL。
    */
  def setModifyAcls(defaultUsers: Set[String], allowedUsers: String) {
    modifyAcls = (adminAcls ++ defaultUsers ++ stringToSet(allowedUsers))
    // 17/12/05 11:56:50 INFO SecurityManager: Changing modify acls to: hzjs,root
    logInfo("Changing modify acls to: " + modifyAcls.mkString(","))
  }

  /**
    * Admin acls groups should be set before the view or modify acls groups. If you modify the admin
    * acls groups you should also set the view and modify acls groups again to pick up the changes.
    *
    * Admin ACL组前应设置查看或修改ACL组。如果你修改admin ACL组还应该设置查看和修改ACL组再次拿起了变化。
    *
    */
  def setModifyAclsGroups(allowedUserGroups: String) {
    modifyAclsGroups = (adminAclsGroups ++ stringToSet(allowedUserGroups));
    // 17/12/05 11:56:50 INFO SecurityManager: Changing modify acls groups to:
    logInfo("Changing modify acls groups to: " + modifyAclsGroups.mkString(","))
  }

  /**
    * Checking the existence of "*" is necessary as YARN can't recognize the "*" in "defaultuser,*"
    */
  def getModifyAcls: String = {
    if (modifyAcls.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      modifyAcls.mkString(",")
    }
  }

  def getModifyAclsGroups: String = {
    if (modifyAclsGroups.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      modifyAclsGroups.mkString(",")
    }
  }

  /**
    * Admin acls should be set before the view or modify acls.  If you modify the admin
    * acls you should also set the view and modify acls again to pick up the changes.
    */
  def setAdminAcls(adminUsers: String) {
    adminAcls = stringToSet(adminUsers)
    logInfo("Changing admin acls to: " + adminAcls.mkString(","))
  }

  /**
    * Admin acls groups should be set before the view or modify acls groups. If you modify the admin
    * acls groups you should also set the view and modify acls groups again to pick up the changes.
    */
  def setAdminAclsGroups(adminUserGroups: String) {
    adminAclsGroups = stringToSet(adminUserGroups)
    logInfo("Changing admin acls groups to: " + adminAclsGroups.mkString(","))
  }

  def setAcls(aclSetting: Boolean) {
    aclsOn = aclSetting
    logInfo("Changing acls enabled to: " + aclsOn)
  }

  def getIOEncryptionKey(): Option[Array[Byte]] = ioEncryptionKey

  /**
    * Generates or looks up the secret key.
    *
    *  生成或查找密钥。
    *
    * The way the key is stored depends on the Spark deployment mode. Yarn
    * uses the Hadoop UGI.
    *
    * 密钥存储方式取决于Spark部署模式。Yarn采用Hadoop UGI。
    *
    * For non-Yarn deployments, If the config variable is not set
    * we throw an exception.
    *
    * 对于non-Yarn部署模式，如果配置变量没有设置，我们将抛出一个异常。
    */
  private def generateSecretKey(): String = {
    if (!isAuthenticationEnabled) {
      null
    } else if (SparkHadoopUtil.get.isYarnMode) {
      // In YARN mode, the secure cookie will be created by the driver and stashed in the
      // user's credentials, where executors can get it. The check for an array of size 0
      // is because of the test code in YarnSparkHadoopUtilSuite.
      val secretKey = SparkHadoopUtil.get.getSecretKeyFromUserCredentials(SECRET_LOOKUP_KEY)  //TODO: secretKey:null
      if (secretKey == null || secretKey.length == 0) {
        logDebug("generateSecretKey: yarn mode, secret key from credentials is null")
        val rnd = new SecureRandom()
        val length = sparkConf.getInt("spark.authenticate.secretBitLength", 256) / JByte.SIZE //TODO: length:32
        val secret = new Array[Byte](length)
        rnd.nextBytes(secret)

        val cookie = HashCodes.fromBytes(secret).toString() //TODO: cookie:"wery7237rwuhr732582irwberyu238grfuyewgr78....."
        SparkHadoopUtil.get.addSecretKeyToUserCredentials(SECRET_LOOKUP_KEY, cookie)
        cookie
      } else {
        new Text(secretKey).toString
      }
    } else {
      // user must have set spark.authenticate.secret config
      // For Master/Worker, auth secret is in conf; for Executors, it is in env variable
      Option(sparkConf.getenv(SecurityManager.ENV_AUTH_SECRET))
        .orElse(sparkConf.getOption(SecurityManager.SPARK_AUTH_SECRET_CONF)) match {
        case Some(value) => value
        case None =>
          throw new IllegalArgumentException(
            "Error: a secret key must be specified via the " +
              SecurityManager.SPARK_AUTH_SECRET_CONF + " config")
      }
    }
  }

  /**
    * Check to see if Acls for the UI are enabled
    * @return true if UI authentication is enabled, otherwise false
    */
  def aclsEnabled(): Boolean = aclsOn

  /**
    * Checks the given user against the view acl and groups list to see if they have
    * authorization to view the UI. If the UI acls are disabled
    * via spark.acls.enable, all users have view access. If the user is null
    * it is assumed authentication is off and all users have access. Also if any one of the
    * UI acls or groups specify the WILDCARD(*) then all users have view access.
    *
    * @param user to see if is authorized
    * @return true is the user has permission, otherwise false
    */
  def checkUIViewPermissions(user: String): Boolean = {
    logDebug("user=" + user + " aclsEnabled=" + aclsEnabled() + " viewAcls=" +
      viewAcls.mkString(",") + " viewAclsGroups=" + viewAclsGroups.mkString(","))
    if (!aclsEnabled || user == null || viewAcls.contains(user) ||
      viewAcls.contains(WILDCARD_ACL) || viewAclsGroups.contains(WILDCARD_ACL)) {
      return true
    }
    val currentUserGroups = Utils.getCurrentUserGroups(sparkConf, user)
    logDebug("userGroups=" + currentUserGroups.mkString(","))
    viewAclsGroups.exists(currentUserGroups.contains(_))
  }

  /**
    * Checks the given user against the modify acl and groups list to see if they have
    * authorization to modify the application. If the modify acls are disabled
    * via spark.acls.enable, all users have modify access. If the user is null
    * it is assumed authentication isn't turned on and all users have access. Also if any one
    * of the modify acls or groups specify the WILDCARD(*) then all users have modify access.
    *
    * @param user to see if is authorized
    * @return true is the user has permission, otherwise false
    */
  def checkModifyPermissions(user: String): Boolean = {
    logDebug("user=" + user + " aclsEnabled=" + aclsEnabled() + " modifyAcls=" +
      modifyAcls.mkString(",") + " modifyAclsGroups=" + modifyAclsGroups.mkString(","))
    if (!aclsEnabled || user == null || modifyAcls.contains(user) ||
      modifyAcls.contains(WILDCARD_ACL) || modifyAclsGroups.contains(WILDCARD_ACL)) {
      return true
    }
    val currentUserGroups = Utils.getCurrentUserGroups(sparkConf, user)
    logDebug("userGroups=" + currentUserGroups)
    modifyAclsGroups.exists(currentUserGroups.contains(_))
  }

  /**
    * Check to see if authentication for the Spark communication protocols is enabled
    * 检查是否启用了Spark通信协议的身份验证。
    * @return true if authentication is enabled, otherwise false
    */
  def isAuthenticationEnabled(): Boolean = authOn

  /**
    * Checks whether network encryption should be enabled.
    * 检查是否应启用网络加密。
    * @return Whether to enable encryption when connecting to services that support it.
    */
  def isEncryptionEnabled(): Boolean = {
    sparkConf.get(NETWORK_ENCRYPTION_ENABLED) || sparkConf.get(SASL_ENCRYPTION_ENABLED)
  }

  /**
    * Gets the user used for authenticating HTTP connections.
    * For now use a single hardcoded user.
    * 获取用于身份验证的HTTP连接的用户，现在使用一个硬编码的用户。
    *
    * @return the HTTP user as a String
    */
  def getHttpUser(): String = "sparkHttpUser"

  /**
    * Gets the user used for authenticating SASL connections.
    * For now use a single hardcoded user.
    * 获取用于验证SASL连接用户。现在使用一个硬编码的用户。
    * @return the SASL user as a String
    */
  def getSaslUser(): String = "sparkSaslUser"

  /**
    * Gets the secret key.
    * 获取密钥。
    * @return the secret key as a String if authentication is enabled, otherwise returns null
    */
  def getSecretKey(): String = secretKey

  // Default SecurityManager only has a single secret key, so ignore appId.
  override def getSaslUser(appId: String): String = getSaslUser()
  override def getSecretKey(appId: String): String = getSecretKey()
}

private[spark] object SecurityManager {

  val SPARK_AUTH_CONF: String = "spark.authenticate"
  val SPARK_AUTH_SECRET_CONF: String = "spark.authenticate.secret"
  // This is used to set auth secret to an executor's env variable. It should have the same
  // value as SPARK_AUTH_SECRET_CONF set in SparkConf
  val ENV_AUTH_SECRET = "_SPARK_AUTH_SECRET"

  // key used to store the spark secret in the Hadoop UGI
  val SECRET_LOOKUP_KEY = "sparkCookie"

}
