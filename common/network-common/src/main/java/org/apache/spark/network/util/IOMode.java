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

package org.apache.spark.network.util;

/**
 * Selector for which form of low-level IO we should use.
 * NIO is always available, while EPOLL is only available on Linux.
 * AUTO is used to select EPOLL if it's available, or NIO otherwise.
 *
 * 我们应该使用的低级别IO的选择器。
 *
 * NIO总是可用的，而EPOLL只在Linux上可用。
 *
 * 如果它是可用的，默认选择EPOLL否则NIO。
 *
 * Spark为何使用Netty通信框架替代Akka：http://www.aboutyun.com/thread-21115-1-1.html
 */
public enum IOMode {
  NIO, EPOLL
}
