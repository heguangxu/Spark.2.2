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

package org.apache.spark.network.client;

import io.netty.channel.Channel;

/**
 * A bootstrap which is executed on a TransportClient before it is returned to the user.
 * This enables an initial exchange of information (e.g., SASL authentication tokens) on a once-per-
 * connection basis.
 *
 * 在返回给用户之前，在传输客户机上执行的引导程序。这允许在一次连接的基础上初始交换信息
 * (例如SASL认证令牌)。
 *
 * Since connections (and TransportClients) are reused as much as possible, it is generally
 * reasonable to perform an expensive bootstrapping operation, as they often share a lifespan with
 * the JVM itself.
 *
 * 由于连接(和传输客户机)尽可能多地重用，所以执行一个昂贵的引导操作通常是合理的，因为它们通常与JVM本身共享一个生命周期。
 */
public interface TransportClientBootstrap {
  /** Performs the bootstrapping operation, throwing an exception on failure.
   *  执行引导操作，对失败抛出异常。
   * */
  void doBootstrap(TransportClient client, Channel channel) throws RuntimeException;
}
