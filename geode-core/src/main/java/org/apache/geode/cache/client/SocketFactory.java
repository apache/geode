/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.cache.client;

import java.io.IOException;
import java.net.Socket;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.net.SSLParameterExtension;

/**
 * A socket factory used to create sockets from a client to locators or servers.
 *
 *
 * Sockets returned by this factory will have the rest of the configuration options
 * specified on the {@link Pool} and on the {@link ClientCache} applied to them. In particular,
 * sockets returned by this factory will be wrapped with SSLSockets if ssl is enabled
 * for this client cache based on {@link ConfigurationProperties#SSL_ENABLED_COMPONENTS}.
 * Sockets return by this factory should not be SSLSockets. For modifying SSL settings,
 * see {@link SSLParameterExtension}
 *
 * Sockets returned by this factory should be in an unconnected state, similar to
 * {@link Socket#Socket()}
 *
 * This factory can be used for configuring a proxy, or overriding various socket settings.
 *
 * @see PoolFactory#setSocketFactory(SocketFactory)
 */
@FunctionalInterface
public interface SocketFactory {

  /**
   * The default socket factory
   */
  @Immutable
  SocketFactory DEFAULT = Socket::new;

  /**
   * Create an unconnected tcp socket for establishing a client.
   *
   * @return an unconnected socket
   */
  Socket createSocket() throws IOException;
}
