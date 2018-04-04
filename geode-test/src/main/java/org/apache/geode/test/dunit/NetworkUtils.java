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
package org.apache.geode.test.dunit;

import static org.apache.geode.distributed.ConfigurationProperties.SERVER_BIND_ADDRESS;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;

import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.geode.internal.net.SocketCreator;

/**
 * {@code NetworkUtils} provides static utility methods to perform network DNS lookups or
 * similar actions.
 *
 * These methods can be used directly: {@code NetworkUtils.getIPLiteral()}, however, they are
 * intended to be referenced through static import:
 *
 * <pre>
 * import static org.apache.geode.test.dunit.NetworkUtils.getIPLiteral;
 *    ...
 *    String hostName = getIPLiteral();
 * </pre>
 *
 * Extracted from DistributedTestCase.
 */
public class NetworkUtils {

  protected NetworkUtils() {
    // nothing
  }

  /**
   * Returns the IP literal name for the current host. Use this instead of "localhost" to avoid IPv6
   * name resolution bugs in the JDK/machine config. This method honors java.net.preferIPvAddresses
   *
   * @return an IP literal which honors java.net.preferIPvAddresses
   */
  public static String getIPLiteral() {
    try {
      return SocketCreator.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      throw new Error("Problem determining host IP address", e);
    }
  }

  /**
   * Returns the host name to use for a server cache in client/server DUnit testing.
   *
   * @return the host name
   */
  public static String getServerHostName() {
    String serverBindAddress = System.getProperty(GEMFIRE_PREFIX + SERVER_BIND_ADDRESS);
    return serverBindAddress != null ? serverBindAddress : getCanonicalHostName();
  }

  /**
   * Returns the host name to use for a server cache in client/server DUnit testing.
   *
   * @param host the DUnit Host to get a machine host name for
   * @return the host name
   * @deprecated Please use {@link #getServerHostName()} instead.
   */
  @Deprecated
  public static String getServerHostName(final Host host) {
    String serverBindAddress = System.getProperty(GEMFIRE_PREFIX + SERVER_BIND_ADDRESS);
    return serverBindAddress != null ? serverBindAddress : host.getHostName();
  }

  /**
   * Returns {@code InetAddress.getLocalHost().getCanonicalHostName()}.
   *
   * @return the canonical host name
   * @throws UncheckedIOException if underlying call threw {@code UnknownHostException}.
   */
  private static String getCanonicalHostName() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new UncheckedIOException(e);
    }
  }
}
