/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.test.dunit;

import java.net.UnknownHostException;

import com.gemstone.gemfire.internal.SocketCreator;

/**
 * <code>NetworkUtils</code> provides static utility methods to perform
 * network DNS lookups or similar actions.
 * 
 * These methods can be used directly: <code>NetworkUtils.getIPLiteral()</code>, 
 * however, they are intended to be referenced through static import:
 *
 * <pre>
 * import static com.gemstone.gemfire.test.dunit.NetworkUtils.*;
 *    ...
 *    String hostName = getIPLiteral();
 * </pre>
 *
 * Extracted from DistributedTestCase.
 */
public class NetworkUtils {

  protected NetworkUtils() {
  }
  
  /** 
   * Get the IP literal name for the current host. Use this instead of  
   * "localhost" to avoid IPv6 name resolution bugs in the JDK/machine config.
   * This method honors java.net.preferIPvAddresses
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
   * Get the host name to use for a server cache in client/server dunit
   * testing.
   * 
   * @param host the dunit Host to get a machine host name for
   * @return the host name
   */
  public static String getServerHostName(final Host host) {
    String serverBindAddress = System.getProperty("gemfire.server-bind-address");
    return serverBindAddress != null ? serverBindAddress : host.getHostName();
  }
}
