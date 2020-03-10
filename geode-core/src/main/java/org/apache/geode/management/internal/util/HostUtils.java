/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.util;

import java.net.UnknownHostException;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.DistributionLocator;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.lang.StringUtils;

public class HostUtils {
  private static final String LOCALHOST = "localhost";

  public static String getLocalHost() {
    try {
      return LocalHostUtil.getCanonicalLocalHostName();
    } catch (UnknownHostException ignore) {
      return LOCALHOST;
    }
  }

  public static String getLocatorId(final String host, final Integer port) {
    final String locatorHost = (host != null ? host : getLocalHost());
    final String locatorPort =
        StringUtils.defaultString(port, String.valueOf(DistributionLocator.DEFAULT_LOCATOR_PORT));
    return locatorHost.concat("[").concat(locatorPort).concat("]");
  }

  public static String getServerId(final String host, final Integer port) {
    String serverHost = (host != null ? host : getLocalHost());
    String serverPort = StringUtils.defaultString(port, String.valueOf(CacheServer.DEFAULT_PORT));
    return serverHost.concat("[").concat(serverPort).concat("]");
  }
}
