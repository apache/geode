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
package com.gemstone.gemfire.security;

import com.gemstone.gemfire.cache.Region;

import java.util.Properties;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static com.gemstone.gemfire.security.SecurityTestUtils.*;
import static org.junit.Assert.assertNotNull;

/**
 * Extracted from ClientAuthenticationDUnitTest
 */
public abstract class ClientAuthenticationTestUtils {

  protected ClientAuthenticationTestUtils() {
  }

  protected static Integer createCacheServer(final int locatorPort, final String locatorString, final String authenticator, final Properties extraProps, final Properties javaProps) {
    Properties authProps;
    if (extraProps == null) {
      authProps = new Properties();
    } else {
      authProps = extraProps;
    }

    if (authenticator != null) {
      authProps.setProperty(SECURITY_CLIENT_AUTHENTICATOR, authenticator);
    }

    return SecurityTestUtils.createCacheServer(authProps, javaProps, locatorPort, locatorString, 0, NO_EXCEPTION);
  }

  protected static void createCacheServer(final int locatorPort, final String locatorString, final int serverPort, final String authenticator, final Properties extraProps, final Properties javaProps) {
    Properties authProps;
    if (extraProps == null) {
      authProps = new Properties();
    } else {
      authProps = extraProps;
    }

    if (authenticator != null) {
      authProps.setProperty(SECURITY_CLIENT_AUTHENTICATOR, authenticator);
    }
    SecurityTestUtils.createCacheServer(authProps, javaProps, locatorPort, locatorString, serverPort, NO_EXCEPTION);
  }

  protected static void createCacheClient(final String authInit, final Properties authProps, final Properties javaProps, final int[] ports, final int numConnections, final boolean multiUserMode, final boolean subscriptionEnabled, final int expectedResult) {
    SecurityTestUtils.createCacheClient(authInit, authProps, javaProps, ports, numConnections, false, multiUserMode, subscriptionEnabled, expectedResult);
  }

  protected static void createCacheClient(final String authInit, final Properties authProps, final Properties javaProps, final int[] ports, final int numConnections, final boolean multiUserMode, final int expectedResult) {
    createCacheClient(authInit, authProps, javaProps, ports, numConnections, multiUserMode, true, expectedResult);
  }

  protected static void createCacheClient(final String authInit, final Properties authProps, final Properties javaProps, final int port1, final int numConnections, final int expectedResult) {
    createCacheClient(authInit, authProps, javaProps, new int[] { port1 }, numConnections, false, true, expectedResult);
  }

  protected static void createCacheClient(final String authInit, final Properties authProps, final Properties javaProps, final int port1, final int port2, final int numConnections, final int expectedResult) {
    createCacheClient(authInit, authProps, javaProps, port1, port2, numConnections, false, expectedResult);
  }

  protected static void createCacheClient(final String authInit, final Properties authProps, final Properties javaProps, final int port1, final int port2, final int numConnections, final boolean multiUserMode, final int expectedResult) {
    createCacheClient(authInit, authProps, javaProps, port1, port2, numConnections, multiUserMode, true, expectedResult);
  }

  protected static void createCacheClient(final String authInit, final Properties authProps, final Properties javaProps, final int port1, final int port2, final int numConnections, final boolean multiUserMode, final boolean subscriptionEnabled, final int expectedResult) {
    createCacheClient(authInit, authProps, javaProps, new int[] { port1, port2 }, numConnections, multiUserMode, subscriptionEnabled, expectedResult);
  }

  protected static void registerAllInterest() {
    Region region = getCache().getRegion(REGION_NAME);
    assertNotNull(region);
    region.registerInterestRegex(".*");
  }
}
