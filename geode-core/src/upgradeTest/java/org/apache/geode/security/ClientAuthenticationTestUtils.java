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
package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static org.apache.geode.security.SecurityTestUtils.NO_EXCEPTION;
import static org.apache.geode.security.SecurityTestUtils.REGION_NAME;
import static org.apache.geode.security.SecurityTestUtils.getCache;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

/**
 * Extracted from ClientAuthenticationDUnitTest
 */
public abstract class ClientAuthenticationTestUtils {

  protected ClientAuthenticationTestUtils() {}

  protected static Integer createCacheServer(final String authenticator,
      final Properties extraProps, final Properties javaProps) {
    return createCacheServer(0, authenticator, extraProps, javaProps,
        NO_EXCEPTION);
  }

  protected static Integer createCacheServer(final int serverPort, final String authenticator,
      final Properties extraProps,
      final Properties javaProps) {
    return createCacheServer(serverPort, authenticator, extraProps,
        javaProps, NO_EXCEPTION);
  }

  protected static Integer createCacheServer(final String authenticator,
      final Properties extraProps, final Properties javaProps,
      final int expectedResult) {
    return createCacheServer(0, authenticator, extraProps, javaProps,
        expectedResult);
  }

  protected static Integer createCacheServer(final int serverPort, final String authenticator,
      final Properties extraProps,
      final Properties javaProps, int expectedResult) {
    Properties authProps;
    if (extraProps == null) {
      authProps = new Properties();
    } else {
      authProps = extraProps;
    }

    if (authenticator != null) {
      authProps.setProperty(SECURITY_CLIENT_AUTHENTICATOR, authenticator);
    }
    return SecurityTestUtils.createCacheServer(authProps, javaProps, serverPort, expectedResult);
  }

  protected static void createCacheClient(final String authInit, final Properties authProps,
      final Properties javaProps, final int[] ports, final int numConnections,
      final boolean multiUserMode, final boolean subscriptionEnabled, final int expectedResult) {
    SecurityTestUtils.createCacheClient(authInit, authProps, javaProps, ports, numConnections,
        false, multiUserMode, subscriptionEnabled, expectedResult);
  }

  protected static void createCacheClient(final String authInit, final Properties authProps,
      final Properties javaProps, final int[] ports, final int numConnections,
      final boolean multiUserMode, final int expectedResult) {
    createCacheClient(authInit, authProps, javaProps, ports, numConnections, multiUserMode, true,
        expectedResult);
  }

  protected static void createCacheClient(final String authInit, final Properties authProps,
      final Properties javaProps, final int port1, final int numConnections,
      final int expectedResult) {
    createCacheClient(authInit, authProps, javaProps, new int[] {port1}, numConnections, false,
        true, expectedResult);
  }

  protected static void createCacheClient(final String authInit, final Properties authProps,
      final Properties javaProps, final int port1, final int port2, final int numConnections,
      final int expectedResult) {
    createCacheClient(authInit, authProps, javaProps, port1, port2, numConnections, false,
        expectedResult);
  }

  protected static void createCacheClient(final String authInit, final Properties authProps,
      final Properties javaProps, final int port1, final int port2, final int numConnections,
      final boolean multiUserMode, final int expectedResult) {
    createCacheClient(authInit, authProps, javaProps, port1, port2, numConnections, multiUserMode,
        true, expectedResult);
  }

  protected static void createCacheClient(final String authInit, final Properties authProps,
      final Properties javaProps, final int port1, final int port2, final int numConnections,
      final boolean multiUserMode, final boolean subscriptionEnabled, final int expectedResult) {
    createCacheClient(authInit, authProps, javaProps, new int[] {port1, port2}, numConnections,
        multiUserMode, subscriptionEnabled, expectedResult);
  }

  protected static void registerAllInterest() {
    Region region = getCache().getRegion(REGION_NAME);
    assertNotNull(region);
    region.registerInterestRegex(".*");
  }

  protected static ExpirableSecurityManager combineSecurityManagerResults(MemberVM... vms) {
    return Arrays.stream(vms)
        .map(vm -> vm.invoke(ClientAuthenticationTestUtils::getSecurityManager))
        .reduce((securityManager0, securityManager1) -> {
          securityManager0.getExpiredUsers().addAll(securityManager1.getExpiredUsers());
          securityManager1.getAuthorizedOps().keySet().forEach(key -> {
            List<String> authOps = securityManager0.getAuthorizedOps().get(key);
            if (authOps == null) {
              authOps = new ArrayList<>();
            }
            authOps.addAll(securityManager1.getAuthorizedOps().get(key));
            securityManager0.getAuthorizedOps().put(key, authOps);
          });
          securityManager1.getUnAuthorizedOps().keySet().forEach(key -> {
            List<String> unAuthOps = securityManager0.getUnAuthorizedOps().get(key);
            if (unAuthOps == null) {
              unAuthOps = new ArrayList<>();
            }
            unAuthOps.addAll(securityManager1.getUnAuthorizedOps().get(key));
            securityManager0.getUnAuthorizedOps().put(key, unAuthOps);
          });
          return securityManager0;
        }).orElse(new ExpirableSecurityManager());
  }

  protected static ExpirableSecurityManager getSecurityManager() {
    return (ExpirableSecurityManager) Objects.requireNonNull(ClusterStartupRule.getCache())
        .getSecurityService()
        .getSecurityManager();
  }
}
