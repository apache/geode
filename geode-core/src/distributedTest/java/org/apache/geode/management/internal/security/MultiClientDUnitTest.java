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

package org.apache.geode.management.internal.security;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.VMProvider;

@Category({SecurityTest.class})
public class MultiClientDUnitTest {
  private static final int KEY_COUNT = 20;

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule(7);

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static ClientVM client3;
  private static ClientVM client4;
  private static ClientVM client5;
  private static ClientVM client6;

  @BeforeClass
  public static void beforeClass() throws Exception {
    IgnoredException.addIgnoredException("org.apache.geode.security.AuthenticationFailedException");
    Properties locatorProps = new Properties();
    locatorProps.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getCanonicalName());
    locator = lsRule.startLocatorVM(0, locatorProps);

    Properties serverProps = new Properties();
    serverProps.setProperty("security-username", "cluster");
    serverProps.setProperty("security-password", "cluster");
    server1 = lsRule.startServerVM(1, serverProps, locator.getPort());
    server2 = lsRule.startServerVM(2, serverProps, locator.getPort());

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION).create("region");
    }, server1, server2);

    int server1Port = server1.getPort();
    int server2Port = server2.getPort();
    client3 = lsRule.startClientVM(3, c -> c.withCredential("data", "data")
        .withPoolSubscription(false)
        .withServerConnection(server1Port, server2Port));
    client4 = lsRule.startClientVM(4, c -> c.withCredential("stranger", "stranger")
        .withPoolSubscription(false)
        .withServerConnection(server1Port, server2Port));
    client5 = lsRule.startClientVM(5, c -> c.withCredential("data", "data")
        .withPoolSubscription(false)
        .withServerConnection(server1Port, server2Port));
    client6 = lsRule.startClientVM(6, c -> c.withCredential("dataWithWrongPswd", "data")
        .withPoolSubscription(false)
        .withServerConnection(server1Port, server2Port));
  }

  @Test
  public void multiClient() throws ExecutionException, InterruptedException, TimeoutException {

    // client 3 keeps logging in, do some successful put, and log out and keep doing it for multiple
    // times
    AsyncInvocation vm3Invoke = client3.invokeAsync("run as data", () -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
      for (int j = 0; j < KEY_COUNT; j++) {
        region.put(j + "", j + "");
      }
    });

    // client 4 keeps logging in, do an unauthorized put, and log out
    AsyncInvocation vm4Invoke = client4.invokeAsync("run as stranger", () -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
      for (int j = 0; j < KEY_COUNT; j++) {
        String value = "" + j;
        assertThatThrownBy(() -> region.put(value, value))
            .isInstanceOf(ServerOperationException.class);
      }
    });

    // client 5 keeps logging in, do some successful get, and log out
    AsyncInvocation vm5Invoke = client5.invokeAsync("run as data", () -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
      for (int j = 0; j < KEY_COUNT; j++) {
        region.get("" + j);
      }
    });

    // // client 6 keeps logging in with incorrect
    AsyncInvocation vm6Invoke = client6.invokeAsync("run as invalid user", () -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
      for (int j = 0; j < 1; j++) {
        String key = "" + j;
        assertThatThrownBy(() -> region.get(key)).isInstanceOf(ServerOperationException.class)
            .hasRootCauseInstanceOf(AuthenticationFailedException.class);
      }
    });

    vm3Invoke.await(60, TimeUnit.MINUTES);
    vm4Invoke.await(60, TimeUnit.MINUTES);
    vm5Invoke.await(60, TimeUnit.MINUTES);
    vm6Invoke.await(60, TimeUnit.MINUTES);
  }
}
