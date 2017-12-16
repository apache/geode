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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
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
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class MultiClientDUnitTest {

  private static int SESSION_COUNT = 2;
  private static int KEY_COUNT = 20;

  @ClassRule
  public static LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  private static MemberVM locator, server1, server2;
  private static VM client3, client4, client5, client6;

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

    MemberVM.invokeInEveryMember(() -> {
      Cache cache = LocatorServerStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION).create("region");
    }, server1, server2);

    client3 = lsRule.getVM(3);
    client4 = lsRule.getVM(4);
    client5 = lsRule.getVM(5);
    client6 = lsRule.getVM(6);
  }

  @Test
  public void multiClient() throws ExecutionException, InterruptedException, TimeoutException {
    int locatorPort = locator.getPort();
    int server1Port = server1.getPort();
    int server2Port = server2.getPort();


    // client 3 keeps logging in, do some successful put, and log out and keep doing it for multiple
    // times
    AsyncInvocation vm3Invoke = client3.invokeAsync("run as data", () -> {
      for (int i = 0; i < SESSION_COUNT; i++) {
        ClientCache cache = createClientCache("data", "data", server1Port, server2Port);
        Region region =
            cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
        for (int j = 0; j < KEY_COUNT; j++) {
          region.put(i + "" + j, i + "" + j);
        }
        cache.close();
      }
    });

    // client 4 keeps logging in, do an unauthorized put, and log out
    AsyncInvocation vm4Invoke = client4.invokeAsync("run as stranger", () -> {
      for (int i = 0; i < SESSION_COUNT; i++) {
        ClientCache cache = createClientCache("stranger", "stranger", server1Port, server2Port);
        Region region =
            cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
        for (int j = 0; j < KEY_COUNT; j++) {
          String value = i + "" + j;
          assertThatThrownBy(() -> region.put(value, value))
              .isInstanceOf(ServerOperationException.class);
        }
        cache.close();
      }
    });

    // client 5 keeps logging in, do some successful get, and log out
    AsyncInvocation vm5Invoke = client5.invokeAsync("run as data", () -> {
      for (int i = 0; i < SESSION_COUNT; i++) {
        ClientCache cache = createClientCache("data", "data", server1Port, server2Port);
        Region region =
            cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
        for (int j = 0; j < KEY_COUNT; j++) {
          region.get(i + "" + j);
        }
        cache.close();
      }
    });

    // // client 6 keeps logging in with incorrect
    AsyncInvocation vm6Invoke = client6.invokeAsync("run as invalid user", () -> {
      for (int i = 0; i < SESSION_COUNT; i++) {
        ClientCache cache =
            createClientCache("dataWithWrongPswd", "password", server1Port, server2Port);
        Region region =
            cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
        for (int j = 0; j < 1; j++) {
          String key = i + "" + j;
          assertThatThrownBy(() -> region.get(key)).isInstanceOf(ServerOperationException.class);
        }
        cache.close();
      }
    });

    vm3Invoke.await(60, TimeUnit.MINUTES);
    vm4Invoke.await(60, TimeUnit.MINUTES);
    vm5Invoke.await(60, TimeUnit.MINUTES);
    vm6Invoke.await(60, TimeUnit.MINUTES);
  }

  private static ClientCache createClientCache(String username, String password, int locatorPort) {
    ClientCache cache = new ClientCacheFactory(getClientCacheProperties(username, password))
        .setPoolSubscriptionEnabled(false).addPoolLocator("localhost", locatorPort).create();
    return cache;
  }

  private static ClientCache createClientCache(String username, String password, int server1Port,
      int server2Port) {
    ClientCache cache = new ClientCacheFactory(getClientCacheProperties(username, password))
        .setPoolSubscriptionEnabled(false).addPoolServer("localhost", server1Port)
        .addPoolServer("localhost", server2Port).create();
    return cache;
  }

  private static Properties getClientCacheProperties(String username, String password) {
    Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, username);
    props.setProperty(UserPasswordAuthInit.PASSWORD, password);
    props.setProperty(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName());
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    return props;
  }

}
