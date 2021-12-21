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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_DHALGO;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.security.SecurityTestUtil.assertNotAuthorized;
import static org.apache.geode.security.SecurityTestUtil.createClientCache;
import static org.apache.geode.security.SecurityTestUtil.createProxyRegion;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class ClientRegisterInterestAuthDUnitTest extends JUnit4DistributedTestCase {

  private static final String REGION_NAME = "AuthRegion";

  final Host host = Host.getHost(0);
  final VM client1 = host.getVM(1);
  final VM client2 = host.getVM(2);
  final VM client3 = host.getVM(3);

  @Rule
  public ServerStarterRule server =
      new ServerStarterRule().withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
          .withProperty(TestSecurityManager.SECURITY_JSON,
              "org/apache/geode/management/internal/security/clientServer.json")
          .withProperty(SECURITY_CLIENT_DHALGO, "AES:128")
          .withRegion(RegionShortcut.REPLICATE, REGION_NAME);

  @Test
  public void testRegisterInterest() throws Exception {
    final Properties extraProperties = new Properties();
    extraProperties.setProperty(SECURITY_CLIENT_DHALGO, "AES:128");

    // client1 connects to server as a user not authorized to do any operations
    AsyncInvocation ai1 = client1.invokeAsync(() -> {
      ClientCache cache =
          createClientCache("stranger", "1234567", server.getPort(), extraProperties);
      Region region = createProxyRegion(cache, REGION_NAME);
      assertNotAuthorized(() -> region.registerInterest("key3"), "DATA:READ:AuthRegion:key3");
    });

    // client2 connects to user as a user authorized to use AuthRegion region
    AsyncInvocation ai2 = client2.invokeAsync(() -> {
      ClientCache cache =
          createClientCache("authRegionUser", "1234567", server.getPort(), extraProperties);
      Region region = createProxyRegion(cache, REGION_NAME);
      region.registerInterest("key3"); // DATA:READ:AuthRegion:key3;
    });

    // client3 connects to user as a user authorized to use key1 in AuthRegion region
    AsyncInvocation ai3 = client3.invokeAsync(() -> {
      ClientCache cache =
          createClientCache("key1User", "1234567", server.getPort(), extraProperties);
      Region region = createProxyRegion(cache, REGION_NAME);
      assertNotAuthorized(() -> region.registerInterest("key2"), "DATA:READ:AuthRegion:key2");
    });

    ai1.await();
    ai2.await();
    ai3.await();
  }

  @Test
  public void testRegisterInterestRegex() throws Exception {
    final Properties extraProperties = new Properties();
    extraProperties.setProperty(SECURITY_CLIENT_DHALGO, "AES:128");

    // client1 connects to server as a user not authorized to do any operations
    AsyncInvocation ai1 = client1.invokeAsync(() -> {
      ClientCache cache =
          createClientCache("stranger", "1234567", server.getPort(), extraProperties);

      Region region =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      assertNotAuthorized(() -> region.registerInterestRegex("key.*"), "DATA:READ:AuthRegion");
    });

    // client2 connects to user as a user authorized to use AuthRegion region
    AsyncInvocation ai2 = client2.invokeAsync(() -> {
      ClientCache cache =
          createClientCache("authRegionUser", "1234567", server.getPort(), extraProperties);

      Region region =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      region.registerInterestRegex("key[0-9]+"); // DATA:READ:AuthRegion:key3;
    });

    // client3 connects to user as a user authorized to use key1 in AuthRegion region
    AsyncInvocation ai3 = client3.invokeAsync(() -> {
      ClientCache cache =
          createClientCache("key1User", "1234567", server.getPort(), extraProperties);

      Region region =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      assertNotAuthorized(() -> region.registerInterestRegex("key[0-9]+"), "DATA:READ:AuthRegion");
      assertNotAuthorized(() -> region.registerInterestRegex("key1"), "DATA:READ:AuthRegion");
    });

    ai1.await();
    ai2.await();
    ai3.await();
  }

  @Test
  public void testRegisterInterestList() throws Exception {
    final Properties extraProperties = new Properties();
    extraProperties.setProperty(SECURITY_CLIENT_DHALGO, "AES:128");

    List<String> keys = new ArrayList<>();
    keys.add("key1");
    keys.add("key2");

    // client1 connects to server as a user not authorized to do any operations
    AsyncInvocation ai1 = client1.invokeAsync(() -> {
      ClientCache cache =
          createClientCache("stranger", "1234567", server.getPort(), extraProperties);

      Region region =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      assertNotAuthorized(() -> region.registerInterest(keys), "DATA:READ:AuthRegion");
    });

    // client2 connects to user as a user authorized to use AuthRegion region
    AsyncInvocation ai2 = client2.invokeAsync(() -> {
      ClientCache cache =
          createClientCache("authRegionUser", "1234567", server.getPort(), extraProperties);

      Region region =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      region.registerInterest(keys); // DATA:READ:AuthRegion;
    });

    // client3 connects to user as a user authorized to use key1 in AuthRegion region
    AsyncInvocation ai3 = client3.invokeAsync(() -> {
      ClientCache cache =
          createClientCache("key1User", "1234567", server.getPort(), extraProperties);

      Region region =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      assertNotAuthorized(() -> region.registerInterest(keys), "DATA:READ:AuthRegion");
    });

    ai1.await();
    ai2.await();
    ai3.await();
  }

}
