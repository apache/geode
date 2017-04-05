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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.junit.Assert.*;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.LocalServerStarterRule;
import org.apache.geode.test.dunit.rules.ServerStarterBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
public class ClientDestroyInvalidateAuthDUnitTest extends JUnit4DistributedTestCase {
  private static String REGION_NAME = "testRegion";

  final Host host = Host.getHost(0);
  final VM client1 = host.getVM(1);
  final VM client2 = host.getVM(2);

  @Rule
  public LocalServerStarterRule server =
      new ServerStarterBuilder().withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
          .withProperty(TestSecurityManager.SECURITY_JSON,
              "org/apache/geode/management/internal/security/clientServer.json")
          .buildInThisVM();

  @Before
  public void before() throws Exception {
    Region region =
        server.getCache().createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);
    for (int i = 0; i < 5; i++) {
      region.put("key" + i, "value" + i);
    }
  }

  @Test
  public void testDestroyInvalidate() throws Exception {

    // Delete one key and invalidate another key with an authorized user.
    AsyncInvocation ai1 = client1.invokeAsync(() -> {
      ClientCache cache =
          SecurityTestUtil.createClientCache("dataUser", "1234567", server.getServerPort());

      Region region =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      assertTrue(region.containsKeyOnServer("key1"));

      // Destroy key1
      region.destroy("key1");
      assertFalse(region.containsKeyOnServer("key1"));

      // Invalidate key2
      assertNotNull("Value of key2 should not be null", region.get("key2"));
      region.invalidate("key2");
      assertNull("Value of key2 should have been null", region.get("key2"));

    });

    // Delete one key and invalidate another key with an unauthorized user.
    AsyncInvocation ai2 = client2.invokeAsync(() -> {
      ClientCache cache =
          SecurityTestUtil.createClientCache("authRegionReader", "1234567", server.getServerPort());

      Region region =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);

      assertTrue(region.containsKeyOnServer("key3"));

      // Destroy key1
      SecurityTestUtil.assertNotAuthorized(() -> region.destroy("key3"), "DATA:WRITE:AuthRegion");
      assertTrue(region.containsKeyOnServer("key3"));

      // Invalidate key2
      assertNotNull("Value of key4 should not be null", region.get("key4"));
      SecurityTestUtil.assertNotAuthorized(() -> region.invalidate("key4"),
          "DATA:WRITE:AuthRegion");
      assertNotNull("Value of key4 should not be null", region.get("key4"));
    });

    ai1.await();
  }

}
