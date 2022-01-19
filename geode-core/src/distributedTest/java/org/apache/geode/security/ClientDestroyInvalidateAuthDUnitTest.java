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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class ClientDestroyInvalidateAuthDUnitTest {
  private static final String REGION_NAME = "AuthRegion";

  private ClientVM client1, client2;
  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();
  @Rule
  public ServerStarterRule server =
      new ServerStarterRule().withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
          .withSecurityManager(SimpleSecurityManager.class).withAutoStart();

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
    int serverPort = server.getPort();
    client1 = lsRule.startClientVM(1, c1 -> c1.withCredential("data", "data")
        .withPoolSubscription(true)
        .withServerConnection(serverPort));
    // Delete one key and invalidate another key with an authorized user.
    client1.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
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
      cache.close();
    });

    client2 = lsRule.startClientVM(2, c -> c.withCredential("dataRead", "dataRead")
        .withPoolSubscription(true)
        .withServerConnection(serverPort));
    // Delete one key and invalidate another key with an unauthorized user.
    client2.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();

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
      cache.close();
    });
  }

}
