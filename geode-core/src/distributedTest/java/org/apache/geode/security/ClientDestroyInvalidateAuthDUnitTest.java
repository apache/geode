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
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category(SecurityTest.class)
public class ClientDestroyInvalidateAuthDUnitTest {
  private static String REGION_NAME = "AuthRegion";

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();
  @Rule
  public ServerStarterRule server =
      new ServerStarterRule().withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
          .withSecurityManager(SimpleTestSecurityManager.class).withAutoStart();

  @Before
  public void before() throws Exception {
    Region<String, String> region =
        server.getCache().<String, String>createRegionFactory(RegionShortcut.REPLICATE)
            .create(REGION_NAME);
    for (int i = 0; i < 5; i++) {
      region.put("key" + i, "value" + i);
    }
  }

  @Test
  public void testDestroyInvalidate() throws Exception {
    ClientVM client1 = lsRule.startClientVM(1, "data", "data", true, server.getPort());
    // Delete one key and invalidate another key with an authorized user.
    client1.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      assertThat(cache).isNotNull();
      Region<String, String> region =
          cache.<String, String>createClientRegionFactory(ClientRegionShortcut.PROXY)
              .create(REGION_NAME);
      assertThat(region.containsKeyOnServer("key1")).isTrue();

      // Destroy key1
      region.destroy("key1");
      assertThat(region.containsKeyOnServer("key1")).isFalse();

      // Invalidate key2
      assertThat(region.get("key2")).as("Value of key2 should not be null").isNotNull();
      region.invalidate("key2");
      assertThat(region.get("key2")).as("Value of key2 should have been null").isNull();
      cache.close();
    });

    ClientVM client2 = lsRule.startClientVM(2, "dataRead", "dataRead", true, server.getPort());
    // Delete one key and invalidate another key with an unauthorized user.
    client2.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      assertThat(cache).isNotNull();
      Region region =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);

      assertThat(region.containsKeyOnServer("key3")).isTrue();

      // Destroy key1
      SecurityTestUtil.assertNotAuthorized(() -> region.destroy("key3"), "DATA:WRITE:AuthRegion");
      assertThat(region.containsKeyOnServer("key3")).isTrue();

      // Invalidate key2
      assertThat(region.get("key4")).as("Value of key4 should not be null").isNotNull();
      SecurityTestUtil.assertNotAuthorized(() -> region.invalidate("key4"),
          "DATA:WRITE:AuthRegion");
      assertThat(region.get("key4")).as("Value of key4 should not be null").isNotNull();
      cache.close();
    });
  }
}
