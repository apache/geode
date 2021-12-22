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
import static org.apache.geode.security.SecurityTestUtil.assertNotAuthorized;
import static org.apache.geode.security.SecurityTestUtil.createClientCache;
import static org.apache.geode.security.SecurityTestUtil.createProxyRegion;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class ClientGetPutAuthDUnitTest extends JUnit4DistributedTestCase {

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
          .withAutoStart();

  @Before
  public void before() throws Exception {
    Region region =
        server.getCache().createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);
    for (int i = 0; i < 5; i++) {
      region.put("key" + i, "value" + i);
    }
  }

  @Test
  public void testGetPutAuthorization() throws InterruptedException {
    Map<String, String> allValues = new HashMap<>();
    allValues.put("key1", "value1");
    allValues.put("key2", "value2");

    List<String> keys = new ArrayList<>();
    keys.add("key1");
    keys.add("key2");

    // client1 connects to server as a user not authorized to do any operations
    AsyncInvocation ai1 = client1.invokeAsync(() -> {
      ClientCache cache = createClientCache("stranger", "1234567", server.getPort());
      Region region = createProxyRegion(cache, REGION_NAME);

      assertNotAuthorized(() -> region.put("key3", "value3"), "DATA:WRITE:AuthRegion:key3");
      assertNotAuthorized(() -> region.get("key3"), "DATA:READ:AuthRegion:key3");

      // putall
      assertNotAuthorized(() -> region.putAll(allValues), "DATA:WRITE:AuthRegion");

      // not authorized for either keys, get no record back
      Map keyValues = region.getAll(keys);
      assertEquals(0, keyValues.size());

      assertNotAuthorized(region::keySetOnServer, "DATA:READ:AuthRegion");
    });


    // client2 connects to user as a user authorized to use AuthRegion region
    AsyncInvocation ai2 = client2.invokeAsync(() -> {
      ClientCache cache = createClientCache("authRegionUser", "1234567", server.getPort());
      Region region = createProxyRegion(cache, REGION_NAME);

      region.put("key3", "value3");
      assertEquals("value3", region.get("key3"));

      // put all
      region.putAll(allValues);

      // get all
      Map keyValues = region.getAll(keys);
      assertEquals(2, keyValues.size());

      // keyset
      Set keySet = region.keySetOnServer();
      assertEquals(5, keySet.size());
    });

    // client3 connects to user as a user authorized to use key1 in AuthRegion region
    AsyncInvocation ai3 = client3.invokeAsync(() -> {
      ClientCache cache = createClientCache("key1User", "1234567", server.getPort());
      Region region = createProxyRegion(cache, REGION_NAME);

      assertNotAuthorized(() -> region.put("key2", "value1"), "DATA:WRITE:AuthRegion:key2");
      assertNotAuthorized(() -> region.get("key2"), "DATA:READ:AuthRegion:key2");

      assertNotAuthorized(() -> region.putAll(allValues), "DATA:WRITE:AuthRegion");

      // only authorized for one recrod
      Map keyValues = region.getAll(keys);
      assertEquals(1, keyValues.size());

      // keyset
      assertNotAuthorized(region::keySetOnServer, "DATA:READ:AuthRegion");
    });

    ai1.join();
    ai2.join();
    ai3.join();

    ai1.checkException();
    ai2.checkException();
    ai3.checkException();
  }

}
