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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.apache.geode.security.SecurityTestUtil.createClientCache;
import static org.apache.geode.security.SecurityTestUtil.createProxyRegion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class NoShowValue1PostProcessorDUnitTest extends JUnit4DistributedTestCase {

  private static final String REGION_NAME = "AuthRegion";

  final Host host = Host.getHost(0);
  final VM client1 = host.getVM(1);

  @Rule
  public ServerStarterRule server =
      new ServerStarterRule().withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
          .withProperty(TestSecurityManager.SECURITY_JSON,
              "org/apache/geode/management/internal/security/clientServer.json")
          .withProperty(SECURITY_POST_PROCESSOR, NoShowValue1PostProcessor.class.getName())
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
  public void testPostProcess() {
    List<String> keys = new ArrayList<>();
    keys.add("key1");
    keys.add("key2");

    client1.invoke(() -> {
      ClientCache cache = createClientCache("super-user", "1234567", server.getPort());
      Region region = createProxyRegion(cache, REGION_NAME);

      // post process for get
      assertEquals("value3", region.get("key3"));

      assertNull(region.get("key1"));

      // post processs for getAll
      Map values = region.getAll(keys);
      assertEquals(2, values.size());
      assertEquals("value2", values.get("key2"));
      assertNull(values.get("key1"));

      // post process for query
      String query = "select * from " + SEPARATOR + "AuthRegion";
      SelectResults result = region.query(query);
      System.out.println("query result: " + result);
      assertEquals(5, result.size());
      assertTrue(result.contains("value0"));
      assertFalse(result.contains("value1"));
      assertTrue(result.contains("value2"));
      assertTrue(result.contains("value3"));
      assertTrue(result.contains("value4"));

      Pool pool = PoolManager.find(region);
      result = (SelectResults) pool.getQueryService().newQuery(query).execute();
      System.out.println("query result: " + result);
      assertTrue(result.contains("value0"));
      assertFalse(result.contains("value1"));
      assertTrue(result.contains("value2"));
      assertTrue(result.contains("value3"));
      assertTrue(result.contains("value4"));
    });
  }

}
