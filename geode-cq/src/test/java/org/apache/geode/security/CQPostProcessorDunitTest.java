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
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.apache.geode.security.SecurityTestUtil.createClientCache;
import static org.apache.geode.security.SecurityTestUtil.createProxyRegion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.CqResults;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.cq.CqListenerImpl;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({DistributedTest.class, SecurityTest.class})
public class CQPostProcessorDunitTest extends JUnit4DistributedTestCase {

  private static String REGION_NAME = "AuthRegion";
  final Host host = Host.getHost(0);
  final VM client1 = host.getVM(1);
  final VM client2 = host.getVM(2);

  @Rule
  public ServerStarterRule server =
      new ServerStarterRule().withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
          .withProperty(TestSecurityManager.SECURITY_JSON,
              "org/apache/geode/management/internal/security/clientServer.json")
          .withProperty(SECURITY_POST_PROCESSOR, TestPostProcessor.class.getName())
          .startAutomatically();

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
    String query = "select * from /AuthRegion";
    client1.invoke(() -> {
      ClientCache cache = createClientCache("super-user", "1234567", server.getPort());
      Region region = createProxyRegion(cache, REGION_NAME);



      Pool pool = PoolManager.find(region);
      QueryService qs = pool.getQueryService();

      CqAttributesFactory factory = new CqAttributesFactory();

      factory.addCqListener(new CqListenerImpl() {
        @Override
        public void onEvent(final CqEvent aCqEvent) {
          assertEquals("key6", aCqEvent.getKey());
          assertEquals("super-user/AuthRegion/key6/value6", aCqEvent.getNewValue());
        }
      });


      CqAttributes cqa = factory.create();

      // Create the CqQuery
      CqQuery cq = qs.newCq("CQ1", query, cqa);
      CqResults results = cq.executeWithInitialResults();
      assertEquals(5, results.size());
      String resultString = results.toString();
      assertTrue(resultString, resultString.contains("key:key0,value:super-user/null/key0/value0"));
      assertTrue(resultString.contains("key:key1,value:super-user/null/key1/value1"));
      assertTrue(resultString.contains("key:key2,value:super-user/null/key2/value2"));
      assertTrue(resultString.contains("key:key3,value:super-user/null/key3/value3"));
      assertTrue(resultString.contains("key:key4,value:super-user/null/key4/value4"));
    });

    client2.invoke(() -> {
      ClientCache cache = createClientCache("authRegionUser", "1234567", server.getPort());
      Region region = createProxyRegion(cache, REGION_NAME);
      region.put("key6", "value6");
    });

  }

}
