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

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.LocalServerStarterRule;
import org.apache.geode.test.dunit.rules.ServerStarterBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
public class ClientQueryAuthDUnitTest extends JUnit4DistributedTestCase {

  private static String REGION_NAME = "AuthRegion";
  final Host host = Host.getHost(0);
  final VM client1 = host.getVM(1);
  final VM client2 = host.getVM(2);
  final VM client3 = host.getVM(3);

  @Rule
  public LocalServerStarterRule server =
      new ServerStarterBuilder().withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
          .withProperty(TestSecurityManager.SECURITY_JSON,
              "org/apache/geode/management/internal/security/clientServer.json")
          .withRegion(RegionShortcut.REPLICATE, REGION_NAME).buildInThisVM();

  @Test
  public void testQuery() {
    client1.invoke(() -> {
      ClientCache cache = createClientCache("stranger", "1234567", server.getServerPort());
      final Region region = createProxyRegion(cache, REGION_NAME);

      String query = "select * from /AuthRegion";
      assertNotAuthorized(() -> region.query(query), "DATA:READ:AuthRegion");

      Pool pool = PoolManager.find(region);
      assertNotAuthorized(() -> pool.getQueryService().newQuery(query).execute(),
          "DATA:READ:AuthRegion");
    });
  }

  @Test
  public void testCQ() {
    String query = "select * from /AuthRegion";
    client1.invoke(() -> {
      ClientCache cache = createClientCache("stranger", "1234567", server.getServerPort());
      Region region = createProxyRegion(cache, REGION_NAME);
      Pool pool = PoolManager.find(region);
      QueryService qs = pool.getQueryService();

      CqAttributes cqa = new CqAttributesFactory().create();

      // Create the CqQuery
      CqQuery cq = qs.newCq("CQ1", query, cqa);

      assertNotAuthorized(() -> cq.executeWithInitialResults(), "DATA:READ:AuthRegion");
      assertNotAuthorized(() -> cq.execute(), "DATA:READ:AuthRegion");

      assertNotAuthorized(() -> cq.close(), "DATA:MANAGE");
    });

    client2.invoke(() -> {
      ClientCache cache = createClientCache("authRegionReader", "1234567", server.getServerPort());
      Region region = createProxyRegion(cache, REGION_NAME);
      Pool pool = PoolManager.find(region);
      QueryService qs = pool.getQueryService();

      CqAttributes cqa = new CqAttributesFactory().create();
      // Create the CqQuery
      CqQuery cq = qs.newCq("CQ1", query, cqa);
      cq.execute();

      assertNotAuthorized(() -> cq.stop(), "DATA:MANAGE");
      assertNotAuthorized(() -> qs.getAllDurableCqsFromServer(), "CLUSTER:READ");
    });

    client3.invoke(() -> {
      ClientCache cache = createClientCache("super-user", "1234567", server.getServerPort());
      Region region = createProxyRegion(cache, REGION_NAME);
      Pool pool = PoolManager.find(region);
      QueryService qs = pool.getQueryService();

      CqAttributesFactory factory = new CqAttributesFactory();
      factory.addCqListener(new CqListener() {
        @Override
        public void onEvent(final CqEvent aCqEvent) {
          System.out.println(aCqEvent);
        }

        @Override
        public void onError(final CqEvent aCqEvent) {

        }

        @Override
        public void close() {

        }
      });


      CqAttributes cqa = factory.create();

      // Create the CqQuery
      CqQuery cq = qs.newCq("CQ1", query, cqa);
      System.out.println("query result: " + cq.executeWithInitialResults());

      cq.stop();
    });
  }

}
