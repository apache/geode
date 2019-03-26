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

import static org.apache.geode.security.SecurityTestUtil.assertNotAuthorized;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class ClientCQAuthDUnitTest {

  private static final String REGION_NAME = "AuthRegion";

  private ClientVM client1;
  private ClientVM client2;
  private ClientVM client3;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public ServerStarterRule server = new ServerStarterRule()
      .withSecurityManager(SimpleSecurityManager.class)
      .withRegion(RegionShortcut.REPLICATE, REGION_NAME);

  @Test
  public void verifyCQPermissions() throws Exception {
    String query = "select * from /AuthRegion";
    int serverPort = server.getPort();

    client1 = cluster.startClientVM(1, c2 -> c2.withCredential("test", "test")
        .withPoolSubscription(true)
        .withServerConnection(serverPort));
    client2 =
        cluster.startClientVM(2, c1 -> c1.withCredential("clusterManageQuery", "clusterManageQuery")
            .withPoolSubscription(true)
            .withServerConnection(serverPort));
    client3 =
        cluster.startClientVM(3,
            c -> c.withCredential("clusterManageQuery,dataRead", "clusterManageQuery,dataRead")
                .withPoolSubscription(true)
                .withServerConnection(serverPort));

    // client has no permission whatsoever
    client1.invoke(() -> {
      final Region region = ClusterStartupRule.clientCacheRule.createProxyRegion(REGION_NAME);
      Pool pool = PoolManager.find(region);
      QueryService qs = pool.getQueryService();
      CqAttributes cqa = new CqAttributesFactory().create();

      // Create the CqQuery (this is on the client side)
      CqQuery cq = qs.newCq("CQ1", query, cqa);

      assertNotAuthorized(cq::execute, "DATA:READ:AuthRegion");
      assertNotAuthorized(cq::executeWithInitialResults, "DATA:READ:AuthRegion");
      assertNotAuthorized(qs::getAllDurableCqsFromServer, "CLUSTER:READ");
    });

    // client2 has part of the permission
    client2.invoke(() -> {
      final Region region = ClusterStartupRule.clientCacheRule.createProxyRegion(REGION_NAME);
      Pool pool = PoolManager.find(region);
      QueryService qs = pool.getQueryService();
      CqAttributes cqa = new CqAttributesFactory().create();

      // Create the CqQuery (this is on the client side)
      CqQuery cq = qs.newCq("CQ1", query, cqa);
      assertNotAuthorized(cq::execute, "DATA:READ:AuthRegion");
      assertNotAuthorized(cq::executeWithInitialResults, "DATA:READ:AuthRegion");
      cq.close();
    });

    // client3 has all the permissions
    client3.invoke(() -> {
      Region region = ClusterStartupRule.clientCacheRule.createProxyRegion(REGION_NAME);
      Pool pool = PoolManager.find(region);
      QueryService qs = pool.getQueryService();
      CqAttributes cqa = new CqAttributesFactory().create();

      // Create the CqQuery
      CqQuery cq = qs.newCq("CQ1", query, cqa);
      cq.execute();
      cq.stop();
    });
  }
}
