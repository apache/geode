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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({DistributedTest.class, SecurityTest.class})
public class ClientCQAuthDUnitTest {

  private static final String REGION_NAME = "AuthRegion";

  private VM client1;
  private VM client2;
  private VM client3;

  @Rule
  public ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public ServerStarterRule server = new ServerStarterRule()
      .withProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName())
      .withRegion(RegionShortcut.REPLICATE, REGION_NAME);

  @Before
  public void setUp() {
    client1 = startupRule.getVM(1);
    client2 = startupRule.getVM(2);
    client3 = startupRule.getVM(3);
  }

  @Test
  public void verifyCQPermissions() {
    String query = "select * from /AuthRegion";
    int serverPort = server.getPort();

    // client has no permission whatsoever
    client1.invoke(() -> {
      ClientCache cache = createClientCache("test", "test", serverPort);
      final Region region = createProxyRegion(cache, REGION_NAME);
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
      ClientCache cache = createClientCache("clusterManageQuery", "clusterManageQuery", serverPort);
      final Region region = createProxyRegion(cache, REGION_NAME);
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
      ClientCache cache = createClientCache("clusterManageQuery,dataRead",
          "clusterManageQuery,dataRead", serverPort);
      Region region = createProxyRegion(cache, REGION_NAME);
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
