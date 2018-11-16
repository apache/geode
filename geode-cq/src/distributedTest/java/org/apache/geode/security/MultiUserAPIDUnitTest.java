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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.Query;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ClientCacheRule;

public class MultiUserAPIDUnitTest {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static MemberVM server1, server2;

  @Rule
  public ClientCacheRule client = new ClientCacheRule();

  @BeforeClass
  public static void setUp() throws Exception {
    server1 = cluster.startServerVM(0, s -> s.withSecurityManager(SimpleSecurityManager.class)
        .withConnectionToLocator(ClusterStartupRule.getDUnitLocatorPort()));
    server2 = cluster.startServerVM(0, s -> s.withSecurityManager(SimpleSecurityManager.class)
        .withConnectionToLocator(ClusterStartupRule.getDUnitLocatorPort()));
    server1.invoke(() -> {
      ClusterStartupRule.memberStarter.createRegion(RegionShortcut.REPLICATE, "authRegion");
    });
  }

  @Test
  public void testSingleUserUnsupportedAPIs() throws Exception {
    client.withCredential("stranger", "stranger").withMultiUser(false)
        .withServerConnection(server1.getPort(), server2.getPort());
    ClientCache clientCache = client.createCache();

    assertThatThrownBy(() -> clientCache.createAuthenticatedView(new Properties()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("did not have multiuser-authentication set to true");
  }

  @Test
  public void testMultiUserUnsupportedAPIs() throws Exception {
    client.withCredential("stranger", "stranger")
        .withPoolSubscription(true)
        .withMultiUser(true)
        .withServerConnection(server1.getPort(), server2.getPort());
    client.createCache();
    Region realRegion = client.createProxyRegion("authRegion");
    Pool pool = client.getCache().getDefaultPool();

    RegionService proxyCache = client.createAuthenticatedView("data", "data");
    Region proxyRegion = proxyCache.getRegion("authRegion");

    assertThatThrownBy(() -> realRegion.create("key", "value"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> realRegion.put("key", "value"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> realRegion.get("key"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> realRegion.containsKeyOnServer("key"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> realRegion.remove("key"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> realRegion.destroy("key"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> realRegion.destroyRegion())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> realRegion.registerInterest("key"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> realRegion.clear()).isInstanceOf(UnsupportedOperationException.class);


    assertThatThrownBy(() -> proxyRegion.createSubregion("subRegion", null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> proxyRegion.forceRolling())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> proxyRegion.getAttributesMutator())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> proxyRegion.loadSnapshot(null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> proxyRegion.saveSnapshot(null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> proxyRegion.registerInterest("key"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> proxyRegion.setUserAttribute(null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> proxyRegion.unregisterInterestRegex("*"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> proxyRegion.localDestroy("key"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> proxyRegion.localInvalidate("key"))
        .isInstanceOf(UnsupportedOperationException.class);

    assertThatThrownBy(() -> FunctionService.onRegion(realRegion))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> FunctionService.onServer(pool))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> FunctionService.onServers(pool))
        .isInstanceOf(UnsupportedOperationException.class);


    assertThatThrownBy(() -> {
      Query query = pool.getQueryService().newQuery("SELECT * FROM /authRegion");
      query.execute();
    }).isInstanceOf(UnsupportedOperationException.class);
    CqQuery cqQuery =
        pool.getQueryService().newCq("SELECT * FROM /authRegion",
            new CqAttributesFactory().create());
    assertThatThrownBy(() -> cqQuery.execute())
        .hasCauseInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> cqQuery.executeWithInitialResults())
        .hasCauseInstanceOf(UnsupportedOperationException.class);

    assertThatThrownBy(() -> proxyCache.getQueryService().getIndexes())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> proxyCache.getQueryService().getIndexes(null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> proxyCache.getQueryService().createIndex(null, null, null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> proxyCache.getQueryService().removeIndexes())
        .isInstanceOf(UnsupportedOperationException.class);


  }
}
