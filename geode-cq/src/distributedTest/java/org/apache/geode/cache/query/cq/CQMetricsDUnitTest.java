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
package org.apache.geode.cache.query.cq;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getCache;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqServiceStatistics;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class CQMetricsDUnitTest {

  private CqAttributes cqAttributes;
  private QueryService queryService;
  private TestCqListener testListener;
  private MemberVM locator, server1, server2;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(5);

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void setUpServers() throws Exception {
    locator = cluster.startLocatorVM(0, l -> l.withoutClusterConfigurationService());
    server1 = cluster.startServerVM(1, locator.getPort());
    server2 = cluster.startServerVM(2, locator.getPort());

    ClientCache clientCache = createClientCache(locator.getPort());
    Region region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");

    queryService = clientCache.getQueryService();
    CqAttributesFactory cqAttributesFactory = new CqAttributesFactory();
    testListener = new TestCqListener();
    cqAttributesFactory.addCqListener(testListener);

    cqAttributes = cqAttributesFactory.create();
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void testStopCq() throws Exception {
    gfsh.executeAndAssertThat("create region --name=region --type=PARTITION")
        .statusIsSuccess();
    queryService.newCq("Select * from /region r where r.ID = 1", cqAttributes).execute();

    server1.invoke(() -> populateRegion(0, 100));

    locator.invoke(() -> {
      Cache cache = getCache();
      ManagementService service = ManagementService.getManagementService(cache);
      DistributedSystemMXBean dsmbean = service.getDistributedSystemMXBean();
      await().atMost(30, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(dsmbean.getActiveCQCount()).isEqualTo(2));
    });

    // stop cq
    queryService.stopCqs();

    locator.invoke(() -> {
      Cache cache = getCache();
      ManagementService service = ManagementService.getManagementService(cache);
      DistributedSystemMXBean dsmbean = service.getDistributedSystemMXBean();
      await().atMost(30, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(dsmbean.getActiveCQCount()).isEqualTo(0));
    });

    checkActiveCqCount(server1, 0);
    checkActiveCqCount(server2, 0);
  }

  @Test
  public void testCloseCq() throws Exception {
    gfsh.executeAndAssertThat("create region --name=region --type=PARTITION")
        .statusIsSuccess();
    queryService.newCq("Select * from /region r where r.ID = 1", cqAttributes).execute();

    server1.invoke(() -> populateRegion(0, 100));

    locator.invoke(() -> {
      Cache cache = getCache();
      ManagementService service = ManagementService.getManagementService(cache);
      DistributedSystemMXBean dsmbean = service.getDistributedSystemMXBean();
      await().atMost(30, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(dsmbean.getActiveCQCount()).isEqualTo(2));
    });

    // close cq
    queryService.closeCqs();

    locator.invoke(() -> {
      Cache cache = getCache();
      ManagementService service = ManagementService.getManagementService(cache);
      DistributedSystemMXBean dsmbean = service.getDistributedSystemMXBean();
      await().atMost(30, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(dsmbean.getActiveCQCount()).isEqualTo(0));
    });
  }

  private class TestCqListener implements CqListener, Serializable {
    public int onEventCalls = 0;

    @Override
    public void onEvent(CqEvent aCqEvent) {
      onEventCalls++;
    }

    @Override
    public void onError(CqEvent aCqEvent) {}

    @Override
    public void close() {}
  }

  private static void populateRegion(int startingId, int endingId) {
    Region exampleRegion = getCache().getRegion("region");
    for (int i = startingId; i < endingId; i++) {
      exampleRegion.put("" + i, new Portfolio(i));
    }
  }

  private ClientCache createClientCache(Integer locator1Port) {
    ClientCacheFactory ccf = new ClientCacheFactory();
    ccf.addPoolLocator("localhost", locator1Port);
    ccf.setPoolSubscriptionEnabled(true);
    return ccf.create();
  }

  private void checkActiveCqCount(MemberVM vm, int expectedResult) {
    vm.invoke(() -> {
      QueryService queryService = getCache().getQueryService();
      CqServiceStatistics cqServiceStats = queryService.getCqStatistics();
      await()
          .atMost(30, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(cqServiceStats.numCqsActive()).isEqualTo(expectedResult));
    });
  }
}
