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

import static junit.framework.TestCase.assertEquals;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;


@Category({ClientSubscriptionTest.class})
public class CQDistributedTest implements Serializable {

  private MemberVM locator;
  private MemberVM server;
  private MemberVM server2;

  private CqAttributes cqa;
  private QueryService qs;
  private TestCqListener testListener;
  private TestCqListener2 testListener2;

  private Region region;

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Before
  public void before() throws Exception {
    locator = clusterStartupRule.startLocatorVM(1, new Properties());
    Integer locator1Port = locator.getPort();
    server = clusterStartupRule.startServerVM(3, locator1Port);
    createServerRegion(server, RegionShortcut.PARTITION);

    server2 = clusterStartupRule.startServerVM(4, locator1Port);
    createServerRegion(server2, RegionShortcut.PARTITION);

    ClientCache clientCache = createClientCache(locator1Port);
    region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");

    qs = clientCache.getQueryService();
    CqAttributesFactory cqaf = new CqAttributesFactory();
    testListener = new TestCqListener();
    testListener2 = new TestCqListener2();
    cqaf.addCqListener(testListener);
    cqaf.addCqListener(testListener2);

    cqa = cqaf.create();
  }

  @Test
  public void cqUsingModShouldFireEventsWhenFilterCriteriaIsMet() throws Exception {
    qs.newCq("Select * from " + SEPARATOR + "region r where r.ID % 2 = 1", cqa).execute();

    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put(0, new Portfolio(0));
      regionOnServer.put(1, new Portfolio(1));
      regionOnServer.put(2, new Portfolio(2));
      regionOnServer.put(3, new Portfolio(3));
      regionOnServer.put(4, new Portfolio(4));
    });

    await()
        .untilAsserted(() -> assertEquals(2, testListener.onEventCalls));
  }

  @Test
  public void cqUsingPlusShouldFireEventsWhenFilterCriteriaIsMet() throws Exception {
    qs.newCq("Select * from " + SEPARATOR + "region r where r.ID + 3 > 4", cqa).execute();
    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put(0, new Portfolio(0));
      regionOnServer.put(1, new Portfolio(1));
      regionOnServer.put(2, new Portfolio(2));
      regionOnServer.put(3, new Portfolio(3));
      regionOnServer.put(4, new Portfolio(4));
    });

    await()
        .untilAsserted(() -> assertEquals(3, testListener.onEventCalls));
  }

  @Test
  public void cqUsingSubtractShouldFireEventsWhenFilterCriteriaIsMet() throws Exception {
    qs.newCq("Select * from " + SEPARATOR + "region r where r.ID - 3 < 0", cqa).execute();
    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put(0, new Portfolio(0));
      regionOnServer.put(1, new Portfolio(1));
      regionOnServer.put(2, new Portfolio(2));
      regionOnServer.put(3, new Portfolio(3));
      regionOnServer.put(4, new Portfolio(4));
    });

    await()
        .untilAsserted(() -> assertEquals(3, testListener.onEventCalls));
  }

  @Test
  public void cqUsingDivideShouldFireEventsWhenFilterCriteriaIsMet() throws Exception {
    qs.newCq("Select * from " + SEPARATOR + "region r where r.ID / 2 = 1", cqa).execute();
    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put(0, new Portfolio(0));
      regionOnServer.put(1, new Portfolio(1));
      regionOnServer.put(2, new Portfolio(2));
      regionOnServer.put(3, new Portfolio(3));
      regionOnServer.put(4, new Portfolio(4));
    });

    await()
        .untilAsserted(() -> assertEquals(2, testListener.onEventCalls));
  }

  @Test
  public void cqUsingMultiplyShouldFireEventsWhenFilterCriteriaIsMet() throws Exception {
    qs.newCq("Select * from " + SEPARATOR + "region r where r.ID * 2 > 3", cqa).execute();
    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put(0, new Portfolio(0));
      regionOnServer.put(1, new Portfolio(1));
      regionOnServer.put(2, new Portfolio(2));
      regionOnServer.put(3, new Portfolio(3));
      regionOnServer.put(4, new Portfolio(4));
    });

    await()
        .untilAsserted(() -> assertEquals(3, testListener.onEventCalls));
  }

  @Test
  public void cqExecuteWithInitialResultsWithValuesMatchingPrimaryKeyShouldNotThrowClassCastException()
      throws Exception {
    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      ClusterStartupRule.getCache().getQueryService().createKeyIndex("PrimaryKeyIndex", "ID",
          SEPARATOR + "region");
      regionOnServer.put(0, new Portfolio(0));
      regionOnServer.put(1, new Portfolio(1));
      regionOnServer.put(2, new Portfolio(2));
      regionOnServer.put(3, new Portfolio(3));
      regionOnServer.put(4, new Portfolio(4));
    });

    SelectResults results =
        qs.newCq("Select * from " + SEPARATOR + "region where ID = 1", cqa)
            .executeWithInitialResults();
    assertEquals(1, results.size());
  }

  @Test
  public void cqWithTransaction() throws Exception {
    qs.newCq("Select * from /region r where r.ID = 1", cqa).execute();

    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      final CacheTransactionManager txMgr =
          ClusterStartupRule.getCache().getCacheTransactionManager();

      // CREATE new entry
      txMgr.begin();
      regionOnServer.put(0, new Portfolio(1));
      txMgr.commit();

      // UPDATE
      txMgr.begin();
      regionOnServer.put(0, new Portfolio(0));
      txMgr.commit();

      // CREATE
      txMgr.begin();
      regionOnServer.put(0, new Portfolio(1));
      txMgr.commit();
    });

    await().untilAsserted(() -> assertThat(testListener2.onEventCreateCalls).isEqualTo(2));
    await().untilAsserted(() -> assertThat(testListener2.onEventUpdateCalls).isEqualTo(0));
  }

  @Test
  public void cqWithoutTransaction() throws Exception {
    qs.newCq("Select * from /region r where r.ID = 1", cqa).execute();

    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      // CREATE new entry
      regionOnServer.put(0, new Portfolio(1));

      // UPDATE
      regionOnServer.put(0, new Portfolio(0));

      // CREATE
      regionOnServer.put(0, new Portfolio(1));
    });

    await().untilAsserted(() -> assertThat(testListener2.onEventCreateCalls).isEqualTo(2));
    await().untilAsserted(() -> assertThat(testListener2.onEventUpdateCalls).isEqualTo(0));
  }

  @Test
  public void cqWithTransaction2Servers() throws Exception {

    qs.newCq("Select * from /region r where r.ID = 1", cqa).execute();

    final CacheTransactionManager txMgr = region.getCache().getCacheTransactionManager();

    // CREATE new entry
    for (int i = 0; i < 4; i++) {
      txMgr.begin();
      region.put(i, new Portfolio(1));
      txMgr.commit();
    }

    // UPDATE
    for (int i = 0; i < 4; i++) {
      txMgr.begin();
      region.put(i, new Portfolio(0));
      txMgr.commit();
    }

    // CREATE
    for (int i = 0; i < 4; i++) {
      txMgr.begin();
      region.put(i, new Portfolio(1));
      txMgr.commit();
    }

    await().untilAsserted(() -> assertThat(testListener2.onEventCreateCalls).isEqualTo(8));
    await().untilAsserted(() -> assertThat(testListener2.onEventUpdateCalls).isEqualTo(0));
  }

  @Test
  public void cqEventsCreatUpdate_suppressUpdate_false() throws Exception {
    qs.newCq("Select * from " + SEPARATOR + "region r where r.ID > 1", cqa, false, 0).execute();

    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put(0, new Portfolio(0));
      regionOnServer.put(1, new Portfolio(1));
      regionOnServer.put(2, new Portfolio(2));
      regionOnServer.put(3, new Portfolio(3));
      regionOnServer.put(4, new Portfolio(4));

      regionOnServer.put(0, new Portfolio(0));
      regionOnServer.put(1, new Portfolio(1));
      regionOnServer.put(2, new Portfolio(2));
      regionOnServer.put(3, new Portfolio(3));
      regionOnServer.put(4, new Portfolio(4));
    });

    await()
        .untilAsserted(() -> assertEquals(6, testListener.onEventCalls));
  }

  @Test
  public void cqEventsCreatUpdate_suppressUpdate_true() throws Exception {
    qs.newCq("Select * from " + SEPARATOR + "region r where r.ID > 1", cqa, false, 2).execute();

    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put(0, new Portfolio(0));
      regionOnServer.put(1, new Portfolio(1));
      regionOnServer.put(2, new Portfolio(2));
      regionOnServer.put(3, new Portfolio(3));
      regionOnServer.put(4, new Portfolio(4));

      regionOnServer.put(0, new Portfolio(0));
      regionOnServer.put(1, new Portfolio(1));
      regionOnServer.put(2, new Portfolio(2));
      regionOnServer.put(3, new Portfolio(3));
      regionOnServer.put(4, new Portfolio(4));
    });

    await()
        .untilAsserted(() -> assertEquals(3, testListener.onEventCalls));
  }

  @Test
  public void cqExecuteWithInitialResultsWithValuesMatchingPrimaryKey_suppressUpdate_false()
      throws Exception {
    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      ClusterStartupRule.getCache().getQueryService().createKeyIndex("PrimaryKeyIndex", "ID",
          SEPARATOR + "region");
      regionOnServer.put(0, new Portfolio(0));
      regionOnServer.put(1, new Portfolio(1));
      regionOnServer.put(2, new Portfolio(2));
      regionOnServer.put(3, new Portfolio(3));
      regionOnServer.put(4, new Portfolio(4));

    });

    SelectResults results =
        qs.newCq("Select * from " + SEPARATOR + "region where ID = 1", cqa, false, 0)
            .executeWithInitialResults();
    assertEquals(1, results.size());

    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put(0, new Portfolio(0));
      regionOnServer.put(1, new Portfolio(1));
      regionOnServer.put(2, new Portfolio(2));
      regionOnServer.put(3, new Portfolio(3));
      regionOnServer.put(4, new Portfolio(4));

      regionOnServer.put(0, new Portfolio(0));
      regionOnServer.put(1, new Portfolio(1));
      regionOnServer.put(2, new Portfolio(2));
      regionOnServer.put(3, new Portfolio(3));
      regionOnServer.put(4, new Portfolio(4));
    });

    await()
        .untilAsserted(() -> assertEquals(2, testListener.onEventCalls));
  }

  @Test
  public void cqExecuteWithInitialResultsWithValuesMatchingPrimaryKey_suppressUpdate_true()
      throws Exception {
    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      ClusterStartupRule.getCache().getQueryService().createKeyIndex("PrimaryKeyIndex", "ID",
          SEPARATOR + "region");
      regionOnServer.put(0, new Portfolio(0));
      regionOnServer.put(1, new Portfolio(1));
      regionOnServer.put(2, new Portfolio(2));
      regionOnServer.put(3, new Portfolio(3));
      regionOnServer.put(4, new Portfolio(4));
    });

    SelectResults results =
        qs.newCq("Select * from " + SEPARATOR + "region where ID = 1", cqa, false, 2)
            .executeWithInitialResults();
    assertEquals(1, results.size());

    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put(0, new Portfolio(0));
      regionOnServer.put(1, new Portfolio(1));
      regionOnServer.put(2, new Portfolio(2));
      regionOnServer.put(3, new Portfolio(3));
      regionOnServer.put(4, new Portfolio(4));

      regionOnServer.put(0, new Portfolio(0));
      regionOnServer.put(1, new Portfolio(1));
      regionOnServer.put(2, new Portfolio(2));
      regionOnServer.put(3, new Portfolio(3));
      regionOnServer.put(4, new Portfolio(4));
    });

    await()
        .untilAsserted(() -> assertEquals(0, testListener.onEventCalls));
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

  private class TestCqListener2 implements CqListener, Serializable {
    public int onEventCreateCalls = 0;
    public int onEventUpdateCalls = 0;

    @Override
    public void onEvent(CqEvent aCqEvent) {
      Operation queryOperation = aCqEvent.getQueryOperation();
      if (queryOperation.isCreate()) {
        onEventCreateCalls++;
      } else if (queryOperation.isUpdate()) {
        onEventUpdateCalls++;
      }
    }

    @Override
    public void onError(CqEvent aCqEvent) {}

    @Override
    public void close() {}
  }


  private void createServerRegion(MemberVM server, RegionShortcut regionShortcut) {
    server.invoke(() -> {
      clusterStartupRule.getCache().createRegionFactory(regionShortcut).create("region");
    });
  }

  private ClientCache createClientCache(Integer locator1Port) {
    ClientCacheFactory ccf = new ClientCacheFactory();
    ccf.addPoolLocator("localhost", locator1Port);
    ccf.setPoolSubscriptionEnabled(true);
    return ccf.create();
  }

}
