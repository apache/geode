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

package org.apache.geode.cache.query.partitioned;

import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.junit.rules.VMProvider.invokeInEveryMember;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.City;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DUnitBlackboard;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ClientCacheRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class PRClearQueryIndexDUnitTest {
  public static final String MUMBAI_QUERY = "select * from /cities c where c.name = 'MUMBAI'";
  public static final String ID_10_QUERY = "select * from /cities c where c.id = 10";
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(4, true);

  private static MemberVM server1;
  private static MemberVM server2;

  private static DUnitBlackboard blackboard;

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public ExecutorServiceRule executor = ExecutorServiceRule.builder().build();

  private ClientCache clientCache;
  private Region cities;

  // class test setup. set up the servers, regions and indexes on the servers
  @BeforeClass
  public static void beforeClass() {
    int locatorPort = ClusterStartupRule.getDUnitLocatorPort();
    server1 = cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort)
        .withProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.query.data.*")
        .withRegion(RegionShortcut.PARTITION, "cities"));
    server2 = cluster.startServerVM(2, s -> s.withConnectionToLocator(locatorPort)
        .withProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.query.data.*")
        .withRegion(RegionShortcut.PARTITION, "cities"));

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion("cities");
      // create indexes
      QueryService queryService = cache.getQueryService();
      queryService.createKeyIndex("cityId", "c.id", "/cities c");
      queryService.createIndex("cityName", "c.name", "/cities c");
      assertThat(cache.getQueryService().getIndexes(region))
          .extracting(Index::getName).containsExactlyInAnyOrder("cityId", "cityName");
    });

    server2.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion("cities");
      assertThat(cache.getQueryService().getIndexes(region))
          .extracting(Index::getName).containsExactlyInAnyOrder("cityId", "cityName");
    });
  }

  // before every test method, create the client cache and region
  @Before
  public void before() throws Exception {
    int locatorPort = ClusterStartupRule.getDUnitLocatorPort();
    clientCache = clientCacheRule.withLocatorConnection(locatorPort).createCache();
    cities = clientCacheRule.createProxyRegion("cities");
  }

  @Test
  public void clearOnEmptyRegion() throws Exception {
    cities.clear();
    invokeInEveryMember(() -> {
      verifyIndexesAfterClear("cities", "cityId", "cityName");
    }, server1, server2);

    IntStream.range(0, 10).forEach(i -> cities.put(i, new City(i)));
    cities.clear();
    invokeInEveryMember(() -> {
      verifyIndexesAfterClear("cities", "cityId", "cityName");
    }, server1, server2);
  }

  @Test
  public void createIndexWhileClear() throws Exception {
    IntStream.range(0, 1000).forEach(i -> cities.put(i, new City(i)));

    // create index while clear
    AsyncInvocation createIndex = server1.invokeAsync("create index", () -> {
      Cache cache = ClusterStartupRule.getCache();
      QueryService queryService = cache.getQueryService();
      Index cityZip = queryService.createIndex("cityZip", "c.zip", "/cities c");
      assertThat(cityZip).isNotNull();
    });

    // do clear for 3 times at the same time to increease the concurrency of clear and createIndex
    for (int i = 0; i < 3; i++) {
      cities.clear();
    }
    createIndex.await();

    invokeInEveryMember(() -> {
      verifyIndexesAfterClear("cities", "cityId", "cityName");
    }, server1, server2);

    QueryService queryService = clientCache.getQueryService();
    Query query =
        queryService.newQuery("select * from /cities c where c.zip < " + (City.ZIP_START + 10));
    assertThat(((SelectResults) query.execute()).size()).isEqualTo(0);

    IntStream.range(0, 10).forEach(i -> cities.put(i, new City(i)));
    assertThat(((SelectResults) query.execute()).size()).isEqualTo(10);
  }

  @Test
  public void createIndexWhileClearOnReplicateRegion() throws Exception {
    invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION)
          .create("replicateCities");
    }, server1, server2);

    Region replicateCities = clientCacheRule.createProxyRegion("replicateCities");
    IntStream.range(0, 1000).forEach(i -> replicateCities.put(i, new City(i)));

    // create index while clear
    AsyncInvocation createIndex = server1.invokeAsync("create index on replicate regions", () -> {
      Cache cache = ClusterStartupRule.getCache();
      QueryService queryService = cache.getQueryService();
      Index cityZip = queryService.createIndex("cityZip_replicate", "c.zip", "/replicateCities c");
      assertThat(cityZip).isNotNull();
    });

    // do clear at the same time for 3 timese
    for (int i = 0; i < 3; i++) {
      replicateCities.clear();
    }
    createIndex.await();

    invokeInEveryMember(() -> {
      verifyIndexesAfterClear("replicateCities", "cityZip_replicate");
    }, server1, server2);

    QueryService queryService = clientCache.getQueryService();
    Query query =
        queryService
            .newQuery("select * from /replicateCities c where c.zip < " + (City.ZIP_START + 10));
    assertThat(((SelectResults) query.execute()).size()).isEqualTo(0);

    IntStream.range(0, 10).forEach(i -> replicateCities.put(i, new City(i)));
    assertThat(((SelectResults) query.execute()).size()).isEqualTo(10);
  }

  @Test
  public void removeIndexWhileClear() throws Exception {
    // create cityZip index
    server1.invoke("create index", () -> {
      Cache cache = ClusterStartupRule.getCache();
      QueryService queryService = cache.getQueryService();
      Index cityZip = queryService.createIndex("cityZip", "c.zip", "/cities c");
      assertThat(cityZip).isNotNull();
    });

    // remove index while clear
    // removeIndex has to be invoked on each server. It's not distributed
    AsyncInvocation removeIndex1 = server1.invokeAsync("remove index",
        PRClearQueryIndexDUnitTest::removeCityZipIndex);
    AsyncInvocation removeIndex2 = server2.invokeAsync("remove index",
        PRClearQueryIndexDUnitTest::removeCityZipIndex);

    cities.clear();
    removeIndex1.await();
    removeIndex2.await();

    // make sure removeIndex and clear operations are successful
    invokeInEveryMember(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      QueryService qs = internalCache.getQueryService();
      Region region = internalCache.getRegion("cities");
      assertThat(region.size()).isEqualTo(0);
      // verify only 2 indexes created in the beginning of the tests exist
      assertThat(qs.getIndexes(region)).extracting(Index::getName)
          .containsExactlyInAnyOrder("cityId", "cityName");
    }, server1, server2);
  }

  private static void removeCityZipIndex() {
    Cache cache = ClusterStartupRule.getCache();
    QueryService qs = cache.getQueryService();
    Region<Object, Object> region = cache.getRegion("cities");
    Index cityZip = qs.getIndex(region, "cityZip");
    if (cityZip != null) {
      qs.removeIndex(cityZip);
    }
  }

  @Test
  public void verifyQuerySucceedsAfterClear() throws Exception {
    // put in some data
    IntStream.range(0, 100).forEach(i -> cities.put(i, new City(i)));

    QueryService queryService = clientCache.getQueryService();
    Query query = queryService.newQuery(MUMBAI_QUERY);
    Query query2 = queryService.newQuery(ID_10_QUERY);
    assertThat(((SelectResults) query.execute()).size()).isEqualTo(50);
    assertThat(((SelectResults) query2.execute()).size()).isEqualTo(1);

    cities.clear();
    invokeInEveryMember(() -> {
      verifyIndexesAfterClear("cities", "cityId", "cityName");
    }, server1, server2);

    assertThat(((SelectResults) query.execute()).size()).isEqualTo(0);
    assertThat(((SelectResults) query2.execute()).size()).isEqualTo(0);
  }

  private static void verifyIndexesAfterClear(String regionName, String... indexes) {
    InternalCache internalCache = ClusterStartupRule.getCache();
    QueryService qs = internalCache.getQueryService();
    Region region = internalCache.getRegion(regionName);
    assertThat(region.size()).isEqualTo(0);
    for (String indexName : indexes) {
      Index index = qs.getIndex(region, indexName);
      IndexStatistics statistics = index.getStatistics();
      assertThat(statistics.getNumberOfKeys()).isEqualTo(0);
      assertThat(statistics.getNumberOfValues()).isEqualTo(0);
    }
  }

  @Test
  public void concurrentClearAndQuery() {
    QueryService queryService = clientCache.getQueryService();
    Query query = queryService.newQuery(MUMBAI_QUERY);
    Query query2 = queryService.newQuery(ID_10_QUERY);

    IntStream.range(0, 100).forEach(i -> cities.put(i, new City(i)));

    server1.invokeAsync(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion("cities");
      region.clear();
    });

    await().untilAsserted(() -> {
      assertThat(((SelectResults) query.execute()).size()).isEqualTo(0);
      assertThat(((SelectResults) query2.execute()).size()).isEqualTo(0);
    });
  }

  @Test
  public void concurrentClearAndPut() throws Exception {
    AsyncInvocation puts = server1.invokeAsync(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion("cities");
      for (int i = 0; i < 1000; i++) {
        // wait for gate to open
        getBlackboard().waitForGate("proceedToPut", 60, TimeUnit.SECONDS);
        region.put(i, new City(i));
      }
    });

    AsyncInvocation clears = server2.invokeAsync(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion("cities");
      // do clear 10 times
      for (int i = 0; i < 10; i++) {
        try {
          // don't allow put to proceed. It's like "close the gate"
          getBlackboard().clearGate("proceedToPut");
          region.clear();
          verifyIndexesAfterClear("cities", "cityId", "cityName");
        } finally {
          // allow put to proceed. It's like "open the gate"
          getBlackboard().signalGate("proceedToPut");
        }
      }
    });

    puts.await();
    clears.await();
  }

  @Test
  public void serverLeavingAndJoiningWhilePutAndClear() throws Exception {
    int locatorPort = ClusterStartupRule.getDUnitLocatorPort();
    Future<Void> startStopServer = executor.submit(() -> {
      for (int i = 0; i < 3; i++) {
        MemberVM server3 = cluster.startServerVM(3, s -> s.withConnectionToLocator(locatorPort)
            .withProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.query.data.*")
            .withRegion(RegionShortcut.PARTITION, "cities"));
        server3.stop(false);
      }
    });

    Future<Void> putAndClear = executor.submit(() -> {
      for (int i = 0; i < 30; i++) {
        IntStream.range(0, 100).forEach(j -> cities.put(j, new City(j)));
        try {
          cities.clear();

          // only verify if clear is successful
          QueryService queryService = clientCache.getQueryService();
          Query query = queryService.newQuery(MUMBAI_QUERY);
          Query query2 = queryService.newQuery(ID_10_QUERY);
          assertThat(((SelectResults) query.execute()).size()).isEqualTo(0);
          assertThat(((SelectResults) query2.execute()).size()).isEqualTo(0);
        } catch (ServerOperationException e) {
          assertThat(e.getCause().getMessage())
              .contains("Unable to clear all the buckets from the partitioned region cities")
              .contains("either data (buckets) moved or member departed");
        }
      }
    });
    startStopServer.get(60, TimeUnit.SECONDS);
    putAndClear.get(60, TimeUnit.SECONDS);
  }

  private static DUnitBlackboard getBlackboard() {
    if (blackboard == null) {
      blackboard = new DUnitBlackboard();
    }
    return blackboard;
  }

  @After
  public void tearDown() {
    invokeInEveryMember(() -> {
      if (blackboard != null) {
        blackboard.clearGate("proceedToPut");
      }
      // remove the cityZip index
      removeCityZipIndex();
    }, server1, server2);
  }
}
