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
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.City;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ClientCacheRule;

public class PRClearQueryIndexDUnitTest {
  public static final String MUMBAI_QUERY = "select * from /cities c where c.name = 'MUMBAI'";
  public static final String ID_10_QUERY = "select * from /cities c where c.id = 10";
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(2, true);

  private static MemberVM server1;
  private static MemberVM server2;

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  private ClientCache clientCache;

  // class test setup. set up the servers, regions and indexes on the servers
  @BeforeClass
  public static void beforeClass() {
    int locatorPort = ClusterStartupRule.getDUnitLocatorPort();
    server1 = cluster.startServerVM(0, s -> s.withConnectionToLocator(locatorPort)
        .withProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.query.data.*")
        .withRegion(RegionShortcut.PARTITION, "cities"));
    server2 = cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort)
        .withProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.query.data.*")
        .withRegion(RegionShortcut.PARTITION, "cities"));

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion("cities");
      // create indexes
      QueryService queryService = cache.getQueryService();
      queryService.createKeyIndex("cityId", "c.id", "/cities c");
      queryService.createIndex("cityName", "c.name", "/cities c");
      assertThat(cache.getQueryService().getIndexes(region)).hasSize(2);
    });

    server2.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion("cities");
      assertThat(cache.getQueryService().getIndexes(region)).hasSize(2);
    });
  }

  // before every test method, create the client cache and region
  @Before
  public void before() throws Exception {
    int locatorPort = ClusterStartupRule.getDUnitLocatorPort();
    clientCache = clientCacheRule.withLocatorConnection(locatorPort).createCache();
  }

  @Test
  public void verifyQuerySucceedsAfterClear() throws Exception {
    Region cities = clientCacheRule.createProxyRegion("cities");
    // put in some data
    IntStream.range(0, 100).forEach(i -> cities.put(i, new City(i)));

    MemberVM.invokeInEveryMember(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      Region region = internalCache.getRegion("cities");
      assertThat(region.size()).isEqualTo(100);
    }, server1, server2);

    QueryService queryService = clientCache.getQueryService();
    Query query = queryService.newQuery(MUMBAI_QUERY);
    Query query2 = queryService.newQuery(ID_10_QUERY);
    assertThat(((SelectResults) query.execute()).size()).isEqualTo(50);
    assertThat(((SelectResults) query2.execute()).size()).isEqualTo(1);

    cities.clear();
    MemberVM.invokeInEveryMember(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      Region region = internalCache.getRegion("cities");
      assertThat(region.size()).isEqualTo(0);
    }, server1, server2);

    assertThat(((SelectResults) query.execute()).size()).isEqualTo(0);
    assertThat(((SelectResults) query2.execute()).size()).isEqualTo(0);

    MemberVM.invokeInEveryMember(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      QueryService qs = internalCache.getQueryService();
      Region region = internalCache.getRegion("cities");
      Index cityId = qs.getIndex(region, "cityId");
      IndexStatistics statistics = cityId.getStatistics();
      assertThat(statistics.getNumberOfKeys()).isEqualTo(0);
      assertThat(statistics.getNumberOfValues()).isEqualTo(0);
      Index cityName = qs.getIndex(region, "cityName");
      IndexStatistics statistics1 = cityName.getStatistics();
      assertThat(statistics1.getNumberOfKeys()).isEqualTo(0);
      assertThat(statistics1.getNumberOfValues()).isEqualTo(0);
    }, server1, server2);
  }

  @Test
  public void concurrentClearAndQuery() {
    QueryService queryService = clientCache.getQueryService();
    Query query = queryService.newQuery(MUMBAI_QUERY);
    Query query2 = queryService.newQuery(ID_10_QUERY);

    server1.invokeAsync(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion("cities");
      IntStream.range(0, 100).forEach(i -> region.put(i, new City(i)));
      region.clear();
    });

    await().pollDelay(5, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      assertThat(((SelectResults) query.execute()).size()).isEqualTo(0);
      assertThat(((SelectResults) query2.execute()).size()).isEqualTo(0);
    });
  }

}
