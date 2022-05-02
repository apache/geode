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

package org.apache.geode.distributed.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class DistributionStatsNodesDistributedTest {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  private MemberVM locator;
  private MemberVM server;

  @Before
  public void before() throws Exception {
    // Start Locator
    locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    // Start server
    server = cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort));
  }

  @Test
  public void testNodesStatistic() {
    // Verify DistributionStats nodes is 1
    server.invoke(() -> verifyNodesStatistic(1));
  }

  @Test
  public void testDecrement() {
    InternalCache internalCache = createCache(locator.getPort());

    DistributionManager distributionManager = internalCache.getDistributionManager();
    DistributionStats distributionStats = (DistributionStats) distributionManager.getStats();
    assertThat(distributionStats.getNodes()).isEqualTo(2);

    server.invoke(() -> ClusterStartupRule.getCache().getDistributionManager().getDistribution()
        .disconnect(false));

    assertThat(distributionStats.getNodes()).isEqualTo(1);
  }

  @Test
  public void testDuplicateEntry() {
    int locatorPort = locator.getPort();
    InternalCache internalCache = createCache(locatorPort);

    DistributionManager distributionManager = internalCache.getDistributionManager();
    DistributionStats distributionStats = (DistributionStats) distributionManager.getStats();
    assertThat(distributionStats.getNodes()).isEqualTo(2);

    server.stop();
    cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort));

    assertThat(distributionStats.getNodes()).isEqualTo(2);
  }

  @Test
  public void testNewServerWillUpdateTheStats() {
    int locatorPort = locator.getPort();
    InternalCache internalCache = createCache(locatorPort);

    DistributionManager distributionManager = internalCache.getDistributionManager();
    DistributionStats distributionStats = (DistributionStats) distributionManager.getStats();
    assertThat(distributionStats.getNodes()).isEqualTo(2);

    VM server2VM = VM.getVM(2);
    server2VM.invoke(() -> {
      InternalCache internalCache3 = createCache(locatorPort);
      DistributionManager distributionManager3 = internalCache3.getDistributionManager();
      DistributionStats distributionStats3 = (DistributionStats) distributionManager3.getStats();
      assertThat(distributionStats3.getNodes()).isEqualTo(3);
    });

    // Verify DistributionStats nodes is updated
    server.invoke(() -> {
      InternalCache internalCache2 = ClusterStartupRule.getCache();
      DistributionManager distributionManager2 = internalCache2.getDistributionManager();
      DistributionStats distributionStats2 = (DistributionStats) distributionManager2.getStats();
      assertThat(distributionStats2.getNodes()).isEqualTo(3);
    });
  }

  private static void verifyNodesStatistic(int numNodes) {
    DistributionManager distributionManager =
        ClusterStartupRule.getCache().getDistributionManager();
    DistributionStats distributionStats = (DistributionStats) distributionManager.getStats();
    assertThat(distributionStats.getNodes()).isEqualTo(numNodes);
  }

  private static InternalCache createCache(int locatorPort) {
    CacheFactory cacheFactory = new CacheFactory();
    return (InternalCache) cacheFactory
        .set("locators", "localhost[" + locatorPort + "]").create();
  }
}
