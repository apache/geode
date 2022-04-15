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

import java.io.IOException;
import java.io.Serializable;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class DistributionStatsNodesDistributedTest implements Serializable {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Test
  public void testNodesStatistic() {
    // Start Locator
    MemberVM locator = cluster.startLocatorVM(0);

    // Start server
    MemberVM server = cluster.startServerVM(1, s -> s.withConnectionToLocator(locator.getPort()));

    // Verify DistributionStats nodes is 1
    server.invoke(() -> verifyNodesStatistic(1));
  }

  @Test
  public void testDecrement() {
    // Start Locator
    MemberVM locator = cluster.startLocatorVM(0);

    // Start server
    MemberVM server = cluster.startServerVM(1, s -> s.withConnectionToLocator(locator.getPort()));

    CacheFactory cacheFactory = new CacheFactory();
    InternalCache internalCache = (InternalCache) cacheFactory
        .set("locators", "localhost[" + locator.getPort() + "]").create();

    DistributionManager distributionManager = internalCache.getDistributionManager();
    DistributionStats distributionStats = (DistributionStats) distributionManager.getStats();
    assertThat(distributionStats.getNodes()).isEqualTo(2);

    server.invoke(() -> ClusterStartupRule.getCache().close());

    assertThat(distributionStats.getNodes()).isEqualTo(1);
  }

  @Test
  public void testDuplicateEntry() {
    // Start Locator
    MemberVM locator = cluster.startLocatorVM(0);

    // Start server
    MemberVM server = cluster.startServerVM(1, s -> s.withConnectionToLocator(locator.getPort()));

    CacheFactory cacheFactory = new CacheFactory();
    InternalCache internalCache = (InternalCache) cacheFactory
        .set("locators", "localhost[" + locator.getPort() + "]").create();

    DistributionManager distributionManager = internalCache.getDistributionManager();
    DistributionStats distributionStats = (DistributionStats) distributionManager.getStats();
    assertThat(distributionStats.getNodes()).isEqualTo(2);

    server.stop();
    cluster.startServerVM(1, s -> s.withConnectionToLocator(locator.getPort()));

    assertThat(distributionStats.getNodes()).isEqualTo(2);
  }

  @Test
  public void testNewTest() {
    // Start Locator
    MemberVM locator = cluster.startLocatorVM(0);

    // Start server
    MemberVM server = cluster.startServerVM(1, s -> s.withConnectionToLocator(locator.getPort()));

    CacheFactory cacheFactory = new CacheFactory();
    InternalCache internalCache = (InternalCache) cacheFactory
        .set("locators", "localhost[" + locator.getPort() + "]").create();

    DistributionManager distributionManager = internalCache.getDistributionManager();
    DistributionStats distributionStats = (DistributionStats) distributionManager.getStats();
    assertThat(distributionStats.getNodes()).isEqualTo(2);

    VM server2VM = VM.getVM(2);
    server2VM.invoke(() -> {
      CacheFactory cacheFactory2 = new CacheFactory();
      InternalCache internalCache3 = (InternalCache) cacheFactory2
          .set("locators", "localhost[" + locator.getPort() + "]").create();

      DistributionManager distributionManager3 = internalCache3.getDistributionManager();
      DistributionStats distributionStats3 = (DistributionStats) distributionManager3.getStats();
      assertThat(distributionStats3.getNodes()).isEqualTo(3);
    });

    // Verify DistributionStats nodes is 1
    server.invoke(() -> {
      InternalCache internalCache2 = ClusterStartupRule.getCache();

      DistributionManager distributionManager2 = internalCache2.getDistributionManager();
      DistributionStats distributionStats2 = (DistributionStats) distributionManager2.getStats();
      assertThat(distributionStats2.getNodes()).isEqualTo(3);
    });
  }

  private void verifyNodesStatistic(int numNodes) {
    DistributionManager distributionManager =
        ClusterStartupRule.getCache().getDistributionManager();
    DistributionStats distributionStats = (DistributionStats) distributionManager.getStats();
    assertThat(distributionStats.getNodes()).isEqualTo(numNodes);
  }
}
