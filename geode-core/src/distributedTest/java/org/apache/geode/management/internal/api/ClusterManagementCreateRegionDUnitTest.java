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
package org.apache.geode.management.internal.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({RegionsTest.class})
public class ClusterManagementCreateRegionDUnitTest {
  private MemberVM locator, server;

  @Rule
  public ClusterStartupRule clusterRule = new ClusterStartupRule();

  @Rule
  public TestName testName = new SerializableTestName();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void before() throws Exception {
    locator = clusterRule.startLocatorVM(0);
    server = clusterRule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void createsPartitionedRegion() {
    String regionName = testName.getMethodName();
    locator.invoke(() -> {
      RegionConfig config = new RegionConfig();
      config.setName(regionName);
      config.setRefid(RegionShortcut.PARTITION.toString());
      ClusterManagementService svc = ClusterManagementServiceProvider.getService();
      ClusterManagementResult result = svc.createCacheElement(config);
      assertThat(result.isSuccessful()).isTrue();
    });


    gfsh.executeAndAssertThat("list regions")
        .statusIsSuccess()
        .containsOutput(regionName);

    server.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion(regionName);
      assertThat(region).isNotNull();
      assertThat(region.getAttributes().getDataPolicy()).isEqualTo(DataPolicy.PARTITION);
    });

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/" + regionName, 1);

    gfsh.executeAndAssertThat("put --key='foo' --value='125' --region=" + regionName)
        .statusIsSuccess();
    gfsh.executeAndAssertThat("get --key='foo' --region=" + regionName)
        .statusIsSuccess()
        .containsKeyValuePair("Value", "\"125\"");
  }

  @Test
  public void createsReplicatedRegion() {
    String regionName = testName.getMethodName();
    locator.invoke(() -> {
      RegionConfig config = new RegionConfig();
      config.setName(regionName);
      config.setRefid(RegionShortcut.REPLICATE.toString());
      ClusterManagementService svc = ClusterManagementServiceProvider.getService();
      ClusterManagementResult result = svc.createCacheElement(config);
      assertThat(result.isSuccessful()).isTrue();
    });

    server.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion(regionName);
      assertThat(region).isNotNull();
      assertThat(region.getAttributes().getDataPolicy()).isEqualTo(DataPolicy.REPLICATE);
    });

    gfsh.executeAndAssertThat("list regions").statusIsSuccess()
        .containsOutput(regionName);
  }

  @Test
  public void createRegionPersists() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --name=Dummy --type=PARTITION").statusIsSuccess();

    locator.invoke(() -> {
      RegionConfig config = new RegionConfig();
      config.setName(regionName);
      config.setRefid(RegionShortcut.PARTITION.toString());
      ClusterManagementService svc = ClusterManagementServiceProvider.getService();
      ClusterManagementResult result = svc.createCacheElement(config);
      assertThat(result.isSuccessful()).isTrue();
    });

    gfsh.executeAndAssertThat("list regions")
        .statusIsSuccess()
        .containsOutput(regionName);

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      RegionConfig regionConfig = regions.get(1);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getName()).isEqualTo(regionName);
      assertThat(regionConfig.getRefid()).isEqualTo("PARTITION");
      assertThat(regionConfig.getIndexes()).isEmpty();
      assertThat(regionConfig.getRegions()).isEmpty();
      assertThat(regionConfig.getEntries()).isEmpty();
      assertThat(regionConfig.getCustomRegionElements()).isEmpty();
    });
  }
}
