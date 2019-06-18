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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({RegionsTest.class})
public class RegionAPIDUnitTest {
  private static MemberVM locator, server;

  @ClassRule
  public static ClusterStartupRule clusterRule = new ClusterStartupRule();

  @Rule
  public TestName testName = new SerializableTestName();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void before() throws Exception {
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
      config.setType(RegionType.PARTITION);
      ClusterManagementResult<RegionConfig> result =
          ClusterStartupRule.getLocator().getClusterManagementService()
              .create(config);
      assertThat(result.isSuccessful()).isTrue();
    });

    server.invoke(() -> verifyRegionCreated(regionName, "PARTITION"));

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/" + regionName, 1);

    gfsh.executeAndAssertThat("put --key='foo' --value='125' --region=" + regionName)
        .statusIsSuccess();
    gfsh.executeAndAssertThat("get --key='foo' --region=" + regionName)
        .statusIsSuccess()
        .containsKeyValuePair("Value", "\"125\"");

    locator.invoke(() -> verifyRegionPersisted(regionName, "PARTITION"));
  }

  @Test
  public void createsReplicatedRegion() {
    String regionName = testName.getMethodName();
    locator.invoke(() -> {
      RegionConfig config = new RegionConfig();
      config.setName(regionName);
      config.setType(RegionType.REPLICATE);
      ClusterManagementResult<RegionConfig> result =
          ClusterStartupRule.getLocator().getClusterManagementService()
              .create(config);
      assertThat(result.isSuccessful()).isTrue();
    });

    server.invoke(() -> verifyRegionCreated(regionName, "REPLICATE"));

    locator.invoke(() -> verifyRegionPersisted(regionName, "REPLICATE"));
  }

  @Test
  public void createPartitionedRegion() throws Exception {
    String regionName = testName.getMethodName();
    locator.invoke(() -> {
      RegionConfig config = new RegionConfig();
      config.setName(regionName);
      config.setType(RegionType.PARTITION);
      ClusterManagementResult<RegionConfig> result =
          ClusterStartupRule.getLocator().getClusterManagementService()
              .create(config);
      assertThat(result.isSuccessful()).isTrue();
    });

    server.invoke(() -> verifyRegionCreated(regionName, "PARTITION"));
    locator.invoke(() -> verifyRegionPersisted(regionName, "PARTITION"));
  }

  private static void verifyRegionPersisted(String regionName, String type) {
    CacheConfig cacheConfig =
        ClusterStartupRule.getLocator().getConfigurationPersistenceService()
            .getCacheConfig("cluster");
    RegionConfig regionConfig = CacheElement.findElement(cacheConfig.getRegions(), regionName);
    assertThat(regionConfig.getType()).isEqualTo(type);
  }

  private static void verifyRegionCreated(String regionName, String type) {
    Cache cache = ClusterStartupRule.getCache();
    Region region = cache.getRegion(regionName);
    assertThat(region).isNotNull();
    assertThat(region.getAttributes().getDataPolicy().toString()).isEqualTo(type);
  }
}
