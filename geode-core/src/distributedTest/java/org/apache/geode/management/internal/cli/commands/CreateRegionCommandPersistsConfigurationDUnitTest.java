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
package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.geode.cache.Region;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.ExpirationAttributesType;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({RegionsTest.class})
public class CreateRegionCommandPersistsConfigurationDUnitTest {

  private MemberVM locator, server1, server2;

  @Rule
  public ClusterStartupRule clusterRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TestName testName = new SerializableTestName();

  @Before
  public void before() throws Exception {
    locator = clusterRule.startLocatorVM(0);
    server1 = clusterRule.startServerVM(1, locator.getPort());
    server2 = clusterRule.startServerVM(2, locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void testCreateRegionPersistsConfig() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();

    server1.getVM().bounce();
    server2.getVM().bounce();
    server1 = clusterRule.startServerVM(1, "group1", locator.getPort());
    server2 = clusterRule.startServerVM(2, "group2", locator.getPort());

    gfsh.executeAndAssertThat("list regions")
        .statusIsSuccess().containsOutput(regionName);

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      RegionConfig regionConfig = regions.get(0);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getName()).isEqualTo(regionName);
      assertThat(regionConfig.getIndexes()).isEmpty();
      assertThat(regionConfig.getRegions()).isEmpty();
      assertThat(regionConfig.getEntries()).isEmpty();
      assertThat(regionConfig.getCustomRegionElements()).isEmpty();
      assertThat(regionConfig.getRegionAttributes()).isEmpty();
    });
  }

  @Test
  public void testCreateRegionPersistsConfigParams() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=PARTITION"
        + " --enable-statistics=true" + " --enable-async-conflation=true"
        + " --entry-idle-time-expiration=100").statusIsSuccess();

    server1.getVM().bounce();
    server2.getVM().bounce();
    server1 = clusterRule.startServerVM(1, "group1", locator.getPort());
    server2 = clusterRule.startServerVM(2, "group2", locator.getPort());

    gfsh.executeAndAssertThat("list regions")
        .statusIsSuccess().containsOutput(regionName);

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      RegionConfig regionConfig = regions.get(0);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getName()).isEqualTo(regionName);
      assertThat(regionConfig.getRegionAttributes()).hasSize(1);

      RegionAttributesType attr = regionConfig.getRegionAttributes().get(0);
      assertThat(attr.isStatisticsEnabled()).isTrue();
      assertThat(attr.isEnableAsyncConflation()).isTrue();

      ExpirationAttributesType entryIdleTimeExp = attr.getEntryIdleTime().getExpirationAttributes();
      assertThat(entryIdleTimeExp.getTimeout()).isEqualTo("100");
    });

    server1.invoke(() -> {
      Region<?, ?> region = ClusterStartupRule.getCache().getRegion(regionName);
      assertThat(region.getAttributes().getStatisticsEnabled())
              .describedAs("Expecting statistics to be enabled")
              .isTrue();
      assertThat(region.getAttributes().getEnableAsyncConflation())
              .describedAs("Expecting async conflation to be enabled")
              .isTrue();
      assertThat(region.getAttributes().getEntryIdleTimeout().getTimeout())
              .describedAs("Expecting entry idle time exp timeout to be 100")
              .isEqualTo(100);
    });
  }
}
