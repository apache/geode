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

package org.apache.geode.management.internal.rest;

import static org.apache.geode.test.junit.assertions.ClusterManagementListResultAssert.assertManagementListResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.IndexType;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.management.runtime.IndexInfo;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.MemberStarterRule;

public class ListIndexOnPartitionRegionTest {
  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  private static ClusterManagementService cms;
  private static MemberVM locator;

  @BeforeClass
  public static void beforeClass() {
    locator = lsRule.startLocatorVM(0, MemberStarterRule::withHttpService);
    lsRule.startServerVM(1, "group1", locator.getPort());
    lsRule.startServerVM(2, "group2", locator.getPort());
    lsRule.startServerVM(3, "group3", locator.getPort());

    cms = new ClusterManagementServiceBuilder()
        .setPort(locator.getHttpPort())
        .build();

    // set up the same region in 3 different groups, one is a proxy region, but with the same name
    Region config = new Region();
    config.setName("testRegion");
    config.setType(RegionType.PARTITION);
    config.setGroup("group1");
    cms.create(config);

    config.setType(RegionType.PARTITION);
    config.setGroup("group2");
    cms.create(config);

    config.setType(RegionType.PARTITION_PROXY);
    config.setGroup("group3");
    cms.create(config);

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/testRegion", 3);
  }

  @Test
  public void createAndListIndex() {
    // create index on group2
    Index index = new Index();
    index.setName("index");
    index.setExpression("id");
    index.setRegionPath("/testRegion");
    index.setIndexType(IndexType.KEY);
    cms.create(index);

    // verify that index configuration is saved on all groups
    locator.invoke(() -> {
      InternalConfigurationPersistenceService cps =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig group1 = cps.getCacheConfig("group1");
      assertThat(group1.findRegionConfiguration("testRegion").getIndexes())
          .extracting(RegionConfig.Index::getName).containsExactly("index");
      CacheConfig group2 = cps.getCacheConfig("group2");
      assertThat(group2.findRegionConfiguration("testRegion").getIndexes())
          .extracting(RegionConfig.Index::getName).containsExactly("index");
      CacheConfig group3 = cps.getCacheConfig("group3");
      assertThat(group3.findRegionConfiguration("testRegion").getIndexes())
          .extracting(RegionConfig.Index::getName).containsExactly("index");
    });

    ClusterManagementListResult<Index, IndexInfo> list = cms.list(new Index());
    // all servers should have this index
    assertManagementListResult(list).hasRuntimeInfos().hasSize(3);
    // even though this index configuration exists on 3 groups, it should only return one back
    // since they are identical
    assertManagementListResult(list).hasConfigurations().hasSize(1);

    // create same region on group4
    Region config = new Region();
    config.setName("testRegion");
    config.setType(RegionType.PARTITION);
    config.setGroup("group4");
    cms.create(config);

    // assert group4 region has no index
    locator.invoke(() -> {
      InternalConfigurationPersistenceService cps =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig group4 = cps.getCacheConfig("group4");
      assertThat(group4.findRegionConfiguration("testRegion").getIndexes()).isEmpty();
    });

    // try to create index again will throw an error since index already exists on some groups
    assertThatThrownBy(() -> cms.create(index))
        .hasMessageContaining("Index 'index' already exists");

    // demonstrate that the only way to put the index on the newly created region on group4 is
    // to delete the index and then recreate the index again.

    // delete the index
    ClusterManagementRealizationResult deleteResult = cms.delete(index);
    assertThat(deleteResult.getStatusMessage()).contains("Successfully updated configuration")
        .contains("group1").contains("group2").contains("group3").doesNotContain("group4");

    // create the index again
    cms.create(index);

    // verify that index configuration is saved on all groups
    locator.invoke(() -> {
      InternalConfigurationPersistenceService cps =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig group1 = cps.getCacheConfig("group1");
      assertThat(group1.findRegionConfiguration("testRegion").getIndexes())
          .extracting(RegionConfig.Index::getName).containsExactly("index");
      CacheConfig group2 = cps.getCacheConfig("group2");
      assertThat(group2.findRegionConfiguration("testRegion").getIndexes())
          .extracting(RegionConfig.Index::getName).containsExactly("index");
      CacheConfig group3 = cps.getCacheConfig("group3");
      assertThat(group3.findRegionConfiguration("testRegion").getIndexes())
          .extracting(RegionConfig.Index::getName).containsExactly("index");
      CacheConfig group4 = cps.getCacheConfig("group3");
      assertThat(group4.findRegionConfiguration("testRegion").getIndexes())
          .extracting(RegionConfig.Index::getName).containsExactly("index");
    });
  }
}
