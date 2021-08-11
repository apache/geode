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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.query.QueryService;
import org.apache.geode.management.api.ClusterManagementGetResult;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.EntityGroupInfo;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.IndexType;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.management.runtime.IndexInfo;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.MemberStarterRule;

public class ListIndexManagementDUnitTest {

  private Region regionConfig;
  private Index indexConfig;

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  private static ClusterManagementService cms;
  private static MemberVM locator;

  @BeforeClass
  public static void beforeClass() {
    locator = lsRule.startLocatorVM(0, MemberStarterRule::withHttpService);
    MemberVM server1 = lsRule.startServerVM(1, locator.getPort());
    MemberVM server2 = lsRule.startServerVM(2, locator.getPort());
    MemberVM server3 = lsRule.startServerVM(3, "group1", locator.getPort());

    cms = new ClusterManagementServiceBuilder().setPort(locator.getHttpPort())
        .build();

    Region config = new Region();
    config.setName("region1");
    config.setType(RegionType.REPLICATE);
    cms.create(config);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + "region1", 3);

    Index index1 = new Index();
    index1.setName("index1");
    index1.setExpression("id");
    index1.setRegionPath(SEPARATOR + "region1");
    index1.setIndexType(IndexType.KEY);
    cms.create(index1);

    Index index2 = new Index();
    index2.setName("index2");
    index2.setExpression("key");
    index2.setRegionPath(SEPARATOR + "region1");
    index2.setIndexType(IndexType.KEY);
    cms.create(index2);

    // make sure indexes are created on each server
    MemberVM.invokeInEveryMember(() -> {
      QueryService queryService =
          Objects.requireNonNull(ClusterStartupRule.getCache()).getQueryService();
      Collection<org.apache.geode.cache.query.Index> indexes = queryService.getIndexes();
      assertThat(indexes).extracting(org.apache.geode.cache.query.Index::getName)
          .containsExactlyInAnyOrder("index1", "index2");
      assertThat(indexes.stream().findFirst()
          .filter(index -> index.getRegion().getName().equals("region1")).isPresent()).isTrue();
    }, server1, server2, server3);
  }

  @Before
  public void before() {
    regionConfig = new Region();
    indexConfig = new Index();
  }

  @Test
  public void listRegion_succeeds_with_empty_filter() {
    List<Region> result =
        cms.list(new Region()).getConfigResult();
    assertThat(result).hasSize(1);
  }

  @Test
  public void getRegion_succeeds_with_region_name_filter() {
    regionConfig.setName("region1");
    Region region = cms.get(regionConfig).getResult().getConfigurations().get(0);
    assertThat(region).isNotNull();
  }

  @Test
  public void getRegion_fails_with_non_existent_region_name_in_filter() {
    regionConfig.setName("notExist");
    assertThatThrownBy(() -> cms.get(regionConfig)).hasMessageContaining("ENTITY_NOT_FOUND");
  }

  @Test
  public void listIndex_succeeds_for_region_name_filter() {
    indexConfig.setRegionPath("region1");
    ClusterManagementListResult<Index, IndexInfo> list = cms.list(indexConfig);
    List<Index> result = list.getConfigResult();
    assertThat(result).hasSize(2);
  }

  @Test
  public void listIndex_succeeds_for_all_indexes() {
    ClusterManagementListResult<Index, IndexInfo> list = cms.list(indexConfig);
    List<Index> result = list.getConfigResult();
    assertThat(result).hasSize(2);
  }

  @Test
  public void getIndex_succeeds_with_index_name_and_region_name_filter() {
    indexConfig.setRegionPath(SEPARATOR + "region1");
    indexConfig.setName("index1");
    ClusterManagementGetResult<Index, IndexInfo> clusterManagementGetResult = cms.get(indexConfig);
    Index indexConfig = clusterManagementGetResult.getResult().getConfigurations().get(0);
    List<IndexInfo> runtimeResult = clusterManagementGetResult.getResult().getRuntimeInfos();

    assertSoftly(softly -> {
      softly.assertThat(indexConfig.getRegionName()).as("get index: region name")
          .isEqualTo("region1");
      softly.assertThat(indexConfig.getName()).as("get index: index name").isEqualTo("index1");
      softly.assertThat(indexConfig.getRegionPath()).as("get index: region path")
          .isEqualTo(SEPARATOR + "region1");
      softly.assertThat(indexConfig.getExpression()).as("get index: expression").isEqualTo("id");
      EntityGroupInfo<Index, IndexInfo> entityGroupInfo =
          cms.get(this.indexConfig).getResult().getGroups().get(0);
      Index indexConfigTwo = entityGroupInfo.getConfiguration();
      softly.assertThat(indexConfigTwo.getLinks().getLinks()).as("get index: links key")
          .containsKey("region");
      softly.assertThat(indexConfigTwo.getLinks().getLinks().get("region"))
          .as("get index: links value")
          .endsWith("regions/region1");
      softly.assertThat(runtimeResult).extracting(IndexInfo::getMemberName)
          .as("get index: runtime servers")
          .containsExactlyInAnyOrder("server-1", "server-2", "server-3");
    });
  }

  @Test
  public void getIndex_fails_with_region_name_only_filter() {
    indexConfig.setRegionPath("region1");
    assertThatThrownBy(() -> cms.get(indexConfig)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unable to construct the URI ");
  }

  @Test
  public void getIndex_fails_when_index_name_and_region_name_are_missing() {
    assertThatThrownBy(() -> cms.get(indexConfig)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unable to construct the URI ");
  }

  @Test
  public void getIndex_fails_when_region_name_is_missing_from_filter() {
    indexConfig.setName("index1");
    assertThatThrownBy(() -> cms.get(indexConfig))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unable to construct the URI with the current configuration");
  }

  @Test
  public void listIndex_succeeds_with_index_name_only_filter() {
    indexConfig.setName("index1");
    assertListIndexResult(indexConfig);
  }

  @Test
  public void listIndex_succeeds_with_region_name_and_index_name_filter() {
    indexConfig.setRegionPath("region1");
    indexConfig.setName("index1");
    assertListIndexResult(indexConfig);
  }

  private void assertListIndexResult(Index index) {
    ClusterManagementListResult<Index, IndexInfo> list = cms.list(index);
    List<Index> result = list.getConfigResult();
    List<IndexInfo> runtimeResult = list.getRuntimeResult();
    assertSoftly(softly -> {
      softly.assertThat(result).hasSize(1);
      Index indexConfig = result.get(0);
      softly.assertThat(indexConfig.getRegionName()).as("list index: region name")
          .isEqualTo("region1");
      softly.assertThat(indexConfig.getName()).as("list index: index name").isEqualTo("index1");
      softly.assertThat(indexConfig.getRegionPath()).as("list index: region path")
          .isEqualTo(SEPARATOR + "region1");
      softly.assertThat(indexConfig.getExpression()).as("list index: expression").isEqualTo("id");
      softly.assertThat(runtimeResult).extracting(IndexInfo::getMemberName)
          .as("list index: runtime servers")
          .containsExactlyInAnyOrder("server-1", "server-2", "server-3");
    });
  }

  @Test
  public void getIndex_fails_with_wrong_index_name_in_filter() {
    indexConfig.setRegionPath("region1");
    indexConfig.setName("index333");
    assertThatThrownBy(() -> cms.get(indexConfig)).hasMessageContaining("ENTITY_NOT_FOUND");
  }

  @Test
  public void listIndex_fails_with_wrong_index_name_in_filter() {
    indexConfig.setRegionPath("region1");
    indexConfig.setName("index333");
    ClusterManagementListResult<Index, IndexInfo> list = cms.list(indexConfig);
    List<Index> result = list.getConfigResult();
    assertSoftly(softly -> {
      softly.assertThat(result).as("list non existing: result size").hasSize(0);
      softly.assertThat(list.isSuccessful()).as("list non existing: success").isTrue();
    });
  }

  @Test
  public void createAndDeleteIndex_success_for_specific_group() {
    Region region = new Region();
    region.setName("region2");
    region.setType(RegionType.REPLICATE);
    region.setGroup("group1");
    cms.create(region);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + "region2", 1);

    Index index = new Index();
    index.setName("index");
    index.setExpression("key");
    index.setRegionPath(SEPARATOR + "region2");
    index.setIndexType(IndexType.KEY);
    cms.create(index);

    ClusterManagementGetResult<Index, IndexInfo> indexResult = cms.get(index);
    Index fetchedIndexConfig = indexResult.getResult().getConfigurations().get(0);
    List<IndexInfo> runtimeResult = indexResult.getResult().getRuntimeInfos();
    assertSoftly(softly -> {
      softly.assertThat(fetchedIndexConfig.getRegionName()).as("index create: region name")
          .isEqualTo("region2");
      softly.assertThat(fetchedIndexConfig.getName()).as("index create: index name")
          .isEqualTo("index");
      softly.assertThat(fetchedIndexConfig.getRegionPath()).as("index create: region path")
          .isEqualTo(SEPARATOR + "region2");
      softly.assertThat(fetchedIndexConfig.getExpression()).as("index create: expression")
          .isEqualTo("key");
      softly.assertThat(runtimeResult).extracting(IndexInfo::getMemberName)
          .as("index create: runtime server")
          .containsExactlyInAnyOrder("server-3");
    });

    ClusterManagementRealizationResult deleteIndexResult = cms.delete(index);
    region.setGroup(null);
    ClusterManagementRealizationResult deleteRegionResult = cms.delete(region);
    assertSoftly(softly -> {
      softly.assertThat(deleteIndexResult.isSuccessful()).isTrue();
      softly.assertThatThrownBy(() -> cms.get(index)).as("delete index confirmation")
          .hasMessageContaining("Index 'index' does not exist");
      softly.assertThat(deleteRegionResult.isSuccessful()).isTrue();
      softly.assertThatThrownBy(() -> cms.get(region)).as("delete region confirmation")
          .hasMessageContaining("Region 'region2' does not exist");
    });
  }

  @Test
  public void createAndDeleteIndex_success_for_cluster() {
    Region region = new Region();
    region.setName("region2");
    region.setType(RegionType.REPLICATE);
    cms.create(region);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + "region2", 3);

    Index index = new Index();
    index.setName("index.1");
    index.setExpression("key");
    index.setRegionPath(SEPARATOR + "region2");
    index.setIndexType(IndexType.KEY);
    cms.create(index);

    ClusterManagementGetResult<Index, IndexInfo> indexResult = cms.get(index);
    Index fetchedIndexConfig = indexResult.getResult().getConfigurations().get(0);
    List<IndexInfo> runtimeResult = indexResult.getResult().getRuntimeInfos();
    assertSoftly(softly -> {
      softly.assertThat(fetchedIndexConfig.getRegionName()).as("index create: region name")
          .isEqualTo("region2");
      softly.assertThat(fetchedIndexConfig.getName()).as("index create: index name")
          .isEqualTo("index.1");
      softly.assertThat(fetchedIndexConfig.getRegionPath()).as("index create: index path")
          .isEqualTo(SEPARATOR + "region2");
      softly.assertThat(fetchedIndexConfig.getExpression()).as("index create: index expression")
          .isEqualTo("key");
      softly.assertThat(runtimeResult).as("index create: runtime servers")
          .extracting(IndexInfo::getMemberName)
          .containsExactlyInAnyOrder("server-1", "server-2", "server-3");
    });

    ClusterManagementRealizationResult deleteIndexResult = cms.delete(index);
    ClusterManagementRealizationResult deleteRegionResult = cms.delete(region);
    assertSoftly(softly -> {
      softly.assertThat(deleteIndexResult.isSuccessful()).isTrue();
      softly.assertThatThrownBy(() -> cms.get(index)).as("index delete confirmation")
          .hasMessageContaining("Index 'index.1' does not exist");
      softly.assertThat(deleteRegionResult.isSuccessful()).isTrue();
      softly.assertThatThrownBy(() -> cms.get(region)).as("region delete confirmation")
          .hasMessageContaining("Region 'region2' does not exist");
    });
  }
}
