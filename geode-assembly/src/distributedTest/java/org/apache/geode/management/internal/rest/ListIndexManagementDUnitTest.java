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
import org.apache.geode.management.api.ConfigurationResult;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
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
  private Index index;

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

    cms = ClusterManagementServiceBuilder.buildWithHostAddress()
        .setHostAddress("localhost", locator.getHttpPort())
        .build();

    Region config = new Region();
    config.setName("region1");
    config.setType(RegionType.REPLICATE);
    cms.create(config);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/region1", 3);

    Index index1 = new Index();
    index1.setName("index1");
    index1.setExpression("id");
    index1.setRegionPath("/region1");
    index1.setIndexType(IndexType.KEY);
    cms.create(index1);

    Index index2 = new Index();
    index2.setName("index2");
    index2.setExpression("key");
    index2.setRegionPath("/region1");
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
    index = new Index();
  }

  @Test
  public void listRegion() {
    List<Region> result =
        cms.list(new Region()).getConfigResult();
    assertThat(result).hasSize(1);
  }

  @Test
  public void getRegion() {
    regionConfig.setName("region1");
    Region region = cms.get(regionConfig).getConfigResult();
    assertThat(region).isNotNull();
  }

  @Test
  public void getNonExistRegion() {
    regionConfig.setName("notExist");
    assertThatThrownBy(() -> cms.get(regionConfig)).hasMessageContaining("ENTITY_NOT_FOUND");
  }

  @Test
  public void listIndexForOneRegion() {
    index.setRegionPath("region1");
    ClusterManagementListResult<Index, IndexInfo> list = cms.list(index);
    List<Index> result = list.getConfigResult();
    assertThat(result).hasSize(2);
  }

  @Test
  public void listAllIndex() {
    ClusterManagementListResult<Index, IndexInfo> list = cms.list(index);
    List<Index> result = list.getConfigResult();
    assertThat(result).hasSize(2);
  }

  @Test
  public void getIndex() {
    index.setRegionPath("/region1");
    index.setName("index1");
    ClusterManagementGetResult<Index, IndexInfo> clusterManagementGetResult = cms.get(index);
    Index indexConfig = clusterManagementGetResult.getConfigResult();
    List<IndexInfo> runtimeResult = clusterManagementGetResult.getRuntimeResult();

    assertSoftly(softly -> {
      softly.assertThat(indexConfig.getRegionName()).isEqualTo("region1");
      softly.assertThat(indexConfig.getName()).isEqualTo("index1");
      softly.assertThat(indexConfig.getRegionPath()).isEqualTo("/region1");
      softly.assertThat(indexConfig.getExpression()).isEqualTo("id");
      ConfigurationResult<Index, IndexInfo> configurationResult = cms.get(index).getResult();
      Index indexConfigTwo = configurationResult.getConfiguration();
      softly.assertThat(indexConfigTwo.getLinks().getLinks()).containsKey("region");
      softly.assertThat(indexConfigTwo.getLinks().getLinks().get("region"))
          .endsWith("regions/region1");
      softly.assertThat(runtimeResult).extracting(IndexInfo::getMemberName)
          .containsExactlyInAnyOrder("server-1", "server-2", "server-3");
    });
  }

  @Test
  public void getIndexWithoutIndexId() {
    index.setRegionPath("region1");
    assertThatThrownBy(() -> cms.get(index)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unable to construct the URI ");
  }

  @Test
  public void getIndexWithoutRegionNameAndIndexId() {
    assertThatThrownBy(() -> cms.get(index)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unable to construct the URI ");
  }

  @Test
  public void getIndexWithoutRegionName() {
    index.setName("index1");
    assertThatThrownBy(() -> cms.get(index))
        .hasMessageContaining("Error while extracting response for type");
  }

  @Test
  public void listIndexWithoutRegionName() {
    index.setName("index1");
    assertListIndexResult(index);
  }

  @Test
  public void listIndexesWithIdFilter() {
    index.setRegionPath("region1");
    index.setName("index1");
    assertListIndexResult(index);
  }

  private void assertListIndexResult(Index index) {
    ClusterManagementListResult<Index, IndexInfo> list = cms.list(index);
    List<Index> result = list.getConfigResult();
    List<IndexInfo> runtimeResult = list.getRuntimeResult();
    assertSoftly(softly -> {
      softly.assertThat(result).hasSize(1);
      Index indexConfig = result.get(0);
      softly.assertThat(indexConfig.getRegionName()).isEqualTo("region1");
      softly.assertThat(indexConfig.getName()).isEqualTo("index1");
      softly.assertThat(indexConfig.getRegionPath()).isEqualTo("/region1");
      softly.assertThat(indexConfig.getExpression()).isEqualTo("id");
      softly.assertThat(runtimeResult).extracting(IndexInfo::getMemberName)
          .containsExactlyInAnyOrder("server-1", "server-2", "server-3");
    });
  }

  @Test
  public void getNonExistingIndex() {
    index.setRegionPath("region1");
    index.setName("index333");
    assertThatThrownBy(() -> cms.get(index)).hasMessageContaining("ENTITY_NOT_FOUND");
  }

  @Test
  public void listNonExistingIndexesWithIdFilter() {
    index.setRegionPath("region1");
    index.setName("index333");
    ClusterManagementListResult<Index, IndexInfo> list = cms.list(index);
    List<Index> result = list.getConfigResult();
    assertSoftly(softly -> {
      softly.assertThat(result).hasSize(0);
      softly.assertThat(list.isSuccessful()).isTrue();
    });
  }

  @Test
  public void createAndDeleteIndex_success_in_specific_group() {
    Region config = new Region();
    config.setName("region2");
    config.setType(RegionType.REPLICATE);
    config.setGroup("group1");
    cms.create(config);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/region2", 1);

    Index index = new Index();
    index.setName("index");
    index.setExpression("key");
    index.setRegionPath("/region2");
    index.setGroup("group1");
    index.setIndexType(IndexType.KEY);
    cms.create(index);

    ClusterManagementGetResult<Index, IndexInfo> indexResult = cms.get(index);
    Index indexConfig = indexResult.getConfigResult();
    List<IndexInfo> runtimeResult = indexResult.getRuntimeResult();
    assertSoftly(softly -> {
      softly.assertThat(indexConfig.getRegionName()).isEqualTo("region2");
      softly.assertThat(indexConfig.getName()).isEqualTo("index");
      softly.assertThat(indexConfig.getRegionPath()).isEqualTo("/region2");
      softly.assertThat(indexConfig.getExpression()).isEqualTo("key");
      softly.assertThat(runtimeResult).extracting(IndexInfo::getMemberName)
          .containsExactlyInAnyOrder("server-3");
    });

    ClusterManagementRealizationResult deleteIndexResult = cms.delete(index);
    config.setGroup(null);
    ClusterManagementRealizationResult deleteRegionResult = cms.delete(config);
    assertSoftly(softly -> {
      softly.assertThat(deleteIndexResult.isSuccessful()).isTrue();
      softly.assertThatThrownBy(() -> cms.get(index))
          .hasMessageContaining("Index 'index' does not exist");
      softly.assertThat(deleteRegionResult.isSuccessful()).isTrue();
      softly.assertThatThrownBy(() -> cms.get(config))
          .hasMessageContaining("Region 'region2' does not exist");
    });
  }

  @Test
  public void createAndDeleteIndex_success_in_cluster() {
    Region config = new Region();
    config.setName("region2");
    config.setType(RegionType.REPLICATE);
    cms.create(config);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/region2", 3);

    Index index = new Index();
    index.setName("index");
    index.setExpression("key");
    index.setRegionPath("/region2");
    index.setIndexType(IndexType.KEY);
    cms.create(index);

    ClusterManagementGetResult<Index, IndexInfo> indexResult = cms.get(index);
    Index indexConfig = indexResult.getConfigResult();
    List<IndexInfo> runtimeResult = indexResult.getRuntimeResult();
    assertSoftly(softly -> {
      softly.assertThat(indexConfig.getRegionName()).isEqualTo("region2");
      softly.assertThat(indexConfig.getName()).isEqualTo("index");
      softly.assertThat(indexConfig.getRegionPath()).isEqualTo("/region2");
      softly.assertThat(indexConfig.getExpression()).isEqualTo("key");
      softly.assertThat(runtimeResult).extracting(IndexInfo::getMemberName)
          .containsExactlyInAnyOrder("server-1", "server-2", "server-3");
    });

    ClusterManagementRealizationResult deleteIndexResult = cms.delete(index);
    ClusterManagementRealizationResult deleteRegionResult = cms.delete(config);
    assertSoftly(softly -> {
      softly.assertThat(deleteIndexResult.isSuccessful()).isTrue();
      softly.assertThatThrownBy(() -> cms.get(index))
          .hasMessageContaining("Index 'index' does not exist");
      softly.assertThat(deleteRegionResult.isSuccessful()).isTrue();
      softly.assertThatThrownBy(() -> cms.get(config))
          .hasMessageContaining("Region 'region2' does not exist");
    });
  }
}
