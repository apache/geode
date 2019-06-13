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

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.configuration.RuntimeRegionConfig;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GeodeDevRestClient;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class RegionManagementDunitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static MemberVM locator, server;

  private static GeodeDevRestClient restClient;

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = cluster.startLocatorVM(0, l -> l.withHttpService());
    server = cluster.startServerVM(1, locator.getPort());
    restClient =
        new GeodeDevRestClient("/geode-management/v2", "localhost", locator.getHttpPort(), false);
  }

  @Test
  public void createsRegion() throws Exception {
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("customers");
    regionConfig.setType(RegionType.REPLICATE);
    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(regionConfig);

    ClusterManagementResult<RuntimeRegionConfig> result =
        restClient.doPostAndAssert("/regions", json)
            .hasStatusCode(201)
            .getClusterManagementResult();

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getMemberStatuses()).containsKeys("server-1").hasSize(1);

    // make sure region is created
    server.invoke(() -> verifyRegionCreated("customers", "REPLICATE"));

    // make sure region is persisted
    locator.invoke(() -> verifyRegionPersisted("customers", "REPLICATE"));

    // verify that additional server can be started with the cluster configuration
    MemberVM server2 = cluster.startServerVM(2, locator.getPort());
    server2.invoke(() -> verifyRegionCreated("customers", "REPLICATE"));

    // stop the 2nd server to avoid test pollution
    server2.stop();
  }

  @Test
  public void createRegionWithKeyValueConstraint() throws Exception {
    RegionConfig config = new RegionConfig();
    config.setName("customers2");
    config.setType(RegionType.PARTITION);
    RegionAttributesType type = new RegionAttributesType();
    type.setKeyConstraint("java.lang.Boolean");
    type.setValueConstraint("java.lang.Integer");
    config.setRegionAttributes(type);

    restClient.doPostAndAssert("/regions", GeodeJsonMapper.getMapper().writeValueAsString(config))
        .statusIsOk();

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/customers2", 1);

    List<?> result =
        restClient.doGetAndAssert("/regions?id=customers2").statusIsOk()
            .getClusterManagementResult().getResult();

    assertThat(result).hasSize(1);
    RuntimeRegionConfig config1 = (RuntimeRegionConfig) result.get(0);
    assertThat(config1.getType()).isEqualTo("PARTITION");
    assertThat(config1.getRegionAttributes().getDataPolicy().name()).isEqualTo("PARTITION");
    assertThat(config1.getRegionAttributes().getValueConstraint()).isEqualTo("java.lang.Integer");
    assertThat(config1.getRegionAttributes().getKeyConstraint()).isEqualTo("java.lang.Boolean");

    server.invoke(() -> {
      Region customers2 = ClusterStartupRule.getCache().getRegionByPath("/customers2");
      assertThatThrownBy(() -> customers2.put("key", 2)).isInstanceOf(ClassCastException.class)
          .hasMessageContaining("does not satisfy keyConstraint");
      assertThatThrownBy(() -> customers2.put(Boolean.TRUE, "2"))
          .isInstanceOf(ClassCastException.class)
          .hasMessageContaining("does not satisfy valueConstraint");
    });
  }

  @Test
  public void createsAPartitionedRegion() throws Exception {
    String json = "{\"name\": \"orders\", \"type\": \"PARTITION\"}";

    ClusterManagementResult<RuntimeRegionConfig> result =
        restClient.doPostAndAssert("/regions", json)
            .hasStatusCode(201)
            .getClusterManagementResult();

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getMemberStatuses()).containsKeys("server-1").hasSize(1);

    // make sure region is created
    server.invoke(() -> verifyRegionCreated("orders", "PARTITION"));

    // make sure region is persisted
    locator.invoke(() -> verifyRegionPersisted("orders", "PARTITION"));

    // create the same region 2nd time
    result = restClient.doPostAndAssert("/regions", json)
        .hasStatusCode(409)
        .getClusterManagementResult();
    assertThat(result.isSuccessful()).isFalse();
  }

  @Test
  public void noNameInConfig() throws Exception {
    IgnoredException.addIgnoredException("Name of the region has to be specified");
    String json = "{\"type\": \"REPLICATE\"}";

    ClusterManagementResult<RuntimeRegionConfig> result =
        restClient.doPostAndAssert("/regions", json)
            .hasStatusCode(400)
            .getClusterManagementResult();

    assertThat(result.isSuccessful()).isFalse();
  }

  static void verifyRegionPersisted(String regionName, String type) {
    CacheConfig cacheConfig =
        ClusterStartupRule.getLocator().getConfigurationPersistenceService()
            .getCacheConfig("cluster");
    RegionConfig regionConfig = CacheElement.findElement(cacheConfig.getRegions(), regionName);
    assertThat(regionConfig.getType()).isEqualTo(type);
  }

  static void verifyRegionCreated(String regionName, String type) {
    Cache cache = ClusterStartupRule.getCache();
    Region region = cache.getRegion(regionName);
    assertThat(region).isNotNull();
    assertThat(region.getAttributes().getDataPolicy().toString()).isEqualTo(type);
  }
}
