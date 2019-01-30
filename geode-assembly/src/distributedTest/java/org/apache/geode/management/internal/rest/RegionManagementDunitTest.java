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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.internal.api.ClusterManagementResult;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GeodeDevRestClient;

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
    regionConfig.setType("REPLICATE");
    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(regionConfig);

    ClusterManagementResult result =
        restClient.doPostAndAssert("/regions", json)
            .hasStatusCode(201)
            .getClusterManagementResult();

    assertThat(result.isSuccessfullyAppliedOnMembers()).isTrue();
    assertThat(result.isSuccessfullyPersisted()).isTrue();
    assertThat(result.getMemberStatuses()).containsKeys("server-1").hasSize(1);

    // make sure region is created
    server.invoke(() -> verifyRegionCreated("customers", "REPLICATE"));

    // make sure region is persisted
    locator.invoke(() -> verifyRegionPersisted("customers", "REPLICATE"));

    // verify that additional server can be started with the cluster configuration
    cluster.startServerVM(2, locator.getPort());
  }

  @Test
  public void createsAPartitionedRegionByDefault() throws Exception {
    String json = "{\"name\": \"orders\"}";

    ClusterManagementResult result = restClient.doPostAndAssert("/regions", json)
        .hasStatusCode(201)
        .getClusterManagementResult();

    assertThat(result.isSuccessfullyAppliedOnMembers()).isTrue();
    assertThat(result.isSuccessfullyPersisted()).isTrue();
    assertThat(result.getMemberStatuses()).containsKeys("server-1").hasSize(1);

    // make sure region is created
    server.invoke(() -> verifyRegionCreated("orders", "PARTITION"));

    // make sure region is persisted
    locator.invoke(() -> verifyRegionPersisted("orders", "PARTITION"));
  }

  @Test
  public void noNameInConfig() throws Exception {
    IgnoredException.addIgnoredException("Name of the region has to be specified");
    String json = "{\"type\": \"REPLICATE\"}";

    ClusterManagementResult result = restClient.doPostAndAssert("/regions", json)
        .hasStatusCode(500)
        .getClusterManagementResult();

    assertThat(result.isSuccessfullyAppliedOnMembers()).isFalse();
    assertThat(result.isSuccessfullyPersisted()).isFalse();
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
