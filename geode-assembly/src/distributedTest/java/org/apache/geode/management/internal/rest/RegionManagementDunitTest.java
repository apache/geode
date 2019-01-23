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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GeodeDevRestClient;

public class RegionManagementDunitTest {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  private MemberVM locator, server;

  private GeodeDevRestClient restClient;

  @Before
  public void before() throws Exception {
    locator = cluster.startLocatorVM(0, l -> l.withHttpService());
    server = cluster.startServerVM(1, locator.getPort());
    restClient =
        new GeodeDevRestClient("/geode-management/v2", "localhost", locator.getHttpPort(), false);
  }

  @Test
  public void createRegion() throws Exception {
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("customers");
    regionConfig.setRefid("REPLICATE");
    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(regionConfig);

    ClusterManagementResult result =
        restClient.doPostAndAssert("/regions", json, "test", "test")
            .hasStatusCode(201)
            .getClusterManagementResult();

    assertThat(result.isSuccessfullyAppliedOnMembers()).isTrue();
    assertThat(result.isSuccessfullyPersisted()).isTrue();
    assertThat(result.getMemberStatuses()).containsKeys("server-1").hasSize(1);

    // make sure region is created
    server.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion("customers");
      assertThat(region).isNotNull();
    });

    // make sure region is persisted
    locator.invoke(() -> {
      CacheConfig cacheConfig =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService()
              .getCacheConfig("cluster");
      assertThat(cacheConfig.getRegions().get(0).getName()).isEqualTo("customers");
    });
  }
}
