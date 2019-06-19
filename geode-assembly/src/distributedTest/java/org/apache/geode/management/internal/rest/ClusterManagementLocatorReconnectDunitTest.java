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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.RuntimeRegionConfig;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GeodeDevRestClient;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

public class ClusterManagementLocatorReconnectDunitTest {
  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  @Rule
  public ClusterStartupRule rule = new ClusterStartupRule();

  private MemberVM locator, server;

  private GeodeDevRestClient restClient;

  @Test
  public void clusterManagementRestServiceStillWorksAfterLocatorReconnects() throws Exception {
    IgnoredException.addIgnoredException("org.apache.geode.ForcedDisconnectException: for testing");
    locator = rule.startLocatorVM(0, l -> l.withHttpService());
    server = rule.startServerVM(1, locator.getPort());
    restClient =
        new GeodeDevRestClient("/geode-management/v2", "localhost", locator.getHttpPort(), false);

    makeRestCallAndVerifyResult("customers");

    locator.forceDisconnect();

    // wait till locator is disconnected and reconnected
    await().pollInterval(1, TimeUnit.SECONDS).until(() -> locator.invoke("waitTillRestarted",
        () -> ClusterStartupRule.getLocator().isReconnected()));

    makeRestCallAndVerifyResult("orders");
  }

  private void makeRestCallAndVerifyResult(String regionName) throws Exception {
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName(regionName);
    regionConfig.setType(RegionType.REPLICATE);
    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(regionConfig);

    ClusterManagementResult<RuntimeRegionConfig> result =
        restClient.doPostAndAssert("/regions", json, "test", "test")
            .hasStatusCode(201)
            .getClusterManagementResult();

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getMemberStatuses()).extracting(RealizationResult::getMemberName)
        .containsExactly("server-1");

    // make sure region is created
    server.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion(regionName);
      assertThat(region).isNotNull();
    });

    // make sure region is persisted
    locator.invoke(() -> {
      CacheConfig cacheConfig =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService()
              .getCacheConfig("cluster");
      assertThat(CacheElement.exists(cacheConfig.getRegions(), regionName)).isTrue();
    });

  }

}
