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

import static org.apache.geode.lang.Identifiable.find;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Objects;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.builder.GeodeClusterManagementServiceBuilder;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.MemberStarterRule;


public class ClientClusterManagementServiceDunitTest {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(4);

  private static MemberVM locator;
  private static MemberVM server;
  private static ClientVM client;

  private static String groupA = "group-a";
  private static ClusterManagementService cmsClient;

  @BeforeClass
  public static void beforeClass() {
    locator = cluster.startLocatorVM(0, MemberStarterRule::withHttpService);
    server = cluster.startServerVM(1, locator.getPort());
    cluster.startServerVM(2, groupA, locator.getPort());
    cmsClient = new ClusterManagementServiceBuilder()
        .setPort(locator.getHttpPort())
        .build();
  }

  @Test
  public void createRegion() {
    Region region = new Region();
    region.setName("customer");
    region.setType(RegionType.PARTITION);

    ClusterManagementRealizationResult result = cmsClient.create(region);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getMemberStatuses()).extracting(RealizationResult::getMemberName)
        .containsExactlyInAnyOrder("server-1", "server-2");

    assertThatThrownBy(() -> cmsClient.create(region)).hasMessageContaining("ENTITY_EXISTS");
  }

  @Test
  public void createRegionWithNullGroup() {
    Region region = new Region();
    region.setName("orders");
    region.setType(RegionType.PARTITION);

    ClusterManagementRealizationResult result = cmsClient.create(region);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getMemberStatuses()).extracting(RealizationResult::getMemberName)
        .containsExactlyInAnyOrder("server-1", "server-2");
  }


  @Test
  public void createRegionWithInvalidName() {
    Region region = new Region();
    region.setName("__test");

    assertThatThrownBy(() -> cmsClient.create(region)).hasMessageContaining("ILLEGAL_ARGUMENT");
  }

  @Test
  public void createRegionWithGroup() {
    Region region = new Region();
    region.setName("company");
    region.setType(RegionType.PARTITION);
    region.setGroup(groupA);

    ClusterManagementRealizationResult result = cmsClient.create(region);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);

    // server 1 should not be in the set
    assertThat(result.getMemberStatuses()).extracting(RealizationResult::getMemberName)
        .containsExactlyInAnyOrder("server-2");

    locator.invoke(() -> {
      InternalConfigurationPersistenceService persistenceService =
          Objects.requireNonNull(ClusterStartupRule.getLocator())
              .getConfigurationPersistenceService();
      CacheConfig clusterCacheConfig = persistenceService.getCacheConfig("cluster", true);
      CacheConfig groupACacheConfig = persistenceService.getCacheConfig("group-a");
      assertThat(find(clusterCacheConfig.getRegions(), "company")).isNull();
      assertThat(find(groupACacheConfig.getRegions(), "company")).isNotNull();
    });
  }

  @Test
  public void invokeFromClientCacheWithLocatorPool() throws Exception {
    int locatorPort = locator.getPort();
    client = cluster.startClientVM(3, c -> c.withLocatorConnection(locatorPort));

    client.invoke(() -> {
      ClusterManagementService service =
          new GeodeClusterManagementServiceBuilder()
              .setCache(ClusterStartupRule.getClientCache())
              .build();
      assertThat(service.isConnected()).isTrue();
    });
    client.stop();
  }

  @Test
  public void invokeFromClientCacheWithServerPool() throws Exception {
    int serverPort = server.getPort();
    client = cluster.startClientVM(3, c -> c.withServerConnection(serverPort));

    client.invoke(() -> {
      assertThatThrownBy(() -> new GeodeClusterManagementServiceBuilder()
          .setCache(ClusterStartupRule.getClientCache())
          .build())
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining(
                  "the client needs to have a client pool connected with a locator");
    });
    client.stop();
  }
}
