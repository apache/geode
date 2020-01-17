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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementListOperationsResult;
import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceRegionResult;
import org.apache.geode.management.runtime.RebalanceResult;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.MemberStarterRule;

public class RebalanceManagementDunitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static MemberVM locator1, locator2, server1, server2;

  private static ClusterManagementService client1, client2;

  @BeforeClass
  public static void beforeClass() {
    locator1 = cluster.startLocatorVM(0, MemberStarterRule::withHttpService);
    locator2 = cluster.startLocatorVM(1, MemberStarterRule::withHttpService);
    server1 = cluster.startServerVM(2, "group1", locator1.getPort());
    server2 = cluster.startServerVM(3, "group2", locator1.getPort());

    client1 = new ClusterManagementServiceBuilder()
        .setHost("localhost")
        .setPort(locator1.getHttpPort())
        .build();
    client2 = new ClusterManagementServiceBuilder()
        .setHost("localhost")
        .setPort(locator2.getHttpPort())
        .build();

    // create regions
    Region regionConfig = new Region();
    regionConfig.setName("customers1");
    regionConfig.setType(RegionType.PARTITION);
    client1.create(regionConfig);
    locator1.waitUntilRegionIsReadyOnExactlyThisManyServers("/customers1", 2);

    regionConfig = new Region();
    regionConfig.setName("customers2");
    regionConfig.setType(RegionType.PARTITION);
    client1.create(regionConfig);
    locator1.waitUntilRegionIsReadyOnExactlyThisManyServers("/customers2", 2);
  }

  @Test
  public void rebalance() {
    ClusterManagementOperationResult<RebalanceResult> cmr =
        client1.start(new RebalanceOperation());
    assertThat(cmr.isSuccessful()).isTrue();
    long now = System.currentTimeMillis();
    assertThat(cmr.getOperationStart().getTime()).isBetween(now - 60000, now);

    GeodeAwaitility.await().untilAsserted(() -> assertThat(cmr.getOperationEnd()).isNotNull());
    long end = cmr.getOperationEnd().getTime();
    now = System.currentTimeMillis();
    assertThat(end).isBetween(now - 60000, now)
        .isGreaterThanOrEqualTo(cmr.getOperationStart().getTime());
    RebalanceResult result = cmr.getOperationResult();
    assertThat(result.getRebalanceRegionResults().size()).isEqualTo(2);
    RebalanceRegionResult firstRegionSummary = result.getRebalanceRegionResults().get(0);
    assertThat(firstRegionSummary.getRegionName()).isIn("customers1", "customers2");
  }

  @Test
  public void rebalanceExistRegion() {
    List<String> includeRegions = new ArrayList<>();
    includeRegions.add("customers2");
    RebalanceOperation op = new RebalanceOperation();
    op.setIncludeRegions(includeRegions);
    int initialSize = client1.list(op).getResult().size();
    ClusterManagementOperationResult<RebalanceResult> cmr = client1.start(op);
    assertThat(cmr.isSuccessful()).isTrue();

    RebalanceResult result = cmr.getOperationResult();
    assertThat(result.getRebalanceRegionResults().size()).isEqualTo(1);
    RebalanceRegionResult firstRegionSummary = result.getRebalanceRegionResults().get(0);
    assertThat(firstRegionSummary.getRegionName()).isEqualTo("customers2");
    assertThat(firstRegionSummary.getBucketCreateBytes()).isEqualTo(0);
    assertThat(firstRegionSummary.getTimeInMilliseconds()).isGreaterThanOrEqualTo(0);

    assertThat(client1.list(op).getResult()).hasSize(initialSize + 1);
    assertThat(client2.list(op).getResult()).hasSize(0); // TODO should be initialSize+1)
  }

  @Test
  public void rebalanceExcludedRegion() {
    RebalanceOperation op = new RebalanceOperation();
    op.setExcludeRegions(Collections.singletonList("customers1"));
    ClusterManagementOperationResult<RebalanceResult> cmr = client1.start(op);
    assertThat(cmr.isSuccessful()).isTrue();

    RebalanceResult result = cmr.getOperationResult();
    assertThat(result.getRebalanceRegionResults().size()).isEqualTo(1);
    RebalanceRegionResult firstRegionSummary = result.getRebalanceRegionResults().get(0);
    assertThat(firstRegionSummary.getRegionName()).isEqualTo("customers2");
    assertThat(firstRegionSummary.getBucketCreateBytes()).isEqualTo(0);
    assertThat(firstRegionSummary.getTimeInMilliseconds()).isGreaterThanOrEqualTo(0);
  }

  @Test
  public void rebalanceNonExistRegion() {
    IgnoredException.addIgnoredException(ExecutionException.class);
    IgnoredException.addIgnoredException(RuntimeException.class);
    RebalanceOperation op = new RebalanceOperation();
    op.setIncludeRegions(Collections.singletonList("nonexisting_region"));
    ClusterManagementOperationResult<RebalanceResult> cmr = client1.start(op);
    assertThat(cmr.isSuccessful()).isTrue();
    String id = cmr.getOperationId();

    ClusterManagementOperationResult<RebalanceResult>[] rebalanceResult = new ClusterManagementOperationResult[1];
    GeodeAwaitility.await().untilAsserted(() -> {
      rebalanceResult[0] = getRebalanceResult(op, id);
      assertThat(rebalanceResult[0]).isNotNull();
    });

    assertThat(rebalanceResult[0].isSuccessful()).isFalse();
    assertThat(rebalanceResult[0].getStatusMessage()).contains("For the region /nonexisting_region, no member was found");

  }

  private ClusterManagementOperationResult<RebalanceResult> getRebalanceResult(RebalanceOperation op, String id) {
    ClusterManagementListOperationsResult<RebalanceResult> listOperationsResult = client1.list(op);
    Optional<ClusterManagementOperationResult<RebalanceResult>> rebalanceResult =
        listOperationsResult.getResult()
            .stream()
            .filter(rbalresult -> rbalresult.getOperationId().equals(id) && rbalresult.getOperationEnd() != null)
            .findFirst();
    return  rebalanceResult.orElse(null);
  }

  @Test
  public void rebalanceOneExistingOneNonExistingRegion() {
    IgnoredException.addIgnoredException(ExecutionException.class);
    IgnoredException.addIgnoredException(RuntimeException.class);
    RebalanceOperation op = new RebalanceOperation();
    op.setIncludeRegions(Arrays.asList("nonexisting_region", "customers1"));
    ClusterManagementOperationResult<RebalanceResult> cmr = client1.start(op);
    assertThat(cmr.isSuccessful()).isTrue();

    RebalanceResult result = cmr.getOperationResult();
    assertThat(result.getRebalanceRegionResults().size()).isEqualTo(1);
    RebalanceRegionResult firstRegionSummary = result.getRebalanceRegionResults().get(0);
    assertThat(firstRegionSummary.getRegionName()).isEqualTo("customers1");
    assertThat(firstRegionSummary.getBucketCreateBytes()).isEqualTo(0);
    assertThat(firstRegionSummary.getTimeInMilliseconds()).isGreaterThanOrEqualTo(0);
  }
}
