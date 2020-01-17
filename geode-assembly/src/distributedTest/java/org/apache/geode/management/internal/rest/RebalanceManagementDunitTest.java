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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

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

public class RebalanceManagementDunitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static MemberVM locator1, locator2, server1, server2;

  private static ClusterManagementService client1, client2;

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator1 = cluster.startLocatorVM(0, l -> l.withHttpService());
    locator2 = cluster.startLocatorVM(1, l -> l.withHttpService());
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
  public void rebalance() throws Exception {
    ClusterManagementOperationResult<RebalanceResult> cmr =
        client1.start(new RebalanceOperation());
    assertThat(cmr.isSuccessful()).isTrue();
    long now = System.currentTimeMillis();
    assertThat(cmr.getOperationStart().getTime()).isBetween(now - 60000, now);

    GeodeAwaitility.await().untilAsserted( () -> assertThat(cmr.getOperationEnd()).isNotNull());
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
  public void rebalanceExistRegion() throws Exception {
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
  public void rebalanceExcludedRegion() throws Exception {
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
  public void rebalanceNonExistRegion() throws Exception {
    IgnoredException.addIgnoredException(ExecutionException.class);
    IgnoredException.addIgnoredException(RuntimeException.class);
    RebalanceOperation op = new RebalanceOperation();
    op.setIncludeRegions(Collections.singletonList("nonexisting_region"));
    ClusterManagementOperationResult<RebalanceResult> cmr = client1.start(op);
    assertThat(cmr.isSuccessful()).isTrue();
    String id = cmr.getOperationId();
    client1.list()
    assertThat(client1.get(op))

    CompletableFuture<RebalanceResult> future = cmr.getFutureResult();
    CompletableFuture<String> message = new CompletableFuture<>();
    future.exceptionally((ex) -> {
      message.complete(ex.getMessage());
      return null;
    }).get();

    assertThat(future.isCompletedExceptionally()).isTrue();
    assertThat(message.get()).contains("For the region /nonexisting_region, no member was found");
  }

  @Test
  public void rebalanceOneExistingOneNonExistingRegion() throws Exception {
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
