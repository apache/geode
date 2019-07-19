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
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceResult;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class RebalanceManagementDunitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static MemberVM locator, server1, server2;

  private static ClusterManagementService client;

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = cluster.startLocatorVM(0, l -> l.withHttpService());
    server1 = cluster.startServerVM(1, "group1", locator.getPort());
    server2 = cluster.startServerVM(2, "group2", locator.getPort());

    client = ClusterManagementServiceBuilder.buildWithHostAddress()
        .setHostAddress("localhost", locator.getHttpPort()).build();
    gfsh.connect(locator);

    // create regions
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("customers1");
    regionConfig.setType(RegionType.PARTITION);
    client.create(regionConfig);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/customers1", 2);

    regionConfig = new RegionConfig();
    regionConfig.setName("customers2");
    regionConfig.setType(RegionType.PARTITION);
    client.create(regionConfig);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/customers2", 2);
  }

  @Test
  public void rebalance() throws Exception {
    ClusterManagementOperationResult<RebalanceResult> cmr =
        client.startOperation(new RebalanceOperation());
    assertThat(cmr.isSuccessful()).isTrue();

    RebalanceResult result = cmr.getResult().get();
    assertThat(result.getRebalanceSummary().size()).isEqualTo(9);
    assertThat(result.getRebalanceSummary().keySet())
        .contains("total-time-in-milliseconds-for-this-rebalance");
    assertThat(result.getPerRegionResults().size()).isEqualTo(0);
  }

  @Test
  public void rebalanceExistRegion() throws Exception {
    List<String> includeRegions = new ArrayList<>();
    includeRegions.add("customers1");
    includeRegions.add("customers2");
    RebalanceOperation op = new RebalanceOperation();
    op.setIncludeRegions(includeRegions);
    ClusterManagementOperationResult<RebalanceResult> cmr = client.startOperation(op);
    assertThat(cmr.isSuccessful()).isTrue();

    RebalanceResult result = cmr.getResult().get();
    assertThat(result.getRebalanceSummary().size()).isEqualTo(9);
    assertThat(result.getRebalanceSummary().keySet())
        .contains("total-time-in-milliseconds-for-this-rebalance");
    assertThat(result.getPerRegionResults().size()).isEqualTo(2);
    // TODO assert something about contents of per region results
  }

  @Test
  public void rebalanceNonExistRegion() throws Exception {
    RebalanceOperation op = new RebalanceOperation();
    op.setIncludeRegions(Collections.singletonList("nonexisting_region"));
    ClusterManagementOperationResult<RebalanceResult> cmr = client.startOperation(op);
    assertThat(cmr.isSuccessful()).isFalse();
    assertThat(cmr.getStatusMessage()).contains("not exist"); // TODO
  }
}
