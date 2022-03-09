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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class RebalanceSubregionDistributedTest implements Serializable {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private static final String ROOT_REGION_NAME = "root";

  @Test
  public void testRebalanceSubregion() throws Exception {
    // Start locator
    MemberVM locator = cluster.startLocatorVM(0);

    // Start 2 servers
    MemberVM server1 = cluster.startServerVM(1, locator.getPort());
    MemberVM server2 = cluster.startServerVM(2, locator.getPort());

    // Create region in server1
    String regionName = testName.getMethodName();
    server1.invoke(() -> createSubregion(regionName));

    // Do puts in server1
    server1.invoke(() -> doPuts(regionName));

    // Create region in server2
    server2.invoke(() -> createSubregion(regionName));

    // Make sure the locator has the MBean
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(
        SEPARATOR + ROOT_REGION_NAME + SEPARATOR + regionName, 2);

    // Connect gfsh to locator
    gfsh.connectAndVerify(locator);

    // Execute rebalance and verify it is successful
    gfsh.executeAndAssertThat("rebalance").statusIsSuccess();

    // Verify the results
    ResultModel result = gfsh.getCommandResult().getResultData();
    List<TabularResultModel> tableSections = result.getTableSections();
    assertThat(tableSections.size()).isEqualTo(1);
    assertThat(tableSections.get(0).getHeader()
        .contains(SEPARATOR + ROOT_REGION_NAME + SEPARATOR + regionName)).isTrue();
  }

  private void createSubregion(String regionName) {
    // Create the root region
    RegionFactory<Object, Object> rootRegionFactory =
        Objects.requireNonNull(ClusterStartupRule.getCache())
            .createRegionFactory(RegionShortcut.REPLICATE);
    Region<Object, Object> rootRegion = rootRegionFactory.create(ROOT_REGION_NAME);

    // Create the subregion
    RegionFactory<Integer, Integer> partitionedRegionFactory =
        Objects.requireNonNull(ClusterStartupRule.getCache())
            .createRegionFactory(RegionShortcut.PARTITION);
    partitionedRegionFactory.setPartitionAttributes(new PartitionAttributesFactory().create());
    partitionedRegionFactory.createSubregion(rootRegion, regionName);
  }

  private void doPuts(String regionName) {
    Region<Integer, Integer> subregion = Objects.requireNonNull(ClusterStartupRule.getCache())
        .getRegion(SEPARATOR + ROOT_REGION_NAME + SEPARATOR + regionName);
    IntStream.range(0, 112).forEach(i -> subregion.put(i, i));
  }
}
