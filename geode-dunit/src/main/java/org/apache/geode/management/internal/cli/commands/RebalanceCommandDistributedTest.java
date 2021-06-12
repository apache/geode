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
import static org.apache.geode.test.junit.rules.GfshCommandRule.PortType.http;
import static org.apache.geode.test.junit.rules.GfshCommandRule.PortType.jmxManager;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.TabularResultModelAssert;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.MemberStarterRule;

@RunWith(Parameterized.class)
public class RebalanceCommandDistributedTest implements Serializable {
  private static final int ENTRIES_PER_REGION = 200;
  private static final String REGION_ONE_NAME = "region-1";
  private static final String REGION_TWO_NAME = "region-2";
  private static final String REGION_THREE_NAME = "region-3";

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  protected MemberVM locator, server1, server2;

  @Parameterized.Parameters(name = "ConnectionType:{0}")
  public static GfshCommandRule.PortType[] connectionTypes() {
    return new GfshCommandRule.PortType[] {http, jmxManager};
  }

  @Parameterized.Parameter
  public static GfshCommandRule.PortType portType;

  private void setUpRegions() {
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      RegionFactory<String, String> dataRegionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      Region<String, String> region1 = dataRegionFactory.create(REGION_ONE_NAME);
      Region<String, String> region2 = dataRegionFactory.create(REGION_TWO_NAME);

      for (int i = 0; i < ENTRIES_PER_REGION; i++) {
        region1.put("key" + i, "Value" + i);
        region2.put("key" + i, "Value" + i);
      }
    });

    server2.invoke(() -> {
      // no need to close cache as it will be closed as part of teardown2
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      RegionFactory<String, String> dataRegionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      dataRegionFactory.create(REGION_ONE_NAME);
      Region<String, String> region3 = dataRegionFactory.create(REGION_THREE_NAME);

      for (int i = 0; i < ENTRIES_PER_REGION; i++) {
        region3.put("key" + i, "Value" + i);
      }
    });
  }

  @Before
  public void setUp() throws Exception {
    locator = cluster.startLocatorVM(0, MemberStarterRule::withHttpService);
    int locatorPort = locator.getPort();

    server1 = cluster.startServerVM(1, "localhost", locatorPort);
    server2 = cluster.startServerVM(2, "localhost", locatorPort);
    setUpRegions();

    switch (portType) {
      case http:
        gfsh.connectAndVerify(locator.getHttpPort(), http);
        break;
      case jmxManager:
        gfsh.connectAndVerify(locator.getJmxPort(), jmxManager);
        break;

      default:
        throw new IllegalArgumentException("Invalid PortType Configured");
    }
  }

  @Test
  public void testSimulateForEntireDSWithTimeout() {
    // check if DistributedRegionMXBean is available so that command will not fail
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + REGION_ONE_NAME, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + REGION_TWO_NAME, 1);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + REGION_THREE_NAME, 1);

    String command = "rebalance --simulate=true --time-out=-1";

    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }

  @Test
  public void testRebalanceResultOutput() {
    // check if DistributedRegionMXBean is available so that command will not fail
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + REGION_ONE_NAME, 2);

    String command = "rebalance";

    TabularResultModelAssert rebalanceResult =
        gfsh.executeAndAssertThat(command).statusIsSuccess().hasTableSection();

    rebalanceResult.hasHeader().contains("Rebalanced partition regions");
    rebalanceResult.hasRow(0).contains(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATEBYTES);
    rebalanceResult.hasRow(1).contains(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATETIM);
    rebalanceResult.hasRow(2).contains(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATESCOMPLETED);
    rebalanceResult.hasRow(3).contains(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERBYTES);
    rebalanceResult.hasRow(4).contains(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERTIME);
    rebalanceResult.hasRow(5).contains(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERSCOMPLETED);
    rebalanceResult.hasRow(6).contains(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERTIME);
    rebalanceResult.hasRow(7).contains(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERSCOMPLETED);
    rebalanceResult.hasRow(8).contains(CliStrings.REBALANCE__MSG__TOTALTIME);
    rebalanceResult.hasRow(9).contains(CliStrings.REBALANCE__MSG__MEMBER_COUNT);
  }

  @Test
  public void testRebalanceResultOutputMemberCount() {
    MemberVM server3 = cluster.startServerVM(3, "localhost", locator.getPort());
    server3.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      cache.createRegionFactory(RegionShortcut.PARTITION).create(REGION_ONE_NAME);
    });

    // check if DistributedRegionMXBean is available so that command will not fail
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + REGION_ONE_NAME, 3);

    Map<String, List<String>> listMembersResult = gfsh.executeAndAssertThat("list members")
        .hasTableSection().getActual().getContent();
    assertThat(listMembersResult.get("Name").size()).isEqualTo(4);

    server3.forceDisconnect();

    Map<String, List<String>> rebalanceResult = gfsh.executeAndAssertThat("rebalance")
        .statusIsSuccess().hasTableSection().getActual().getContent();

    server3.waitTilFullyReconnected();

    listMembersResult = gfsh.executeAndAssertThat("list members")
        .hasTableSection().getActual().getContent();
    assertThat(listMembersResult.get("Name").size()).isEqualTo(4);
    assertThat(rebalanceResult.get("Rebalanced Stats").get(9))
        .isEqualTo(CliStrings.REBALANCE__MSG__MEMBER_COUNT);
    assertThat(rebalanceResult.get("Value").get(9)).isEqualTo("2");
  }

  /**
   * See GEODE-8071.
   * The test simply asserts that the re-balance command doesn't launch a non daemon thread
   * during its execution.
   */
  @Test
  public void rebalanceCommandShouldNotLaunchNonDaemonThreads() {
    gfsh.executeAndAssertThat("rebalance").statusIsSuccess();
    locator.invoke(() -> {
      assertThat(Thread.getAllStackTraces().keySet().stream()
          .anyMatch(thread -> !thread.isDaemon()
              && thread.getName().contains(RebalanceCommand.THREAD_NAME)))
                  .as("Rebalance Command should not launch non daemon threads")
                  .isFalse();
    });
  }
}
