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

import static java.lang.Math.abs;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class RebalanceCommandDUnitTest {
  private static final String SHARED_REGION_NAME = "GemfireDataCommandsDUnitTestRegion";
  private static final String REGION1_NAME = "GemfireDataCommandsDUnitTestRegion1";
  private static final String REGION2_NAME = "GemfireDataCommandsDUnitTestRegion2";

  private static final Integer SERVER1_SHARED_REGION_SIZE = 10;
  private static final Integer SERVER2_SHARED_REGION_SIZE = 100;
  private static final Integer SERVER1_REGION1_SIZE = 100;
  private static final Integer SERVER2_REGION2_SIZE = 10;

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  private MemberVM server1;
  private MemberVM server2;
  private static int server1SharedRegionInitialSize, server2SharedRegionInitialSize,
      server1Region1InitialSize, server2Region2InitialSize;

  @Before
  public void before() throws Exception {
    MemberVM locator = cluster.startLocatorVM(0, locatorProperties());
    server1 = cluster.startServerVM(1, locator.getPort());
    server2 = cluster.startServerVM(2, locator.getPort());

    gfsh.connectAndVerify(locator);

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();

      RegionFactory<String, String> dataRegionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      Region<String, String> region = dataRegionFactory.create(SHARED_REGION_NAME);
      for (int i = 0; i < SERVER1_SHARED_REGION_SIZE; i++) {
        region.put("key" + (i + 200), "value" + (i + 200));
      }
      region = dataRegionFactory.create(REGION1_NAME);
      for (int i = 0; i < SERVER1_REGION1_SIZE; i++) {
        region.put("key" + (i + 200), "value" + (i + 200));
      }
    });

    server2.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      RegionFactory<String, String> dataRegionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);

      Region<String, String> region = dataRegionFactory.create(SHARED_REGION_NAME);
      for (int i = 0; i < SERVER2_SHARED_REGION_SIZE; i++) {
        region.put("key" + (i + 400), "value" + (i + 400));
      }

      region = dataRegionFactory.create(REGION2_NAME);
      for (int i = 0; i < SERVER2_REGION2_SIZE; i++) {
        region.put("key" + (i + 200), "value" + (i + 200));
      }
    });

    // check if DistributedRegionMXBean is available so that command will not fail
    locator.invoke(RebalanceCommandDUnitTest::waitForManagerMBean);

    server1SharedRegionInitialSize =
        server1.invoke(() -> getLocalDataSizeForRegion(SHARED_REGION_NAME));
    server2SharedRegionInitialSize =
        server2.invoke(() -> getLocalDataSizeForRegion(SHARED_REGION_NAME));
    server1Region1InitialSize = server1.invoke(() -> getLocalDataSizeForRegion(REGION1_NAME));
    server2Region2InitialSize = server2.invoke(() -> getLocalDataSizeForRegion(REGION2_NAME));
  }

  @Test
  public void testRegionNameInResultStartsWithSlash() {
    final String REGION_NAME_WITH_SLASH = "/" + SHARED_REGION_NAME;
    String command = "rebalance --include-region=" + "/" + SHARED_REGION_NAME;
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    ResultModel result = gfsh.getCommandResult().getResultData();

    assertThat(result.getTableSections().size()).isEqualTo(1);
    assertThat(result.getTableSections().get(0).getHeader().contains(REGION_NAME_WITH_SLASH))
        .isTrue();
  }

  @Test
  public void testWithTimeOut() {
    String command = "rebalance --time-out=1";
    gfsh.executeAndAssertThat(command).statusIsSuccess();
    assertRegionBalanced(SHARED_REGION_NAME);
  }

  @Test
  public void testWithTimeOutAndRegion() {
    String command = "rebalance --time-out=1 --include-region=" + "/" + SHARED_REGION_NAME;
    gfsh.executeAndAssertThat(command).statusIsSuccess();
    assertRegionBalanced(SHARED_REGION_NAME);
  }

  @Test
  public void testWithSimulateAndRegion() {
    String command = "rebalance --simulate=true --include-region=" + "/" + SHARED_REGION_NAME;
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    assertAllRegionsUnchanged();
  }

  @Test
  public void testWithSimulate() {
    String command = "rebalance --simulate=true";
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    assertAllRegionsUnchanged();
  }

  @Test
  public void testWithTwoRegions() {
    String command =
        "rebalance --include-region=" + "/" + SHARED_REGION_NAME + ",/" + REGION2_NAME;
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    assertRegionBalanced(SHARED_REGION_NAME);
    assertThat(server2.invoke(() -> getLocalDataSizeForRegion(REGION2_NAME)))
        .isEqualTo(server2Region2InitialSize);
  }

  @Test
  public void testWithTwoSharedRegions() {
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      RegionFactory<String, String> dataRegionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      Region<String, String> region = dataRegionFactory.create(REGION2_NAME);
      for (int i = 0; i < 15; i++) {
        region.put("key" + (i + 210), "value" + (i + 210));
      }
    });

    String command =
        "rebalance --include-region=" + "/" + SHARED_REGION_NAME + ",/" + REGION2_NAME;
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    assertRegionBalanced(SHARED_REGION_NAME);
    assertRegionBalanced(REGION2_NAME);
    assertThat(server1.invoke(() -> getLocalDataSizeForRegion(REGION1_NAME)))
        .isEqualTo(server1Region1InitialSize);
  }

  @Test
  public void testWithBadRegionNames() {
    String command =
        "rebalance --include-region=" + "/" + "randomGarbageString" + ",/" + "otherRandomGarbage";
    gfsh.executeAndAssertThat(command).statusIsError();
    assertAllRegionsUnchanged();
  }

  @Test
  public void testWithOneGoodAndOneBadRegionName() {
    String command =
        "rebalance --include-region=" + "/" + SHARED_REGION_NAME + ",/" + "otherRandomGarbage";
    gfsh.executeAndAssertThat(command).statusIsSuccess();
    assertRegionBalanced(SHARED_REGION_NAME);
    assertThat(server1.invoke(() -> getLocalDataSizeForRegion(REGION1_NAME)))
        .isEqualTo(server1Region1InitialSize);
    assertThat(server2.invoke(() -> getLocalDataSizeForRegion(REGION2_NAME)))
        .isEqualTo(server2Region2InitialSize);
  }

  @Test
  public void testWithNonSharedRegions() {
    String command = "rebalance --include-region=" + "/" + REGION1_NAME + ",/" + REGION2_NAME;
    gfsh.executeAndAssertThat(command).statusIsError();

    assertAllRegionsUnchanged();
  }

  @Test
  public void testWithSimulateAndTimeout() {
    String command = "rebalance --simulate=true --time-out=1";
    gfsh.executeAndAssertThat(command).statusIsSuccess();
    assertAllRegionsUnchanged();
  }

  @Test
  public void testWithNoArgs() {
    String command = "rebalance";
    gfsh.executeAndAssertThat(command).statusIsSuccess();
    assertRegionBalanced(SHARED_REGION_NAME);
    assertThat(server1.invoke(() -> getLocalDataSizeForRegion(REGION1_NAME)))
        .isEqualTo(server1Region1InitialSize);
    assertThat(server2.invoke(() -> getLocalDataSizeForRegion(REGION2_NAME)))
        .isEqualTo(server2Region2InitialSize);
  }

  @Test
  public void testWithExcludedRegion() {
    String command = "rebalance --exclude-region=" + "/" + REGION2_NAME;
    gfsh.executeAndAssertThat(command).statusIsSuccess();
    assertRegionBalanced(SHARED_REGION_NAME);
    assertThat(server1.invoke(() -> getLocalDataSizeForRegion(REGION1_NAME)))
        .isEqualTo(server1Region1InitialSize);
    assertThat(server2.invoke(() -> getLocalDataSizeForRegion(REGION2_NAME)))
        .isEqualTo(server2Region2InitialSize);
  }

  @Test
  public void testWithExcludedSharedRegion() {
    String command = "rebalance --exclude-region=" + "/" + SHARED_REGION_NAME;
    gfsh.executeAndAssertThat(command).statusIsSuccess();
    assertAllRegionsUnchanged();
  }

  @Test
  public void testWithExcludedBadRegion() {
    String command = "rebalance --exclude-region=/asdf";
    gfsh.executeAndAssertThat(command).statusIsSuccess();
    assertRegionBalanced(SHARED_REGION_NAME);
    assertThat(server1.invoke(() -> getLocalDataSizeForRegion(REGION1_NAME)))
        .isEqualTo(server1Region1InitialSize);
    assertThat(server2.invoke(() -> getLocalDataSizeForRegion(REGION2_NAME)))
        .isEqualTo(server2Region2InitialSize);
  }

  private void assertAllRegionsUnchanged() {
    Integer sharedServer1Size = server1.invoke(() -> getLocalDataSizeForRegion(SHARED_REGION_NAME));
    Integer sharedServer2Size = server2.invoke(() -> getLocalDataSizeForRegion(SHARED_REGION_NAME));
    Integer region1Server1Size = server1.invoke(() -> getLocalDataSizeForRegion(REGION1_NAME));
    Integer region2Server2Size = server2.invoke(() -> getLocalDataSizeForRegion(REGION2_NAME));

    assertThat(sharedServer1Size).isEqualTo(server1SharedRegionInitialSize);
    assertThat(sharedServer2Size).isEqualTo(server2SharedRegionInitialSize);
    assertThat(region1Server1Size).isEqualTo(server1Region1InitialSize);
    assertThat(region2Server2Size).isEqualTo(server2Region2InitialSize);
  }

  private void assertRegionBalanced(String regionName) {
    Integer size1 = server1.invoke(() -> getLocalDataSizeForRegion(regionName));
    Integer size2 = server2.invoke(() -> getLocalDataSizeForRegion(regionName));

    assertThat(abs(size1 - size2)).isLessThanOrEqualTo(1);
  }

  private static Integer getLocalDataSizeForRegion(String regionName) {
    InternalCache cache = ClusterStartupRule.getCache();
    Region<?, ?> region = cache.getInternalRegionByPath("/" + regionName);
    return PartitionRegionHelper.getLocalData(region).size();
  }

  private static void waitForManagerMBean() {
    await().until(() -> {
      final ManagementService service =
          ManagementService.getManagementService(ClusterStartupRule.getCache());
      final DistributedRegionMXBean bean =
          service.getDistributedRegionMXBean("/" + SHARED_REGION_NAME);


      return bean != null && bean.getMembers() != null && bean.getMembers().length > 1
          && bean.getMemberCount() > 0
          && service.getDistributedSystemMXBean().listRegions().length >= 2;
    });
  }

  private Properties locatorProperties() {
    int jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOG_LEVEL, "fine");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_PORT, "" + jmxPort);

    return props;
  }
}
