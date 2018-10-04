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

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.junit.rules.GfshCommandRule.PortType.jmxManager;
import static org.assertj.core.api.Java6Assertions.assertThat;

import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@SuppressWarnings("serial")
public class RegionMembershipMBeanDUnitTestBase {
  private static final String DATA_REGION_NAME = "GemfireDataCommandsTestRegion";
  private static final String DATA_REGION_NAME_VM1 = "GemfireDataCommandsTestRegion_Vm1";
  private static final String DATA_REGION_NAME_VM2 = "GemfireDataCommandsTestRegion_Vm2";
  private static final String DATA_REGION_NAME_PATH = "/GemfireDataCommandsTestRegion";
  private static final String DATA_REGION_NAME_VM1_PATH = "/GemfireDataCommandsTestRegion_Vm1";
  private static final String DATA_REGION_NAME_VM2_PATH = "/GemfireDataCommandsTestRegion_Vm2";

  private static final String DATA_PAR_REGION_NAME = "GemfireDataCommandsTestParRegion";
  private static final String DATA_PAR_REGION_NAME_VM1 = "GemfireDataCommandsTestParRegion_Vm1";
  private static final String DATA_PAR_REGION_NAME_VM2 = "GemfireDataCommandsTestParRegion_Vm2";
  private static final String DATA_PAR_REGION_NAME_PATH = "/GemfireDataCommandsTestParRegion";
  private static final String DATA_PAR_REGION_NAME_VM1_PATH =
      "/GemfireDataCommandsTestParRegion_Vm1";
  private static final String DATA_PAR_REGION_NAME_VM2_PATH =
      "/GemfireDataCommandsTestParRegion_Vm2";
  private static final String DATA_REGION_NAME_CHILD_1 = "ChildRegionRegion1";
  private static final String DATA_REGION_NAME_CHILD_1_PATH =
      "/GemfireDataCommandsTestRegion/ChildRegionRegion1";
  private static final String DATA_REGION_NAME_CHILD_1_2 = "ChildRegionRegion12";
  private static final String DATA_REGION_NAME_CHILD_1_2_PATH =
      "/GemfireDataCommandsTestRegion/ChildRegionRegion1/ChildRegionRegion12";

  private static final String SERIALIZATION_FILTER =
      "org.apache.geode.management.internal.cli.**";

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  protected MemberVM locator, server1, server2;

  @Before
  public void before() throws Exception {
    Properties props = locatorProperties();
    props.setProperty(SERIALIZABLE_OBJECT_FILTER, SERIALIZATION_FILTER);
    props.setProperty(NAME, "locator");

    Properties serverProps = new Properties();
    serverProps.setProperty(SERIALIZABLE_OBJECT_FILTER, SERIALIZATION_FILTER);
    locator = cluster.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    server1 = cluster.startServerVM(1, serverProps, locator.getPort());
    server2 = cluster.startServerVM(2, serverProps, locator.getPort());

    connectToLocator();
  }

  // extracted in order to override in RegionMembershipMBeanOverHttpDUnitTest to use http connection
  public void connectToLocator() throws Exception {
    gfsh.connectAndVerify(locator.getJmxPort(), jmxManager);
  }

  private static void setupDataRegionAndSubregions() {
    InternalCache cache = ClusterStartupRule.getCache();
    RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE);

    Region dataRegion = regionFactory.create(DATA_REGION_NAME);
    assertThat(dataRegion).isNotNull();

    Region dataSubRegion = regionFactory.createSubregion(dataRegion, DATA_REGION_NAME_CHILD_1);
    assertThat(dataSubRegion).isNotNull();

    dataSubRegion = regionFactory.createSubregion(dataSubRegion, DATA_REGION_NAME_CHILD_1_2);
    assertThat(dataSubRegion).isNotNull();
    assertThat(dataRegion).isNotNull();
  }

  private static void setupReplicatedRegion(String regionName) {
    InternalCache cache = ClusterStartupRule.getCache();
    RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE);

    Region dataRegion = regionFactory.create(regionName);
    assertThat(dataRegion).isNotNull();
    assertThat(dataRegion.getFullPath()).contains(regionName);
  }

  private static void setupPartitionedRegion(String regionName) {
    InternalCache cache = ClusterStartupRule.getCache();
    PartitionAttributes partitionAttrs =
        new PartitionAttributesFactory().setRedundantCopies(2).create();
    RegionFactory<Object, Object> partitionRegionFactory =
        cache.createRegionFactory(RegionShortcut.PARTITION);

    partitionRegionFactory.setPartitionAttributes(partitionAttrs);
    Region dataParRegion = partitionRegionFactory.create(regionName);

    assertThat(dataParRegion).isNotNull();
    assertThat(dataParRegion.getFullPath()).contains(regionName);
  }

  @Test
  public void testReplicatedRegionOnOneMember() {
    server1.invoke(() -> setupReplicatedRegion(DATA_REGION_NAME));
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_PATH, 1);

    Integer memSizeFromMBean = distributedRegionMembersSizeFromMBean(DATA_REGION_NAME_PATH);
    Integer memSizeFromFunctionCall =
        distributedRegionMembersSizeFromFunction(DATA_REGION_NAME_PATH);
    assertThat(memSizeFromFunctionCall).isEqualTo(memSizeFromMBean).isEqualTo(1);
  }

  @Test
  public void testMultipleReplicatedRegionsAndSubregions() {
    server1.invoke(() -> setupDataRegionAndSubregions());
    server1.invoke(() -> setupReplicatedRegion(DATA_REGION_NAME_VM1));

    server2.invoke(() -> setupDataRegionAndSubregions());
    server2.invoke(() -> setupReplicatedRegion(DATA_REGION_NAME_VM2));

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_PATH, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_CHILD_1_PATH, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_CHILD_1_2_PATH, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_VM1_PATH, 1);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_VM2_PATH, 1);

    Integer memSizeFromMBean = distributedRegionMembersSizeFromMBean(DATA_REGION_NAME_PATH);
    Integer memSizeFromFunctionCall =
        distributedRegionMembersSizeFromFunction(DATA_REGION_NAME_PATH);
    assertThat(memSizeFromFunctionCall).isEqualTo(memSizeFromMBean);

    memSizeFromMBean = distributedRegionMembersSizeFromMBean(DATA_REGION_NAME_CHILD_1_PATH);
    memSizeFromFunctionCall =
        distributedRegionMembersSizeFromFunction(DATA_REGION_NAME_CHILD_1_PATH);
    assertThat(memSizeFromFunctionCall).isEqualTo(memSizeFromMBean);

    memSizeFromMBean = distributedRegionMembersSizeFromMBean(DATA_REGION_NAME_CHILD_1_2_PATH);
    memSizeFromFunctionCall =
        distributedRegionMembersSizeFromFunction(DATA_REGION_NAME_CHILD_1_2_PATH);
    assertThat(memSizeFromFunctionCall).isEqualTo(memSizeFromMBean);

    memSizeFromMBean = distributedRegionMembersSizeFromMBean(DATA_REGION_NAME_VM1_PATH);
    memSizeFromFunctionCall = distributedRegionMembersSizeFromFunction(DATA_REGION_NAME_VM1_PATH);
    assertThat(memSizeFromFunctionCall).isEqualTo(memSizeFromMBean);

    memSizeFromMBean = distributedRegionMembersSizeFromMBean(DATA_REGION_NAME_VM2_PATH);
    memSizeFromFunctionCall = distributedRegionMembersSizeFromFunction(DATA_REGION_NAME_VM2_PATH);
    assertThat(memSizeFromFunctionCall).isEqualTo(memSizeFromMBean);
  }

  @Test
  public void testAddRmNewMemberWithReplicatedRegionsAndSubregions() {
    server1.invoke(() -> setupDataRegionAndSubregions());
    server2.invoke(() -> setupDataRegionAndSubregions());

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_PATH, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_CHILD_1_PATH, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_CHILD_1_2_PATH, 2);

    Integer initialMemberSizeFromMBean =
        distributedRegionMembersSizeFromMBean(DATA_REGION_NAME_PATH);
    Integer initialMemberSizeFromFunction =
        distributedRegionMembersSizeFromFunction(DATA_REGION_NAME_PATH);
    assertThat(initialMemberSizeFromFunction).isEqualTo(initialMemberSizeFromMBean);

    Integer initialMemberSizeChild1FromMBean =
        distributedRegionMembersSizeFromMBean(DATA_REGION_NAME_CHILD_1_PATH);
    Integer initialMemberSizeChild1FromFunction =
        distributedRegionMembersSizeFromFunction(DATA_REGION_NAME_CHILD_1_PATH);
    assertThat(initialMemberSizeChild1FromFunction).isEqualTo(initialMemberSizeChild1FromMBean);

    Integer initialMemberSizeChild2FromMBean =
        distributedRegionMembersSizeFromMBean(DATA_REGION_NAME_CHILD_1_2_PATH);
    Integer initialMemberSizeChild2FromFunction =
        distributedRegionMembersSizeFromFunction(DATA_REGION_NAME_CHILD_1_2_PATH);
    assertThat(initialMemberSizeChild2FromFunction).isEqualTo(initialMemberSizeChild2FromMBean);

    MemberVM server3 = cluster.startServerVM(3, locator.getPort());
    server3.invoke(() -> setupDataRegionAndSubregions());

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_PATH, 3);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_CHILD_1_PATH, 3);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_CHILD_1_2_PATH, 3);

    Integer intermediateMemberSizeFromMBean =
        distributedRegionMembersSizeFromMBean(DATA_REGION_NAME_PATH);
    Integer intermediateMemberSizeFromFunction =
        distributedRegionMembersSizeFromFunction(DATA_REGION_NAME_PATH);
    assertThat(intermediateMemberSizeFromFunction).isEqualTo(intermediateMemberSizeFromMBean)
        .isEqualTo(initialMemberSizeFromFunction + 1);

    Integer intermediateMemberSizeChild1FromMBean =
        distributedRegionMembersSizeFromMBean(DATA_REGION_NAME_CHILD_1_PATH);
    Integer intermediateMemberSizeChild1FromFunction =
        distributedRegionMembersSizeFromFunction(DATA_REGION_NAME_CHILD_1_PATH);
    assertThat(intermediateMemberSizeChild1FromMBean)
        .isEqualTo(intermediateMemberSizeChild1FromFunction)
        .isEqualTo(initialMemberSizeChild1FromFunction + 1);

    Integer intermediateMemberSizeChild2FromMBean =
        distributedRegionMembersSizeFromMBean(DATA_REGION_NAME_CHILD_1_2_PATH);
    Integer intermediateMemberSizeChild2FromFunction =
        distributedRegionMembersSizeFromFunction(DATA_REGION_NAME_CHILD_1_2_PATH);
    assertThat(intermediateMemberSizeChild2FromFunction)
        .isEqualTo(intermediateMemberSizeChild2FromMBean)
        .isEqualTo(initialMemberSizeChild2FromMBean + 1);

    server3.stop(Boolean.TRUE);

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_PATH, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_CHILD_1_PATH, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_CHILD_1_2_PATH, 2);

    Integer finalMemberSizeFromMBean = distributedRegionMembersSizeFromMBean(DATA_REGION_NAME_PATH);
    Integer finalMemberSizeFromFunction =
        distributedRegionMembersSizeFromFunction(DATA_REGION_NAME_PATH);
    assertThat(finalMemberSizeFromFunction).isEqualTo(finalMemberSizeFromMBean)
        .isEqualTo(initialMemberSizeFromFunction);

    Integer finalMemberSizeChild1FromMBean =
        distributedRegionMembersSizeFromMBean(DATA_REGION_NAME_CHILD_1_PATH);
    Integer finalMemberSizeChild1FromFunction =
        distributedRegionMembersSizeFromFunction(DATA_REGION_NAME_CHILD_1_PATH);
    assertThat(finalMemberSizeChild1FromMBean).isEqualTo(finalMemberSizeChild1FromFunction)
        .isEqualTo(initialMemberSizeChild1FromFunction);

    Integer finalMemberSizeChild2FromMBean =
        distributedRegionMembersSizeFromMBean(DATA_REGION_NAME_CHILD_1_2_PATH);
    Integer finalMemberSizeChild2FromFunction =
        distributedRegionMembersSizeFromFunction(DATA_REGION_NAME_CHILD_1_2_PATH);
    assertThat(finalMemberSizeChild2FromFunction).isEqualTo(finalMemberSizeChild2FromMBean)
        .isEqualTo(initialMemberSizeChild2FromMBean);
  }

  @Test
  public void testPartitionedRegionOnOneMember() {
    server1.invoke(() -> setupPartitionedRegion(DATA_PAR_REGION_NAME));
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_PAR_REGION_NAME_PATH, 1);

    Integer memSizeFromMBean = distributedRegionMembersSizeFromMBean(DATA_PAR_REGION_NAME_PATH);
    Integer memSizeFromFunctionCall =
        distributedRegionMembersSizeFromFunction(DATA_PAR_REGION_NAME_PATH);
    assertThat(memSizeFromFunctionCall).isEqualTo(memSizeFromMBean).isEqualTo(1);
  }

  @Test
  public void testMultiplePartitionedRegions() {
    server1.invoke(() -> setupPartitionedRegion(DATA_PAR_REGION_NAME));
    server1.invoke(() -> setupReplicatedRegion(DATA_PAR_REGION_NAME_VM1));

    server2.invoke(() -> setupPartitionedRegion(DATA_PAR_REGION_NAME));
    server2.invoke(() -> setupReplicatedRegion(DATA_PAR_REGION_NAME_VM2));

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_PAR_REGION_NAME_PATH, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_PAR_REGION_NAME_VM1_PATH, 1);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_PAR_REGION_NAME_VM2_PATH, 1);

    Integer memSizeFromMBean = distributedRegionMembersSizeFromMBean(DATA_PAR_REGION_NAME_PATH);
    Integer memSizeFromFunctionCall =
        distributedRegionMembersSizeFromFunction(DATA_PAR_REGION_NAME_PATH);
    assertThat(memSizeFromFunctionCall).isEqualTo(memSizeFromMBean);

    memSizeFromMBean = distributedRegionMembersSizeFromMBean(DATA_PAR_REGION_NAME_VM1_PATH);
    memSizeFromFunctionCall =
        distributedRegionMembersSizeFromFunction(DATA_PAR_REGION_NAME_VM1_PATH);
    assertThat(memSizeFromFunctionCall).isEqualTo(memSizeFromMBean);

    memSizeFromMBean = distributedRegionMembersSizeFromMBean(DATA_PAR_REGION_NAME_VM2_PATH);
    memSizeFromFunctionCall =
        distributedRegionMembersSizeFromFunction(DATA_PAR_REGION_NAME_VM2_PATH);
    assertThat(memSizeFromFunctionCall).isEqualTo(memSizeFromMBean);
  }

  @Test
  public void testAddRmNewMemberWithPartitionedRegions() {
    server1.invoke(() -> setupPartitionedRegion(DATA_PAR_REGION_NAME));
    server2.invoke(() -> setupPartitionedRegion(DATA_PAR_REGION_NAME));
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_PAR_REGION_NAME_PATH, 2);

    Integer initialMemberSizeFromMBean =
        distributedRegionMembersSizeFromMBean(DATA_PAR_REGION_NAME_PATH);
    Integer initialMemberSizeFromFunctionCall =
        distributedRegionMembersSizeFromFunction(DATA_PAR_REGION_NAME_PATH);
    assertThat(initialMemberSizeFromFunctionCall).isEqualTo(initialMemberSizeFromMBean);

    MemberVM server3 = cluster.startServerVM(3, locator.getPort());
    server3.invoke(() -> setupPartitionedRegion(DATA_PAR_REGION_NAME));
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_PAR_REGION_NAME_PATH, 3);

    Integer intermediateMemberSizeFromMBean =
        distributedRegionMembersSizeFromMBean(DATA_PAR_REGION_NAME_PATH);
    Integer intermediateMemberSizeFromFunctionCall =
        distributedRegionMembersSizeFromFunction(DATA_PAR_REGION_NAME_PATH);
    assertThat(intermediateMemberSizeFromFunctionCall).isEqualTo(intermediateMemberSizeFromMBean)
        .isEqualTo(initialMemberSizeFromMBean + 1);

    server3.stop(Boolean.TRUE);

    Integer finalMemberSizeFromMBean =
        distributedRegionMembersSizeFromMBean(DATA_PAR_REGION_NAME_PATH);
    Integer finalMemberSizeFromFunctionCall =
        distributedRegionMembersSizeFromFunction(DATA_PAR_REGION_NAME_PATH);
    assertThat(finalMemberSizeFromFunctionCall).isEqualTo(finalMemberSizeFromMBean)
        .isEqualTo(initialMemberSizeFromFunctionCall);
  }

  private Integer distributedRegionMembersSizeFromFunction(String regionName) {
    return locator.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      Set<DistributedMember> distributedMembers =
          CliUtil.getRegionAssociatedMembers(regionName, cache, true);

      return distributedMembers.size();
    });
  }

  private Integer distributedRegionMembersSizeFromMBean(String regionPath) {
    return locator.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();

      await().untilAsserted(() -> {
        DistributedRegionMXBean bean = ManagementService.getManagementService(cache)
            .getDistributedRegionMXBean(regionPath);
        assertThat(bean).isNotNull();
      });

      DistributedRegionMXBean bean = ManagementService.getManagementService(cache)
          .getDistributedRegionMXBean(regionPath);
      String[] membersName = bean.getMembers();
      return membersName.length;
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
