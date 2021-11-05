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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.client.internal.ClientPartitionAdvisor;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.execute.data.ShipmentId;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.Locator;
import org.apache.geode.util.internal.GeodeGlossary;

@Category(ClientServerTest.class)
public class PartitionedRegionSingleHopWithServerGroupDUnitTest implements Serializable {
  private static final Logger logger = LogService.getLogger();

  @Rule
  public final ClusterStartupRule clusterStartupRule = new ClusterStartupRule(4);

  private static final String TEST_REGION = "single_hop_pr";
  private static final String TEST_REGION2 = "single_hop_pr_2";
  private static final String PR_NAME3 = "single_hop_pr_3";
  private static final String CUSTOMER = "CUSTOMER";
  private static final String ORDER = "ORDER";
  private static final String SHIPMENT = "SHIPMENT";

  private static final String CUSTOMER2 = "CUSTOMER2";
  private static final String ORDER2 = "ORDER2";
  private static final String SHIPMENT2 = "SHIPMENT2";

  private MemberVM member0 = null;
  private MemberVM member1 = null;
  private MemberVM member2 = null;
  private MemberVM member3 = null;

  @Before
  public final void postSetUp() throws Exception {
    IgnoredException.addIgnoredException("java.net.SocketException");
  }

  @After
  public final void preTearDownCacheTestCase() throws Exception {
    // close the clients first
    for (int i = 0; i < 4; i++) {
      VM vm = clusterStartupRule.getVM(i);
      vm.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::closeCacheAndDisconnect);
    }
    closeCacheAndDisconnect();
    DistributedTestUtils.unregisterAllDataSerializersFromAllVms();
  }

  private static void closeCacheAndDisconnect() {
    Cache cache = CacheFactory.getAnyInstance();
    resetHonourServerGroupsInPRSingleHop();
    if (!cache.isClosed()) {
      cache.close();
    }
  }

  @Test
  public void test_SingleHopWith2ServerGroup() {
    final String host0 = NetworkUtils.getServerHostName();
    member3 = clusterStartupRule.startLocatorVM(3);
    int locatorPort = member3.getPort();
    final String locator = host0 + "[" + locatorPort + "]";
    try {
      Properties props = new Properties();
      props.setProperty(LOCATORS, locator);
      System.setProperty(
          GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true");

      member0 = clusterStartupRule.startServerVM(0, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group1"));
      member1 = clusterStartupRule.startServerVM(1, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group2"));
      member2 = clusterStartupRule.startServerVM(2, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group2"));

      setupRegionsWithLocalMaxMemory(member0, 100);
      setupRegionsWithLocalMaxMemory(member1, 100);
      setupRegionsWithLocalMaxMemory(member2, 100);

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, locatorPort, "group1");

      // put
      putIntoPartitionedRegions();

      getFromPartitionedRegions();

      try {
        verifyMetadata(4, 1);
      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      PartitionedRegionSingleHopWithServerGroupDUnitTest.stopLocator(member3);
    }
  }

  private static void setupRegionsWithLocalMaxMemory(MemberVM memberVM, int localMaxMemory) {
    memberVM.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      int redundantCopies = 2;
      int numBuckets = 8;

      createBasicPartitionedRegion(cache, redundantCopies, numBuckets, localMaxMemory, TEST_REGION);
      // creating colocated Regions
      createColocatedRegion(cache, CUSTOMER, null, redundantCopies, numBuckets,
          localMaxMemory, new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));
      createColocatedRegion(cache, ORDER, CUSTOMER, redundantCopies, numBuckets,
          localMaxMemory, new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));
      createColocatedRegion(cache, SHIPMENT, ORDER, redundantCopies, numBuckets,
          localMaxMemory, new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));
    });

  }

  @Test
  public void test_SingleHopWith2ServerGroup2() {
    final String host0 = NetworkUtils.getServerHostName();
    member3 = clusterStartupRule.startLocatorVM(3);
    int locatorPort = member3.getPort();
    final String locator = host0 + "[" + locatorPort + "]";
    try {

      Properties props = new Properties();
      props.setProperty(LOCATORS, locator);
      System.setProperty(
          GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true");

      member0 = clusterStartupRule.startServerVM(0, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group1"));
      member1 = clusterStartupRule.startServerVM(1, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group1"));
      member2 = clusterStartupRule.startServerVM(2, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group2"));

      setupRegionsWithLocalMaxMemory(member0, 100);
      setupRegionsWithLocalMaxMemory(member1, 100);
      setupRegionsWithLocalMaxMemory(member2, 100);

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, locatorPort, "group1");

      // put
      putIntoPartitionedRegions();

      getFromPartitionedRegions();

      try {
        verifyMetadata(4, 2);
      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(() -> stopLocator(member3));
    }
  }

  @Test
  public void test_SingleHopWith2ServerGroup2WithoutSystemProperty() {
    final String host0 = NetworkUtils.getServerHostName();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    props.setProperty(LOG_FILE, "");
    member3 = clusterStartupRule.startLocatorVM(3, props);
    int locatorPort = member3.getPort();
    final String locator = host0 + "[" + locatorPort + "]";
    try {
      Properties serverProperties = new Properties();
      serverProperties.setProperty(LOCATORS, locator);
      serverProperties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

      member0 = clusterStartupRule.startServerVM(0, serverStarterRule -> serverStarterRule
          .withProperties(serverProperties).withPort(getRandomAvailableTCPPort())
          .withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group1")
          .withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true"));
      member1 = clusterStartupRule.startServerVM(1, serverStarterRule -> serverStarterRule
          .withProperties(serverProperties)
          .withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group1").withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true"));
      member2 = clusterStartupRule.startServerVM(2, serverStarterRule -> serverStarterRule
          .withProperties(serverProperties).withPort(getRandomAvailableTCPPort())
          .withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group2"));

      setupRegionsWithLocalMaxMemory(member0, 100);
      setupRegionsWithLocalMaxMemory(member1, 100);
      setupRegionsWithLocalMaxMemory(member2, 100);

      createClientWithLocator(host0, locatorPort, "group1");

      // put
      putIntoPartitionedRegions();

      getFromPartitionedRegions();

      verifyMetadata(4, 3);
    } finally {
      member3.invoke(() -> stopLocator(member3));
    }
  }

  @Test
  public void test_SingleHopWithServerGroupAccessor() {
    final String host0 = NetworkUtils.getServerHostName();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    props.setProperty(LOG_FILE, "");
    member3 = clusterStartupRule.startLocatorVM(3, props);
    int locatorPort = member3.getPort();
    final String locator = host0 + "[" + locatorPort + "]";
    try {

      Properties serverProperties = new Properties();
      serverProperties.setProperty(LOCATORS, locator);
      System.setProperty(
          GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true");

      member0 = clusterStartupRule.startServerVM(0, serverStarterRule -> serverStarterRule
          .withProperties(serverProperties).withPort(getRandomAvailableTCPPort())
          .withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group1"));
      member1 = clusterStartupRule.startServerVM(1, serverStarterRule -> serverStarterRule
          .withProperties(serverProperties).withPort(getRandomAvailableTCPPort())
          .withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group2"));
      member2 = clusterStartupRule.startServerVM(2, serverStarterRule -> serverStarterRule
          .withProperties(serverProperties).withPort(getRandomAvailableTCPPort())
          .withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group2"));

      setupRegionsWithLocalMaxMemory(member0, 0);
      setupRegionsWithLocalMaxMemory(member1, 100);
      setupRegionsWithLocalMaxMemory(member2, 100);

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, locatorPort, "group1");

      putIntoPartitionedRegions();

      getFromPartitionedRegions();

      try {
        verifyMetadata(0, 0);
      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(() -> stopLocator(member3));
    }
  }

  @Test
  public void test_SingleHopWithServerGroupOneServerInTwoGroups() {
    final String host0 = NetworkUtils.getServerHostName();
    member3 = clusterStartupRule.startLocatorVM(3);
    int locatorPort = member3.getPort();
    final String locator = host0 + "[" + locatorPort + "]";
    try {

      Properties props = new Properties();
      props.setProperty(LOCATORS, locator);
      System.setProperty(
          GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true");

      member0 = clusterStartupRule.startServerVM(0, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group1"));
      member1 = clusterStartupRule.startServerVM(1, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group1"));
      member2 = clusterStartupRule.startServerVM(2, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group1,group2"));

      setupRegionsWithLocalMaxMemory(member0, 100);
      setupRegionsWithLocalMaxMemory(member1, 100);
      setupRegionsWithLocalMaxMemory(member2, 100);

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, locatorPort, "group1");

      putIntoPartitionedRegions();

      getFromPartitionedRegions();

      try {
        verifyMetadata(4, 3);
      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(() -> stopLocator(member3));
    }
  }

  @Test
  public void test_SingleHopWithServerGroupWithOneDefaultServer() {
    final String host0 = NetworkUtils.getServerHostName();
    member3 = clusterStartupRule.startLocatorVM(3);
    int locatorPort = member3.getPort();
    final String locator = host0 + "[" + locatorPort + "]";

    try {
      Properties props = new Properties();
      props.setProperty(LOCATORS, locator);
      System.setProperty(
          GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true");

      member0 = clusterStartupRule.startServerVM(0, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group1"));
      member1 = clusterStartupRule.startServerVM(1, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group2"));
      member2 = clusterStartupRule.startServerVM(2, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, ""));

      setupRegionsWithLocalMaxMemory(member0, 100);
      setupRegionsWithLocalMaxMemory(member1, 100);
      setupRegionsWithLocalMaxMemory(member2, 100);

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, locatorPort, "group1");

      putIntoPartitionedRegions();

      getFromPartitionedRegions();

      try {
        verifyMetadata(4, 2);
      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(() -> stopLocator(member3));
    }
  }

  @Test
  public void test_SingleHopWithServerGroupClientServerGroupNull() {
    final String host0 = NetworkUtils.getServerHostName();
    member3 = clusterStartupRule.startLocatorVM(3);
    int locatorPort = member3.getPort();
    final String locator = host0 + "[" + locatorPort + "]";
    try {

      Properties props = new Properties();
      props.setProperty(LOCATORS, locator);
      System.setProperty(
          GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true");

      member0 = clusterStartupRule.startServerVM(0, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group1"));
      member1 = clusterStartupRule.startServerVM(1, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group2"));
      member2 = clusterStartupRule.startServerVM(2, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group3"));

      setupRegionsWithLocalMaxMemory(member0, 100);
      setupRegionsWithLocalMaxMemory(member1, 100);
      setupRegionsWithLocalMaxMemory(member2, 100);

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, locatorPort, "");

      putIntoPartitionedRegions();

      getFromPartitionedRegions();

      try {
        verifyMetadata(4, 3);
      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(() -> stopLocator(member3));
    }
  }

  @Test
  public void test_SingleHopWithServerGroupTwoClientServerGroup() {
    final String host0 = NetworkUtils.getServerHostName();
    member3 = clusterStartupRule.startLocatorVM(3);
    int locatorPort = member3.getPort();
    final String locator = host0 + "[" + locatorPort + "]";
    try {

      Properties props = new Properties();
      props.setProperty(LOCATORS, locator);
      System.setProperty(
          GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true");

      member0 = clusterStartupRule.startServerVM(0, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group1"));
      member1 = clusterStartupRule.startServerVM(1, serverStarterRule -> serverStarterRule
          .withProperties(props).withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group2"));

      setupRegionsWithLocalMaxMemory(member0, 100);
      setupRegionsWithLocalMaxMemory(member1, 100);

      VM clientVM = clusterStartupRule.getVM(2);
      clientVM.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::setHonourServerGroupsInPRSingleHop);

      clientVM.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createClientWithLocator(host0, locatorPort, "group1"));

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, locatorPort, "");

      clientVM.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::putIntoPartitionedRegions);

      putIntoPartitionedRegions();

      getFromPartitionedRegions();
      clientVM.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::getFromPartitionedRegions);

      try {
        verifyMetadata(4, 2);
        clientVM
            .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.verifyMetadata(4, 1));

      } finally {
        resetHonourServerGroupsInPRSingleHop();
        clientVM.invoke(
            PartitionedRegionSingleHopWithServerGroupDUnitTest::resetHonourServerGroupsInPRSingleHop);
      }
    } finally {
      member3.invoke(() -> stopLocator(member3));
    }
  }

  @Test
  public void test_SingleHopWithServerGroupTwoClientServerGroup2() {
    final String host0 = NetworkUtils.getServerHostName();
    member3 = clusterStartupRule.startLocatorVM(3);
    int locatorPort = member3.getPort();
    final String locator = host0 + "[" + locatorPort + "]";
    try {

      Properties props = new Properties();
      props.setProperty(LOCATORS, locator);
      System.setProperty(
          GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true");

      member0 = clusterStartupRule.startServerVM(0, serverStarterRule -> serverStarterRule
          .withProperties(props)
          .withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group1,group2"));
      member1 = clusterStartupRule.startServerVM(1, serverStarterRule -> serverStarterRule
          .withProperties(props)
          .withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group2"));

      setupRegionsWithLocalMaxMemory(member0, 100);
      setupRegionsWithLocalMaxMemory(member1, 100);

      VM clientVM = clusterStartupRule.getVM(2);
      clientVM.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::setHonourServerGroupsInPRSingleHop);

      clientVM.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createClientWithLocator(host0, locatorPort, "group1"));

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, locatorPort, "group2");
      clientVM.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::putIntoPartitionedRegions);
      putIntoPartitionedRegions();
      clientVM.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::getFromPartitionedRegions);
      getFromPartitionedRegions();

      try {
        verifyMetadata(4, 2);
        clientVM.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
            .verifyMetadata(4, 1));

      } finally {
        resetHonourServerGroupsInPRSingleHop();
        clientVM.invoke(
            PartitionedRegionSingleHopWithServerGroupDUnitTest::resetHonourServerGroupsInPRSingleHop);
      }
    } finally {
      member3.invoke(() -> stopLocator(member3));
    }
  }

  @Test
  public void test_SingleHopWithServerGroupTwoClientOneWithOneWithoutServerGroup() {
    final String host0 = NetworkUtils.getServerHostName();
    member3 = clusterStartupRule.startLocatorVM(3);
    int locatorPort = member3.getPort();
    final String locator = host0 + "[" + locatorPort + "]";
    try {
      Properties props = new Properties();
      props.setProperty(LOCATORS, locator);
      System.setProperty(
          GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true");

      member0 = clusterStartupRule.startServerVM(0,
          serverStarterRule -> serverStarterRule
              .withProperties(props)
              .withPort(getRandomAvailableTCPPort()).withSystemProperty(
                  GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
              .withProperty(GROUPS, "group1,group2"));
      member1 = clusterStartupRule.startServerVM(1, serverStarterRule -> serverStarterRule
          .withProperties(props)
          .withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group2"));

      setupRegionsWithLocalMaxMemory(member0, 100);
      setupRegionsWithLocalMaxMemory(member1, 100);

      VM clientVM = clusterStartupRule.getVM(2);
      clientVM.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createClientWithLocator(host0, locatorPort, "group1"));

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, locatorPort, "group2");
      clientVM.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::putIntoPartitionedRegions);
      putIntoPartitionedRegions();
      clientVM.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::getFromPartitionedRegions);
      getFromPartitionedRegions();

      try {
        verifyMetadata(4, 2);
        clientVM.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
            .verifyMetadata(4, 2));

      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(() -> stopLocator(member3));
    }
  }

  @Test
  public void test_SingleHopWithServerGroup2ClientInOneVMServerGroup() {
    final String host0 = NetworkUtils.getServerHostName();
    member3 = clusterStartupRule.startLocatorVM(3);
    int locatorPort = member3.getPort();
    final String locator = host0 + "[" + locatorPort + "]";
    try {
      Properties props = new Properties();
      props.setProperty(LOCATORS, locator);
      System.setProperty(
          GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true");

      member0 = clusterStartupRule.startServerVM(0, serverStarterRule -> serverStarterRule
          .withProperties(props)
          .withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group1,group2"));
      member1 = clusterStartupRule.startServerVM(1, serverStarterRule -> serverStarterRule
          .withProperties(props)
          .withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group2"));

      member0.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::setupMultiRegions);
      member1.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::setupMultiRegions);

      VM clientVM = clusterStartupRule.getVM(2);
      clientVM.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::setHonourServerGroupsInPRSingleHop);

      clientVM.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .create2ClientWithLocator(host0, locatorPort, "group1", ""));

      setHonourServerGroupsInPRSingleHop();
      create2ClientWithLocator(host0, locatorPort, "group2", "group1");
      clientVM.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::putIntoPartitionedRegions2Client);
      putIntoPartitionedRegions2Client();
      clientVM.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::getFromPartitionedRegions2Client);
      getFromPartitionedRegions2Client();

      try {
        verifyMetadataFor2ClientsInOneVM(2, 1);
        clientVM.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
            .verifyMetadataFor2ClientsInOneVM(1, 2));

      } finally {
        resetHonourServerGroupsInPRSingleHop();
        clientVM.invoke(
            PartitionedRegionSingleHopWithServerGroupDUnitTest::resetHonourServerGroupsInPRSingleHop);
      }
    } finally {
      member3.invoke(() -> stopLocator(member3));
    }
  }

  @Test
  public void test_SingleHopWithServerGroupColocatedRegionsInDifferentGroup() {
    final String host0 = NetworkUtils.getServerHostName();
    member3 = clusterStartupRule.startLocatorVM(3);
    int locatorPort = member3.getPort();
    final String locator = host0 + "[" + locatorPort + "]";
    try {
      Properties props = new Properties();
      props.setProperty(LOCATORS, locator);
      System.setProperty(
          GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true");

      member0 = clusterStartupRule.startServerVM(0,
          serverStarterRule -> serverStarterRule
              .withProperties(props)
              .withPort(getRandomAvailableTCPPort()).withSystemProperty(
                  GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
              .withProperty(GROUPS, "group1,group2"));
      member1 = clusterStartupRule.startServerVM(1, serverStarterRule -> serverStarterRule
          .withProperties(props)
          .withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group2"));
      member2 = clusterStartupRule.startServerVM(2, serverStarterRule -> serverStarterRule
          .withProperties(props)
          .withPort(getRandomAvailableTCPPort()).withSystemProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true")
          .withProperty(GROUPS, "group3"));

      setupRegionsWithLocalMaxMemory(member0, 100);
      setupRegionsWithLocalMaxMemory(member1, 100);
      setupRegionsWithLocalMaxMemory(member2, 100);

      setHonourServerGroupsInPRSingleHop();
      createClientWith3PoolLocator(host0, locatorPort);
      putIntoPartitionedRegions();
      getFromPartitionedRegions();

      try {
        verifyMetadataForColocatedRegionWithDiffPool();
      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(() -> stopLocator(member3));
    }
  }

  private static void verifyMetadata(final int numRegions, final int numBucketLocations) {
    Cache cache = CacheFactory.getAnyInstance();
    Region<Object, Object> testRegion = getRegion(TEST_REGION);

    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();


    await().alias("expected metadata for each region to be" + numRegions + " but it is "
        + regionMetaData.size() + "Metadata is " + regionMetaData.keySet())
        .until(() -> regionMetaData.size() == numRegions);

    if (numRegions != 0) {
      assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();
      ClientPartitionAdvisor prMetaData = regionMetaData.get(testRegion.getFullPath());
      if (logger.getLevel() == Level.DEBUG) {
        for (Entry<?, ?> e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
          logger.debug("For bucket id " + e.getKey() + " the locations are " + e.getValue());
        }
      }

      for (Entry<?, ?> entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
        assertThat(((List<?>) entry.getValue()).size()).isEqualTo(numBucketLocations);
      }
    }
  }

  private static void verifyMetadataForColocatedRegionWithDiffPool() {
    Cache cache = CacheFactory.getAnyInstance();
    Region<Object, Object> customerRegion = getRegion(CUSTOMER);
    Region<Object, Object> orderRegion = getRegion(ORDER);
    Region<Object, Object> shipmentRegion = getRegion(SHIPMENT);
    Region<Object, Object> testRegion = getRegion(TEST_REGION);

    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();


    await().alias("expected metadata for each region to be " + 4 + " but it is "
        + regionMetaData.size() + " they are " + regionMetaData.keySet())
        .until(() -> regionMetaData.size() == 4);

    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();
    ClientPartitionAdvisor prMetaData = regionMetaData.get(testRegion.getFullPath());
    for (Entry<?, ?> e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      logger.debug("For bucket id " + e.getKey() + " the locations are " + e.getValue());
    }

    for (Entry<?, ?> entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertThat(((List<?>) entry.getValue()).size()).isEqualTo(2);
    }

    assertThat(regionMetaData.containsKey(customerRegion.getFullPath())).isTrue();
    prMetaData = regionMetaData.get(customerRegion.getFullPath());
    for (Entry<?, ?> e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      logger.debug("For bucket id " + e.getKey() + " the locations are " + e.getValue());
    }

    for (Entry<?, ?> entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertThat(((List<?>) entry.getValue()).size()).isEqualTo(2);
    }

    assertThat(regionMetaData.containsKey(orderRegion.getFullPath())).isTrue();
    prMetaData = regionMetaData.get(orderRegion.getFullPath());
    for (Entry<?, ?> e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      logger.debug("For bucket id " + e.getKey() + " the locations are " + e.getValue());
    }

    for (Entry<?, ?> entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertThat(((List<?>) entry.getValue()).size()).isEqualTo(1);
    }

    assertThat(regionMetaData.containsKey(shipmentRegion.getFullPath())).isTrue();
    prMetaData = regionMetaData.get(shipmentRegion.getFullPath());
    for (Entry<?, ?> e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      logger.debug("For bucket id " + e.getKey() + " the locations are " + e.getValue());
    }

    for (Entry<?, ?> entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertThat(((List<?>) entry.getValue()).size()).isEqualTo(3);
    }

  }

  private static void verifyMetadataFor2ClientsInOneVM(final int numBucketLocations,
      final int numBucketLocations2) {
    Cache cache = CacheFactory.getAnyInstance();
    Region<Object, Object> customerRegion = getRegion(CUSTOMER);
    Region<Object, Object> customerRegion2 = getRegion(CUSTOMER2);
    Region<Object, Object> testRegion = getRegion(TEST_REGION);

    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();

    await().alias("expected metadata for each region to be " + 8 + " but it is "
        + regionMetaData.size() + " they are " + regionMetaData.keySet())
        .until(() -> regionMetaData.size() == 8);

    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();
    ClientPartitionAdvisor prMetaData = regionMetaData.get(testRegion.getFullPath());
    for (Entry<?, ?> e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      logger.debug("For bucket id " + e.getKey() + " the locations are " + e.getValue());
    }

    for (Entry<?, ?> entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertThat(((List<?>) entry.getValue()).size()).isEqualTo(numBucketLocations);
    }

    assertThat(regionMetaData.containsKey(customerRegion.getFullPath())).isTrue();
    prMetaData = regionMetaData.get(customerRegion.getFullPath());
    for (Entry<?, ?> e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      logger.debug("For bucket id " + e.getKey() + " the locations are " + e.getValue());
    }

    for (Entry<?, ?> entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertThat(((List<?>) entry.getValue()).size()).isEqualTo(numBucketLocations);
    }

    assertThat(regionMetaData.containsKey(customerRegion2.getFullPath())).isTrue();
    prMetaData = regionMetaData.get(customerRegion2.getFullPath());
    for (Entry<?, ?> e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      logger.debug("For bucket id " + e.getKey() + " the locations are " + e.getValue());
    }

    for (Entry<?, ?> entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertThat(((List<?>) entry.getValue()).size()).isEqualTo(numBucketLocations2);
    }
  }

  private static <K, V> void createColocatedRegion(Cache cache, String regionName,
      String colocatedRegionName, int redundantCopies, int totalNumberOfBuckets, int localMaxMemory,
      PartitionResolver<K, V> partitionResolver) {

    PartitionAttributesFactory<K, V> partitionAttributesFactory =
        new PartitionAttributesFactory<>();
    partitionAttributesFactory.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNumberOfBuckets);
    if (partitionResolver != null) {
      partitionAttributesFactory.setPartitionResolver(partitionResolver);
    }


    if (colocatedRegionName != null) {
      partitionAttributesFactory.setColocatedWith(colocatedRegionName);
    }

    if (localMaxMemory > -1) {
      partitionAttributesFactory.setLocalMaxMemory(localMaxMemory);
    }

    RegionFactory<K, V> regionFactory = cache.createRegionFactory();
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
    regionFactory.setConcurrencyChecksEnabled(true);
    Region<K, V> region = regionFactory.create(regionName);
    assertThat(region).isNotNull();
    logger.info("Partitioned Region " + regionName + " created Successfully :" + region);
  }


  private static void setupMultiRegions() {
    Cache cache = CacheFactory.getAnyInstance();
    final int redundantCopies = 2;
    final int localMaxMemory = 100;
    final int totalNumberOfBuckets = 8;
    createBasicPartitionedRegion(cache, redundantCopies, totalNumberOfBuckets, localMaxMemory,
        TEST_REGION);

    // creating colocated Regions
    createColocatedRegion(cache, CUSTOMER, null, redundantCopies, localMaxMemory,
        totalNumberOfBuckets, new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));

    createColocatedRegion(cache, ORDER, null, redundantCopies, localMaxMemory,
        totalNumberOfBuckets, new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));

    createColocatedRegion(cache, SHIPMENT, null, redundantCopies, localMaxMemory,
        totalNumberOfBuckets, new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));

    createBasicPartitionedRegion(cache, redundantCopies, totalNumberOfBuckets, localMaxMemory,
        TEST_REGION2);

    // creating colocated Regions
    createColocatedRegion(cache, CUSTOMER2, null, redundantCopies, localMaxMemory,
        totalNumberOfBuckets, new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));

    createColocatedRegion(cache, ORDER2, null, redundantCopies, localMaxMemory,
        totalNumberOfBuckets, new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));

    createColocatedRegion(cache, SHIPMENT2, null, redundantCopies, localMaxMemory,
        totalNumberOfBuckets, new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));
  }

  private static <K, V> void createBasicPartitionedRegion(Cache cache, int redundantCopies,
      int totalNumberOfBuckets,
      int localMaxMemory,
      final String regionName) {
    PartitionAttributesFactory<K, V> partitionAttributesFactory =
        new PartitionAttributesFactory<>();

    partitionAttributesFactory.setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNumberOfBuckets)
        .setLocalMaxMemory(localMaxMemory);
    RegionFactory<K, V> regionFactory = cache.createRegionFactory();

    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    Region<K, V> region = regionFactory.create(regionName);
    assertThat(region).isNotNull();
    logger.info("Partitioned Region " + regionName + " created Successfully :" + region);
  }

  private static void createClientWithLocator(String host, int port0, String group) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(LOG_FILE, "");
    CacheFactory cacheFactory = new CacheFactory(props);
    Cache cache = cacheFactory.create();
    assertThat(cache).isNotNull();
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool pool;
    try {
      pool = PoolManager.createFactory().addLocator(host, port0).setServerGroup(group)
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(TEST_REGION);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    createRegionsInClientCache(pool.getName());
  }

  private static void create2ClientWithLocator(String host, int port0, String group1,
      String group2) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    CacheFactory cacheFactory = new CacheFactory(props);
    Cache cache = cacheFactory.create();
    assertThat(cache).isNotNull();
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p1, p2;
    try {
      p1 = PoolManager.createFactory().addLocator(host, port0).setServerGroup(group1)
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(TEST_REGION);
      p2 = PoolManager.createFactory().addLocator(host, port0).setServerGroup(group2)
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(TEST_REGION2);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    create2RegionsInClientCache(p1.getName(), p2.getName());
  }

  private static void createClientWith3PoolLocator(String host, int port0) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    CacheFactory cacheFactory = new CacheFactory(props);
    Cache cache = cacheFactory.create();
    assertThat(cache).isNotNull();
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p1, p2, p3;
    try {
      p1 = PoolManager.createFactory().addLocator(host, port0).setServerGroup("group2")
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(TEST_REGION);
      p2 = PoolManager.createFactory().addLocator(host, port0).setServerGroup("group1")
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(TEST_REGION2);
      p3 = PoolManager.createFactory().addLocator(host, port0).setServerGroup("")
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(PR_NAME3);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    createColocatedRegionsInClientCacheWithDiffPool(p1.getName(), p2.getName(), p3.getName());
  }

  private static <K, V> void createColocatedDistributedRegion(String poolName,
      final String regionName) {
    Cache cache = CacheFactory.getAnyInstance();
    RegionFactory<K, V> factory = cache.createRegionFactory();
    factory.setPoolName(poolName);
    Region<K, V> region = factory.create(regionName);
    assertThat(region).isNotNull();
    logger.info("Distributed Region " + regionName + " created Successfully :" + region);
  }

  private static void createRegionsInClientCache(String poolName) {
    createBaseDistributedRegion(poolName, TEST_REGION);
    createColocatedDistributedRegion(poolName, CUSTOMER);
    createColocatedDistributedRegion(poolName, ORDER);
    createColocatedDistributedRegion(poolName, SHIPMENT);

  }


  private static void create2RegionsInClientCache(String poolName1, String poolName2) {
    createRegionsInClientCache(poolName1);
    createBaseDistributedRegion(poolName2, TEST_REGION2);
    createColocatedDistributedRegion(poolName2, CUSTOMER2);
    createColocatedDistributedRegion(poolName2, ORDER2);
    createColocatedDistributedRegion(poolName2, SHIPMENT2);

  }

  private static void createColocatedRegionsInClientCacheWithDiffPool(String poolName1,
      String poolName2, String poolName3) {
    createBaseDistributedRegion(poolName1, TEST_REGION);
    createColocatedDistributedRegion(poolName1, CUSTOMER);
    createColocatedDistributedRegion(poolName2, ORDER);
    createColocatedDistributedRegion(poolName3, SHIPMENT);

  }

  private static <K, V> void createBaseDistributedRegion(String poolName1,
      final String regionName) {
    Cache cache = CacheFactory.getAnyInstance();
    RegionFactory<K, V> factory = cache.createRegionFactory();
    factory.setPoolName(poolName1);
    factory.setDataPolicy(DataPolicy.EMPTY);
    Region<K, V> tmpRegion = factory.create(regionName);
    assertThat(tmpRegion).isNotNull();
    logger.info(
        "Distributed Region " + regionName + " created Successfully :" + tmpRegion);
  }

  private static void putIntoPartitionedRegions() {
    Region<Object, Object> testRegion = getRegion(TEST_REGION);

    populateRegions();

    String[] dataSet1 = new String[] {"create0", "create1", "create2", "create3", "create0",
        "create1", "create2", "create3"};
    insertData(testRegion, dataSet1);

    String[] dataSet2 = new String[] {"update0", "update1", "update2", "update3", "update0",
        "update1", "update2", "update3"};
    insertData(testRegion, dataSet2);

    String[] dataSet3 = new String[] {"update00", "update11", "update22", "update33", "update00",
        "update11", "update22", "update33"};
    insertData(testRegion, dataSet3);
  }

  private static void populateRegions() {
    Region<Object, Object> customerRegion = getRegion(CUSTOMER);
    Region<Object, Object> orderRegion = getRegion(ORDER);
    Region<Object, Object> shipmentRegion = getRegion(SHIPMENT);

    for (int k = 0; k <= 800; k++) {
      CustId custid = new CustId(k);
      OrderId orderId = new OrderId(k, custid);
      ShipmentId shipmentId = new ShipmentId(k, orderId);
      Customer customer = new Customer("name" + k, "Address" + k);
      customerRegion.put(custid, customer);

      Order order = new Order(ORDER + k);
      orderRegion.put(orderId, order);

      Shipment shipment = new Shipment("Shipment" + k);
      shipmentRegion.put(shipmentId, shipment);
    }
  }

  private static void getDataFromRegions() {

    Region<Object, Object> customerRegion = getRegion(CUSTOMER);
    Region<Object, Object> orderRegion = getRegion(ORDER);
    Region<Object, Object> shipmentRegion = getRegion(SHIPMENT);
    for (int k = 0; k <= 800; k++) {
      CustId custid = new CustId(k);
      OrderId orderId = new OrderId(k, custid);
      ShipmentId shipmentId = new ShipmentId(k, orderId);
      customerRegion.get(custid);
      orderRegion.get(orderId);
      shipmentRegion.get(shipmentId);
    }
  }

  private static void putIntoPartitionedRegions2Client() {
    Region<Object, Object> customerRegion = getRegion(CUSTOMER);
    Region<Object, Object> customerRegion2 = getRegion(CUSTOMER2);
    Region<Object, Object> orderRegion = getRegion(ORDER);
    Region<Object, Object> orderRegion2 = getRegion(ORDER2);
    Region<Object, Object> shipmentRegion = getRegion(SHIPMENT);
    Region<Object, Object> shipmentRegion2 = getRegion(SHIPMENT2);
    Region<Object, Object> testRegion = getRegion(TEST_REGION);
    Region<Object, Object> testRegion2 = getRegion(TEST_REGION2);

    for (int k = 0; k <= 800; k++) {
      CustId custid = new CustId(k);
      OrderId orderId = new OrderId(k, custid);
      ShipmentId shipmentId = new ShipmentId(k, orderId);
      Customer customer = new Customer("name" + k, "Address" + k);
      customerRegion.put(custid, customer);
      customerRegion2.put(custid, customer);

      Order order = new Order(ORDER + k);
      orderRegion.put(orderId, order);
      orderRegion2.put(orderId, order);

      Shipment shipment = new Shipment("Shipment" + k);
      shipmentRegion.put(shipmentId, shipment);
      shipmentRegion2.put(shipmentId, shipment);
    }

    String[] dataSet1 = new String[] {"create0", "create1", "create2", "create3", "create0",
        "create1", "create2", "create3"};
    insertData(testRegion, dataSet1);

    String[] dataSet2 = new String[] {"update0", "update1", "update2", "update3", "update0",
        "update1", "update2", "update3"};
    insertData(testRegion, dataSet2);

    String[] dataSet3 = new String[] {"update00", "update11", "update22", "update33", "update00",
        "update11", "update22", "update33"};
    insertData(testRegion, dataSet3);

    insertData(testRegion2, dataSet1);
    insertData(testRegion2, dataSet2);
    insertData(testRegion2, dataSet3);
  }

  private static void insertData(Region<Object, Object> testRegion, String[] values) {
    for (int i = 0; i < values.length; i++) {
      testRegion.put(i, values[i]);
    }
  }

  private static void getFromPartitionedRegions() {
    Region<Object, Object> testRegion = getRegion(TEST_REGION);

    getDataFromRegions();

    testRegion.get(0);
    testRegion.get(1);
    testRegion.get(2);
    testRegion.get(3);
    testRegion.get(4);
    testRegion.get(5);
    testRegion.get(6);
    testRegion.get(7);
  }

  private static void getFromPartitionedRegions2Client() {
    Region<Object, Object> customerRegion = getRegion(CUSTOMER);
    Region<Object, Object> customerRegion2 = getRegion(CUSTOMER2);
    Region<Object, Object> orderRegion = getRegion(ORDER);
    Region<Object, Object> orderRegion2 = getRegion(ORDER2);
    Region<Object, Object> shipmentRegion = getRegion(SHIPMENT);
    Region<Object, Object> shipmentRegion2 = getRegion(SHIPMENT2);
    Region<Object, Object> testRegion = getRegion(TEST_REGION);
    Region<Object, Object> testRegion2 = getRegion(TEST_REGION2);

    for (int i = 0; i <= 800; i++) {
      CustId custid = new CustId(i);
      OrderId orderId = new OrderId(i, custid);
      ShipmentId shipmentId = new ShipmentId(i, orderId);
      customerRegion.get(custid);
      customerRegion2.get(custid);
      orderRegion.get(orderId);
      orderRegion2.get(orderId);
      shipmentRegion.get(shipmentId);
      shipmentRegion2.get(shipmentId);
    }

    for (int i = 0; i < 8; i++) {
      testRegion.get(i);
      testRegion2.get(i);
    }
  }

  private static void stopLocator(MemberVM member) {
    InternalLocator locator = ((Locator) member.getMember()).getLocator();
    if (locator != null) {
      locator.stop();
    }
  }

  private static void resetHonourServerGroupsInPRSingleHop() {
    System.setProperty(
        GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "False");
  }

  private static void setHonourServerGroupsInPRSingleHop() {
    System.setProperty(
        GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "True");
  }

  private static class Customer implements DataSerializable {
    private String name;
    private String address;

    public Customer() {
      // nothing
    }

    public Customer(String name, String address) {
      this.name = name;
      this.address = address;
    }

    @Override
    public void fromData(DataInput in) throws IOException {
      name = DataSerializer.readString(in);
      address = DataSerializer.readString(in);

    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(name, out);
      DataSerializer.writeString(address, out);
    }

    @Override
    public String toString() {
      return "Customer { name=" + name + " address=" + address + "}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof Customer)) {
        return false;
      }

      Customer customer = (Customer) o;
      return customer.name.equals(name) && customer.address.equals(address);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, address);
    }
  }

  private static class Order implements DataSerializable {
    private String orderName;

    public Order() {
      // nothing
    }

    private Order(String orderName) {
      this.orderName = orderName;
    }

    @Override
    public void fromData(DataInput in) throws IOException {
      orderName = DataSerializer.readString(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(orderName, out);
    }

    @Override
    public String toString() {
      return orderName;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (obj instanceof Order) {
        Order other = (Order) obj;
        return other.orderName != null && other.orderName.equals(orderName);
      }
      return false;
    }

    @Override
    public int hashCode() {
      if (orderName == null) {
        return super.hashCode();
      }
      return orderName.hashCode();
    }
  }

  private static class Shipment implements DataSerializable {
    private String shipmentName;

    public Shipment() {
      // nothing
    }

    private Shipment(String shipmentName) {
      this.shipmentName = shipmentName;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      shipmentName = DataSerializer.readString(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(shipmentName, out);
    }

    @Override
    public String toString() {
      return shipmentName;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (obj instanceof Shipment) {
        Shipment other = (Shipment) obj;
        return other.shipmentName != null && other.shipmentName.equals(shipmentName);
      }
      return false;
    }

    @Override
    public int hashCode() {
      if (shipmentName == null) {
        return super.hashCode();
      }
      return shipmentName.hashCode();
    }
  }

  private static Region<Object, Object> getRegion(String regionName) {
    return CacheFactory.getAnyInstance().getRegion(regionName);
  }
}
