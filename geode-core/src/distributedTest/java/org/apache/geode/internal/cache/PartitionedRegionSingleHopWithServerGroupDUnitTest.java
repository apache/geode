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
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.client.internal.ClientPartitionAdvisor;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.execute.data.ShipmentId;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class PartitionedRegionSingleHopWithServerGroupDUnitTest {
  private static final Logger logger = LogService.getLogger();
  private static final String PR_NAME = "single_hop_pr";
  private static final String PR_NAME2 = "single_hop_pr_2";
  private static final String PR_NAME3 = "single_hop_pr_3";
  public static final String CUSTOMER = "CUSTOMER";
  public static final String ORDER = "ORDER";
  public static final String SHIPMENT = "SHIPMENT";
  protected static InternalDistributedSystem system;

  private static final String CUSTOMER2 = "CUSTOMER2";
  private static final String ORDER2 = "ORDER2";
  private static final String SHIPMENT2 = "SHIPMENT2";


  private VM member0 = null;
  private VM member1 = null;
  private VM member2 = null;
  private VM member3 = null;

  private static Region<Object, Object> region = null;
  private static Region<Object, Object> customerRegion = null;
  private static Region<Object, Object> orderRegion = null;
  private static Region<Object, Object> shipmentRegion = null;
  private static Region<Object, Object> region2 = null;
  private static Region<Object, Object> customerRegion2 = null;
  private static Region<Object, Object> orderRegion2 = null;
  private static Region<Object, Object> shipmentRegion2 = null;
  private static Cache cache = null;
  private static Locator locator = null;

  @Rule
  public final ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Before
  public final void postSetUp() {

    member0 = VM.getVM(0);
    member1 = VM.getVM(1);
    member2 = VM.getVM(2);
    member3 = VM.getVM(3);
    IgnoredException.addIgnoredException("java.net.SocketException");
  }


  public final void preTearDownCacheTestCase() {
    // close the clients first
    member0
        .invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::closeCacheAndDisconnect);
    member1
        .invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::closeCacheAndDisconnect);
    member2
        .invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::closeCacheAndDisconnect);
    member3
        .invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::closeCacheAndDisconnect);
    closeCacheAndDisconnect();
  }

  @After
  public final void postTearDownCacheTestCase() {
    preTearDownCacheTestCase();
    try {
      member0 = null;
      member1 = null;
      member2 = null;
      member3 = null;
      Invoke.invokeInEveryVM(() -> {
        cache = null;
        orderRegion = null;
        orderRegion2 = null;
        customerRegion = null;
        customerRegion2 = null;
        shipmentRegion = null;
        shipmentRegion2 = null;
        region = null;
        region2 = null;
        locator = null;
      });

    } finally {
      DistributedTestUtils.unregisterAllDataSerializersFromAllVms();
    }
  }

  private static void closeCacheAndDisconnect() {
    resetHonourServerGroupsInPRSingleHop();
    if (cache != null && !cache.isClosed()) {
      cache.close();
    }
  }


  @Test
  public void test_SingleHopWith2ServerGroup() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName();
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group1"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group2"));
      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group2"));

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, port3, "group1");

      // put
      putIntoPartitionedRegions();

      getFromPartitionedRegions();

      try {
        verifyMetadata(4, 1);
      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::stopLocator);
    }
  }

  @Test
  public void test_SingleHopWith2ServerGroup2() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName();
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group1"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group1"));
      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group2"));

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, port3, "group1");

      // put
      putIntoPartitionedRegions();

      getFromPartitionedRegions();

      try {
        verifyMetadata(4, 2);
      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::stopLocator);
    }
  }

  @Test
  public void test_SingleHopWith2ServerGroup2WithoutSystemProperty() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName();
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group1"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group1"));
      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group2"));

      createClientWithLocator(host0, port3, "group1");

      // put
      putIntoPartitionedRegions();

      getFromPartitionedRegions();

      verifyMetadata(4, 3);
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::stopLocator);
    }
  }

  @Test
  public void test_SingleHopWithServerGroupAccessor() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName();
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 0, "group1"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group2"));
      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group2"));

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, port3, "group1");

      putIntoPartitionedRegions();

      getFromPartitionedRegions();

      try {
        verifyMetadata(0, 0);
      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::stopLocator);
    }
  }

  @Test
  public void test_SingleHopWithServerGroupOneServerInTwoGroups() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName();
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group1"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group1"));
      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group1,group2"));

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, port3, "group1");

      putIntoPartitionedRegions();

      getFromPartitionedRegions();

      try {
        verifyMetadata(4, 3);
      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::stopLocator);
    }
  }

  @Test
  public void test_SingleHopWithServerGroupWithOneDefaultServer() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName();
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group1"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group2"));
      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, ""));

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, port3, "group1");

      putIntoPartitionedRegions();

      getFromPartitionedRegions();

      try {
        verifyMetadata(4, 2);
      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::stopLocator);
    }
  }

  @Test
  public void test_SingleHopWithServerGroupClientServerGroupNull() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName();
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group1"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group2"));
      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group3"));

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, port3, "");

      putIntoPartitionedRegions();

      getFromPartitionedRegions();

      try {
        verifyMetadata(4, 3);
      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::stopLocator);
    }
  }

  @Test
  public void test_SingleHopWithServerGroupTwoClientServerGroup() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName();
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group1"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group2"));

      member2.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::setHonourServerGroupsInPRSingleHop);

      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createClientWithLocator(host0, port3, "group1"));

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, port3, "");

      member2.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::putIntoPartitionedRegions);

      putIntoPartitionedRegions();

      getFromPartitionedRegions();
      member2.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::getFromPartitionedRegions);

      try {
        verifyMetadata(4, 2);
        member2
            .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.verifyMetadata(4, 1));

      } finally {
        resetHonourServerGroupsInPRSingleHop();
        member2.invoke(
            PartitionedRegionSingleHopWithServerGroupDUnitTest::resetHonourServerGroupsInPRSingleHop);
      }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::stopLocator);
    }
  }

  @Test
  public void test_SingleHopWithServerGroupTwoClientServerGroup2() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName();
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group1,group2"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group2"));

      member2.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::setHonourServerGroupsInPRSingleHop);

      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createClientWithLocator(host0, port3, "group1"));

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, port3, "group2");
      member2.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::putIntoPartitionedRegions);
      putIntoPartitionedRegions();
      member2.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::getFromPartitionedRegions);
      getFromPartitionedRegions();

      try {
        verifyMetadata(4, 2);
        member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
            .verifyMetadata(4, 1));

      } finally {
        resetHonourServerGroupsInPRSingleHop();
        member2.invoke(
            PartitionedRegionSingleHopWithServerGroupDUnitTest::resetHonourServerGroupsInPRSingleHop);
      }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::stopLocator);
    }
  }

  @Test
  public void test_SingleHopWithServerGroupTwoClientOneWithOneWithoutServerGroup() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName();
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group1,group2"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group2"));

      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createClientWithLocator(host0, port3, "group1"));

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, port3, "group2");
      member2.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::putIntoPartitionedRegions);
      putIntoPartitionedRegions();
      member2.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::getFromPartitionedRegions);
      getFromPartitionedRegions();

      try {
        verifyMetadata(4, 2);
        member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
            .verifyMetadata(4, 2));

      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::stopLocator);
    }
  }

  @Test
  public void test_SingleHopWithServerGroup2ClientInOneVMServerGroup() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName();
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup2Regions(locator, "group1,group2"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup2Regions(locator, "group2"));

      member2.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::setHonourServerGroupsInPRSingleHop);

      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .create2ClientWithLocator(host0, port3, "group1", ""));

      setHonourServerGroupsInPRSingleHop();
      create2ClientWithLocator(host0, port3, "group2", "group1");
      member2.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::putIntoPartitionedRegions2Client);
      putIntoPartitionedRegions2Client();
      member2.invoke(
          PartitionedRegionSingleHopWithServerGroupDUnitTest::getFromPartitionedRegions2Client);
      getFromPartitionedRegions2Client();

      try {
        verifyMetadataFor2ClientsInOneVM(2, 1);
        member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
            .verifyMetadataFor2ClientsInOneVM(1, 2));

      } finally {
        resetHonourServerGroupsInPRSingleHop();
        member2.invoke(
            PartitionedRegionSingleHopWithServerGroupDUnitTest::resetHonourServerGroupsInPRSingleHop);
      }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::stopLocator);
    }
  }

  @Test
  public void test_SingleHopWithServerGroupColocatedRegionsInDifferentGroup() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName();
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group1,group2"));

      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group2"));

      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, "group3"));

      setHonourServerGroupsInPRSingleHop();
      createClientWith3PoolLocator(host0, port3);
      putIntoPartitionedRegions();
      getFromPartitionedRegions();

      try {
        verifyMetadataForColocatedRegionWithDiffPool();
      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(PartitionedRegionSingleHopWithServerGroupDUnitTest::stopLocator);
    }
  }

  private static void verifyMetadata(final int numRegions, final int numBucketLocations) {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();


    await().alias("expected metadata for each region to be" + numRegions + " but it is "
        + regionMetaData.size() + "Metadata is " + regionMetaData.keySet())
        .until(() -> regionMetaData.size() == numRegions);

    if (numRegions != 0) {
      assertThat(regionMetaData.containsKey(region.getFullPath())).isTrue();
      ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
      if (logger.getLevel() == Level.DEBUG) {
        for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
          logger.debug("For bucket id " + e.getKey() + " the locations are " + e.getValue());
        }
      }

      for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
        assertThat(((List) entry.getValue()).size()).isEqualTo(numBucketLocations);
      }
    }
  }

  private static void verifyMetadataForColocatedRegionWithDiffPool() {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();


    await().alias("expected metadata for each region to be " + 4 + " but it is "
        + regionMetaData.size() + " they are " + regionMetaData.keySet())
        .until(() -> regionMetaData.size() == 4);

    assertThat(regionMetaData.containsKey(region.getFullPath())).isTrue();
    ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
    for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      logger.debug("For bucket id " + e.getKey() + " the locations are " + e.getValue());
    }

    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertThat(((List) entry.getValue()).size()).isEqualTo(2);
    }

    assertThat(regionMetaData.containsKey(customerRegion.getFullPath())).isTrue();
    prMetaData = regionMetaData.get(customerRegion.getFullPath());
    for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      logger.debug("For bucket id " + e.getKey() + " the locations are " + e.getValue());
    }

    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertThat(((List) entry.getValue()).size()).isEqualTo(2);
    }

    assertThat(regionMetaData.containsKey(orderRegion.getFullPath())).isTrue();
    prMetaData = regionMetaData.get(orderRegion.getFullPath());
    for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      logger.debug("For bucket id " + e.getKey() + " the locations are " + e.getValue());
    }

    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertThat(((List) entry.getValue()).size()).isEqualTo(1);
    }

    assertThat(regionMetaData.containsKey(shipmentRegion.getFullPath())).isTrue();
    prMetaData = regionMetaData.get(shipmentRegion.getFullPath());
    for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      logger.debug("For bucket id " + e.getKey() + " the locations are " + e.getValue());
    }

    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertThat(((List) entry.getValue()).size()).isEqualTo(3);
    }

  }

  private static void verifyMetadataFor2ClientsInOneVM(final int numBucketLocations,
      final int numBucketLocations2) {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();

    await().alias("expected metadata for each region to be " + 8 + " but it is "
        + regionMetaData.size() + " they are " + regionMetaData.keySet())
        .until(() -> regionMetaData.size() == 8);

    if (8 != 0) {
      assertThat(regionMetaData.containsKey(region.getFullPath())).isTrue();
      ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
      for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
        logger.debug("For bucket id " + e.getKey() + " the locations are " + e.getValue());
      }

      for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
        assertThat(((List) entry.getValue()).size()).isEqualTo(numBucketLocations);
      }

      assertThat(regionMetaData.containsKey(customerRegion.getFullPath())).isTrue();
      prMetaData = regionMetaData.get(customerRegion.getFullPath());
      for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
        logger.debug("For bucket id " + e.getKey() + " the locations are " + e.getValue());
      }

      for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
        assertThat(((List) entry.getValue()).size()).isEqualTo(numBucketLocations);
      }

      assertThat(regionMetaData.containsKey(customerRegion2.getFullPath())).isTrue();
      prMetaData = regionMetaData.get(customerRegion2.getFullPath());
      for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
        logger.debug("For bucket id " + e.getKey() + " the locations are " + e.getValue());
      }

      for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
        assertThat(((List) entry.getValue()).size()).isEqualTo(numBucketLocations2);
      }
    }
  }


  private static int createServerWithLocatorAndServerGroup(String locator, int localMaxMemory,
      String group) {

    Properties props = new Properties();
    props.setProperty(LOCATORS, locator);

    System.setProperty(
        DistributionConfig.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true");
    CacheFactory cacheFactory = new CacheFactory(props);
    cache = cacheFactory.create();

    CacheServer server = cache.addCacheServer();
    if (group.length() != 0) {
      StringTokenizer t = new StringTokenizer(group, ",");
      String[] a = new String[t.countTokens()];
      int i = 0;
      while (t.hasMoreTokens()) {
        a[i] = t.nextToken();
        i++;
      }
      server.setGroups(a);
    }
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      fail("Failed to start server ", e);
    }

    PartitionAttributesFactory<Object, Object> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(2).setLocalMaxMemory(localMaxMemory).setTotalNumBuckets(8);
    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    regionFactory.setPartitionAttributes(paf.create());
    PartitionedRegionSingleHopWithServerGroupDUnitTest.region = regionFactory.create(PR_NAME);
    assertThat(PartitionedRegionSingleHopWithServerGroupDUnitTest.region).isNotNull();
    logger.info("Partitioned Region " + PR_NAME + " created Successfully :"
        + PartitionedRegionSingleHopWithServerGroupDUnitTest.region
            .toString());

    // creating colocated Regions
    paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(2).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(8)
        .setPartitionResolver(new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));
    regionFactory = cache.createRegionFactory();
    regionFactory.setPartitionAttributes(paf.create());
    customerRegion = regionFactory.create(CUSTOMER);
    assertThat(customerRegion).isNotNull();
    logger.info("Partitioned Region CUSTOMER created Successfully :" + customerRegion.toString());

    paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(2).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(8).setColocatedWith(CUSTOMER)
        .setPartitionResolver(new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));
    regionFactory = cache.createRegionFactory();
    regionFactory.setPartitionAttributes(paf.create());
    orderRegion = regionFactory.create(ORDER);
    assertThat(orderRegion).isNotNull();
    logger.info("Partitioned Region ORDER created Successfully :" + orderRegion.toString());


    paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(2).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(8).setColocatedWith(ORDER)
        .setPartitionResolver(new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));
    regionFactory = cache.createRegionFactory();
    regionFactory.setPartitionAttributes(paf.create());
    shipmentRegion = regionFactory.create(SHIPMENT);
    assertThat(shipmentRegion).isNotNull();
    logger.info("Partitioned Region SHIPMENT created Successfully :" + shipmentRegion.toString());
    return port;
  }

  private static int createServerWithLocatorAndServerGroup2Regions(String locator,
      String group) {

    Properties props = new Properties();
    props.setProperty(LOCATORS, locator);

    System.setProperty(
        DistributionConfig.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true");
    CacheFactory cacheFactory = new CacheFactory(props);
    cache = cacheFactory.create();

    CacheServer server = cache.addCacheServer();
    if (group.length() != 0) {
      StringTokenizer t = new StringTokenizer(group, ",");
      String[] a = new String[t.countTokens()];
      int i = 0;
      while (t.hasMoreTokens()) {
        a[i] = t.nextToken();
        i++;
      }
      server.setGroups(a);
    }
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      fail("Failed to start server ", e);
    }

    PartitionAttributesFactory<Object, Object> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(2).setLocalMaxMemory(100).setTotalNumBuckets(8);
    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    regionFactory.setPartitionAttributes(paf.create());
    region = regionFactory.create(PR_NAME);
    assertThat(region).isNotNull();
    logger.info("Partitioned Region " + PR_NAME + " created Successfully :" + region.toString());

    // creating colocated Regions
    customerRegion = CreateColocatedRegion(CUSTOMER);

    orderRegion = CreateColocatedRegion(ORDER);

    shipmentRegion = CreateColocatedRegion(SHIPMENT);


    paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(2).setLocalMaxMemory(100).setTotalNumBuckets(8);
    regionFactory = cache.createRegionFactory();
    regionFactory.setPartitionAttributes(paf.create());
    region2 = regionFactory.create(PR_NAME2);
    assertThat(region2).isNotNull();
    logger.info("Partitioned Region " + PR_NAME2 + " created Successfully :" + region2.toString());

    orderRegion2 = CreateColocatedRegion(CUSTOMER2);

    customerRegion2 = CreateColocatedRegion(ORDER2);

    shipmentRegion2 = CreateColocatedRegion(SHIPMENT2);



    return port;
  }

  private static <K, V> Region<K, V> CreateColocatedRegion(String regionName) {

    PartitionAttributesFactory<K, V> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(2).setLocalMaxMemory(100).setTotalNumBuckets(8)
        .setPartitionResolver(new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));

    RegionFactory<K, V> regionFactory = cache.createRegionFactory();
    regionFactory.setPartitionAttributes(paf.create());

    Region<K, V> region = regionFactory.create(regionName);
    assertThat(region).isNotNull();
    logger.info("Partitioned Region " + regionName + " created Successfully :" + region.toString());
    return region;
  }

  private static void createClientWithLocator(String host, int port0, String group) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(LOG_FILE, "");
    CacheFactory cacheFactory = new CacheFactory(props);
    cache = cacheFactory.create();
    assertThat(cache).isNotNull();
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addLocator(host, port0).setServerGroup(group)
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(PR_NAME);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    createRegionsInClientCache(p.getName());
  }

  private static void create2ClientWithLocator(String host, int port0, String group1,
      String group2) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    CacheFactory cacheFactory = new CacheFactory(props);
    cache = cacheFactory.create();
    assertThat(cache).isNotNull();
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p1, p2;
    try {
      p1 = PoolManager.createFactory().addLocator(host, port0).setServerGroup(group1)
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(PR_NAME);
      p2 = PoolManager.createFactory().addLocator(host, port0).setServerGroup(group2)
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(PR_NAME2);
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
    cache = cacheFactory.create();
    assertThat(cache).isNotNull();
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p1, p2, p3;
    try {
      p1 = PoolManager.createFactory().addLocator(host, port0).setServerGroup("group2")
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(PR_NAME);
      p2 = PoolManager.createFactory().addLocator(host, port0).setServerGroup("group1")
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(PR_NAME2);
      p3 = PoolManager.createFactory().addLocator(host, port0).setServerGroup("")
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(PR_NAME3);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    createColocatedRegionsInClientCacheWithDiffPool(p1.getName(), p2.getName(), p3.getName());
  }

  private static void createRegionsInClientCache(String poolName) {
    RegionFactory<Object, Object> factory = cache.createRegionFactory();
    factory.setPoolName(poolName);
    factory.setDataPolicy(DataPolicy.EMPTY);
    region = factory.create(PR_NAME);
    assertThat(region).isNotNull();
    logger.info("Distributed Region " + PR_NAME + " created Successfully :" + region.toString());

    customerRegion = CreateColocatedDistributedRegion(poolName, CUSTOMER);

    orderRegion = CreateColocatedDistributedRegion(poolName, ORDER);

    shipmentRegion = CreateColocatedDistributedRegion(poolName, SHIPMENT);

  }

  private static <K, V> Region<K, V> CreateColocatedDistributedRegion(String poolName,
      final String regionName) {
    RegionFactory<K, V> factory;
    factory = cache.createRegionFactory();
    factory.setPoolName(poolName);
    Region<K, V> region = factory.create(regionName);
    assertThat(region).isNotNull();
    logger.info("Distributed Region " + regionName + " created Successfully :" + region.toString());
    return region;
  }

  private static void create2RegionsInClientCache(String poolName1, String poolName2) {
    RegionFactory<Object, Object> factory = cache.createRegionFactory();
    factory.setPoolName(poolName1);
    factory.setDataPolicy(DataPolicy.EMPTY);
    region = factory.create(PR_NAME);
    assertThat(region).isNotNull();
    logger.info("Distributed Region " + PR_NAME + " created Successfully :" + region.toString());

    customerRegion = CreateColocatedDistributedRegion(poolName1, CUSTOMER);

    orderRegion = CreateColocatedDistributedRegion(poolName1, ORDER);

    shipmentRegion = CreateColocatedDistributedRegion(poolName1, SHIPMENT);


    factory = cache.createRegionFactory();
    factory.setPoolName(poolName2);
    factory.setDataPolicy(DataPolicy.EMPTY);
    region2 = factory.create(PR_NAME2);
    assertThat(region2).isNotNull();
    logger.info("Distributed Region " + PR_NAME2 + " created Successfully :" + region2.toString());


    customerRegion2 = CreateColocatedDistributedRegion(poolName2, CUSTOMER2);

    orderRegion2 = CreateColocatedDistributedRegion(poolName2, ORDER2);

    shipmentRegion2 = CreateColocatedDistributedRegion(poolName2, SHIPMENT2);

  }

  private static void createColocatedRegionsInClientCacheWithDiffPool(String poolName1,
      String poolName2, String poolName3) {
    RegionFactory<Object, Object> factory = cache.createRegionFactory();
    factory.setPoolName(poolName1);
    factory.setDataPolicy(DataPolicy.EMPTY);
    region = factory.create(PR_NAME);
    assertThat(region).isNotNull();
    logger.info("Distributed Region " + PR_NAME + " created Successfully :" + region.toString());

    customerRegion = CreateColocatedDistributedRegion(poolName1, CUSTOMER);

    orderRegion = CreateColocatedDistributedRegion(poolName2, ORDER);

    shipmentRegion = CreateColocatedDistributedRegion(poolName3, SHIPMENT);

  }

  private static void putIntoPartitionedRegions() {
    for (int i = 0; i <= 800; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      customerRegion.put(custid, customer);
    }
    for (int j = 0; j <= 800; j++) {
      CustId custid = new CustId(j);
      OrderId orderId = new OrderId(j, custid);
      Order order = new Order(ORDER + j);
      orderRegion.put(orderId, order);
    }
    for (int k = 0; k <= 800; k++) {
      CustId custid = new CustId(k);
      OrderId orderId = new OrderId(k, custid);
      ShipmentId shipmentId = new ShipmentId(k, orderId);
      Shipment shipment = new Shipment("Shipment" + k);
      shipmentRegion.put(shipmentId, shipment);
    }

    region.put(0, "create0");
    region.put(1, "create1");
    region.put(2, "create2");
    region.put(3, "create3");
    region.put(4, "create0");
    region.put(5, "create1");
    region.put(6, "create2");
    region.put(7, "create3");

    region.put(0, "update0");
    region.put(1, "update1");
    region.put(2, "update2");
    region.put(3, "update3");
    region.put(4, "update0");
    region.put(5, "update1");
    region.put(6, "update2");
    region.put(7, "update3");

    region.put(0, "update00");
    region.put(1, "update11");
    region.put(2, "update22");
    region.put(3, "update33");
    region.put(4, "update00");
    region.put(5, "update11");
    region.put(6, "update22");
    region.put(7, "update33");
  }

  private static void putIntoPartitionedRegions2Client() {
    for (int i = 0; i <= 800; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      customerRegion.put(custid, customer);
      customerRegion2.put(custid, customer);
    }
    for (int j = 0; j <= 800; j++) {
      CustId custid = new CustId(j);
      OrderId orderId = new OrderId(j, custid);
      Order order = new Order(ORDER + j);
      orderRegion.put(orderId, order);
      orderRegion2.put(orderId, order);
    }
    for (int k = 0; k <= 800; k++) {
      CustId custid = new CustId(k);
      OrderId orderId = new OrderId(k, custid);
      ShipmentId shipmentId = new ShipmentId(k, orderId);
      Shipment shipment = new Shipment("Shipment" + k);
      shipmentRegion.put(shipmentId, shipment);
      shipmentRegion2.put(shipmentId, shipment);
    }

    region.put(0, "create0");
    region.put(1, "create1");
    region.put(2, "create2");
    region.put(3, "create3");
    region.put(4, "create0");
    region.put(5, "create1");
    region.put(6, "create2");
    region.put(7, "create3");

    region.put(0, "update0");
    region.put(1, "update1");
    region.put(2, "update2");
    region.put(3, "update3");
    region.put(4, "update0");
    region.put(5, "update1");
    region.put(6, "update2");
    region.put(7, "update3");

    region.put(0, "update00");
    region.put(1, "update11");
    region.put(2, "update22");
    region.put(3, "update33");
    region.put(4, "update00");
    region.put(5, "update11");
    region.put(6, "update22");
    region.put(7, "update33");

    region2.put(0, "create0");
    region2.put(1, "create1");
    region2.put(2, "create2");
    region2.put(3, "create3");
    region2.put(4, "create0");
    region2.put(5, "create1");
    region2.put(6, "create2");
    region2.put(7, "create3");

    region2.put(0, "update0");
    region2.put(1, "update1");
    region2.put(2, "update2");
    region2.put(3, "update3");
    region2.put(4, "update0");
    region2.put(5, "update1");
    region2.put(6, "update2");
    region2.put(7, "update3");

    region2.put(0, "update00");
    region2.put(1, "update11");
    region2.put(2, "update22");
    region2.put(3, "update33");
    region2.put(4, "update00");
    region2.put(5, "update11");
    region2.put(6, "update22");
    region2.put(7, "update33");
  }

  private static void getFromPartitionedRegions() {
    for (int i = 0; i <= 800; i++) {
      CustId custid = new CustId(i);
      customerRegion.get(custid);
    }
    for (int j = 0; j <= 800; j++) {
      CustId custid = new CustId(j);
      OrderId orderId = new OrderId(j, custid);
      orderRegion.get(orderId);
    }
    for (int k = 0; k <= 800; k++) {
      CustId custid = new CustId(k);
      OrderId orderId = new OrderId(k, custid);
      ShipmentId shipmentId = new ShipmentId(k, orderId);
      shipmentRegion.get(shipmentId);
    }

    region.get(0);
    region.get(1);
    region.get(2);
    region.get(3);
    region.get(4);
    region.get(5);
    region.get(6);
    region.get(7);
  }

  private static void getFromPartitionedRegions2Client() {

    for (int i = 0; i <= 800; i++) {
      CustId custid = new CustId(i);
      customerRegion.get(custid);
      customerRegion2.get(custid);
    }
    for (int j = 0; j <= 800; j++) {
      CustId custid = new CustId(j);
      OrderId orderId = new OrderId(j, custid);
      orderRegion.get(orderId);
      orderRegion2.get(orderId);
    }
    for (int k = 0; k <= 800; k++) {
      CustId custid = new CustId(k);
      OrderId orderId = new OrderId(k, custid);
      ShipmentId shipmentId = new ShipmentId(k, orderId);
      shipmentRegion.get(shipmentId);
      shipmentRegion2.get(shipmentId);
    }

    region.get(0);
    region.get(1);
    region.get(2);
    region.get(3);
    region.get(4);
    region.get(5);
    region.get(6);
    region.get(7);

    region2.get(0);
    region2.get(1);
    region2.get(2);
    region2.get(3);
    region2.get(4);
    region2.get(5);
    region2.get(6);
    region.get(7);
  }

  private static void startLocatorInVM(final int locatorPort) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    props.setProperty(LOG_FILE, "");

    try {
      locator = Locator.startLocatorAndDS(locatorPort, null, null, props);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static void stopLocator() {
    locator.stop();
  }

  private static void resetHonourServerGroupsInPRSingleHop() {
    System.setProperty(
        DistributionConfig.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "False");
  }

  private static void setHonourServerGroupsInPRSingleHop() {
    System.setProperty(
        DistributionConfig.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "True");
  }
}
