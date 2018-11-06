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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.StringTokenizer;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.client.internal.ClientPartitionAdvisor;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.execute.data.ShipmentId;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class PartitionedRegionSingleHopWithServerGroupDUnitTest extends JUnit4CacheTestCase {

  protected static final String PR_NAME = "single_hop_pr";
  protected static final String PR_NAME2 = "single_hop_pr_2";
  protected static final String PR_NAME3 = "single_hop_pr_3";
  private static final String CUSTOMER = "CUSTOMER";
  private static final String ORDER = "ORDER";
  private static final String SHIPMENT = "SHIPMENT";

  private static final String CUSTOMER2 = "CUSTOMER2";
  private static final String ORDER2 = "ORDER2";
  private static final String SHIPMENT2 = "SHIPMENT2";

  protected static final int locatorPort = 12345;

  protected VM member0 = null;
  protected VM member1 = null;
  protected VM member2 = null;
  protected VM member3 = null;

  protected static Region region = null;
  protected static Region customerRegion = null;
  protected static Region orderRegion = null;
  protected static Region shipmentRegion = null;
  protected static Region region2 = null;
  protected static Region customerRegion2 = null;
  protected static Region orderRegion2 = null;
  protected static Region shipmentRegion2 = null;
  protected static Cache cache = null;
  protected static Locator locator = null;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    member0 = host.getVM(0);
    member1 = host.getVM(1);
    member2 = host.getVM(2);
    member3 = host.getVM(3);
    IgnoredException.addIgnoredException("java.net.SocketException");
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    // close the clients first
    member0
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.closeCacheAndDisconnect());
    member1
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.closeCacheAndDisconnect());
    member2
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.closeCacheAndDisconnect());
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.closeCacheAndDisconnect());
    closeCacheAndDisconnect();
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    try {
      member0 = null;
      member1 = null;
      member2 = null;
      member3 = null;
      Invoke.invokeInEveryVM(new SerializableRunnable() {
        public void run() {
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
        }
      });

    } finally {
      DistributedTestUtils.unregisterAllDataSerializersFromAllVms();
    }
  }

  public static void closeCacheAndDisconnect() {
    resetHonourServerGroupsInPRSingleHop();
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public static void stopServer() {
    for (CacheServer cacheServer : cache.getCacheServers()) {
      cacheServer.stop();
    }
  }

  @Test
  public void test_SingleHopWith2ServerGroup() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group1"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group2"));
      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group2"));

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
      member3.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.stopLocator());
    }
  }

  @Test
  public void test_SingleHopWith2ServerGroup2() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group1"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group1"));
      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group2"));

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
      member3.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.stopLocator());
    }
  }

  @Test
  public void test_SingleHopWith2ServerGroup2WithoutSystemProperty() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group1"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group1"));
      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group2"));

      createClientWithLocator(host0, port3, "group1");

      // put
      putIntoPartitionedRegions();

      getFromPartitionedRegions();

      verifyMetadata(4, 3);
    } finally {
      member3.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.stopLocator());
    }
  }

  @Test
  public void test_SingleHopWithServerGroupAccessor() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 0, 2, 8, "group1"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group2"));
      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group2"));

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
      member3.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.stopLocator());
    }
  }

  @Test
  public void test_SingleHopWithServerGroupOneServerInTwoGroups() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group1"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group1"));
      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group1,group2"));

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
      member3.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.stopLocator());
    }
  }

  @Test
  public void test_SingleHopWithServerGroupWithOneDefaultServer() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group1"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group2"));
      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, ""));

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
      member3.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.stopLocator());
    }
  }

  @Test
  public void test_SingleHopWithServerGroupClientServerGroupNull() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group1"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group2"));
      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group3"));

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
      member3.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.stopLocator());
    }
  }

  @Test
  public void test_SingleHopWithServerGroupTwoClientServerGroup() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group1"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group2"));

      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .setHonourServerGroupsInPRSingleHop());

      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createClientWithLocator(host0, port3, "group1"));

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, port3, "");

      member2.invoke(
          () -> PartitionedRegionSingleHopWithServerGroupDUnitTest.putIntoPartitionedRegions());

      putIntoPartitionedRegions();

      getFromPartitionedRegions();
      member2.invoke(
          () -> PartitionedRegionSingleHopWithServerGroupDUnitTest.getFromPartitionedRegions());

      try {
        verifyMetadata(4, 2);
        member2
            .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.verifyMetadata(4, 1));

      } finally {
        resetHonourServerGroupsInPRSingleHop();
        member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
            .resetHonourServerGroupsInPRSingleHop());
      }
    } finally {
      member3.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.stopLocator());
    }
  }

  @Test
  public void test_SingleHopWithServerGroupTwoClientServerGroup2() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group1,group2"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group2"));

      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .setHonourServerGroupsInPRSingleHop());

      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createClientWithLocator(host0, port3, "group1"));

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, port3, "group2");
      member2.invoke(
          () -> PartitionedRegionSingleHopWithServerGroupDUnitTest.putIntoPartitionedRegions());
      putIntoPartitionedRegions();
      member2.invoke(
          () -> PartitionedRegionSingleHopWithServerGroupDUnitTest.getFromPartitionedRegions());
      getFromPartitionedRegions();

      try {
        verifyMetadata(4, 2);
        member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
            .verifyMetadata(new Integer(4), new Integer(1)));

      } finally {
        resetHonourServerGroupsInPRSingleHop();
        member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
            .resetHonourServerGroupsInPRSingleHop());
      }
    } finally {
      member3.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.stopLocator());
    }
  }

  @Test
  public void test_SingleHopWithServerGroupTwoClientOneWithOneWithoutServerGroup() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group1,group2"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group2"));

      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createClientWithLocator(host0, port3, "group1"));

      setHonourServerGroupsInPRSingleHop();
      createClientWithLocator(host0, port3, "group2");
      member2.invoke(
          () -> PartitionedRegionSingleHopWithServerGroupDUnitTest.putIntoPartitionedRegions());
      putIntoPartitionedRegions();
      member2.invoke(
          () -> PartitionedRegionSingleHopWithServerGroupDUnitTest.getFromPartitionedRegions());
      getFromPartitionedRegions();

      try {
        verifyMetadata(4, 2);
        member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
            .verifyMetadata(new Integer(4), new Integer(2)));

      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.stopLocator());
    }
  }

  @Test
  public void test_SingleHopWithServerGroup2ClientInOneVMServerGroup() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup2Regions(locator, 100, 2, 8, "group1,group2"));
      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup2Regions(locator, 100, 2, 8, "group2"));

      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .setHonourServerGroupsInPRSingleHop());

      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .create2ClientWithLocator(host0, port3, "group1", ""));

      setHonourServerGroupsInPRSingleHop();
      create2ClientWithLocator(host0, port3, "group2", "group1");
      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .putIntoPartitionedRegions2Client());
      putIntoPartitionedRegions2Client();
      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .getFromPartitionedRegions2Client());
      getFromPartitionedRegions2Client();

      try {
        verifyMetadataFor2ClientsInOneVM(8, 2, 1);
        member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
            .verifyMetadataFor2ClientsInOneVM(new Integer(8), new Integer(1), new Integer(2)));

      } finally {
        resetHonourServerGroupsInPRSingleHop();
        member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
            .resetHonourServerGroupsInPRSingleHop());
      }
    } finally {
      member3.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.stopLocator());
    }
  }

  @Test
  public void test_SingleHopWithServerGroupColocatedRegionsInDifferentGroup() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3
        .invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.startLocatorInVM(port3));
    try {

      member0.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group1,group2"));

      member1.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group2"));

      member2.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest
          .createServerWithLocatorAndServerGroup(locator, 100, 2, 8, "group3"));

      setHonourServerGroupsInPRSingleHop();
      createClientWith3PoolLocator(host0, port3, "group2", "group1", "");
      putIntoPartitionedRegions();
      getFromPartitionedRegions();

      try {
        verifyMetadataForColocatedRegionWithDiffPool(4, 2, 1, 3);
      } finally {
        resetHonourServerGroupsInPRSingleHop();
      }
    } finally {
      member3.invoke(() -> PartitionedRegionSingleHopWithServerGroupDUnitTest.stopLocator());
    }
  }

  public static void verifyMetadata(final int numRegions, final int numBucketLocations) {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (regionMetaData.size() == numRegions) {
          return true;
        }
        return false;
      }

      public String description() {
        return "expected metadata for each region to be" + numRegions + " but it is "
            + regionMetaData.size() + "Metadata is " + regionMetaData.keySet();
      }
    };

    GeodeAwaitility.await().untilAsserted(wc);

    if (numRegions != 0) {
      assertTrue(regionMetaData.containsKey(region.getFullPath()));
      ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
      if (cache.getLogger().fineEnabled()) {
        for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
          cache.getLogger()
              .fine("For bucket id " + e.getKey() + " the locations are " + e.getValue());
        }
      }

      for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
        assertEquals(numBucketLocations, ((List) entry.getValue()).size());
      }
    }
  }

  public static void verifyMetadataForColocatedRegionWithDiffPool(final int numRegions,
      final int numBucketLocations, final int numBucketLocations2, final int numBucketLocations3) {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (regionMetaData.size() == numRegions) {
          return true;
        }
        return false;
      }

      public String description() {
        return "expected metadata for each region to be " + numRegions + " but it is "
            + regionMetaData.size() + " they are " + regionMetaData.keySet();
      }
    };

    GeodeAwaitility.await().untilAsserted(wc);

    assertTrue(regionMetaData.containsKey(region.getFullPath()));
    ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
    for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      cache.getLogger().fine("For bucket id " + e.getKey() + " the locations are " + e.getValue());
    }

    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertEquals(numBucketLocations, ((List) entry.getValue()).size());
    }

    assertTrue(regionMetaData.containsKey(customerRegion.getFullPath()));
    prMetaData = regionMetaData.get(customerRegion.getFullPath());
    for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      cache.getLogger().fine("For bucket id " + e.getKey() + " the locations are " + e.getValue());
    }

    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertEquals(numBucketLocations, ((List) entry.getValue()).size());
    }

    assertTrue(regionMetaData.containsKey(orderRegion.getFullPath()));
    prMetaData = regionMetaData.get(orderRegion.getFullPath());
    for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      cache.getLogger().fine("For bucket id " + e.getKey() + " the locations are " + e.getValue());
    }

    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertEquals(numBucketLocations2, ((List) entry.getValue()).size());
    }

    assertTrue(regionMetaData.containsKey(shipmentRegion.getFullPath()));
    prMetaData = regionMetaData.get(shipmentRegion.getFullPath());
    for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      cache.getLogger().fine("For bucket id " + e.getKey() + " the locations are " + e.getValue());
    }

    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertEquals(numBucketLocations3, ((List) entry.getValue()).size());
    }

  }

  public static void verifyMetadataFor2ClientsInOneVM(final int numRegions,
      final int numBucketLocations, final int numBucketLocations2) {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (regionMetaData.size() == numRegions) {
          return true;
        }
        return false;
      }

      public String description() {
        return "expected metadata for each region to be " + numRegions + " but it is "
            + regionMetaData.size() + " they are " + regionMetaData.keySet();
      }
    };

    GeodeAwaitility.await().untilAsserted(wc);

    if (numRegions != 0) {
      assertTrue(regionMetaData.containsKey(region.getFullPath()));
      ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
      for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
        cache.getLogger()
            .fine("For bucket id " + e.getKey() + " the locations are " + e.getValue());
      }

      for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
        assertEquals(numBucketLocations, ((List) entry.getValue()).size());
      }

      assertTrue(regionMetaData.containsKey(customerRegion.getFullPath()));
      prMetaData = regionMetaData.get(customerRegion.getFullPath());
      for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
        cache.getLogger()
            .fine("For bucket id " + e.getKey() + " the locations are " + e.getValue());
      }

      for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
        assertEquals(numBucketLocations, ((List) entry.getValue()).size());
      }

      assertTrue(regionMetaData.containsKey(customerRegion2.getFullPath()));
      prMetaData = regionMetaData.get(customerRegion2.getFullPath());
      for (Entry e : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
        cache.getLogger()
            .fine("For bucket id " + e.getKey() + " the locations are " + e.getValue());
      }

      for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
        assertEquals(numBucketLocations2, ((List) entry.getValue()).size());
      }
    }
  }

  public static int createServer(int redundantCopies, int totalNoofBuckets, String group) {
    PartitionedRegionSingleHopWithServerGroupDUnitTest test =
        new PartitionedRegionSingleHopWithServerGroupDUnitTest();
    cache = test.getCache();
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    if (group.length() != 0)
      server.setGroups(new String[] {group});
    try {
      server.start();
    } catch (IOException e) {
      Assert.fail("Failed to start server ", e);
    }

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNoofBuckets);
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    region = cache.createRegion(PR_NAME, attr.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region " + PR_NAME + " created Successfully :" + region.toString());

    // creating colocated Regions
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNoofBuckets)
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    customerRegion = cache.createRegion("CUSTOMER", attr.create());
    assertNotNull(customerRegion);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region CUSTOMER created Successfully :" + customerRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNoofBuckets)
        .setColocatedWith("CUSTOMER")
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    orderRegion = cache.createRegion("ORDER", attr.create());
    assertNotNull(orderRegion);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region ORDER created Successfully :" + orderRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNoofBuckets)
        .setColocatedWith("ORDER")
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    shipmentRegion = cache.createRegion("SHIPMENT", attr.create());
    assertNotNull(shipmentRegion);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region SHIPMENT created Successfully :" + shipmentRegion.toString());
    return port;
  }

  public static int createServerWithLocatorAndServerGroup(String locator, int localMaxMemory,
      int redundantCopies, int totalNoofBuckets, String group) {

    Properties props = new Properties();
    props = new Properties();
    props.setProperty(LOCATORS, locator);

    System.setProperty(
        DistributionConfig.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true");
    PartitionedRegionSingleHopWithServerGroupDUnitTest test =
        new PartitionedRegionSingleHopWithServerGroupDUnitTest();

    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

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
      Assert.fail("Failed to start server ", e);
    }

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets);
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    region = cache.createRegion(PR_NAME, attr.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region " + PR_NAME + " created Successfully :" + region.toString());

    // creating colocated Regions
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets)
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    customerRegion = cache.createRegion("CUSTOMER", attr.create());
    assertNotNull(customerRegion);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region CUSTOMER created Successfully :" + customerRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets).setColocatedWith("CUSTOMER")
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    orderRegion = cache.createRegion("ORDER", attr.create());
    assertNotNull(orderRegion);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region ORDER created Successfully :" + orderRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets).setColocatedWith("ORDER")
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    shipmentRegion = cache.createRegion("SHIPMENT", attr.create());
    assertNotNull(shipmentRegion);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region SHIPMENT created Successfully :" + shipmentRegion.toString());
    return port;
  }

  public static int createServerWithLocatorAndServerGroup2Regions(String locator,
      int localMaxMemory, int redundantCopies, int totalNoofBuckets, String group) {

    Properties props = new Properties();
    props = new Properties();
    props.setProperty(LOCATORS, locator);

    System.setProperty(
        DistributionConfig.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "true");
    PartitionedRegionSingleHopWithServerGroupDUnitTest test =
        new PartitionedRegionSingleHopWithServerGroupDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

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
      Assert.fail("Failed to start server ", e);
    }

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets);
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    region = cache.createRegion(PR_NAME, attr.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region " + PR_NAME + " created Successfully :" + region.toString());

    // creating colocated Regions
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets)
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    customerRegion = cache.createRegion("CUSTOMER", attr.create());
    assertNotNull(customerRegion);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region CUSTOMER created Successfully :" + customerRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets)
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    orderRegion = cache.createRegion("ORDER", attr.create());
    assertNotNull(orderRegion);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region ORDER created Successfully :" + orderRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets)
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    shipmentRegion = cache.createRegion("SHIPMENT", attr.create());
    assertNotNull(shipmentRegion);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region SHIPMENT created Successfully :" + shipmentRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets);
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    region2 = cache.createRegion(PR_NAME2, attr.create());
    assertNotNull(region2);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region " + PR_NAME2 + " created Successfully :" + region2.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets)
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    customerRegion2 = cache.createRegion(CUSTOMER2, attr.create());
    assertNotNull(customerRegion2);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region CUSTOMER2 created Successfully :" + customerRegion2.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets)
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    orderRegion2 = cache.createRegion(ORDER2, attr.create());
    assertNotNull(orderRegion2);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region ORDER2 created Successfully :" + orderRegion2.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNoofBuckets)
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    shipmentRegion2 = cache.createRegion(SHIPMENT2, attr.create());
    assertNotNull(shipmentRegion2);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region SHIPMENT2 created Successfully :" + shipmentRegion2.toString());

    return port;
  }

  public static void createClientWithLocator(String host, int port0, String group) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(LOG_FILE, "");
    PartitionedRegionSingleHopWithServerGroupDUnitTest test =
        new PartitionedRegionSingleHopWithServerGroupDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
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

  public static void create2ClientWithLocator(String host, int port0, String group1,
      String group2) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    PartitionedRegionSingleHopWithServerGroupDUnitTest test =
        new PartitionedRegionSingleHopWithServerGroupDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p1, p2, p3;
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

  public static void createClientWith3PoolLocator(String host, int port0, String group1,
      String group2, String group3) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    PartitionedRegionSingleHopWithServerGroupDUnitTest test =
        new PartitionedRegionSingleHopWithServerGroupDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p1, p2, p3;
    try {
      p1 = PoolManager.createFactory().addLocator(host, port0).setServerGroup(group1)
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(PR_NAME);
      p2 = PoolManager.createFactory().addLocator(host, port0).setServerGroup(group2)
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(PR_NAME2);
      p3 = PoolManager.createFactory().addLocator(host, port0).setServerGroup(group3)
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(PR_NAME3);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    createColocatedRegionsInClientCacheWithDiffPool(p1.getName(), p2.getName(), p3.getName());
  }

  private static void createRegionsInClientCache(String poolName) {
    AttributesFactory factory = new AttributesFactory();
    factory.setPoolName(poolName);
    factory.setDataPolicy(DataPolicy.EMPTY);
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(PR_NAME, attrs);
    assertNotNull(region);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region " + PR_NAME + " created Successfully :" + region.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName);
    attrs = factory.create();
    customerRegion = cache.createRegion("CUSTOMER", attrs);
    assertNotNull(customerRegion);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region CUSTOMER created Successfully :" + customerRegion.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName);
    attrs = factory.create();
    orderRegion = cache.createRegion("ORDER", attrs);
    assertNotNull(orderRegion);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region ORDER created Successfully :" + orderRegion.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName);
    attrs = factory.create();
    shipmentRegion = cache.createRegion("SHIPMENT", attrs);
    assertNotNull(shipmentRegion);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region SHIPMENT created Successfully :" + shipmentRegion.toString());
  }

  private static void create2RegionsInClientCache(String poolName1, String poolName2) {
    AttributesFactory factory = new AttributesFactory();
    factory.setPoolName(poolName1);
    factory.setDataPolicy(DataPolicy.EMPTY);
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(PR_NAME, attrs);
    assertNotNull(region);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region " + PR_NAME + " created Successfully :" + region.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName1);
    attrs = factory.create();
    customerRegion = cache.createRegion("CUSTOMER", attrs);
    assertNotNull(customerRegion);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region CUSTOMER created Successfully :" + customerRegion.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName1);
    attrs = factory.create();
    orderRegion = cache.createRegion("ORDER", attrs);
    assertNotNull(orderRegion);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region ORDER created Successfully :" + orderRegion.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName1);
    attrs = factory.create();
    shipmentRegion = cache.createRegion("SHIPMENT", attrs);
    assertNotNull(shipmentRegion);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region SHIPMENT created Successfully :" + shipmentRegion.toString());


    factory = new AttributesFactory();
    factory.setPoolName(poolName2);
    factory.setDataPolicy(DataPolicy.EMPTY);
    attrs = factory.create();
    region2 = cache.createRegion(PR_NAME2, attrs);
    assertNotNull(region2);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region " + PR_NAME2 + " created Successfully :" + region2.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName2);
    attrs = factory.create();
    customerRegion2 = cache.createRegion(CUSTOMER2, attrs);
    assertNotNull(customerRegion2);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region CUSTOMER2 created Successfully :" + customerRegion2.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName2);
    attrs = factory.create();
    orderRegion2 = cache.createRegion(ORDER2, attrs);
    assertNotNull(orderRegion2);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region ORDER2 created Successfully :" + orderRegion2.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName2);
    attrs = factory.create();
    shipmentRegion2 = cache.createRegion(SHIPMENT2, attrs);
    assertNotNull(shipmentRegion2);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region SHIPMENT2 created Successfully :" + shipmentRegion2.toString());
  }

  private static void createColocatedRegionsInClientCacheWithDiffPool(String poolName1,
      String poolName2, String poolName3) {
    AttributesFactory factory = new AttributesFactory();
    factory.setPoolName(poolName1);
    factory.setDataPolicy(DataPolicy.EMPTY);
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(PR_NAME, attrs);
    assertNotNull(region);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region " + PR_NAME + " created Successfully :" + region.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName1);
    attrs = factory.create();
    customerRegion = cache.createRegion("CUSTOMER", attrs);
    assertNotNull(customerRegion);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region CUSTOMER created Successfully :" + customerRegion.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName2);
    attrs = factory.create();
    orderRegion = cache.createRegion("ORDER", attrs);
    assertNotNull(orderRegion);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region ORDER created Successfully :" + orderRegion.toString());

    factory = new AttributesFactory();
    factory.setPoolName(poolName3);
    attrs = factory.create();
    shipmentRegion = cache.createRegion("SHIPMENT", attrs);
    assertNotNull(shipmentRegion);
    LogWriterUtils.getLogWriter()
        .info("Distributed Region SHIPMENT created Successfully :" + shipmentRegion.toString());
  }

  public static int createAccessorServer(int redundantCopies, int numBuckets, String group) {
    PartitionedRegionSingleHopWithServerGroupDUnitTest test =
        new PartitionedRegionSingleHopWithServerGroupDUnitTest();
    cache = test.getCache();
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    if (group.length() != 0) {
      server.setGroups(new String[] {group});
    }
    try {
      server.start();
    } catch (IOException e) {
      Assert.fail("Failed to start server ", e);
    }

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(numBuckets).setLocalMaxMemory(0);
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    region = cache.createRegion(PR_NAME, attr.create());

    assertNotNull(region);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region " + PR_NAME + " created Successfully :" + region.toString());

    // creating colocated Regions
    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(numBuckets).setLocalMaxMemory(0)
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    customerRegion = cache.createRegion("CUSTOMER", attr.create());
    assertNotNull(customerRegion);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region CUSTOMER created Successfully :" + customerRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(numBuckets).setLocalMaxMemory(0)
        .setColocatedWith("CUSTOMER")
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    orderRegion = cache.createRegion("ORDER", attr.create());
    assertNotNull(orderRegion);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region ORDER created Successfully :" + orderRegion.toString());

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(numBuckets).setLocalMaxMemory(0)
        .setColocatedWith("ORDER")
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    shipmentRegion = cache.createRegion("SHIPMENT", attr.create());
    assertNotNull(shipmentRegion);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region SHIPMENT created Successfully :" + shipmentRegion.toString());
    return port;
  }

  public static void createClientWithLocatorWithoutSystemProperty(String host, int port0,
      String group) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    PartitionedRegionSingleHopWithServerGroupDUnitTest test =
        new PartitionedRegionSingleHopWithServerGroupDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
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

  public static void putIntoPartitionedRegions() {
    for (int i = 0; i <= 800; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      customerRegion.put(custid, customer);
    }
    for (int j = 0; j <= 800; j++) {
      CustId custid = new CustId(j);
      OrderId orderId = new OrderId(j, custid);
      Order order = new Order("OREDR" + j);
      orderRegion.put(orderId, order);
    }
    for (int k = 0; k <= 800; k++) {
      CustId custid = new CustId(k);
      OrderId orderId = new OrderId(k, custid);
      ShipmentId shipmentId = new ShipmentId(k, orderId);
      Shipment shipment = new Shipment("Shipment" + k);
      shipmentRegion.put(shipmentId, shipment);
    }

    region.put(new Integer(0), "create0");
    region.put(new Integer(1), "create1");
    region.put(new Integer(2), "create2");
    region.put(new Integer(3), "create3");
    region.put(new Integer(4), "create0");
    region.put(new Integer(5), "create1");
    region.put(new Integer(6), "create2");
    region.put(new Integer(7), "create3");

    region.put(new Integer(0), "update0");
    region.put(new Integer(1), "update1");
    region.put(new Integer(2), "update2");
    region.put(new Integer(3), "update3");
    region.put(new Integer(4), "update0");
    region.put(new Integer(5), "update1");
    region.put(new Integer(6), "update2");
    region.put(new Integer(7), "update3");

    region.put(new Integer(0), "update00");
    region.put(new Integer(1), "update11");
    region.put(new Integer(2), "update22");
    region.put(new Integer(3), "update33");
    region.put(new Integer(4), "update00");
    region.put(new Integer(5), "update11");
    region.put(new Integer(6), "update22");
    region.put(new Integer(7), "update33");
  }

  public static void putIntoPartitionedRegions2Client() {
    for (int i = 0; i <= 800; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      customerRegion.put(custid, customer);
      customerRegion2.put(custid, customer);
    }
    for (int j = 0; j <= 800; j++) {
      CustId custid = new CustId(j);
      OrderId orderId = new OrderId(j, custid);
      Order order = new Order("OREDR" + j);
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

    region.put(new Integer(0), "create0");
    region.put(new Integer(1), "create1");
    region.put(new Integer(2), "create2");
    region.put(new Integer(3), "create3");
    region.put(new Integer(4), "create0");
    region.put(new Integer(5), "create1");
    region.put(new Integer(6), "create2");
    region.put(new Integer(7), "create3");

    region.put(new Integer(0), "update0");
    region.put(new Integer(1), "update1");
    region.put(new Integer(2), "update2");
    region.put(new Integer(3), "update3");
    region.put(new Integer(4), "update0");
    region.put(new Integer(5), "update1");
    region.put(new Integer(6), "update2");
    region.put(new Integer(7), "update3");

    region.put(new Integer(0), "update00");
    region.put(new Integer(1), "update11");
    region.put(new Integer(2), "update22");
    region.put(new Integer(3), "update33");
    region.put(new Integer(4), "update00");
    region.put(new Integer(5), "update11");
    region.put(new Integer(6), "update22");
    region.put(new Integer(7), "update33");

    region2.put(new Integer(0), "create0");
    region2.put(new Integer(1), "create1");
    region2.put(new Integer(2), "create2");
    region2.put(new Integer(3), "create3");
    region2.put(new Integer(4), "create0");
    region2.put(new Integer(5), "create1");
    region2.put(new Integer(6), "create2");
    region2.put(new Integer(7), "create3");

    region2.put(new Integer(0), "update0");
    region2.put(new Integer(1), "update1");
    region2.put(new Integer(2), "update2");
    region2.put(new Integer(3), "update3");
    region2.put(new Integer(4), "update0");
    region2.put(new Integer(5), "update1");
    region2.put(new Integer(6), "update2");
    region2.put(new Integer(7), "update3");

    region2.put(new Integer(0), "update00");
    region2.put(new Integer(1), "update11");
    region2.put(new Integer(2), "update22");
    region2.put(new Integer(3), "update33");
    region2.put(new Integer(4), "update00");
    region2.put(new Integer(5), "update11");
    region2.put(new Integer(6), "update22");
    region2.put(new Integer(7), "update33");
  }

  public static void getFromPartitionedRegions() {
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

    region.get(new Integer(0));
    region.get(new Integer(1));
    region.get(new Integer(2));
    region.get(new Integer(3));
    region.get(new Integer(4));
    region.get(new Integer(5));
    region.get(new Integer(6));
    region.get(new Integer(7));
  }

  public static void getFromPartitionedRegions2Client() {

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

    region.get(new Integer(0));
    region.get(new Integer(1));
    region.get(new Integer(2));
    region.get(new Integer(3));
    region.get(new Integer(4));
    region.get(new Integer(5));
    region.get(new Integer(6));
    region.get(new Integer(7));

    region2.get(new Integer(0));
    region2.get(new Integer(1));
    region2.get(new Integer(2));
    region2.get(new Integer(3));
    region2.get(new Integer(4));
    region2.get(new Integer(5));
    region2.get(new Integer(6));
    region.get(new Integer(7));
  }

  public static void startLocatorInVM(final int locatorPort) {
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

  public static void stopLocator() {
    locator.stop();
  }

  public static void resetHonourServerGroupsInPRSingleHop() {
    System.setProperty(
        DistributionConfig.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "False");
  }

  public static void setHonourServerGroupsInPRSingleHop() {
    System.setProperty(
        DistributionConfig.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop", "True");
  }
}
