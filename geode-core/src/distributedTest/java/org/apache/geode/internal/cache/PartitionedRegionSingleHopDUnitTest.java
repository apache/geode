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
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.client.internal.ClientPartitionAdvisor;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.BucketAdvisor.ServerBucketProfile;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.execute.data.ShipmentId;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class PartitionedRegionSingleHopDUnitTest extends JUnit4CacheTestCase {

  private static final String PR_NAME = "single_hop_pr";
  private static final String ORDER = "ORDER";
  private static final String CUSTOMER = "CUSTOMER";
  private static final String SHIPMENT = "SHIPMENT";

  private VM member0 = null;

  private VM member1 = null;

  private VM member2 = null;

  private VM member3 = null;

  private static Region<Object, Object> testRegion = null;

  private static Region<Object, Object> customerRegion = null;

  private static Region<Object, Object> orderRegion = null;

  private static Region<Object, Object> shipmentRegion = null;

  private static Region<Object, Object> replicatedRegion = null;

  private static Cache cache = null;

  private static Locator locator = null;

  @Override
  public final void postSetUp() {
    IgnoredException.addIgnoredException("Connection refused");

    member0 = VM.getVM(0);
    member1 = VM.getVM(1);
    member2 = VM.getVM(2);
    member3 = VM.getVM(3);
  }

  @Override
  public final void postTearDownCacheTestCase() {
    try {
      closeCacheAndDisconnect();

      member0 = null;
      member1 = null;
      member2 = null;
      member3 = null;

    } finally {
      DistributedTestUtils.unregisterAllDataSerializersFromAllVms();
    }
  }

  private static void closeCacheAndDisconnect() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  private static void stopServer() {
    for (CacheServer cacheServer : cache.getCacheServers()) {
      cacheServer.stop();
    }
  }

  private static void startLocatorInVM(final int locatorPort) {

    File logFile = new File("locator-" + locatorPort + ".log");

    Properties props = new Properties();
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    try {
      locator = Locator.startLocatorAndDS(locatorPort, logFile, null, props);
    } catch (IOException e) {
      Assertions.fail("failed to startLocatorInVM", e);
    }
  }

  private static void stopLocator() {
    locator.stop();
  }

  private static int createServerWithLocator(String locString, int redundantCopies,
      int totalNoofBuckets) {

    Properties props = new Properties();
    props.setProperty(LOCATORS, locString);
    PartitionedRegionSingleHopDUnitTest test = new PartitionedRegionSingleHopDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    CacheServer server = cache.addCacheServer();
    server.setPort(0);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      Assertions.fail("Failed to start server ", e);
    }

    testRegion = createBasicPartitionedRegion(redundantCopies, totalNoofBuckets, -1, PR_NAME);

    customerRegion = createColocatedRegion(ORDER, redundantCopies, totalNoofBuckets, CUSTOMER);

    shipmentRegion = createColocatedRegion(SHIPMENT, redundantCopies, totalNoofBuckets, ORDER);

    return server.getPort();
  }

  private static void clearMetadata() {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.getClientPartitionAttributesMap().clear();
    cms.getClientPRMetadata_TEST_ONLY().clear();
  }

  /**
   * 2 peers 2 servers 1 accessor.No client.Should work without any exceptions.
   */
  @Test
  public void test_NoClient() {
    member0.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(1, 4));
    member1.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(1, 4));

    member2.invoke(PartitionedRegionSingleHopDUnitTest::createPeer);
    member3.invoke(PartitionedRegionSingleHopDUnitTest::createPeer);

    createAccessorServer();
    member0.invoke(PartitionedRegionSingleHopDUnitTest::clearMetadata);
    member1.invoke(PartitionedRegionSingleHopDUnitTest::clearMetadata);
    member2.invoke(PartitionedRegionSingleHopDUnitTest::clearMetadata);
    member3.invoke(PartitionedRegionSingleHopDUnitTest::clearMetadata);
    clearMetadata();

    member0.invoke(PartitionedRegionSingleHopDUnitTest::putIntoPartitionedRegions);
    member1.invoke(PartitionedRegionSingleHopDUnitTest::putIntoPartitionedRegions);
    member2.invoke(PartitionedRegionSingleHopDUnitTest::putIntoPartitionedRegions);
    member3.invoke(PartitionedRegionSingleHopDUnitTest::putIntoPartitionedRegions);
    putIntoPartitionedRegions();

    member0.invoke(PartitionedRegionSingleHopDUnitTest::getFromPartitionedRegions);
    member1.invoke(PartitionedRegionSingleHopDUnitTest::getFromPartitionedRegions);
    member2.invoke(PartitionedRegionSingleHopDUnitTest::getFromPartitionedRegions);
    member3.invoke(PartitionedRegionSingleHopDUnitTest::getFromPartitionedRegions);
    getFromPartitionedRegions();

    member0.invoke(PartitionedRegionSingleHopDUnitTest::verifyEmptyMetadata);
    member1.invoke(PartitionedRegionSingleHopDUnitTest::verifyEmptyMetadata);
    member2.invoke(PartitionedRegionSingleHopDUnitTest::verifyEmptyMetadata);
    member3.invoke(PartitionedRegionSingleHopDUnitTest::verifyEmptyMetadata);
    verifyEmptyMetadata();

    member0.invoke(PartitionedRegionSingleHopDUnitTest::verifyEmptyStaticData);
    member1.invoke(PartitionedRegionSingleHopDUnitTest::verifyEmptyStaticData);
    member2.invoke(PartitionedRegionSingleHopDUnitTest::verifyEmptyStaticData);
    member3.invoke(PartitionedRegionSingleHopDUnitTest::verifyEmptyStaticData);
    verifyEmptyStaticData();
  }

  /**
   * 2 AccessorServers, 2 Peers 1 Client connected to 2 AccessorServers. Hence metadata should not
   * be fetched.
   */
  @Test
  public void test_ClientConnectedToAccessors() {
    Integer port0 =
        member0.invoke(PartitionedRegionSingleHopDUnitTest::createAccessorServer);
    Integer port1 =
        member1.invoke(PartitionedRegionSingleHopDUnitTest::createAccessorServer);

    member2.invoke(PartitionedRegionSingleHopDUnitTest::createPeer);

    member3.invoke(PartitionedRegionSingleHopDUnitTest::createPeer);

    createClient(port0, port1);

    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    verifyEmptyMetadata();

    verifyEmptyStaticData();
  }

  /**
   * 1 server 2 accesorservers 2 peers.i client connected to the server Since only 1 server hence
   * Metadata should not be fetched.
   */
  @Test
  public void test_ClientConnectedTo1Server() {
    Integer port0 =
        member0.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(1, 4));

    member1.invoke(PartitionedRegionSingleHopDUnitTest::createPeer);
    member2.invoke(PartitionedRegionSingleHopDUnitTest::createPeer);

    member3.invoke(PartitionedRegionSingleHopDUnitTest::createAccessorServer);

    createClient(port0);

    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    verifyEmptyMetadata();

    verifyEmptyStaticData();
  }

  /**
   * 4 servers, 1 client connected to all 4 servers. Put data, get data and make the metadata
   * stable. Now verify that metadata has all 8 buckets info. Now update and ensure the fetch
   * service is never called.
   */
  @Test
  public void test_MetadataContents() {
    Integer port0 =
        member0.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(1, 4));
    Integer port1 =
        member1.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(1, 4));
    Integer port2 =
        member2.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(1, 4));
    Integer port3 =
        member3.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(1, 4));
    createClient(port0, port1, port2, port3);

    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    member0.invoke(PartitionedRegionSingleHopDUnitTest::printView);
    member1.invoke(PartitionedRegionSingleHopDUnitTest::printView);
    member2.invoke(PartitionedRegionSingleHopDUnitTest::printView);
    member3.invoke(PartitionedRegionSingleHopDUnitTest::printView);

    verifyMetadata();
    updateIntoSinglePR();
  }

  /**
   * 2 servers, 2 clients.One client to one server. Put from c1 to s1. Now put from c2. So since
   * there will be a hop at least once, fetchservice has to be triggered. Now put again from
   * c2.There should be no hop at all.
   */
  @Test
  public void test_MetadataServiceCallAccuracy() {
    Integer port0 =
        member0.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(1, 4));
    Integer port1 =
        member1.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(1, 4));

    member2.invoke(() -> PartitionedRegionSingleHopDUnitTest.createClient(port0));
    createClient(port1);

    member2.invoke(PartitionedRegionSingleHopDUnitTest::putIntoSinglePR);

    member0.invoke(PartitionedRegionSingleHopDUnitTest::printView);
    member1.invoke(PartitionedRegionSingleHopDUnitTest::printView);
    final ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.put(0, "create0");
    testRegion.put(1, "create1");
    testRegion.put(2, "create2");
    testRegion.put(3, "create3");

    await()
        .until(cms::isRefreshMetadataTestOnly);

    // make sure all fetch tasks are completed
    await()
        .until(() -> cms.getRefreshTaskCount_TEST_ONLY() == 0);

    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    testRegion.put(0, "create0");
    testRegion.put(1, "create1");
    testRegion.put(2, "create2");
    testRegion.put(3, "create3");

    await()
        .until(() -> !cms.isRefreshMetadataTestOnly());
  }

  @Test
  public void test_MetadataServiceCallAccuracy_FromDestroyOp() {
    Integer port0 =
        member0.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(0, 4));
    Integer port1 =
        member1.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(0, 4));

    member2.invoke(() -> PartitionedRegionSingleHopDUnitTest.createClient(port0));
    createClient(port1);

    member2.invoke(PartitionedRegionSingleHopDUnitTest::putIntoSinglePR);

    member0.invoke(PartitionedRegionSingleHopDUnitTest::printView);
    member1.invoke(PartitionedRegionSingleHopDUnitTest::printView);
    final ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.destroy(0);
    testRegion.destroy(1);
    testRegion.destroy(2);
    testRegion.destroy(3);

    await()
        .until(cms::isRefreshMetadataTestOnly);
  }

  @Test
  public void test_MetadataServiceCallAccuracy_FromGetOp() {
    Integer port0 =
        member0.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(0, 4));
    Integer port1 =
        member1.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(0, 4));

    member2.invoke(() -> PartitionedRegionSingleHopDUnitTest.createClient(port0));
    createClient(port1);

    member2.invoke(PartitionedRegionSingleHopDUnitTest::putIntoSinglePR);

    member0.invoke(PartitionedRegionSingleHopDUnitTest::printView);
    member1.invoke(PartitionedRegionSingleHopDUnitTest::printView);
    final ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.get(0);
    testRegion.get(1);
    testRegion.get(2);
    testRegion.get(3);

    await()
        .until(cms::isRefreshMetadataTestOnly);
    printMetadata();
    Wait.pause(5000);
    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    testRegion.get(0);
    testRegion.get(1);
    testRegion.get(2);
    testRegion.get(3);
    Wait.pause(5000);
    assertFalse(cms.isRefreshMetadataTestOnly());
  }

  @Test
  public void test_SingleHopWithHA() {
    Integer port0 =
        member0.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(0, 8));
    Integer port1 =
        member1.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(0, 8));
    Integer port2 =
        member2.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(0, 8));
    Integer port3 =
        member3.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(0, 8));
    createClient(port0, port1, port2, port3);
    final ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);

    // put
    for (int i = 1; i <= 16; i++) {
      testRegion.put(i, i);
    }

    // update
    for (int i = 1; i <= 16; i++) {
      testRegion.put(i, i + 1);
    }

    await()
        .until(cms::isRefreshMetadataTestOnly);

    // kill server
    member0.invoke(PartitionedRegionSingleHopDUnitTest::stopServer);

    // again update
    for (int i = 1; i <= 16; i++) {
      testRegion.put(i, i + 10);
    }
  }

  @Test
  public void test_SingleHopWithHAWithLocator() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = NetworkUtils.getServerHostName(member3.getHost());
    final String locator = host0 + "[" + port3 + "]";
    member3.invoke(() -> PartitionedRegionSingleHopDUnitTest.startLocatorInVM(port3));
    try {

      member0
          .invoke(() -> PartitionedRegionSingleHopDUnitTest.createServerWithLocator(locator, 0, 8));
      member1
          .invoke(() -> PartitionedRegionSingleHopDUnitTest.createServerWithLocator(locator, 0, 8));
      member2
          .invoke(() -> PartitionedRegionSingleHopDUnitTest.createServerWithLocator(locator, 0, 8));

      createClientWithLocator(host0, port3);

      // put
      for (int i = 1; i <= 16; i++) {
        testRegion.put(i, i);
      }

      // update
      for (int i = 1; i <= 16; i++) {
        testRegion.put(i, i + 1);
      }

      // kill server
      member0.invoke(PartitionedRegionSingleHopDUnitTest::stopServer);

      // again update
      for (int i = 1; i <= 16; i++) {
        testRegion.put(i, i + 10);
      }

    } finally {
      member3.invoke(PartitionedRegionSingleHopDUnitTest::stopLocator);
    }
  }

  @Test
  public void test_NoMetadataServiceCall_ForGetOp() {
    Integer port0 =
        member0.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(0, 4));
    Integer port1 =
        member1.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(0, 4));

    member2.invoke(
        () -> PartitionedRegionSingleHopDUnitTest.createClientWithoutPRSingleHopEnabled(port0));
    createClientWithoutPRSingleHopEnabled(port1);
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    member2.invoke(PartitionedRegionSingleHopDUnitTest::putIntoSinglePR);

    member0.invoke(PartitionedRegionSingleHopDUnitTest::printView);
    member1.invoke(PartitionedRegionSingleHopDUnitTest::printView);

    testRegion.get(0);
    testRegion.get(1);
    testRegion.get(2);
    testRegion.get(3);
    Wait.pause(5000);
    assertFalse(cms.isRefreshMetadataTestOnly());
    printMetadata();

    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    testRegion.get(0);
    testRegion.get(1);
    testRegion.get(2);
    testRegion.get(3);
    Wait.pause(5000);
    assertFalse(cms.isRefreshMetadataTestOnly());
  }

  @Test
  public void test_NoMetadataServiceCall() {
    Integer port0 =
        member0.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(1, 4));
    Integer port1 =
        member1.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(1, 4));

    member2.invoke(
        () -> PartitionedRegionSingleHopDUnitTest.createClientWithoutPRSingleHopEnabled(port0));
    createClientWithoutPRSingleHopEnabled(port1);

    member2.invoke(PartitionedRegionSingleHopDUnitTest::putIntoSinglePR);
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);

    member0.invoke(PartitionedRegionSingleHopDUnitTest::printView);
    member1.invoke(PartitionedRegionSingleHopDUnitTest::printView);

    testRegion.put(0, "create0");
    final boolean metadataRefreshed_get1 = cms.isRefreshMetadataTestOnly();

    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    testRegion.put(1, "create1");
    final boolean metadataRefreshed_get2 = cms.isRefreshMetadataTestOnly();

    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    testRegion.put(2, "create2");
    final boolean metadataRefreshed_get3 = cms.isRefreshMetadataTestOnly();

    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    testRegion.put(3, "create3");
    final boolean metadataRefreshed_get4 = cms.isRefreshMetadataTestOnly();
    Wait.pause(5000);
    assertFalse(metadataRefreshed_get1 || metadataRefreshed_get2 || metadataRefreshed_get3
        || metadataRefreshed_get4);

    printMetadata();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    testRegion.put(0, "create0");
    testRegion.put(1, "create1");
    testRegion.put(2, "create2");
    testRegion.put(3, "create3");
    Wait.pause(5000);
    assertFalse(cms.isRefreshMetadataTestOnly());
  }

  @Test
  public void test_NoMetadataServiceCall_ForDestroyOp() {
    Integer port0 =
        member0.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(0, 4));
    Integer port1 =
        member1.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(0, 4));

    member2.invoke(
        () -> PartitionedRegionSingleHopDUnitTest.createClientWithoutPRSingleHopEnabled(port0));
    createClientWithoutPRSingleHopEnabled(port1);

    member2.invoke(PartitionedRegionSingleHopDUnitTest::putIntoSinglePR);

    member0.invoke(PartitionedRegionSingleHopDUnitTest::printView);
    member1.invoke(PartitionedRegionSingleHopDUnitTest::printView);
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    testRegion.destroy(0);
    testRegion.destroy(1);
    testRegion.destroy(2);
    testRegion.destroy(3);
    Wait.pause(5000);
    assertFalse(cms.isRefreshMetadataTestOnly());
  }

  @Test
  public void testServerLocationRemovalThroughPing() {
    Integer port0 =
        member0.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(3, 4));
    Integer port1 =
        member1.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(3, 4));
    Integer port2 =
        member2.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(3, 4));
    Integer port3 =
        member3.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(3, 4));
    createClient(port0, port1, port2, port3);
    putIntoPartitionedRegions();
    getFromPartitionedRegions();
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();
    await().until(() -> regionMetaData.size() == 4);
    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();
    assertThat(regionMetaData.containsKey(customerRegion.getFullPath())).isTrue();
    assertThat(regionMetaData.containsKey(orderRegion.getFullPath())).isTrue();
    assertThat(regionMetaData.containsKey(shipmentRegion.getFullPath())).isTrue();

    ClientPartitionAdvisor prMetaData = regionMetaData.get(testRegion.getFullPath());
    assertThat(prMetaData.getBucketServerLocationsMap_TEST_ONLY().size())
        .isEqualTo(4/* numBuckets */);

    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertThat(((List) entry.getValue()).size()).isEqualTo(4);
    }
    member0.invoke(PartitionedRegionSingleHopDUnitTest::stopServer);
    member1.invoke(PartitionedRegionSingleHopDUnitTest::stopServer);
    Wait.pause(5000);// make sure that ping detects the dead servers
    getFromPartitionedRegions();
    verifyDeadServer(regionMetaData, customerRegion, port0, port1);
    verifyDeadServer(regionMetaData, testRegion, port0, port1);
  }

  @Test
  public void testMetadataFetchOnlyThroughFunctions() throws Exception {
    // Workaround for 52004
    IgnoredException.addIgnoredException("InternalFunctionInvocationTargetException");
    Integer port0 =
        member0.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(3, 4));
    Integer port1 =
        member1.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(3, 4));
    Integer port2 =
        member2.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(3, 4));
    Integer port3 =
        member3.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(3, 4));
    createClient(port0, port1, port2, port3);
    executeFunctions();
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();

    await().until(() -> regionMetaData.size() == 1);

    assertThat(regionMetaData.size()).isEqualTo(1);
    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();

    final ClientPartitionAdvisor prMetaData = regionMetaData.get(testRegion.getFullPath());

    long start = System.currentTimeMillis();
    do {
      if ((prMetaData.getBucketServerLocationsMap_TEST_ONLY().size() != 4)) {
        // waiting if there is another thread holding the lock
        Thread.sleep(1000);
        cms.getClientPRMetadata((LocalRegion) testRegion);
      } else {
        break;
      }
    } while (System.currentTimeMillis() - start < 60000);

    await()
        .until(() -> (prMetaData.getBucketServerLocationsMap_TEST_ONLY().size() == 4));
  }

  @Test
  public void testMetadataFetchOnlyThroughputAll() {
    Integer port0 =
        member0.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(3, 4));
    Integer port1 =
        member1.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(3, 4));
    Integer port2 =
        member2.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(3, 4));
    Integer port3 =
        member3.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(3, 4));
    createClient(port0, port1, port2, port3);
    putAll();
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();

    await().until(() -> (regionMetaData.size() == 1));
    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();

    final ClientPartitionAdvisor prMetaData = regionMetaData.get(testRegion.getFullPath());

    await()
        .until(() -> (prMetaData.getBucketServerLocationsMap_TEST_ONLY().size() == 4));
  }

  @Test
  public void testMetadataIsSameOnAllServersAndClients() {
    Integer port0 = member0.invoke(() -> createServer(3, 4));
    Integer port1 = member1.invoke(() -> createServer(3, 4));
    Integer port2 = member2.invoke(() -> createServer(3, 4));
    Integer port3 = member3.invoke(() -> createServer(3, 4));
    createClient(port0, port1, port2, port3);
    put();
    member0.invoke(() -> waitForLocalBucketsCreation(4));
    member1.invoke(() -> waitForLocalBucketsCreation(4));
    member2.invoke(() -> waitForLocalBucketsCreation(4));
    member3.invoke(() -> waitForLocalBucketsCreation(4));

    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.getClientPRMetadata((LocalRegion) testRegion);

    Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();
    assertThat(regionMetaData.size()).isEqualTo(1);
    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();

    ClientPartitionAdvisor prMetaData = regionMetaData.get(testRegion.getFullPath());
    final Map<Integer, List<BucketServerLocation66>> clientMap =
        prMetaData.getBucketServerLocationsMap_TEST_ONLY();

    await().alias("expected no metadata to be refreshed").until(() -> clientMap.size() == 4);

    for (Entry entry : clientMap.entrySet()) {
      assertThat(((List) entry.getValue()).size()).isEqualTo(4);
    }
    member0.invoke(() -> verifyMetadata(clientMap));
    member1.invoke(() -> verifyMetadata(clientMap));
    member2.invoke(() -> verifyMetadata(clientMap));
    member3.invoke(() -> verifyMetadata(clientMap));

    member0.invoke(PartitionedRegionSingleHopDUnitTest::stopServer);
    member1.invoke(PartitionedRegionSingleHopDUnitTest::stopServer);

    member0.invoke(() -> startServerOnPort(port0));
    member1.invoke(() -> startServerOnPort(port1));
    put();
    member0.invoke(() -> waitForLocalBucketsCreation(4));
    member1.invoke(() -> waitForLocalBucketsCreation(4));
    member2.invoke(() -> waitForLocalBucketsCreation(4));
    member3.invoke(() -> waitForLocalBucketsCreation(4));

    ((GemFireCacheImpl) cache).getClientMetadataService();
    await().alias("bucket copies are not created").until(() -> {
      ClientMetadataService lambdaCms = ((GemFireCacheImpl) cache).getClientMetadataService();
      Map<String, ClientPartitionAdvisor> lambdaRegionMetaData =
          lambdaCms.getClientPRMetadata_TEST_ONLY();
      assertThat(lambdaRegionMetaData.size()).isEqualTo(1);
      assertThat(lambdaRegionMetaData.containsKey(testRegion.getFullPath())).isTrue();
      ClientPartitionAdvisor lambdaPartitionMetaData =
          lambdaRegionMetaData.get(testRegion.getFullPath());
      Map<Integer, List<BucketServerLocation66>> lambdaClientMap =
          lambdaPartitionMetaData.getBucketServerLocationsMap_TEST_ONLY();
      assertThat(lambdaClientMap.size()).isEqualTo(4/* numBuckets */);
      boolean finished = true;
      for (Entry entry : lambdaClientMap.entrySet()) {
        List list = (List) entry.getValue();
        if (list.size() < 4) {
          logger
              .info("still waiting for 4 bucket owners in " + entry.getKey() + ": " + list);
          finished = false;
          break;
        }
      }
      return finished;
    });

    cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.getClientPRMetadata((LocalRegion) testRegion);

    regionMetaData = cms.getClientPRMetadata_TEST_ONLY();
    assertThat(regionMetaData.size()).isEqualTo(1);
    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();

    prMetaData = regionMetaData.get(testRegion.getFullPath());
    final Map<Integer, List<BucketServerLocation66>> clientMap2 =
        prMetaData.getBucketServerLocationsMap_TEST_ONLY();

    await().alias("expected no metadata to be refreshed").until(() -> clientMap2.size() == 4);
    for (Entry entry : clientMap.entrySet()) {
      assertThat(((List) entry.getValue()).size()).isEqualTo(4);
    }

    member0.invoke(() -> verifyMetadata(clientMap));
    member1.invoke(() -> verifyMetadata(clientMap));
    member2.invoke(() -> verifyMetadata(clientMap));
    member3.invoke(() -> verifyMetadata(clientMap));

    member0.invoke(PartitionedRegionSingleHopDUnitTest::closeCacheAndDisconnect);
    member1.invoke(PartitionedRegionSingleHopDUnitTest::closeCacheAndDisconnect);

    put();
    member2.invoke("aba", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        final PartitionedRegion pr = (PartitionedRegion) testRegion;
        pr.getRegionAdvisor().getAllClientBucketProfilesTest();
      }
    });

    member3.invoke("aba", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        final PartitionedRegion pr = (PartitionedRegion) testRegion;
        pr.getRegionAdvisor().getAllClientBucketProfilesTest();
      }
    });

    member2.invoke(() -> waitForLocalBucketsCreation(4));
    member3.invoke(() -> waitForLocalBucketsCreation(4));

    cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.getClientPRMetadata((LocalRegion) testRegion);

    regionMetaData = cms.getClientPRMetadata_TEST_ONLY();
    assertThat(regionMetaData.size()).isEqualTo(1);
    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();

    prMetaData = regionMetaData.get(testRegion.getFullPath());
    final Map<Integer, List<BucketServerLocation66>> clientMap3 =
        prMetaData.getBucketServerLocationsMap_TEST_ONLY();

    await().until(() -> (clientMap3.size() == 4));
    for (Entry entry : clientMap.entrySet()) {
      assertThat(((List) entry.getValue()).size()).isEqualTo(2);
    }

    await().alias("verification of metadata on all members").until(() -> {
      try {
        member2.invoke(() -> verifyMetadata(clientMap));
        member3.invoke(() -> verifyMetadata(clientMap));
      } catch (Exception e) {
        logger.info("verification failed", e);
        return false;
      }
      return true;
    });
  }

  @Test
  public void testMetadataIsSameOnAllServersAndClientsHA() {
    Integer port0 =
        member0.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(2, 4));
    Integer port1 =
        member1.invoke(() -> PartitionedRegionSingleHopDUnitTest.createServer(2, 4));

    createClient(port0, port1, port0, port1);
    put();

    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.getClientPRMetadata((LocalRegion) testRegion);

    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();

    await().until(() -> (regionMetaData.size() == 1));

    assertThat(regionMetaData.size()).isEqualTo(1);
    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();

    member0.invoke("aba", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        final PartitionedRegion pr = (PartitionedRegion) testRegion;
        pr.getRegionAdvisor().getAllClientBucketProfilesTest();
      }
    });

    member1.invoke("aba", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        final PartitionedRegion pr = (PartitionedRegion) testRegion;
        pr.getRegionAdvisor().getAllClientBucketProfilesTest();
      }
    });

    ClientPartitionAdvisor prMetaData = regionMetaData.get(testRegion.getFullPath());
    final Map<Integer, List<BucketServerLocation66>> clientMap =
        prMetaData.getBucketServerLocationsMap_TEST_ONLY();
    await().until(() -> (clientMap.size() == 4));
    for (Entry entry : clientMap.entrySet()) {
      assertThat(((List) entry.getValue()).size()).isEqualTo(2);
    }
    member0.invoke(() -> PartitionedRegionSingleHopDUnitTest.verifyMetadata(clientMap));
    member1.invoke(() -> PartitionedRegionSingleHopDUnitTest.verifyMetadata(clientMap));

    member0.invoke(PartitionedRegionSingleHopDUnitTest::stopServer);

    put();

    cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.getClientPRMetadata((LocalRegion) testRegion);

    assertThat(clientMap.size()).isEqualTo(4/* numBuckets */);
    for (Entry entry : clientMap.entrySet()) {
      assertThat(((List) entry.getValue()).size()).isEqualTo(1);
    }

    assertThat(regionMetaData.size()).isEqualTo(1);
    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();
    assertThat(clientMap.size()).isEqualTo(4/* numBuckets */);
    await().until(() -> {
      int bucketId;
      int size;
      List globalList;
      boolean finished = true;
      for (Entry entry : clientMap.entrySet()) {
        List list = (List) entry.getValue();
        if (list.size() != 1) {
          size = list.size();
          globalList = list;
          bucketId = (Integer) entry.getKey();
          finished = false;
          System.out.println("bucket copies are not created, the locations size for bucket id : "
              + bucketId + " size : " + size + " the list is " + globalList);
        }
      }
      return finished;
    });
  }

  @Test
  public void testClientMetadataForPersistentPrs() throws Exception {
    Integer port0 = member0
        .invoke(() -> PartitionedRegionSingleHopDUnitTest.createPersistentPrsAndServer(3, 4));
    Integer port1 = member1
        .invoke(() -> PartitionedRegionSingleHopDUnitTest.createPersistentPrsAndServer(3, 4));
    Integer port2 = member2
        .invoke(() -> PartitionedRegionSingleHopDUnitTest.createPersistentPrsAndServer(3, 4));
    Integer port3 = member3
        .invoke(() -> PartitionedRegionSingleHopDUnitTest.createPersistentPrsAndServer(3, 4));

    member3.invoke(PartitionedRegionSingleHopDUnitTest::putIntoPartitionedRegions);

    member0.invoke(() -> PartitionedRegionSingleHopDUnitTest.waitForLocalBucketsCreation(4));
    member1.invoke(() -> PartitionedRegionSingleHopDUnitTest.waitForLocalBucketsCreation(4));
    member2.invoke(() -> PartitionedRegionSingleHopDUnitTest.waitForLocalBucketsCreation(4));
    member3.invoke(() -> PartitionedRegionSingleHopDUnitTest.waitForLocalBucketsCreation(4));

    createClient(port0, port1, port2, port3);
    await().until(this::fetchAndValidateMetadata);

    member0.invoke(PartitionedRegionSingleHopDUnitTest::closeCacheAndDisconnect);
    member1.invoke(PartitionedRegionSingleHopDUnitTest::closeCacheAndDisconnect);
    member2.invoke(PartitionedRegionSingleHopDUnitTest::closeCacheAndDisconnect);
    member3.invoke(PartitionedRegionSingleHopDUnitTest::closeCacheAndDisconnect);
    Wait.pause(1000); // let client detect that servers are dead through ping
    AsyncInvocation m3 = member3.invokeAsync(
        () -> PartitionedRegionSingleHopDUnitTest.createPersistentPrsAndServerOnPort(3, 4, port3));
    AsyncInvocation m2 = member2.invokeAsync(
        () -> PartitionedRegionSingleHopDUnitTest.createPersistentPrsAndServerOnPort(3, 4, port2));
    AsyncInvocation m1 = member1.invokeAsync(
        () -> PartitionedRegionSingleHopDUnitTest.createPersistentPrsAndServerOnPort(3, 4, port1));
    AsyncInvocation m0 = member0.invokeAsync(
        () -> PartitionedRegionSingleHopDUnitTest.createPersistentPrsAndServerOnPort(3, 4, port0));
    m3.join();
    m2.join();
    m1.join();
    m0.join();
    fetchAndValidateMetadata();
  }

  private boolean fetchAndValidateMetadata() {
    ClientMetadataService service = ((GemFireCacheImpl) cache).getClientMetadataService();
    service.getClientPRMetadata((LocalRegion) testRegion);
    HashMap<ServerLocation, HashSet<Integer>> servers =
        service.groupByServerToAllBuckets(testRegion, true);
    if (servers == null) {
      return false;
    } else if (servers.size() == 4) {
      logger.debug("The client metadata contains the following "
          + servers.size() + " servers for region " + testRegion.getFullPath() + ":");
      for (Map.Entry entry : servers.entrySet()) {
        logger.debug(entry.getKey() + "->" + entry.getValue());
      }
      if (servers.size() < 4) {
        logger
            .info("Servers size is " + servers.size() + " less than expected 4.");
        return false;
      }
    }
    return true;
  }

  private static void verifyMetadata(Map<Integer, List<BucketServerLocation66>> clientMap) {
    final PartitionedRegion pr = (PartitionedRegion) testRegion;
    ConcurrentHashMap<Integer, Set<ServerBucketProfile>> serverMap =
        pr.getRegionAdvisor().getAllClientBucketProfilesTest();
    assertThat(serverMap.size()).isEqualTo(clientMap.size());
    assertThat(clientMap.keySet().containsAll(serverMap.keySet())).isTrue();
    for (Map.Entry<Integer, List<BucketServerLocation66>> entry : clientMap.entrySet()) {
      int bucketId = entry.getKey();
      List<BucketServerLocation66> list = entry.getValue();
      BucketServerLocation66 primaryBSL = null;
      int primaryCnt = 0;
      for (BucketServerLocation66 bsl : list) {
        if (bsl.isPrimary()) {
          primaryBSL = bsl;
          primaryCnt++;
        }
      }
      assertThat(primaryCnt).isEqualTo(1);
      Set<ServerBucketProfile> set = serverMap.get(bucketId);
      assertThat(set.size()).isEqualTo(list.size());
      primaryCnt = 0;
      for (ServerBucketProfile bp : set) {
        ServerLocation sl = (ServerLocation) bp.bucketServerLocations.toArray()[0];
        assertThat(list.contains(sl)).isTrue();
        // should be only one primary
        if (bp.isPrimary) {
          primaryCnt++;
          assertThat(sl).isEqualTo(primaryBSL);
        }
      }
      assertThat(primaryCnt).isEqualTo(1);
    }
  }

  private static void waitForLocalBucketsCreation(final int numBuckets) {
    final PartitionedRegion pr = (PartitionedRegion) testRegion;

    await().alias("bucket copies are not created, the total number of buckets expected are "
        + numBuckets + " but the total num of buckets are "
        + pr.getDataStore().getAllLocalBuckets().size())
        .until(() -> pr.getDataStore().getAllLocalBuckets().size() == numBuckets);
  }

  private void verifyDeadServer(Map<String, ClientPartitionAdvisor> regionMetaData, Region region,
      int port0, int port1) {

    ServerLocation sl0 = new ServerLocation("localhost", port0);
    ServerLocation sl1 = new ServerLocation("localhost", port1);

    final ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());

    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      List servers = (List) entry.getValue();
      assertFalse(servers.contains(sl0));
      assertFalse(servers.contains(sl1));
    }
  }

  private static void createClientWithoutPRSingleHopEnabled(int port0) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    PartitionedRegionSingleHopDUnitTest test = new PartitionedRegionSingleHopDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertThat(cache).isNotNull();
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port0).setPingInterval(250)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10).setRetryAttempts(3)
          .setPRSingleHopEnabled(false).create(PR_NAME);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    createRegionsInClientCache(p.getName());
  }

  private static int createAccessorServer() {
    final int redundantCopies = 1;
    final int totalNoofBuckets = 4;
    final int localMaxMemory = 0;
    PartitionedRegionSingleHopDUnitTest test = new PartitionedRegionSingleHopDUnitTest();
    cache = test.getCache();
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      Assertions.fail("Failed to start server ", e);
    }

    createBasicPartitionedRegion(redundantCopies, totalNoofBuckets, localMaxMemory, PR_NAME);

    customerRegion =
        createColocatedPersistentRegionForTest(redundantCopies, totalNoofBuckets, CUSTOMER, null,
            localMaxMemory);

    orderRegion =
        createColocatedPersistentRegionForTest(redundantCopies, totalNoofBuckets, ORDER, CUSTOMER,
            localMaxMemory);

    orderRegion =
        createColocatedPersistentRegionForTest(redundantCopies, totalNoofBuckets, SHIPMENT, ORDER,
            localMaxMemory);

    replicatedRegion = cache.createRegion("rr", new AttributesFactory<>().create());
    return port;
  }

  private static <K, V> Region<K, V> createBasicPartitionedRegion(int redundantCopies,
      int totalNoofBuckets,
      int localMaxMemory,
      final String regionName) {
    PartitionAttributesFactory<K, V> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNoofBuckets)
        .setLocalMaxMemory(localMaxMemory);
    RegionFactory<K, V> regionFactory = cache.createRegionFactory();

    regionFactory.setPartitionAttributes(paf.create());
    regionFactory.setConcurrencyChecksEnabled(true);

    Region<K, V> region = regionFactory.create(regionName);
    assertThat(region).isNotNull();
    logger
        .info("Partitioned Region " + regionName + " created Successfully :" + region.toString());
    return region;
  }

  private static <K, V> Region<K, V> createColocatedRegion(String regionName, int redundantCopies,
      int totalNoofBuckets,
      String colocatedRegionName) {

    PartitionAttributesFactory<K, V> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNoofBuckets)
        .setPartitionResolver(new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));

    if (colocatedRegionName != null) {
      paf.setColocatedWith(colocatedRegionName);
    }

    RegionFactory<K, V> regionFactory = cache.createRegionFactory();
    regionFactory.setPartitionAttributes(paf.create());
    regionFactory.setConcurrencyChecksEnabled(true);
    Region<K, V> region = regionFactory.create(regionName);
    assertThat(region).isNotNull();
    logger.info("Partitioned Region " + regionName + " created Successfully :" + region.toString());
    return region;
  }

  private static int createServer(int redundantCopies, int totalNoofBuckets) {
    PartitionedRegionSingleHopDUnitTest test = new PartitionedRegionSingleHopDUnitTest();
    cache = test.getCache();
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      Assertions.fail("Failed to start server ", e);
    }

    testRegion = createBasicPartitionedRegion(redundantCopies, totalNoofBuckets, -1, PR_NAME);

    // creating colocated Regions
    customerRegion = createColocatedRegion(CUSTOMER, redundantCopies, totalNoofBuckets, null);

    orderRegion = createColocatedRegion(ORDER, redundantCopies, totalNoofBuckets, CUSTOMER);

    shipmentRegion = createColocatedRegion(ORDER, redundantCopies, totalNoofBuckets, CUSTOMER);

    replicatedRegion = cache.createRegion("rr", new AttributesFactory<>().create());

    return port;
  }

  private static int createPersistentPrsAndServer(int redundantCopies, int totalNoofBuckets) {
    PartitionedRegionSingleHopDUnitTest test = new PartitionedRegionSingleHopDUnitTest();
    cache = test.getCache();
    DiskStore disk = cache.findDiskStore("disk");
    if (disk == null) {
      disk = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
    }
    testRegion = createBasicPersistentPartitionRegion(redundantCopies, totalNoofBuckets, PR_NAME);

    // creating colocated Regions

    customerRegion =
        createColocatedPersistentRegionForTest(redundantCopies, totalNoofBuckets, CUSTOMER, null,
            -1);

    orderRegion =
        createColocatedPersistentRegionForTest(redundantCopies, totalNoofBuckets, ORDER, CUSTOMER,
            -1);

    orderRegion =
        createColocatedPersistentRegionForTest(redundantCopies, totalNoofBuckets, SHIPMENT, ORDER,
            -1);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    replicatedRegion = regionFactory.create("rr");
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      Assertions.fail("Failed to start server ", e);
    }
    return port;
  }

  private static <K, V> Region<K, V> createBasicPersistentPartitionRegion(int redundantCopies,
      int totalNoofBuckets,
      final String regionName) {
    PartitionAttributesFactory<K, V> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNoofBuckets);

    RegionFactory<K, V> regionFactory = cache.createRegionFactory();
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    regionFactory.setDiskStoreName("disk");
    regionFactory.setPartitionAttributes(paf.create());

    Region<K, V> region = regionFactory.create(regionName);
    assertThat(region).isNotNull();
    logger
        .info("Partitioned Region " + regionName + " created Successfully :" + region.toString());
    return region;
  }

  private static <K, V> Region<K, V> createColocatedPersistentRegionForTest(int redundantCopies,
      int totalNoofBuckets,
      final String regionName,
      String colocatedRegionName,
      int localMaxMemory) {
    PartitionAttributesFactory<K, V> paf = new PartitionAttributesFactory<>();

    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNoofBuckets)
        .setPartitionResolver(new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));

    if (localMaxMemory > -1) {
      paf.setLocalMaxMemory(localMaxMemory);
    }

    if (colocatedRegionName != null) {
      paf.setColocatedWith(colocatedRegionName);
    }

    RegionFactory<K, V> regionFactory = cache.createRegionFactory();

    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    regionFactory.setDiskStoreName("disk");
    regionFactory.setPartitionAttributes(paf.create());
    Region<K, V> region = regionFactory.create(regionName);

    assertThat(region).isNotNull();
    logger.info("Partitioned Region " + regionName + " created Successfully :" + region.toString());
    return region;
  }

  private static int createPersistentPrsAndServerOnPort(int redundantCopies, int totalNoofBuckets,
      int port) {
    PartitionedRegionSingleHopDUnitTest test = new PartitionedRegionSingleHopDUnitTest();
    cache = test.getCache();
    DiskStore disk = cache.findDiskStore("disk");
    if (disk == null) {
      disk = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
    }
    testRegion = createBasicPersistentPartitionRegion(redundantCopies, totalNoofBuckets, PR_NAME);

    // creating colocated Regions
    customerRegion =
        createColocatedPersistentRegionForTest(redundantCopies, totalNoofBuckets, CUSTOMER, null,
            -1);

    orderRegion =
        createColocatedPersistentRegionForTest(redundantCopies, totalNoofBuckets, ORDER, CUSTOMER,
            -1);

    orderRegion =
        createColocatedPersistentRegionForTest(redundantCopies, totalNoofBuckets, SHIPMENT, ORDER,
            -1);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    replicatedRegion = regionFactory.create("rr");
    CacheServer server = cache.addCacheServer();
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      Assertions.fail("Failed to start server ", e);
    }
    return port;
  }

  private static void startServerOnPort(int port) {
    PartitionedRegionSingleHopDUnitTest test = new PartitionedRegionSingleHopDUnitTest();
    cache = test.getCache();
    CacheServer server = cache.addCacheServer();
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      Assertions.fail("Failed to start server ", e);
    }
  }

  private static void createPeer() {
    int redundantCopies = 1;
    int totalNoofBuckets = 4;
    PartitionedRegionSingleHopDUnitTest test = new PartitionedRegionSingleHopDUnitTest();
    cache = test.getCache();

    testRegion = createBasicPartitionedRegion(redundantCopies, totalNoofBuckets, -1, PR_NAME);

    customerRegion =
        createColocatedPersistentRegionForTest(redundantCopies, totalNoofBuckets, CUSTOMER, null,
            -1);

    orderRegion =
        createColocatedPersistentRegionForTest(redundantCopies, totalNoofBuckets, ORDER, CUSTOMER,
            -1);

    customerRegion =
        createColocatedPersistentRegionForTest(redundantCopies, totalNoofBuckets, SHIPMENT, ORDER,
            -1);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    replicatedRegion = regionFactory.create("rr");
  }

  private static void createClient(int port0) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    PartitionedRegionSingleHopDUnitTest test = new PartitionedRegionSingleHopDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertThat(cache).isNotNull();
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port0).setPingInterval(250)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10).setRetryAttempts(3)
          .create(PR_NAME);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    createRegionsInClientCache(p.getName());
  }

  private static void createClient(int port0, int port1) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    PartitionedRegionSingleHopDUnitTest test = new PartitionedRegionSingleHopDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertThat(cache).isNotNull();
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port0).addServer("localhost", port1)
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(PR_NAME);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    createRegionsInClientCache(p.getName());
  }

  private static void createClientWithLocator(String host, int port0) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    PartitionedRegionSingleHopDUnitTest test = new PartitionedRegionSingleHopDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertThat(cache).isNotNull();
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addLocator(host, port0).setPingInterval(250)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10).setRetryAttempts(3)
          .create(PR_NAME);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    createRegionsInClientCache(p.getName());
  }

  private static void createClient(int port0, int port1, int port2, int port3) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    PartitionedRegionSingleHopDUnitTest test = new PartitionedRegionSingleHopDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertThat(cache).isNotNull();
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port0).addServer("localhost", port1)
          .addServer("localhost", port2).addServer("localhost", port3).setPingInterval(100)
          .setSubscriptionEnabled(false).setReadTimeout(2000).setSocketBufferSize(1000)
          .setMinConnections(6).setMaxConnections(10).setRetryAttempts(3).create(PR_NAME);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    createRegionsInClientCache(p.getName());
  }

  private static void createRegionsInClientCache(String poolName) {
    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    regionFactory.setPoolName(poolName);
    regionFactory.setDataPolicy(DataPolicy.EMPTY);
    testRegion = regionFactory.create(PR_NAME);
    assertThat(testRegion).isNotNull();
    logger
        .info("Distributed Region " + PR_NAME + " created Successfully :" + testRegion.toString());

    regionFactory = cache.createRegionFactory();
    customerRegion = regionFactory.create(CUSTOMER);
    assertThat(customerRegion).isNotNull();
    logger
        .info("Distributed Region CUSTOMER created Successfully :" + customerRegion.toString());

    orderRegion = regionFactory.create(ORDER);
    assertThat(orderRegion).isNotNull();
    logger
        .info("Distributed Region ORDER created Successfully :" + orderRegion.toString());

    shipmentRegion = regionFactory.create(SHIPMENT);
    assertThat(shipmentRegion).isNotNull();
    logger
        .info("Distributed Region SHIPMENT created Successfully :" + shipmentRegion.toString());
    replicatedRegion = regionFactory.create("rr");
  }

  private static void putIntoPartitionedRegions() {
    for (int i = 0; i <= 3; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      customerRegion.put(custid, customer);
      for (int j = 1; j <= 10; j++) {
        int oid = (i * 10) + j;
        OrderId orderId = new OrderId(oid, custid);
        Order order = new Order("OREDR" + oid);
        orderRegion.put(orderId, order);
        for (int k = 1; k <= 10; k++) {
          int sid = (oid * 10) + k;
          ShipmentId shipmentId = new ShipmentId(sid, orderId);
          Shipment shipment = new Shipment("Shipment" + sid);
          shipmentRegion.put(shipmentId, shipment);
        }
      }
    }

    testRegion.put(0, "create0");
    testRegion.put(1, "create1");
    testRegion.put(2, "create2");
    testRegion.put(3, "create3");

    testRegion.put(0, "update0");
    testRegion.put(1, "update1");
    testRegion.put(2, "update2");
    testRegion.put(3, "update3");

    testRegion.put(0, "update00");
    testRegion.put(1, "update11");
    testRegion.put(2, "update22");
    testRegion.put(3, "update33");
    Map<Object, Object> map = new HashMap<>();
    map.put(1, 1);
    replicatedRegion.putAll(map);
  }

  static class MyFunctionAdapter extends FunctionAdapter implements DataSerializable {
    MyFunctionAdapter() {}

    @Override
    public String getId() {
      return "fid";
    }

    @Override
    public void execute(FunctionContext context) {
      System.out.println("YOGS function called");
      RegionFunctionContext rc = (RegionFunctionContext) context;
      Region<Object, Object> r = rc.getDataSet();
      Set filter = rc.getFilter();
      if (rc.getFilter() == null) {
        for (int i = 0; i < 200; i++) {
          r.put(i, i);
        }
      } else {
        for (Object key : filter) {
          r.put(key, key);
        }
      }
      context.getResultSender().lastResult(Boolean.TRUE);
    }

    @Override
    public boolean isHA() {
      return false;
    }

    @Override
    public boolean optimizeForWrite() {
      return true;
    }

    @Override
    public void toData(DataOutput out) {}

    @Override
    public void fromData(DataInput in) {

    }
  }

  private static void executeFunctions() {
    Set<Object> filter = new HashSet<>();
    filter.add(0);
    FunctionService.onRegion(testRegion).withFilter(filter).execute(new MyFunctionAdapter())
        .getResult();
    filter.add(1);
    FunctionService.onRegion(testRegion).withFilter(filter).execute(new MyFunctionAdapter())
        .getResult();
    filter.add(2);
    filter.add(3);
    FunctionService.onRegion(testRegion).withFilter(filter).execute(new MyFunctionAdapter())
        .getResult();
    FunctionService.onRegion(testRegion).execute(new MyFunctionAdapter()).getResult();
  }

  private static void putAll() {
    Map<Object, Object> map = new HashMap<>();
    map.put(0, 0);
    map.put(1, 1);
    map.put(2, 2);
    map.put(3, 3);
    testRegion.putAll(map, "putAllCallback");
    testRegion.putAll(map);
    testRegion.putAll(map);
    testRegion.putAll(map);
  }

  private static void put() {
    testRegion.put(0, "create0");
    testRegion.put(1, "create1");
    testRegion.put(2, "create2");
    testRegion.put(3, "create3");
    for (int i = 0; i < 40; i++) {
      testRegion.put(i, "create" + i);
    }
  }

  private static void getFromPartitionedRegions() {
    for (int i = 0; i <= 3; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      customerRegion.get(custid, customer);
      for (int j = 1; j <= 10; j++) {
        int oid = (i * 10) + j;
        OrderId orderId = new OrderId(oid, custid);
        Order order = new Order("OREDR" + oid);
        orderRegion.get(orderId, order);
        for (int k = 1; k <= 10; k++) {
          int sid = (oid * 10) + k;
          ShipmentId shipmentId = new ShipmentId(sid, orderId);
          Shipment shipment = new Shipment("Shipment" + sid);
          shipmentRegion.get(shipmentId, shipment);
        }
      }
    }

    testRegion.get(0, "create0");
    testRegion.get(1, "create1");
    testRegion.get(2, "create2");
    testRegion.get(3, "create3");

    testRegion.get(0, "update0");
    testRegion.get(1, "update1");
    testRegion.get(2, "update2");
    testRegion.get(3, "update3");

    testRegion.get(0, "update00");
    testRegion.get(1, "update11");
    testRegion.get(2, "update22");
    testRegion.get(3, "update33");
  }

  private static void putIntoSinglePR() {
    testRegion.put(0, "create0");
    testRegion.put(1, "create1");
    testRegion.put(2, "create2");
    testRegion.put(3, "create3");
  }

  private static void updateIntoSinglePR() {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();

    cms.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.put(0, "update0");
    assertFalse(cms.isRefreshMetadataTestOnly());

    testRegion.put(1, "update1");
    assertFalse(cms.isRefreshMetadataTestOnly());

    testRegion.put(2, "update2");
    assertFalse(cms.isRefreshMetadataTestOnly());

    testRegion.put(3, "update3");
    assertFalse(cms.isRefreshMetadataTestOnly());

    testRegion.put(0, "update00");
    assertFalse(cms.isRefreshMetadataTestOnly());

    testRegion.put(1, "update11");
    assertFalse(cms.isRefreshMetadataTestOnly());

    testRegion.put(2, "update22");
    assertFalse(cms.isRefreshMetadataTestOnly());

    testRegion.put(3, "update33");
    assertFalse(cms.isRefreshMetadataTestOnly());
  }

  private static void verifyEmptyMetadata() {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    assertThat(cms.getClientPRMetadata_TEST_ONLY().isEmpty()).isTrue();
  }

  private static void verifyEmptyStaticData() {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    assertThat(cms.getClientPartitionAttributesMap().isEmpty()).isTrue();
  }

  private static void printMetadata() {
    if (cache != null) {
      ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
      cache.getLogger().info("Metadata is " + cms.getClientPRMetadata_TEST_ONLY());
    }
  }

  private static void printView() {
    PartitionedRegion pr = (PartitionedRegion) testRegion;
    if (pr.cache != null) {
      cache.getLogger().info("Primary Bucket view of server  "
          + pr.getDataStore().getLocalPrimaryBucketsListTestOnly());
      cache.getLogger().info("Secondary Bucket view of server  "
          + pr.getDataStore().getLocalNonPrimaryBucketsListTestOnly());
    }
  }

  private void verifyMetadata() {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    // make sure all fetch tasks are completed
    await()
        .until(() -> cms.getRefreshTaskCount_TEST_ONLY() == 0);
  }
}


class Customer implements DataSerializable { // TODO: move this to be an inner class and make it
  // static
  private String name;

  private String address;

  public Customer(String name, String address) {
    this.name = name;
    this.address = address;
  }

  @Override
  public void fromData(DataInput in) throws IOException {
    this.name = DataSerializer.readString(in);
    this.address = DataSerializer.readString(in);

  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.name, out);
    DataSerializer.writeString(this.address, out);
  }

  @Override
  public String toString() {
    return "Customer { name=" + this.name + " address=" + this.address + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof Customer)) {
      return false;
    }

    Customer cust = (Customer) o;
    return (cust.name.equals(name) && cust.address.equals(address));
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, address);
  }
}


class Order implements DataSerializable {
  private String orderName;

  public Order(String orderName) {
    this.orderName = orderName;
  }

  @Override
  public void fromData(DataInput in) throws IOException {
    this.orderName = DataSerializer.readString(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.orderName, out);
  }

  @Override
  public String toString() {
    return this.orderName;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof Order) {
      Order other = (Order) obj;
      return other.orderName != null && other.orderName.equals(this.orderName);
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


class Shipment implements DataSerializable {
  private String shipmentName;

  public Shipment(String shipmentName) {
    this.shipmentName = shipmentName;
  }

  @Override
  public void fromData(DataInput in) throws IOException {
    this.shipmentName = DataSerializer.readString(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.shipmentName, out);
  }

  @Override
  public String toString() {
    return this.shipmentName;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof Shipment) {
      Shipment other = (Shipment) obj;
      return other.shipmentName != null && other.shipmentName.equals(this.shipmentName);
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
