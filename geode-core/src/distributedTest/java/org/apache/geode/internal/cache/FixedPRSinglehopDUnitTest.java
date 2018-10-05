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

import static java.lang.System.out;
import static java.util.Map.Entry;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.FixedPartitionAttributes;
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
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.partitioned.fixed.QuarterPartitionResolver;
import org.apache.geode.internal.cache.partitioned.fixed.SingleHopQuarterPartitionResolver;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class FixedPRSinglehopDUnitTest extends JUnit4CacheTestCase {

  private static final String PR_NAME = "fixed_single_hop_pr";

  private static Cache cache = null;

  private static Locator locator = null;

  private static Region region = null;

  private static final Date q1dateJan1 = new Date(2010, 0, 1);

  private static final Date q1dateFeb1 = new Date(2010, 1, 1);

  private static final Date q1dateMar1 = new Date(2010, 2, 1);

  private static final Date q2dateApr1 = new Date(2010, 3, 1);

  private static final Date q2dateMay1 = new Date(2010, 4, 1);

  private static final Date q2dateJun1 = new Date(2010, 5, 1);

  private static final Date q3dateJuly1 = new Date(2010, 6, 1);

  private static final Date q3dateAug1 = new Date(2010, 7, 1);

  private static final Date q3dateSep1 = new Date(2010, 8, 1);

  private static final Date q4dateOct1 = new Date(2010, 9, 1);

  private static final Date q4dateNov1 = new Date(2010, 10, 1);

  private static final Date q4dateDec1 = new Date(2010, 11, 1);

  @Test
  public void testNoClientConnected() {
    final Host host = Host.getHost(0);
    VM accessorServer = host.getVM(0);
    VM datastoreServer = host.getVM(1);
    VM peer1 = host.getVM(2);
    VM peer2 = host.getVM(3);

    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", 3));

    datastoreServer.invoke(() -> FixedPRSinglehopDUnitTest.createServer(false, fpaList));

    fpaList.clear();
    accessorServer.invoke(() -> FixedPRSinglehopDUnitTest.createServer(true, fpaList));

    fpaList.clear();
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q4", 3));

    peer1.invoke(() -> FixedPRSinglehopDUnitTest.createPeer(false, fpaList));

    fpaList.clear();
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q4", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", 3));

    peer2.invoke(() -> FixedPRSinglehopDUnitTest.createPeer(false, fpaList));

    datastoreServer.invoke(() -> FixedPRSinglehopDUnitTest.putIntoPartitionedRegions());
    accessorServer.invoke(() -> FixedPRSinglehopDUnitTest.putIntoPartitionedRegions());
    peer1.invoke(() -> FixedPRSinglehopDUnitTest.putIntoPartitionedRegions());
    peer2.invoke(() -> FixedPRSinglehopDUnitTest.putIntoPartitionedRegions());

    datastoreServer.invoke(() -> FixedPRSinglehopDUnitTest.getFromPartitionedRegions());
    accessorServer.invoke(() -> FixedPRSinglehopDUnitTest.getFromPartitionedRegions());
    peer1.invoke(() -> FixedPRSinglehopDUnitTest.getFromPartitionedRegions());
    peer2.invoke(() -> FixedPRSinglehopDUnitTest.getFromPartitionedRegions());

    datastoreServer.invoke(() -> FixedPRSinglehopDUnitTest.verifyEmptyMetadata());
    accessorServer.invoke(() -> FixedPRSinglehopDUnitTest.verifyEmptyMetadata());
    peer1.invoke(() -> FixedPRSinglehopDUnitTest.verifyEmptyMetadata());
    peer2.invoke(() -> FixedPRSinglehopDUnitTest.verifyEmptyMetadata());

    datastoreServer.invoke(() -> FixedPRSinglehopDUnitTest.verifyEmptyStaticData());
    accessorServer.invoke(() -> FixedPRSinglehopDUnitTest.verifyEmptyStaticData());
    peer1.invoke(() -> FixedPRSinglehopDUnitTest.verifyEmptyStaticData());
    peer2.invoke(() -> FixedPRSinglehopDUnitTest.verifyEmptyStaticData());
  }

  // 2 AccessorServers, 2 Peers
  // 1 Client connected to 2 AccessorServers. Hence metadata should not be
  // fetched.
  @Test
  public void testClientConnectedToAccessors() {
    final Host host = Host.getHost(0);
    VM accessorServer1 = host.getVM(0);
    VM accessorServer2 = host.getVM(1);
    VM peer1 = host.getVM(2);
    VM peer2 = host.getVM(3);

    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();

    Integer port0 = (Integer) accessorServer1
        .invoke(() -> FixedPRSinglehopDUnitTest.createServer(true, fpaList));

    Integer port1 = (Integer) accessorServer2
        .invoke(() -> FixedPRSinglehopDUnitTest.createServer(true, fpaList));

    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q4", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", true, 3));

    peer1.invoke(() -> FixedPRSinglehopDUnitTest.createPeer(false, fpaList));
    fpaList.clear();
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", true, 3));

    peer2.invoke(() -> FixedPRSinglehopDUnitTest.createPeer(false, fpaList));

    createClient(port0, port1);

    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    verifyEmptyMetadata();

    verifyEmptyStaticData();
  }

  // 4 servers, 1 client connected to all 4 servers.
  // Put data, get data and make the metadata stable.
  // Now verify that metadata has all 8 buckets info.
  // Now update and ensure the fetch service is never called.
  @Test
  public void test_MetadataContents() {

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    VM server4 = host.getVM(3);

    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();

    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", false, 3));

    Integer port1 =
        (Integer) server1.invoke(() -> FixedPRSinglehopDUnitTest.createServer(false, fpaList));
    fpaList.clear();
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q4", false, 3));

    Integer port2 =
        (Integer) server2.invoke(() -> FixedPRSinglehopDUnitTest.createServer(false, fpaList));
    fpaList.clear();
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", false, 3));

    Integer port3 =
        (Integer) server3.invoke(() -> FixedPRSinglehopDUnitTest.createServer(false, fpaList));
    fpaList.clear();
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q4", true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", false, 3));

    Integer port4 =
        (Integer) server4.invoke(() -> FixedPRSinglehopDUnitTest.createServer(false, fpaList));

    createClient(port1, port2, port3, port4);

    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    SerializableRunnableIF printView = () -> FixedPRSinglehopDUnitTest.printView();
    server1.invoke(printView);
    server2.invoke(printView);
    server3.invoke(printView);
    server4.invoke(printView);

    int totalBucketOnServer = 0;
    SerializableCallableIF<Integer> getBucketCount =
        () -> FixedPRSinglehopDUnitTest.primaryBucketsOnServer();
    totalBucketOnServer += server1.invoke(getBucketCount);
    totalBucketOnServer += server2.invoke(getBucketCount);
    totalBucketOnServer += server3.invoke(getBucketCount);
    totalBucketOnServer += server4.invoke(getBucketCount);

    verifyMetadata(totalBucketOnServer, 2);
    updateIntoSinglePR(true /* assert no refresh */);
  }

  /**
   * This test will check to see if all the partitionAttributes are sent to the client. In case one
   * partition comes late, we should fetch that when there is a network hop because of that
   * partitioned region. This test will create 3 servers with partition. Do some operations on them.
   * Validate that the metadata are fetched and then later up one more partition and do some
   * operations on them. It should fetch new fpa. Verify that the correct servers are known to the
   * client metadata service at the end.
   */
  @Test
  public void testMetadataInClientWithFixedPartitions() throws Exception {

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    VM server4 = host.getVM(3);
    Boolean simpleFPR = false;
    final int portLocator = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String hostLocator = NetworkUtils.getServerHostName(server1.getHost());
    final String locator = hostLocator + "[" + portLocator + "]";
    server3.invoke(() -> FixedPRSinglehopDUnitTest.startLocatorInVM(portLocator));
    try {

      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();

      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", true, 3));
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", false, 3));

      Integer port1 = (Integer) server1.invoke(() -> FixedPRSinglehopDUnitTest
          .createServerWithLocator(locator, false, fpaList, simpleFPR));
      fpaList.clear();
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", true, 3));

      Integer port2 = (Integer) server2.invoke(() -> FixedPRSinglehopDUnitTest
          .createServerWithLocator(locator, false, fpaList, simpleFPR));
      fpaList.clear();

      createClientWithLocator(hostLocator, portLocator);

      putIntoPartitionedRegionsThreeQs();

      getFromPartitionedRegionsFor3Qs();
      // Server 1 is actually primary for both Q1 and Q2, since there is no FPA server with
      // primary set to true.
      await()
          .until(() -> (server1.invoke(FixedPRSinglehopDUnitTest::primaryBucketsOnServer) == 6)
              && (server2.invoke(FixedPRSinglehopDUnitTest::primaryBucketsOnServer) == 3));

      // TODO: Verify that all the fpa's are in the map
      server1.invoke(FixedPRSinglehopDUnitTest::printView);
      server2.invoke(FixedPRSinglehopDUnitTest::printView);

      int totalBucketOnServer = 0;
      totalBucketOnServer +=
          (Integer) server1.invoke(FixedPRSinglehopDUnitTest::primaryBucketsOnServer);
      totalBucketOnServer +=
          (Integer) server2.invoke(FixedPRSinglehopDUnitTest::primaryBucketsOnServer);

      verifyMetadata(totalBucketOnServer, 1);
      updateIntoSinglePRFor3Qs();

      // now create one more partition
      fpaList.clear();
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q4", true, 3));
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q2", true, 3));
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q1", false, 3));
      fpaList.add(FixedPartitionAttributes.createFixedPartition("Q3", false, 3));

      Integer port4 = (Integer) server4.invoke(() -> FixedPRSinglehopDUnitTest
          .createServerWithLocator(locator, false, fpaList, simpleFPR));

      putIntoPartitionedRegions();
      // Wait to make sure that the buckets have actually moved.
      await()
          .until(() -> (server1.invoke(FixedPRSinglehopDUnitTest::primaryBucketsOnServer) == 3)
              && (server2.invoke(FixedPRSinglehopDUnitTest::primaryBucketsOnServer) == 3)
              && (server4.invoke(FixedPRSinglehopDUnitTest::primaryBucketsOnServer) == 6));

      getFromPartitionedRegions();

      server1.invoke(FixedPRSinglehopDUnitTest::printView);
      server2.invoke(FixedPRSinglehopDUnitTest::printView);
      server4.invoke(FixedPRSinglehopDUnitTest::printView);

      updateIntoSinglePR(false);

      ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
      ClientPartitionAdvisor advisor = cms.getClientPartitionAdvisor("/" + PR_NAME);
      int[] expected = new int[] {port1, port1, port1, port4, port4, port4, port2, port2, port2,
          port4, port4, port4};
      for (int i = 0; i < expected.length; i++) {
        ServerLocation primary = advisor.advisePrimaryServerLocation(i);
        assertNotNull("bucket " + i + " had no primary server", primary);
        assertEquals("bucket " + i + " was incorrect", expected[i], primary.getPort());
      }
    } finally {
      server3.invoke(FixedPRSinglehopDUnitTest::stopLocator);
    }
  }

  public int getServerPort() {
    return GemFireCacheImpl.getInstance().getCacheServers().get(0).getPort();
  }

  public static int createServer(boolean isAccessor, List<FixedPartitionAttributes> fpaList) {

    FixedPRSinglehopDUnitTest test = new FixedPRSinglehopDUnitTest();
    cache = test.getCache();
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      Assert.fail("Failed to start server ", e);
    }

    if (!fpaList.isEmpty() || isAccessor) {
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(1).setTotalNumBuckets(12);
      if (isAccessor) {
        paf.setLocalMaxMemory(0);
      }
      for (FixedPartitionAttributes fpa : fpaList) {
        paf.addFixedPartitionAttributes(fpa);
      }
      paf.setPartitionResolver(new SingleHopQuarterPartitionResolver());

      AttributesFactory attr = new AttributesFactory();
      attr.setPartitionAttributes(paf.create());
      region = cache.createRegion(PR_NAME, attr.create());
      assertNotNull(region);
      LogWriterUtils.getLogWriter()
          .info("Partitioned Region " + PR_NAME + " created Successfully :" + region.toString());
    }
    return port;
  }


  public static int createServerWithLocator(String locator, boolean isAccessor,
      List<FixedPartitionAttributes> fpaList, boolean simpleFPR) {

    FixedPRSinglehopDUnitTest test = new FixedPRSinglehopDUnitTest();
    Properties props = new Properties();
    props = new Properties();
    props.setProperty(LOCATORS, locator);
    DistributedSystem ds = test.getSystem(props);
    cache = new CacheFactory(props).create(ds);

    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      Assert.fail("Failed to start server ", e);
    }

    if (!fpaList.isEmpty() || isAccessor) {
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(1).setTotalNumBuckets(12);
      if (isAccessor) {
        paf.setLocalMaxMemory(0);
      }
      for (FixedPartitionAttributes fpa : fpaList) {
        paf.addFixedPartitionAttributes(fpa);
      }
      // paf.setPartitionResolver(new SingleHopQuarterPartitionResolver());
      paf.setPartitionResolver(new QuarterPartitionResolver());

      AttributesFactory attr = new AttributesFactory();
      attr.setPartitionAttributes(paf.create());
      region = cache.createRegion(PR_NAME, attr.create());
      assertNotNull(region);
      LogWriterUtils.getLogWriter()
          .info("Partitioned Region " + PR_NAME + " created Successfully :" + region.toString());
    }
    return port;
  }

  public static void startLocatorInVM(final int locatorPort) {

    File logFile = new File("");

    Properties props = new Properties();
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
    try {
      locator = Locator.startLocatorAndDS(locatorPort, logFile, null, props);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static void stopLocator() {
    locator.stop();
  }

  public static int primaryBucketsOnServer() {
    FixedPRSinglehopDUnitTest test = new FixedPRSinglehopDUnitTest();
    PartitionedRegion pr = (PartitionedRegion) cache.getRegion(PR_NAME);
    assertNotNull(pr);
    return pr.getLocalPrimaryBucketsListTestOnly().size();
  }

  public static void createPeer(boolean isAccessor, List<FixedPartitionAttributes> fpaList) {
    FixedPRSinglehopDUnitTest test = new FixedPRSinglehopDUnitTest();
    cache = test.getCache();

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1).setTotalNumBuckets(12);
    if (isAccessor) {
      paf.setLocalMaxMemory(0);
    }
    for (FixedPartitionAttributes fpa : fpaList) {
      paf.addFixedPartitionAttributes(fpa);
    }
    paf.setPartitionResolver(new SingleHopQuarterPartitionResolver());

    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    region = cache.createRegion(PR_NAME, attr.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter()
        .info("Partitioned Region " + PR_NAME + " created Successfully :" + region.toString());

  }

  public static void createClient(int port0) {
    Properties props = new Properties();
    props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    FixedPRSinglehopDUnitTest test = new FixedPRSinglehopDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
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

  public static void createClient(int port0, int port1) {
    Properties props = new Properties();
    props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    FixedPRSinglehopDUnitTest test = new FixedPRSinglehopDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
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

  public static void createClientWithLocator(String host, int port0) {
    Properties props = new Properties();
    props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    FixedPRSinglehopDUnitTest test = new FixedPRSinglehopDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
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

  public static void createClient(int port0, int port1, int port2, int port3) {
    Properties props = new Properties();
    props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    FixedPRSinglehopDUnitTest test = new FixedPRSinglehopDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port0).addServer("localhost", port1)
          .addServer("localhost", port2).addServer("localhost", port3).setPingInterval(250)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10).setRetryAttempts(3)
          .create(PR_NAME);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    createRegionsInClientCache(p.getName());
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
  }

  public static void putIntoPartitionedRegions() {
    region.put(q1dateJan1, "create0");
    region.put(q2dateApr1, "create1");
    region.put(q3dateJuly1, "create2");
    region.put(q4dateOct1, "create3");
    region.put(q1dateFeb1, "create4");
    region.put(q2dateMay1, "create5");
    region.put(q3dateAug1, "create6");
    region.put(q4dateNov1, "create7");
    region.put(q1dateMar1, "create8");
    region.put(q2dateJun1, "create9");
    region.put(q3dateSep1, "create10");
    region.put(q4dateDec1, "create11");

    region.put(q1dateJan1, "update0");
    region.put(q2dateApr1, "update1");
    region.put(q3dateJuly1, "update2");
    region.put(q4dateOct1, "update3");
    region.put(q1dateFeb1, "update4");
    region.put(q2dateMay1, "update5");
    region.put(q3dateAug1, "update6");
    region.put(q4dateNov1, "update7");
    region.put(q1dateMar1, "update8");
    region.put(q2dateJun1, "update9");
    region.put(q3dateSep1, "update10");
    region.put(q4dateDec1, "update11");


    region.put(q1dateJan1, "update00");
    region.put(q2dateApr1, "update11");
    region.put(q3dateJuly1, "update22");
    region.put(q4dateOct1, "update33");
    region.put(q1dateFeb1, "update44");
    region.put(q2dateMay1, "update55");
    region.put(q3dateAug1, "update66");
    region.put(q4dateNov1, "update77");
    region.put(q1dateMar1, "update88");
    region.put(q2dateJun1, "update99");
    region.put(q3dateSep1, "update1010");
    region.put(q4dateDec1, "update1111");

    region.put(q1dateJan1, "update000");
    region.put(q1dateFeb1, "update444");
    region.put(q1dateMar1, "update888");
    region.put(q2dateApr1, "update111");
    region.put(q2dateMay1, "update555");
    region.put(q2dateJun1, "update999");
    region.put(q1dateJan1, "update0000");
    region.put(q3dateJuly1, "update222");
    region.put(q3dateAug1, "update666");
    region.put(q3dateSep1, "update101010");
    region.put(q1dateJan1, "update00000");
    region.put(q4dateOct1, "update333");
    region.put(q4dateNov1, "update777");
    region.put(q4dateDec1, "update111111");
    region.put(q1dateJan1, "update000000");

  }

  public static void putIntoPartitionedRegionsThreeQs() {
    region.put(q1dateJan1, "create0");
    region.put(q2dateApr1, "create1");
    region.put(q3dateJuly1, "create2");

    region.put(q1dateFeb1, "create4");
    region.put(q2dateMay1, "create5");
    region.put(q3dateAug1, "create6");
    region.put(q1dateMar1, "create8");
    region.put(q2dateJun1, "create9");
    region.put(q3dateSep1, "create10");

    region.put(q1dateJan1, "update0");
    region.put(q2dateApr1, "update1");
    region.put(q3dateJuly1, "update2");
    region.put(q1dateFeb1, "update4");
    region.put(q2dateMay1, "update5");
    region.put(q3dateAug1, "update6");
    region.put(q1dateMar1, "update8");
    region.put(q2dateJun1, "update9");
    region.put(q3dateSep1, "update10");

    region.put(q1dateJan1, "update00");
    region.put(q2dateApr1, "update11");
    region.put(q3dateJuly1, "update22");
    region.put(q1dateFeb1, "update44");
    region.put(q2dateMay1, "update55");
    region.put(q3dateAug1, "update66");
    region.put(q1dateMar1, "update88");
    region.put(q2dateJun1, "update99");
    region.put(q3dateSep1, "update1010");
  }

  public static void getFromPartitionedRegions() {
    region.get(q1dateJan1, "create0");
    region.get(q2dateApr1, "create1");
    region.get(q3dateJuly1, "create2");
    region.get(q4dateOct1, "create3");

    region.get(q1dateJan1, "update0");
    region.get(q2dateApr1, "update1");
    region.get(q3dateJuly1, "update2");
    region.get(q4dateOct1, "update3");

    region.get(q1dateJan1, "update00");
    region.get(q2dateApr1, "update11");
    region.get(q3dateJuly1, "update22");
    region.get(q4dateOct1, "update33");
  }

  public static void getFromPartitionedRegionsFor3Qs() {
    region.get(q1dateJan1, "create0");
    region.get(q2dateApr1, "create1");
    region.get(q3dateJuly1, "create2");

    region.get(q1dateJan1, "update0");
    region.get(q2dateApr1, "update1");
    region.get(q3dateJuly1, "update2");

    region.get(q1dateJan1, "update00");
    region.get(q2dateApr1, "update11");
    region.get(q3dateJuly1, "update22");
  }

  public static void putIntoSinglePR() {
    region.put(q1dateJan1, "create0");
    region.put(q2dateApr1, "create1");
    region.put(q3dateJuly1, "create2");
    region.put(q4dateOct1, "create3");
  }

  public static void getDataFromSinglePR() {
    for (int i = 0; i < 10; i++) {
      region.get(q1dateJan1);
      region.get(q2dateApr1);
      region.get(q3dateJuly1);
      region.get(q4dateOct1);
    }
  }

  public static void updateIntoSinglePR(boolean assertNoMetadataRefreshes) {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);

    region.put(q1dateJan1, "update0");
    if (assertNoMetadataRefreshes) {
      assertFalse(cms.isRefreshMetadataTestOnly());
    }

    region.put(q1dateFeb1, "update00");
    if (assertNoMetadataRefreshes) {
      assertFalse(cms.isRefreshMetadataTestOnly());
    }

    region.put(q1dateMar1, "update000");
    if (assertNoMetadataRefreshes) {
      assertFalse(cms.isRefreshMetadataTestOnly());
    }

    region.put(q2dateApr1, "update1");
    if (assertNoMetadataRefreshes) {
      assertFalse(cms.isRefreshMetadataTestOnly());
    }

    region.put(q2dateMay1, "update11");
    if (assertNoMetadataRefreshes) {
      assertFalse(cms.isRefreshMetadataTestOnly());
    }

    region.put(q2dateJun1, "update111");
    if (assertNoMetadataRefreshes) {
      assertFalse(cms.isRefreshMetadataTestOnly());
    }

    region.put(q3dateJuly1, "update2");
    if (assertNoMetadataRefreshes) {
      assertFalse(cms.isRefreshMetadataTestOnly());
    }

    region.put(q3dateAug1, "update22");
    if (assertNoMetadataRefreshes) {
      assertFalse(cms.isRefreshMetadataTestOnly());
    }

    region.put(q3dateSep1, "update2222");
    if (assertNoMetadataRefreshes) {
      assertFalse(cms.isRefreshMetadataTestOnly());
    }

    region.put(q4dateOct1, "update3");
    if (assertNoMetadataRefreshes) {
      assertFalse(cms.isRefreshMetadataTestOnly());
    }

    region.put(q4dateNov1, "update33");
    if (assertNoMetadataRefreshes) {
      assertFalse(cms.isRefreshMetadataTestOnly());
    }

    region.put(q4dateDec1, "update3333");
    if (assertNoMetadataRefreshes) {
      assertFalse(cms.isRefreshMetadataTestOnly());
    }

  }

  public static void updateIntoSinglePRFor3Qs() {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.satisfyRefreshMetadata_TEST_ONLY(false);
    region.put(q1dateJan1, "update0");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q1dateFeb1, "update00");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q1dateMar1, "update000");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q2dateApr1, "update1");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q2dateMay1, "update11");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q2dateJun1, "update111");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q3dateJuly1, "update2");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q3dateAug1, "update22");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

    region.put(q3dateSep1, "update2222");
    assertEquals(false, cms.isRefreshMetadataTestOnly());

  }

  public static void verifyEmptyMetadata() {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    assertTrue(cms.getClientPRMetadata_TEST_ONLY().isEmpty());
  }

  public static void verifyEmptyStaticData() {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    assertTrue(cms.getClientPartitionAttributesMap().isEmpty());
  }

  public static void verifyNonEmptyMetadata() {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    assertTrue(!cms.getClientPRMetadata_TEST_ONLY().isEmpty());
    assertTrue(!cms.getClientPartitionAttributesMap().isEmpty());
  }

  public static void printMetadata() {
    if (cache != null) {
      ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
      ((GemFireCacheImpl) cache).getLogger()
          .info("Metadata is " + cms.getClientPRMetadata_TEST_ONLY());
    }
  }

  public static void printView() {
    PartitionedRegion pr = (PartitionedRegion) region;
    if (pr.cache != null) {
      ((GemFireCacheImpl) cache).getLogger().info("Primary Bucket view of server  "
          + pr.getDataStore().getLocalPrimaryBucketsListTestOnly());
      ((GemFireCacheImpl) cache).getLogger().info("Secondary Bucket view of server  "
          + pr.getDataStore().getLocalNonPrimaryBucketsListTestOnly());
    }
  }

  private void verifyMetadata(final int totalBuckets, int currentRedundancy) {
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return (regionMetaData.size() == 1);
      }

      public String description() {
        return "expected no metadata to be refreshed";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    assertTrue(regionMetaData.containsKey(region.getFullPath()));
    final ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
    wc = new WaitCriterion() {
      public boolean done() {
        return (prMetaData.getBucketServerLocationsMap_TEST_ONLY().size() == totalBuckets);
      }

      public String description() {
        return "expected no metadata to be refreshed.  Expected " + totalBuckets
            + " entries but found " + prMetaData.getBucketServerLocationsMap_TEST_ONLY();
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
    out.println("metadata is " + prMetaData);
    out.println(
        "metadata bucket locations map is " + prMetaData.getBucketServerLocationsMap_TEST_ONLY());
    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertEquals("list has wrong contents: " + entry.getValue(), currentRedundancy,
          ((List) entry.getValue()).size());
    }
  }

  // public static void clearMetadata() {
  // ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
  // cms.getClientPartitionAttributesMap().clear();
  // cms.getClientPRMetadata_TEST_ONLY().clear();
  // }

}
