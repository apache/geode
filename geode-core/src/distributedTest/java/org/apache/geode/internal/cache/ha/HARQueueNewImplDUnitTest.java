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
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.cache.Region.Entry;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.DELTA_PROPAGATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.internal.cache.CacheServerImpl.generateNameForClientMsgsRegion;
import static org.apache.geode.internal.lang.SystemProperty.GEMFIRE_PREFIX;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.GemFireException;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessage;
import org.apache.geode.internal.cache.tier.sockets.ConflationDUnitTestHelper;
import org.apache.geode.internal.cache.tier.sockets.HAEventWrapper;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * This DUnit contains various tests to ensure new implementation of ha region queues works as
 * expected.
 *
 * @since GemFire 5.7
 */
@Category({ClientSubscriptionTest.class})
public class HARQueueNewImplDUnitTest extends JUnit4DistributedTestCase {

  private static final String regionName = HARQueueNewImplDUnitTest.class.getSimpleName();
  private static final Map<Object, Object> map = new HashMap<>();

  private static Cache cache = null;
  private static VM serverVM0 = null;
  private static VM serverVM1 = null;
  private static VM clientVM1 = null;
  private static VM clientVM2 = null;

  private static final Logger logger = LogService.getLogger();
  private static int numOfCreates = 0;
  private static int numOfUpdates = 0;
  private static int numOfInvalidates = 0;
  private static Object[] deletedValues = null;

  private int PORT1;
  private int PORT2;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  /**
   * Sets up the test.
   */
  @Before
  public void setUp() {
    map.clear();

    serverVM0 = VM.getVM(0);
    serverVM1 = VM.getVM(1);
    clientVM1 = VM.getVM(2);
    clientVM2 = VM.getVM(3);

    PORT1 = serverVM0.invoke(
        () -> HARQueueNewImplDUnitTest.createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));
    PORT2 = serverVM1.invoke(
        () -> HARQueueNewImplDUnitTest.createServerCache(HARegionQueue.HA_EVICTION_POLICY_ENTRY));

    numOfCreates = 0;
    numOfUpdates = 0;
    numOfInvalidates = 0;
    clientVM1.invoke(() -> {
      numOfCreates = 0;
      numOfUpdates = 0;
      numOfInvalidates = 0;
    });
  }

  /**
   * Tears down the test.
   */
  @After
  public void tearDown() {
    map.clear();

    closeCache();
    clientVM1.invoke(HARQueueNewImplDUnitTest::closeCache);
    clientVM2.invoke(HARQueueNewImplDUnitTest::closeCache);

    // Unset the isSlowStartForTesting flag
    serverVM0.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
    serverVM1.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);

    // then close the servers
    serverVM0.invoke(HARQueueNewImplDUnitTest::closeCache);
    serverVM1.invoke(HARQueueNewImplDUnitTest::closeCache);


    disconnectAllFromDS();
  }

  private void createCache(Properties props) throws Exception {
    props.setProperty(DELTA_PROPAGATION, "false");
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertThat(ds).isNotNull();
    cache = CacheFactory.create(ds);
    assertThat(cache).isNotNull();
  }

  public static Integer createServerCache() throws Exception {
    return createServerCache(null);
  }

  public static Integer createServerCache(String ePolicy) throws Exception {
    return createServerCache(ePolicy, 1);
  }

  public static Integer createServerCache(String ePolicy, Integer cap) throws Exception {
    new HARQueueNewImplDUnitTest().createCache(new Properties());
    RegionFactory<Object, Object> factory = cache.createRegionFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.create(regionName);

    int port = getRandomAvailableTCPPort();
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    if (ePolicy != null) {
      File overflowDirectory = new File("bsi_overflow_" + port);
      overflowDirectory.mkdir();
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      File[] dirs1 = new File[] {overflowDirectory};

      server1.getClientSubscriptionConfig().setEvictionPolicy(ePolicy);
      server1.getClientSubscriptionConfig().setCapacity(cap);
      // specify disk store for this server
      server1.getClientSubscriptionConfig()
          .setDiskStoreName(dsf.setDiskDirs(dirs1).create("bsi").getName());
    }
    server1.start();
    return server1.getPort();
  }

  private static Integer createOneMoreBridgeServer(Boolean notifyBySubscription) throws Exception {
    int port = getRandomAvailableTCPPort();
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(notifyBySubscription);
    server1.getClientSubscriptionConfig()
        .setEvictionPolicy(HARegionQueue.HA_EVICTION_POLICY_MEMORY);
    // let this server to use default disk store
    server1.start();
    return server1.getPort();
  }

  public static void createClientCache(String host, Integer port1, Integer port2, String rLevel,
      Boolean addListener) throws Exception {
    System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new HARQueueNewImplDUnitTest().createCache(props);
    AttributesFactory<Object, Object> factory = new AttributesFactory<>();
    ClientServerTestCase
        .configureConnectionPool(factory, host, port1, port2, true,
            Integer.parseInt(rLevel),
            2, null, 1000, 250,
            -2);

    factory.setScope(Scope.LOCAL);

    if (addListener) {
      factory.addCacheListener(new CacheListenerAdapter<Object, Object>() {
        @Override
        public void afterInvalidate(EntryEvent event) {
          logger.debug("Invalidate Event: <" + event.getKey() + ", " + event.getNewValue() + ">");
          numOfInvalidates++;
        }

        @Override
        public void afterCreate(EntryEvent event) {
          logger.debug("Create Event: <" + event.getKey() + ", " + event.getNewValue() + ">");
          numOfCreates++;
        }

        @Override
        public void afterUpdate(EntryEvent event) {
          logger.debug("Update Event: <" + event.getKey() + ", " + event.getNewValue() + ">");
          numOfUpdates++;
        }
      });
    }

    cache.createRegion(regionName, factory.create());
  }

  public static void createClientCache(String host, Integer port1, Integer port2, String rLevel)
      throws Exception {
    createClientCache(host, port1, port2, rLevel, Boolean.FALSE);
  }

  private static void registerInterestListAll() {
    try {
      Region<Object, Object> region = cache.getRegion(SEPARATOR + regionName);
      assertThat(region).isNotNull();
      region.registerInterest("ALL_KEYS");
    } catch (GemFireException ex) {
      fail("failed in registerInterestListAll", ex);
    }
  }

  private static void registerInterestList() {
    try {
      Region<Object, Object> region = cache.getRegion(SEPARATOR + regionName);
      assertThat(region).isNotNull();
      region.registerInterest("k1");
      region.registerInterest("k3");
      region.registerInterest("k5");
    } catch (GemFireException ex) {
      fail("failed while registering keys", ex);
    }
  }

  private static void putEntries() {
    try {

      Region<Object, Object> region = cache.getRegion(SEPARATOR + regionName);
      assertThat(region).isNotNull();

      region.put("k1", "pv1");
      region.put("k2", "pv2");
      region.put("k3", "pv3");
      region.put("k4", "pv4");
      region.put("k5", "pv5");
    } catch (GemFireException ex) {
      fail("failed in putEntries()", ex);
    }
  }

  public static void createEntries() {
    try {
      Region<Object, Object> region = cache.getRegion(SEPARATOR + regionName);
      assertThat(region).isNotNull();

      region.create("k1", "v1");
      region.create("k2", "v2");
      region.create("k3", "v3");
      region.create("k4", "v4");
      region.create("k5", "v5");
    } catch (GemFireException ex) {
      fail("failed in createEntries()", ex);
    }
  }

  public static void createEntries(Long num) {
    try {
      Region<Object, Object> region = cache.getRegion(SEPARATOR + regionName);
      assertThat(region).isNotNull();
      for (long i = 0; i < num; i++) {
        region.create("k" + i, "v" + i);
      }
    } catch (GemFireException ex) {
      fail("failed in createEntries(Long)", ex);
    }
  }

  private static void putHeavyEntries(Integer num) {
    try {
      byte[] val;
      Region<Object, Object> region = cache.getRegion(SEPARATOR + regionName);
      assertThat(region).isNotNull();
      for (long i = 0; i < num; i++) {
        val = new byte[1024 * 1024 * 5]; // 5 MB
        region.put("k0", val);
      }
    } catch (GemFireException ex) {
      fail("failed in putHeavyEntries(Long)", ex);
    }
  }

  /**
   * This test verifies that the client-messages-region does not store duplicate
   * ClientUpdateMessageImpl instances, during a normal put path as well as the GII path.
   */
  @Test
  public void testClientMsgsRegionSize() throws Exception {
    // slow start for dispatcher
    serverVM0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("30000"));
    serverVM1.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("30000"));

    createClientCache(getServerHostName(), PORT1, PORT2,
        "1");
    final String client1Host = getServerHostName();
    clientVM1.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client1Host,
        PORT1, PORT2, "1"));
    final String client2Host = getServerHostName();
    clientVM2.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client2Host,
        PORT1, PORT2, "1"));

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest::registerInterestList);
    clientVM2.invoke(HARQueueNewImplDUnitTest::registerInterestList);

    serverVM1.invoke(HARQueueNewImplDUnitTest::stopServer);

    serverVM0.invoke((SerializableRunnableIF) HARQueueNewImplDUnitTest::createEntries);

    serverVM1.invoke(HARQueueNewImplDUnitTest::startServer);

    serverVM0.invoke(() -> HARQueueNewImplDUnitTest.verifyRegionSize(5, 5));
    serverVM1.invoke(() -> HARQueueNewImplDUnitTest.verifyRegionSize(5, 5));
  }

  /**
   * This test verifies that the ha-region-queues increment the reference count of their respective
   * HAEventWrapper instances in the client-messages-region correctly, during put as well as GII
   * path.
   */
  @Test
  public void testRefCountForNormalAndGIIPut() throws Exception {
    // slow start for dispatcher
    serverVM0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("240000"));
    serverVM1.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("240000"));

    createClientCache(getServerHostName(), PORT1, PORT2,
        "1");
    final String client1Host = getServerHostName();
    clientVM1.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client1Host,
        PORT1, PORT2, "1"));
    final String client2Host = getServerHostName();
    clientVM2.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client2Host,
        PORT1, PORT2, "1"));

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest::registerInterestList);
    clientVM2.invoke(HARQueueNewImplDUnitTest::registerInterestList);

    serverVM1.invoke(HARQueueNewImplDUnitTest::stopServer);

    serverVM0.invoke((SerializableRunnableIF) HARQueueNewImplDUnitTest::createEntries);

    serverVM1.invoke(HARQueueNewImplDUnitTest::startServer);

    serverVM1.invoke(() -> ValidateRegionSizes(PORT2));
    serverVM0.invoke(() -> ValidateRegionSizes(PORT1));


    serverVM0.invoke(HARQueueNewImplDUnitTest::updateMapForVM0);
    serverVM1.invoke(HARQueueNewImplDUnitTest::updateMapForVM1);

    serverVM0.invoke(() -> HARQueueNewImplDUnitTest.verifyQueueData(
        PORT1));
    serverVM1.invoke(() -> HARQueueNewImplDUnitTest.verifyQueueData(
        PORT2));
  }

  private void ValidateRegionSizes(int port) {
    await().untilAsserted(() -> {
      Region region = cache.getRegion(SEPARATOR + regionName);
      Region<Object, Object> msgsRegion =
          cache.getRegion(CacheServerImpl.generateNameForClientMsgsRegion(port));
      int clientMsgRegionSize = msgsRegion.size();
      int regionSize = region.size();
      assertThat(((5 == clientMsgRegionSize) && (5 == regionSize))).describedAs(
          "Region sizes were not as expected after 60 seconds elapsed. Actual region size = "
              + regionSize + "Actual client msg region size = " + clientMsgRegionSize)
          .isTrue();
    });
  }

  /**
   * This test verifies that the ha-region-queues decrement the reference count of their respective
   * HAEventWrapper instances in the client-messages-region correctly, after the events have been
   * peeked and removed from the queue.
   */
  @Test
  public void testRefCountForPeekAndRemove() throws Exception {
    serverVM0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("30000"));

    createClientCache(getServerHostName(), PORT1, PORT2,
        "1");
    final String client1Host = getServerHostName();
    clientVM1.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client1Host,
        PORT1, PORT2, "1"));
    final String client2Host = getServerHostName();
    clientVM2.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client2Host,
        PORT1, PORT2, "1"));

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest::registerInterestList);
    clientVM2.invoke(HARQueueNewImplDUnitTest::registerInterestList);

    serverVM0.invoke((SerializableRunnableIF) HARQueueNewImplDUnitTest::createEntries);

    serverVM0.invoke(() -> HARQueueNewImplDUnitTest.verifyRegionSize(5, 5));

    serverVM0.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
    serverVM0.invoke(() -> HARQueueNewImplDUnitTest
        .waitTillMessagesAreDispatched(PORT1));

    serverVM0.invoke(() -> HARQueueNewImplDUnitTest.verifyRegionSize(5, 0));
  }

  /**
   * This test verifies that the processing of the QRM messages results in decrementing the
   * reference count of corresponding HAEventWrapper instances, correctly.
   */
  @Test
  public void testRefCountForQRM() throws Exception {
    serverVM0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("30000"));

    createClientCache(getServerHostName(), PORT1, PORT2,
        "1");
    final String client1Host = getServerHostName();
    clientVM1.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client1Host,
        PORT1, PORT2, "1"));
    final String client2Host = getServerHostName();
    clientVM2.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client2Host,
        PORT1, PORT2, "1"));

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest::registerInterestList);
    clientVM2.invoke(HARQueueNewImplDUnitTest::registerInterestList);

    serverVM1.invoke(HARQueueNewImplDUnitTest::stopServer);

    serverVM0.invoke((SerializableRunnableIF) HARQueueNewImplDUnitTest::createEntries);

    serverVM1.invoke(HARQueueNewImplDUnitTest::startServer);

    serverVM1.invoke(() -> HARQueueNewImplDUnitTest.verifyRegionSize(5, 5));

    serverVM0.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);

    serverVM1.invoke(() -> HARQueueNewImplDUnitTest.verifyRegionSize(5, 0));
  }

  /**
   * This test verifies that the destruction of a ha-region (caused by proxy/client disconnect),
   * causes the reference count of all HAEventWrapper instances belonging to the ha-region-queue to
   * be decremented by one, and makes it visible to the client-messages-region.
   */
  @Test
  public void testRefCountForDestroy() throws Exception {
    // slow start for dispatcher
    serverVM0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("30000"));
    serverVM1.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("30000"));

    createClientCache(getServerHostName(), PORT1, PORT2,
        "1");
    final String client1Host = getServerHostName();
    clientVM1.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client1Host,
        PORT1, PORT2, "1"));
    final String client2Host = getServerHostName();
    clientVM2.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client2Host,
        PORT1, PORT2, "1"));

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest::registerInterestList);
    clientVM2.invoke(HARQueueNewImplDUnitTest::registerInterestList);
    // 1. stop the second server
    serverVM1.invoke(HARQueueNewImplDUnitTest::stopServer);

    serverVM0.invoke((SerializableRunnableIF) HARQueueNewImplDUnitTest::createEntries);
    // 3. start the second server.
    serverVM1.invoke(HARQueueNewImplDUnitTest::startServer);
    Thread.sleep(3000);

    clientVM1.invoke(HARQueueNewImplDUnitTest::closeCache);

    Thread.sleep(1000);
    serverVM0.invoke(HARQueueNewImplDUnitTest::updateMap1);
    serverVM1.invoke(HARQueueNewImplDUnitTest::updateMap1);

    serverVM0.invoke(() -> HARQueueNewImplDUnitTest.verifyQueueData(
        PORT1));
    serverVM1.invoke(() -> HARQueueNewImplDUnitTest.verifyQueueData(
        PORT2));

    clientVM2.invoke(HARQueueNewImplDUnitTest::closeCache);

    serverVM0.invoke(HARQueueNewImplDUnitTest::updateMap2);
    serverVM1.invoke(HARQueueNewImplDUnitTest::updateMap2);

    Thread.sleep(1000);
    serverVM0.invoke(() -> HARQueueNewImplDUnitTest.verifyQueueData(
        PORT1));
    serverVM1.invoke(() -> HARQueueNewImplDUnitTest.verifyQueueData(
        PORT2));
  }

  /**
   * Addresses the bug 39179. If a clientUpdateMessage is dispatched to the client while its GII was
   * under way, then it should not be put into the HARegionQueue of a client at receiving server
   * side.
   */
  @Test
  public void testConcurrentGIIAndDispatch() throws Exception {
    serverVM0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("40000"));
    serverVM1.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("40000"));

    createClientCache(getServerHostName(), PORT1, PORT2,
        "1");
    final String client1Host = getServerHostName();
    clientVM1.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client1Host,
        PORT1, PORT2, "1"));
    final String client2Host = getServerHostName();
    clientVM2.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client2Host,
        PORT1, PORT2, "1"));

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest::registerInterestListAll);
    clientVM2.invoke(HARQueueNewImplDUnitTest::registerInterestListAll);
    // 1. stop the second server
    serverVM1.invoke(HARQueueNewImplDUnitTest::stopServer);

    serverVM0.invoke((SerializableRunnableIF) HARQueueNewImplDUnitTest::createEntries);
    serverVM0.invoke(() -> HARQueueNewImplDUnitTest
        .makeValuesOfSomeKeysNullInClientMsgsRegion(PORT1, new String[] {"k1", "k3"}));
    // 3. start the second server.
    serverVM1.invoke(HARQueueNewImplDUnitTest::startServer);
    serverVM0.invoke(() -> HARQueueNewImplDUnitTest.verifyRegionSize(5, 3));

    serverVM1.invoke(() -> HARQueueNewImplDUnitTest.verifyNullValuesInCMR(
        PORT2, new String[] {"k1", "k3"}));
    serverVM1.invoke(() -> HARQueueNewImplDUnitTest.verifyRegionSize(5, 3));

    serverVM0.invoke(() -> HARQueueNewImplDUnitTest
        .populateValuesOfSomeKeysInClientMsgsRegion(PORT1, new String[] {"k1", "k3"}));

    serverVM0.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
    serverVM1.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
  }

  /**
   * This test verifies that when two BridgeServerImpl instances are created in a single VM, they do
   * share the client-messages-region.
   */
  @Test
  public void testTwoBridgeServersInOneVMDoShareCMR() throws Exception {
    // slow start for dispatcher
    serverVM0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("30000"));

    Integer port3 = serverVM0
        .invoke(() -> HARQueueNewImplDUnitTest.createOneMoreBridgeServer(Boolean.TRUE));

    createClientCache(getServerHostName(), PORT1, port3, "0");
    final String client1Host = getServerHostName();
    clientVM1.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client1Host,
        PORT1, PORT2, "1"));
    final String client2Host = getServerHostName();
    clientVM2.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client2Host,
        PORT1, PORT2, "1"));

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest::registerInterestList);
    clientVM2.invoke(HARQueueNewImplDUnitTest::registerInterestList);

    serverVM0.invoke((SerializableRunnableIF) HARQueueNewImplDUnitTest::createEntries);

    serverVM0.invoke(() -> HARQueueNewImplDUnitTest.verifyRegionSize(5, 5));
    serverVM0.invoke(
        () -> HARQueueNewImplDUnitTest.verifyRegionSize(5, 5));
  }

  /**
   * This test verifies that two clients, connected to two cache servers with different
   * notifyBySubscription values, on a single VM, receive updates/invalidates depending upon their
   * notifyBySubscription value.
   */
  @Test
  public void testUpdatesWithTwoBridgeServersInOneVM() throws Exception {
    Integer port3 = serverVM0
        .invoke(() -> HARQueueNewImplDUnitTest.createOneMoreBridgeServer(Boolean.FALSE));

    createClientCache(getServerHostName(), PORT1, PORT2,
        "1", Boolean.TRUE);
    final String client1Host = getServerHostName();
    clientVM1.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client1Host, port3,
        PORT2, "1", Boolean.TRUE));

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest::registerInterestListAll);

    clientVM1.invoke((SerializableRunnableIF) HARQueueNewImplDUnitTest::createEntries);
    serverVM0.invoke(HARQueueNewImplDUnitTest::putEntries);

    serverVM0.invoke(() -> HARQueueNewImplDUnitTest.waitTillMessagesAreDispatched(PORT1));
    serverVM0.invoke(() -> HARQueueNewImplDUnitTest.waitTillMessagesAreDispatched(port3));

    // expect updates
    verifyUpdatesReceived();
    // expect invalidates
    clientVM1.invoke(HARQueueNewImplDUnitTest::verifyUpdatesReceived);
  }

  /**
   * This test verifies that the HAEventWrapper instances present in the client-messages-region give
   * up the references to their respective ClientUpdateMessageImpl instances.
   */
  @Test
  public void testHAEventWrapperDoesNotHoldCUMOnceInsideCMR() throws Exception {
    // slow start for dispatcher
    serverVM0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("30000"));

    createClientCache(getServerHostName(), PORT1, PORT2,
        "1");
    final String client1Host = getServerHostName();
    clientVM1.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client1Host,
        PORT1, PORT2, "1"));
    final String client2Host = getServerHostName();
    clientVM2.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client2Host,
        PORT1, PORT2, "1"));

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest::registerInterestList);
    clientVM2.invoke(HARQueueNewImplDUnitTest::registerInterestList);

    serverVM1.invoke(HARQueueNewImplDUnitTest::stopServer);

    serverVM0.invoke(() -> HARQueueNewImplDUnitTest.createEntries(1000L));

    serverVM1.invoke(HARQueueNewImplDUnitTest::startServer);
    Thread.sleep(2000);

    serverVM0.invoke(() -> HARQueueNewImplDUnitTest.verifyNullCUMReference(PORT1));
    serverVM1.invoke(() -> HARQueueNewImplDUnitTest.verifyNullCUMReference(PORT2));
  }

  /**
   * This test verifies that client-messages-regions are not created for the cache servers who have
   * eviction policy as 'none'. Instead, such cache servers will have simple HashMap structures.
   * Also, it verifies that such a structure (referred to as haContainer, in general) is destroyed
   * when its cache server is stopped.
   */
  @Test
  public void testCMRNotCreatedForNoneEvictionPolicy() throws Exception {
    serverVM0.invoke(HARQueueNewImplDUnitTest::closeCache);
    serverVM1.invoke(HARQueueNewImplDUnitTest::closeCache);
    Thread.sleep(2000);
    PORT1 = serverVM0.invoke(
        () -> HARQueueNewImplDUnitTest.createServerCache(HARegionQueue.HA_EVICTION_POLICY_NONE));
    PORT2 = serverVM1.invoke(
        () -> HARQueueNewImplDUnitTest.createServerCache(HARegionQueue.HA_EVICTION_POLICY_NONE));
    Boolean isRegion = Boolean.FALSE;
    // slow start for dispatcher
    serverVM0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("30000"));

    createClientCache(getServerHostName(), PORT1, PORT2,
        "1");
    final String client1Host = getServerHostName();
    clientVM1.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client1Host,
        PORT1, PORT2, "1"));
    final String client2Host = getServerHostName();
    clientVM2.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client2Host,
        PORT1, PORT2, "1"));

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest::registerInterestList);
    clientVM2.invoke(HARQueueNewImplDUnitTest::registerInterestList);

    serverVM0
        .invoke(() -> HARQueueNewImplDUnitTest.verifyHaContainerType(isRegion, PORT1));
    serverVM1
        .invoke(() -> HARQueueNewImplDUnitTest.verifyHaContainerType(isRegion, PORT2));

    serverVM0.invoke(() -> HARQueueNewImplDUnitTest.stopOneBridgeServer(PORT1));
    serverVM1.invoke(() -> HARQueueNewImplDUnitTest.stopOneBridgeServer(PORT2));

    serverVM0.invoke(
        () -> HARQueueNewImplDUnitTest.verifyHaContainerDestroyed(isRegion, PORT1));
    serverVM1.invoke(
        () -> HARQueueNewImplDUnitTest.verifyHaContainerDestroyed(isRegion, PORT2));
  }

  /**
   * This test verifies that client-messages-regions are created for the cache servers who have
   * eviction policy either as 'mem' or as 'entry'. Also, it verifies that such a
   * client-messages-region is destroyed when its cache server is stopped.
   */
  @Test
  public void testCMRCreatedForMemOrEntryEvictionPolicy() throws Exception {
    Boolean isRegion = Boolean.TRUE;
    // slow start for dispatcher
    serverVM0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("30000"));

    createClientCache(getServerHostName(), PORT1, PORT2,
        "1");
    final String client1Host = getServerHostName();
    clientVM1.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client1Host,
        PORT1, PORT2, "1"));
    final String client2Host = getServerHostName();
    clientVM2.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client2Host,
        PORT1, PORT2, "1"));

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest::registerInterestList);
    clientVM2.invoke(HARQueueNewImplDUnitTest::registerInterestList);

    serverVM0
        .invoke(() -> HARQueueNewImplDUnitTest.verifyHaContainerType(isRegion, PORT1));
    serverVM1
        .invoke(() -> HARQueueNewImplDUnitTest.verifyHaContainerType(isRegion, PORT2));

    serverVM0.invoke(() -> HARQueueNewImplDUnitTest.stopOneBridgeServer(PORT1));
    serverVM1.invoke(() -> HARQueueNewImplDUnitTest.stopOneBridgeServer(PORT2));

    serverVM0.invoke(
        () -> HARQueueNewImplDUnitTest.verifyHaContainerDestroyed(isRegion, PORT1));
    serverVM1.invoke(
        () -> HARQueueNewImplDUnitTest.verifyHaContainerDestroyed(isRegion, PORT2));
  }

  /**
   * This test verifies that the Cache.rootRegions() method does not return the
   * client-messages-region of any of the cache's attached cache servers.
   */
  @Test
  public void testCMRNotReturnedByRootRegionsMethod() throws Exception {

    createClientCache(getServerHostName(), PORT1, PORT2,
        "1");
    final String client1Host = getServerHostName();
    clientVM1.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client1Host,
        PORT1, PORT2, "1"));
    final String client2Host = getServerHostName();
    clientVM2.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client2Host,
        PORT1, PORT2, "1"));

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest::registerInterestList);
    clientVM2.invoke(HARQueueNewImplDUnitTest::registerInterestList);

    serverVM0.invoke((SerializableRunnableIF) HARQueueNewImplDUnitTest::createEntries);

    serverVM0.invoke(
        () -> HARQueueNewImplDUnitTest.verifyRootRegionsDoesNotReturnCMR(PORT1));
    serverVM1.invoke(
        () -> HARQueueNewImplDUnitTest.verifyRootRegionsDoesNotReturnCMR(PORT2));
  }

  /**
   * This test verifies that the memory footprint of the ha region queues is less when ha-overflow
   * is enabled (with an appropriate value of haCapacity) compared to when it is disabled, for the
   * same amount of data feed.
   */
  @Ignore("TODO")
  @Test
  public void testMemoryFootprintOfHARegionQueuesWithAndWithoutOverflow() throws Exception {
    serverVM0.invoke(HARQueueNewImplDUnitTest::closeCache);
    serverVM1.invoke(HARQueueNewImplDUnitTest::closeCache);
    Thread.sleep(2000);
    Integer numOfEntries = 30;

    PORT1 = serverVM0.invoke(() -> HARQueueNewImplDUnitTest
        .createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY, 30));
    PORT2 = serverVM1.invoke(
        () -> HARQueueNewImplDUnitTest.createServerCache(HARegionQueue.HA_EVICTION_POLICY_NONE));

    serverVM0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));
    serverVM1.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));

    createClientCache(getServerHostName(), PORT1, PORT2,
        "1");
    final String client1Host = getServerHostName();
    clientVM1.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client1Host,
        PORT1, PORT2, "1"));
    final String client2Host = getServerHostName();
    clientVM2.invoke(() -> HARQueueNewImplDUnitTest.createClientCache(client2Host,
        PORT1, PORT2, "1"));

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest::registerInterestListAll);
    clientVM2.invoke(HARQueueNewImplDUnitTest::registerInterestListAll);

    serverVM0.invoke(() -> HARQueueNewImplDUnitTest.putHeavyEntries(numOfEntries));

    Long usedMemInVM0 = serverVM0.invoke(() -> HARQueueNewImplDUnitTest
        .getUsedMemoryAndVerifyRegionSize(numOfEntries, PORT1));
    Long usedMemInVM1 = serverVM1.invoke(() -> HARQueueNewImplDUnitTest
        .getUsedMemoryAndVerifyRegionSize(numOfEntries, -1));

    serverVM0.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
    serverVM1.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);

    logger.debug("Used Mem: " + usedMemInVM1 + "(without overflow), "
        + usedMemInVM0 + "(with overflow)");

    assertThat(usedMemInVM0 < usedMemInVM1).isTrue();
  }

  private static void verifyNullCUMReference(Integer port) {
    Region<Object, Object> region =
        cache.getRegion(SEPARATOR + CacheServerImpl.generateNameForClientMsgsRegion(port));
    assertThat(region).isNotNull();

    Object[] arr = region.keySet().toArray();
    for (Object o : arr) {
      assertThat(((HAEventWrapper) o).getClientUpdateMessage()).isNull();
    }

  }

  private static void verifyHaContainerDestroyed(Boolean isRegion, Integer port) {
    Map region = cache.getRegion(SEPARATOR + CacheServerImpl.generateNameForClientMsgsRegion(port));

    if (isRegion) {
      if (region != null) {
        assertThat(((Region) region).isDestroyed()).isTrue();
      }
    } else {
      region = ((InternalCacheServer) cache.getCacheServers().toArray()[0]).getAcceptor()
          .getCacheClientNotifier().getHaContainer();
      if (region != null) {
        assertThat(region.isEmpty()).isTrue();
      }
    }
  }

  private static Long getUsedMemoryAndVerifyRegionSize(Integer haContainerSize,
      Integer port) {
    Long retVal;
    retVal = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    if (port != -1) {
      verifyRegionSize(1, haContainerSize);
    } else {
      verifyRegionSize(haContainerSize);
    }
    return retVal;
  }

  private static void stopOneBridgeServer(Integer port) {
    Iterator iterator = cache.getCacheServers().iterator();
    if (iterator.hasNext()) {
      CacheServer server = (CacheServer) iterator.next();
      if (server.getPort() == port) {
        server.stop();
      }
    }
  }

  public static void stopServer() {
    Iterator iterator = cache.getCacheServers().iterator();
    if (iterator.hasNext()) {
      CacheServer server = (CacheServer) iterator.next();
      server.stop();
    }
  }

  private static void updateMapForVM0() {
    map.put("k1", 3L);
    map.put("k2", 1L);
    map.put("k3", 3L);
    map.put("k4", 1L);
    map.put("k5", 3L);
  }

  private static void updateMap1() {
    map.put("k1", 2L);
    map.put("k2", 1L);
    map.put("k3", 2L);
    map.put("k4", 1L);
    map.put("k5", 2L);
  }

  private static void updateMap2() {
    map.put("k1", 1L);
    map.put("k2", 1L);
    map.put("k3", 1L);
    map.put("k4", 1L);
    map.put("k5", 1L);

  }

  private static void updateMapForVM1() {
    updateMapForVM0();
  }

  private static void verifyNullValuesInCMR(final Integer port,
      String[] keys) {
    final Region<Object, Object> msgsRegion =
        cache.getRegion(generateNameForClientMsgsRegion(port));

    GeodeAwaitility.await().until(() -> msgsRegion.size() == 3);

    Set entries = msgsRegion.entrySet();
    Iterator iterator = entries.iterator();
    for (; iterator.hasNext();) {
      Entry entry = (Entry) iterator.next();
      ClientUpdateMessage cum = (ClientUpdateMessage) entry.getValue();
      for (String key : keys) {
        logger.debug("cum.key: " + cum.getKeyToConflate());
        // assert that the keys are not present in entries set
        assertThat(!key.equals(cum.getKeyToConflate())).isTrue();
      }
    }
  }

  private static void makeValuesOfSomeKeysNullInClientMsgsRegion(Integer port, String[] keys) {
    Region<Object, Object> msgsRegion =
        cache.getRegion(CacheServerImpl.generateNameForClientMsgsRegion(port));
    assertThat(msgsRegion).isNotNull();

    Set entries = msgsRegion.entrySet();
    Iterator iterator = entries.iterator();
    deletedValues = new Object[keys.length];
    while (iterator.hasNext()) {
      Region.Entry entry = (Region.Entry) iterator.next();
      ClientUpdateMessage cum = (ClientUpdateMessage) entry.getValue();
      for (int i = 0; i < keys.length; i++) {
        if (keys[i].equals(cum.getKeyToConflate())) {
          logger.debug("HARQueueNewImplDUnit: Removing " + cum.getKeyOfInterest());
          deletedValues[i] = msgsRegion.remove(entry.getKey());
        }
      }
    }
  }

  private static void populateValuesOfSomeKeysInClientMsgsRegion(Integer port, String[] keys) {
    Region<Object, Object> msgsRegion =
        cache.getRegion(CacheServerImpl.generateNameForClientMsgsRegion(port));
    assertThat(msgsRegion).isNotNull();

    for (int i = 0; i < keys.length; i++) {
      logger.debug("HARQueueNewImplDUnit: populating " + deletedValues[i]);
      msgsRegion.put(keys[1], deletedValues[i]);
    }
  }

  public static void startServer() throws IOException {

    Iterator iterator = cache.getCacheServers().iterator();
    if (iterator.hasNext()) {
      CacheServer server = (CacheServer) iterator.next();
      server.start();
    }

  }

  private static void verifyQueueData(Integer port) {
    // Get the clientMessagesRegion and check the size.
    Region<Object, Object> msgsRegion =
        cache.getRegion(CacheServerImpl.generateNameForClientMsgsRegion(port));
    Region region = cache.getRegion(SEPARATOR + regionName);
    logger.debug(
        "size<serverRegion, clientMsgsRegion>: " + region.size() + ", " + msgsRegion.size());
    assertThat(region.size()).isEqualTo(((Integer) 5).intValue());
    assertThat(msgsRegion.size()).isEqualTo(((Integer) 5).intValue());

    for (Object o : msgsRegion.entrySet()) {
      await().untilAsserted(() -> {
        Entry entry = (Entry) o;
        HAEventWrapper wrapper = (HAEventWrapper) entry.getKey();
        ClientUpdateMessage cum = (ClientUpdateMessage) entry.getValue();
        Object key = cum.getKeyOfInterest();
        logger.debug("key<feedCount, regionCount>: " + key + "<"
            + map.get(key) + ", " + wrapper.getReferenceCount() + ">");
        assertThat(wrapper.getReferenceCount()).isEqualTo(((Long) map.get(key)).longValue());
      });
    }
  }

  private static void verifyRegionSize(final Integer regionSize, final Integer msgsRegionSize) {
    GeodeAwaitility.await().until(() -> {
      // Get the clientMessagesRegion and check the size.
      Region<Object, Object> region = cache.getRegion(SEPARATOR + regionName);
      int sz = region.size();
      if (regionSize != sz) {
        return false;
      }

      Iterator iterator = cache.getCacheServers().iterator();
      if (iterator.hasNext()) {
        InternalCacheServer server = (InternalCacheServer) iterator.next();
        Map msgsRegion = server.getAcceptor().getCacheClientNotifier().getHaContainer();

        sz = msgsRegion.size();
        return msgsRegionSize == sz;
      }
      return true;
    });
  }

  private static void verifyRegionSize(final Integer msgsRegionSize) {

    GeodeAwaitility.await().until(() -> {
      try {
        // Get the clientMessagesRegion and check the size.
        Region<Object, Object> region = cache.getRegion(SEPARATOR + regionName);
        int sz = region.size();
        if (sz != 1) {
          return false;
        }
        Iterator iterator = cache.getCacheServers().iterator();
        if (!iterator.hasNext()) {
          return true;
        }
        InternalCacheServer server = (InternalCacheServer) iterator.next();
        sz = server.getAcceptor().getCacheClientNotifier().getHaContainer().size();
        return sz == msgsRegionSize;
      } catch (Exception e) {
        return false;
      }
    });
  }

  private static void verifyHaContainerType(Boolean isRegion, Integer port) {
    Map<Object, Object> haMap =
        cache.getRegion(CacheServerImpl.generateNameForClientMsgsRegion(port));
    if (isRegion) {
      assertThat(haMap).isNotNull();
      assertThat(haMap instanceof LocalRegion).isTrue();
      haMap = (Map<Object, Object>) ((InternalCacheServer) cache.getCacheServers().toArray()[0])
          .getAcceptor()
          .getCacheClientNotifier().getHaContainer();
      assertThat(haMap).isNotNull();
      assertThat(haMap instanceof HAContainerRegion).isTrue();
    } else {
      assertThat(haMap).isNull();
      haMap = (Map<Object, Object>) ((InternalCacheServer) cache.getCacheServers().toArray()[0])
          .getAcceptor()
          .getCacheClientNotifier().getHaContainer();
      assertThat(haMap).isNotNull();
      assertThat(haMap instanceof HAContainerMap).isTrue();
    }
  }

  private static void verifyRootRegionsDoesNotReturnCMR(Integer port) {
    String cmrName = CacheServerImpl.generateNameForClientMsgsRegion(port);
    Map<Object, Object> haMap = cache.getRegion(cmrName);
    assertThat(haMap).isNotNull();
    String rName;

    for (Region<?, ?> region : cache.rootRegions()) {
      rName = region.getName();
      if (cmrName.equals(rName)) {
        throw new AssertionError(
            "Cache.rootRegions() method should not return the client_messages_region.");
      }
      logger.debug("Region name returned from cache.rootRegions(): " + rName);
    }
  }

  private static void verifyUpdatesReceived() {
    GeodeAwaitility.await().until(() -> 5 == numOfUpdates);
  }

  private static void waitTillMessagesAreDispatched(Integer port) {
    Map haContainer;
    haContainer = cache.getRegion(
        SEPARATOR + generateNameForClientMsgsRegion(port));
    if (haContainer == null) {
      Object[] servers = cache.getCacheServers().toArray();
      for (Object server : servers) {
        if (port == ((InternalCacheServer) server).getPort()) {
          haContainer = ((InternalCacheServer) server).getAcceptor().getCacheClientNotifier()
              .getHaContainer();
          break;
        }
      }
    }
    final Map m = haContainer;
    GeodeAwaitility.await().until(() -> m.size() == 0);
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().getDistributedMember();
    }
  }

}
