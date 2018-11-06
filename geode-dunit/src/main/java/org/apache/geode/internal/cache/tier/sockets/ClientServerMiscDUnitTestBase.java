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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.GemFireIOException;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.Op;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.QueueConnectionImpl;
import org.apache.geode.cache.client.internal.RegisterInterestTracker;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Tests client server corner cases between Region and Pool
 */
@Category({ClientServerTest.class})
public class ClientServerMiscDUnitTestBase extends JUnit4CacheTestCase {

  protected static PoolImpl pool = null;

  protected static Connection conn = null;

  static Cache static_cache;

  static int PORT1;

  private static final String k1 = "k1";

  private static final String k2 = "k2";

  static final String server_k1 = "server-k1";

  static final String server_k2 = "server-k2";

  static final String REGION_NAME1 = "ClientServerMiscDUnitTest_region1";

  static final String REGION_NAME2 = "ClientServerMiscDUnitTest_region2";

  private static final String PR_REGION_NAME = "ClientServerMiscDUnitTest_PRregion";

  private static Host host;

  protected static VM server1;

  protected static VM server2;

  private static RegionAttributes attrs;

  // variables for concurrent map API test
  Properties props = new Properties();
  private final int putRange_1Start = 1;
  private final int putRange_1End = 5;
  private final int putRange_2Start = 6;
  private final int putRange_2End = 10;


  String testVersion; // version for client caches for backward-compatibility
                      // testing

  public ClientServerMiscDUnitTestBase() {
    testVersion = VersionManager.CURRENT_VERSION;
  }

  @Override
  public final void postSetUp() {
    host = Host.getHost(0);
    server1 = host.getVM(VersionManager.CURRENT_VERSION, 2);
    server2 = host.getVM(VersionManager.CURRENT_VERSION, 3);
  }

  int initServerCache(boolean notifyBySub) {
    return initServerCache(notifyBySub, false);
  }

  int initServerCache2() {
    return initServerCache2(false);
  }

  private int initServerCache(boolean notifyBySub, boolean isHA) {
    return initServerCache(notifyBySub, server1, isHA);
  }

  private int initServerCache2(boolean isHA) {
    return initServerCache(true, server2, isHA);
  }

  int initServerCache(boolean notifyBySub, VM vm, boolean isHA) {
    return vm.invoke(() -> createServerCache(notifyBySub, getMaxThreads(), isHA));
  }

  @Test
  public void testConcurrentOperationsWithDRandPR() {
    int port1 = initServerCache(true); // vm0
    int port2 = initServerCache2(); // vm1
    String serverName = NetworkUtils.getServerHostName();
    host.getVM(testVersion, 0).invoke(() -> createClientCacheV(serverName, port1));
    host.getVM(testVersion, 1).invoke(() -> createClientCacheV(serverName, port2));
    LogService.getLogger()
        .info("Testing concurrent map operations from a client with a distributed region");
    concurrentMapTest(host.getVM(testVersion, 0), "/" + REGION_NAME1);
    // TODO add verification in vm1
    LogService.getLogger()
        .info("Testing concurrent map operations from a client with a partitioned region");
    concurrentMapTest(host.getVM(testVersion, 0), "/" + PR_REGION_NAME);
    // TODO add verification in vm1
  }

  /**
   * When a client's subscription thread connects to a server it should receive the server's
   * pingInterval setting. This is used by the client to set a read-timeout in order to avoid
   * hanging should the server's machine crash.
   */
  @Test
  public void testClientReceivesPingIntervalSetting() {
    VM clientVM = Host.getHost(0).getVM(testVersion, 0);

    final int port = initServerCache(true);
    final String host = NetworkUtils.getServerHostName();

    clientVM.invoke("create client cache and verify", () -> {
      createClientCacheAndVerifyPingIntervalIsSet(host, port);
    });
  }

  void createClientCacheAndVerifyPingIntervalIsSet(String host, int port) throws Exception {
    PoolImpl pool = null;
    try {
      Properties props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, "");

      createCache(props);

      pool = (PoolImpl) PoolManager.createFactory().addServer(host, port)
          .setSubscriptionEnabled(true).setThreadLocalConnections(false).setReadTimeout(1000)
          .setSocketBufferSize(32768).setMinConnections(1).setSubscriptionRedundancy(-1)
          .setPingInterval(2000).create("test pool");

      Region<Object, Object> region = cache.createRegionFactory(RegionShortcut.LOCAL)
          .setPoolName("test pool").create(REGION_NAME1);
      region.registerInterest(".*");

      /** get the subscription connection and verify that it has the correct timeout setting */
      QueueConnectionImpl primaryConnection = (QueueConnectionImpl) pool.getPrimaryConnection();
      int pingInterval = ((CacheClientUpdater) primaryConnection.getUpdater())
          .getServerQueueStatus().getPingInterval();
      assertNotEquals(0, pingInterval);
      assertEquals(CacheClientNotifier.getClientPingInterval(), pingInterval);
    } finally {
      cache.close();
    }

  }

  @Test
  public void testConcurrentOperationsWithDRandPRandEmptyClient() {
    int port1 = initServerCache(true); // vm0
    int port2 = initServerCache2(); // vm1
    String serverName = NetworkUtils.getServerHostName();
    host.getVM(testVersion, 0).invoke(() -> createEmptyClientCache(serverName, port1));
    host.getVM(testVersion, 1).invoke(() -> createClientCacheV(serverName, port2));
    LogService.getLogger()
        .info("Testing concurrent map operations from a client with a distributed region");
    concurrentMapTest(host.getVM(testVersion, 0), "/" + REGION_NAME1);
    // TODO add verification in vm1
    LogService.getLogger()
        .info("Testing concurrent map operations from a client with a partitioned region");
    concurrentMapTest(host.getVM(testVersion, 0), "/" + PR_REGION_NAME);
    // TODO add verification in vm1
  }

  /**
   * Do putIfAbsent(), replace(Object, Object), replace(Object, Object, Object), remove(Object,
   * Object) operations
   */
  private void concurrentMapTest(final VM clientVM, final String rName) {

    // String exceptionStr = "";
    clientVM.invoke(new CacheSerializableRunnable("doConcurrentMapOperations") {
      public void run2() throws CacheException {
        Cache cache = getCache();
        final Region pr = cache.getRegion(rName);
        assertNotNull(rName + " not created", pr);
        boolean isEmpty = pr.getAttributes().getDataPolicy() == DataPolicy.EMPTY;

        // test successful putIfAbsent
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          Object putResult = pr.putIfAbsent(Integer.toString(i), Integer.toString(i));
          assertNull("Expected null, but got " + putResult + " for key " + i, putResult);
        }
        int size;
        if (!isEmpty) {
          size = pr.size();
          assertEquals("Size doesn't return expected value", putRange_1End, size);
          assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", pr.isEmpty());
        }

        // test unsuccessful putIfAbsent
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          Object putResult = pr.putIfAbsent(Integer.toString(i), Integer.toString(i + 1));
          assertEquals("for i=" + i, Integer.toString(i), putResult);
          assertEquals("for i=" + i, Integer.toString(i), pr.get(Integer.toString(i)));
        }
        if (!isEmpty) {
          size = pr.size();
          assertEquals("Size doesn't return expected value", putRange_1End, size);
          assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", pr.isEmpty());
        }

        // test successful replace(key, oldValue, newValue)
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          boolean replaceSucceeded =
              pr.replace(Integer.toString(i), Integer.toString(i), "replaced" + i);
          assertTrue("for i=" + i, replaceSucceeded);
          assertEquals("for i=" + i, "replaced" + i, pr.get(Integer.toString(i)));
        }
        if (!isEmpty) {
          size = pr.size();
          assertEquals("Size doesn't return expected value", putRange_1End, size);
          assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", pr.isEmpty());
        }

        // test unsuccessful replace(key, oldValue, newValue)
        for (int i = putRange_1Start; i <= putRange_2End; i++) {
          boolean replaceSucceeded = pr.replace(Integer.toString(i), Integer.toString(i), // wrong
                                                                                          // expected
                                                                                          // old
                                                                                          // value
              "not" + i);
          assertFalse("for i=" + i, replaceSucceeded);
          assertEquals("for i=" + i, i <= putRange_1End ? "replaced" + i : null,
              pr.get(Integer.toString(i)));
        }
        if (!isEmpty) {
          size = pr.size();
          assertEquals("Size doesn't return expected value", putRange_1End, size);
          assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", pr.isEmpty());
        }

        // test successful replace(key, value)
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          Object replaceResult = pr.replace(Integer.toString(i), "twice replaced" + i);
          assertEquals("for i=" + i, "replaced" + i, replaceResult);
          assertEquals("for i=" + i, "twice replaced" + i, pr.get(Integer.toString(i)));
        }
        if (!isEmpty) {
          size = pr.size();
          assertEquals("Size doesn't return expected value", putRange_1End, size);
          assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", pr.isEmpty());
        }

        // test unsuccessful replace(key, value)
        for (int i = putRange_2Start; i <= putRange_2End; i++) {
          Object replaceResult = pr.replace(Integer.toString(i), "thrice replaced" + i);
          assertNull("for i=" + i, replaceResult);
          assertNull("for i=" + i, pr.get(Integer.toString(i)));
        }
        if (!isEmpty) {
          size = pr.size();
          assertEquals("Size doesn't return expected value", putRange_1End, size);
          assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", pr.isEmpty());
        }

        // test unsuccessful remove(key, value)
        for (int i = putRange_1Start; i <= putRange_2End; i++) {
          boolean removeResult = pr.remove(Integer.toString(i), Integer.toString(-i));
          assertFalse("for i=" + i, removeResult);
          assertEquals("for i=" + i, i <= putRange_1End ? "twice replaced" + i : null,
              pr.get(Integer.toString(i)));
        }
        if (!isEmpty) {
          size = pr.size();
          assertEquals("Size doesn't return expected value", putRange_1End, size);
          assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", pr.isEmpty());
        }

        // test successful remove(key, value)
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          boolean removeResult = pr.remove(Integer.toString(i), "twice replaced" + i);
          assertTrue("for i=" + i, removeResult);
          assertNull("for i=" + i, pr.get(Integer.toString(i)));
        }
        if (!isEmpty) {
          size = pr.size();
          assertEquals("Size doesn't return expected value", 0, size);
          pr.localClear();
          assertTrue("isEmpty doesnt return proper state of the PartitionedRegion", pr.isEmpty());
        }

        if (!isEmpty) {
          // bug #42169 - entry not updated on server when locally destroyed on client
          String key42169 = "key42169";
          pr.put(key42169, "initialValue42169");
          pr.localDestroy(key42169);
          boolean success = pr.replace(key42169, "initialValue42169", "newValue42169");
          assertTrue("expected replace to succeed", success);
          pr.destroy(key42169);
          pr.put(key42169, "secondRound");
          pr.localDestroy(key42169);
          Object result = pr.putIfAbsent(key42169, null);
          assertEquals("expected putIfAbsent to fail", result, "secondRound");
          pr.destroy(key42169);
        }

        if (isEmpty) {
          String key41265 = "key41265";
          boolean success = pr.remove(key41265, null);
          assertFalse("expected remove to fail because key does not exist", success);
        }

        // test null values

        // putIfAbsent with null value creates invalid entry
        Object oldValue = pr.putIfAbsent("keyForNull", null);
        assertNull(oldValue);
        if (!isEmpty) {
          assertTrue(pr.containsKey("keyForNull"));
          assertTrue(!pr.containsValueForKey("keyForNull"));
        }

        // replace allows null value for oldValue, meaning invalidated entry
        assertTrue(pr.replace("keyForNull", null, "no longer invalid"));

        // replace does not allow null value for new value
        assertThatThrownBy(() -> pr.replace("keyForNull", "no longer invalid", null))
            .isExactlyInstanceOf(NullPointerException.class);

        // other variant of replace does not allow null value for new value
        assertThatThrownBy(() -> pr.replace("keyForNull", null))
            .isExactlyInstanceOf(NullPointerException.class);

        // replace with null oldvalue matches invalidated entry
        pr.putIfAbsent("otherKeyForNull", null);
        int puts = ((GemFireCacheImpl) pr.getCache()).getCachePerfStats().getPuts();
        boolean success = pr.replace("otherKeyForNull", null, "no longer invalid");
        assertTrue(success);
        int newputs = ((GemFireCacheImpl) pr.getCache()).getCachePerfStats().getPuts();
        assertEquals("stats not updated properly or replace malfunctioned", newputs, puts + 1);

      }
    });
  }

  /**
   * Test two regions: notify by subscription is true. For region1 the interest list is empty , for
   * region 2 the intetest list is all keys. If an update/create is made on region1 , the client
   * should not receive any. If the create/update is on region2 , the client should receive the
   * update.
   */
  @Test
  public void testForTwoRegionHavingDifferentInterestList() {
    // start server first
    PORT1 = initServerCache(true);
    int serverPort = PORT1;
    VM client1 = Host.getHost(0).getVM(testVersion, 1);
    String hostname = NetworkUtils.getServerHostName();
    client1.invoke("create client1 cache", () -> {
      createClientCache(hostname, serverPort);
      populateCache();
      registerInterest();
    });

    server1.invoke("putting entries in server1", () -> put());

    client1.invoke(() -> verifyUpdates());
  }

  /**
   * Test two regions: notify by subscription is true. Both the regions have registered interest in
   * all the keys. Now close region1 on the client. The region1 should get removed from the interest
   * list on CCP at server. Any update on region1 on server should not get pushed to the client.
   * Ensure that the message related is not added to the client's queue at all ( which is diferent
   * from not receiving a callbak on the client). If an update on region2 is made on the server ,
   * then client should receive the calback
   */
  @Test
  public void testForTwoRegionHavingALLKEYSInterest() throws Exception {
    // start server first
    PORT1 = initServerCache(true);
    createClientCache(NetworkUtils.getServerHostName(), PORT1);
    populateCache();
    registerInterestInBothTheRegions();
    closeRegion1();
    Wait.pause(6000);
    server1.invoke(() -> ClientServerMiscDUnitTestBase.verifyInterestListOnServer());
    server1.invoke(() -> ClientServerMiscDUnitTestBase.put());
    // pause(5000);
    verifyUpdatesOnRegion2();
  }

  /**
   * Test two regions: notify by subscription is true. Both the regions have registered interest in
   * all the keys. Close both the regions. When the last region is closed , it should close the
   * ConnectionProxy on the client , close all the server connection threads on the server & remove
   * the CacheClientProxy from the CacheClient notifier
   */
  @Test
  public void testRegionClose() throws Exception {
    // start server first
    PORT1 = initServerCache(true);
    pool = (PoolImpl) createClientCache(NetworkUtils.getServerHostName(), PORT1);
    populateCache();
    registerInterestInBothTheRegions();
    closeBothRegions();

    assertFalse(pool.isDestroyed());
    pool.destroy();
    assertTrue(pool.isDestroyed());
    server1.invoke(() -> ClientServerMiscDUnitTestBase.verifyNoCacheClientProxyOnServer());

  }

  /**
   * Test two regions: notify by subscription is true. Both the regions have registered interest in
   * all the keys. Destroy region1 on the client. It should reach the server , kill the region on
   * the server , propagate it to the interested clients , but it should keep CacheClient Proxy
   * alive. Destroy Region2 . It should reach server , close conenction proxy , destroy the region2
   * on the server , remove the cache client proxy from the cache client notifier & propagate it to
   * the clients. Then create third region and verify that no CacheClientProxy is created on server
   */
  @Test
  public void testCCPDestroyOnLastDestroyRegion() throws Exception {
    PORT1 = initServerCache(true);
    PoolImpl pool =
        (PoolImpl) createClientCache(NetworkUtils.getServerHostName(), PORT1);
    destroyRegion1();
    // pause(5000);
    server1.invoke(
        () -> ClientServerMiscDUnitTestBase.verifyCacheClientProxyOnServer(REGION_NAME1));
    Connection conn = pool.acquireConnection();
    assertNotNull(conn);
    assertEquals(1, pool.getConnectedServerCount());
    assertFalse(pool.isDestroyed());
    destroyRegion2();
    assertFalse(pool.isDestroyed());
    destroyPRRegion();
    assertFalse(pool.isDestroyed());
    pool.destroy();
    assertTrue(pool.isDestroyed());
    // pause(5000);
    server1.invoke(() -> ClientServerMiscDUnitTestBase.verifyNoCacheClientProxyOnServer());

    assertThatThrownBy(() -> getCache().createRegion(REGION_NAME2, attrs))
        .isExactlyInstanceOf(IllegalStateException.class);
  }

  /**
   * Test two regions:If notify by subscription is false , both the regions should receive
   * invalidates for the updates on server in their respective regions
   *
   */
  @Test
  public void testInvalidatesPropagateOnTwoRegions() throws Exception {
    // start server first
    PORT1 = initServerCache(false);
    createClientCache(NetworkUtils.getServerHostName(), PORT1);
    registerInterestForInvalidatesInBothTheRegions();
    populateCache();
    server1.invoke(() -> ClientServerMiscDUnitTestBase.put());
    // pause(5000);
    verifyInvalidatesOnBothRegions();

  }

  /**
   * Test for bug 43407, where LRU in the client caused an entry to be evicted with DESTROY(), then
   * the client invalidated the entry and did a get(). After the get() the entry was not seen to be
   * in the client's cache. This turned out to be expected behavior, but we now have this test to
   * guarantee that the product behaves as expected.
   */
  @Test
  public void testGetInClientCreatesEntry() throws Exception {
    // start server first
    PORT1 = initServerCache(false);
    createClientCache(NetworkUtils.getServerHostName(), PORT1);
    registerInterestForInvalidatesInBothTheRegions();
    Region region = static_cache.getRegion(REGION_NAME1);
    populateCache();
    region.put("invalidationKey", "invalidationValue");

    region.localDestroy("invalidationKey");
    assertThat(region.containsKey("invalidationKey")).isFalse();

    region.invalidate("invalidationKey");
    assertThat(region.containsKey("invalidationKey")).isTrue();

    Object value = region.get("invalidationKey");
    assertThat(value).isNull();
    assertThat(region.containsKeyOnServer("invalidationKey")).isTrue();
  }

  /**
   * GEODE-478 - large payloads are rejected by client->server
   */
  @Test
  public void testLargeMessageIsRejected() throws Exception {
    PORT1 = initServerCache(false);
    createClientCache(NetworkUtils.getServerHostName(), PORT1);
    Region region = static_cache.getRegion(REGION_NAME1);
    Op operation = new Op() {
      @Override
      public Object attempt(Connection cnx) throws Exception {
        throw new MessageTooLargeException("message is too big");
      }

      @Override
      public boolean useThreadLocalConnection() {
        return false;
      }
    };
    try {
      ((LocalRegion) region).getServerProxy().getPool().execute(operation);
    } catch (GemFireIOException e) {
      assertTrue(e.getCause() instanceof MessageTooLargeException);
      return;
    }
    fail("expected an exception to be thrown");
  }

  /**
   * Create cache, create pool, notify-by-subscription=false, create a region and on client and on
   * server. Do not attach pool to region , populate some entries on region both on client and
   * server. Update the entries on server the client. The client should not have entry invalidate.
   */
  @Test
  public void testInvalidatesPropagateOnRegionHavingNoPool() {
    // start server first
    PORT1 = initServerCache(false);
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new ClientServerMiscDUnitTestBase().createCache(props);
    String host = NetworkUtils.getServerHostName();
    PoolImpl p =
        (PoolImpl) PoolManager.createFactory().addServer(host, PORT1).setSubscriptionEnabled(true)
            .setThreadLocalConnections(true).setReadTimeout(1000).setSocketBufferSize(32768)
            .setMinConnections(3).setSubscriptionRedundancy(-1).setPingInterval(2000)
            // .setRetryAttempts(5)
            // .setRetryInterval(2000)
            .create("testInvalidatesPropagateOnRegionHavingNoPool");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    // factory.setPoolName(p.getName());

    attrs = factory.create();
    final Region region1 = getCache().createRegion(REGION_NAME1, attrs);
    final Region region2 = getCache().createRegion(REGION_NAME2, attrs);
    assertNotNull(region1);
    assertNotNull(region2);
    pool = p;
    conn = pool.acquireConnection();
    assertNotNull(conn);

    populateCache();
    server1.invoke(() -> ClientServerMiscDUnitTestBase.put());

    await().until(() -> {
      Object val = region1.getEntry(k1).getValue();
      return k1.equals(val);
    });

    await().until(() -> {
      Object val = region1.getEntry(k2).getValue();
      return k2.equals(val);
    });

    await().until(() -> {
      Object val = region2.getEntry(k1).getValue();
      return k1.equals(val);
    });

    await().until(() -> {
      Object val = region2.getEntry(k2).getValue();
      return k2.equals(val);
    });

    // assertIndexDetailsEquals(region2.getEntry(k2).getValue(), k2);
  }

  /**
   * Create proxy before cache creation, create cache, create two regions, attach same bridge writer
   * to both of the regions Region interests AL_KEYS on both the regions,
   * notify-by-subscription=true . The CCP should have both the regions in interest list.
   *
   */

  @Test
  public void testProxyCreationBeforeCacheCreation() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    PORT1 = initServerCache(true);
    String host = NetworkUtils.getServerHostName();
    Pool p = PoolManager.createFactory().addServer(host, PORT1).setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(-1)
        // .setRetryAttempts(5)
        .create("testProxyCreationBeforeCacheCreationPool");

    Cache cache = getCache();
    assertNotNull(cache);

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());

    RegionAttributes myAttrs = factory.create();
    Region region1 = cache.createRegion(REGION_NAME1, myAttrs);
    Region region2 = cache.createRegion(REGION_NAME2, myAttrs);
    assertNotNull(region1);
    assertNotNull(region2);
    // region1.registerInterest(CacheClientProxy.ALL_KEYS);
    region2.registerInterest("ALL_KEYS");
    Wait.pause(6000);
    server1.invoke(() -> ClientServerMiscDUnitTestBase.verifyInterestListOnServer());

  }

  /**
   *
   * bug 35380: Cycling a DistributedSystem with an initialized pool causes interest registration
   * NPE
   *
   * Test Scenario:
   *
   * Create a DistributedSystem (DS1). Create a pool, initialize (creates a proxy with DS1 memberid)
   * Disconnect DS1. Create a DistributedSystem (DS2). Create a Region with pool, it attempts to
   * register interest using DS2 memberid, gets NPE.
   *
   */
  @Test
  public void testSystemCanBeCycledWithAnInitializedPool() {
    // work around GEODE-477
    IgnoredException.addIgnoredException("Connection reset");
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);

    PORT1 = initServerCache(true);
    String host = NetworkUtils.getServerHostName();
    Pool p = PoolManager.createFactory().addServer(host, PORT1).setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(-1)
        // .setRetryAttempts(5)
        .create("testBug35380Pool");

    Cache cache = getCache();

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());

    RegionAttributes myAttrs = factory.create();
    Region region1 = cache.createRegion(REGION_NAME1, myAttrs);
    Region region2 = cache.createRegion(REGION_NAME2, myAttrs);
    assertNotNull(region1);
    assertNotNull(region2);

    region2.registerInterest("ALL_KEYS");

    ds.disconnect();
    Properties prop = new Properties();
    prop.setProperty(MCAST_PORT, "0");
    prop.setProperty(LOCATORS, "");
    ds = getSystem(prop);

    final Cache cacheForLamda = getCache();
    assertNotNull(cacheForLamda);

    AttributesFactory factory1 = new AttributesFactory();
    factory1.setScope(Scope.DISTRIBUTED_ACK);
    // reuse writer from prev DS
    factory1.setPoolName(p.getName());

    final RegionAttributes attrs1 = factory1.create();
    assertThatThrownBy(() -> cacheForLamda.createRegion(REGION_NAME1, attrs1))
        .isInstanceOfAny(IllegalStateException.class, DistributedSystemDisconnectedException.class);
  }

  @Test(expected = GemFireConfigException.class)
  public void clientIsPreventedFromConnectingToLocatorAsServer() {
    IgnoredException.addIgnoredException("Improperly configured client detected");
    ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
    clientCacheFactory.addPoolServer("localhost", DistributedTestUtils.getDUnitLocatorPort());
    clientCacheFactory.setPoolSubscriptionEnabled(true);
    getClientCache(clientCacheFactory);
    Region region = ((ClientCache) cache).createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create(REGION_NAME1);
    region.registerInterest(k1);
  }


  private void createCache(Properties props) {
    createCacheV(props);
  }

  private Cache createCacheV(Properties props) {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    Cache cache = getCache();
    assertNotNull(cache);
    return cache;
  }

  public static void createClientCacheV(String h, int port) {
    _createClientCache(h, false, -1, port);
  }

  public static void createEmptyClientCache(String h, int... ports) {
    _createClientCache(h, false, -1, ports);
  }

  public static Pool createClientCache(String h, int... ports) {
    return _createClientCache(h, false, -1, ports);
  }

  public static Pool createClientCache(String h, int subscriptionAckInterval, boolean empty,
      int... ports) {
    return _createClientCache(h, empty, subscriptionAckInterval, ports);
  }

  private static PoolFactory addServers(PoolFactory factory, String h, int... ports) {
    for (int port : ports) {
      factory.addServer(h, port);
    }
    return factory;
  }

  public static Pool _createClientCache(String h, boolean empty, int subscriptionAckInterval,
      int... ports) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(LOG_LEVEL, DUnitLauncher.logLevel);
    Cache cache = new ClientServerMiscDUnitTestBase().createCacheV(props);
    ClientServerMiscDUnitTestBase.static_cache = cache;
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
        "true");
    PoolImpl p;
    try {
      PoolFactory poolFactory = PoolManager.createFactory();
      addServers(poolFactory, h, ports).setSubscriptionEnabled(true).setThreadLocalConnections(true)
          .setReadTimeout(5000).setSocketBufferSize(32768).setMinConnections(3)
          .setSubscriptionRedundancy(1).setPingInterval(2000);
      // .setRetryAttempts(5)
      // .setRetryInterval(2000)
      if (subscriptionAckInterval > 0) {
        poolFactory.setSubscriptionAckInterval(subscriptionAckInterval);
      }
      p = (PoolImpl) poolFactory.create("ClientServerMiscDUnitTestPool");

      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      if (empty) {
        factory.setDataPolicy(DataPolicy.EMPTY);
      }
      factory.setPoolName(p.getName());

      attrs = factory.create();
    } finally {
      System.getProperties()
          .remove(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints");
    }

    Region region1 = cache.createRegion(REGION_NAME1, attrs);
    Region region2 = cache.createRegion(REGION_NAME2, attrs);
    Region prRegion = cache.createRegion(PR_REGION_NAME, attrs);
    assertNotNull(region1);
    assertNotNull(region2);
    assertNotNull(prRegion);
    pool = p;

    await().until(() -> {
      try {
        conn = pool.acquireConnection();
        return conn != null;
      } catch (NoAvailableServersException e) {
        return false;
      }
    });

    return p;
  }

  static class MemberIDVerifier extends CacheListenerAdapter {
    boolean memberIDNotReceived = true;
    boolean eventReceived = false;

    @Override
    public void afterCreate(EntryEvent event) {
      eventReceived(event);
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      eventReceived(event);
    }

    private void eventReceived(EntryEvent event) {
      eventReceived = true;
      DistributedMember memberID = event.getDistributedMember();
      memberIDNotReceived = (memberID == null);
      // System.out.println("received event " + event);
    }

    public void reset() {
      memberIDNotReceived = true;
      eventReceived = false;
    }
  }

  public static void dumpPoolIdentifiers() throws Exception {
    // duplicate events were received, so let's look at the thread identifiers we have
    PoolImpl pool = (PoolImpl) PoolManager.find("ClientServerMiscDUnitTestPool");

    Map seqMap = pool.getThreadIdToSequenceIdMap();
    for (Object o : seqMap.keySet()) {
      ThreadIdentifier tid = (ThreadIdentifier) o;
      byte[] memberBytes = tid.getMembershipID();
      dumpMemberId(tid, memberBytes);
    }
  }

  public static void dumpMemberId(Object holder, byte[] memberBytes) throws Exception {
    byte[] newBytes = new byte[memberBytes.length + 17];
    System.arraycopy(memberBytes, 0, newBytes, 0, memberBytes.length);
    ByteArrayInputStream bais = new ByteArrayInputStream(newBytes);
    DataInputStream dataIn = new DataInputStream(bais);
    InternalDistributedMember memberId = InternalDistributedMember.readEssentialData(dataIn);
    String sb = "<" + Thread.currentThread().getName() + "> " + holder
        + " is " + memberId + " byte count = " + memberBytes.length
        + " bytes = " + Arrays.toString(memberBytes);
    System.out.println(sb);
  }


  public static Integer createServerCache(Boolean notifyBySubscription, Integer maxThreads,
      boolean isHA) throws Exception {
    Cache cache = new ClientServerMiscDUnitTestBase().createCacheV(new Properties());
    unsetSlowDispatcherFlag();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEnableConflation(true);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setConcurrencyChecksEnabled(true);
    RegionAttributes myAttrs = factory.create();
    Region r1 = cache.createRegion(REGION_NAME1, myAttrs);
    Region r2 = cache.createRegion(REGION_NAME2, myAttrs);
    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.PARTITION);
    if (isHA) {
      PartitionAttributesFactory paf = new PartitionAttributesFactory().setRedundantCopies(1);
      factory.setPartitionAttributes(paf.create());
    }
    RegionAttributes prAttrs = factory.create();

    Region pr = cache.createRegion(PR_REGION_NAME, prAttrs);
    assertNotNull(r1);
    assertNotNull(r2);
    assertNotNull(pr);

    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    r1.getCache().getDistributedSystem().getLogWriter().info("Starting server on port " + port);
    server.setPort(port);
    server.setMaxThreads(maxThreads);
    server.setNotifyBySubscription(notifyBySubscription);
    server.start();
    r1.getCache().getDistributedSystem().getLogWriter()
        .info("Started server on port " + server.getPort());
    return server.getPort();

  }

  protected int getMaxThreads() {
    return 0;
  }

  public static void registerInterest() {
    Cache cache = new ClientServerMiscDUnitTestBase().getCache();
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
    assertNotNull(r);
    r.registerInterest("ALL_KEYS");
    r.getAttributesMutator().addCacheListener(new MemberIDVerifier());
  }

  private static void registerInterestForInvalidatesInBothTheRegions() {
    try {
      Cache cache = new ClientServerMiscDUnitTestBase().getCache();
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      assertNotNull(r1);
      Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      assertNotNull(r2);
      r1.registerInterestForAllKeys(InterestResultPolicy.KEYS, false, false);
      r2.registerInterestForAllKeys(InterestResultPolicy.KEYS, false, false);
    } catch (CacheWriterException e) {
      e.printStackTrace();
      fail("Test failed due to CacheWriterException during registerInterestnBothRegions" + e);
    }
  }

  private static void registerInterestInBothTheRegions() {
    try {
      Cache cache = new ClientServerMiscDUnitTestBase().getCache();
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      assertNotNull(r1);
      Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      assertNotNull(r2);
      r1.registerInterestForAllKeys();
      r2.registerInterestForAllKeys();
    } catch (CacheWriterException e) {
      e.printStackTrace();
      fail("Test failed due to CacheWriterException during registerInterestnBothRegions" + e);
    }
  }

  private static void closeRegion1() {
    try {
      Cache cache = new ClientServerMiscDUnitTestBase().getCache();
      Region<String, String> r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      assertNotNull(r1);
      r1.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to Exception during closeRegion1" + e);
    }
  }

  private static void closeBothRegions() {
    try {
      Cache cache = new ClientServerMiscDUnitTestBase().getCache();
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      Region pr = cache.getRegion(Region.SEPARATOR + PR_REGION_NAME);
      assertNotNull(r1);
      assertNotNull(r2);
      assertNotNull(pr);
      r1.close();
      r2.close();
      pr.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to Exception during closeBothRegions" + e);
    }
  }

  private static void destroyRegion1() {
    try {
      Cache cache = new ClientServerMiscDUnitTestBase().getCache();
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      assertNotNull(r1);
      r1.destroyRegion();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to Exception during closeBothRegions" + e);
    }
  }

  private static void destroyRegion2() {
    try {
      Cache cache = new ClientServerMiscDUnitTestBase().getCache();
      Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      assertNotNull(r2);
      r2.destroyRegion();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to Exception during closeBothRegions" + e);
    }
  }

  private static void destroyPRRegion() {
    try {
      Cache cache = new ClientServerMiscDUnitTestBase().getCache();
      Region r2 = cache.getRegion(Region.SEPARATOR + PR_REGION_NAME);
      assertNotNull(r2);
      r2.destroyRegion();
    } catch (Exception e) {
      // e.printStackTrace();
      fail("Test failed due to Exception during closeBothRegions" + e);
    }
  }

  private static void verifyInterestListOnServer() {
    try {
      Cache cache = new ClientServerMiscDUnitTestBase().getCache();
      assertEquals("More than one BridgeServer", 1, cache.getCacheServers().size());
      CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
      assertNotNull(bs);
      assertNotNull(bs.getAcceptor());
      assertNotNull(bs.getAcceptor().getCacheClientNotifier());
      for (CacheClientProxy ccp : bs.getAcceptor().getCacheClientNotifier().getClientProxies()) {
        // CCP should not contain region1
        Set<String> akr = ccp.cils[RegisterInterestTracker.interestListIndex].regions;
        assertNotNull(akr);
        assertTrue(!akr.contains(Region.SEPARATOR + REGION_NAME1));
        // CCP should contain region2
        assertTrue(akr.contains(Region.SEPARATOR + REGION_NAME2));
        assertEquals(1, akr.size());
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("while setting verifyInterestListOnServer  " + ex);
    }
  }

  private static void verifyNoCacheClientProxyOnServer() {
    try {
      Cache cache = new ClientServerMiscDUnitTestBase().getCache();
      assertEquals("More than one BridgeServer", 1, cache.getCacheServers().size());
      CacheServerImpl cacheServer = (CacheServerImpl) cache.getCacheServers().iterator().next();
      assertNotNull(cacheServer);
      assertNotNull(cacheServer.getAcceptor());
      final CacheClientNotifier ccn = cacheServer.getAcceptor().getCacheClientNotifier();

      assertNotNull(ccn);
      await()
          .until(() -> ccn.getClientProxies().size() == 0);
    } catch (Exception ex) {
      System.out.println("The size of the client proxies != 0");
      OSProcess.printStacks(0);
      throw ex;
    }
  }

  private static void verifyCacheClientProxyOnServer(String regionName) {
    try {
      Cache cache = new ClientServerMiscDUnitTestBase().getCache();
      assertNull(cache.getRegion(Region.SEPARATOR + regionName));
      verifyCacheClientProxyOnServer();

      // assertIndexDetailsEquals(1,
      // bs.getAcceptor().getCacheClientNotifier().getClientProxies().size());
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("while setting verifyNoCacheClientProxyOnServer  " + ex);
    }
  }

  private static void verifyCacheClientProxyOnServer() {
    Cache cache = new ClientServerMiscDUnitTestBase().getCache();
    assertEquals("More than one BridgeServer", 1, cache.getCacheServers().size());
    CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertNotNull(bs);
    assertNotNull(bs.getAcceptor());
    final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();

    assertNotNull(ccn);

    await()
        .until(() -> ccn.getClientProxies().size() == 1);
  }

  public static void populateCache() {
    Cache cache = new ClientServerMiscDUnitTestBase().getCache();
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
    Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
    assertNotNull(r1);
    assertNotNull(r2);

    if (!r1.containsKey(k1))
      r1.create(k1, k1);
    if (!r1.containsKey(k2))
      r1.create(k2, k2);
    if (!r2.containsKey(k1))
      r2.create(k1, k1);
    if (!r2.containsKey(k2))
      r2.create(k2, k2);

    assertEquals(r1.getEntry(k1).getValue(), k1);
    assertEquals(r1.getEntry(k2).getValue(), k2);
    assertEquals(r2.getEntry(k1).getValue(), k1);
    assertEquals(r2.getEntry(k2).getValue(), k2);
  }

  public static void put() {
    Cache cache = new ClientServerMiscDUnitTestBase().getCache();
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
    Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
    assertNotNull(r1);
    assertNotNull(r2);

    r1.put(k1, server_k1);
    r1.put(k2, server_k2);

    r2.put(k1, server_k1);
    r2.put(k2, server_k2);

    assertEquals(r1.getEntry(k1).getValue(), server_k1);
    assertEquals(r1.getEntry(k2).getValue(), server_k2);
    assertEquals(r2.getEntry(k1).getValue(), server_k1);
    assertEquals(r2.getEntry(k2).getValue(), server_k2);
  }

  static void putForClient() {
    Cache cache = new ClientServerMiscDUnitTestBase().getCache();
    Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
    if (r2 == null) {
      r2 = cache.createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME2);
    }

    r2.put(k1, "client2_k1");
    r2.put(k2, "client2_k2");
  }

  private static void verifyUpdates() {
    Cache cache = new ClientServerMiscDUnitTestBase().getCache();
    final Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
    final Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
    assertNotNull(r1);
    assertNotNull(r2);

    // no interest registered in region1 - it should hold client values, which are
    // the same as the keys
    await().until(() -> {
      Object val = r1.getEntry(k1).getValue();
      return k1.equals(val);
    });

    await().until(() -> {
      Object val = r1.getEntry(k2).getValue();
      return k2.equals(val);
    });

    // interest was registered in region2 - it should contain server values
    await().until(() -> {
      Object val = r2.getEntry(k1).getValue();
      return server_k1.equals(val);
    });

    await().until(() -> {
      Object val = r2.getEntry(k2).getValue();
      return server_k2.equals(val);
    });

    // events should have contained a memberID
    MemberIDVerifier verifier = (MemberIDVerifier) ((LocalRegion) r2).getCacheListener();
    assertTrue("client should have received a listener event", verifier.eventReceived);
    assertFalse("client received an update but the event had no member id",
        verifier.memberIDNotReceived);
    verifier.reset();

  }

  private static void verifyInvalidatesOnBothRegions() {
    try {
      Cache cache = new ClientServerMiscDUnitTestBase().getCache();
      final Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      final Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      assertNotNull(r1);
      assertNotNull(r2);

      await()
          .until(() -> r1.getEntry(k1).getValue() == null);

      await()
          .until(() -> r1.getEntry(k2).getValue() == null);

      await()
          .until(() -> r2.getEntry(k1).getValue() == null);

      await()
          .until(() -> r2.getEntry(k2).getValue() == null);

    } catch (Exception ex) {
      fail("failed while verifyInvalidatesOnBothRegions()" + ex);
    }
  }

  private static void verifyUpdatesOnRegion2() {
    try {
      Cache cache = new ClientServerMiscDUnitTestBase().getCache();
      final Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      assertNotNull(r2);

      await()
          .until(() -> server_k1.equals(r2.getEntry(k1).getValue()));

      await()
          .until(() -> server_k2.equals(r2.getEntry(k2).getValue()));

      // assertIndexDetailsEquals(server_k2, r2.getEntry(k2).getValue());
    } catch (Exception ex) {
      fail("failed while verifyUpdatesOnRegion2()" + ex);
    }
  }

  /**
   * set the boolean for starting the dispatcher thread a bit later to FALSE. This is just a
   * precaution in case any test set it to true and did not unset it on completion.
   *
   */
  private static void unsetSlowDispatcherFlag() {
    CacheClientProxy.isSlowStartForTesting = false;
  }

  @Test
  public void testOnSeverMethodsWithProxyClient() throws Exception {
    testOnServerMethods(false, false);
  }

  @Test
  public void testOnSeverMethodsWithCachingProxyClient() throws Exception {
    testOnServerMethods(true, false);
  }

  @Test
  public void testOnSeverMethodsWithProxyClientHA() throws Exception {
    testOnServerMethods(false, true);
  }

  @Test
  public void testOnSeverMethodsWithCachingProxyClientHA() throws Exception {
    testOnServerMethods(true, true);
  }

  private void testOnServerMethods(boolean isCachingProxy, boolean isHA) throws Exception {
    int port1 = initServerCache(true, isHA); // vm0
    int port2 = initServerCache2(isHA); // vm1
    String serverName = NetworkUtils.getServerHostName();
    if (isCachingProxy) {
      createClientCache(serverName, port1, port2);
    } else {
      createEmptyClientCache(serverName, port1, port2);
    }
    if (isHA) {
      // add another server for HA scenario
      initServerCache(true, host.getVM(VersionManager.CURRENT_VERSION, 1), true);
    }
    String rName = "/" + REGION_NAME1;
    String prName = "/" + PR_REGION_NAME;

    verifyIsEmptyOnServer(rName, true);
    verifyIsEmptyOnServer(prName, true);

    int size = 10;
    putIntoRegion(rName, size, isCachingProxy);
    verifySizeOnServer(rName, size);

    putIntoRegion(prName, size, isCachingProxy);
    if (isHA) {
      server1.invoke(() -> closeMyCache());
    }
    verifySizeOnServer(prName, size);

    verifyIsEmptyOnServer(rName, false);
    verifyIsEmptyOnServer(prName, false);

    destroyEntries(rName, size);
    destroyEntries(prName, size);

    verifyIsEmptyOnServer(rName, true);
    verifyIsEmptyOnServer(prName, true);
    verifySizeOnServer(rName, 0);
    verifySizeOnServer(prName, 0);
  }

  private void putIntoRegion(String regionName, int size, boolean isCachingProxy) {
    Cache cache = getCache();
    final Region region = cache.getRegion(regionName);
    for (int i = 0; i < size; i++) {
      region.put(i, i);
    }

    if (isCachingProxy) {
      for (int i = 0; i < size; i++) {
        region.localDestroy(i, i);
      }
    }
  }

  private void destroyEntries(String regionName, int size) {
    Cache cache = getCache();
    final Region region = cache.getRegion(regionName);

    for (int i = 0; i < size; i++) {
      region.destroy(i);
    }
  }

  private void verifySizeOnServer(String regionName, int expectedSize) {
    Cache cache = getCache();
    final Region region = cache.getRegion(regionName);
    int actualSize = region.sizeOnServer();
    assertEquals(
        "sizeOnServer returns unexpected " + actualSize + " instead of expected " + expectedSize,
        expectedSize, actualSize);
  }

  private void verifyIsEmptyOnServer(String regionName, boolean expected) {
    Cache cache = getCache();
    final Region region = cache.getRegion(regionName);
    boolean isEmptyOnServer = region.isEmptyOnServer();
    assertEquals("isEmptyOnServer returns unexpected " + isEmptyOnServer + " instead of expected "
        + expected, expected, isEmptyOnServer);
  }

  private void closeMyCache() {
    Cache cache = getCache();
    cache.close();
  }

}
