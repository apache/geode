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

import static org.apache.geode.distributed.ConfigurationProperties.DELTA_PROPAGATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * This test verifies the per-client notify-by-subscription (NBS) override functionality along with
 * register interest new receiveValues flag. Taken from the existing ClientConflationDUnitTest.java
 * and modified.
 *
 * @since GemFire 6.0.3
 */
@Category({ClientServerTest.class})
public class ClientInterestNotifyDUnitTest extends JUnit4DistributedTestCase {

  class EventListener extends CacheListenerAdapter {
    public EventListener(String name) {
      m_name = name;
    }

    private String m_name = null;
    private int m_creates = 0;
    private int m_updates = 0;
    private int m_invalidates = 0;
    private int m_destroys = 0;

    public void afterCreate(EntryEvent event) {
      m_creates++;
    }

    public void afterUpdate(EntryEvent event) {
      m_updates++;
    }

    public void afterInvalidate(EntryEvent event) {
      m_invalidates++;
    }

    public void afterDestroy(EntryEvent event) {
      m_destroys++;
    }

    public void reset() {
      m_creates = 0;
      m_updates = 0;
      m_invalidates = 0;
      m_destroys = 0;
    }

    // validate expected event counts with actual counted
    public void validate(int creates, int updates, int invalidates, int destroys) {
      // Wait for the last destroy event to arrive.
      try {
        await().until(() -> {
          return (destroys == m_destroys);
        });
      } catch (Exception ex) {
        // The event is not received in a given wait time.
        // The check will be done below to report the valid reason.
      }

      GemFireCacheImpl.getInstance().getLogger()
          .info(m_name + ": creates: expected=" + creates + ", actual=" + m_creates);
      GemFireCacheImpl.getInstance().getLogger()
          .info(m_name + ": updates: expected=" + updates + ", actual=" + m_updates);
      GemFireCacheImpl.getInstance().getLogger()
          .info(m_name + ": invalidates: expected=" + invalidates + ", actual=" + m_invalidates);
      GemFireCacheImpl.getInstance().getLogger()
          .info(m_name + ": destroys: expected=" + destroys + ", actual=" + m_destroys);

      assertEquals("Creates :", creates, m_creates);
      assertEquals("Updates :", updates, m_updates);
      assertEquals("Invalidates :", invalidates, m_invalidates);
      assertEquals("Destroys :", destroys, m_destroys);
    }
  }

  // server is on master controller
  VM vm0 = null; // feeder
  VM vm1 = null; // client1
  /*
   * VM vm2 = null; // client2 VM vm3 = null; // client3
   */
  private static Cache cacheServer = null;
  private int PORT;
  private static int poolNameCounter = 0;
  // Region 1 only does interest registrations with receiveValues flag set to true
  private static final String REGION_NAME1 = "ClientInterestNotifyDUnitTest_region1";
  // Region 2 only does interest registrations with receiveValues flag set to false
  private static final String REGION_NAME2 = "ClientInterestNotifyDUnitTest_region2";
  // Region 3 does NOT register any interest
  private static final String REGION_NAME3 = "ClientInterestNotifyDUnitTest_region3";

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
  }

  private Cache createCache(Properties props) throws Exception {
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    DistributedSystem ds = DistributedSystem.connect(props);
    Cache cache = CacheFactory.create(ds);
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
    return cache;
  }

  @Test
  public void testInterestNotify() throws Exception {
    performSteps();
  }

  private void performSteps() throws Exception {

    // Server is created with notify-by-subscription (NBS) set to false.
    PORT = createServerCache();

    Host host = Host.getHost(0);
    // Create a feeder.
    vm0.invoke(() -> ClientInterestNotifyDUnitTest
        .createClientCacheFeeder(NetworkUtils.getServerHostName(host), new Integer(PORT)));

    // Client 1 overrides NBS to true.
    // Client 2 "overrides" NSB to false.
    // Client 3 uses the default NBS which is false on the server.

    vm1.invoke(() -> ClientInterestNotifyDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(host), new Integer(PORT), "ClientOn"));

    // Feeder doFeed does one put on one key for each of the 3 regions so
    // that the following client RI with ALL_KEYS and KEYS_VALUE result works.

    vm0.invoke(() -> ClientInterestNotifyDUnitTest.doFeed());

    // RI on ALL_KEYS with InterestResultPolicy KEYS_VALUES.

    vm1.invoke(() -> ClientInterestNotifyDUnitTest.registerInterest());

    // Get key for region 3 for all clients to check no unwanted notifications
    // arrive on client 1 region 3 since we do not register interest on any
    // client but notifications should arrive for client 2 and client 3.

    vm1.invoke(() -> ClientInterestNotifyDUnitTest.getEntries());

    // Feeder doEntryOps does 2 puts, 1 invalidate and 1 destroy on a
    // single key for each of the 3 regions.

    vm0.invoke(() -> ClientInterestNotifyDUnitTest.doEntryOps());

    waitForQueuesToDrain();

    // Unregister interest to check it works and no extra notifications received.

    vm1.invoke(() -> ClientInterestNotifyDUnitTest.unregisterInterest());

    // Feeder doEntryOps again does 2 puts, 1 invalidate and 1 destroy on a
    // single key for each of the 3 regions while no interest on the clients.

    vm0.invoke(() -> ClientInterestNotifyDUnitTest.doEntryOps());

    assertAllQueuesEmpty(); // since no client has registered interest

    // Re-register interest on all clients except for region 3 again.

    vm1.invoke(() -> ClientInterestNotifyDUnitTest.registerInterest());

    // Feeder doEntryOps again does 2 puts, 1 invalidate and 1 destroy on a
    // single key for each of the 3 regions after clients re-register interest.

    vm0.invoke(() -> ClientInterestNotifyDUnitTest.doEntryOps());

    waitForQueuesToDrain();

    // doValidation on all listeners:

    // Client invokes listeners as follows:
    // 1. For update notification:
    // a. afterCreate() if the key does not exist in the local cache.
    // b. afterUpdate() if the key exists even if the entry is invalid.
    // 2. For invalidate notification:
    // a. afterInvalidate() only if the key/entry exists and is not already invalid.
    // 3. For destroy notification:
    // a. afterDestroy() only if the key/entry is not already destroyed.

    // RI is done for ALL_KEYS with InterestResultPolicy KEYS_VALUES.

    // For region 1 we set the RI receiveValues param to true so
    // that it behaves like notify-by-subscription is set to true.
    // For region 2 we set the RI receiveValues param to false so
    // that it behaves like notify-by-subscription is set to false but
    // only for matching interested keys.
    // For region 3 we do not register any interest to check that it
    // does not get any events other than those expected from the
    // server's notify-by-subscription setting which may be overridden.

    // Region 3 calls a single afterCreate() because of the single get() done.
    // This is to verify client 1 does not receive any unwanted event traffic.
    // Region 3 of clients 2 and 3 receive an invalidate and a destroy each
    // because NBS is set to false for those clients.

    vm1.invoke(() -> ClientInterestNotifyDUnitTest.doValidation(REGION_NAME1, 1, 3, 2, 2));
    vm1.invoke(() -> ClientInterestNotifyDUnitTest.doValidation(REGION_NAME2, 0, 0, 1, 1));
    vm1.invoke(() -> ClientInterestNotifyDUnitTest.doValidation(REGION_NAME3, 1, 0, 0, 0));

  }

  /**
   * create properties for a loner VM
   */
  private static Properties createProperties1(/* String nbs */) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    return props;
  }


  private static void createPool2(String host, AttributesFactory factory, Integer port) {
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, port.intValue()).setSubscriptionEnabled(true).setThreadLocalConnections(true)
        .setReadTimeout(10000).setSocketBufferSize(32768).setPingInterval(1000).setMinConnections(3)
        .setSubscriptionRedundancy(-1);
    Pool pool = pf.create("superpoolish" + (poolNameCounter++));
    factory.setPoolName(pool.getName());
  }

  /**
   * Do validation based on feeder events received by the client
   */
  public static void doValidation(String region, int creates, int updates, int invalidates,
      int destroys) {
    Cache cacheClient = GemFireCacheImpl.getInstance();
    EventListener listener = null;
    listener = (EventListener) cacheClient.getRegion(region).getAttributes().getCacheListeners()[0];
    listener.validate(creates, updates, invalidates, destroys);
  }

  /**
   * create client with 3 regions each with a unique listener
   *
   */
  public static void createClientCache(String host, Integer port, /* String nbs, */
      String name) throws Exception {
    ClientInterestNotifyDUnitTest test = new ClientInterestNotifyDUnitTest();
    Cache cacheClient = test.createCache(createProperties1(/* nbs */));
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(false);
    createPool2(host, factory, port);
    factory.setCacheListener(test.new EventListener(name + REGION_NAME1));
    RegionAttributes attrs = factory.create();
    cacheClient.createRegion(REGION_NAME1, attrs);

    factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(false);
    createPool2(host, factory, port);
    factory.setCacheListener(test.new EventListener(name + REGION_NAME2));
    attrs = factory.create();
    cacheClient.createRegion(REGION_NAME2, attrs);

    factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(false);
    createPool2(host, factory, port);
    factory.setCacheListener(test.new EventListener(name + REGION_NAME3));
    attrs = factory.create();
    cacheClient.createRegion(REGION_NAME3, attrs);
  }

  public static void createClientCacheFeeder(String host, Integer port) throws Exception {
    ClientInterestNotifyDUnitTest test = new ClientInterestNotifyDUnitTest();
    Cache cacheFeeder = test.createCache(createProperties1(
    /* DistributionConfig.NOTIFY_BY_SUBSCRIPTION_OVERRIDE_PROP_VALUE_DEFAULT */));
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(false);
    createPool2(host, factory, port);
    RegionAttributes attrs = factory.create();
    cacheFeeder.createRegion(REGION_NAME1, attrs);
    cacheFeeder.createRegion(REGION_NAME2, attrs);
    cacheFeeder.createRegion(REGION_NAME3, attrs);
  }

  /**
   * Assert all queues are empty to aid later assertion for listener event counts.
   */

  // NOTE: replaced with waitForQueuesToDrain() using waitcriterion to avoid
  // occasional failures in precheckins and cruisecontrol.

  public static void assertAllQueuesEmpty() {
    Iterator servers = cacheServer.getCacheServers().iterator();
    assertTrue("No servers found!", servers.hasNext());
    while (servers.hasNext()) {
      Iterator proxies = ((CacheServerImpl) servers.next()).getAcceptor().getCacheClientNotifier()
          .getClientProxies().iterator();
      assertTrue("No proxies found!", proxies.hasNext());
      while (proxies.hasNext()) {
        int qsize = ((CacheClientProxy) proxies.next()).getQueueSize();
        assertTrue("Queue size expected to be zero but is " + qsize, qsize == 0);
      }
    }
  }

  public static void waitForQueuesToDrain() {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;

      public boolean done() {
        // assume a single cache server as configured in this test
        CacheServerImpl bridgeServer =
            (CacheServerImpl) cacheServer.getCacheServers().iterator().next();
        if (bridgeServer == null) {
          excuse = "No Cache Server";
          return false;
        }
        Iterator proxies =
            bridgeServer.getAcceptor().getCacheClientNotifier().getClientProxies().iterator();
        if (!proxies.hasNext()) {
          excuse = "No CacheClientProxy";
          return false;
        }
        while (proxies.hasNext()) {
          CacheClientProxy proxy = (CacheClientProxy) proxies.next();
          if (proxy == null) {
            excuse = "No CacheClientProxy";
            return false;
          }
          // Verify the queue size
          int sz = proxy.getQueueSize();
          if (0 != sz) {
            excuse = "Queue did not drain. Expected size = 0, actual = " + sz + "for " + proxy;
            return false;
          }
        }
        return true;
      }

      public String description() {
        return excuse;
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
  }

  /**
   * create a server cache and start the server
   *
   */
  public static Integer createServerCache() throws Exception {
    ClientInterestNotifyDUnitTest test = new ClientInterestNotifyDUnitTest();
    Properties props = new Properties();
    props.setProperty(DELTA_PROPAGATION, "false");
    cacheServer = test.createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setConcurrencyChecksEnabled(false);
    RegionAttributes attrs = factory.create();
    cacheServer.createRegion(REGION_NAME1, attrs);
    cacheServer.createRegion(REGION_NAME2, attrs);
    cacheServer.createRegion(REGION_NAME3, attrs);
    CacheServer server = cacheServer.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.setSocketBufferSize(32768);
    server.start();
    return new Integer(server.getPort());
  }

  /**
   * close the client cache
   *
   */
  public static void closeCache() {
    Cache cacheClient = GemFireCacheImpl.getInstance();
    if (cacheClient != null && !cacheClient.isClosed()) {
      cacheClient.close();
      cacheClient.getDistributedSystem().disconnect();
    }
  }

  /**
   * close the server cache
   *
   */
  public static void closeCacheServer() {
    if (cacheServer != null && !cacheServer.isClosed()) {
      cacheServer.close();
      cacheServer.getDistributedSystem().disconnect();
    }
  }

  /**
   * register interest with the server on ALL_KEYS
   *
   */
  public static void registerInterest() {
    try {
      Cache cacheClient = GemFireCacheImpl.getInstance();
      Region region1 = cacheClient.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region region2 = cacheClient.getRegion(Region.SEPARATOR + REGION_NAME2);
      // We intentionally do not register interest in region 3 to check no events recvd.
      Region region3 = cacheClient.getRegion(Region.SEPARATOR + REGION_NAME3);
      assertTrue(region1 != null);
      assertTrue(region2 != null);
      assertTrue(region3 != null);
      region1.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES, false, true);
      region2.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES, false, false);
      // We intentionally do not register interest in region 3 to check no events recvd.
      // region3.registerInterestBlah();
    } catch (CacheWriterException e) {
      fail("test failed due to " + e);
    }
  }


  /**
   * register interest with the server on ALL_KEYS
   *
   */

  public static void unregisterInterest() {
    try {
      Cache cacheClient = GemFireCacheImpl.getInstance();
      Region region1 = cacheClient.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region region2 = cacheClient.getRegion(Region.SEPARATOR + REGION_NAME2);
      region1.unregisterInterest("ALL_KEYS");
      region2.unregisterInterest("ALL_KEYS");
    } catch (CacheWriterException e) {
      fail("test failed due to " + e);
    }
  }

  /**
   * Do 2 puts, 1 invalidate and 1 destroy for a key "key-1"
   */
  public static void doEntryOps() {
    try {
      LogWriterUtils.getLogWriter().info("Putting entries...");
      Cache cacheClient = GemFireCacheImpl.getInstance();
      Region r1 = cacheClient.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region r2 = cacheClient.getRegion(Region.SEPARATOR + REGION_NAME2);
      Region r3 = cacheClient.getRegion(Region.SEPARATOR + REGION_NAME3);
      r1.put("key-1", "11");
      r2.put("key-1", "11");
      r3.put("key-1", "11");
      r1.put("key-1", "22");
      r2.put("key-1", "22");
      r3.put("key-1", "22");
      r1.invalidate("key-1");
      r2.invalidate("key-1");
      r3.invalidate("key-1");
      r1.destroy("key-1");
      r2.destroy("key-1");
      r3.destroy("key-1");
    } catch (Exception ex) {
      ex.printStackTrace();
      Assert.fail("failed while region doing ops", ex);
    }
  }

  /**
   * Do initial puts
   */
  public static void doFeed() {
    try {
      LogWriterUtils.getLogWriter().info("Putting entries...");
      Cache cacheClient = GemFireCacheImpl.getInstance();
      Region r1 = cacheClient.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region r2 = cacheClient.getRegion(Region.SEPARATOR + REGION_NAME2);
      Region r3 = cacheClient.getRegion(Region.SEPARATOR + REGION_NAME3);
      r1.put("key-1", "00");
      r2.put("key-1", "00");
      r3.put("key-1", "00");
    } catch (Exception ex) {
      ex.printStackTrace();
      Assert.fail("failed while region doing ops", ex);
    }
  }

  /**
   * Get entries on all clients' region 3 since it does not register interest.
   */
  public static void getEntries() {
    try {
      LogWriterUtils.getLogWriter().info("Getting entries...");
      Cache cacheClient = GemFireCacheImpl.getInstance();
      Region r3 = cacheClient.getRegion(Region.SEPARATOR + REGION_NAME3);
      r3.get("key-1");
    } catch (Exception ex) {
      ex.printStackTrace();
      Assert.fail("failed while region doing ops", ex);
    }
  }

  /**
   * close the caches in tearDown
   */
  @Override
  public final void preTearDown() throws Exception {
    vm0.invoke(() -> ClientInterestNotifyDUnitTest.closeCache());
    vm1.invoke(() -> ClientInterestNotifyDUnitTest.closeCache());
    closeCacheServer();
  }
}
