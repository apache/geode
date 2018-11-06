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
package org.apache.geode.cache.query.cq.dunit;

import static org.apache.geode.cache.query.internal.cq.CqQueryImpl.testHook;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.cq.CqQueryImpl;
import org.apache.geode.cache.query.internal.cq.CqQueryImpl.TestHook;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.CertifiableTestCacheListener;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * This class tests the ContinuousQuery mechanism in GemFire. This includes the test with different
 * data activities.
 */
@Category({ClientSubscriptionTest.class})
public class CqDataUsingPoolDUnitTest extends JUnit4CacheTestCase {

  protected CqQueryUsingPoolDUnitTest cqDUnitTest = new CqQueryUsingPoolDUnitTest(); // TODO: don't
                                                                                     // do this!
  private static final Logger logger = LogService.getLogger();

  @Override
  public final void postSetUp() throws Exception {
    // avoid IllegalStateException from HandShake by connecting all vms tor
    // system before creating ConnectionPools
    getSystem();
    Invoke.invokeInEveryVM(new SerializableRunnable("getSystem") {
      @Override
      public void run() {
        getSystem();
      }
    });
    postSetUpCqDataUsingPoolDUnitTest();
  }

  protected void postSetUpCqDataUsingPoolDUnitTest() throws Exception {}

  /**
   * Tests with client acting as feeder/publisher and registering cq. Added wrt bug 37161. In case
   * of InterestList the events are not sent back to the client if its the originator, this is not
   * true for cq.
   */
  @Test
  public void testClientWithFeederAndCQ() throws Exception {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    cqDUnitTest.createServer(server);

    final int port = server.invoke(() -> CqQueryUsingPoolDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    String poolName = "testClientWithFeederAndCQ";
    cqDUnitTest.createPool(client, poolName, host0, port);

    // Create client.
    cqDUnitTest.createClient(client, port, host0);


    cqDUnitTest.createCQ(client, poolName, "testClientWithFeederAndCQ_0", cqDUnitTest.cqs[0]);
    cqDUnitTest.executeCQ(client, "testClientWithFeederAndCQ_0", false, null);

    final int size = 10;
    cqDUnitTest.createValues(client, cqDUnitTest.regions[0], size);
    cqDUnitTest.waitForCreated(client, "testClientWithFeederAndCQ_0",
        CqQueryUsingPoolDUnitTest.KEY + size);

    cqDUnitTest.validateCQ(client, "testClientWithFeederAndCQ_0",
        /* resultSize: */ CqQueryUsingPoolDUnitTest.noTest, /* creates: */ size, /* updates: */ 0,
        /* deletes; */ 0, /* queryInserts: */ size, /* queryUpdates: */ 0, /* queryDeletes: */ 0,
        /* totalEvents: */ size);

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }

  /**
   * Test for CQ Fail over/HA with redundancy level set.
   */
  @Test
  public void testCQHAWithState() throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);

    VM client = host.getVM(3);

    cqDUnitTest.createServer(server1);

    final int port1 = server1.invoke(() -> CqQueryUsingPoolDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    cqDUnitTest.createServer(server2, ports[0]);
    final int port2 = server2.invoke(() -> CqQueryUsingPoolDUnitTest.getCacheServerPort());

    // Create client - With 3 server endpoints and redundancy level set to 2.

    // Create client with redundancyLevel 1

    String poolName = "testCQHAWithState";
    cqDUnitTest.createPool(client, poolName, new String[] {host0, host0, host0},
        new int[] {port1, port2, ports[1]}, "1");

    // Create CQs.
    int numCQs = 1;
    for (int i = 0; i < numCQs; i++) {
      // Create CQs.
      cqDUnitTest.createCQ(client, poolName, "testCQHAWithState_" + i, cqDUnitTest.cqs[i]);
      cqDUnitTest.executeCQ(client, "testCQHAWithState_" + i, false, null);
    }

    Wait.pause(1 * 1000);

    int size = 10;

    // CREATE.
    cqDUnitTest.createValues(server1, cqDUnitTest.regions[0], size);
    cqDUnitTest.createValues(server1, cqDUnitTest.regions[1], size);

    for (int i = 1; i <= size; i++) {
      cqDUnitTest.waitForCreated(client, "testCQHAWithState_0", CqQueryUsingPoolDUnitTest.KEY + i);
    }

    // Clients expected initial result.
    int[] resultsCnt = new int[] {10, 1, 2};

    for (int i = 0; i < numCQs; i++) {
      cqDUnitTest.validateCQ(client, "testCQHAWithState_" + i, CqQueryUsingPoolDUnitTest.noTest,
          resultsCnt[i], 0, 0);
    }

    // Close server1.
    // To maintain the redundancy; it will make connection to endpoint-3.
    cqDUnitTest.closeServer(server1);
    Wait.pause(3 * 1000);


    // UPDATE-1.
    cqDUnitTest.createValues(server2, cqDUnitTest.regions[0], 10);
    cqDUnitTest.createValues(server2, cqDUnitTest.regions[1], 10);

    for (int i = 1; i <= size; i++) {
      cqDUnitTest.waitForUpdated(client, "testCQHAWithState_0",
          CqQueryUsingPoolDUnitTest.KEY + size);
    }

    for (int i = 0; i < numCQs; i++) {
      cqDUnitTest.validateCQ(client, "testCQHAWithState_" + i, CqQueryUsingPoolDUnitTest.noTest,
          resultsCnt[i], resultsCnt[i], CqQueryUsingPoolDUnitTest.noTest);
    }

    // Stop cq.
    cqDUnitTest.stopCQ(client, "testCQHAWithState_0");

    Wait.pause(2 * 1000);

    // UPDATE with stop.
    cqDUnitTest.createServer(server3, ports[1]);
    server3.invoke(() -> CqQueryUsingPoolDUnitTest.getCacheServerPort());
    Wait.pause(2 * 1000);

    cqDUnitTest.clearCQListenerEvents(client, "testCQHAWithState_0");

    cqDUnitTest.createValues(server2, cqDUnitTest.regions[0], 10);
    cqDUnitTest.createValues(server2, cqDUnitTest.regions[1], 10);

    // Wait for events at client.
    try {
      cqDUnitTest.waitForUpdated(client, "testCQHAWithState_0", CqQueryUsingPoolDUnitTest.KEY + 1);
      fail("Events not expected since CQ is in stop state.");
    } catch (Exception expected) {
      // Success.
    }

    cqDUnitTest.executeCQ(client, "testCQHAWithState_0", false, null);

    // Update - 2
    cqDUnitTest.createValues(server3, cqDUnitTest.regions[0], 10);
    cqDUnitTest.createValues(server3, cqDUnitTest.regions[1], 10);

    for (int i = 1; i <= size; i++) {
      cqDUnitTest.waitForUpdated(client, "testCQHAWithState_0",
          CqQueryUsingPoolDUnitTest.KEY + size);
    }

    for (int i = 0; i < numCQs; i++) {
      cqDUnitTest.validateCQ(client, "testCQHAWithState_" + i, CqQueryUsingPoolDUnitTest.noTest,
          resultsCnt[i], resultsCnt[i] * 2, CqQueryUsingPoolDUnitTest.noTest);
    }

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server2);
    cqDUnitTest.closeServer(server3);
  }

  /**
   * Tests propogation of invalidates and destorys to the clients. Bug 37242.
   */
  @Test
  public void testCQWithDestroysAndInvalidates() throws Exception {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    VM producer = host.getVM(2);
    cqDUnitTest.createServer(server, 0, true);
    final int port = server.invoke(() -> CqQueryUsingPoolDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    String poolName = "testCQWithDestroysAndInvalidates";
    cqDUnitTest.createPool(client, poolName, host0, port);

    // Create client.
    // cqDUnitTest.createClient(client, port, host0);

    // producer is not doing any thing.
    cqDUnitTest.createClient(producer, port, host0);

    final int size = 10;
    final String name = "testQuery_4";
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);

    cqDUnitTest.createCQ(client, poolName, name, cqDUnitTest.cqs[4]);
    cqDUnitTest.executeCQ(client, name, true, null);

    // do destroys and invalidates.
    server.invoke(new CacheSerializableRunnable("Create values") {
      @Override
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(cqDUnitTest.regions[0]);
        for (int i = 1; i <= 5; i++) {
          region1.destroy(CqQueryUsingPoolDUnitTest.KEY + i);
        }
      }
    });
    for (int i = 1; i <= 5; i++) {
      cqDUnitTest.waitForDestroyed(client, name, CqQueryUsingPoolDUnitTest.KEY + i);
    }
    // recreate the key values from 1 - 5
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], 5);
    // wait for all creates to arrive.
    for (int i = 1; i <= 5; i++) {
      cqDUnitTest.waitForCreated(client, name, CqQueryUsingPoolDUnitTest.KEY + i);
    }

    // do more puts to push first five key-value to disk.
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], 10);
    // do invalidates on fisrt five keys.
    server.invoke(new CacheSerializableRunnable("Create values") {
      @Override
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(cqDUnitTest.regions[0]);
        for (int i = 1; i <= 5; i++) {
          region1.invalidate(CqQueryUsingPoolDUnitTest.KEY + i);
        }
      }
    });
    // wait for invalidates now.
    for (int i = 1; i <= 5; i++) {
      cqDUnitTest.waitForInvalidated(client, name, CqQueryUsingPoolDUnitTest.KEY + i);
    }

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }

  /**
   * Tests make sure that the second client doesnt get more events then there should be. This will
   * test the fix for bug 37295.
   */
  @Test
  public void testCQWithMultipleClients() throws Exception {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client1 = host.getVM(1);
    VM client2 = host.getVM(2);
    VM client3 = host.getVM(3);

    /* Create Server and Client */
    cqDUnitTest.createServer(server);
    final int port = server.invoke(() -> CqQueryUsingPoolDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    String poolName1 = "testCQWithMultipleClients1";
    String poolName2 = "testCQWithMultipleClients2";

    cqDUnitTest.createPool(client1, poolName1, host0, port);
    cqDUnitTest.createPool(client2, poolName2, host0, port);

    /* Create CQs. and initialize the region */
    // this should stasify every thing since id is always greater than
    // zero.
    cqDUnitTest.createCQ(client1, poolName1, "testCQWithMultipleClients_0", cqDUnitTest.cqs[0]);
    cqDUnitTest.executeCQ(client1, "testCQWithMultipleClients_0", false, null);
    // should only satisfy one key-value pair in the region.
    cqDUnitTest.createCQ(client2, poolName2, "testCQWithMultipleClients_0", cqDUnitTest.cqs[1]);
    cqDUnitTest.executeCQ(client2, "testCQWithMultipleClients_0", false, null);

    int size = 10;

    // Create Values on Server.
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);

    cqDUnitTest.waitForCreated(client1, "testCQWithMultipleClients_0",
        CqQueryUsingPoolDUnitTest.KEY + 10);

    /* Validate the CQs */
    cqDUnitTest.validateCQ(client1, "testCQWithMultipleClients_0",
        /* resultSize: */ CqQueryUsingPoolDUnitTest.noTest, /* creates: */ size, /* updates: */ 0,
        /* deletes; */ 0, /* queryInserts: */ size, /* queryUpdates: */ 0, /* queryDeletes: */ 0,
        /* totalEvents: */ size);

    cqDUnitTest.waitForCreated(client2, "testCQWithMultipleClients_0",
        CqQueryUsingPoolDUnitTest.KEY + 2);

    cqDUnitTest.validateCQ(client2, "testCQWithMultipleClients_0",
        /* resultSize: */ CqQueryUsingPoolDUnitTest.noTest, /* creates: */ 1, /* updates: */ 0,
        /* deletes; */ 0, /* queryInserts: */ 1, /* queryUpdates: */ 0, /* queryDeletes: */ 0,
        /* totalEvents: */ 1);

    /* Close Server and Client */
    cqDUnitTest.closeClient(client2);
    cqDUnitTest.closeClient(client3);
    cqDUnitTest.closeServer(server);
  }

  /**
   * Test for CQ when region is populated with net load.
   */
  @Test
  public void testCQWithLoad() throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    VM client = host.getVM(2);

    cqDUnitTest.createServer(server1, 0, false, MirrorType.KEYS_VALUES);
    cqDUnitTest.createServer(server2, 0, false, MirrorType.KEYS);

    final int port1 = server1.invoke(() -> CqQueryUsingPoolDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());

    String poolName = "testCQWithLoad";
    cqDUnitTest.createPool(client, poolName, host0, port1);

    // cqDUnitTest.createClient(client, port1, host0);

    // Create CQs.
    cqDUnitTest.createCQ(client, poolName, "testCQWithLoad_0", cqDUnitTest.cqs[0]);
    cqDUnitTest.executeCQ(client, "testCQWithLoad_0", false, null);

    Wait.pause(1 * 1000);

    final int size = 10;

    // CREATE VALUES.
    cqDUnitTest.createValues(server2, cqDUnitTest.regions[0], size);

    server1.invoke(new CacheSerializableRunnable("Load from second server") {
      @Override
      public void run2() throws CacheException {
        Region region1 = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = 1; i <= size; i++) {
          region1.get(CqQueryUsingPoolDUnitTest.KEY + i);
        }
      }
    });

    for (int i = 1; i <= size; i++) {
      cqDUnitTest.waitForCreated(client, "testCQWithLoad_0", CqQueryUsingPoolDUnitTest.KEY + i);
    }

    cqDUnitTest.validateCQ(client, "testCQWithLoad_0", CqQueryUsingPoolDUnitTest.noTest, size, 0,
        0);

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server1);
    cqDUnitTest.closeServer(server2);
  }

  /**
   * Test for CQ when entries are evicted from region.
   */
  @Test
  public void testCQWithEviction() throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    VM client = host.getVM(2);

    final int evictionThreshold = 5;
    server1.invoke(new CacheSerializableRunnable("Create Cache Server") {
      @Override
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Create Cache Server. ###");
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setMirrorType(MirrorType.NONE);

        // setting the eviction attributes.
        factory
            .setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(evictionThreshold));
        for (int i = 0; i < cqDUnitTest.regions.length; i++) {
          Region region = createRegion(cqDUnitTest.regions[i], factory.createRegionAttributes());
          // Set CacheListener.
          region.getAttributesMutator()
              .addCacheListener(new CertifiableTestCacheListener(LogWriterUtils.getLogWriter()));
        }
        Wait.pause(2000);

        try {
          cqDUnitTest.startBridgeServer(0, true);
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
        Wait.pause(2000);
      }
    });

    cqDUnitTest.createServer(server2, 0, false, MirrorType.NONE);

    final int port1 = server1.invoke(() -> CqQueryUsingPoolDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());

    String poolName = "testCQWithEviction";
    cqDUnitTest.createPool(client, poolName, host0, port1);

    // cqDUnitTest.createClient(client, port1, host0);

    // Create CQs.
    cqDUnitTest.createCQ(client, poolName, "testCQWithEviction_0", cqDUnitTest.cqs[0]);

    // This should fail as Region is not replicated.
    // There is a bug37966 filed on this.
    try {
      cqDUnitTest.executeCQ(client, "testCQWithEviction_0", false, "CqException");
      fail("Should have thrown exception, cq not supported on Non-replicated region.");
    } catch (Exception expected) {
      // Ignore expected.
    }

    Wait.pause(1 * 1000);

    final int size = 10;

    // CREATE VALUES.
    cqDUnitTest.createValues(server2, cqDUnitTest.regions[0], size);

    server1.invoke(new CacheSerializableRunnable("Load from second server") {
      @Override
      public void run2() throws CacheException {
        Region region1 = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = 1; i <= size; i++) {
          region1.get(CqQueryUsingPoolDUnitTest.KEY + i);
        }
      }
    });

    Wait.pause(2 * 1000);

    server1.invoke(new CacheSerializableRunnable("validate destroy") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(cqDUnitTest.regions[0]);
        assertNotNull(region);

        Set keys = region.entrySet();
        int keyCnt = size - evictionThreshold;
        assertEquals("Mismatch, number of keys in local region is not equal to the expected size",
            keyCnt, keys.size());

        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        for (int i = 1; i <= keyCnt; i++) {
          ctl.waitForDestroyed(CqQueryUsingPoolDUnitTest.KEY + i);
          assertNull(region.getEntry(CqQueryUsingPoolDUnitTest.KEY + i));
        }
      }
    });

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server1);
    cqDUnitTest.closeServer(server2);
  }

  /**
   * Test for CQ with establishCallBackConnection.
   */
  @Test
  public void testCQWithEstablishCallBackConnection() throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM client = host.getVM(1);

    cqDUnitTest.createServer(server1, 0, false, MirrorType.KEYS_VALUES);

    final int port1 = server1.invoke(() -> CqQueryUsingPoolDUnitTest.getCacheServerPort());
    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    final String poolName = "testCQWithEstablishCallBackConnection";

    client.invoke(new CacheSerializableRunnable("createPool :" + poolName) {
      @Override
      public void run2() throws CacheException {
        // Create Cache.
        getCache();

        PoolFactory cpf = PoolManager.createFactory();
        cpf.setSubscriptionEnabled(false);
        cpf.addServer(serverHost, port1);
        cpf.create(poolName);
      }
    });

    // Create CQs.
    cqDUnitTest.createCQ(client, poolName, "testCQWithEstablishCallBackConnection_0",
        cqDUnitTest.cqs[0]);

    // This should fail.
    try {
      cqDUnitTest.executeCQ(client, "testCQWithEstablishCallBackConnection_0", false,
          "CqException");
      fail("Test should have failed with connection with establishCallBackConnection not found.");
    } catch (Exception expected) {
      // Expected.
    }

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server1);
  }

  /**
   * Test for: Region destroy, calls close on the server. Region clear triggers cqEvent with query
   * op region clear. Region invalidate triggers cqEvent with query op region invalidate.
   */
  @Test
  public void testRegionEvents() throws Exception {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    cqDUnitTest.createServer(server);
    final int port = server.invoke(() -> CqQueryUsingPoolDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    String poolName = "testRegionEvents";
    cqDUnitTest.createPool(client, poolName, host0, port);

    // cqDUnitTest.createClient(client, port, host0);

    // Create CQ on regionA
    cqDUnitTest.createCQ(client, poolName, "testRegionEvents_0", cqDUnitTest.cqs[0]);
    cqDUnitTest.executeCQ(client, "testRegionEvents_0", false, null);

    // Create CQ on regionB
    cqDUnitTest.createCQ(client, poolName, "testRegionEvents_1", cqDUnitTest.cqs[2]);
    cqDUnitTest.executeCQ(client, "testRegionEvents_1", false, null);

    // Test for Event on Region Clear.
    server.invoke(new CacheSerializableRunnable("testRegionEvents") {
      @Override
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Clearing the region on the server ###");
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = 1; i <= 5; i++) {
          region.put(CqQueryUsingPoolDUnitTest.KEY + i, new Portfolio(i));
        }
        region.clear();
      }
    });

    cqDUnitTest.waitForRegionClear(client, "testRegionEvents_0");

    // Test for Event on Region invalidate.
    server.invoke(new CacheSerializableRunnable("testRegionEvents") {
      @Override
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Invalidate the region on the server ###");
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = 1; i <= 5; i++) {
          region.put(CqQueryUsingPoolDUnitTest.KEY + i, new Portfolio(i));
        }
        region.invalidateRegion();
      }
    });

    cqDUnitTest.waitForRegionInvalidate(client, "testRegionEvents_0");

    // Test for Event on Region destroy.
    server.invoke(new CacheSerializableRunnable("testRegionEvents") {
      @Override
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Destroying the region on the server ###");
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[1]);
        for (int i = 1; i <= 5; i++) {
          region.put(CqQueryUsingPoolDUnitTest.KEY + i, new Portfolio(i));
        }
        // this should close one cq on client.
        region.destroyRegion();
      }
    });

    Wait.pause(1000); // wait for cq to close becuse of region destroy on server.
    // cqDUnitTest.waitForClose(client,"testRegionEvents_1");
    cqDUnitTest.validateCQCount(client, 1);

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }

  /**
   * Test for events created during the CQ query execution. When CQs are executed using
   * executeWithInitialResults there may be possibility that the region changes during that time may
   * not be reflected in the query result set thus making the query data and region data
   * inconsistent.
   */
  @Test
  public void testEventsDuringQueryExecution() throws Exception {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final String cqName = "testEventsDuringQueryExecution_0";
    cqDUnitTest.createServer(server);

    final int port = server.invoke(() -> CqQueryUsingPoolDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    String poolName = "testEventsDuringQueryExecution";
    cqDUnitTest.createPool(client, poolName, host0, port);

    // create CQ.
    cqDUnitTest.createCQ(client, poolName, cqName, cqDUnitTest.cqs[0]);

    final int numObjects = 200;
    final int totalObjects = 500;

    // initialize Region.
    server.invoke(new CacheSerializableRunnable("Update Region") {
      @Override
      public void run2() throws CacheException {
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = 1; i <= numObjects; i++) {
          Portfolio p = new Portfolio(i);
          region.put("" + i, p);
        }
      }
    });

    // First set testhook in executeWithInitialResults so that queued events
    // are not drained before we verify there number.
    client.invoke(setTestHook());

    // Execute CQ while update is in progress.
    AsyncInvocation executeCq =
        client.invokeAsync(new CacheSerializableRunnable("Execute CQ AsyncInvoke") {
          @Override
          public void run2() throws CacheException {
            QueryService cqService = getCache().getQueryService();
            // Get CqQuery object.
            CqQuery cq1 = cqService.getCq(cqName);
            if (cq1 == null) {
              fail("Failed to get CQ " + cqName);
            }
            SelectResults cqResults = null;
            try {
              cqResults = cq1.executeWithInitialResults();
            } catch (Exception ex) {
              fail("CQ execution failed", ex);
            }

            // Check num of events received during executeWithInitialResults.
            final TestHook testHook = CqQueryImpl.testHook;
            GeodeAwaitility.await().untilAsserted(new WaitCriterion() {

              @Override
              public boolean done() {
                return testHook.numQueuedEvents() > 0;
              }

              @Override
              public String description() {
                return "No queued events found.";
              }
            });

            getCache().getLogger().fine("Queued Events Size" + testHook.numQueuedEvents());
            // Make sure CQEvents are queued during execute with initial results.

            CqQueryTestListener cqListener =
                (CqQueryTestListener) cq1.getCqAttributes().getCqListener();
            // Wait for the last key to arrive.
            cqListener.waitForCreated("" + totalObjects);

            // Check if the events from CqListener are in order.
            int oldId = 0;
            for (Object cqEvent : cqListener.events.toArray()) {
              int newId = new Integer(cqEvent.toString()).intValue();
              if (oldId > newId) {
                fail("Queued events for CQ Listener during execution with "
                    + "Initial results is not in the order in which they are created.");
              }
              oldId = newId;
            }

            // Check if all the IDs are present as part of Select Results and CQ Events.
            HashSet ids = new HashSet(cqListener.events);

            for (Object o : cqResults.asList()) {
              Struct s = (Struct) o;
              ids.add(s.get("key"));
            }

            HashSet missingIds = new HashSet();
            String key = "";
            for (int i = 1; i <= totalObjects; i++) {
              key = "" + i;
              if (!(ids.contains(key))) {
                missingIds.add(key);
              }
            }

            if (!missingIds.isEmpty()) {
              fail("Missing Keys in either ResultSet or the Cq Event list. "
                  + " Missing keys : [size : " + missingIds.size() + "]" + missingIds
                  + " Ids in ResultSet and CQ Events :" + ids);
            }
          }
        });

    // Keep updating region (async invocation).
    server.invoke(new CacheSerializableRunnable("Update Region") {
      @Override
      public void run2() throws CacheException {
        Wait.pause(200);
        client.invoke(new CacheSerializableRunnable("Releasing the latch") {
          @Override
          public void run2() throws CacheException {
            // Now release the testhook so that CQListener can proceed.
            final TestHook testHook = CqQueryImpl.testHook;
            testHook.ready();
          }
        });
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = numObjects + 1; i <= totalObjects; i++) {
          Portfolio p = new Portfolio(i);
          region.put("" + i, p);
        }
      }
    });

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }

  @Test
  public void testCqStatInitializationTimingIssue() {
    disconnectAllFromDS();

    // The async close can cause this exception on the server
    IgnoredException.addIgnoredException("java.net.SocketException: Broken pipe");
    final String regionName = "testCqStatInitializationTimingIssue";
    final String cq1Name = "testCq1";
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    VM client2 = host.getVM(2);

    // Start server 1
    final int server1Port = ((Integer) server
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, new Boolean(true))))
            .intValue();

    // Start a client
    client.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(client.getHost()), server1Port), regionName));

    // Start a pub client
    client2.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(client2.getHost()), server1Port), regionName));

    // client has thread that invokes new and remove cq over and over
    client.invokeAsync(new CacheSerializableRunnable("Register cq") {
      @Override
      public void run2() throws CacheException {
        for (int i = 0; i < 10000; i++) {
          CqQuery query = createCq(regionName, cq1Name);
          if (query != null) {
            try {
              query.close();
            } catch (Exception e) {
              fail("exception while closing cq:", e);
            }
          }
        }
      }
    });

    client2.invokeAsync(new CacheSerializableRunnable("pub updates") {
      @Override
      public void run2() throws CacheException {
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        while (true) {
          for (int i = 0; i < 50000; i++) {
            region.put("" + i, "" + Math.random());
          }
        }
      }
    });

    server.invokeAsync(new CacheSerializableRunnable("pub updates") {
      @Override
      public void run2() throws CacheException {
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        while (true) {
          for (int i = 0; i < 50000; i++) {
            region.put("" + i, "" + Math.random());
          }
        }
      }
    });

    // client has another thread that retrieves cq map and checks stat over and over
    client.invoke(new CacheSerializableRunnable("Check Stats") {
      @Override
      public void run2() throws CacheException {
        for (int i = 0; i < 10000; i++) {
          checkCqStats(cq1Name);
        }
      }
    });

    client.invoke(() -> CacheServerTestUtil.closeCache());
    client2.invoke(() -> CacheServerTestUtil.closeCache());
    server.invoke(() -> CacheServerTestUtil.closeCache());
  }

  @Test
  public void testGetDurableCQsFromPoolOnly() throws Exception {
    final String regionName = "regionA";
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client1 = host.getVM(1);
    VM client2 = host.getVM(2);

    /* Create Server and Client */
    cqDUnitTest.createServer(server);
    final int port = server.invoke(() -> CqQueryUsingPoolDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    final String poolName1 = "pool1";
    final String poolName2 = "pool2";

    cqDUnitTest.createPool(client1, poolName1, host0, port);
    cqDUnitTest.createPool(client2, poolName2, host0, port);

    client1.invoke(new CacheSerializableRunnable("Register cq for client 1") {
      @Override
      public void run2() throws CacheException {

        QueryService queryService = null;
        try {
          queryService = (PoolManager.find(poolName1)).getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        try {
          CqAttributesFactory cqAf = new CqAttributesFactory();
          CqAttributes attributes = cqAf.create();
          queryService.newCq("client1DCQ1", "Select * From /root/" + regionName + " where id = 1",
              attributes, true).execute();
          queryService.newCq("client1DCQ2", "Select * From /root/" + regionName + " where id = 10",
              attributes, true).execute();
          queryService.newCq("client1NoDC1", "Select * From /root/" + regionName, attributes, false)
              .execute();
          queryService.newCq("client1NoDC2", "Select * From /root/" + regionName + " where id = 3",
              attributes, false).execute();
        } catch (CqException e) {
          fail("failed", e);
        } catch (CqExistsException e) {
          fail("failed", e);
        } catch (RegionNotFoundException e) {
          fail("failed", e);
        }
      }
    });

    client2.invoke(new CacheSerializableRunnable("Register cq for client 2") {
      @Override
      public void run2() throws CacheException {

        QueryService queryService = null;
        try {
          queryService = (PoolManager.find(poolName2)).getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        try {
          CqAttributesFactory cqAf = new CqAttributesFactory();
          CqAttributes attributes = cqAf.create();
          queryService.newCq("client2DCQ1", "Select * From /root/" + regionName + " where id = 1",
              attributes, true).execute();
          queryService.newCq("client2DCQ2", "Select * From /root/" + regionName + " where id = 10",
              attributes, true).execute();
          queryService.newCq("client2DCQ3", "Select * From /root/" + regionName, attributes, true)
              .execute();
          queryService.newCq("client2DCQ4", "Select * From /root/" + regionName + " where id = 3",
              attributes, true).execute();
        } catch (CqException e) {
          fail("failed", e);
        } catch (CqExistsException e) {
          fail("failed", e);
        } catch (RegionNotFoundException e) {
          fail("failed", e);
        }
      }
    });

    client2.invoke(new CacheSerializableRunnable("test getDurableCQsFromServer for client2") {
      @Override
      public void run2() throws CacheException {
        QueryService queryService = null;
        try {
          queryService = (PoolManager.find(poolName2)).getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        List<String> list = null;
        try {
          list = queryService.getAllDurableCqsFromServer();
        } catch (CqException e) {
          fail("failed", e);
        }
        assertEquals(4, list.size());
        assertTrue(list.contains("client2DCQ1"));
        assertTrue(list.contains("client2DCQ2"));
        assertTrue(list.contains("client2DCQ3"));
        assertTrue(list.contains("client2DCQ4"));
      }
    });

    client1.invoke(new CacheSerializableRunnable("test getDurableCQsFromServer for client1") {
      @Override
      public void run2() throws CacheException {
        QueryService queryService = null;
        try {
          queryService = (PoolManager.find(poolName1)).getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        List<String> list = null;
        try {
          list = queryService.getAllDurableCqsFromServer();
        } catch (CqException e) {
          fail("failed", e);
        }
        assertEquals(2, list.size());
        assertTrue(list.contains("client1DCQ1"));
        assertTrue(list.contains("client1DCQ2"));
      }
    });

    cqDUnitTest.closeClient(client2);
    cqDUnitTest.closeClient(client1);
    cqDUnitTest.closeServer(server);
  }

  @Test
  public void testGetDurableCQsFromServerWithEmptyList() throws Exception {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client1 = host.getVM(1);

    /* Create Server and Client */
    cqDUnitTest.createServer(server);
    final int port = server.invoke(() -> CqQueryUsingPoolDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    final String poolName1 = "pool1";

    cqDUnitTest.createPool(client1, poolName1, host0, port);

    client1.invoke(new CacheSerializableRunnable("test getDurableCQsFromServer for client1") {
      @Override
      public void run2() throws CacheException {
        QueryService queryService = null;
        try {
          queryService = (PoolManager.find(poolName1)).getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        List<String> list = null;
        try {
          list = queryService.getAllDurableCqsFromServer();
        } catch (CqException e) {
          fail("failed", e);
        }
        assertEquals(0, list.size());
        assertFalse(list.contains("client1DCQ1"));
        assertFalse(list.contains("client1DCQ2"));
      }
    });

    cqDUnitTest.closeClient(client1);
    cqDUnitTest.closeServer(server);
  }

  @Test
  public void testGetDurableCqsFromServer() {
    disconnectAllFromDS();

    final String regionName = "testGetAllDurableCqsFromServer";
    final String cq1Name = "testCq1";
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client1 = host.getVM(1);
    VM client2 = host.getVM(2);

    // Start server 1
    final int server1Port = ((Integer) server
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, new Boolean(true))))
            .intValue();

    // Start client 1
    client1.invoke(() -> CacheServerTestUtil.createClientCache(
        getClientPool(NetworkUtils.getServerHostName(client1.getHost()), server1Port), regionName));

    // Start client 2
    client2.invoke(() -> CacheServerTestUtil.createClientCache(
        getClientPool(NetworkUtils.getServerHostName(client2.getHost()), server1Port), regionName));

    createClient1CqsAndDurableCqs(client1, regionName);
    createClient2CqsAndDurableCqs(client2, regionName);

    client2.invoke(new CacheSerializableRunnable("check durable cqs for client 2") {
      @Override
      public void run2() throws CacheException {
        QueryService queryService = CacheServerTestUtil.getCache().getQueryService();
        List<String> list = null;
        try {
          list = queryService.getAllDurableCqsFromServer();
        } catch (CqException e) {
          fail("failed", e);
        }
        assertEquals(4, list.size());
        assertTrue(list.contains("client2DCQ1"));
        assertTrue(list.contains("client2DCQ2"));
        assertTrue(list.contains("client2DCQ3"));
        assertTrue(list.contains("client2DCQ4"));
      }
    });

    client1.invoke(new CacheSerializableRunnable("check durable cqs for client 1") {
      @Override
      public void run2() throws CacheException {
        QueryService queryService = CacheServerTestUtil.getCache().getQueryService();
        List<String> list = null;
        try {
          list = queryService.getAllDurableCqsFromServer();
        } catch (CqException e) {
          fail("failed", e);
        }
        assertEquals(2, list.size());
        assertTrue(list.contains("client1DCQ1"));
        assertTrue(list.contains("client1DCQ2"));
      }
    });

    client1.invoke(() -> CacheServerTestUtil.closeCache());
    client2.invoke(() -> CacheServerTestUtil.closeCache());
    server.invoke(() -> CacheServerTestUtil.closeCache());
  }

  @Test
  public void testGetDurableCqsFromServerCycleClients() {
    disconnectAllFromDS();

    final String regionName = "testGetAllDurableCqsFromServerCycleClients";
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client1 = host.getVM(1);
    VM client2 = host.getVM(2);
    int timeout = 60000;
    // Start server 1
    final int server1Port = ((Integer) server
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, new Boolean(true))))
            .intValue();

    // Start client 1
    client1.invoke(() -> CacheServerTestUtil.createClientCache(
        getClientPool(NetworkUtils.getServerHostName(client1.getHost()), server1Port), regionName,
        getDurableClientProperties("client1_dc", timeout)));

    // Start client 2
    client2.invoke(() -> CacheServerTestUtil.createClientCache(
        getClientPool(NetworkUtils.getServerHostName(client2.getHost()), server1Port), regionName,
        getDurableClientProperties("client2_dc", timeout)));

    createClient1CqsAndDurableCqs(client1, regionName);
    createClient2CqsAndDurableCqs(client2, regionName);

    cycleDurableClient(client1, "client1_dc", server1Port, regionName, timeout);
    cycleDurableClient(client2, "client2_dc", server1Port, regionName, timeout);

    client2.invoke(new CacheSerializableRunnable("check durable cqs for client 2") {
      @Override
      public void run2() throws CacheException {
        QueryService queryService = CacheServerTestUtil.getCache().getQueryService();
        List<String> list = null;
        try {
          list = queryService.getAllDurableCqsFromServer();
        } catch (CqException e) {
          fail("failed", e);
        }
        assertEquals(4, list.size());
        assertTrue(list.contains("client2DCQ1"));
        assertTrue(list.contains("client2DCQ2"));
        assertTrue(list.contains("client2DCQ3"));
        assertTrue(list.contains("client2DCQ4"));
      }
    });

    client1.invoke(new CacheSerializableRunnable("check durable cqs for client 1") {
      @Override
      public void run2() throws CacheException {
        QueryService queryService = CacheServerTestUtil.getCache().getQueryService();
        List<String> list = null;
        try {
          list = queryService.getAllDurableCqsFromServer();
        } catch (CqException e) {
          fail("failed", e);
        }
        assertEquals(2, list.size());
        assertTrue(list.contains("client1DCQ1"));
        assertTrue(list.contains("client1DCQ2"));
      }
    });

    client1.invoke(() -> CacheServerTestUtil.closeCache());
    client2.invoke(() -> CacheServerTestUtil.closeCache());
    server.invoke(() -> CacheServerTestUtil.closeCache());
  }

  @Test
  public void testGetDurableCqsFromServerCycleClientsAndMoreCqs() {
    final String regionName = "testGetAllDurableCqsFromServerCycleClients";
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client1 = host.getVM(1);
    VM client2 = host.getVM(2);
    int timeout = 60000;
    // Start server 1
    final int server1Port = ((Integer) server
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, new Boolean(true))))
            .intValue();

    // Start client 1
    client1.invoke(() -> CacheServerTestUtil.createClientCache(
        getClientPool(NetworkUtils.getServerHostName(client1.getHost()), server1Port), regionName,
        getDurableClientProperties("client1_dc", timeout)));

    // Start client 2
    client2.invoke(() -> CacheServerTestUtil.createClientCache(
        getClientPool(NetworkUtils.getServerHostName(client2.getHost()), server1Port), regionName,
        getDurableClientProperties("client2_dc", timeout)));

    // create the test cqs
    createClient1CqsAndDurableCqs(client1, regionName);
    createClient2CqsAndDurableCqs(client2, regionName);

    cycleDurableClient(client1, "client1_dc", server1Port, regionName, timeout);
    cycleDurableClient(client2, "client2_dc", server1Port, regionName, timeout);

    client1.invoke(new CacheSerializableRunnable("Register more cq for client 1") {
      @Override
      public void run2() throws CacheException {
        // register the cq's
        QueryService queryService = CacheServerTestUtil.getCache().getQueryService();
        CqAttributesFactory cqAf = new CqAttributesFactory();
        CqAttributes attributes = cqAf.create();
        try {
          queryService.newCq("client1MoreDCQ1", "Select * From /" + regionName + " where id = 1",
              attributes, true).execute();
          queryService.newCq("client1MoreDCQ2", "Select * From /" + regionName + " where id = 10",
              attributes, true).execute();
          queryService.newCq("client1MoreNoDC1", "Select * From /" + regionName, attributes, false)
              .execute();
          queryService.newCq("client1MoreNoDC2", "Select * From /" + regionName + " where id = 3",
              attributes, false).execute();
        } catch (RegionNotFoundException e) {
          fail("failed", e);
        } catch (CqException e) {
          fail("failed", e);
        } catch (CqExistsException e) {
          fail("failed", e);
        }
      }
    });

    client2.invoke(new CacheSerializableRunnable("Register more cq for client 2") {
      @Override
      public void run2() throws CacheException {
        // register the cq's
        QueryService queryService = CacheServerTestUtil.getCache().getQueryService();
        CqAttributesFactory cqAf = new CqAttributesFactory();
        CqAttributes attributes = cqAf.create();
        try {
          queryService.newCq("client2MoreDCQ1", "Select * From /" + regionName + " where id = 1",
              attributes, true).execute();
          queryService.newCq("client2MoreDCQ2", "Select * From /" + regionName + " where id = 10",
              attributes, true).execute();
          queryService.newCq("client2MoreDCQ3", "Select * From /" + regionName, attributes, true)
              .execute();
          queryService.newCq("client2MoreDCQ4", "Select * From /" + regionName + " where id = 3",
              attributes, true).execute();
        } catch (RegionNotFoundException e) {
          fail("failed", e);
        } catch (CqException e) {
          fail("failed", e);
        } catch (CqExistsException e) {
          fail("failed", e);
        }
      }
    });

    // Cycle clients a second time
    cycleDurableClient(client1, "client1_dc", server1Port, regionName, timeout);
    cycleDurableClient(client2, "client2_dc", server1Port, regionName, timeout);

    client2.invoke(new CacheSerializableRunnable("check durable cqs for client 2") {
      @Override
      public void run2() throws CacheException {
        QueryService queryService = CacheServerTestUtil.getCache().getQueryService();
        List<String> list = null;
        try {
          list = queryService.getAllDurableCqsFromServer();
        } catch (CqException e) {
          fail("failed", e);
        }
        assertEquals(8, list.size());
        assertTrue(list.contains("client2DCQ1"));
        assertTrue(list.contains("client2DCQ2"));
        assertTrue(list.contains("client2DCQ3"));
        assertTrue(list.contains("client2DCQ4"));
        assertTrue(list.contains("client2MoreDCQ1"));
        assertTrue(list.contains("client2MoreDCQ2"));
        assertTrue(list.contains("client2MoreDCQ3"));
        assertTrue(list.contains("client2MoreDCQ4"));
      }
    });

    client1.invoke(new CacheSerializableRunnable("check durable cqs for client 1") {
      @Override
      public void run2() throws CacheException {
        QueryService queryService = CacheServerTestUtil.getCache().getQueryService();
        List<String> list = null;
        try {
          list = queryService.getAllDurableCqsFromServer();
        } catch (CqException e) {
          fail("failed", e);
        }
        assertEquals(4, list.size());
        assertTrue(list.contains("client1DCQ1"));
        assertTrue(list.contains("client1DCQ2"));
        assertTrue(list.contains("client1MoreDCQ1"));
        assertTrue(list.contains("client1MoreDCQ2"));
      }
    });

    client1.invoke(() -> CacheServerTestUtil.closeCache());
    client2.invoke(() -> CacheServerTestUtil.closeCache());
    server.invoke(() -> CacheServerTestUtil.closeCache());
  }

  private Properties getDurableClientProperties(String durableClientId, int durableClientTimeout) {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    properties.setProperty(DURABLE_CLIENT_ID, durableClientId);
    properties.setProperty(DURABLE_CLIENT_TIMEOUT, String.valueOf(durableClientTimeout));
    return properties;
  }

  // helper to create durable cqs to test out getAllDurableCqs functionality
  private void createClient1CqsAndDurableCqs(VM client, final String regionName) {
    client.invoke(new CacheSerializableRunnable("Register cq for client 1") {
      @Override
      public void run2() throws CacheException {
        // register the cq's
        QueryService queryService = CacheServerTestUtil.getCache().getQueryService();
        CqAttributesFactory cqAf = new CqAttributesFactory();
        CqAttributes attributes = cqAf.create();
        try {
          queryService.newCq("client1DCQ1", "Select * From /" + regionName + " where id = 1",
              attributes, true).execute();
          queryService.newCq("client1DCQ2", "Select * From /" + regionName + " where id = 10",
              attributes, true).execute();
          queryService.newCq("client1NoDC1", "Select * From /" + regionName, attributes, false)
              .execute();
          queryService.newCq("client1NoDC2", "Select * From /" + regionName + " where id = 3",
              attributes, false).execute();
        } catch (RegionNotFoundException e) {
          fail("failed", e);
        } catch (CqException e) {
          fail("failed", e);
        } catch (CqExistsException e) {
          fail("failed", e);
        }
      }
    });
  }

  private void createClient2CqsAndDurableCqs(VM client, final String regionName) {
    client.invoke(new CacheSerializableRunnable("Register cq for client 2") {
      @Override
      public void run2() throws CacheException {
        // register the cq's
        QueryService queryService = CacheServerTestUtil.getCache().getQueryService();
        CqAttributesFactory cqAf = new CqAttributesFactory();
        CqAttributes attributes = cqAf.create();
        try {
          queryService.newCq("client2DCQ1", "Select * From /" + regionName + " where id = 1",
              attributes, true).execute();
          queryService.newCq("client2DCQ2", "Select * From /" + regionName + " where id = 10",
              attributes, true).execute();
          queryService.newCq("client2DCQ3", "Select * From /" + regionName, attributes, true)
              .execute();
          queryService.newCq("client2DCQ4", "Select * From /" + regionName + " where id = 3",
              attributes, true).execute();
        } catch (RegionNotFoundException e) {
          fail("failed", e);
        } catch (CqException e) {
          fail("failed", e);
        } catch (CqExistsException e) {
          fail("failed", e);
        }

      }
    });
  }

  private void cycleDurableClient(VM client, final String dcName, final int serverPort,
      final String regionName, final int durableClientTimeout) {
    client.invoke(new CacheSerializableRunnable("cycle client") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.closeCache(true);
      }
    });

    client.invoke(() -> CacheServerTestUtil.createClientCache(
        getClientPool(NetworkUtils.getServerHostName(client.getHost()), serverPort), regionName,
        getDurableClientProperties(dcName, durableClientTimeout)));
  }

  private CqQuery createCq(String regionName, String cqName) {
    // Create CQ Attributes.
    CqAttributesFactory cqAf = new CqAttributesFactory();

    // Initialize and set CqListener.
    CqListener[] cqListeners = {new CqListener() {
      @Override
      public void close() {}

      @Override
      public void onEvent(CqEvent aCqEvent) {}

      @Override
      public void onError(CqEvent aCqEvent) {}
    }};
    cqAf.initCqListeners(cqListeners);
    CqAttributes cqa = cqAf.create();

    // Create cq's
    // Get the query service for the Pool
    QueryService queryService = CacheServerTestUtil.getCache().getQueryService();
    CqQuery query = null;
    try {
      query = queryService.newCq(cqName, "Select * from /" + regionName, cqa);
      query.execute();
    } catch (CqExistsException e) {
      fail("Could not find specified region:" + regionName + ":", e);
    } catch (CqException e) {
      fail("Could not find specified region:" + regionName + ":", e);

    } catch (RegionNotFoundException e) {
      fail("Could not find specified region:" + regionName + ":", e);
    }
    return query;
  }

  private void checkCqStats(String cqName) {
    QueryService queryService = CacheServerTestUtil.getPool().getQueryService();
    CqQueryImpl query = null;
    query = (CqQueryImpl) queryService.getCq(cqName);
    if (query != null) {
      query.getVsdStats();
      query.getVsdStats().getNumEvents();
    }
  }

  private Pool getClientPool(String host, int serverPort) {
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, serverPort).setSubscriptionAckInterval(1).setSubscriptionEnabled(true);
    return ((PoolFactoryImpl) pf).getPoolAttributes();
  }

  public CacheSerializableRunnable setTestHook() {
    SerializableRunnable sr = new CacheSerializableRunnable("TestHook") {
      @Override
      public void run2() {
        class CqQueryTestHook implements CqQueryImpl.TestHook {

          CountDownLatch latch = new CountDownLatch(1);
          private int numEvents = 0;

          @Override
          public void pauseUntilReady() {
            try {
              logger.debug("CqQueryTestHook: Going to wait on latch until ready is called.");
              if (!latch.await(10, TimeUnit.SECONDS)) {
                fail("query was never unlatched");
              }
            } catch (Exception e) {
              fail("interrupted", e);
            }
          }

          @Override
          public void ready() {
            latch.countDown();
            logger.debug("CqQueryTestHook: The latch has been released.");
          }

          @Override
          public int numQueuedEvents() {
            return numEvents;
          }

          @Override
          public void setEventCount(int count) {
            logger.debug("CqQueryTestHook: Setting numEVents to: " + count);
            numEvents = count;
          }
        };
        CqQueryImpl.testHook = new CqQueryTestHook();
      }
    };
    return (CacheSerializableRunnable) sr;
  }

}
