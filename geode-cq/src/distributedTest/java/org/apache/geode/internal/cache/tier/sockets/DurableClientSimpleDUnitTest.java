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

import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.TYPE_CREATE;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.ClientSession;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.ServerRefusedConnectionException;
import org.apache.geode.cache.client.internal.ConnectionStats;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.ha.HARegionQueueStats;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class DurableClientSimpleDUnitTest extends DurableClientTestCase {

  /**
   * Test that a durable client correctly receives updates.
   */
  @Test
  public void testSimpleDurableClientUpdate() {
    // Start a server
    int serverPort = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is not kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), serverPort, true), regionName,
        getClientDistributedSystemProperties(durableClientId), Boolean.TRUE));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), serverPort, false),
        regionName));

    // Publish some entries
    final int numberOfEntries = 10;
    publishEntries(numberOfEntries);

    // Verify the durable client received the updates
    verifyDurableClientEvents(this.durableClientVM, numberOfEntries);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test that a durable client VM with multiple BridgeClients correctly registers on the server.
   */
  @Test
  public void testMultipleBridgeClientsInSingleDurableVM() {
    // Start a server
    int serverPort = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client with 2 regions (and 2 BridgeClients) that is not
    // kept alive on the server when it stops normally
    final String durableClientId = getName() + "_client";
    final String regionName1 = regionName + "1";
    final String regionName2 = regionName + "2";
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClients(
        getClientPool(getServerHostName(), serverPort, true), regionName1,
        regionName2, getClientDistributedSystemProperties(durableClientId)));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        assertEquals(2, PoolManager.getAll().size());
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Verify durable clients on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

        // Iterate the CacheClientProxies
        checkNumberOfClientProxies(2);
        String firstProxyRegionName = null;
        for (CacheClientProxy proxy : notifier.getClientProxies()) {
          assertTrue(proxy.isDurable());
          assertEquals(durableClientId, proxy.getDurableId());
          assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT,
              proxy.getDurableTimeout());

          // Verify the two HA region names aren't the same
          if (firstProxyRegionName == null) {
            firstProxyRegionName = proxy.getHARegionName();
          } else {
            assertTrue(!firstProxyRegionName.equals(proxy.getHARegionName()));
          }
        }
      }
    });

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Verify the durable client is no longer on the server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(0);
      }
    });

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test that a second VM with the same durable id cannot connect to the server while the first VM
   * is connected. Also, verify that the first client is not affected by the second one attempting
   * to connect.
   */
  @Ignore("TODO: test is disabled")
  @Test
  public void testMultipleVMsWithSameDurableId() {
    // Start a server
    final int serverPort = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), serverPort, true), regionName,
        getClientDistributedSystemProperties(durableClientId), Boolean.TRUE));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE);
      }
    });

    // Attempt to start another durable client VM with the same id.
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Create another durable client") {
      @Override
      public void run2() throws CacheException {
        getSystem(getClientDistributedSystemProperties(durableClientId));
        PoolFactoryImpl pf = (PoolFactoryImpl) PoolManager.createFactory();
        pf.init(getClientPool(getServerHostName(), serverPort, true));
        try {
          pf.create("uncreatablePool");
          fail("Should not have been able to create the pool");
        } catch (ServerRefusedConnectionException expected) {
          // expected exception
          disconnectFromDS();
        } catch (Exception e) {
          fail("Should not have gotten here", e);
        }
      }
    });

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, proxy.getDurableTimeout());
      }
    });

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), serverPort, false),
        regionName));

    // Publish some entries
    final int numberOfEntries = 10;
    publishEntries(numberOfEntries);

    // Verify the durable client received the updates
    verifyDurableClientEvents(this.durableClientVM, numberOfEntries);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test that the server correctly processes starting two durable clients.
   */
  @Test
  public void testSimpleTwoDurableClients() {
    // Start a server
    int serverPort = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), serverPort, true), regionName,
        getClientDistributedSystemProperties(durableClientId)));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Start another durable client that is not kept alive on the server when
    // it stops normally. Use the 'publisherClientVM' as a durable client.
    VM durableClient2VM = this.publisherClientVM;
    final String durableClientId2 = getName() + "_client2";
    durableClient2VM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), serverPort, true), regionName,
        getClientDistributedSystemProperties(durableClientId2)));

    // Send clientReady message
    durableClient2VM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Verify durable clients on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

        // Iterate the CacheClientProxies and verify they are correct
        checkNumberOfClientProxies(2);
        boolean durableClient1Found = false, durableClient2Found = false;
        for (CacheClientProxy proxy : notifier.getClientProxies()) {
          assertTrue(proxy.isDurable());
          if (proxy.getDurableId().equals(durableClientId)) {
            durableClient1Found = true;
          }
          if (proxy.getDurableId().equals(durableClientId2)) {
            durableClient2Found = true;
          }
          assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT,
              proxy.getDurableTimeout());
        }
        assertTrue(durableClient1Found);
        assertTrue(durableClient2Found);
      }
    });

    // Stop the durable clients
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    durableClient2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test that starting a durable client on multiple servers (one live and one not live) is
   * processed correctly.
   */
  @Ignore("TODO: test is disabled for bug 52043")
  @Test
  public void testDurableClientMultipleServersOneLive() {
    // Start server 1
    final int server1Port = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start server 2
    final int server2Port = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Stop server 2
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    // final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        Boolean.TRUE));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });

    // Verify durable client on server1
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(durableClientTimeout, proxy.getDurableTimeout());
      }
    });

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> CacheServerTestUtil
        .createCacheClient(getClientPool(getServerHostName(),
            server1Port, server2Port, false), regionName));

    // Publish some entries
    final int numberOfEntries = 10;
    publishEntries(numberOfEntries);

    // Verify the durable client received the updates
    this.durableClientVM.invoke(new CacheSerializableRunnable("Verify updates") {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Get the listener and wait for the appropriate number of events
        CacheServerTestUtil.ControlListener listener =
            (CacheServerTestUtil.ControlListener) region.getAttributes().getCacheListeners()[0];
        listener.waitWhileNotEnoughEvents(30000, numberOfEntries);
        assertEquals("Events were" + listener.events, numberOfEntries, listener.events.size());
      }
    });

    try {
      Thread.sleep(10000);
    } catch (InterruptedException ex) {
      fail("interrupted", ex);
    }

    // Stop the durable client
    this.durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(Boolean.TRUE));

    // Verify the durable client still exists on the server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
      }
    });

    // Publish some more entries
    publishEntries(numberOfEntries);

    // Re-start the durable client
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true),
        regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
      }
    });

    // Verify the durable client received the updates held for it on the server
    this.durableClientVM.invoke(new CacheSerializableRunnable("Verify updates") {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Get the listener and wait for the appropriate number of events
        CacheServerTestUtil.ControlListener listener =
            (CacheServerTestUtil.ControlListener) region.getAttributes().getCacheListeners()[0];
        listener.waitWhileNotEnoughEvents(30000, numberOfEntries);
        assertEquals("Events were" + listener.events, numberOfEntries, listener.events.size());
      }
    });

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 1
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test that updates to two durable clients are processed correctly.
   */
  @Test
  public void testTwoDurableClientsStartStopUpdate() {
    // Start a server
    int serverPort = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    // final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), serverPort, true), regionName,
        getClientDistributedSystemProperties(durableClientId, durableClientTimeout), Boolean.TRUE));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });

    // Start another durable client that is not kept alive on the server when
    // it stops normally. Use the 'server2VM' as the second durable client.
    VM durableClient2VM = this.server2VM;
    final String durableClientId2 = getName() + "_client2";
    durableClient2VM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), serverPort, true), regionName,
        getClientDistributedSystemProperties(durableClientId2, durableClientTimeout),
        Boolean.TRUE));

    // Send clientReady message
    durableClient2VM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Have the durable client register interest in all keys
    durableClient2VM.invoke(new CacheSerializableRunnable("Register interest") {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });

    // Verify durable clients on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

        // Iterate the CacheClientProxies and verify they are correct
        checkNumberOfClientProxies(2);
        boolean durableClient1Found = false, durableClient2Found = false;
        for (CacheClientProxy proxy : notifier.getClientProxies()) {
          assertTrue(proxy.isDurable());
          if (proxy.getDurableId().equals(durableClientId)) {
            durableClient1Found = true;
          }
          if (proxy.getDurableId().equals(durableClientId2)) {
            durableClient2Found = true;
          }
          assertEquals(durableClientTimeout, proxy.getDurableTimeout());
        }
        assertTrue(durableClient1Found);
        assertTrue(durableClient2Found);
      }
    });

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), serverPort, false),
        regionName));

    // Publish some entries
    final int numberOfEntries = 10;
    publishEntries(numberOfEntries);

    // Verify durable client 1 received the updates
    verifyDurableClientEvents(this.durableClientVM, numberOfEntries);

    // Verify durable client 2 received the updates
    verifyDurableClientEvents(durableClient2VM, numberOfEntries);

    // ARB: Wait for queue ack to arrive at server.
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      fail("interrupted", ex);
    }

    // Stop the durable clients
    this.durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(Boolean.TRUE));
    durableClient2VM.invoke(() -> CacheServerTestUtil.closeCache(Boolean.TRUE));

    // Verify the durable clients still exist on the server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

        // Iterate the CacheClientProxies and verify they are correct
        checkNumberOfClientProxies(2);
        boolean durableClient1Found = false, durableClient2Found = false;
        for (CacheClientProxy proxy : notifier.getClientProxies()) {
          assertTrue(proxy.isDurable());
          if (proxy.getDurableId().equals(durableClientId)) {
            durableClient1Found = true;
          }
          if (proxy.getDurableId().equals(durableClientId2)) {
            durableClient2Found = true;
          }
          assertEquals(durableClientTimeout, proxy.getDurableTimeout());
        }
        assertTrue(durableClient1Found);
        assertTrue(durableClient2Found);
      }
    });

    // Publish some more entries
    publishEntries(numberOfEntries);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      fail("interrupted", ex);
    }

    // Verify the durable clients' queues contain the entries
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

        // Iterate the CacheClientProxies and verify the queue sizes
        checkNumberOfClientProxies(2);
        for (CacheClientProxy proxy : notifier.getClientProxies()) {
          assertEquals(numberOfEntries, proxy.getQueueSize());
        }
      }
    });

    // Re-start durable client 1
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), serverPort, true), regionName,
        getClientDistributedSystemProperties(durableClientId), Boolean.TRUE));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Re-start durable client 2
    durableClient2VM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), serverPort, true), regionName,
        getClientDistributedSystemProperties(durableClientId2), Boolean.TRUE));

    // Send clientReady message
    durableClient2VM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Verify durable client 1 received the updates held for it on the server
    verifyDurableClientEvents(this.durableClientVM, numberOfEntries);

    // Verify durable client 2 received the updates held for it on the server
    verifyDurableClientEvents(durableClient2VM, numberOfEntries);

    // Stop durable client 1
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop durable client 2
    durableClient2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Tests whether a durable client reconnects properly to two servers.
   */
  @Test
  public void testDurableClientReconnectTwoServers() {
    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));

    // on test flag for periodic ack
    this.server1VM
        .invoke(() -> DurableClientTestCase.setTestFlagToVerifyActForMarker(Boolean.TRUE));

    final int server1Port = ports[0];

    // Start server 2 using the same mcast port as server 1
    final int server2Port = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Stop server 2
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    // final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        Boolean.TRUE));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });

    // Verify durable client on server 1
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(durableClientTimeout, proxy.getDurableTimeout());
        verifyReceivedMarkerAck(proxy);
      }
    });

    // VJR: wait for ack to go out
    Wait.pause(5000);

    // Stop the durable client
    this.durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(Boolean.TRUE));

    // Verify durable client on server 1
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
      }
    });

    // Re-start server2
    this.server2VM.invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE,
        server2Port));

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> CacheServerTestUtil
        .createCacheClient(getClientPool(getServerHostName(),
            server1Port, server2Port, false), regionName));

    // Publish some entries
    final int numberOfEntries = 10;
    publishEntries(numberOfEntries);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      fail("interrupted", ex);
    }

    // Verify the durable client's queue contains the entries
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify the queue size
        assertEquals(numberOfEntries, proxy.getQueueSize());
      }
    });

    // Re-start the durable client that is kept alive on the server when it stops
    // normally
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        Boolean.TRUE));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Verify durable client on server 1
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(durableClientTimeout, proxy.getDurableTimeout());
      }
    });

    // Verify durable client on server 2
    this.server2VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(durableClientTimeout, proxy.getDurableTimeout());
      }
    });

    // Verify the HA region names are the same on both servers
    String server1HARegionQueueName =
        this.server1VM.invoke(DurableClientTestCase::getHARegionQueueName);
    String server2HARegionQueueName =
        this.server2VM.invoke(DurableClientTestCase::getHARegionQueueName);
    assertEquals(server1HARegionQueueName, server2HARegionQueueName);

    // Verify the durable client received the updates
    verifyDurableClientEvents(this.durableClientVM, numberOfEntries);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // off test flag for periodic ack
    this.server1VM
        .invoke(() -> DurableClientTestCase.setTestFlagToVerifyActForMarker(Boolean.FALSE));

    // Stop server 1
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 2
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  @Test
  public void testReadyForEventsNotCalledImplicitly() {
    // Start a server
    int serverPort = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    // make the client use ClientCacheFactory so it will have a default pool
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createClientCache(
        getClientPool(getServerHostName(), serverPort, true), regionName,
        getClientDistributedSystemProperties(durableClientId)));

    // verify that readyForEvents has not yet been called on the client's default pool
    this.durableClientVM.invoke(new CacheSerializableRunnable("check readyForEvents not called") {
      @Override
      public void run2() throws CacheException {
        for (Pool p : PoolManager.getAll().values()) {
          assertFalse(((PoolImpl) p).getReadyForEventsCalled());
        }
      }
    });

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Verify durable clients on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

        // Iterate the CacheClientProxies and verify they are correct
        checkNumberOfClientProxies(1);
        boolean durableClient1Found = false;
        for (CacheClientProxy proxy : notifier.getClientProxies()) {
          assertTrue(proxy.isDurable());
          if (proxy.getDurableId().equals(durableClientId)) {
            durableClient1Found = true;
          }
          assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT,
              proxy.getDurableTimeout());
        }
        assertTrue(durableClient1Found);
      }
    });

    // Stop the durable clients
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * This test method is disabled because it is failing periodically and causing cruise control
   * failures See bug #47060 (test seems to be enabled now!)
   */
  @Test
  public void testReadyForEventsNotCalledImplicitlyWithCacheXML() {
    try {
      setPeriodicACKObserver(durableClientVM);
      final String cqName = "cqTest";
      // Start a server
      int serverPort =
          this.server1VM.invoke(() -> CacheServerTestUtil.createCacheServerFromXml(
              DurableClientTestCase.class.getResource("durablecq-server-cache.xml")));

      // Start a durable client that is not kept alive on the server when it
      // stops normally
      final String durableClientId = getName() + "_client";

      // create client cache from xml
      this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClientFromXml(
          DurableClientTestCase.class.getResource("durablecq-client-cache.xml"), "client",
          durableClientId, 45, Boolean.FALSE));

      // verify that readyForEvents has not yet been called on all the client's pools
      this.durableClientVM.invoke(new CacheSerializableRunnable("check readyForEvents not called") {
        @Override
        public void run2() throws CacheException {
          for (Pool p : PoolManager.getAll().values()) {
            assertFalse(((PoolImpl) p).getReadyForEventsCalled());
          }
        }
      });

      // Send clientReady message
      this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
        @Override
        public void run2() throws CacheException {
          CacheServerTestUtil.getClientCache().readyForEvents();
        }
      });
      registerDurableCq(cqName);

      // Verify durable client on server1
      this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
        @Override
        public void run2() throws CacheException {
          // Find the proxy
          checkNumberOfClientProxies(1);
          CacheClientProxy proxy = getClientProxy();
          assertNotNull(proxy);

          // Verify that it is durable
          assertTrue(proxy.isDurable());
          assertEquals(durableClientId, proxy.getDurableId());
        }
      });

      // Start normal publisher client
      this.publisherClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
          getClientPool(getServerHostName(), serverPort, false),
          regionName));

      // Publish some entries
      final int numberOfEntries = 10;
      publishEntries(numberOfEntries);

      // Verify the durable client received the updates
      this.durableClientVM.invoke(new CacheSerializableRunnable("Verify updates") {
        @Override
        public void run2() throws CacheException {
          // Get the region
          Region region = CacheServerTestUtil.getCache().getRegion(regionName);
          assertNotNull(region);

          // Get the listener and wait for the appropriate number of events
          QueryService queryService = CacheServerTestUtil.getPool().getQueryService();
          CqQuery cqQuery = queryService.getCq(cqName);
          CacheServerTestUtil.ControlCqListener cqlistener =
              (CacheServerTestUtil.ControlCqListener) cqQuery.getCqAttributes().getCqListener();
          cqlistener.waitWhileNotEnoughEvents(30000, numberOfEntries);
          assertEquals(numberOfEntries, cqlistener.events.size());
        }
      });

      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        fail("interrupted", e);
      }

      // Stop the durable client
      this.durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(Boolean.TRUE));

      // Verify the durable client still exists on the server
      this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
        @Override
        public void run2() throws CacheException {
          // Find the proxy
          CacheClientProxy proxy = getClientProxy();
          assertNotNull(proxy);
        }
      });

      // Publish some more entries
      publishEntries(numberOfEntries);

      this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

      // Re-start the durable client
      this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClientFromXml(
          DurableClientTestCase.class.getResource("durablecq-client-cache.xml"), "client",
          durableClientId, 45, Boolean.FALSE));


      // Durable client registers durable cq on server
      this.durableClientVM.invoke(new CacheSerializableRunnable("Register cq") {
        @Override
        public void run2() throws CacheException {
          // Get the region
          Region region = CacheServerTestUtil.getCache().getRegion(regionName);
          assertNotNull(region);

          // Create CQ Attributes.
          CqAttributesFactory cqAf = new CqAttributesFactory();

          // Initialize and set CqListener.
          CqListener[] cqListeners = {new CacheServerTestUtil.ControlCqListener()};
          cqAf.initCqListeners(cqListeners);
          CqAttributes cqa = cqAf.create();

          // Create cq's
          // Get the query service for the Pool
          QueryService queryService = CacheServerTestUtil.getPool().getQueryService();

          try {
            CqQuery query = queryService.newCq(cqName, "Select * from /" + regionName, cqa, true);
            query.execute();
          } catch (CqExistsException | CqException e) {
            fail("Failed due to ", e);
          } catch (RegionNotFoundException e) {
            fail("Could not find specified region:" + regionName + ":", e);
          }

        }
      });

      // Send clientReady message
      this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
        @Override
        public void run2() throws CacheException {
          CacheServerTestUtil.getClientCache().readyForEvents();
        }
      });

      // Verify durable client on server
      this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
        @Override
        public void run2() throws CacheException {
          // Find the proxy
          checkNumberOfClientProxies(1);
          CacheClientProxy proxy = getClientProxy();
          assertNotNull(proxy);

          // Verify that it is durable and its properties are correct
          assertTrue(proxy.isDurable());
          assertEquals(durableClientId, proxy.getDurableId());
        }
      });

      // Verify the durable client received the updates held for it on the server
      this.durableClientVM.invoke(new CacheSerializableRunnable("Verify updates") {
        @Override
        public void run2() throws CacheException {
          // Get the region
          Region region = CacheServerTestUtil.getCache().getRegion(regionName);
          assertNotNull(region);

          QueryService queryService = CacheServerTestUtil.getPool().getQueryService();

          CqQuery cqQuery = queryService.getCq(cqName);

          CacheServerTestUtil.ControlCqListener cqlistener =
              (CacheServerTestUtil.ControlCqListener) cqQuery.getCqAttributes().getCqListener();
          cqlistener.waitWhileNotEnoughEvents(30000, numberOfEntries);
          assertEquals(numberOfEntries, cqlistener.events.size());
        }
      });

      // Stop the durable client
      this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

      // Stop the server
      this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    } finally {
      unsetPeriodicACKObserver(durableClientVM);
    }
  }

  protected void registerDurableCq(final String cqName) {
    // Durable client registers durable cq on server
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register Cq") {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Create CQ Attributes.
        CqAttributesFactory cqAf = new CqAttributesFactory();

        // Initialize and set CqListener.
        CqListener[] cqListeners = {new CacheServerTestUtil.ControlCqListener()};
        cqAf.initCqListeners(cqListeners);
        CqAttributes cqa = cqAf.create();

        // Create cq's
        // Get the query service for the Pool
        QueryService queryService = CacheServerTestUtil.getPool().getQueryService();

        try {
          CqQuery query = queryService.newCq(cqName, "Select * from /" + regionName, cqa, true);
          query.execute();
        } catch (CqExistsException | CqException e) {
          fail("Failed due to ", e);
        } catch (RegionNotFoundException e) {
          fail("Could not find specified region:" + regionName + ":", e);
        }
      }
    });
  }

  private void setPeriodicACKObserver(VM vm) {
    CacheSerializableRunnable cacheSerializableRunnable =
        new CacheSerializableRunnable("Set ClientServerObserver") {
          @Override
          public void run2() throws CacheException {
            PoolImpl.BEFORE_SENDING_CLIENT_ACK_CALLBACK_FLAG = true;
            ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
              @Override
              public void beforeSendingClientAck() {
                LogWriterUtils.getLogWriter().info("beforeSendingClientAck invoked");

              }
            });

          }
        };
    vm.invoke(cacheSerializableRunnable);
  }

  private void unsetPeriodicACKObserver(VM vm) {
    CacheSerializableRunnable cacheSerializableRunnable =
        new CacheSerializableRunnable("Unset ClientServerObserver") {
          @Override
          public void run2() throws CacheException {
            PoolImpl.BEFORE_SENDING_CLIENT_ACK_CALLBACK_FLAG = false;
          }
        };
    vm.invoke(cacheSerializableRunnable);
  }

  @Test
  public void testReadyForEventsNotCalledImplicitlyForRegisterInterestWithCacheXML() {
    regionName = "testReadyForEventsNotCalledImplicitlyWithCacheXML_region";
    // Start a server
    int serverPort =
        this.server1VM.invoke(() -> CacheServerTestUtil.createCacheServerFromXmlN(
            DurableClientTestCase.class.getResource("durablecq-server-cache.xml")));

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";

    // create client cache from xml
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClientFromXmlN(
        DurableClientTestCase.class.getResource("durablecq-client-cache.xml"), "client",
        durableClientId, 45, Boolean.TRUE));

    // verify that readyForEvents has not yet been called on all the client's pools
    this.durableClientVM.invoke(new CacheSerializableRunnable("check readyForEvents not called") {
      @Override
      public void run2() throws CacheException {
        for (Pool p : PoolManager.getAll().values()) {
          assertFalse(((PoolImpl) p).getReadyForEventsCalled());
        }
      }
    });

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Durable client registers durable cq on server
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register Interest") {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.KEYS_VALUES, true);
      }
    });

    // Verify durable client on server1
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
      }
    });

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), serverPort, false),
        regionName));

    // Publish some entries
    final int numberOfEntries = 10;
    publishEntries(numberOfEntries);

    // Verify the durable client received the updates
    this.durableClientVM.invoke(new CacheSerializableRunnable("Verify updates") {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Get the listener and wait for the appropriate number of events
        CacheServerTestUtil.ControlListener listener =
            (CacheServerTestUtil.ControlListener) region.getAttributes().getCacheListeners()[0];
        listener.waitWhileNotEnoughEvents(30000, numberOfEntries);
        assertEquals(numberOfEntries, listener.events.size());
      }
    });
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      fail("interrupted", e);
    }
    // Stop the durable client
    this.durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(Boolean.TRUE));

    // Verify the durable client still exists on the server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
      }
    });

    // Publish some more entries
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish additional updates") {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region<String, String> region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i = 0; i < numberOfEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue + "lkj");
        }
      }
    });
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Re-start the durable client
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClientFromXmlN(
        DurableClientTestCase.class.getResource("durablecq-client-cache.xml"), "client",
        durableClientId, 45, Boolean.TRUE));


    // Durable client registers durable cq on server
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.KEYS_VALUES, true);
      }
    });

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
      }
    });

    // Verify the durable client received the updates held for it on the server
    this.durableClientVM.invoke(new CacheSerializableRunnable("Verify updates") {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Get the listener and wait for the appropriate number of events
        CacheServerTestUtil.ControlListener listener =
            (CacheServerTestUtil.ControlListener) region.getAttributes().getCacheListeners()[0];
        listener.waitWhileNotEnoughEvents(30000, numberOfEntries);
        assertEquals(numberOfEntries, listener.events.size());
      }
    });

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Tests the ha queued events stat Connects a durable client, registers durable cqs and then shuts
   * down the durable client Publisher then does puts onto the server Events are queued up and the
   * stats are checked Durable client is then reconnected, events are dispatched and stats are
   * rechecked
   */
  @Test
  public void testHAQueueSizeStat() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int serverPort = ports[0];

    final String durableClientId = getName() + "_client";

    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);

    // verify cq stats are correct
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkHAQueueSize(server1VM, durableClientId, 10, 11);

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        4/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        5/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        10/* numEventsToWaitFor */, 15/* secondsToWait */);

    // Due to the implementation of DurableHARegionQueue where remove is called after dispatch.
    // This can cause events to linger in the queue due to a "later" ack and only cleared on
    // the next dispatch. We need to send one more message to dispatch, that calls remove one more
    // time and any remaining acks (with or without this final published events ack)
    flushEntries(server1VM, durableClientVM, regionName);
    checkHAQueueSize(server1VM, durableClientId, 0, 1);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Tests the ha queued events stat Connects a durable client, registers durable cqs and then shuts
   * down the durable client Publisher then does puts onto the server Events are queued up and the
   * stats are checked Test sleeps until durable client times out Stats should now be 0 Durable
   * client is then reconnected, no events should exist and stats are rechecked
   */
  @Test
  public void testHAQueueSizeStatExpired() {
    int timeoutInSeconds = 20;
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int serverPort = ports[0];

    final String durableClientId = getName() + "_client";

    startDurableClient(durableClientVM, durableClientId, serverPort, regionName, timeoutInSeconds);
    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);

    // verify cq stats are correct
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkHAQueueSize(server1VM, durableClientId, 10, 11);

    // pause until timeout
    try {
      Thread.sleep((timeoutInSeconds + 2) * 1000);
    } catch (InterruptedException ie) {
      fail("interrupted", ie);
    }
    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    checkCqListenerEvents(durableClientVM, "GreaterThan5", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);

    // Due to the implementation of DurableHARegionQueue where remove is called after dispatch.
    // This can cause events to linger in the queue due to a "later" ack and only cleared on
    // the next dispatch. We need to send one more message to dispatch, that calls remove one more
    // time and any remaining acks (with or without this final published events ack)
    flushEntries(server1VM, durableClientVM, regionName);
    checkHAQueueSize(server1VM, durableClientId, 0, 1);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Tests the ha queued events stat Starts up two servers, shuts one down Connects a durable
   * client, registers durable cqs and then shuts down the durable client Publisher then does puts
   * onto the server Events are queued up Durable client is then reconnected but does not send ready
   * for events Secondary server is brought back up Stats are checked Durable client then
   * reregisters cqs and sends ready for events
   */
  @Test
  public void testHAQueueSizeStatForGII() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int serverPort = ports[0];

    // Start server 2 using the same mcast port as server 1
    final int serverPort2 = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // shut down server 2
    closeCache(server2VM);

    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);

    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);

    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    verifyDurableClientOnServer(server1VM, durableClientId);
    checkNumDurableCqs(server1VM, durableClientId, 3);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);

    // Re-start server2, at this point it will be the first time server2 has connected to client
    this.server2VM.invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE,
        serverPort2));

    // Verify durable client on server2
    verifyDurableClientOnServer(server2VM, durableClientId);

    // verify cqs and stats on server 2. These events are through gii, stats should be correct
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkHAQueueSize(server2VM, durableClientId, 10, 11);

    closeCache(server1VM);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    // verify cq listeners received events
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        4/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        5/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        10/* numEventsToWaitFor */, 15/* secondsToWait */);

    // Verify stats are 0 for server2 (we failed over)
    flushEntries(server2VM, durableClientVM, regionName);
    checkHAQueueSize(server2VM, durableClientId, 0, 1);

    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the servers
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Tests the ha queued cq stat
   */
  @Test
  public void testHAQueuedCqStat() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int serverPort = ports[0];

    final String durableClientId = getName() + "_client";

    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);

    // verify cq stats are correct
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkCqStatOnServer(server1VM, durableClientId, "All", 10);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 5);

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        4/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        5/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        10/* numEventsToWaitFor */, 15/* secondsToWait */);


    // Due to the implementation of DurableHARegionQueue where remove is called after dispatch.
    // This can cause events to linger in the queue due to a "later" ack and only cleared on
    // the next dispatch. We need to send one more message to dispatch, that calls remove one more
    // time and any remaining acks (with or without this final published events ack)
    flushEntries(server1VM, durableClientVM, regionName);

    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "All", 0);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  @Test
  public void testHAQueuedCqStatOnSecondary() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int serverPort = ports[0];

    // Start server 2 using the same mcast port as server 1
    final int serverPort2 = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);

    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);

    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server 2
    verifyDurableClientOnServer(server2VM, durableClientId);

    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);

    // verify cq stats are correct on both servers
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkCqStatOnServer(server1VM, durableClientId, "All", 10);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 5);

    // verify cq stats are correct
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkCqStatOnServer(server2VM, durableClientId, "All", 10);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 5);

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        4/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        5/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        10/* numEventsToWaitFor */, 15/* secondsToWait */);

    // Verify stats are 0 for both servers
    flushEntries(server1VM, durableClientVM, regionName);

    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "All", 0);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Server 2 comes up, client connects and registers cqs, server 2 then disconnects events are put
   * into region client goes away server 2 comes back up and should get a gii check stats server 1
   * goes away client comes back and receives all events stats should still be correct
   */
  @Test
  public void testHAQueuedCqStatForGII() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int serverPort = ports[0];

    // Start server 2 using the same mcast port as server 1
    final int serverPort2 = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);

    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);

    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on both servers
    verifyDurableClientOnServer(server2VM, durableClientId);
    verifyDurableClientOnServer(server1VM, durableClientId);

    // verify durable cqs on both servers
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkNumDurableCqs(server2VM, durableClientId, 3);

    // shutdown server 2
    closeCache(server2VM);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);

    // Re-start server2, should get events through gii
    this.server2VM.invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE,
        serverPort2));

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);

    // verify cq stats are correct on server 2
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkCqStatOnServer(server2VM, durableClientId, "All", 10);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 5);

    closeCache(server1VM);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        4/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        5/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        10/* numEventsToWaitFor */, 15/* secondsToWait */);

    // Verify stats are 0 for server2 (we failed over)
    flushEntries(server2VM, durableClientVM, regionName);

    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);


    // Stop the durable clients
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the servers
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Start both servers, but shut down secondary server before durable client has connected. Connect
   * durable client to primary, register cqs and then shutdown durable client Publish events,
   * reconnect durable client but do not send ready for events Restart secondary and check stats to
   * be sure cqs have correct stats due to GII Shutdown primary and fail over to secondary Durable
   * Client sends ready or events and receives events Recheck stats
   */
  @Test
  public void testHAQueuedCqStatForGII2() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int serverPort = ports[0];

    // Start server 2 using the same mcast port as server 1
    final int serverPort2 = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // shut down server 2
    closeCache(server2VM);

    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);

    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);

    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    verifyDurableClientOnServer(server1VM, durableClientId);
    checkNumDurableCqs(server1VM, durableClientId, 3);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);

    // Re-start server2, at this point it will be the first time server2 has connected to client
    this.server2VM.invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE,
        serverPort2));

    // Verify durable client on server2
    verifyDurableClientOnServer(server2VM, durableClientId);

    // verify cqs and stats on server 2. These events are through gii, stats should be correct
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkCqStatOnServer(server2VM, durableClientId, "All", 10);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 5);

    closeCache(server1VM);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        4/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        5/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        10/* numEventsToWaitFor */, 15/* secondsToWait */);

    // Verify stats are 0 for server2 (we failed over)
    flushEntries(server2VM, durableClientVM, regionName);

    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the servers
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Server 2 comes up and goes down after client connects and registers cqs events are put into
   * region client goes away server 2 comes back up and should get a gii check stats client comes
   * back and receives all events stats should still be correct
   */
  @Test
  public void testHAQueuedCqStatForGIINoFailover() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int serverPort = ports[0];

    // Start server 2
    final int serverPort2 = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);

    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);

    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on both servers
    verifyDurableClientOnServer(server2VM, durableClientId);
    verifyDurableClientOnServer(server1VM, durableClientId);

    // verify durable cqs on both servers
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkNumDurableCqs(server2VM, durableClientId, 3);

    // shutdown server 2
    closeCache(server2VM);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);

    // Re-start server2, should get events through gii
    this.server2VM.invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE,
        serverPort2));

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);

    // verify cq stats are correct on server 2
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkCqStatOnServer(server2VM, durableClientId, "All", 10);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 5);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        4/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        5/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        10/* numEventsToWaitFor */, 15/* secondsToWait */);

    // Verify stats are 0 for server2 (we failed over)
    flushEntries(server2VM, durableClientVM, regionName);

    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "All", 0);

    // Stop the durable clients
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the servers
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * server 1 and 2 both get events server 1 goes down dc reconnects server 2 behaves accordingly
   */
  @Test
  public void testHAQueuedCqStatForFailover() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int serverPort = ports[0];

    // Start server 2 using the same mcast port as server 1
    final int serverPort2 = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);

    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);

    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on both servers
    verifyDurableClientOnServer(server2VM, durableClientId);
    verifyDurableClientOnServer(server1VM, durableClientId);

    // verify durable cqs on both servers
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkNumDurableCqs(server2VM, durableClientId, 3);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);

    closeCache(server1VM);

    // verify cq stats are correct on server 2
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkCqStatOnServer(server2VM, durableClientId, "All", 10);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 5);

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    // verify listeners on client
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        4/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        5/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        10/* numEventsToWaitFor */, 15/* secondsToWait */);

    // Verify stats are 0 for both servers
    flushEntries(server2VM, durableClientVM, regionName);

    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Tests the ha queued cq stat
   */
  @Test
  public void testHAQueuedCqStatWithTimeOut() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    int timeoutInSeconds = 20;
    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int serverPort = ports[0];

    final String durableClientId = getName() + "_client";

    startDurableClient(durableClientVM, durableClientId, serverPort, regionName, timeoutInSeconds);
    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);

    // verify cq stats are correct
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkCqStatOnServer(server1VM, durableClientId, "All", 10);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 5);

    // Pause for timeout
    try {
      Thread.sleep((timeoutInSeconds + 5) * 1000);
    } catch (InterruptedException e) {
      fail("interrupted", e);
    }

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    // Make sure all events are expired and are not sent
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);

    // Due to the implementation of DurableHARegionQueue where remove is called after dispatch.
    // This can cause events to linger in the queue due to a "later" ack and only cleared on
    // the next dispatch. We need to send one more message to dispatch, that calls remove one more
    // time and any remaining acks (with or without this final published events ack)
    flushEntries(server1VM, durableClientVM, regionName);

    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "All", 0);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test functionality to close the cq and drain all events from the ha queue from the server
   */
  @Test
  public void testCloseCqAndDrainEvents() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start a server
    int serverPort = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);


    this.server1VM.invoke(new CacheSerializableRunnable("Close cq for durable client") {
      @Override
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();

        try {
          ccnInstance.closeClientCq(durableClientId, "All");
        } catch (CqException e) {
          fail("failed", e);
        }
      }
    });

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    // verify cq events for all 3 cqs
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        4/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        5/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);


    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test functionality to close the cq and drain all events from the ha queue from the server This
   * draining should not affect events that still have register interest
   *
   */
  @Test
  public void testCloseAllCqsAndDrainEvents() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int serverPort = ports[0];

    final String durableClientId = getName() + "_client";

    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    // register durable cqs
    registerInterest(durableClientVM, regionName, true);
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);


    this.server1VM.invoke(new CacheSerializableRunnable("Close cq for durable client") {
      @Override
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();

        try {
          ccnInstance.closeClientCq(durableClientId, "All");
          ccnInstance.closeClientCq(durableClientId, "GreaterThan5");
          ccnInstance.closeClientCq(durableClientId, "LessThan5");
        } catch (CqException e) {
          fail("failed", e);
        }
      }
    });

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);

    // Reregister durable cqs
    registerInterest(durableClientVM, regionName, true);
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    checkCqListenerEvents(durableClientVM, "GreaterThan5", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);
    checkInterestEvents(durableClientVM, regionName, 10);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test functionality to close the cq and drain all events from the ha queue from the server This
   * draining should remove all events due to no interest registered Continues to publish afterwards
   * to verify that stats are correct
   */
  @Test
  public void testCloseAllCqsAndDrainEventsNoInterestRegistered() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int serverPort = ports[0];

    final String durableClientId = getName() + "_client";

    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);


    this.server1VM.invoke(new CacheSerializableRunnable("Close cq for durable client") {
      @Override
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();

        try {
          ccnInstance.closeClientCq(durableClientId, "All");
          ccnInstance.closeClientCq(durableClientId, "GreaterThan5");
          ccnInstance.closeClientCq(durableClientId, "LessThan5");
        } catch (CqException e) {
          fail("failed", e);
        }
      }
    });

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);


    checkCqListenerEvents(durableClientVM, "GreaterThan5", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);
    // Due to the implementation of DurableHARegionQueue where remove is called after dispatch.
    // This can cause events to linger in the queue due to a "later" ack and only cleared on
    // the next dispatch. We need to send one more message to dispatch, that calls remove one more
    // time and any remaining acks (with or without this final published events ack)
    flushEntries(server1VM, durableClientVM, regionName);

    // the flush entry message may remain in the queue due
    // verify the queue stats are as close/correct as possible
    this.checkHAQueueSize(server1VM, durableClientId, 0, 1);


    // continue to publish and make sure we get the events
    publishEntries(publisherClientVM, regionName, 10);
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        4/* numEventsToWaitFor */, 10/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        5/* numEventsToWaitFor */, 10/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        10/* numEventsToWaitFor */, 10/* secondsToWait */);

    // Due to the implementation of DurableHARegionQueue where remove is called after dispatch.
    // This can cause events to linger in the queue due to a "later" ack and only cleared on
    // the next dispatch. We need to send one more message to dispatch, that calls remove one more
    // time and any remaining acks (with or without this final published events ack)
    flushEntries(server1VM, durableClientVM, regionName);

    // the flush entry message may remain in the queue due
    // verify the queue stats are as close/correct as possible
    this.checkHAQueueSize(server1VM, durableClientId, 0, 1);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test functionality to close the cq and drain all events from the ha queue from the server Two
   * durable clients, one will have a cq be closed, the other should be unaffected
   */
  @Test
  public void testCloseCqAndDrainEvents2Client() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int serverPort = ports[0];

    final String durableClientId = getName() + "_client";
    final String durableClientId2 = getName() + "_client2";
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);

    // Stop the durable client
    this.disconnectDurableClient(true);

    startDurableClient(durableClientVM, durableClientId2, serverPort, regionName);
    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify 2nd durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify 2nd durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(2);
      }
    });

    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);


    this.server1VM.invoke(new CacheSerializableRunnable("Close cq for durable client 1") {
      @Override
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();

        try {
          ccnInstance.closeClientCq(durableClientId, "All");
        } catch (CqException e) {
          fail("failed", e);
        }
      }
    });

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);


    // verify cq events for all 3 cqs, where ALL should have 0 entries
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        4/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        5/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);

    this.disconnectDurableClient(false);

    // Restart the 2nd durable client
    startDurableClient(durableClientVM, durableClientId2, serverPort, regionName);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    // verify cq events for all 3 cqs, where ALL should have 10 entries
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        4/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        5/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        10/* numEventsToWaitFor */, 15/* secondsToWait */);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Tests situation where a client is trying to reconnect while a cq is being drained. The client
   * should be rejected until no cqs are currently being drained
   */
  @Test
  public void testRejectClientWhenDrainingCq() {
    try {
      IgnoredException.addIgnoredException(
          LocalizedStrings.CacheClientNotifier_COULD_NOT_CONNECT_DUE_TO_CQ_BEING_DRAINED
              .toLocalizedString());
      IgnoredException.addIgnoredException(
          "Could not initialize a primary queue on startup. No queue servers available.");

      String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
      String allQuery = "select * from /" + regionName + " p where p.ID > -1";
      String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

      // Start server 1
      Integer[] ports = this.server1VM.invoke(
          () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
      final int serverPort = ports[0];

      final String durableClientId = getName() + "_client";
      this.durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);

      startDurableClient(durableClientVM, durableClientId, serverPort, regionName);

      // register durable cqs
      createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
      createCq(durableClientVM, "All", allQuery, true);
      createCq(durableClientVM, "LessThan5", lessThan5Query, true);
      // send client ready
      sendClientReady(durableClientVM);

      verifyDurableClientOnServer(server1VM, durableClientId);

      // Stop the durable client
      this.disconnectDurableClient(true);

      // Start normal publisher client
      startClient(publisherClientVM, serverPort, regionName);

      // Publish some entries
      publishEntries(publisherClientVM, regionName, 10);

      this.server1VM.invoke(new CacheSerializableRunnable("Set test hook") {
        @Override
        public void run2() throws CacheException {
          // Set the Test Hook!
          // This test hook will pause during the drain process
          CacheClientProxy.testHook = new RejectClientReconnectTestHook();
        }
      });

      this.server1VM.invokeAsync(new CacheSerializableRunnable("Close cq for durable client") {
        @Override
        public void run2() throws CacheException {

          final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();

          try {
            ccnInstance.closeClientCq(durableClientId, "All");
          } catch (CqException e) {
            fail("failed", e);
          }
        }
      });

      // Restart the durable client
      startDurableClient(durableClientVM, durableClientId, serverPort, regionName);

      this.server1VM.invoke(new CacheSerializableRunnable("verify was rejected at least once") {
        @Override
        public void run2() throws CacheException {
          Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
              .until(() -> CacheClientProxy.testHook != null
                  && (((RejectClientReconnectTestHook) CacheClientProxy.testHook)
                      .wasClientRejected()));
          assertTrue(
              ((RejectClientReconnectTestHook) CacheClientProxy.testHook).wasClientRejected());
        }
      });

      checkPrimaryUpdater(durableClientVM);

      // After rejection, the client will retry and eventually connect
      // Verify durable client on server2
      verifyDurableClientOnServer(server1VM, durableClientId);

      createCq(durableClientVM, "GreaterThan5",
          "select * from /" + regionName + " p where p.ID > 5", true);
      createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
      createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
          true);
      // send client ready
      sendClientReady(durableClientVM);

      checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
          4/* numEventsToWaitFor */, 15/* secondsToWait */);
      checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
          5/* numEventsToWaitFor */, 15/* secondsToWait */);
      checkCqListenerEvents(durableClientVM, "All", 0 /* numEventsExpected */,
          1/* numEventsToWaitFor */, 5/* secondsToWait */);

      // Stop the durable client
      this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

      // Stop the publisher client
      this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

      // Stop the server
      this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    } finally {
      this.server1VM.invoke(new CacheSerializableRunnable("unset test hook") {
        @Override
        public void run2() throws CacheException {
          CacheClientProxy.testHook = null;
        }
      });
    }
  }


  /**
   * Tests scenario where close cq will throw an exception due to a client being reactivated
   */
  @Test
  public void testCqCloseExceptionDueToActivatingClient() throws Exception {
    try {
      String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
      String allQuery = "select * from /" + regionName + " p where p.ID > -1";
      String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

      // Start server 1
      Integer[] ports = this.server1VM.invoke(
          () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
      final int serverPort = ports[0];

      final String durableClientId = getName() + "_client";

      startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
      // register durable cqs
      createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
      createCq(durableClientVM, "All", allQuery, true);
      createCq(durableClientVM, "LessThan5", lessThan5Query, true);
      // send client ready
      sendClientReady(durableClientVM);

      // Verify durable client on server
      verifyDurableClientOnServer(server1VM, durableClientId);

      // Stop the durable client
      this.disconnectDurableClient(true);

      // Start normal publisher client
      startClient(publisherClientVM, serverPort, regionName);

      // Publish some entries
      publishEntries(publisherClientVM, regionName, 10);


      AsyncInvocation async =
          this.server1VM.invokeAsync(new CacheSerializableRunnable("Close cq for durable client") {
            @Override
            public void run2() throws CacheException {

              // Set the Test Hook!
              // This test hook will pause during the drain process
              CacheClientProxy.testHook = new CqExceptionDueToActivatingClientTestHook();

              final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();
              final CacheClientProxy clientProxy = ccnInstance.getClientProxy(durableClientId);
              ClientProxyMembershipID proxyId = clientProxy.getProxyID();

              try {
                ccnInstance.closeClientCq(durableClientId, "All");
                fail("Should have thrown an exception due to activating client");
              } catch (CqException e) {
                String expected =
                    LocalizedStrings.CacheClientProxy_COULD_NOT_DRAIN_CQ_DUE_TO_RESTARTING_DURABLE_CLIENT
                        .toLocalizedString("All", proxyId.getDurableId());
                if (!e.getMessage().equals(expected)) {
                  fail("Not the expected exception, was expecting "
                      + (LocalizedStrings.CacheClientProxy_COULD_NOT_DRAIN_CQ_DUE_TO_RESTARTING_DURABLE_CLIENT
                          .toLocalizedString("All", proxyId.getDurableId())
                          + " instead of exception: " + e.getMessage()),
                      e);
                }
              }
            }
          });

      // Restart the durable client
      startDurableClient(durableClientVM, durableClientId, serverPort, regionName);

      // Reregister durable cqs
      createCq(durableClientVM, "GreaterThan5",
          "select * from /" + regionName + " p where p.ID > 5", true);
      createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
      createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
          true);
      // send client ready
      sendClientReady(durableClientVM);

      async.join();
      assertFalse(async.getException() != null ? async.getException().toString() : "No error",
          async.exceptionOccurred());

      // verify cq listener events
      checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
          4/* numEventsToWaitFor */, 15/* secondsToWait */);
      checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
          5/* numEventsToWaitFor */, 15/* secondsToWait */);
      checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
          10/* numEventsToWaitFor */, 15/* secondsToWait */);

      // Stop the durable client
      this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

      // Stop the publisher client
      this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

      // Stop the server
      this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    } finally {
      this.server1VM.invoke(new CacheSerializableRunnable("unset test hook") {
        @Override
        public void run2() throws CacheException {
          CacheClientProxy.testHook = null;
        }
      });
    }
  }

  /**
   * Tests situation where a client is trying to reconnect while a cq is being drained
   */
  @Test
  public void testCqCloseExceptionDueToActiveConnection() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start a server
    int serverPort = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    sendClientReady(durableClientVM);

    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);

    verifyDurableClientOnServer(server1VM, durableClientId);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);

    // Attempt to close a cq even though the client is running
    this.server1VM.invoke(new CacheSerializableRunnable("Close cq for durable client") {
      @Override
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();
        final CacheClientProxy clientProxy = ccnInstance.getClientProxy(durableClientId);
        ClientProxyMembershipID proxyId = clientProxy.getProxyID();

        try {
          ccnInstance.closeClientCq(durableClientId, "All");
          fail(
              "expected a cq exception.  We have an active client proxy, the close cq command should have failed");
        } catch (CqException e) {
          // expected exception;
          String expected =
              LocalizedStrings.CacheClientProxy_COULD_NOT_DRAIN_CQ_DUE_TO_ACTIVE_DURABLE_CLIENT
                  .toLocalizedString("All", proxyId.getDurableId());
          if (!e.getMessage().equals(expected)) {
            fail("Not the expected exception, was expecting "
                + (LocalizedStrings.CacheClientProxy_COULD_NOT_DRAIN_CQ_DUE_TO_ACTIVE_DURABLE_CLIENT
                    .toLocalizedString("All", proxyId.getDurableId()) + " instead of exception: "
                    + e.getMessage()),
                e);
          }
        }
      }
    });

    // verify cq events for all 3 cqs
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        4/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        5/* numEventsToWaitFor */, 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        10/* numEventsToWaitFor */, 15/* secondsToWait */);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test functionality to close the durable client and drain all events from the ha queue from the
   * server
   */
  @Test
  public void testCloseCacheProxy() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start a server
    int serverPort = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);

    // verify cq stats are correct
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkCqStatOnServer(server1VM, durableClientId, "All", 10);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 5);

    // drop client proxy
    this.server1VM.invoke(
        new CacheSerializableRunnable("Close client proxy on server for client" + durableClientId) {
          @Override
          public void run2() throws CacheException {

            final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();

            ccnInstance.closeDurableClientProxy(durableClientId);
          }
        });


    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);

    // check that cqs are no longer registered
    checkNumDurableCqs(server1VM, durableClientId, 0);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);

    // Before sending client ready, lets make sure the stats already reflect 0 queued events
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "All", 0);

    // send client ready
    sendClientReady(durableClientVM);

    // verify cq events for all 3 cqs are 0 events
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 0 /* numEventsExpected */,
        1/* numEventsToWaitFor */, 5/* secondsToWait */);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test that starting a durable client on multiple servers is processed correctly.
   */
  @Test
  public void testSimpleDurableClientMultipleServers() {
    // Start server 1
    Integer[] ports = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int server1Port = ports[0];

    // Start server 2 using the same mcast port as server 1
    final int server2Port = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client connected to both servers that is kept alive when
    // it stops normally
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    // final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        Boolean.TRUE));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Verify durable client on server 1
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(durableClientTimeout, proxy.getDurableTimeout());
      }
    });

    // Verify durable client on server 2
    this.server2VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(durableClientTimeout, proxy.getDurableTimeout());
      }
    });

    // Stop the durable client
    this.durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(Boolean.TRUE));

    // Verify the durable client is still on server 1
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
      }
    });

    // Verify the durable client is still on server 2
    this.server2VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
      }
    });

    // Start up the client again. This time initialize it so that it is not kept
    // alive on the servers when it stops normally.
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true),
        regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Verify durable client on server1
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, proxy.getDurableTimeout());
      }
    });

    // Verify durable client on server2
    this.server2VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        checkProxyIsAlive(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, proxy.getDurableTimeout());
      }
    });

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    this.verifySimpleDurableClientMultipleServers();

    // Stop server 1
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 2
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  @Test
  public void testDurableClientReceivedClientSessionInitialValue() {
    // Start server 1
    int server1Port = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start server 2
    int server2Port = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> CacheServerTestUtil
        .createCacheClient(getClientPool(getServerHostName(),
            server1Port, server2Port, false), this.regionName));

    // Create an entry
    publishEntries(1);

    // Start a durable client with the ControlListener
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    restartDurableClient(new Object[] {
        getClientPool(getServerHostName(), server1Port, server2Port,
            true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        true});

    // Use ClientSession on the server to register interest in entry key on behalf of durable client
    boolean server1IsPrimary = false;
    boolean registered = this.server1VM.invoke(() -> DurableClientSimpleDUnitTest
        .registerInterestWithClientSession(durableClientId, this.regionName, String.valueOf(0)));
    if (registered) {
      server1IsPrimary = true;
    } else {
      registered = this.server2VM.invoke(() -> DurableClientSimpleDUnitTest
          .registerInterestWithClientSession(durableClientId, this.regionName, String.valueOf(0)));
      if (!registered) {
        fail("ClientSession interest registration failed to occur in either server.");
      }
    }

    // Verify durable client received create event
    verifyDurableClientEvents(this.durableClientVM, 1, TYPE_CREATE);

    // Wait for QRM to be processed on the secondary
    waitForEventsRemovedByQueueRemovalMessage(server1IsPrimary ? this.server2VM : this.server1VM,
        durableClientId, 2);

    // Stop durable client
    disconnectDurableClient(true);

    // Restart durable client
    restartDurableClient(new Object[] {
        getClientPool(getServerHostName(), server1Port, server2Port,
            true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        true});

    // Verify durable client does not receive create event
    verifyNoDurableClientEvents(this.durableClientVM, 1, TYPE_CREATE);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 1
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 2
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  public static boolean registerInterestWithClientSession(String durableClientId, String regionName,
      Object keyOfInterest) {
    ClientSession session = getBridgeServer().getClientSession(durableClientId);
    boolean registered = false;
    if (session.isPrimary()) {
      session.registerInterest(regionName, keyOfInterest, InterestResultPolicy.KEYS_VALUES, true,
          true);
      registered = true;
    }
    return registered;
  }

  @Test
  public void testGetAllDurableCqsFromServer() {
    // Start server 1
    final int server1Port = this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, Boolean.TRUE});

    // Start server 2
    final int server2Port = this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, Boolean.TRUE});

    // Start a durable client
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient",
        new Object[] {
            getClientPool(getServerHostName(), server1Port, server2Port,
                true, 0),
            regionName, getClientDistributedSystemProperties(durableClientId, 60), Boolean.TRUE});

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getClientCache().readyForEvents();
      }
    });

    // Register durable CQ
    String cqName = getName() + "_cq";
    registerDurableCq(cqName);

    // Execute getAllDurableCqsFromServer on the client
    List<String> durableCqNames =
        this.durableClientVM.invoke(DurableClientSimpleDUnitTest::getAllDurableCqsFromServer);

    this.durableClientVM.invoke(() -> verifyDurableCqs(durableCqNames, cqName));

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the servers
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  public static List<String> getAllDurableCqsFromServer() throws CqException {
    QueryService queryService = CacheServerTestUtil.getPool().getQueryService();
    return queryService.getAllDurableCqsFromServer();
  }

  public static void verifyDurableCqs(final List<String> durableCqNames,
      final String registeredCqName) {
    // Verify the number of durable CQ names is one, and it matches the registered name
    assertEquals(1, durableCqNames.size());
    String returnedCqName = durableCqNames.get(0);
    assertEquals(registeredCqName, returnedCqName);

    // Get client's primary server
    PoolImpl pool = CacheServerTestUtil.getPool();
    ServerLocation primaryServerLocation = pool.getPrimary();

    // Verify the primary server was used and no other server was used
    Map<ServerLocation, ConnectionStats> statistics = pool.getEndpointManager().getAllStats();
    for (Map.Entry<ServerLocation, ConnectionStats> entry : statistics.entrySet()) {
      int expectedGetDurableCqInvocations = entry.getKey().equals(primaryServerLocation) ? 1 : 0;
      assertEquals(expectedGetDurableCqInvocations, entry.getValue().getGetDurableCqs());
    }
  }


  private void waitForEventsRemovedByQueueRemovalMessage(VM secondaryServerVM,
      final String durableClientId, final int numEvents) {
    secondaryServerVM.invoke(() -> DurableClientSimpleDUnitTest
        .waitForEventsRemovedByQueueRemovalMessage(durableClientId, numEvents));
  }

  private static void waitForEventsRemovedByQueueRemovalMessage(String durableClientId,
      final int numEvents) {
    CacheClientNotifier ccn = CacheClientNotifier.getInstance();
    CacheClientProxy ccp = ccn.getClientProxy(durableClientId);
    HARegionQueue harq = ccp.getHARegionQueue();
    HARegionQueueStats harqStats = harq.getStatistics();
    Awaitility.await().atMost(10, TimeUnit.SECONDS)
        .until(
            () -> assertEquals(
                "Expected queue removal messages: " + numEvents + " but actual messages: "
                    + harqStats.getEventsRemovedByQrm(),
                numEvents, harqStats.getEventsRemovedByQrm()));
  }
}
