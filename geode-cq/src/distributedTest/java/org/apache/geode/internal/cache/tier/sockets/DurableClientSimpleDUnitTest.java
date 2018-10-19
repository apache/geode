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

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.Thread.sleep;
import static org.apache.geode.cache.InterestResultPolicy.NONE;
import static org.apache.geode.cache.client.PoolManager.createFactory;
import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.TYPE_CREATE;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createCacheClient;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createCacheServer;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.apache.geode.test.dunit.Wait.pause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.ha.HARegionQueueStats;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class DurableClientSimpleDUnitTest extends DurableClientTestBase {

  /**
   * Test that a durable client correctly receives updates.
   */
  @Test
  public void testSimpleDurableClientUpdate() {
    // Start a server
    server1Port = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is not kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId), Boolean.TRUE));

    // Send clientReady message
    sendClientReady(this.durableClientVM);

    registerInterest(durableClientVM, regionName, true, InterestResultPolicy.NONE);

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), server1Port, false),
        regionName));

    // Publish some entries
    final int numberOfEntries = 10;
    publishEntries(0, 10);

    // Verify the durable client received the updates
    checkListenerEvents(numberOfEntries, 1, -1, this.durableClientVM);

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
    server1Port = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client with 2 regions (and 2 BridgeClients) that is not
    // kept alive on the server when it stops normally
    final String durableClientId = getName() + "_client";
    final String regionName1 = regionName + "1";
    final String regionName2 = regionName + "2";
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClients(
        getClientPool(getServerHostName(), server1Port, true), regionName1,
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
    server1Port = this.server1VM
        .invoke(() -> createCacheServer(regionName, TRUE));

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId), TRUE));

    // Send clientReady message
    sendClientReady(this.durableClientVM);

    registerInterest(durableClientVM, regionName, true, NONE);

    // Attempt to start another durable client VM with the same id.
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Create another durable client") {
      @Override
      public void run2() throws CacheException {
        getSystem(getClientDistributedSystemProperties(durableClientId));
        PoolFactoryImpl pf = (PoolFactoryImpl) createFactory();
        pf.init(getClientPool(getServerHostName(), server1Port, true));
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
    verifyDurableClientPresent(DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, false),
        regionName));

    // Publish some entries
    final int numberOfEntries = 10;
    publishEntries(0, numberOfEntries);

    // Verify the durable client received the updates
    checkListenerEvents(numberOfEntries, 1, -1, this.durableClientVM);

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
    server1Port = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId)));

    // Send clientReady message
    sendClientReady(this.durableClientVM);

    // Start another durable client that is not kept alive on the server when
    // it stops normally. Use the 'publisherClientVM' as a durable client.
    VM durableClient2VM = this.publisherClientVM;
    final String durableClientId2 = getName() + "_client2";
    durableClient2VM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId2)));

    // Send clientReady message
    sendClientReady(durableClient2VM);

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
  public void testDurableClientMultipleServersOneLive() throws InterruptedException {
    // Start server 1
    server1Port = this.server1VM
        .invoke(() -> createCacheServer(regionName, TRUE));

    // Start server 2
    final int server2Port = this.server2VM
        .invoke(() -> createCacheServer(regionName, TRUE));

    // Stop server 2
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    // final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        TRUE));

    // Send clientReady message
    sendClientReady(this.durableClientVM);

    registerInterest(durableClientVM, regionName, true, NONE);

    // Verify durable client on server1
    verifyDurableClientPresent(durableClientTimeout, durableClientId, server1VM);
    // Start normal publisher client
    this.publisherClientVM.invoke(() -> createCacheClient(getClientPool(getServerHostName(),
        server1Port, server2Port, false), regionName));

    // Publish some entries
    final int numberOfEntries = 10;
    publishEntries(0, numberOfEntries);

    // Verify the durable client received the updates
    checkListenerEvents(numberOfEntries, 1, -1, this.durableClientVM);

    sleep(10000);

    // Stop the durable client
    this.durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(TRUE));

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
    publishEntries(10, numberOfEntries);

    // Re-start the durable client
    this.durableClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true),
        regionName, getClientDistributedSystemProperties(durableClientId), TRUE));

    // Send clientReady message
    sendClientReady(this.durableClientVM);

    // Verify durable client on server
    verifyDurableClientPresent(durableClientTimeout, durableClientId, server1VM);

    // Verify the durable client received the updates held for it on the server
    checkListenerEvents(numberOfEntries, 1, -1, this.durableClientVM);

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
  public void testTwoDurableClientsStartStopUpdate() throws InterruptedException {
    // Start a server
    server1Port = this.server1VM
        .invoke(() -> createCacheServer(regionName, TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    // final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        TRUE));

    // Send clientReady message
    sendClientReady(this.durableClientVM);

    registerInterest(durableClientVM, regionName, true, NONE);

    // Start another durable client that is not kept alive on the server when
    // it stops normally. Use the 'server2VM' as the second durable client.
    VM durableClient2VM = this.server2VM;
    final String durableClientId2 = getName() + "_client2";
    durableClient2VM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId2, durableClientTimeout),
        TRUE));

    // Send clientReady message
    sendClientReady(durableClient2VM);

    registerInterest(durableClient2VM, regionName, true, NONE);

    // Verify durable clients on server
    verifyMultupleDurableClients(durableClientId, durableClientTimeout, durableClientId2);

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, false),
        regionName));

    // Publish some entries
    final int numberOfEntries = 10;
    publishEntries(0, numberOfEntries);

    // Verify durable client 1 received the updates
    checkListenerEvents(numberOfEntries, 1, -1, this.durableClientVM);

    // Verify durable client 2 received the updates
    checkListenerEvents(numberOfEntries, 1, -1, durableClient2VM);

    // ARB: Wait for queue ack to arrive at server.
    sleep(1000);

    // Stop the durable clients
    this.durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(TRUE));
    durableClient2VM.invoke(() -> CacheServerTestUtil.closeCache(TRUE));

    // Verify the durable clients still exist on the server
    verifyMultupleDurableClients(durableClientId, durableClientTimeout, durableClientId2);

    // Publish some more entries
    publishEntries(10, numberOfEntries);

    sleep(1000);

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
    this.durableClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId), TRUE));

    // Send clientReady message
    sendClientReady(this.durableClientVM);

    // Re-start durable client 2
    durableClient2VM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId2), TRUE));

    // Send clientReady message
    sendClientReady(durableClient2VM);

    // Verify durable client 1 received the updates held for it on the server
    checkListenerEvents(numberOfEntries, 1, -1, this.durableClientVM);

    // Verify durable client 2 received the updates held for it on the server
    checkListenerEvents(numberOfEntries, 1, -1, this.durableClientVM);

    // Stop durable client 1
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop durable client 2
    durableClient2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  private void verifyMultupleDurableClients(String durableClientId, int durableClientTimeout,
      String durableClientId2) {
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
  }


  /**
   * Tests whether a durable client reconnects properly to two servers.
   */
  @Test
  public void testDurableClientReconnectTwoServers() throws InterruptedException {
    // Start server 1
    server1Port = this.server1VM.invoke(
        () -> createCacheServer(regionName, TRUE));

    // on test flag for periodic ack
    this.server1VM
        .invoke(() -> setTestFlagToVerifyActForMarker(TRUE));

    // Start server 2 using the same mcast port as server 1
    final int server2Port = this.server2VM
        .invoke(() -> createCacheServer(regionName, TRUE));

    // Stop server 2
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    // final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        TRUE));

    // Send clientReady message
    sendClientReady(this.durableClientVM);

    registerInterest(durableClientVM, regionName, true, NONE);

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
        verifyReceivedMarkerAck();
      }
    });

    // VJR: wait for ack to go out
    pause(5000);

    // Stop the durable client
    this.durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(TRUE));

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
    this.server2VM.invoke(() -> createCacheServer(regionName, TRUE,
        server2Port));

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> createCacheClient(getClientPool(getServerHostName(),
        server1Port, server2Port, false), regionName));

    // Publish some entries
    final int numberOfEntries = 10;
    publishEntries(0, numberOfEntries);

    sleep(1000);

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
    this.durableClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        TRUE));

    // Send clientReady message
    sendClientReady(this.durableClientVM);

    // Verify durable client on server 1
    verifyDurableClientPresence(durableClientTimeout, durableClientId, server1VM, 1);

    // Verify durable client on server 2
    verifyDurableClientPresence(durableClientTimeout, durableClientId, server2VM, 1);

    // Verify the HA region names are the same on both servers
    String server1HARegionQueueName =
        this.server1VM.invoke(DurableClientTestBase::getHARegionQueueName);
    String server2HARegionQueueName =
        this.server2VM.invoke(DurableClientTestBase::getHARegionQueueName);
    assertEquals(server1HARegionQueueName, server2HARegionQueueName);

    // Verify the durable client received the updates
    checkListenerEvents(numberOfEntries, 1, -1, this.durableClientVM);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // off test flag for periodic ack
    this.server1VM
        .invoke(() -> setTestFlagToVerifyActForMarker(FALSE));

    // Stop server 1
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 2
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  @Test
  public void testReadyForEventsNotCalledImplicitly() {
    // Start a server
    server1Port = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    // make the client use ClientCacheFactory so it will have a default pool
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createClientCache(
        getClientPool(getServerHostName(), server1Port, true), regionName,
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
    sendClientReady(this.durableClientVM);

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



  @Test
  public void testReadyForEventsNotCalledImplicitlyForRegisterInterestWithCacheXML() {
    regionName = "testReadyForEventsNotCalledImplicitlyWithCacheXML_region";
    // Start a server
    server1Port =
        this.server1VM.invoke(() -> CacheServerTestUtil.createCacheServerFromXmlN(
            DurableClientTestBase.class.getResource("durablecq-server-cache.xml")));

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";

    // create client cache from xml
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClientFromXmlN(
        DurableClientTestBase.class.getResource("durablecq-client-cache.xml"), "client",
        durableClientId, DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, Boolean.TRUE));

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
    sendClientReady(this.durableClientVM);

    // Durable client registers durable cq on server
    registerInterest(durableClientVM, regionName, true, InterestResultPolicy.KEYS_VALUES);

    // Verify durable client on server1
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), server1Port, false),
        regionName));

    // Publish some entries
    final int numberOfEntries = 10;
    publishEntries(0, numberOfEntries);

    // Verify the durable client received the updates
    checkListenerEvents(numberOfEntries, 1, -1, durableClientVM);

    // Stop the durable client
    this.durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(Boolean.TRUE));

    // Verify the durable client still exists on the server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

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
        DurableClientTestBase.class.getResource("durablecq-client-cache.xml"), "client",
        durableClientId, DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, Boolean.TRUE));

    // Durable client registers durable cq on server'
    registerInterest(durableClientVM, regionName, true, InterestResultPolicy.KEYS_VALUES);

    // Send clientReady message
    sendClientReady(this.durableClientVM);

    // Verify durable client on server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Verify the durable client received the updates held for it on the server
    checkListenerEvents(numberOfEntries, 1, -1, durableClientVM);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

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
    server1Port = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    durableClientId = getName() + "_client";
    startDurableClient(durableClientVM, durableClientId, server1Port, regionName);
    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, server1Port, regionName);

    // Publish some entries
    publishEntries(regionName, 10);

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
    startDurableClient(durableClientVM, durableClientId, server1Port, regionName);

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
        /* numEventsToWaitFor */ 5/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 0 /* numEventsExpected */,
        /* numEventsToWaitFor */ 5/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 0 /* numEventsExpected */,
        /* numEventsToWaitFor */ 5/* secondsToWait */);

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
    server1Port = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start server 2 using the same mcast port as server 1
    final int server2Port = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client connected to both servers that is kept alive when
    // it stops normally

    durableClientId = getName() + "_client";
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true),
        regionName,
        getClientDistributedSystemProperties(durableClientId, VERY_LONG_DURABLE_TIMEOUT_SECONDS),
        Boolean.TRUE));

    // Send clientReady message
    sendClientReady(this.durableClientVM);

    // Verify durable client on server 1
    verifyDurableClientPresence(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server1VM, 1);

    // Verify durable client on server 2
    verifyDurableClientPresence(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server2VM, 1);

    // Stop the durable client
    this.durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(Boolean.TRUE));

    // Verify the durable client is still on server 1
    verifyDurableClientPresence(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server1VM, 1);

    // Verify the durable client is still on server 2
    verifyDurableClientPresence(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server2VM, 1);

    // Start up the client again. This time initialize it so that it is not kept
    // alive on the servers when it stops normally.
    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true),
        regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE));

    // Send clientReady message
    sendClientReady(this.durableClientVM);

    // Verify durable client on server1
    verifyDurableClientPresence(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM, 1);

    // Verify durable client on server2
    verifyDurableClientPresence(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server2VM, 1);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    this.verifyDurableClientPresence(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT,
        durableClientId, server1VM, 0);

    this.verifyDurableClientPresence(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT,
        durableClientId, server2VM, 0);

    // Stop server 1
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 2
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  @Test
  public void testDurableClientReceivedClientSessionInitialValue() {
    // Start server 1
    server1Port = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start server 2
    int server2Port = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start normal publisher client
    this.publisherClientVM.invoke(() -> CacheServerTestUtil
        .createCacheClient(getClientPool(getServerHostName(),
            server1Port, server2Port, false), this.regionName));

    // Create an entry
    publishEntries(0, 1);

    // Start a durable client with the ControlListener
    durableClientId = getName() + "_client";
    startupDurableClient(VERY_LONG_DURABLE_TIMEOUT_SECONDS,
        getClientPool(getServerHostName(), server1Port, server2Port,
            true),
        Boolean.TRUE);

    // Use ClientSession on the server to ` in entry key on behalf of durable client
    boolean server1IsPrimary = false;
    boolean registered = this.server1VM.invoke(() -> DurableClientSimpleDUnitTest
        .registerInterestWithClientSession(durableClientId, this.regionName, String.valueOf(0)));
    if (registered) {
      server1IsPrimary = true;
    } else {
      registered = this.server2VM.invoke(() -> DurableClientSimpleDUnitTest
          .registerInterestWithClientSession(durableClientId, this.regionName, String.valueOf(0)));
    }
    assertThat(registered)
        .describedAs("ClientSession interest registration failed to occur in either server.")
        .isTrue();

    // Verify durable client received create event
    checkListenerEvents(1, 1, TYPE_CREATE, this.durableClientVM);

    // Wait for QRM to be processed on the secondary
    waitForEventsRemovedByQueueRemovalMessage(server1IsPrimary ? this.server2VM : this.server1VM,
        durableClientId, 2);

    // Stop durable client
    disconnectDurableClient(true);

    // restart durable client
    restartDurableClient(VERY_LONG_DURABLE_TIMEOUT_SECONDS,
        getClientPool(getServerHostName(), server1Port, server2Port, true), Boolean.TRUE);

    // Verify durable client does not receive create event
    checkListenerEvents(0, 1, TYPE_CREATE, this.durableClientVM);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 1
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 2
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  private static boolean registerInterestWithClientSession(String durableClientId,
      String regionName,
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


  private void waitForEventsRemovedByQueueRemovalMessage(VM secondaryServerVM,
      final String durableClientId,
      final int numEvents) {
    secondaryServerVM.invoke(() -> DurableClientSimpleDUnitTest
        .waitForEventsRemovedByQueueRemovalMessage(durableClientId, numEvents));
  }

  private static void waitForEventsRemovedByQueueRemovalMessage(String durableClientId,
      final int numEvents) {
    CacheClientNotifier ccn = CacheClientNotifier.getInstance();
    CacheClientProxy ccp = ccn.getClientProxy(durableClientId);
    HARegionQueue haRegionQueue = ccp.getHARegionQueue();
    HARegionQueueStats haRegionQueueStats = haRegionQueue.getStatistics();
    await()
        .untilAsserted(
            () -> assertEquals(
                "Expected queue removal messages: " + numEvents + " but actual messages: "
                    + haRegionQueueStats.getEventsRemovedByQrm(),
                numEvents, haRegionQueueStats.getEventsRemovedByQrm()));
  }
}
