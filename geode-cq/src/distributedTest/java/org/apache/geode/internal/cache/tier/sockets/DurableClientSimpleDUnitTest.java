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

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.cache.InterestResultPolicy.NONE;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.TYPE_CREATE;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createCacheClient;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createCacheClientFromXmlN;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createCacheClients;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createCacheServer;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createCacheServerFromXmlN;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createClientCache;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.getCache;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.getClientCache;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.apache.geode.test.dunit.Wait.pause;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.ClientSession;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.DistributionConfig;
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
    server1Port = server1VM
        .invoke(() -> createCacheServer(regionName, true));

    // Start a durable client that is not kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId), true));

    // Send clientReady message
    sendClientReady(durableClientVM);

    registerInterest(durableClientVM, regionName, true, InterestResultPolicy.NONE);

    // Start normal publisher client
    publisherClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, false),
        regionName));

    // Publish some entries
    final int numberOfEntries = 10;
    publishEntries(publisherClientVM, 0, 10);

    // Verify the durable client received the updates
    checkListenerEvents(numberOfEntries, 1, -1, durableClientVM);

    // Stop the durable client
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test that a durable client VM with multiple BridgeClients correctly registers on the server.
   */
  @Test
  public void testMultipleBridgeClientsInSingleDurableVM() {
    // Start a server
    server1Port = server1VM.invoke(() -> createCacheServer(regionName, true));

    // Start a durable client with 2 regions (and 2 BridgeClients) that is not
    // kept alive on the server when it stops normally
    final String durableClientId = getName() + "_client";
    final String regionName1 = regionName + "1";
    final String regionName2 = regionName + "2";
    durableClientVM.invoke(() -> createCacheClients(
        getClientPool(getServerHostName(), server1Port, true), regionName1,
        regionName2, getClientDistributedSystemProperties(durableClientId)));

    // Send clientReady message
    durableClientVM.invoke("Send clientReady", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        assertThat(PoolManager.getAll()).hasSize(2);
        getClientCache().readyForEvents();
      }
    });

    // Verify durable clients on server
    server1VM.invoke("Verify durable client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

        // Iterate the CacheClientProxies
        checkNumberOfClientProxies(2);
        String firstProxyRegionName = null;
        for (CacheClientProxy proxy : notifier.getClientProxies()) {
          assertThat(proxy.isDurable()).isTrue();
          assertThat(proxy.getDurableId()).isEqualTo(durableClientId);
          assertThat(proxy.getDurableTimeout())
              .isEqualTo(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT);

          // Verify the two HA region names aren't the same
          if (firstProxyRegionName == null) {
            firstProxyRegionName = proxy.getHARegionName();
          } else {
            assertThat(proxy.getHARegionName()).isNotEqualTo(firstProxyRegionName);
          }
        }
      }
    });

    // Stop the durable client
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Verify the durable client is no longer on the server
    server1VM.invoke("Verify durable client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(0);
      }
    });

    // Stop the server
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test that the server correctly processes starting two durable clients.
   */
  @Test
  public void testSimpleTwoDurableClients() {
    // Start a server
    server1Port = server1VM
        .invoke(() -> createCacheServer(regionName, true));

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId)));

    // Send clientReady message
    sendClientReady(durableClientVM);

    // Start another durable client that is not kept alive on the server when
    // it stops normally. Use the 'publisherClientVM' as a durable client.
    VM durableClient2VM = publisherClientVM;
    final String durableClientId2 = getName() + "_client2";
    durableClient2VM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId2)));

    // Send clientReady message
    sendClientReady(durableClient2VM);

    // Verify durable clients on server
    server1VM.invoke("Verify durable client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

        // Iterate the CacheClientProxies and verify they are correct
        checkNumberOfClientProxies(2);
        boolean durableClient1Found = false, durableClient2Found = false;
        for (CacheClientProxy proxy : notifier.getClientProxies()) {
          assertThat(proxy.isDurable()).isTrue();
          if (proxy.getDurableId().equals(durableClientId)) {
            durableClient1Found = true;
          }
          if (proxy.getDurableId().equals(durableClientId2)) {
            durableClient2Found = true;
          }
          assertThat(proxy.getDurableTimeout())
              .isEqualTo(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT);
        }
        assertThat(durableClient1Found).isTrue();
        assertThat(durableClient2Found).isTrue();
      }
    });

    // Stop the durable clients
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    durableClient2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test that updates to two durable clients are processed correctly.
   */
  @Test
  public void testTwoDurableClientsStartStopUpdate() throws InterruptedException {
    // Start a server
    server1Port = server1VM
        .invoke(() -> createCacheServer(regionName, true));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    // final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        true));

    // Send clientReady message
    sendClientReady(durableClientVM);

    registerInterest(durableClientVM, regionName, true, NONE);

    // Start another durable client that is not kept alive on the server when
    // it stops normally. Use the 'server2VM' as the second durable client.
    VM durableClient2VM = server2VM;
    final String durableClientId2 = getName() + "_client2";
    durableClient2VM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId2, durableClientTimeout),
        true));

    // Send clientReady message
    sendClientReady(durableClient2VM);

    registerInterest(durableClient2VM, regionName, true, NONE);

    // Verify durable clients on server
    verifyMultupleDurableClients(durableClientId, durableClientId2);

    // Start normal publisher client
    publisherClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, false),
        regionName));

    // Publish some entries
    final int numberOfEntries = 10;
    publishEntries(publisherClientVM, 0, numberOfEntries);

    // Verify durable client 1 received the updates
    checkListenerEvents(numberOfEntries, 1, -1, durableClientVM);

    // Verify durable client 2 received the updates
    checkListenerEvents(numberOfEntries, 1, -1, durableClient2VM);

    // ARB: Wait for queue ack to arrive at server.
    sleep(1000);

    // Stop the durable clients
    durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(true));
    durableClient2VM.invoke(() -> CacheServerTestUtil.closeCache(true));

    // Verify the durable clients still exist on the server
    verifyMultupleDurableClients(durableClientId, durableClientId2);

    // Publish some more entries
    publishEntries(publisherClientVM, 10, numberOfEntries);

    sleep(1000);

    // Verify the durable clients' queues contain the entries
    server1VM.invoke("Verify durable client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

        // Iterate the CacheClientProxies and verify the queue sizes
        checkNumberOfClientProxies(2);
        for (CacheClientProxy proxy : notifier.getClientProxies()) {
          assertThat(proxy.getQueueSize()).isEqualTo(numberOfEntries);
        }
      }
    });

    // Re-start durable client 1
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId), true));

    // Send clientReady message
    sendClientReady(durableClientVM);

    // Re-start durable client 2
    durableClient2VM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId2), true));

    // Send clientReady message
    sendClientReady(durableClient2VM);

    // Verify durable client 1 received the updates held for it on the server
    checkListenerEvents(numberOfEntries, 1, -1, durableClientVM);

    // Verify durable client 2 received the updates held for it on the server
    checkListenerEvents(numberOfEntries, 1, -1, durableClientVM);

    // Stop durable client 1
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop durable client 2
    durableClient2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  private void verifyMultupleDurableClients(String durableClientId,
      String durableClientId2) {
    server1VM.invoke("Verify durable client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

        // Iterate the CacheClientProxies and verify they are correct
        checkNumberOfClientProxies(2);
        boolean durableClient1Found = false, durableClient2Found = false;
        for (CacheClientProxy proxy : notifier.getClientProxies()) {
          assertThat(proxy.isDurable()).isTrue();
          if (proxy.getDurableId().equals(durableClientId)) {
            durableClient1Found = true;
          }
          if (proxy.getDurableId().equals(durableClientId2)) {
            durableClient2Found = true;
          }
          assertThat(60).isEqualTo(proxy.getDurableTimeout());
        }
        assertThat(durableClient1Found).isTrue();
        assertThat(durableClient2Found).isTrue();
      }
    });
  }


  /**
   * Tests whether a durable client reconnects properly to two servers.
   */
  @Test
  public void testDurableClientReconnectTwoServers() throws InterruptedException {
    // Start server 1
    server1Port = server1VM.invoke(
        () -> createCacheServer(regionName, true));

    // on test flag for periodic ack
    server1VM
        .invoke(() -> setTestFlagToVerifyActForMarker(true));

    // Start server 2 using the same mcast port as server 1
    final int server2Port = server2VM
        .invoke(() -> createCacheServer(regionName, true));

    // Stop server 2
    server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    // final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        true));

    // Send clientReady message
    sendClientReady(durableClientVM);

    registerInterest(durableClientVM, regionName, true, NONE);

    // Verify durable client on server 1
    server1VM.invoke("Verify durable client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertThat(proxy).isNotNull();

        // Verify that it is durable and its properties are correct
        assertThat(proxy.isDurable()).isTrue();
        assertThat(durableClientId).isEqualTo(proxy.getDurableId());
        assertThat(durableClientTimeout).isEqualTo(proxy.getDurableTimeout());
        verifyReceivedMarkerAck();
      }
    });

    // VJR: wait for ack to go out
    pause(5000);

    // Stop the durable client
    durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(true));

    // Verify durable client on server 1
    server1VM.invoke("Verify durable client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertThat(proxy).isNotNull();
      }
    });

    // Re-start server2
    server2VM.invoke(() -> createCacheServer(regionName, true,
        server2Port));

    // Start normal publisher client
    publisherClientVM.invoke(() -> createCacheClient(getClientPool(getServerHostName(),
        server1Port, server2Port, false), regionName));

    // Publish some entries
    final int numberOfEntries = 10;
    publishEntries(publisherClientVM, 0, numberOfEntries);

    sleep(1000);

    // Verify the durable client's queue contains the entries
    server1VM.invoke("Verify durable client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        CacheClientProxy proxy = getClientProxy();
        assertThat(proxy).isNotNull();

        // Verify the queue size
        assertThat(numberOfEntries).isEqualTo(proxy.getQueueSize());
      }
    });

    // Re-start the durable client that is kept alive on the server when it stops
    // normally
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        true));

    // Send clientReady message
    sendClientReady(durableClientVM);

    // Verify durable client on server 1
    verifyDurableClientPresence(durableClientTimeout, durableClientId, server1VM, 1);

    // Verify durable client on server 2
    verifyDurableClientPresence(durableClientTimeout, durableClientId, server2VM, 1);

    // Verify the HA region names are the same on both servers
    String server1HARegionQueueName =
        server1VM.invoke(DurableClientTestBase::getHARegionQueueName);
    String server2HARegionQueueName =
        server2VM.invoke(DurableClientTestBase::getHARegionQueueName);
    assertThat(server1HARegionQueueName).isEqualTo(server2HARegionQueueName);

    // Verify the durable client received the updates
    checkListenerEvents(numberOfEntries, 1, -1, durableClientVM);

    // Stop the durable client
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // off test flag for periodic ack
    server1VM.invoke(() -> setTestFlagToVerifyActForMarker(false));

    // Stop server 1
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 2
    server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  @Test
  public void testReadyForEventsNotCalledImplicitly() {
    // Start a server
    server1Port = server1VM
        .invoke(() -> createCacheServer(regionName, true));

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    // make the client use ClientCacheFactory so it will have a default pool
    durableClientVM.invoke(() -> createClientCache(
        getClientPool(getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId)));

    // verify that readyForEvents has not yet been called on the client's default pool
    durableClientVM.invoke("check readyForEvents not called", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        for (Pool p : PoolManager.getAll().values()) {
          assertThat(((PoolImpl) p).getReadyForEventsCalled()).isFalse();
        }
      }
    });

    // Send clientReady message
    sendClientReady(durableClientVM);

    // Verify durable clients on server
    server1VM.invoke("Verify durable client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

        // Iterate the CacheClientProxies and verify they are correct
        checkNumberOfClientProxies(1);
        boolean durableClient1Found = false;
        for (CacheClientProxy proxy : notifier.getClientProxies()) {
          assertThat(proxy.isDurable()).isTrue();
          if (proxy.getDurableId().equals(durableClientId)) {
            durableClient1Found = true;
          }
          assertThat(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT)
              .isEqualTo(proxy.getDurableTimeout());
        }
        assertThat(durableClient1Found).isTrue();
      }
    });

    // Stop the durable clients
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }



  @Test
  public void testReadyForEventsNotCalledImplicitlyForRegisterInterestWithCacheXML() {
    regionName = "testReadyForEventsNotCalledImplicitlyWithCacheXML_region";
    // Start a server
    server1Port =
        server1VM.invoke(() -> createCacheServerFromXmlN(
            DurableClientTestBase.class.getResource("durablecq-server-cache.xml")));

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";

    // create client cache from xml
    durableClientVM.invoke(() -> createCacheClientFromXmlN(
        DurableClientTestBase.class.getResource("durablecq-client-cache.xml"), "client",
        durableClientId, DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, true));

    // verify that readyForEvents has not yet been called on all the client's pools
    durableClientVM.invoke("check readyForEvents not called", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        for (Pool p : PoolManager.getAll().values()) {
          assertThat(((PoolImpl) p).getReadyForEventsCalled()).isFalse();
        }
      }
    });

    // Send clientReady message
    sendClientReady(durableClientVM);

    // Durable client registers durable cq on server
    registerInterest(durableClientVM, regionName, true, InterestResultPolicy.KEYS_VALUES);

    // Verify durable client on server1
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Start normal publisher client
    publisherClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, false),
        regionName));

    // Publish some entries
    final int numberOfEntries = 10;
    publishEntries(publisherClientVM, 0, numberOfEntries);

    // Verify the durable client received the updates
    checkListenerEvents(numberOfEntries, 1, -1, durableClientVM);

    // Stop the durable client
    durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(true));

    // Verify the durable client still exists on the server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Publish some more entries
    publisherClientVM.invoke("Publish additional updates", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region<String, String> region = getCache().getRegion(regionName);
        assertThat(region).isNotNull();

        // Publish some entries
        for (int i = 0; i < numberOfEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue + "lkj");
        }
      }
    });

    publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Re-start the durable client
    durableClientVM.invoke(() -> createCacheClientFromXmlN(
        DurableClientTestBase.class.getResource("durablecq-client-cache.xml"), "client",
        durableClientId, DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, true));

    // Durable client registers durable cq on server'
    registerInterest(durableClientVM, regionName, true, InterestResultPolicy.KEYS_VALUES);

    // Send clientReady message
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Verify the durable client received the updates held for it on the server
    checkListenerEvents(numberOfEntries, 1, -1, durableClientVM);

    // Stop the durable client
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }



  /**
   * Test functionality to close the durable client and drain all events from the ha queue from the
   * server
   */
  @Test
  public void testCloseCacheProxy() {
    String greaterThan5Query = "select * from " + SEPARATOR + regionName + " p where p.ID > 5";
    String allQuery = "select * from " + SEPARATOR + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from " + SEPARATOR + regionName + " p where p.ID < 5";

    // Start a server
    server1Port = server1VM
        .invoke(() -> createCacheServer(regionName, true));

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
    disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, server1Port, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);

    // verify cq stats are correct
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkCqStatOnServer(server1VM, durableClientId, "All", 10);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 5);

    // drop client proxy
    server1VM.invoke("Close client proxy on server for client" + durableClientId,
        new CacheSerializableRunnable() {
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
    createCq(durableClientVM, "GreaterThan5",
        "select * from " + SEPARATOR + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All",
        "select * from " + SEPARATOR + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5",
        "select * from " + SEPARATOR + regionName + " p where p.ID < 5",
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
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test that starting a durable client on multiple servers is processed correctly.
   */
  @Test
  public void testSimpleDurableClientMultipleServers() {
    // Start server 1
    server1Port = server1VM.invoke(
        () -> createCacheServer(regionName, true));

    // Start server 2 using the same mcast port as server 1
    final int server2Port = server2VM
        .invoke(() -> createCacheServer(regionName, true));

    // Start a durable client connected to both servers that is kept alive when
    // it stops normally

    durableClientId = getName() + "_client";
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true),
        regionName,
        getClientDistributedSystemProperties(durableClientId, VERY_LONG_DURABLE_TIMEOUT_SECONDS),
        true));

    // Send clientReady message
    sendClientReady(durableClientVM);

    // Verify durable client on server 1
    verifyDurableClientPresence(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server1VM, 1);

    // Verify durable client on server 2
    verifyDurableClientPresence(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server2VM, 1);

    // Stop the durable client
    durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(true));

    // Verify the durable client is still on server 1
    verifyDurableClientPresence(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server1VM, 1);

    // Verify the durable client is still on server 2
    verifyDurableClientPresence(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server2VM, 1);

    // Start up the client again. This time initialize it so that it is not kept
    // alive on the servers when it stops normally.
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true),
        regionName, getClientDistributedSystemProperties(durableClientId), true));

    // Send clientReady message
    sendClientReady(durableClientVM);

    // Verify durable client on server1
    verifyDurableClientPresence(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM, 1);

    // Verify durable client on server2
    verifyDurableClientPresence(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server2VM, 1);

    // Stop the durable client
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    verifyDurableClientPresence(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT,
        durableClientId, server1VM, 0);

    verifyDurableClientPresence(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT,
        durableClientId, server2VM, 0);

    // Stop server 1
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 2
    server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  @Test
  public void testDurableClientReceivedClientSessionInitialValue() {
    // Start server 1
    server1Port = server1VM
        .invoke(() -> createCacheServer(regionName, true));

    // Start server 2
    int server2Port = server2VM
        .invoke(() -> createCacheServer(regionName, true));

    // Start normal publisher client
    publisherClientVM.invoke(() -> createCacheClient(getClientPool(getServerHostName(),
        server1Port, server2Port, false), regionName));

    // Create an entry
    publishEntries(publisherClientVM, 0, 1);

    // Start a durable client with the ControlListener
    durableClientId = getName() + "_client";
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(getServerHostName(), server1Port, server2Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId, VERY_LONG_DURABLE_TIMEOUT_SECONDS),
        true));

    durableClientVM.invoke(() -> {
      await().atMost(HEAVY_TEST_LOAD_DELAY_SUPPORT_MULTIPLIER, MINUTES)
          .pollInterval(100, MILLISECONDS)
          .untilAsserted(() -> assertThat(getCache()).isNotNull());
    });

    // Send clientReady message
    sendClientReady(durableClientVM);

    // Use ClientSession on the server to ` in entry key on behalf of durable client
    boolean server1IsPrimary = false;
    boolean registered = server1VM.invoke(() -> DurableClientSimpleDUnitTest
        .registerInterestWithClientSession(durableClientId, regionName, String.valueOf(0)));
    if (registered) {
      server1IsPrimary = true;
    } else {
      registered = server2VM.invoke(() -> DurableClientSimpleDUnitTest
          .registerInterestWithClientSession(durableClientId, regionName, String.valueOf(0)));
    }
    assertThat(registered)
        .describedAs("ClientSession interest registration failed to occur in either server.")
        .isTrue();

    // Verify durable client received create event
    checkListenerEvents(1, 1, TYPE_CREATE, durableClientVM);

    // Wait for QRM to be processed on the secondary
    waitForEventsRemovedByQueueRemovalMessage(server1IsPrimary ? server2VM : server1VM,
        durableClientId);

    // Stop durable client
    disconnectDurableClient(true);

    // restart durable client
    restartDurableClient(VERY_LONG_DURABLE_TIMEOUT_SECONDS,
        getClientPool(getServerHostName(), server1Port, server2Port, true), true);

    // Send clientReady message
    sendClientReady(durableClientVM);
    // Verify durable client does not receive create event
    checkListenerEvents(0, 1, TYPE_CREATE, durableClientVM);

    // Stop the durable client
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 1
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 2
    server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
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
      final String durableClientId) {
    secondaryServerVM.invoke(() -> DurableClientSimpleDUnitTest
        .waitForEventsRemovedByQueueRemovalMessage(durableClientId));
  }

  private static void waitForEventsRemovedByQueueRemovalMessage(String durableClientId) {
    CacheClientNotifier ccn = CacheClientNotifier.getInstance();
    CacheClientProxy ccp = ccn.getClientProxy(durableClientId);
    HARegionQueue haRegionQueue = ccp.getHARegionQueue();
    HARegionQueueStats haRegionQueueStats = haRegionQueue.getStatistics();
    await()
        .untilAsserted(
            () -> assertThat(haRegionQueueStats.getEventsRemovedByQrm()).describedAs(
                "Expected queue removal messages: " + 2 + " but actual messages: "
                    + haRegionQueueStats.getEventsRemovedByQrm())
                .isEqualTo(2));
  }
}
