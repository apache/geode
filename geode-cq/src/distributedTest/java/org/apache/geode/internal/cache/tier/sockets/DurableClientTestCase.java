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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createCacheClient;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createCacheServer;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createCacheServerReturnPorts;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.getCache;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.getClientCache;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.unsetJavaSystemProperties;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;
import org.apache.geode.util.internal.GeodeGlossary;


/**
 * Class <code>DurableClientTestCase</code> tests durable client functionality.
 *
 * @since GemFire 5.2
 */
@Category({ClientSubscriptionTest.class})
public class DurableClientTestCase extends DurableClientTestBase {


  /**
   * Test that starting a durable client is correctly processed by the server.
   */
  @Test
  public void testSimpleDurableClient() {
    startupDurableClientAndServer(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT);

    // Stop the server
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    closeDurableClient();

  }

  /**
   * Test that starting a durable client is correctly processed by the server. In this test we will
   * set gemfire.SPECIAL_DURABLE property to true and will see durableID appended by poolname or
   * not
   */
  @Test
  public void testSpecialDurableProperty() throws InterruptedException {
    final Properties jp = new Properties();
    jp.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "SPECIAL_DURABLE", "true");

    try {

      server1Port = server1VM
          .invoke(() -> createCacheServer(regionName, Boolean.TRUE));

      durableClientId = getName() + "_client";
      final String dId = durableClientId + "_gem_" + "CacheServerTestUtil";

      durableClientVM.invoke(() -> createCacheClient(
          getClientPool(NetworkUtils.getServerHostName(), server1Port, Boolean.TRUE),
          regionName, getClientDistributedSystemProperties(durableClientId,
              DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT),
          Boolean.TRUE, jp));

      durableClientVM.invoke(() -> {
        await().atMost(1 * HEAVY_TEST_LOAD_DELAY_SUPPORT_MULTIPLIER, MINUTES)
            .pollInterval(100, MILLISECONDS)
            .until(CacheServerTestUtil::getCache, notNullValue());
      });

      // Send clientReady message
      sendClientReady(durableClientVM);

      // Verify durable client on server
      server1VM.invoke(() -> {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertThat(proxy).isNotNull();

        // Verify that it is durable and its properties are correct
        assertThat(proxy.isDurable()).isTrue();
        assertThat(durableClientId).isNotEqualTo(proxy.getDurableId());

        /*
         * new durable id will be like this durableClientId _gem_ //separator client pool name
         */

        assertThat(dId).isEqualTo(proxy.getDurableId());
        assertThat(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT)
            .isEqualTo(proxy.getDurableTimeout());
      });

      // Stop the durable client
      disconnectDurableClient(false);

      // Verify the durable client is present on the server for closeCache=false case.
      verifyDurableClientNotPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT,
          durableClientId, server1VM);

      // Stop the server
      server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

      closeDurableClient();
    } finally {

      durableClientVM.invoke(() -> unsetJavaSystemProperties(jp));
    }

  }

  /**
   * Test that starting, stopping then restarting a durable client is correctly processed by the
   * server.
   */
  @Test
  public void testStartStopStartDurableClient() {

    startupDurableClientAndServer(VERY_LONG_DURABLE_TIMEOUT_SECONDS);

    // Stop the durable client
    disconnectDurableClient(true);

    verifyDurableClientPresent(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server1VM);

    // Re-start the durable client
    restartDurableClient(VERY_LONG_DURABLE_TIMEOUT_SECONDS, Boolean.TRUE);

    // Verify durable client on server
    verifyDurableClientPresent(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server1VM);

    // Stop the durable client
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test that starting, stopping then restarting a durable client is correctly processed by the
   * server. This is a test of bug 39630
   */
  @Test
  public void test39630() {
    startupDurableClientAndServer(VERY_LONG_DURABLE_TIMEOUT_SECONDS);

    // Stop the durable client
    disconnectDurableClient(true);

    // Verify the durable client still exists on the server, and the socket is closed
    server1VM.invoke(() -> {
      // Find the proxy
      CacheClientProxy proxy = getClientProxy();
      assertThat(proxy).isNotNull();
      assertThat(proxy._socket).isNotNull();

      await().untilAsserted(() -> assertThat(proxy._socket.isClosed()).isTrue());
    });

    // Re-start the durable client (this is necessary so the
    // netDown test will set the appropriate system properties.
    restartDurableClient(VERY_LONG_DURABLE_TIMEOUT_SECONDS, Boolean.TRUE);

    // Stop the durable client
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test that disconnecting a durable client for longer than the timeout period is correctly
   * processed by the server.
   */
  @Test
  public void testStartStopTimeoutDurableClient() {

    final int durableClientTimeout = 5;
    startupDurableClientAndServer(durableClientTimeout);

    // Stop the durable client
    disconnectDurableClient(true);

    // Verify it no longer exists on the server
    server1VM.invoke("Verify durable client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(0);
        CacheClientProxy proxy = getClientProxy();
        assertThat(proxy).isNull();
      }
    });

    restartDurableClient(durableClientTimeout, Boolean.TRUE);

    // Stop the durable client
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test that a durable client correctly receives updates after it reconnects.
   */
  @Test
  public void testDurableClientPrimaryUpdate() {
    startupDurableClientAndServer(VERY_LONG_DURABLE_TIMEOUT_SECONDS);

    registerInterest(durableClientVM, regionName, true, InterestResultPolicy.NONE);

    // Start normal publisher client
    publisherClientVM.invoke(() -> createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), server1Port,
            false),
        regionName));

    // Publish some entries
    publishEntries(publisherClientVM, 0, 1);

    // Wait until queue count is 0 on server1VM
    waitUntilQueueContainsRequiredNumberOfEvents(server1VM, 0);

    // Verify the durable client received the updates
    checkListenerEvents(1, 1, -1, durableClientVM);

    // Stop the durable client
    disconnectDurableClient(true);

    // Make sure the proxy is actually paused, not dispatching
    server1VM.invoke(DurableClientTestBase::waitForCacheClientProxyPaused);

    // Publish some more entries
    publishEntries(publisherClientVM, 1, 1);

    // Verify the durable client's queue contains the entries
    waitUntilQueueContainsRequiredNumberOfEvents(server1VM, 1);

    // Verify that disconnected client does not receive any events.
    verifyListenerUpdatesDisconnected(1);

    // Re-start the durable client
    restartDurableClient(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, Boolean.TRUE);

    // Verify durable client on server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Verify the durable client received the updates held for it on the server
    checkListenerEvents(1, 1, -1, durableClientVM);

    // Stop the publisher client
    publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the durable client VM
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }


  /**
   * Test that a durable client correctly receives updates after it reconnects.
   */
  @Test
  public void testStartStopStartDurableClientUpdate() {
    startupDurableClientAndServer(VERY_LONG_DURABLE_TIMEOUT_SECONDS);
    // Have the durable client register interest in all keys
    registerInterest(durableClientVM, regionName, true, InterestResultPolicy.NONE);

    // Start normal publisher client
    publisherClientVM.invoke(() -> createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), server1Port,
            false),
        regionName));

    // Publish some entries
    publishEntries(publisherClientVM, 0, 1);

    // Verify the durable client received the updates
    checkListenerEvents(1, 1, -1, durableClientVM);

    server1VM.invoke("wait for client acknowledgement", () -> {
      CacheClientProxy proxy = getClientProxy();
      await().untilAsserted(
          () -> assertThat(proxy._messageDispatcher._messageQueue.stats.getEventsRemoved())
              .isGreaterThan(0));
    });

    // Stop the durable client
    disconnectDurableClient(true);

    // Verify the durable client still exists on the server
    server1VM.invoke(DurableClientTestBase::waitForCacheClientProxyPaused);

    // Publish some entries
    publishEntries(publisherClientVM, 1, 1);

    // Verify the durable client's queue contains the entries
    server1VM.invoke("Verify durable client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        CacheClientProxy proxy = getClientProxy();
        assertThat(proxy).isNotNull();
        // Verify the queue size
        assertThat(proxy.getQueueSize()).isEqualTo(1);
      }
    });

    // Verify that disconnected client does not receive any events.
    verifyListenerUpdatesDisconnected(1);

    // Re-start the durable client
    restartDurableClient(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, Boolean.TRUE);

    // Verify durable client on server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Verify the durable client received the updates held for it on the server
    checkListenerEvents(1, 1, -1, durableClientVM);

    // Stop the publisher client
    publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the durable client VM
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test whether a durable client reconnects properly to a server that is stopped and restarted.
   */
  @Test
  public void testDurableClientConnectServerStopStart() {
    // Start a server
    // Start server 1
    Integer[] ports = server1VM.invoke(
        () -> createCacheServerReturnPorts(regionName, Boolean.TRUE));
    final int serverPort = ports[0];

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort, true),
        regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE));

    registerInterest(durableClientVM, regionName, true, InterestResultPolicy.NONE);

    // Send clientReady message
    durableClientVM.invoke("Send clientReady", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getClientCache().readyForEvents();
      }
    });

    // Verify durable client on server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Stop the server
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Re-start the server
    server1VM.invoke(() -> createCacheServer(regionName, Boolean.TRUE, serverPort));

    // Verify durable client on server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Start a publisher
    publisherClientVM.invoke(() -> createCacheClient(getClientPool(NetworkUtils.getServerHostName(),
        serverPort, false), regionName));

    // Publish some entries
    publishEntries(publisherClientVM, 0, 10);

    // Verify the durable client received the updates
    checkListenerEvents(10, 1, -1, durableClientVM);

    // Stop the durable client
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }


  @Test
  public void testDurableNonHAFailover() {
    durableFailover(0);
  }

  @Test
  public void testDurableNonHAFailoverAfterReconnect() {
    durableFailoverAfterReconnect(0);
  }

  @Test
  public void testDurableHAFailover() {
    // Clients see this when the servers disconnect
    IgnoredException.addIgnoredException("Could not find any server");
    durableFailover(1);
  }

  @Test
  public void testDurableHAFailoverAfterReconnect() {
    // Clients see this when the servers disconnect
    IgnoredException.addIgnoredException("Could not find any server");
    durableFailoverAfterReconnect(1);
  }

  /**
   * Test a durable client with 2 servers where the client fails over from one to another server
   * with a publisher/feeder performing operations and the client verifying updates received.
   * Redundancy level is set to 1 for this test case.
   */
  private void durableFailover(int redundancyLevel) {
    // Start server 1
    server1Port = server1VM.invoke(() -> createCacheServer(regionName, Boolean.TRUE));

    // Start server 2 using the same mcast port as server 1
    final int server2Port = server2VM.invoke(() -> createCacheServer(regionName, Boolean.TRUE));

    // Stop server 2
    server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Start a durable client
    durableClientId = getName() + "_client";

    Pool clientPool;
    if (redundancyLevel == 1) {
      clientPool = getClientPool(NetworkUtils.getServerHostName(), server1Port,
          server2Port, true);
    } else {
      clientPool = getClientPool(NetworkUtils.getServerHostName(), server1Port,
          server2Port, true, 0);
    }

    durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);
    durableClientVM.invoke(() -> createCacheClient(clientPool, regionName,
        getClientDistributedSystemProperties(durableClientId, VERY_LONG_DURABLE_TIMEOUT_SECONDS),
        Boolean.TRUE));

    registerInterest(durableClientVM, regionName, true, InterestResultPolicy.NONE);
    // Send clientReady message
    durableClientVM.invoke("Send clientReady", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getClientCache().readyForEvents();
      }
    });


    // Re-start server2
    server2VM.invoke(() -> createCacheServer(regionName, Boolean.TRUE,
        server2Port));

    // Start normal publisher client
    publisherClientVM.invoke(() -> createCacheClient(getClientPool(
        NetworkUtils.getServerHostName(), server1Port, server2Port,
        false), regionName));

    // Publish some entries
    publishEntries(publisherClientVM, 0, 1);

    // Verify the durable client received the updates
    checkListenerEvents(1, 1, -1, durableClientVM);

    // Stop the durable client, which discards the known entry
    disconnectDurableClient(true);

    // Publish updates during client downtime
    publishEntries(publisherClientVM, 1, 1);

    // Re-start the durable client that is kept alive on the server
    restartDurableClient(VERY_LONG_DURABLE_TIMEOUT_SECONDS, clientPool, Boolean.TRUE);

    registerInterest(durableClientVM, regionName, true, InterestResultPolicy.NONE);

    // Send clientReady message
    sendClientReady(durableClientVM);
    publishEntries(publisherClientVM, 2, 1);

    // Verify the durable client received the updates before failover
    checkListenerEvents(2, 1, -1, durableClientVM);

    durableClientVM.invoke("Get", () -> {
      await().untilAsserted(() -> {
        Region<Object, Object> region = getCache().getRegion(regionName);
        assertThat(region).isNotNull();

        assertThat(region.getEntry("0")).isNull();
        assertThat(region.getEntry("2")).isNotNull();
      });
    });

    // Stop server 1
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Verify durable client failed over if redundancyLevel=0
    if (redundancyLevel == 0) {
      server2VM.invoke(this::verifyClientHasConnected);
    }

    publishEntries(publisherClientVM, 3, 1);

    // Verify the durable client received the updates after failover
    checkListenerEvents(3, 1, -1, durableClientVM);

    // Stop the durable client
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 2
    server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  private void waitUntilQueueContainsRequiredNumberOfEvents(final VM vm,
      final int requiredEntryCount) {
    vm.invoke("Verify durable client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {

        await().until(() -> {
          CacheClientProxy proxy = getClientProxy();
          if (proxy == null) {
            return false;
          }
          // Verify the queue size
          int sz = proxy.getQueueSize();
          return requiredEntryCount == sz;
        });
      }
    });
  }

  private void durableFailoverAfterReconnect(int redundancyLevel) {
    // Start server 1
    server1Port = server1VM.invoke(() -> createCacheServer(regionName, true));

    int server2Port = server2VM.invoke(() -> createCacheServer(regionName, true));

    // Start a durable client
    durableClientId = getName() + "_client";

    Pool clientPool;
    if (redundancyLevel == 1) {
      clientPool = getClientPool(NetworkUtils.getServerHostName(), server1Port,
          server2Port, true);
    } else {
      clientPool = getClientPool(NetworkUtils.getServerHostName(), server1Port,
          server2Port, true, 0);
    }

    durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);
    durableClientVM.invoke(() -> createCacheClient(clientPool, regionName,
        getClientDistributedSystemProperties(durableClientId, VERY_LONG_DURABLE_TIMEOUT_SECONDS),
        Boolean.TRUE));
    registerInterest(durableClientVM, regionName, true, InterestResultPolicy.NONE);

    // Send clientReady message
    durableClientVM.invoke("Send clientReady", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getCache().readyForEvents();
      }
    });

    // Start normal publisher client
    publisherClientVM.invoke(() -> createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), server1Port,
            server2Port, false),
        regionName));

    // Publish some entries
    publishEntries(publisherClientVM, 0, 1);

    // Verify the durable client received the updates
    checkListenerEvents(1, 1, -1, durableClientVM);

    verifyDurableClientPresent(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server1VM);
    verifyDurableClientPresent(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server2VM);

    waitUntilHARegionQueueSizeIsZero(server1VM);
    waitUntilHARegionQueueSizeIsZero(server2VM);

    // Stop the durable client
    disconnectDurableClient(true);

    // Stop server 1 - publisher will put 10 entries during shutdown/primary identification
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Publish updates during client downtime
    publishEntries(publisherClientVM, 1, 1);

    // Re-start the durable client that is kept alive on the server
    restartDurableClient(VERY_LONG_DURABLE_TIMEOUT_SECONDS, clientPool, Boolean.TRUE);

    registerInterest(durableClientVM, regionName, true, InterestResultPolicy.NONE);

    // Send clientReady message
    sendClientReady(durableClientVM);

    publishEntries(publisherClientVM, 2, 2);

    // Verify the durable client received the updates before failover
    if (redundancyLevel == 1) {
      checkListenerEvents(3, 1, -1, durableClientVM);
    } else {
      checkListenerEvents(1, 1, -1, durableClientVM);
    }

    durableClientVM.invoke("Get", () -> {
      await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
        Region<Object, Object> region = getCache().getRegion(regionName);
        assertThat(region).isNotNull();
        // Register interest in all keys
        assertThat(region.getEntry("0")).isNull();
      });
    });

    publishEntries(publisherClientVM, 4, 1);

    // Verify the durable client received the updates after failover
    if (redundancyLevel == 1) {
      checkListenerEvents(4, 1, -1, durableClientVM);
    } else {
      checkListenerEvents(2, 1, -1, durableClientVM);
    }

    // Stop the durable client
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop server 2
    server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  private void verifyClientHasConnected() {
    CacheServer cacheServer = getCache().getCacheServers().get(0);
    CacheClientNotifier ccn =
        ((InternalCacheServer) cacheServer).getAcceptor().getCacheClientNotifier();
    await().until(() -> ccn.getClientProxies().size() == 1);
  }
}
