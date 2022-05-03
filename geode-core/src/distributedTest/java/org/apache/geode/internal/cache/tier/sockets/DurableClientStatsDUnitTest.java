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

import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createCacheClient;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createCacheServer;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.disableShufflingOfEndpoints;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.getCache;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.getClientCache;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.resetDisableShufflingOfEndpointsFlag;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * The DUnitTest checks whether the following Three counts are incremented correctly or not: 1)
 * DurableReconnectionCount -> Incremented Each time a Proxy present in server for a DurableClient
 * 2) QueueDroppedCount -> Incremented Each time a queue for a durable client is dropped after
 * durable Timeout 3) EventsEnqueuedWhileClientAwayCount -> Incremented Each time an entry is made
 * when client is away.
 *
 * In the given test DurableClient comes up and goes down discreetly with different
 * DurableClientTimeouts so as to increment the counts
 */
@Category({ClientSubscriptionTest.class})
public class DurableClientStatsDUnitTest extends JUnit4DistributedTestCase {

  private VM server1VM;

  private VM durableClientVM;

  private String regionName;

  private int PORT1;

  @Override
  public final void postSetUp() {
    server1VM = VM.getVM(0);
    durableClientVM = VM.getVM(1);
    regionName = DurableClientStatsDUnitTest.class.getName() + "_region";
    disableShufflingOfEndpoints();
  }

  @Override
  public final void preTearDown() {
    // Stop server 1
    server1VM.invoke(() -> CacheServerTestUtil.closeCache());
    resetDisableShufflingOfEndpointsFlag();
  }

  @Test
  public void testNonDurableClientStatistics() {
    // Step 1: Starting the servers
    PORT1 = server1VM
        .invoke(() -> createCacheServer(regionName, true));
    server1VM.invoke(DurableClientStatsDUnitTest::checkStatistics);
    // Step 2: Bring Up the Client
    // Start a durable client that is not kept alive on the server when it
    // stops normally

    startAndCloseNonDurableClientCache();
    startAndCloseNonDurableClientCache(); //////// -> Reconnection1
    Wait.pause(1400); //////// -> Queue Dropped1
    startAndCloseNonDurableClientCache();
    Wait.pause(1400); //////// -> Queue Dropped2

    startRegisterAndCloseNonDurableClientCache();
    Wait.pause(500);

    server1VM.invoke(() -> DurableClientStatsDUnitTest.putValue("Value1")); //////// ->
    //////// Enqueue
    //////// Message1

    Wait.pause(500);
    startAndCloseNonDurableClientCache(); //////// -> Reconnection2
    Wait.pause(1400); //////// -> Queue Dropped3
    startAndCloseNonDurableClientCache();
    Wait.pause(1400); //////// -> Queue Dropped4
    startRegisterAndCloseNonDurableClientCache();
    Wait.pause(500);

    server1VM.invoke(() -> DurableClientStatsDUnitTest.putValue("NewValue1")); //////// ->
    //////// Enqueue
    //////// Message2

    startAndCloseNonDurableClientCache(); //////// -> Reconnection3

    server1VM.invoke(() -> DurableClientStatsDUnitTest
        .checkStatisticsWithExpectedValues(0, 0, 0));
  }

  @Test
  public void testDurableClientStatistics() {

    assertThat(server1VM).isNotNull();
    // Step 1: Starting the servers
    PORT1 = server1VM
        .invoke(() -> createCacheServer(regionName, true));
    server1VM.invoke(DurableClientStatsDUnitTest::checkStatistics);
    // Step 2: Bring Up the Client
    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final int durableClientTimeout = 600; // keep the client alive for 600
    // seconds

    startAndCloseDurableClientCache(durableClientTimeout);
    startAndCloseDurableClientCache(1); //////// -> Reconnection1
    Wait.pause(1400); //////// -> Queue Dropped1
    startAndCloseDurableClientCache(1);
    Wait.pause(1400); //////// -> Queue Dropped2

    startRegisterAndCloseDurableClientCache(durableClientTimeout);
    Wait.pause(500);

    server1VM.invoke(() -> DurableClientStatsDUnitTest.putValue("Value1")); //////// ->
    //////// Enqueue
    //////// Message1

    Wait.pause(500);
    startAndCloseDurableClientCache(1); //////// -> Reconnection2
    Wait.pause(1400); //////// -> Queue Dropped3
    startAndCloseDurableClientCache(1);
    Wait.pause(1400); //////// -> Queue Dropped4
    startRegisterAndCloseDurableClientCache(durableClientTimeout);
    Wait.pause(500);

    server1VM.invoke(() -> DurableClientStatsDUnitTest.putValue("NewValue1")); //////// ->
    //////// Enqueue
    //////// Message2

    startAndCloseDurableClientCache(durableClientTimeout); //////// -> Reconnection3

    server1VM.invoke(() -> DurableClientStatsDUnitTest
        .checkStatisticsWithExpectedValues(3, 4, 2));
  }

  public void startRegisterAndCloseDurableClientCache(int durableClientTimeout) {
    final String durableClientId = getName() + "_client";

    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), PORT1), regionName,
        getDurableClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        true));

    durableClientVM.invoke(() -> DurableClientStatsDUnitTest.registerKey(true));

    // Send clientReady message
    durableClientVM.invoke("Send clientReady", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getClientCache().readyForEvents();
      }
    });

    durableClientVM.invoke(DurableClientStatsDUnitTest::closeCache);
  }

  public void startRegisterAndCloseNonDurableClientCache() {

    durableClientVM
        .invoke(() -> createCacheClient(
            getClientPool(NetworkUtils.getServerHostName(), PORT1),
            regionName,
            getNonDurableClientDistributedSystemProperties(), true));

    durableClientVM.invoke(() -> DurableClientStatsDUnitTest.registerKey(false));

    durableClientVM.invoke(DurableClientStatsDUnitTest::closeCache);
  }

  public void startAndCloseDurableClientCache(int durableClientTimeout) {

    final String durableClientId = getName() + "_client";

    durableClientVM
        .invoke(() -> createCacheClient(
            getClientPool(NetworkUtils.getServerHostName(), PORT1),
            regionName,
            getDurableClientDistributedSystemProperties(durableClientId, durableClientTimeout),
            true));

    // Send clientReady message
    durableClientVM.invoke("Send clientReady", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getClientCache().readyForEvents();
      }
    });

    durableClientVM.invoke(DurableClientStatsDUnitTest::closeCache);

  }

  public void startAndCloseNonDurableClientCache() {

    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), PORT1), regionName,
        getNonDurableClientDistributedSystemProperties(), true));

    durableClientVM.invoke(DurableClientStatsDUnitTest::closeCache);

  }

  public static void checkStatistics() {
    try {
      Cache cache = getCache();
      org.apache.geode.LogWriter logger = cache.getLogger();
      CacheServerImpl currentServer =
          (CacheServerImpl) (new ArrayList<>(cache.getCacheServers()).get(0));
      Acceptor ai = currentServer.getAcceptor();
      CacheClientNotifier notifier = ai.getCacheClientNotifier();
      CacheClientNotifierStats stats = notifier.getStats();
      logger.info("Stats:" + "\nDurableReconnectionCount:" + stats.get_durableReconnectionCount()
          + "\nQueueDroppedCount" + stats.get_queueDroppedCount()
          + "\nEventsEnqueuedWhileClientAwayCount" + stats.get_eventEnqueuedWhileClientAwayCount());
    } catch (Exception e) {
      fail("Exception thrown while executing checkStatistics()", e);
    }
  }

  public static void checkStatisticsWithExpectedValues(int reconnectionCount, int queueDropCount,
      int enqueueCount) {
    try {
      Cache cache = getCache();
      org.apache.geode.LogWriter logger = cache.getLogger();
      CacheServerImpl currentServer =
          (CacheServerImpl) (new ArrayList<>(cache.getCacheServers()).get(0));
      Acceptor ai = currentServer.getAcceptor();
      CacheClientNotifier notifier = ai.getCacheClientNotifier();
      CacheClientNotifierStats stats = notifier.getStats();
      logger.info("Stats:" + "\nDurableReconnectionCount:" + stats.get_durableReconnectionCount()
          + "\nQueueDroppedCount" + stats.get_queueDroppedCount()
          + "\nEventsEnqueuedWhileClientAwayCount" + stats.get_eventEnqueuedWhileClientAwayCount());
      await().untilAsserted(
          () -> assertThat(stats.get_durableReconnectionCount()).isEqualTo(reconnectionCount));
      await()
          .untilAsserted(() -> assertThat(stats.get_queueDroppedCount()).isEqualTo(queueDropCount));
      await().untilAsserted(
          () -> assertThat(stats.get_eventEnqueuedWhileClientAwayCount()).isEqualTo(enqueueCount));
    } catch (Exception e) {
      fail("Exception thrown while executing checkStatisticsWithExpectedValues()", e);
    }
  }

  public static void closeCache() {
    ClientCache clientCache = getClientCache();
    if (clientCache != null && !clientCache.isClosed()) {
      // might fail in DataSerializerRecoveryListener.RecoveryTask in shutdown
      clientCache.getLogger().info("<ExpectedException action=add>"
          + RejectedExecutionException.class.getName() + "</ExpectedException>");
      clientCache.close(true);
    }
  }

  private static void registerKey(boolean isDurable) {
    // Get the region
    Region<String, String> region = getClientCache()
        .getRegion(DurableClientStatsDUnitTest.class.getName() + "_region");

    assertThat(region).isNotNull();
    region.registerInterest("Key1", InterestResultPolicy.NONE, isDurable);
  }

  private static void putValue(String value) {
    Region<String, String> r = getClientCache()
        .getRegion(DurableClientStatsDUnitTest.class.getName() + "_region");
    assertThat(r).isNotNull();

    if (r.getEntry("Key1") != null) {
      r.put("Key1", value);
    } else {
      r.create("Key1", value);
    }
    assertThat(r).contains(entry("Key1", value));
  }

  private Pool getClientPool(String host, int server1Port) {
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, server1Port).setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(0);
    return ((PoolFactoryImpl) pf).getPoolAttributes();
  }

  private Properties getDurableClientDistributedSystemProperties(String durableClientId,
      int durableClientTimeout) {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    properties.setProperty(DURABLE_CLIENT_ID, durableClientId);
    properties.setProperty(DURABLE_CLIENT_TIMEOUT, String.valueOf(durableClientTimeout));
    return properties;
  }

  private Properties getNonDurableClientDistributedSystemProperties() {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    return properties;
  }
}
