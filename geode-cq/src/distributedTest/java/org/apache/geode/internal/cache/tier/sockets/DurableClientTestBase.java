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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.ControlCqListener;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.ControlListener;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createCacheClient;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createCacheServer;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.getCache;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.getClientCache;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.getPool;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.cq.internal.CqQueryImpl;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;

public class DurableClientTestBase extends JUnit4DistributedTestCase {

  private static final Duration VERY_LONG_DURABLE_CLIENT_TIMEOUT = Duration.ofMinutes(10);
  static final int VERY_LONG_DURABLE_TIMEOUT_SECONDS =
      (int) VERY_LONG_DURABLE_CLIENT_TIMEOUT.getSeconds();
  static final int HEAVY_TEST_LOAD_DELAY_SUPPORT_MULTIPLIER = 10;

  VM server1VM;
  VM server2VM;
  VM durableClientVM;
  VM publisherClientVM;
  protected String regionName;
  int server1Port;
  int server2Port;
  String durableClientId;

  @Override
  public final void postSetUp() {
    server1VM = VM.getVM(0);
    server2VM = VM.getVM(1);
    durableClientVM = VM.getVM(2);
    publisherClientVM = VM.getVM(3);
    regionName = getName() + "_region";
    // Clients see this when the servers disconnect
    IgnoredException.addIgnoredException("Could not find any server");
    System.out.println("\n\n[setup] START TEST " + getClass().getSimpleName() + "."
        + getTestMethodName() + "\n\n");
    postSetUpDurableClientTestBase();
  }

  protected void postSetUpDurableClientTestBase() {}

  @Override
  public final void preTearDown() {
    preTearDownDurableClientTestBase();

    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  protected void preTearDownDurableClientTestBase() {}


  void startupDurableClientAndServer(final int durableClientTimeout) {

    server1Port = server1VM
        .invoke(() -> createCacheServer(regionName, Boolean.TRUE));

    durableClientId = getName() + "_client";
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        Boolean.TRUE));

    durableClientVM.invoke(() -> {
      await().atMost(HEAVY_TEST_LOAD_DELAY_SUPPORT_MULTIPLIER, MINUTES)
          .pollInterval(100, MILLISECONDS)
          .until(CacheServerTestUtil::getCache, notNullValue());
    });

    // Send clientReady message
    sendClientReady(durableClientVM);
    verifyDurableClientPresent(durableClientTimeout, durableClientId, server1VM);

  }

  // This exists so child classes can override the behavior and mock out network failures
  public void restartDurableClient(int durableClientTimeout, Pool clientPool,
      Boolean addControlListener) {
    durableClientVM.invoke(() -> createCacheClient(clientPool, regionName,
        getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        addControlListener));

    durableClientVM.invoke(() -> {
      await().atMost(HEAVY_TEST_LOAD_DELAY_SUPPORT_MULTIPLIER, MINUTES)
          .pollInterval(100, MILLISECONDS)
          .until(CacheServerTestUtil::getCache, notNullValue());
    });
  }

  // This exists so child classes can override the behavior and mock out network failures
  public void restartDurableClient(int durableClientTimeout, Boolean addControlListener) {
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), server1Port, true), regionName,
        getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        addControlListener));

    durableClientVM.invoke(() -> {
      await().atMost(HEAVY_TEST_LOAD_DELAY_SUPPORT_MULTIPLIER, MINUTES)
          .pollInterval(100, MILLISECONDS)
          .until(CacheServerTestUtil::getCache, notNullValue());
    });

    // Send clientReady message
    sendClientReady(durableClientVM);
  }

  void verifyDurableClientPresent(int durableClientTimeout, String durableClientId,
      final VM serverVM) {
    verifyDurableClientPresence(durableClientTimeout, durableClientId, serverVM, 1);
  }

  void verifyDurableClientNotPresent(String durableClientId,
      final VM serverVM) {
    verifyDurableClientPresence(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        serverVM, 0);
  }

  void waitForDurableClientPresence(String durableClientId, VM serverVM) {
    serverVM.invoke(() -> {
      GeodeAwaitility.await().until(() -> {
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        return proxy != null && durableClientId.equals(proxy.getDurableId());
      });
    });
  }

  void verifyDurableClientPresence(int durableClientTimeout, String durableClientId,
      VM serverVM, final int count) {
    serverVM.invoke(() -> {
      checkNumberOfClientProxies(count);

      if (count > 0) {
        CacheClientProxy proxy = getClientProxy();
        assertThat(proxy).isNotNull();

        // Verify that it is durable and its properties are correct
        assertThat(proxy.isDurable()).isTrue();
        assertThat(durableClientId).isEqualTo(proxy.getDurableId());
        assertThat(durableClientTimeout).isEqualTo(proxy.getDurableTimeout());
      }
    });
  }

  void waitUntilHARegionQueueSizeIsZero(VM serverVM) {
    serverVM.invoke(() -> await().atMost(60, SECONDS)
        .until(() -> getClientProxy().getHARegionQueue().size() == 0));
  }

  public void closeDurableClient() {
    durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  public void disconnectDurableClient(boolean keepAlive) {
    durableClientVM.invoke("close durable client cache",
        () -> CacheServerTestUtil.closeCache(keepAlive));
    await().until(CacheServerTestUtil::getCache, nullValue());
  }

  private static String printMap(Map<String, Object[]> m) {
    Iterator<Map.Entry<String, Object[]>> itr = m.entrySet().iterator();
    StringBuilder sb = new StringBuilder();
    sb.append("size = ").append(m.size()).append(" ");
    while (itr.hasNext()) {
      sb.append("{");
      Map.Entry<String, Object[]> entry = itr.next();
      sb.append(entry.getKey());
      sb.append(", ");
      printMapValue(entry.getValue(), sb);
      sb.append("}");
    }
    return sb.toString();
  }

  private static void printMapValue(Object value, StringBuilder sb) {
    if (value.getClass().isArray()) {

      sb.append("{");
      sb.append(java.util.Arrays.toString((Object[]) value));
      sb.append("}");
    } else {
      sb.append(value);
    }
  }

  static void waitForCacheClientProxyPaused() {
    final CacheClientProxy proxy = getClientProxy();
    assertThat(proxy).isNotNull();

    await()
        .until(proxy::isPaused);

    assertThat(proxy.isPaused()).isTrue();
  }

  /*
   * Due to the way removal from ha region queue is implemented a dummy cq or interest needs to be
   * created and a dummy value used so that none of the actual cqs will be triggered and yet an
   * event will flush the queue
   */
  void flushEntries(VM server, VM client, final String regionName) {
    // This wait is to make sure that all acks have been responded to...
    // We can add a stat later on the cache client proxy stats that checks
    // ack counts
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    registerInterest(client, regionName, false, InterestResultPolicy.NONE);
    server.invoke("flush entries", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<String, String> region = getCache().getRegion(regionName);
        assertThat(region).isNotNull();
        region.put("LAST", "ENTRY");
      }
    });
  }

  // First we will have the client wait before trying to reconnect
  // Then the drain will lock and begins to drain
  // The client will then be able to continue, and get rejected
  // Then we proceed to drain and release all locks
  // The client will then reconnect
  public static class RejectClientReconnectTestHook implements CacheClientProxy.TestHook {
    final CountDownLatch reconnectLatch = new CountDownLatch(1);
    final CountDownLatch continueDrain = new CountDownLatch(1);
    volatile boolean clientWasRejected = false;

    @Override
    public void doTestHook(String spot) {
      try {
        switch (spot) {
          case "CLIENT_PRE_RECONNECT":
            if (!reconnectLatch.await(60, SECONDS)) {
              fail("reconnect latch was never released.");
            }
            break;
          case "DRAIN_IN_PROGRESS_BEFORE_DRAIN_LOCK_CHECK":
            // let client try to reconnect
            reconnectLatch.countDown();
            // we wait until the client is rejected
            if (!continueDrain.await(120, SECONDS)) {
              fail("Latch was never released.");
            }
            break;
          case "CLIENT_REJECTED_DUE_TO_CQ_BEING_DRAINED":
            clientWasRejected = true;
            continueDrain.countDown();
            break;
          default:
            break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }

    boolean wasClientRejected() {
      return clientWasRejected;
    }
  }

  /*
   * This hook will cause the close cq to throw an exception due to a client in the middle of
   * activating sequence - server will pause before draining client will begin to reconnect and then
   * wait to continue server will be unblocked, and rejected client will the be unlocked after
   * server is rejected and continue
   */
  public static class CqExceptionDueToActivatingClientTestHook
      implements CacheClientProxy.TestHook {
    final CountDownLatch unblockDrain = new CountDownLatch(1);
    final CountDownLatch unblockClient = new CountDownLatch(1);
    final CountDownLatch finish = new CountDownLatch(1);

    @Override
    public void doTestHook(String spot) {
      if (spot.equals("PRE_DRAIN_IN_PROGRESS")) {
        try {
          // Unblock any client waiting to reconnect
          unblockClient.countDown();
          // Wait until client is reconnecting
          assertThat(unblockDrain.await(120, SECONDS))
              .describedAs("client never got far enough reconnected to unlatch lock.").isTrue();
        } catch (InterruptedException e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }
      if (spot.equals("PRE_RELEASE_DRAIN_LOCK")) {
        // Client is reconnecting but still holds the drain lock
        // let the test continue to try to close a cq
        unblockDrain.countDown();
        // wait until the server has finished attempting to close the cq
        try {
          assertThat(finish.await(30, SECONDS))
              .describedAs("Test did not complete, server never finished attempting to close cq")
              .isTrue();
        } catch (InterruptedException e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }
      if (spot.equals("DRAIN_COMPLETE")) {
        finish.countDown();
      }
    }
  }

  CqQuery createCq(String cqName, String cqQuery, boolean durable)
      throws CqException, CqExistsException {
    QueryService qs = getCache().getQueryService();
    CqAttributesFactory cqf = new CqAttributesFactory();
    CqListener[] cqListeners = {new ControlCqListener()};
    cqf.initCqListeners(cqListeners);
    CqAttributes cqa = cqf.create();
    return qs.newCq(cqName, cqQuery, cqa, durable);

  }

  Pool getClientPool(String host, int serverPort, boolean establishCallbackConnection) {
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, serverPort).setSubscriptionEnabled(establishCallbackConnection)
        .setSubscriptionAckInterval(1);
    return ((PoolFactoryImpl) pf).getPoolAttributes();
  }

  Pool getClientPool(String host, int server1Port, int server2Port,
      boolean establishCallbackConnection) {
    return getClientPool(host, server1Port, server2Port, establishCallbackConnection, 1);
  }

  Properties getClientDistributedSystemProperties(String durableClientId) {
    return getClientDistributedSystemProperties(durableClientId,
        DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT);
  }

  Properties getClientDistributedSystemProperties(String durableClientId,
      int durableClientTimeout) {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    properties.setProperty(DURABLE_CLIENT_ID, durableClientId);
    properties.setProperty(DURABLE_CLIENT_TIMEOUT, String.valueOf(durableClientTimeout));
    return properties;
  }

  static CacheClientProxy getClientProxy() {
    // Get the CacheClientNotifier
    CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

    // Get the CacheClientProxy or not (if proxy set is empty)
    CacheClientProxy proxy = null;
    java.util.Iterator<CacheClientProxy> i = notifier.getClientProxies().iterator();
    if (i.hasNext()) {
      proxy = i.next();
    }
    return proxy;
  }

  private static String getAllClientProxyState() {
    // Get the CacheClientNotifier
    CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

    // Get the CacheClientProxy or not (if proxy set is empty)
    java.util.Iterator<CacheClientProxy> i = notifier.getClientProxies().iterator();
    StringBuilder sb = new StringBuilder();
    while (i.hasNext()) {
      sb.append(" [");
      sb.append(i.next().getState());
      sb.append(" ]");
    }
    return sb.toString();
  }

  static void checkNumberOfClientProxies(final int expected) {
    await().atMost(30, SECONDS).until(() -> expected == getNumberOfClientProxies());
  }

  private static int getNumberOfClientProxies() {
    return getBridgeServer().getAcceptor().getCacheClientNotifier().getClientProxies().size();
  }

  static CacheServerImpl getBridgeServer() {
    CacheServerImpl bridgeServer =
        (CacheServerImpl) getCache().getCacheServers().iterator().next();
    assertThat(bridgeServer).isNotNull();
    return bridgeServer;
  }

  Pool getClientPool(String host, int server1Port, int server2Port,
      boolean establishCallbackConnection, int redundancyLevel) {
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, server1Port).addServer(host, server2Port)
        .setSubscriptionEnabled(establishCallbackConnection)
        .setSubscriptionRedundancy(redundancyLevel).setSubscriptionAckInterval(1);
    return ((PoolFactoryImpl) pf).getPoolAttributes();
  }

  /**
   * Returns the durable client proxy's HARegionQueue region name. This method is accessed via
   * reflection on a server VM.
   *
   * @return the durable client proxy's HARegionQueue region name
   */
  static String getHARegionQueueName() {
    checkNumberOfClientProxies(1);
    CacheClientProxy proxy = getClientProxy();
    assertThat(proxy).isNotNull();
    return proxy.getHARegionName();
  }

  static void verifyReceivedMarkerAck() {
    await().atMost(3 * HEAVY_TEST_LOAD_DELAY_SUPPORT_MULTIPLIER, MINUTES)
        .pollInterval(200, MILLISECONDS)
        .until(HARegionQueue::isTestMarkerMessageReceived);
  }

  static void setTestFlagToVerifyActForMarker(Boolean flag) {
    HARegionQueue.setUsedByTest(flag);
  }

  void sendClientReady(VM vm) {
    // Send clientReady message
    vm.invoke("Send clientReady", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getClientCache().readyForEvents();
      }
    });
  }

  protected void registerInterest(VM vm, final String regionName, final boolean durable,
      final InterestResultPolicy interestResultPolicy) {
    vm.invoke("Register interest on region : " + regionName, new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {

        Region<Object, Object> region = getCache().getRegion(regionName);
        assertThat(region).isNotNull();

        // Register interest in all keys
        region.registerInterestRegex(".*", interestResultPolicy, durable);
      }
    });

    // This seems to be necessary for the queue to start up. Ideally should be replaced with
    // Awaitility if possible.
    try {
      java.lang.Thread.sleep(5000);
    } catch (java.lang.InterruptedException ex) {
      fail("interrupted");
    }
  }

  void createCq(VM vm, final String cqName, final String cqQuery, final boolean durable) {
    vm.invoke("Register cq " + cqName, new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {

        try {
          createCq(cqName, cqQuery, durable).execute();
        } catch (CqExistsException | CqException | RegionNotFoundException e) {
          throw new CacheException(e) {};
        }

      }
    });
  }

  // Publishes strings
  void publishEntries(VM publisherClientVM, int startingValue, final int count) {
    publisherClientVM.invoke("Publish entries", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<String, String> region = getCache().getRegion(
            regionName);
        assertThat(region).isNotNull();
        // Publish some entries
        for (int i = startingValue; i < startingValue + count; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }

        assertThat(region.get(String.valueOf(startingValue))).isNotNull();
      }
    });
  }

  // Publishes portfolios
  void publishEntries(VM publisherClientVM, final String regionName, final int numEntries) {
    publisherClientVM.invoke("publish " + numEntries + " entries", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region<Object, Object> region = getCache().getRegion(regionName);
        assertThat(region).isNotNull();

        // Publish some entries
        for (int i = 0; i < numEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, new Portfolio(i));
        }

        assertThat(region.get(String.valueOf(0))).isNotNull();
      }
    });
  }

  public void verifyListenerUpdatesDisconnected(int numberOfEntries) {
    // ARB: do nothing.
  }

  void checkCqStatOnServer(VM server, final String durableClientId, final String cqName,
      final int expectedNumber) {
    server.invoke(
        "Check ha queued cq stats for durable client " + durableClientId + " cq: " + cqName,
        new CacheSerializableRunnable() {
          @Override
          public void run2() throws CacheException {

            final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();
            final CacheClientProxy clientProxy = ccnInstance.getClientProxy(durableClientId);
            ClientProxyMembershipID proxyId = clientProxy.getProxyID();
            CqService cqService = ((InternalCache) getCache()).getCqService();
            cqService.start();
            final CqQueryImpl cqQuery =
                (CqQueryImpl) cqService.getClientCqFromServer(proxyId, cqName);

            // Wait until we get the expected number of events or until 10 seconds are up
            await()
                .until(() -> cqQuery.getVsdStats().getNumHAQueuedEvents() == expectedNumber);

            assertThat(expectedNumber).isEqualTo(cqQuery.getVsdStats().getNumHAQueuedEvents());
          }
        });
  }

  /*
   * Remaining is the number of events that could still be in the queue due to timing issues with
   * acks and receiving them after remove from ha queue region has been called.
   */
  void checkHAQueueSize(VM server, final String durableClientId, final int expectedNumber,
      final int remaining) {
    server.invoke("Check ha queued size for durable client " + durableClientId,
        new CacheSerializableRunnable() {
          @Override
          public void run2() throws CacheException {

            final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();
            final CacheClientProxy clientProxy = ccnInstance.getClientProxy(durableClientId);

            // Wait until we get the expected number of events or until 10 seconds are up
            await().untilAsserted(
                () -> assertThat(clientProxy.getQueueSizeStat()).isIn(expectedNumber, remaining));
          }
        });
  }

  void checkNumDurableCqs(VM server, final String durableClientId,
      final int expectedNumber) {
    server.invoke("check number of durable cqs on server for durable client: " + durableClientId,
        new CacheSerializableRunnable() {
          @Override
          public void run2() throws CacheException {
            try {
              final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();
              final CacheClientProxy clientProxy = ccnInstance.getClientProxy(durableClientId);
              ClientProxyMembershipID proxyId = clientProxy.getProxyID();
              CqService cqService = ((InternalCache) getCache()).getCqService();
              cqService.start();
              java.util.List<String> cqNames = cqService.getAllDurableClientCqs(proxyId);
              assertThat(expectedNumber).isEqualTo(cqNames.size());
            } catch (Exception e) {
              throw new CacheException(e) {};
            }
          }
        });
  }

  /*
   * @param numEventsToWaitFor most times will be the same as numEvents, but there are times where
   * we want to wait for an event we know is not coming just to be sure an event actually isn't
   * received
   *
   */
  void checkCqListenerEvents(VM vm, final String cqName, final int numEvents,
      final int secondsToWait) {
    vm.invoke(() -> checkCqListenerEvents(cqName, numEvents, secondsToWait));
  }

  void checkCqListenerEvents(final String cqName, final int numEvents, final int secondsToWait) {
    QueryService qs = getCache().getQueryService();
    CqQuery cq = qs.getCq(cqName);
    // Get the listener and wait for the appropriate number of events
    ControlCqListener listener =
        (ControlCqListener) cq.getCqAttributes().getCqListener();
    listener.waitWhileNotEnoughEvents(SECONDS.toMillis(secondsToWait), numEvents);
    assertThat(numEvents).isEqualTo(listener.events.size());
  }

  void checkListenerEvents(int numberOfEntries, final int sleepMinutes, final int eventType,
      final VM vm) {
    vm.invoke(() -> {
      // Get the region
      Region<Object, Object> region = getCache().getRegion(regionName);
      assertThat(region).isNotNull();

      // Get the listener and wait for the appropriate number of events
      ControlListener controlListener =
          (ControlListener) region.getAttributes().getCacheListeners()[0];
      controlListener.waitWhileNotEnoughEvents(MINUTES.toMillis(sleepMinutes), numberOfEntries,
          controlListener.getEvents(eventType));
    });
  }

  void startDurableClient(VM vm, String durableClientId, int serverPort1,
      String regionName, int durableTimeoutInSeconds) {
    vm.invoke(() -> createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort1, true),
        regionName, getClientDistributedSystemProperties(durableClientId, durableTimeoutInSeconds),
        Boolean.TRUE));
  }

  void startDurableClient(VM vm, String durableClientId, int serverPort1,
      String regionName) {
    vm.invoke(() -> {
      createCacheClient(
          getClientPool(NetworkUtils.getServerHostName(), serverPort1, true),
          regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE);
      assertThat(getClientCache()).isNotNull();
    });
  }

  void startDurableClient(VM vm, String durableClientId, int serverPort1, int serverPort2,
      String regionName) {
    vm.invoke(() -> createCacheClient(
        getClientPool(NetworkUtils.getServerHostName(), serverPort1, serverPort2, true),
        regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE));
  }

  void startClient(VM vm, int serverPort1, String regionName) {
    vm.invoke(() -> {
      createCacheClient(
          getClientPool(NetworkUtils.getServerHostName(), serverPort1, false),
          regionName);
      assertThat(getClientCache()).isNotNull();
    });
  }

  void checkPrimaryUpdater(VM vm) {
    vm.invoke("Verify durable client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        await().until(() -> getPool().isPrimaryUpdaterAlive());
        assertThat(getPool().isPrimaryUpdaterAlive()).isTrue();
      }
    });
  }

  protected void closeCache(VM vm) {
    vm.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }
}
