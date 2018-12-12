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
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.AbstractRegionMap;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Tests client server FORCE_INVALIDATE
 */
@Category({ClientServerTest.class})
public class ClientServerForceInvalidateDUnitTest extends JUnit4CacheTestCase {

  private static final Logger logger = LogService.getLogger();

  private static Region<String, String> region1;

  private static final String REGION_NAME1 = "ClientServerForceInvalidateDUnitTest_region1";

  private static Host host;

  private static VM server1;
  private static VM server2;

  @Override
  public final void postSetUp() throws Exception {
    host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
  }

  private int initServerCache(VM vm, boolean concurrencyChecksEnabled, boolean partitioned) {
    return vm.invoke(() -> createServerCache(concurrencyChecksEnabled, partitioned, 0));
  }

  @Test
  public void testForceInvalidateOnCachingProxyWithConcurrencyChecks() throws Exception {
    dotestForceInvalidate(true, true, false, true);
  }

  @Test
  public void testForceInvalidateOnCachingProxyWithConcurrencyChecksOnlyOnServer()
      throws Exception {
    dotestForceInvalidate(true, false, false, true);
  }

  @Test
  public void testForceInvalidateOnCachingProxyWithConcurrencyChecksOnlyOnClient()
      throws Exception {
    dotestForceInvalidate(false, true, false, true);
  }

  @Test
  public void testForceInvalidateOnProxyWithConcurrencyChecks() throws Exception {
    dotestForceInvalidate(true, true, true, true);
  }

  @Test
  public void testForceInvalidateOnProxyWithConcurrencyChecksOnlyOnServer() throws Exception {
    dotestForceInvalidate(true, false, true, true);
  }

  @Test
  public void testForceInvalidateOnProxyWithConcurrencyChecksOnlyOnClient() throws Exception {
    dotestForceInvalidate(false, true, true, true);
  }

  @Test
  public void testForceInvalidateOnCachingProxyWithConcurrencyChecksServerReplicated()
      throws Exception {
    dotestForceInvalidate(true, true, false, false);
  }

  @Test
  public void testForceInvalidateOnProxyWithConcurrencyChecksServerReplicated() throws Exception {
    dotestForceInvalidate(true, true, true, false);
  }

  /**
   * 1. create an entry 2. Install a observer to pause sending subscription events to the client 3.
   * invalidate the entry from the server (it will be done on server but pause prevents it from
   * being sent to the client). 4. verify that afterInvalidate was invoked on the server. 5. change
   * the same entry (do a put). Both the client and server now have the latest version which is this
   * update. 6. unpause the observer so that it now sends invalidate event to client. It will arrive
   * late and not be done because of concurrency checks. 7. verify that afterInvalidate was invoked
   * on the client.
   */
  @Test
  public void testInvalidateLosingOnConcurrencyChecks() throws Exception {
    try {
      setupServerAndClientVMs(true, true, false, false);
      final String key = "delayInvalidate";
      region1.registerInterest("ALL_KEYS", InterestResultPolicy.NONE, false, false);
      region1.put(key, "1000");
      logger.info("installing observers");
      server1.invoke(() -> installObserver());
      server2.invoke(() -> installObserver());

      server2.invoke(() -> invalidateOnServer(key));

      validateServerListenerInvoked();

      logger.info("putting a new value 1001");
      region1.put(key, "1001");
      logger.info("UnPausing observers");
      server1.invoke(() -> unpauseObserver());
      server2.invoke(() -> unpauseObserver());

      waitForClientInvalidate();

    } finally {
      server1.invoke(() -> cleanupObserver());
      server2.invoke(() -> cleanupObserver());
    }
  }

  private static void installObserver() {
    CacheClientProxy.AFTER_MESSAGE_CREATION_FLAG = true;
    ClientServerObserverHolder.setInstance(new DelaySendingEvent());
  }

  private static void unpauseObserver() {
    DelaySendingEvent observer = (DelaySendingEvent) ClientServerObserverHolder.getInstance();
    observer.latch.countDown();
  }

  private static void cleanupObserver() {
    CacheClientProxy.AFTER_MESSAGE_CREATION_FLAG = false;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter());
  }

  private static void invalidateOnServer(final Object key) {
    Region<?, ?> r = GemFireCacheImpl.getExisting().getRegion(REGION_NAME1);
    r.invalidate(key);
  }

  private static void createOnServer(final Object key, final Object value) {
    @SuppressWarnings("unchecked")
    Region<Object, Object> r = GemFireCacheImpl.getExisting().getRegion(REGION_NAME1);
    r.create(key, value);
  }

  private void waitForClientInvalidate() {
    await()
        .until(() -> hasClientListenerAfterInvalidateBeenInvoked());
  }

  static class DelaySendingEvent extends ClientServerObserverAdapter {
    CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void afterMessageCreation(Message msg) {
      try {
        logger.info("waiting in DelaySendingEvent...");
        latch.await();
        logger.info("finished waiting in DelaySendingEvent");
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * 1. Invalidate a non-existent entry from the server. 2. Validate that the servers see after
   * invalidate. 3. Validate that the subscribed client invokes after invalidate.
   */
  private void dotestForceInvalidate(boolean concurrencyChecksOnServer,
      boolean concurrencyChecksOnClient, boolean clientEmpty, boolean serverPartitioned)
      throws Exception {
    setupServerAndClientVMs(concurrencyChecksOnServer, concurrencyChecksOnClient, clientEmpty,
        serverPartitioned);

    server2.invoke(() -> createOnServer("key", "value"));
    region1.registerInterest("ALL_KEYS", InterestResultPolicy.NONE, false, false);
    server2.invoke(() -> invalidateOnServer("key"));

    validateServerListenerInvoked();
    waitForClientInvalidate();
  }

  private void setupServerAndClientVMs(boolean concurrencyChecksOnServer,
      boolean concurrencyChecksOnClient, boolean clientEmpty, boolean serverPartitioned)
      throws Exception {
    int port1 = initServerCache(server1, concurrencyChecksOnServer, serverPartitioned); // vm0
    int port2 = initServerCache(server2, concurrencyChecksOnServer, serverPartitioned); // vm1
    String serverName = NetworkUtils.getServerHostName(Host.getHost(0));
    createClientCache(serverName, port1, port2, clientEmpty, concurrencyChecksOnClient);
    logger.info("testing force invalidate on on client");
  }

  private void validateServerListenerInvoked() {
    boolean listenerInvoked =
        server1.invoke(() -> validateOnServer()) || server2.invoke(() -> validateOnServer());
    assertTrue(listenerInvoked);
  }

  private static boolean validateOnServer() {
    Region<?, ?> region = GemFireCacheImpl.getExisting().getRegion(REGION_NAME1);
    CacheListener<?, ?>[] listeners = region.getAttributes().getCacheListeners();
    for (CacheListener<?, ?> listener : listeners) {
      if (listener instanceof ServerListener) {
        ServerListener serverListener = (ServerListener) listener;
        if (serverListener.afterInvalidateInvoked) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean hasClientListenerAfterInvalidateBeenInvoked() {
    Region<?, ?> region = getCache().getRegion(REGION_NAME1);
    CacheListener<?, ?>[] listeners = region.getAttributes().getCacheListeners();
    for (CacheListener<?, ?> listener : listeners) {
      if (listener instanceof ClientListener) {
        ClientListener clientListener = (ClientListener) listener;
        if (clientListener.afterInvalidateInvoked) {
          return true;
        }
      }
    }
    return false;
  }

  private static Integer createServerCache(Boolean concurrencyChecksEnabled, Boolean partitioned,
      Integer maxThreads) throws Exception {
    AbstractRegionMap.FORCE_INVALIDATE_EVENT = true;
    Properties props = new Properties();
    Cache cache = new ClientServerForceInvalidateDUnitTest().createCacheV(props);
    RegionFactory<String, String> factory = cache.createRegionFactory();
    if (partitioned) {
      factory.setDataPolicy(DataPolicy.PARTITION);
      factory.setPartitionAttributes(new PartitionAttributesFactory<String, String>()
          .setRedundantCopies(0).setTotalNumBuckets(251).create());
    } else {
      factory.setDataPolicy(DataPolicy.REPLICATE);
    }
    factory.setConcurrencyChecksEnabled(concurrencyChecksEnabled);
    factory.addCacheListener(new ServerListener());
    Region<String, String> r1 = factory.create(REGION_NAME1);
    assertNotNull(r1);

    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    logger.info("Starting server on port " + port);
    server.setPort(port);
    server.setMaxThreads(maxThreads.intValue());
    server.start();
    logger.info("Started server on port " + server.getPort());
    return new Integer(server.getPort());

  }

  public static void createClientCache(String h, int port1, int port2, boolean empty,
      boolean concurrenctChecksEnabled) throws Exception {
    AbstractRegionMap.FORCE_INVALIDATE_EVENT = true;
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    Cache cache = new ClientServerForceInvalidateDUnitTest().createCacheV(props);
    PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(h, port1).addServer(h, port2)
        .setSubscriptionEnabled(true).setThreadLocalConnections(true).setReadTimeout(1000)
        .setSocketBufferSize(32768).setMinConnections(3).setSubscriptionRedundancy(-1)
        .setPingInterval(2000).create("ClientServerForceInvalidateDUnitTestPool");

    RegionFactory<String, String> factory = cache.createRegionFactory();
    if (empty) {
      factory.setDataPolicy(DataPolicy.EMPTY);
      factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
    } else {
      factory.setDataPolicy(DataPolicy.NORMAL);
    }
    factory.setPoolName(p.getName());
    factory.setConcurrencyChecksEnabled(concurrenctChecksEnabled);
    region1 = factory.create(REGION_NAME1);
    region1.registerInterest("ALL_KEYS", InterestResultPolicy.NONE, false, false);
    region1.getAttributesMutator().addCacheListener(new ClientListener());
    assertNotNull(region1);
    await().until(() -> poolReady(p));
  }

  private static boolean poolReady(final PoolImpl pool) {
    try {
      Connection conn = pool.acquireConnection();
      if (conn == null) {
        // excuse = "acquireConnection returned null?";
        return false;
      }
      return true;
    } catch (NoAvailableServersException e) {
      // excuse = "Cannot find a server: " + e;
      return false;
    }
  }

  @SuppressWarnings("deprecation")
  private Cache createCacheV(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    Cache cache = getCache();
    assertNotNull(cache);
    return cache;
  }

  static class ClientListener extends CacheListenerAdapter<String, String> {
    public boolean afterInvalidateInvoked;

    @Override
    public void afterCreate(EntryEvent<String, String> event) {
      super.afterCreate(event);
      logger.info("afterCreate: {" + event.getOldValue() + " -> " + event.getNewValue() + "} at="
          + System.currentTimeMillis());
    }

    @Override
    public void afterUpdate(EntryEvent<String, String> event) {
      super.afterUpdate(event);
      logger.info("afterUpdate: {" + event.getOldValue() + " -> " + event.getNewValue() + "} at="
          + System.currentTimeMillis());
    }

    @Override
    public void afterInvalidate(final EntryEvent<String, String> event) {
      super.afterInvalidate(event);
      afterInvalidateInvoked = true;
      String prefix = "";
      if (!event.isOriginRemote()) {
        prefix = "    ";
      }
      logger.info(prefix + "afterInvalidate: {" + event.getOldValue() + " -> " + event.getNewValue()
          + "} at=" + System.currentTimeMillis());
    }
  }

  static class ServerListener extends CacheListenerAdapter<String, String> {
    boolean afterInvalidateInvoked;

    @Override
    public void afterCreate(EntryEvent<String, String> event) {
      super.afterCreate(event);
      logger.info("afterCreate: {" + event.getOldValue() + " -> " + event.getNewValue() + "} at="
          + System.currentTimeMillis());
    }

    @Override
    public void afterUpdate(EntryEvent<String, String> event) {
      super.afterUpdate(event);
      logger.info("afterUpdate: {" + event.getOldValue() + " -> " + event.getNewValue() + "} at="
          + System.currentTimeMillis());
    }

    @Override
    public void afterInvalidate(EntryEvent<String, String> event) {
      super.afterInvalidate(event);
      afterInvalidateInvoked = true;
      logger.info("afterInvalidate: {" + event.getOldValue() + " -> " + event.getNewValue()
          + "} at=" + System.currentTimeMillis());
    }
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    // close the clients first
    closeForceInvalidateCache();
    // then close the servers
    server1.invoke(() -> closeForceInvalidateCache());
    server2.invoke(() -> closeForceInvalidateCache());
  }

  @SuppressWarnings("deprecation")
  private static void closeForceInvalidateCache() {
    AbstractRegionMap.FORCE_INVALIDATE_EVENT = false;
    Cache cache = new ClientServerForceInvalidateDUnitTest().getCache();
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

}
