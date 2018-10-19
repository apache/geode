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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
@SuppressWarnings("serial")
public class DurableClientQueueSizeDUnitTest extends JUnit4DistributedTestCase {

  public static final String MY_DURABLE_CLIENT = "my-durable-client";
  private static VM vm0 = null;
  private static VM vm1 = null;
  private static VM vm2 = null;
  private static VM vm3 = null;

  private static GemFireCacheImpl cache;

  private static int port0;

  private static int port1;

  private static final int EXCEPTION = -5;

  public static final String REGION_NAME =
      DurableClientQueueSizeDUnitTest.class.getSimpleName() + "_region";

  public static final String NEW_REGION =
      DurableClientQueueSizeDUnitTest.class.getSimpleName() + "_region_2";

  public static final String POOL_NAME = "my-pool";

  public static final String DEFAULT_POOL_NAME = "DEFAULT";

  @Override
  public final void postSetUp() throws Exception {
    vm0 = Host.getHost(0).getVM(0);
    vm1 = Host.getHost(0).getVM(1);
    vm2 = Host.getHost(0).getVM(2);
    vm3 = Host.getHost(0).getVM(3);

    port0 = (Integer) vm0.invoke(() -> DurableClientQueueSizeDUnitTest.createCacheServer());
    port1 = (Integer) vm1.invoke(() -> DurableClientQueueSizeDUnitTest.createCacheServer());
    IgnoredException.addIgnoredException("java.net.SocketException");
    IgnoredException.addIgnoredException("Unexpected IOException");
  }

  @Override
  public final void preTearDown() throws Exception {
    closeCache();

    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.closeCache());
    vm3.invoke(() -> DurableClientQueueSizeDUnitTest.closeCache());

    vm0.invoke(() -> DurableClientQueueSizeDUnitTest.closeCache());
    vm1.invoke(() -> DurableClientQueueSizeDUnitTest.closeCache());
  }

  @Test
  public void testNonDurableClientFails() throws Exception {
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache", // TODO: change to lambda
        new Object[] {vm2.getHost(), new Integer[] {port0, port1}, false});

    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.verifyQueueSize(EXCEPTION));
  }

  // this test is disabled due to a high rate of failure. It fails with
  // the queue size being 11 instead of 10 in the first verifyQueueSize check.
  // See internal ticket #52227.
  @Ignore("TODO: test is disabled due to #52227")
  @Test
  public void testSinglePoolClientReconnectsBeforeTimeOut() throws Exception {
    int num = 10;
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] {vm2.getHost(), new Integer[] {port0, port1}});
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest
        .verifyQueueSize(PoolImpl.PRIMARY_QUEUE_NOT_AVAILABLE));
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.doRI());
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.readyForEvents());

    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.closeCache(Boolean.TRUE));

    vm0.invoke(() -> DurableClientQueueSizeDUnitTest.doPuts(num));

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] {vm2.getHost(), new Integer[] {port0, port1}});

    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.verifyQueueSize(num + 1 /* +1 for marker */));
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.readyForEvents());
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.verifyQueueSize(EXCEPTION));

  }

  // Slows down client consumption from queue, this should cause a delay from server queue dispatch
  // and receiving an ack from the client. The server should still clean up acked events from the
  // queue even if no more messages are being sent. Previously events would get stuck in the queue
  // if no new messages were sent because clean up was only done while sending messages
  @Test
  public void ackedEventsShouldBeRemovedFromTheQueueEventuallyEvenIfNoNewMessagesAreSent()
      throws Exception {
    int num = 10;
    CacheListener slowListener = new SlowListener();

    vm1.invoke(() -> DurableClientQueueSizeDUnitTest.closeCache());

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] {vm2.getHost(), new Integer[] {port0, port1}, "300", Boolean.TRUE,
            Boolean.FALSE, slowListener});
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.doRI());
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.readyForEvents());

    vm0.invoke(() -> DurableClientQueueSizeDUnitTest.doPutsIntoRegion(REGION_NAME, num));

    vm0.invoke(() -> await().untilAsserted(() -> {
      CacheClientProxy ccp = DurableClientQueueSizeDUnitTest.getCacheClientProxy(MY_DURABLE_CLIENT);
      assertEquals(0, ccp.getQueueSize());
      assertEquals(0, ccp.getQueueSizeStat());
    }));
  }

  @Test
  public void testSinglePoolClientReconnectsAfterTimeOut() throws Exception {
    int num = 10;
    long timeoutSeconds = 10;
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache", new Object[] {
        vm2.getHost(), new Integer[] {port0, port1}, String.valueOf(timeoutSeconds), true});
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.doRI());
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.readyForEvents());

    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.closeCache(Boolean.TRUE));

    vm0.invoke(() -> DurableClientQueueSizeDUnitTest.doPuts(num));
    Thread.sleep(timeoutSeconds * 1000); // TODO use a waitCriterion

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] {vm2.getHost(), new Integer[] {port0, port1}});

    vm2.invoke(
        () -> DurableClientQueueSizeDUnitTest.verifyQueueSize(PoolImpl.PRIMARY_QUEUE_TIMED_OUT));
  }

  @Test
  public void testPrimaryServerRebootReturnsCorrectResponse() throws Exception {
    int num = 10;
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] {vm2.getHost(), new Integer[] {port0, port1}});
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.doRI());
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.readyForEvents());

    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.closeCache(Boolean.TRUE));

    vm0.invoke(() -> DurableClientQueueSizeDUnitTest.doPuts(num));

    // Identify primary and restart it
    boolean isVM0Primary = (Boolean) vm0.invoke(() -> DurableClientQueueSizeDUnitTest.isPrimary());
    int port = 0;
    if (isVM0Primary) {
      vm0.invoke(() -> DurableClientQueueSizeDUnitTest.closeCache());
      vm0.invoke(() -> DurableClientQueueSizeDUnitTest.createCacheServer(port0));
      port = port0;
    } else { // vm1 is primary
      vm1.invoke(() -> DurableClientQueueSizeDUnitTest.closeCache());
      vm1.invoke(() -> DurableClientQueueSizeDUnitTest.createCacheServer(port1));
      port = port1;
    }

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] {vm2.getHost(), new Integer[] {port}});

    vm2.invoke(() -> DurableClientQueueSizeDUnitTest
        .verifyQueueSize(PoolImpl.PRIMARY_QUEUE_NOT_AVAILABLE));
  }

  @Ignore("TODO: test is disabled due to #51854")
  @Test
  public void testMultiPoolClientReconnectsBeforeTimeOut() throws Exception {
    int num = 10;
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] {vm2.getHost(), new Integer[] {port0, port1}, "300", true/* durable */,
            true /* multiPool */});
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.verifyQueueSize(
        PoolImpl.PRIMARY_QUEUE_NOT_AVAILABLE, PoolImpl.PRIMARY_QUEUE_NOT_AVAILABLE));
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.doRI());
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.readyForEvents());

    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.closeCache(Boolean.TRUE));

    vm0.invoke(() -> DurableClientQueueSizeDUnitTest.doPuts(num));

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] {vm2.getHost(), new Integer[] {port0, port1}, "300", true/* durable */,
            true /* multiPool */});

    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.verifyQueueSize(num + 1 /* +1 for marker */,
        (num * 2) + 1));
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.readyForEvents());
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.verifyQueueSize(EXCEPTION, EXCEPTION));
  }

  @Ignore("TODO: test is disabled due to #51854")
  @Test
  public void testMultiPoolClientReconnectsAfterTimeOut() throws Exception {
    int num = 10;
    long timeout = 10;
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] {vm2.getHost(), new Integer[] {port0, port1}, String.valueOf(timeout),
            true/* durable */, true /* multiPool */});
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.verifyQueueSize(
        PoolImpl.PRIMARY_QUEUE_NOT_AVAILABLE, PoolImpl.PRIMARY_QUEUE_NOT_AVAILABLE));
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.doRI());
    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.readyForEvents());

    vm2.invoke(() -> DurableClientQueueSizeDUnitTest.closeCache(Boolean.TRUE));

    vm0.invoke(() -> DurableClientQueueSizeDUnitTest.doPuts(num));
    // vm0.invoke(DurableClientQueueSizeDUnitTest.class,
    // "verifyQueueSizeAtServer", new Object[] { DEFAULT_POOL_NAME, num + 1 });
    // vm0.invoke(DurableClientQueueSizeDUnitTest.class,
    // "verifyQueueSizeAtServer", new Object[] { POOL_NAME, num * 2 + 1 });
    Thread.sleep(timeout * 1000); // TODO use a waitCriterion

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] {vm2.getHost(), new Integer[] {port0, port1}, "300", true/* durable */,
            true /* multiPool */});

    vm2.invoke(() -> DurableClientQueueSizeDUnitTest
        .verifyQueueSize(PoolImpl.PRIMARY_QUEUE_TIMED_OUT, PoolImpl.PRIMARY_QUEUE_TIMED_OUT));
  }

  @Ignore("TODO: test is not implemented")
  @Test
  public void testMultiPoolClientFailsOver() throws Exception {}

  public static void closeCache() throws Exception {
    closeCache(false);
  }

  public static void closeCache(Boolean keepAlive) throws Exception {
    setSpecialDurable(false);
    if (cache != null) {
      cache.close(keepAlive);
    }
  }

  public static Boolean isPrimary() throws Exception {
    return CacheClientNotifier.getInstance().getClientProxies().iterator().next().isPrimary();
  }

  public static Integer createCacheServer() throws Exception {
    return createCacheServer(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
  }

  @SuppressWarnings("deprecation")
  public static Integer createCacheServer(Integer serverPort) throws Exception {
    Properties props = new Properties();
    props.setProperty(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
    DurableClientQueueSizeDUnitTest test = new DurableClientQueueSizeDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    ds.disconnect();
    cache = (GemFireCacheImpl) CacheFactory.create(test.getSystem());

    RegionFactory<String, String> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);

    rf.create(REGION_NAME);
    rf.create(NEW_REGION);

    CacheServer server = cache.addCacheServer();
    server.setPort(serverPort);
    server.start();
    return server.getPort();
  }

  public static void createClientCache(Host host, Integer[] ports) throws Exception {
    createClientCache(host, ports, "300", Boolean.TRUE);
  }

  public static void createClientCache(Host host, Integer[] ports, Boolean durable)
      throws Exception {
    createClientCache(host, ports, "300", durable);
  }

  public static void createClientCache(Host host, Integer[] ports, String timeoutMilis,
      Boolean durable) throws Exception {
    createClientCache(host, ports, timeoutMilis, durable, false, null);
  }

  public static void setSpecialDurable(Boolean bool) {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "SPECIAL_DURABLE", bool.toString());
  }

  @SuppressWarnings("deprecation")
  public static void createClientCache(Host host, Integer[] ports, String timeoutSeconds,
      Boolean durable, Boolean multiPool, CacheListener cacheListener) throws Exception {
    if (multiPool) {
      setSpecialDurable(true);
    }
    Properties props = new Properties();
    if (durable) {
      props.setProperty(DURABLE_CLIENT_ID, MY_DURABLE_CLIENT);
      props.setProperty(DURABLE_CLIENT_TIMEOUT, timeoutSeconds);
    }

    DistributedSystem ds = new DurableClientQueueSizeDUnitTest().getSystem(props);
    ds.disconnect();
    ClientCacheFactory ccf = new ClientCacheFactory(props);
    ccf.setPoolSubscriptionEnabled(true);
    ccf.setPoolSubscriptionAckInterval(50);
    ccf.setPoolSubscriptionRedundancy(1);
    ccf.setPoolMaxConnections(1);
    for (int port : ports) {
      ccf.addPoolServer(host.getHostName(), port);
    }
    cache = (GemFireCacheImpl) ccf.create();

    ClientRegionFactory<String, String> crf =
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
    if (cacheListener != null) {
      crf.addCacheListener(cacheListener);
    }
    crf.setPoolName(cache.getDefaultPool().getName());
    crf.create(REGION_NAME);

    if (multiPool) {
      String poolName = POOL_NAME;
      PoolFactory pf = PoolManager.createFactory();
      for (int port : ports) {
        pf.addServer(host.getHostName(), port);
      }
      pf.setSubscriptionEnabled(true);
      pf.create(poolName);
      crf.setPoolName(poolName);
      crf.create(NEW_REGION);
    }
  }

  @SuppressWarnings("unchecked")
  public static void doRI() {
    cache.getRegion(REGION_NAME).registerInterest("ALL_KEYS", true);
    if (cache.getRegion(NEW_REGION) != null) {
      cache.getRegion(NEW_REGION).registerInterest("ALL_KEYS", true);
    }
  }

  public static void readyForEvents() {
    cache.readyForEvents();
  }

  @SuppressWarnings("unchecked")
  public static void doPuts(Integer numOfPuts) throws Exception {
    Region<String, String> region = cache.getRegion(REGION_NAME);

    for (int j = 0; j < numOfPuts; j++) {
      region.put("KEY_" + j, "VALUE_" + j);
    }

    region = cache.getRegion(NEW_REGION);

    for (int j = 0; j < (numOfPuts * 2); j++) {
      region.put("KEY_" + j, "VALUE_" + j);
    }
  }

  public static void doPutsIntoRegion(String regionName, Integer numOfPuts) throws Exception {
    Region<String, String> region = cache.getRegion(regionName);

    for (int j = 0; j < numOfPuts; j++) {
      region.put("KEY_" + j, "VALUE_" + j);
    }
  }

  public static void verifyQueueSizeAtServer(String poolName, Integer num) {
    Iterator<CacheClientProxy> it = CacheClientNotifier.getInstance().getClientProxies().iterator();
    while (it.hasNext()) {
      CacheClientProxy ccp = it.next();
      if (ccp.getDurableId().contains(poolName)) {
        assertEquals(num.intValue(), ccp.getStatistics().getMessagesQueued());
      }
    }
  }

  public static CacheClientProxy getCacheClientProxy(String durableClientName) {
    Iterator<CacheClientProxy> it = CacheClientNotifier.getInstance().getClientProxies().iterator();
    while (it.hasNext()) {
      CacheClientProxy ccp = it.next();
      if (ccp.getDurableId().contains(durableClientName)) {
        return ccp;
      }
    }
    return null;
  }

  public static void verifyQueueSize(Integer num) throws Exception {
    verifyQueueSize(num, Integer.MIN_VALUE);
  }

  public static void verifyQueueSize(Integer num1, Integer num2) throws Exception {
    try {
      assertEquals(num1.intValue(), PoolManager.find(DEFAULT_POOL_NAME).getPendingEventCount());
    } catch (IllegalStateException ise) {
      assertEquals(EXCEPTION, num1.intValue());
    }
    if (num2 != Integer.MIN_VALUE) {
      try {
        assertEquals(num2.intValue(), PoolManager.find(POOL_NAME).getPendingEventCount());
      } catch (IllegalStateException ise) {
        assertEquals(EXCEPTION, num2.intValue());
      }
    }
  }

  private class SlowListener implements CacheListener, Serializable {

    @Override
    public void afterCreate(final EntryEvent event) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void afterUpdate(final EntryEvent event) {

    }

    @Override
    public void afterInvalidate(final EntryEvent event) {

    }

    @Override
    public void afterDestroy(final EntryEvent event) {

    }

    @Override
    public void afterRegionInvalidate(final RegionEvent event) {

    }

    @Override
    public void afterRegionDestroy(final RegionEvent event) {

    }

    @Override
    public void afterRegionClear(final RegionEvent event) {

    }

    @Override
    public void afterRegionCreate(final RegionEvent event) {

    }

    @Override
    public void afterRegionLive(final RegionEvent event) {

    }

    @Override
    public void close() {

    }
  }
}
