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

import static java.lang.Thread.yield;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.ha.HAHelper;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * This test verifies the conflation functionality of the dispatcher.
 *
 * A sequence of create, put, put, put, destroy is sent and a total of three operations should be
 * sent to the client from the server and not all the five.
 *
 * The test has two regions. In one scenario they share a common bridgewriter and in the second
 * scenario, each has a unique bridgewriter.
 */
@Category({ClientSubscriptionTest.class})
public class ConflationDUnitTest extends JUnit4DistributedTestCase {

  VM vm0 = null;
  VM vm2 = null;
  private static Cache cache = null;
  private int PORT;
  private static final String REGION_NAME1 = "ConflationDUnitTest_region1";
  private static final String REGION_NAME2 = "ConflationDUnitTest_region2";
  static final String MARKER = "markerKey";

  private static HashMap statMap = new HashMap();

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm2 = host.getVM(2);
    PORT = ((Integer) vm0.invoke(() -> ConflationDUnitTest.createServerCache())).intValue();
  }

  private Cache createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    Cache cache = CacheFactory.create(ds);
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
    return cache;
  }

  /**
   * test more messages are not sent to client from server
   *
   */
  @Test
  public void testNotMoreMessagesSent() throws Exception {
    vm0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart());
    Host host = Host.getHost(0);
    createClientCache1CommonWriterTest3(NetworkUtils.getServerHostName(host), new Integer(PORT));
    vm2.invoke(() -> ConflationDUnitTest.createClientCache2CommonWriterTest3(
        NetworkUtils.getServerHostName(host), new Integer(PORT)));
    vm2.invoke(() -> ConflationDUnitTest.setClientServerObserverForBeforeInterestRecovery());
    vm2.invoke(() -> ConflationDUnitTest.setAllCountersZero());
    vm2.invoke(() -> ConflationDUnitTest.assertAllCountersZero());
    vm2.invoke(() -> ConflationDUnitTest.registerInterest());
    create();
    put200();
    createMarker();
    vm2.invoke(() -> ConflationDUnitTest.waitForMarker());
    vm2.invoke(() -> ConflationDUnitTest.assertValue());
    vm2.invoke(() -> ConflationDUnitTest.destroyMarker());
    destroy();
    createMarker();
    vm2.invoke(() -> ConflationDUnitTest.waitForMarker());
    vm2.invoke(() -> ConflationDUnitTest.assertCounterSizesLessThan200());
    vm0.invoke(() -> ConflationDUnitTest.getStatsOnServer());
    vm0.invoke(() -> ConflationDUnitTest.assertConflationStatus());
  }

  /**
   * create properties for a loner VM
   */
  private static Properties createProperties1() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    return props;
  }

  /**
   * create pool for a client
   *
   * @return created pool
   */
  private static Pool createPool(String host, String name, Integer port, boolean enableQueue) {
    return PoolManager.createFactory().addServer(host, port.intValue())
        .setSubscriptionEnabled(enableQueue).setSubscriptionRedundancy(-1).setReadTimeout(10000)
        .setSocketBufferSize(32768).setMinConnections(3).setThreadLocalConnections(true)
        // .setRetryInterval(10000)
        // .setRetryAttempts(5)
        .create("ConflationUnitTestPool" + name);
  }

  /**
   * create a client with 2 regions sharing a common writer
   *
   */

  public static void createClientCache1CommonWriter(String host, Integer port) throws Exception {
    ConflationDUnitTest test = new ConflationDUnitTest();
    cache = test.createCache(createProperties1());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(createPool(host, "p1", port, true).getName());
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME1, attrs);
    cache.createRegion(REGION_NAME2, attrs);
  }

  /**
   * create a client with 2 regions sharing a common writer
   *
   */


  public static void createClientCache1CommonWriterTest3(String host, Integer port)
      throws Exception {
    ConflationDUnitTest test = new ConflationDUnitTest();
    cache = test.createCache(createProperties1());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(createPool(host, "p1", port, false).getName());
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME1, attrs);
    cache.createRegion(REGION_NAME2, attrs);
  }

  /**
   * create client 2 with 2 regions with sharing a common writer and having a common listener
   *
   */
  public static void createClientCache2CommonWriter(String host, Integer port) throws Exception {
    ConflationDUnitTest test = new ConflationDUnitTest();
    cache = test.createCache(createProperties1());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(createPool(host, "p1", port, true).getName());
    factory.addCacheListener(new CacheListenerAdapter() {
      public void afterCreate(EntryEvent event) {
        LogWriterUtils.getLogWriter().info("Listener received event " + event);
        String val = (String) event.getNewValue();
        synchronized (ConflationDUnitTest.class) {
          if (val.equals(MARKER)) {
            count++;
          } else {
            counterCreate++;
          }
          if (2 == count) {
            ConflationDUnitTest.class.notify();
          }
        }
      }

      public void afterUpdate(EntryEvent event) {
        LogWriterUtils.getLogWriter().info("Listener received event " + event);
        synchronized (this) {
          counterUpdate++;
        }
      }

      public void afterDestroy(EntryEvent event) {
        LogWriterUtils.getLogWriter().info("Listener received event " + event);
        synchronized (this) {
          if (!event.getKey().equals(MARKER)) {
            counterDestroy++;
          }
        }
      }
    });
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME1, attrs);
    cache.createRegion(REGION_NAME2, attrs);
  }

  public static void createClientCache2CommonWriterTest3(String host, Integer port)
      throws Exception {
    ConflationDUnitTest test = new ConflationDUnitTest();
    cache = test.createCache(createProperties1());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(createPool(host, "p1", port, true).getName());
    factory.addCacheListener(new CacheListenerAdapter() {
      public void afterCreate(EntryEvent event) {
        LogWriterUtils.getLogWriter().info("Listener received event " + event);
        String val = (String) event.getNewValue();
        synchronized (ConflationDUnitTest.class) {
          if (val.equals(MARKER)) {
            count++;
          } else {
            counterCreate++;
          }
          if (2 == count) {
            ConflationDUnitTest.class.notify();
          }
        }
      }

      public void afterUpdate(EntryEvent event) {
        LogWriterUtils.getLogWriter().info("Listener received event " + event);
        synchronized (this) {
          counterUpdate++;
        }
      }

      public void afterDestroy(EntryEvent event) {
        LogWriterUtils.getLogWriter().info("Listener received event " + event);
        synchronized (this) {
          if (!event.getKey().equals(MARKER)) {
            counterDestroy++;
          }
        }
      }
    });
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME1, attrs);
    cache.createRegion(REGION_NAME2, attrs);
  }


  /**
   * create a client with 2 regions each having its own writer
   *
   */

  public static void createClientCache1UniqueWriter(String host, Integer port) throws Exception {
    ConflationDUnitTest test = new ConflationDUnitTest();
    cache = test.createCache(createProperties1());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(createPool(host, "p1", port, true).getName());
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME1, attrs);
    factory.setPoolName(createPool(host, "p2", port, true).getName());
    attrs = factory.create();
    cache.createRegion(REGION_NAME2, attrs);
  }

  /**
   * create client 2 with 2 regions each with a unique writer but both having a common listener
   *
   */
  public static void createClientCache2UniqueWriter(String host, Integer port) throws Exception {
    ConflationDUnitTest test = new ConflationDUnitTest();
    cache = test.createCache(createProperties1());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(createPool(host, "p1", port, true).getName());
    factory.addCacheListener(new CacheListenerAdapter() {
      public void afterCreate(EntryEvent event) {
        String val = (String) event.getNewValue();
        LogWriterUtils.getLogWriter().info("Listener received event " + event);
        synchronized (ConflationDUnitTest.class) {
          if (val.equals(MARKER)) {
            count++;
          } else {
            counterCreate++;
          }
          if (2 == count) {
            ConflationDUnitTest.class.notify();
          }
        }
      }

      public void afterUpdate(EntryEvent event) {
        synchronized (this) {
          counterUpdate++;
        }
      }

      public void afterDestroy(EntryEvent event) {
        synchronized (this) {
          if (!event.getKey().equals(MARKER)) {
            counterDestroy++;
          }
        }
      }
    });
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME1, attrs);
    factory.setPoolName(createPool(host, "p2", port, true).getName());
    attrs = factory.create();
    cache.createRegion(REGION_NAME2, attrs);
  }

  /**
   * variables to count operations (messages received on client from server)
   */
  static volatile int count = 0;
  static volatile int counterCreate = 0;
  static volatile int counterUpdate = 0;
  static volatile int counterDestroy = 0;

  /**
   * assert all the counters are zero
   *
   */
  public static void assertAllCountersZero() {
    assertEquals(0, count);
    assertEquals(0, counterCreate);
    assertEquals(0, counterUpdate);
    assertEquals(0, counterDestroy);
  }

  /**
   * set all the counters to zero
   *
   */
  public static void setAllCountersZero() {
    count = 0;
    counterCreate = 0;
    counterUpdate = 0;
    counterDestroy = 0;
  }

  /**
   * reset all counters to zero before interest recovery
   *
   */
  public static void setClientServerObserverForBeforeInterestRecovery() {
    PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      public void beforeInterestRecovery() {
        setAllCountersZero();
      }
    });
  }

  /**
   * assert the counters size are as expected (2)
   *
   */
  public static void assertCounterSizes() {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return counterCreate == 2;
      }

      public String description() {
        return "Expected counterCreate to be 2. Instead it was " + counterCreate + ".";
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);

    ev = new WaitCriterion() {
      public boolean done() {
        return counterUpdate == 2;
      }

      public String description() {
        return "Expected counterUpdate to be 2. Instead it was " + counterUpdate + ".";
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);

    ev = new WaitCriterion() {
      public boolean done() {
        return counterDestroy == 2;
      }

      public String description() {
        return "Expected counterDestroy to be 2. Instead it was " + counterDestroy + ".";
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
  }


  /**
   * assert the counters size less than 20000
   *
   */
  public static void assertCounterSizesLessThan200() {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        yield(); // TODO is this necessary?
        return counterCreate == 2;
      }

      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
    // assertIndexDetailsEquals("creates", 2, counterCreate);

    ev = new WaitCriterion() {
      public boolean done() {
        yield(); // TODO is this necessary?
        return counterDestroy == 2;
      }

      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);

    // assertIndexDetailsEquals("destroys", 2, counterDestroy);
    // assertTrue("updates", 20000 >= counterUpdate);
    ev = new WaitCriterion() {
      public boolean done() {
        yield(); // TODO is this necessary?
        return counterUpdate <= 200;
      }

      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
  }

  public static void waitForMarker() {
    cache.getRegion(Region.SEPARATOR + REGION_NAME1);
    cache.getRegion(Region.SEPARATOR + REGION_NAME2);
    long giveUpTime = System.currentTimeMillis() + 30000;
    synchronized (ConflationDUnitTest.class) {
      while (count != 2) {
        if (System.currentTimeMillis() > giveUpTime) {
          assertTrue("Count (" + count + ") failed to reach 2", count == 2);
        }
        try {
          ConflationDUnitTest.class.wait(1000);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      }
    }
  }

  /**
   * assert that the final value is 33
   *
   */
  public static void assertValue() {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      assertTrue(r1.containsKey("key-1"));
      assertTrue(r1.get("key-1").equals("33"));
      assertTrue(r2.containsKey("key-1"));
      assertTrue(r2.get("key-1").equals("33"));
    } catch (Exception e) {
      fail("Exception in trying to get due to " + e);
    }
  }

  /**
   * assert Conflation Status
   *
   */
  public static void assertConflationStatus() {
    assertNotNull(statMap);
    Long confCount = (Long) statMap.get("eventsConflated");
    assertTrue("No Conflation found: eventsConflated value is " + confCount.longValue(),
        confCount.longValue() > (0));
    assertTrue("Error in Conflation found: eventsConflated value is " + confCount.longValue(),
        confCount.longValue() <= (200));
  }


  /**
   * create a server cache and start the server
   *
   */
  public static Integer createServerCache() throws Exception {
    ConflationDUnitTest test = new ConflationDUnitTest();
    cache = test.createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEnableConflation(true);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME1, attrs);
    cache.createRegion(REGION_NAME2, attrs);
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.setSocketBufferSize(32768);
    server.start();
    return new Integer(server.getPort());
  }

  /**
   * close the cache
   *
   */
  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  /**
   * register interest with the server on ALL_KEYS
   *
   */
  public static void registerInterest() {
    try {
      Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region region2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      assertTrue(region1 != null);
      assertTrue(region2 != null);
      region1.registerInterest("ALL_KEYS");
      region2.registerInterest("ALL_KEYS");
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
      Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region region2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      region1.unregisterInterest("ALL_KEYS");
      region2.unregisterInterest("ALL_KEYS");
    } catch (CacheWriterException e) {
      fail("test failed due to " + e);
    }
  }

  /**
   * Create an entry on two region with key : key-1 and value
   *
   */
  public static void create() {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      r1.create("key-1", "value");
      r2.create("key-1", "value");
    } catch (Exception ex) {
      ex.printStackTrace();
      Assert.fail("failed while region.create()", ex);
    }
  }

  /**
   * do three puts on key-1
   *
   */
  public static void put() {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      r1.put("key-1", "11");
      r1.put("key-1", "22");
      r1.put("key-1", "33");
      r2.put("key-1", "11");
      r2.put("key-1", "22");
      r2.put("key-1", "33");
    } catch (Exception ex) {
      ex.printStackTrace();
      Assert.fail("failed while region.put()", ex);
    }
  }

  public static void createMarker() {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      r1.put(MARKER, MARKER);
      r2.put(MARKER, MARKER);
    } catch (Exception ex) {
      ex.printStackTrace();
      Assert.fail("failed while region.create() marker", ex);
    }
  }

  /**
   * do 200 puts on key-1
   *
   */
  public static void put200() {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      for (int i = 1; i < 100; i++) {
        r1.put("key-1", "11");
        r2.put("key-1", "11");

      }
      r1.put("key-1", "33");
      r2.put("key-1", "33");
    } catch (Exception ex) {
      ex.printStackTrace();
      Assert.fail("failed while region.put()", ex);
    }
  }


  /**
   * getting conflation Stats on server
   *
   */

  public static void getStatsOnServer() {
    Cache cache = GemFireCacheImpl.getInstance();
    Iterator itr = cache.getCacheServers().iterator();
    CacheServerImpl server = (CacheServerImpl) itr.next();
    Iterator iter_prox =
        server.getAcceptor().getCacheClientNotifier().getClientProxies().iterator();
    int ccpCount = 0;
    while (iter_prox.hasNext()) {
      ccpCount++;
      CacheClientProxy proxy = (CacheClientProxy) iter_prox.next();
      if (HaHelper.checkPrimary(proxy)) {
        HARegion region = (HARegion) proxy.getHARegion();
        assertNotNull(region);
        HARegionQueue haRegionQueue = HAHelper.getRegionQueue(region);
        statMap.put("eventsConflated",
            new Long(HAHelper.getRegionQueueStats(haRegionQueue).getEventsConflated()));
        LogWriterUtils.getLogWriter().info("new Stats Map  : " + statMap.toString());

      }
    }
    assertTrue("CCP Count is not 1 ", ccpCount == 1);
    // }
  }

  /**
   * do a get on region1
   *
   */
  public static void get() {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      r.get("key-1");
    } catch (Exception ex) {
      ex.printStackTrace();
      Assert.fail("failed while region.get()", ex);
    }
  }

  /**
   * destroy the regions
   *
   */
  public static void destroyRegion() {
    try {
      Region region1 = cache.getRegion("/region1");
      if (region1 != null) {
        region1.destroyRegion();
      }
      Region region2 = cache.getRegion("/region1");
      if (region2 != null) {
        region2.destroyRegion();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("test failed due to exception in destroy region" + e);
    }
  }

  /**
   * destroy key-1
   *
   */
  public static void destroy() {
    try {
      Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region region2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      region1.destroy("key-1");
      region2.destroy("key-1");

    } catch (Exception e) {
      e.printStackTrace();
      fail("test failed due to exception in destroy ");
    }
  }

  /**
   * destroy marker
   *
   */
  public static void destroyMarker() {
    try {
      Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region region2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      region1.destroy(MARKER);
      region2.destroy(MARKER);
      count = 0;

    } catch (Exception e) {
      e.printStackTrace();
      fail("test failed due to exception in destroy ");
    }
  }

  /**
   * close the cache in tearDown
   */
  @Override
  public final void preTearDown() throws Exception {
    // close client
    closeCache();
    vm2.invoke(() -> ConflationDUnitTest.closeCache());
    // close server
    vm0.invoke(() -> ConflationDUnitTest.closeCache());
  }
}
