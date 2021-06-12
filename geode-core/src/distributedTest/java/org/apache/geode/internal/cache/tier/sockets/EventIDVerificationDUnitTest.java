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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.CacheObserverAdapter;
import org.apache.geode.internal.cache.CacheObserverHolder;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.RegionEventImpl;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Test to verify EventID generated from Cache Client is correctly passed on to the cache server for
 * create, update, remove and destroy operations.It also checks that peer nodes also get the same
 * EventID.
 */
@Category({ClientServerTest.class})
public class EventIDVerificationDUnitTest extends JUnit4DistributedTestCase {

  private static Cache cache = null;

  static VM vm0 = null;

  static VM vm1 = null;

  private static int PORT1;

  private static int PORT2;

  private static final String REGION_NAME =
      EventIDVerificationDUnitTest.class.getSimpleName() + "_region";

  protected static EventID eventId;

  static boolean gotCallback = false;

  static boolean testEventIDResult = false;

  /* Constructor */

  public EventIDVerificationDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);

    // start servers first
    PORT1 =
        ((Integer) vm0.invoke(() -> EventIDVerificationDUnitTest.createServerCache())).intValue();
    PORT2 =
        ((Integer) vm1.invoke(() -> EventIDVerificationDUnitTest.createServerCache())).intValue();

    // vm2.invoke(EventIDVerificationDUnitTest.class, "createClientCache", new
    // Object[] { new Integer(PORT1),new Integer(PORT2)});
    createClientCache(NetworkUtils.getServerHostName(host), new Integer(PORT1), new Integer(PORT2));
    CacheObserverHolder.setInstance(new CacheObserverAdapter());
  }

  @Test
  public void testEventIDOnServer() {
    createEntry();
    Boolean pass = (Boolean) vm0.invoke(() -> EventIDVerificationDUnitTest.verifyResult());
    assertTrue(pass.booleanValue());
    put();
    pass = (Boolean) vm0.invoke(() -> EventIDVerificationDUnitTest.verifyResult());
    assertTrue(pass.booleanValue());
    destroy();
    pass = (Boolean) vm0.invoke(() -> EventIDVerificationDUnitTest.verifyResult());
    assertTrue(pass.booleanValue());

    put();
    cache.getLogger().info("going to remove");
    remove();
    cache.getLogger().info("after remove");
    pass = (Boolean) vm0.invoke(() -> EventIDVerificationDUnitTest.verifyResult());
    assertTrue(pass.booleanValue());
  }

  /**
   * Verify that EventId is prapogated to server in case of region destroy. It also checks that peer
   * nodes also get the same EventID.
   *
   */
  @Test
  public void testEventIDPrapogationOnServerDuringRegionDestroy() {
    destroyRegion();
    Boolean pass = (Boolean) vm0.invoke(() -> EventIDVerificationDUnitTest.verifyResult());
    assertTrue(pass.booleanValue());
    pass = (Boolean) vm1.invoke(() -> EventIDVerificationDUnitTest.verifyResult());
    assertTrue(pass.booleanValue());
  }

  /**
   * Verify that EventId is prapogated to server in case of region clear. It also checks that peer
   * nodes also get the same EventID.
   *
   */
  @Test
  public void testEventIDPrapogationOnServerDuringRegionClear() {
    clearRegion();
    Boolean pass = (Boolean) vm0.invoke(() -> EventIDVerificationDUnitTest.verifyResult());
    assertTrue(pass.booleanValue());
    pass = (Boolean) vm1.invoke(() -> EventIDVerificationDUnitTest.verifyResult());
    assertTrue(pass.booleanValue());
  }


  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientCache(String host, Integer port1, Integer port2) throws Exception {
    PORT1 = port1.intValue();
    PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new EventIDVerificationDUnitTest().createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setMirrorType(MirrorType.NONE);

    ClientServerTestCase.configureConnectionPool(factory, host, new int[] {PORT1, PORT2}, true, -1,
        2, null, -1, -1, -2);


    CacheWriter writer = new CacheWriterAdapter() {
      @Override
      public void beforeCreate(EntryEvent event) {
        EventID eventId = ((EntryEventImpl) event).getEventId();
        vm0.invoke(() -> EventIDVerificationDUnitTest.setEventIDData(eventId));
        vm1.invoke(() -> EventIDVerificationDUnitTest.setEventIDData(eventId));
        try {
          super.beforeCreate(event);
        } catch (CacheWriterException e) {
          e.printStackTrace();
          fail("Test failed bcoz of exception =" + e);
        }
      }


      @Override
      public void beforeUpdate(EntryEvent event) {

        EventID eventId = ((EntryEventImpl) event).getEventId();
        vm0.invoke(() -> EventIDVerificationDUnitTest.setEventIDData(eventId));
        vm1.invoke(() -> EventIDVerificationDUnitTest.setEventIDData(eventId));
        try {
          super.beforeUpdate(event);
        } catch (CacheWriterException e) {
          e.printStackTrace();
          fail("Test failed bcoz of exception =" + e);
        }

      }

      @Override
      public void beforeDestroy(EntryEvent event) {
        EventID eventId = ((EntryEventImpl) event).getEventId();
        vm0.invoke(() -> EventIDVerificationDUnitTest.setEventIDData(eventId));
        vm1.invoke(() -> EventIDVerificationDUnitTest.setEventIDData(eventId));
        try {
          super.beforeDestroy(event);
        } catch (CacheWriterException e) {
          e.printStackTrace();
          fail("Test failed bcoz of exception =" + e);
        }

      }

      @Override
      public void beforeRegionDestroy(RegionEvent event) {
        EventID eventId = ((RegionEventImpl) event).getEventId();
        vm0.invoke(() -> EventIDVerificationDUnitTest.setEventIDData(eventId));
        vm1.invoke(() -> EventIDVerificationDUnitTest.setEventIDData(eventId));
        try {
          super.beforeRegionDestroy(event);
        } catch (CacheWriterException e) {
          e.printStackTrace();
          fail("Test failed bcoz of exception =" + e);
        }

      }

      @Override
      public void beforeRegionClear(RegionEvent event) {
        EventID eventId = ((RegionEventImpl) event).getEventId();
        vm0.invoke(() -> EventIDVerificationDUnitTest.setEventIDData(eventId));
        vm1.invoke(() -> EventIDVerificationDUnitTest.setEventIDData(eventId));
        try {
          super.beforeRegionClear(event);
        } catch (CacheWriterException e) {
          e.printStackTrace();
          fail("Test failed bcoz of exception =" + e);
        }
      }
    };
    factory.setCacheWriter(writer);
    /*
     * factory.setCacheListener(new CacheListenerAdapter() { public void afterCreate(EntryEvent
     * event) { synchronized (this) { threadId = ((EntryEventImpl)event).getEventId().getThreadID();
     * membershipId = ((EntryEventImpl)event).getEventId().getMembershipID(); } }
     *
     * public void afterUpdate(EntryEvent event) { synchronized (this) { verifyEventIDs(event); } }
     *
     * public void afterDestroy(EntryEvent event) { synchronized (this) { verifyEventIDs(event); } }
     * public void afterRegionDestroy(RegionEvent event) { synchronized (this) { threadId =
     * ((RegionEventImpl)event).getEventId().getThreadID(); membershipId =
     * ((RegionEventImpl)event).getEventId().getMembershipID(); } } });
     */

    RegionAttributes attrs = factory.create();
    Region r = cache.createRegion(REGION_NAME, attrs);
    r.registerInterest("ALL_KEYS");
  }

  public static void setEventIDData(Object evID) {
    eventId = (EventID) evID;

  }

  public static Integer createServerCache() throws Exception {
    new EventIDVerificationDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);

    factory.setCacheListener(new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {

        synchronized (EventIDVerificationDUnitTest.class) {
          gotCallback = true;
          testEventIDResult = ((EntryEventImpl) event).getEventId().equals(eventId);
          EventIDVerificationDUnitTest.class.notify();
        }

      }

      @Override
      public void afterUpdate(EntryEvent event) {

        synchronized (EventIDVerificationDUnitTest.class) {
          gotCallback = true;
          testEventIDResult = ((EntryEventImpl) event).getEventId().equals(eventId);
          EventIDVerificationDUnitTest.class.notify();
        }

      }

      @Override
      public void afterDestroy(EntryEvent event) {

        synchronized (EventIDVerificationDUnitTest.class) {
          gotCallback = true;
          testEventIDResult = ((EntryEventImpl) event).getEventId().equals(eventId);
          EventIDVerificationDUnitTest.class.notify();
        }

      }

      @Override
      public void afterRegionDestroy(RegionEvent event) {

        synchronized (EventIDVerificationDUnitTest.class) {
          gotCallback = true;
          testEventIDResult = ((RegionEventImpl) event).getEventId().equals(eventId);
          EventIDVerificationDUnitTest.class.notify();
        }

      }

      @Override
      public void afterRegionClear(RegionEvent event) {
        synchronized (EventIDVerificationDUnitTest.class) {
          gotCallback = true;
          // verifyEventIDsDuringRegionDestroy(event);
          testEventIDResult = ((RegionEventImpl) event).getEventId().equals(eventId);
          if (!testEventIDResult) {
            cache.getLogger().warning("Expected " + eventId.expensiveToString() + ", but processed"
                + ((RegionEventImpl) event).getEventId().expensiveToString());
          }
          EventIDVerificationDUnitTest.class.notify();
        }
      }

    });

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    int port = getRandomAvailableTCPPort();
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());
  }

  public static void createEntry() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r);

      if (!r.containsKey("key-1")) {
        r.create("key-1", "key-1");
      }
      // Verify that no invalidates occurred to this region
      assertEquals(r.getEntry("key-1").getValue(), "key-1");
    } catch (Exception ex) {
      Assert.fail("failed while createEntries()", ex);
    }
  }

  public static void put() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r);

      r.put("key-1", "vm2-key-1");
      // Verify that no invalidates occurred to this region
      assertEquals(r.getEntry("key-1").getValue(), "vm2-key-1");

    } catch (Exception ex) {
      Assert.fail("failed while r.put()", ex);
    }
  }

  public static void destroy() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.destroy("key-1");
    } catch (Exception ex) {
      Assert.fail("test failed due to exception in destroy ", ex);
    }
  }


  public static void remove() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.remove("key-1");
    } catch (Exception ex) {
      Assert.fail("test failed due to exception in remove ", ex);
    }
  }


  public static void destroyRegion() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.destroyRegion();
    } catch (Exception ex) {
      Assert.fail("test failed due to exception in destroyRegion ", ex);
    }
  }

  public static void clearRegion() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.clear();
    } catch (Exception ex) {
      Assert.fail("test failed due to exception in clearRegion ", ex);
    }
  }

  public static Boolean verifyResult() {
    synchronized (EventIDVerificationDUnitTest.class) {
      if (!gotCallback) {
        try {
          EventIDVerificationDUnitTest.class.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          gotCallback = false;
          e.printStackTrace();
          return Boolean.FALSE;

        }
      }
      gotCallback = false;
    }
    boolean temp = testEventIDResult;
    testEventIDResult = false;
    return new Boolean(temp);

  }

  public static void verifyEventIDsDuringRegionDestroy(RegionEvent event) {
    assertEquals(eventId, ((RegionEventImpl) event).getEventId());
  }

  @Override
  public final void preTearDown() throws Exception {
    // close the clients first
    closeCache();
    // then close the servers
    vm0.invoke(() -> EventIDVerificationDUnitTest.closeCache());
    vm1.invoke(() -> EventIDVerificationDUnitTest.closeCache());
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}
