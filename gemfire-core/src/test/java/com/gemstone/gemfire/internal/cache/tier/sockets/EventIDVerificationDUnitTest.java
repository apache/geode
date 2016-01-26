/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheObserverAdapter;
import com.gemstone.gemfire.internal.cache.CacheObserverHolder;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.RegionEventImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Test to verify EventID generated from Cache Client is correctly passed on to
 * the cache server for create, update, remove and destroy operations.It also checks
 * that peer nodes also get the same EventID.
 *
 * @author Suyog Bhokare
 * @author Asif
 *
 */

public class EventIDVerificationDUnitTest extends DistributedTestCase
{
  private static Cache cache = null;

  static VM vm0 = null;

  static VM vm1 = null;

  private static int PORT1;

  private static int PORT2;

  private static final String REGION_NAME = "EventIDVerificationDUnitTest_region";

  protected static EventID eventId;

  static boolean gotCallback = false;

  static boolean testEventIDResult = false;

  /* Constructor */

  public EventIDVerificationDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception
  {
    super.setUp();
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);

    //start servers first
    PORT1 = ((Integer)vm0.invoke(EventIDVerificationDUnitTest.class,
        "createServerCache")).intValue();
    PORT2 = ((Integer)vm1.invoke(EventIDVerificationDUnitTest.class,
        "createServerCache")).intValue();

    //vm2.invoke(EventIDVerificationDUnitTest.class, "createClientCache", new
    // Object[] { new Integer(PORT1),new Integer(PORT2)});
    createClientCache(getServerHostName(host), new Integer(PORT1), new Integer(PORT2));
    CacheObserverHolder.setInstance(new CacheObserverAdapter());

  }

  public void testEventIDOnServer()
  {
    createEntry();
    Boolean pass = (Boolean)vm0.invoke(EventIDVerificationDUnitTest.class,
        "verifyResult");
    assertTrue(pass.booleanValue());
    put();
    pass = (Boolean)vm0.invoke(EventIDVerificationDUnitTest.class,
        "verifyResult");
    assertTrue(pass.booleanValue());
    destroy();
    pass = (Boolean)vm0.invoke(EventIDVerificationDUnitTest.class,
        "verifyResult");
    assertTrue(pass.booleanValue());

    put();
    cache.getLogger().info("going to remove");
    remove();
    cache.getLogger().info("after remove");
    pass = (Boolean)vm0.invoke(EventIDVerificationDUnitTest.class,
        "verifyResult");
    assertTrue(pass.booleanValue());
  }

  /**
   * Verify that EventId is prapogated to server in case of region destroy. It
   * also checks that peer nodes also get the same EventID.
   *
   */
  public void testEventIDPrapogationOnServerDuringRegionDestroy()
  {
    destroyRegion();
    Boolean pass = (Boolean)vm0.invoke(EventIDVerificationDUnitTest.class,
    "verifyResult");
    assertTrue(pass.booleanValue());
    pass = (Boolean)vm1.invoke(EventIDVerificationDUnitTest.class,
    "verifyResult");
    assertTrue(pass.booleanValue());
  }

  /**
   * Verify that EventId is prapogated to server in case of region clear. It
   * also checks that peer nodes also get the same EventID.

   */
  public void testEventIDPrapogationOnServerDuringRegionClear()
  {
    clearRegion();
    Boolean pass = (Boolean)vm0.invoke(EventIDVerificationDUnitTest.class,
    "verifyResult");
    assertTrue(pass.booleanValue());
    pass = (Boolean)vm1.invoke(EventIDVerificationDUnitTest.class,
    "verifyResult");
    assertTrue(pass.booleanValue());
  }


  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientCache(String host, Integer port1, Integer port2)
      throws Exception
  {
    PORT1 = port1.intValue();
    PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new EventIDVerificationDUnitTest("temp").createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setMirrorType(MirrorType.NONE);
    
    ClientServerTestCase.configureConnectionPool(factory, host, new int[] {PORT1,PORT2}, true, -1, 2, null, -1, -1, false, -2);

    
    CacheWriter writer = new CacheWriterAdapter() {
      public void beforeCreate(EntryEvent event)
      {
        vm0.invoke(EventIDVerificationDUnitTest.class, "setEventIDData",
            new Object[] { ((EntryEventImpl)event).getEventId() });
        vm1.invoke(EventIDVerificationDUnitTest.class, "setEventIDData",
            new Object[] { ((EntryEventImpl)event).getEventId() });
        try {
          super.beforeCreate(event);
        }
        catch (CacheWriterException e) {
          e.printStackTrace();
          fail("Test failed bcoz of exception =" + e);
        }
      }
      

      public void beforeUpdate(EntryEvent event)
      {

        vm0.invoke(EventIDVerificationDUnitTest.class, "setEventIDData",
            new Object[] { ((EntryEventImpl)event).getEventId() });
        vm1.invoke(EventIDVerificationDUnitTest.class, "setEventIDData",
            new Object[] { ((EntryEventImpl)event).getEventId() });
        try {
          super.beforeUpdate(event);
        }
        catch (CacheWriterException e) {
          e.printStackTrace();
          fail("Test failed bcoz of exception =" + e);
        }

      }

      public void beforeDestroy(EntryEvent event)
      {

        vm0.invoke(EventIDVerificationDUnitTest.class, "setEventIDData",
            new Object[] { ((EntryEventImpl)event).getEventId() });
        vm1.invoke(EventIDVerificationDUnitTest.class, "setEventIDData",
            new Object[] { ((EntryEventImpl)event).getEventId() });
        try {
          super.beforeDestroy(event);
        }
        catch (CacheWriterException e) {
          e.printStackTrace();
          fail("Test failed bcoz of exception =" + e);
        }

      }

      public void beforeRegionDestroy(RegionEvent event)
      {

        vm0.invoke(EventIDVerificationDUnitTest.class, "setEventIDData",
            new Object[] { ((RegionEventImpl)event).getEventId() });
        vm1.invoke(EventIDVerificationDUnitTest.class, "setEventIDData",
            new Object[] { ((RegionEventImpl)event).getEventId() });
        try {
          super.beforeRegionDestroy(event);
        }
        catch (CacheWriterException e) {
          e.printStackTrace();
          fail("Test failed bcoz of exception =" + e);
        }

      }

      public void beforeRegionClear(RegionEvent event)
      {
        vm0.invoke(EventIDVerificationDUnitTest.class, "setEventIDData",
            new Object[] { ((RegionEventImpl)event).getEventId() });
        vm1.invoke(EventIDVerificationDUnitTest.class, "setEventIDData",
            new Object[] { ((RegionEventImpl)event).getEventId() });
        try {
          super.beforeRegionClear(event);
        }
        catch (CacheWriterException e) {
          e.printStackTrace();
          fail("Test failed bcoz of exception =" + e);
        }
      }
    };
    factory.setCacheWriter(writer);
    /*
     * factory.setCacheListener(new CacheListenerAdapter() { public void
     * afterCreate(EntryEvent event) { synchronized (this) { threadId =
     * ((EntryEventImpl)event).getEventId().getThreadID(); membershipId =
     * ((EntryEventImpl)event).getEventId().getMembershipID();
     *  } }
     *
     * public void afterUpdate(EntryEvent event) { synchronized (this) {
     * verifyEventIDs(event); } }
     *
     * public void afterDestroy(EntryEvent event) { synchronized (this) {
     * verifyEventIDs(event); } } public void afterRegionDestroy(RegionEvent
     * event) { synchronized (this) { threadId =
     * ((RegionEventImpl)event).getEventId().getThreadID(); membershipId =
     * ((RegionEventImpl)event).getEventId().getMembershipID(); } } });
     */

    RegionAttributes attrs = factory.create();
    Region r = cache.createRegion(REGION_NAME, attrs);
    r.registerInterest("ALL_KEYS");
  }

  public static void setEventIDData(Object evID)
  {
    eventId = (EventID)evID;

  }

  public static Integer createServerCache() throws Exception
  {
    new EventIDVerificationDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);

    factory.setCacheListener(new CacheListenerAdapter() {
      public void afterCreate(EntryEvent event)
      {

        synchronized (EventIDVerificationDUnitTest.class) {
          gotCallback = true;
          testEventIDResult = ((EntryEventImpl)event).getEventId().equals(
              eventId);
          EventIDVerificationDUnitTest.class.notify();
        }

      }

      public void afterUpdate(EntryEvent event)
      {

        synchronized (EventIDVerificationDUnitTest.class) {
          gotCallback = true;
          testEventIDResult = ((EntryEventImpl)event).getEventId().equals(
              eventId);
          EventIDVerificationDUnitTest.class.notify();
        }

      }

      public void afterDestroy(EntryEvent event)
      {

        synchronized (EventIDVerificationDUnitTest.class) {
          gotCallback = true;
          testEventIDResult = ((EntryEventImpl)event).getEventId().equals(
              eventId);
          EventIDVerificationDUnitTest.class.notify();
        }

      }

      public void afterRegionDestroy(RegionEvent event)
      {

        synchronized (EventIDVerificationDUnitTest.class) {
          gotCallback = true;
          testEventIDResult = ((RegionEventImpl)event).getEventId().equals(
              eventId);
          EventIDVerificationDUnitTest.class.notify();
        }

      }

      public void afterRegionClear(RegionEvent event) {
        synchronized (EventIDVerificationDUnitTest.class) {
          gotCallback = true;
          //verifyEventIDsDuringRegionDestroy(event);
          testEventIDResult = ((RegionEventImpl)event).getEventId().equals(
              eventId);
          EventIDVerificationDUnitTest.class.notify();
        }
      }

    });

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());
  }

  public static void createEntry()
  {
    try {
      Region r = cache.getRegion("/" + REGION_NAME);
      assertNotNull(r);

      if (!r.containsKey("key-1")) {
        r.create("key-1", "key-1");
      }
      // Verify that no invalidates occurred to this region
      assertEquals(r.getEntry("key-1").getValue(), "key-1");
    }
    catch (Exception ex) {
      fail("failed while createEntries()", ex);
    }
  }

  public static void put()
  {
    try {
      Region r = cache.getRegion("/" + REGION_NAME);
      assertNotNull(r);

      r.put("key-1", "vm2-key-1");
      // Verify that no invalidates occurred to this region
      assertEquals(r.getEntry("key-1").getValue(), "vm2-key-1");

    }
    catch (Exception ex) {
      fail("failed while r.put()", ex);
    }
  }

  public static void destroy()
  {
    try {
      Region r = cache.getRegion("/" + REGION_NAME);
      assertNotNull(r);
      r.destroy("key-1");
    }
    catch (Exception ex) {
      fail("test failed due to exception in destroy ", ex);
    }
  }


  public static void remove()
  {
    try {
      Region r = cache.getRegion("/" + REGION_NAME);
      assertNotNull(r);
      r.remove("key-1");
    }
    catch (Exception ex) {
      fail("test failed due to exception in remove ", ex);
    }
  }


  public static void destroyRegion()
  {
    try {
      Region r = cache.getRegion("/" + REGION_NAME);
      assertNotNull(r);
      r.destroyRegion();
    }
    catch (Exception ex) {
      fail("test failed due to exception in destroyRegion ", ex);
    }
  }

  public static void clearRegion()
  {
    try {
      Region r = cache.getRegion("/" + REGION_NAME);
      assertNotNull(r);
      r.clear();
    }
    catch (Exception ex) {
      fail("test failed due to exception in clearRegion ", ex);
    }
  }

  public static Boolean verifyResult()
  {
    synchronized (EventIDVerificationDUnitTest.class) {
      if (!gotCallback) {
        try {
          EventIDVerificationDUnitTest.class.wait();
        }
        catch (InterruptedException e) {
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

  public static void verifyEventIDsDuringRegionDestroy(RegionEvent event)
  {
    assertEquals(eventId, ((RegionEventImpl)event).getEventId());
  }

  public void tearDown2() throws Exception
  {
    super.tearDown2();
    // close the clients first
    closeCache();
    // then close the servers
    vm0.invoke(EventIDVerificationDUnitTest.class, "closeCache");
    vm1.invoke(EventIDVerificationDUnitTest.class, "closeCache");
  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}

