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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCacheEvent;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Test to verify EventID generated from a peer is correctly passed on to the other peer for create,
 * update and destroy operations. In case of D-ACK or GLOBAL scope the EventIDs should be same in
 * P2P for a propagation of given operation. In case of NO-ACK EventIDs should be
 * different.Currently this test is commented because of a bug.
 */
@Category({ClientServerTest.class})
public class EventIDVerificationInP2PDUnitTest extends JUnit4DistributedTestCase {

  private static Cache cache = null;

  static VM vm0 = null;

  private static final String REGION_NAME =
      EventIDVerificationInP2PDUnitTest.class.getSimpleName() + "_region";

  protected static EventID eventId;

  static boolean receiver = true;

  static boolean gotCallback = false;

  static int DISTRIBUTED_ACK = 1;

  static int GLOBAL = 2;

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    receiver = false;
  }

  @Test
  public void testEventIDsDACK() throws Exception {
    createServerCache(new Integer(DISTRIBUTED_ACK));
    vm0.invoke(
        () -> EventIDVerificationInP2PDUnitTest.createServerCache(new Integer(DISTRIBUTED_ACK)));
    verifyOperations();
  }

  @Test
  public void testEventIDsGLOBAL() throws Exception {
    createServerCache(new Integer(GLOBAL));
    vm0.invoke(() -> EventIDVerificationInP2PDUnitTest.createServerCache(new Integer(GLOBAL)));
    verifyOperations();
  }

  @Ignore("TODO")
  @Test
  public void testEventIDsNOACK() throws Exception {
    createServerCache(new Integer(0));
    vm0.invoke(() -> EventIDVerificationInP2PDUnitTest.createServerCache(new Integer(0)));

    createEntry();
    Boolean pass =
        vm0.invoke(() -> EventIDVerificationInP2PDUnitTest.verifyResult(eventId));
    assertFalse(pass.booleanValue());
    put();
    pass = vm0.invoke(() -> EventIDVerificationInP2PDUnitTest.verifyResult(eventId));
    assertFalse(pass.booleanValue());
    destroy();
    pass = vm0.invoke(() -> EventIDVerificationInP2PDUnitTest.verifyResult(eventId));
    assertFalse(pass.booleanValue());
    destroyRegion();
    pass = vm0.invoke(() -> EventIDVerificationInP2PDUnitTest.verifyResult(eventId));
    assertFalse(pass.booleanValue());
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createServerCache(Integer type) throws Exception {
    new EventIDVerificationInP2PDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    if (type.intValue() == DISTRIBUTED_ACK) {
      factory.setScope(Scope.DISTRIBUTED_ACK);
    }
    if (type.intValue() == GLOBAL) {
      factory.setScope(Scope.GLOBAL);
    } else {
      factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    }

    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.addCacheListener(new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {

        eventId = ((InternalCacheEvent) event).getEventId();
        if (receiver) {
          synchronized (EventIDVerificationInP2PDUnitTest.class) {
            gotCallback = true;
            EventIDVerificationInP2PDUnitTest.class.notify();
          }
        }
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        eventId = ((InternalCacheEvent) event).getEventId();
        if (receiver) {
          synchronized (EventIDVerificationInP2PDUnitTest.class) {
            gotCallback = true;
            EventIDVerificationInP2PDUnitTest.class.notify();
          }
        }
      }

      @Override
      public void afterDestroy(EntryEvent event) {
        eventId = ((InternalCacheEvent) event).getEventId();
        if (receiver) {
          synchronized (EventIDVerificationInP2PDUnitTest.class) {
            gotCallback = true;
            EventIDVerificationInP2PDUnitTest.class.notify();
          }
        }
      }

      @Override
      public void afterRegionDestroy(RegionEvent event) {
        eventId = ((InternalCacheEvent) event).getEventId();
        if (receiver) {
          synchronized (EventIDVerificationInP2PDUnitTest.class) {
            gotCallback = true;
            EventIDVerificationInP2PDUnitTest.class.notify();
          }
        }
      }

      @Override
      public void afterRegionInvalidate(RegionEvent event) {
        eventId = ((InternalCacheEvent) event).getEventId();
        if (receiver) {
          synchronized (EventIDVerificationInP2PDUnitTest.class) {
            gotCallback = true;
            EventIDVerificationInP2PDUnitTest.class.notify();
          }
        }
      }
    });

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

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

      r.put("key-1", "vm0-key-1");
      // Verify that no invalidates occurred to this region
      assertEquals(r.getEntry("key-1").getValue(), "vm0-key-1");

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

  public static void destroyRegion() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.destroyRegion();
    } catch (Exception ex) {
      Assert.fail("test failed due to exception in destroyRegion ", ex);
    }
  }

  public static void invalidateRegion() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.invalidateRegion();
    } catch (Exception ex) {
      Assert.fail("test failed due to exception in invalidateRegion ", ex);
    }
  }

  public static Boolean verifyResult(EventID correctId) {
    synchronized (EventIDVerificationInP2PDUnitTest.class) {
      if (!gotCallback) {
        try {
          EventIDVerificationInP2PDUnitTest.class.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          gotCallback = false;
          e.printStackTrace();
          return Boolean.FALSE;
        }
      }
      gotCallback = false;
    }
    boolean temp = correctId.equals(eventId);
    return new Boolean(temp);
  }


  public static void verifyOperations() {
    createEntry();
    Boolean pass;
    verifyEventID();
    put();
    verifyEventID();
    invalidateRegion();
    verifyEventID();
    destroy();
    verifyEventID();
    destroyRegion();
    verifyEventID();
  }

  protected static void verifyEventID() {
    Boolean pass;
    EventID eventId = EventIDVerificationInP2PDUnitTest.eventId;
    pass = vm0.invoke(() -> EventIDVerificationInP2PDUnitTest.verifyResult(eventId));
    assertTrue(pass.booleanValue());
  }

  @Override
  public final void preTearDown() throws Exception {
    closeCache();
    vm0.invoke(EventIDVerificationInP2PDUnitTest::closeCache);
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}
