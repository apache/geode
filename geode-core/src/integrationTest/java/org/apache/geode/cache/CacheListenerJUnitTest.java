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
package org.apache.geode.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.RegionMembershipListenerAdapter;
import org.apache.geode.cache.util.RegionRoleListenerAdapter;
import org.apache.geode.cache.util.TransactionListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;

/**
 * Unit test for basic CacheListener feature. NOTE: these tests using a loner DistributedSystem and
 * local scope regions so all the listener features tested are for local listeners being invoked for
 * local operations.
 *
 * @since GemFire 5.0
 */
public class CacheListenerJUnitTest {

  private DistributedSystem ds;
  private Cache c;
  private int invokeCount;
  private CacheEvent lastEvent;

  @Before
  public void setUp() throws Exception {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0");
    p.setProperty(LOCATORS, "");
    ds = DistributedSystem.connect(p);
    c = CacheFactory.create(ds);
  }

  @After
  public void tearDown() throws Exception {
    if (c != null) {
      c.close();
      c = null;
    }
    if (ds != null) {
      ds.disconnect();
      ds = null;
    }
  }

  /**
   * Confirms that listeners are invoked in the correct order
   */
  @Test
  public void testInvocationOrder() throws Exception {
    CacheListener cl1 = new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {
        invokeCount++;
        assertEquals(1, invokeCount);
      }
    };
    CacheListener cl2 = new RegionMembershipListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {
        invokeCount++;
        assertEquals(2, invokeCount);
      }
    };
    CacheListener cl3 = new RegionRoleListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {
        invokeCount++;
        assertEquals(3, invokeCount);
      }
    };
    AttributesFactory af = new AttributesFactory();
    af.addCacheListener(cl1);
    af.addCacheListener(cl2);
    af.addCacheListener(cl3);
    Region r = c.createRegion("r", af.create());

    clearListener();
    r.create("key1", "value1");
    assertEquals(3, invokeCount);

    CacheListener cl4 = new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {
        invokeCount++;
        assertEquals(4, invokeCount);
      }
    };
    clearListener();
    r.getAttributesMutator().addCacheListener(cl4);
    r.create("key2", "value2");
    assertEquals(4, invokeCount);
  }

  /**
   * Tests the mutator CacheListener ops to make sure they do the correct thing
   */
  @Test
  public void testBasicMutator() throws Exception {
    CacheListener cl1 = new CacheListenerAdapter() {};
    CacheListener cl2 = new CacheListenerAdapter() {};
    // CacheListener cl3 = new CacheListenerAdapter() {};
    AttributesFactory af = new AttributesFactory();
    Region r = c.createRegion("r", af.create());
    RegionAttributes ra = r.getAttributes();
    AttributesMutator am = r.getAttributesMutator();
    assertEquals(null, ra.getCacheListener());
    assertEquals(Collections.EMPTY_LIST, Arrays.asList(ra.getCacheListeners()));
    try {
      am.addCacheListener(null);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException ignored) {
    }
    try {
      am.removeCacheListener(null);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException ignored) {
    }
    try {
      am.initCacheListeners(new CacheListener[] {cl1, null});
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException ignored) {
    }
    am.addCacheListener(cl1);
    assertEquals(cl1, ra.getCacheListener());
    assertEquals(Arrays.asList(cl1), Arrays.asList(ra.getCacheListeners()));

    am.addCacheListener(cl2);
    assertEquals(Arrays.asList(cl1, cl2),
        Arrays.asList(ra.getCacheListeners()));
    try {
      ra.getCacheListener();
      fail("expected IllegalStateException");
    } catch (IllegalStateException ignored) {
    }

    am.removeCacheListener(cl1);
    assertEquals(Arrays.asList(cl2), Arrays.asList(ra.getCacheListeners()));
    am.removeCacheListener(cl1);
    assertEquals(Arrays.asList(cl2), Arrays.asList(ra.getCacheListeners()));
    am.removeCacheListener(cl2);
    assertEquals(Arrays.asList(), Arrays.asList(ra.getCacheListeners()));
    am.initCacheListeners(new CacheListener[] {cl1, cl2});
    assertEquals(Arrays.asList(cl1, cl2),
        Arrays.asList(ra.getCacheListeners()));
    am.initCacheListeners(null);
    assertEquals(Arrays.asList(), Arrays.asList(ra.getCacheListeners()));
    am.initCacheListeners(new CacheListener[] {});
    assertEquals(Arrays.asList(), Arrays.asList(ra.getCacheListeners()));
  }

  private void clearListener() {
    invokeCount = 0;
    lastEvent = null;
  }

  /**
   * Tests the local afterRegionCreate event
   */
  @Test
  public void testAfterRegionCreate() throws Exception {
    CacheListener cl1 = new CacheListenerAdapter() {
      @Override
      public void afterRegionCreate(RegionEvent event) {
        invokeCount++;
        lastEvent = event;
      }
    };
    AttributesFactory af = new AttributesFactory();
    af.addCacheListener(cl1);
    clearListener();
    Region r = c.createRegion("r", af.create());
    assertEquals(1, invokeCount);
    assertTrue(lastEvent instanceof RegionEvent);
    CacheEvent e = lastEvent;
    assertEquals(r, e.getRegion());
    assertEquals(ds.getDistributedMember(), e.getDistributedMember());
    assertEquals(null, e.getCallbackArgument());
    assertEquals(Operation.REGION_CREATE, e.getOperation());
    assertEquals(false, ((RegionEvent) e).isReinitializing());
    assertEquals(false, e.isOriginRemote());
    assertEquals(false, e.getOperation().isExpiration());
    assertEquals(false, e.getOperation().isDistributed());
  }

  @Test
  public void testTxPutThatIsCreate() throws Exception {
    CacheListener cl1 = new CacheListenerAdapter() {
      @Override
      public void afterUpdate(EntryEvent e) {
        invokeCount = 2;
        lastEvent = e;
      }

      @Override
      public void afterCreate(EntryEvent e) {
        invokeCount = 1;
        lastEvent = e;
      }
    };
    AttributesFactory af = new AttributesFactory();
    af.addCacheListener(cl1);
    clearListener();
    Region r = c.createRegion("r", af.create());
    r.put("key1", "value1-0");
    assertEquals(1, invokeCount);
    assertEquals(Operation.CREATE, lastEvent.getOperation());

    clearListener();
    r.put("key1", "value1-1");
    assertEquals(2, invokeCount);
    assertEquals(Operation.UPDATE, lastEvent.getOperation());

    r.localDestroy("key1");

    // now try it with a transaction
    TransactionListener tl1 = new TransactionListenerAdapter() {
      @Override
      public void afterRollback(TransactionEvent e) {
        invokeCount = 1;
        assertEquals(1, e.getEvents().size());
        lastEvent = e.getEvents().get(0);
      }
    };
    CacheTransactionManager ctm = c.getCacheTransactionManager();
    ctm.addListener(tl1);

    ctm.begin();
    clearListener();
    r.put("key1", "value1-0");
    assertEquals(0, invokeCount);
    assertNull(lastEvent);

    clearListener();
    r.put("key1", "value1-1");
    assertEquals(0, invokeCount);
    assertNull(lastEvent);
    clearListener();
    ctm.rollback();
    assertEquals(1, invokeCount);
    assertEquals(Operation.CREATE, lastEvent.getOperation());

    ctm.begin();
    clearListener();
    r.put("key1", "value1-0");
    assertEquals(0, invokeCount);
    assertNull(lastEvent);

    clearListener();
    r.put("key1", "value1-1");
    assertEquals(0, invokeCount);
    assertNull(lastEvent);
    clearListener();
    ctm.commit();
    assertEquals(1, invokeCount);
    assertEquals(Operation.CREATE, lastEvent.getOperation());

  }

  @Test
  public void testTxOpOrder() throws Exception {
    AttributesFactory af = new AttributesFactory();
    clearListener();
    Region r = c.createRegion("r", af.create());

    TransactionListener tl1 = new TransactionListenerAdapter() {
      @Override
      public void afterRollback(TransactionEvent e) {
        assertEquals(3, e.getEvents().size());
        String[] keys = new String[] {(String) ((EntryEvent) e.getEvents().get(0)).getKey(),
            (String) ((EntryEvent) e.getEvents().get(1)).getKey(),
            (String) ((EntryEvent) e.getEvents().get(2)).getKey()};
        assertEquals(Arrays.asList("b", "c", "a"), Arrays.asList(keys));
        invokeCount = 1;
      }
    };
    CacheTransactionManager ctm = c.getCacheTransactionManager();
    ctm.addListener(tl1);

    ctm.begin();
    clearListener();
    r.put("b", "value1");
    r.put("c", "value2");
    r.put("a", "value3");
    ctm.rollback();
    assertEquals(1, invokeCount);
  }

  @Test
  public void testMultiRegionTxOpOrder() throws Exception {
    AttributesFactory af = new AttributesFactory();
    clearListener();
    Region r1 = c.createRegion("r1", af.create());
    Region r2 = r1.createSubregion("r2", af.create());
    Region r3 = r2.createSubregion("r3", af.create());

    TransactionListener tl1 = new TransactionListenerAdapter() {
      @Override
      public void afterCommit(TransactionEvent e) {
        assertEquals(3, e.getEvents().size());
        String[] keys = new String[] {(String) ((EntryEvent) e.getEvents().get(0)).getKey(),
            (String) ((EntryEvent) e.getEvents().get(1)).getKey(),
            (String) ((EntryEvent) e.getEvents().get(2)).getKey()};
        assertEquals(Arrays.asList("b", "c", "a"), Arrays.asList(keys));
        invokeCount = 1;
      }
    };
    CacheTransactionManager ctm = c.getCacheTransactionManager();
    ctm.addListener(tl1);

    ctm.begin();
    clearListener();
    r2.put("b", "value1");
    r3.put("c", "value2");
    r1.put("a", "value3");
    ctm.commit();
    assertEquals(1, invokeCount);
  }
}
