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
    this.ds = DistributedSystem.connect(p);
    this.c = CacheFactory.create(this.ds);
  }

  @After
  public void tearDown() throws Exception {
    if (this.c != null) {
      this.c.close();
      this.c = null;
    }
    if (this.ds != null) {
      this.ds.disconnect();
      this.ds = null;
    }
  }

  /**
   * Confirms that listeners are invoked in the correct order
   */
  @Test
  public void testInvocationOrder() throws Exception {
    CacheListener cl1 = new CacheListenerAdapter() {
      public void afterCreate(EntryEvent event) {
        CacheListenerJUnitTest.this.invokeCount++;
        assertEquals(1, CacheListenerJUnitTest.this.invokeCount);
      }
    };
    CacheListener cl2 = new RegionMembershipListenerAdapter() {
      public void afterCreate(EntryEvent event) {
        CacheListenerJUnitTest.this.invokeCount++;
        assertEquals(2, CacheListenerJUnitTest.this.invokeCount);
      }
    };
    CacheListener cl3 = new RegionRoleListenerAdapter() {
      public void afterCreate(EntryEvent event) {
        CacheListenerJUnitTest.this.invokeCount++;
        assertEquals(3, CacheListenerJUnitTest.this.invokeCount);
      }
    };
    AttributesFactory af = new AttributesFactory();
    af.addCacheListener(cl1);
    af.addCacheListener(cl2);
    af.addCacheListener(cl3);
    Region r = this.c.createRegion("r", af.create());

    clearListener();
    r.create("key1", "value1");
    assertEquals(3, this.invokeCount);

    CacheListener cl4 = new CacheListenerAdapter() {
      public void afterCreate(EntryEvent event) {
        CacheListenerJUnitTest.this.invokeCount++;
        assertEquals(4, CacheListenerJUnitTest.this.invokeCount);
      }
    };
    clearListener();
    r.getAttributesMutator().addCacheListener(cl4);
    r.create("key2", "value2");
    assertEquals(4, this.invokeCount);
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
    Region r = this.c.createRegion("r", af.create());
    RegionAttributes ra = r.getAttributes();
    AttributesMutator am = r.getAttributesMutator();
    assertEquals(null, ra.getCacheListener());
    assertEquals(Collections.EMPTY_LIST, Arrays.asList(ra.getCacheListeners()));
    try {
      am.addCacheListener(null);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      am.removeCacheListener(null);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      am.initCacheListeners(new CacheListener[] {cl1, null});
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    am.addCacheListener(cl1);
    assertEquals(cl1, ra.getCacheListener());
    assertEquals(Arrays.asList(new CacheListener[] {cl1}), Arrays.asList(ra.getCacheListeners()));

    am.addCacheListener(cl2);
    assertEquals(Arrays.asList(new CacheListener[] {cl1, cl2}),
        Arrays.asList(ra.getCacheListeners()));
    try {
      ra.getCacheListener();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }

    am.removeCacheListener(cl1);
    assertEquals(Arrays.asList(new CacheListener[] {cl2}), Arrays.asList(ra.getCacheListeners()));
    am.removeCacheListener(cl1);
    assertEquals(Arrays.asList(new CacheListener[] {cl2}), Arrays.asList(ra.getCacheListeners()));
    am.removeCacheListener(cl2);
    assertEquals(Arrays.asList(new CacheListener[] {}), Arrays.asList(ra.getCacheListeners()));
    am.initCacheListeners(new CacheListener[] {cl1, cl2});
    assertEquals(Arrays.asList(new CacheListener[] {cl1, cl2}),
        Arrays.asList(ra.getCacheListeners()));
    am.initCacheListeners(null);
    assertEquals(Arrays.asList(new CacheListener[] {}), Arrays.asList(ra.getCacheListeners()));
    am.initCacheListeners(new CacheListener[] {});
    assertEquals(Arrays.asList(new CacheListener[] {}), Arrays.asList(ra.getCacheListeners()));
  }

  private void clearListener() {
    this.invokeCount = 0;
    this.lastEvent = null;
  }

  /**
   * Tests the local afterRegionCreate event
   */
  @Test
  public void testAfterRegionCreate() throws Exception {
    CacheListener cl1 = new CacheListenerAdapter() {
      public void afterRegionCreate(RegionEvent event) {
        CacheListenerJUnitTest.this.invokeCount++;
        CacheListenerJUnitTest.this.lastEvent = event;
      }
    };
    AttributesFactory af = new AttributesFactory();
    af.addCacheListener(cl1);
    clearListener();
    Region r = this.c.createRegion("r", af.create());
    assertEquals(1, this.invokeCount);
    assertTrue(this.lastEvent instanceof RegionEvent);
    CacheEvent e = this.lastEvent;
    assertEquals(r, e.getRegion());
    assertEquals(this.ds.getDistributedMember(), e.getDistributedMember());
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
      public void afterUpdate(EntryEvent e) {
        CacheListenerJUnitTest.this.invokeCount = 2;
        CacheListenerJUnitTest.this.lastEvent = e;
      }

      public void afterCreate(EntryEvent e) {
        CacheListenerJUnitTest.this.invokeCount = 1;
        CacheListenerJUnitTest.this.lastEvent = e;
      }
    };
    AttributesFactory af = new AttributesFactory();
    af.addCacheListener(cl1);
    clearListener();
    Region r = this.c.createRegion("r", af.create());
    r.put("key1", "value1-0");
    assertEquals(1, this.invokeCount);
    assertEquals(Operation.CREATE, this.lastEvent.getOperation());

    clearListener();
    r.put("key1", "value1-1");
    assertEquals(2, this.invokeCount);
    assertEquals(Operation.UPDATE, this.lastEvent.getOperation());

    r.localDestroy("key1");

    // now try it with a transaction
    TransactionListener tl1 = new TransactionListenerAdapter() {
      public void afterRollback(TransactionEvent e) {
        CacheListenerJUnitTest.this.invokeCount = 1;
        assertEquals(1, e.getEvents().size());
        CacheListenerJUnitTest.this.lastEvent = (CacheEvent) e.getEvents().get(0);
      }
    };
    CacheTransactionManager ctm = this.c.getCacheTransactionManager();
    ctm.addListener(tl1);

    ctm.begin();
    clearListener();
    r.put("key1", "value1-0");
    assertEquals(0, this.invokeCount);
    assertNull(this.lastEvent);

    clearListener();
    r.put("key1", "value1-1");
    assertEquals(0, this.invokeCount);
    assertNull(this.lastEvent);
    clearListener();
    ctm.rollback();
    assertEquals(1, this.invokeCount);
    assertEquals(Operation.CREATE, this.lastEvent.getOperation());

    ctm.begin();
    clearListener();
    r.put("key1", "value1-0");
    assertEquals(0, this.invokeCount);
    assertNull(this.lastEvent);

    clearListener();
    r.put("key1", "value1-1");
    assertEquals(0, this.invokeCount);
    assertNull(this.lastEvent);
    clearListener();
    ctm.commit();
    assertEquals(1, this.invokeCount);
    assertEquals(Operation.CREATE, this.lastEvent.getOperation());

  }

  @Test
  public void testTxOpOrder() throws Exception {
    AttributesFactory af = new AttributesFactory();
    clearListener();
    Region r = this.c.createRegion("r", af.create());

    TransactionListener tl1 = new TransactionListenerAdapter() {
      public void afterRollback(TransactionEvent e) {
        assertEquals(3, e.getEvents().size());
        String[] keys = new String[] {(String) ((EntryEvent) e.getEvents().get(0)).getKey(),
            (String) ((EntryEvent) e.getEvents().get(1)).getKey(),
            (String) ((EntryEvent) e.getEvents().get(2)).getKey()};
        assertEquals(Arrays.asList(new String[] {"b", "c", "a"}), Arrays.asList(keys));
        CacheListenerJUnitTest.this.invokeCount = 1;
      }
    };
    CacheTransactionManager ctm = this.c.getCacheTransactionManager();
    ctm.addListener(tl1);

    ctm.begin();
    clearListener();
    r.put("b", "value1");
    r.put("c", "value2");
    r.put("a", "value3");
    ctm.rollback();
    assertEquals(1, this.invokeCount);
  }

  @Test
  public void testMultiRegionTxOpOrder() throws Exception {
    AttributesFactory af = new AttributesFactory();
    clearListener();
    Region r1 = this.c.createRegion("r1", af.create());
    Region r2 = r1.createSubregion("r2", af.create());
    Region r3 = r2.createSubregion("r3", af.create());

    TransactionListener tl1 = new TransactionListenerAdapter() {
      public void afterCommit(TransactionEvent e) {
        assertEquals(3, e.getEvents().size());
        String[] keys = new String[] {(String) ((EntryEvent) e.getEvents().get(0)).getKey(),
            (String) ((EntryEvent) e.getEvents().get(1)).getKey(),
            (String) ((EntryEvent) e.getEvents().get(2)).getKey()};
        assertEquals(Arrays.asList(new String[] {"b", "c", "a"}), Arrays.asList(keys));
        CacheListenerJUnitTest.this.invokeCount = 1;
      }
    };
    CacheTransactionManager ctm = this.c.getCacheTransactionManager();
    ctm.addListener(tl1);

    ctm.begin();
    clearListener();
    r2.put("b", "value1");
    r3.put("c", "value2");
    r1.put("a", "value3");
    ctm.commit();
    assertEquals(1, this.invokeCount);
  }
}
