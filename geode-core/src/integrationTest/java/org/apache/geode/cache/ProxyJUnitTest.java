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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.TransactionListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;

/**
 * Unit test for basic DataPolicy.EMPTY feature. NOTE: these tests using a loner DistributedSystem
 * and local scope regions
 *
 * @since GemFire 5.0
 */
public class ProxyJUnitTest {

  private DistributedSystem ds;
  private Cache c;

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
    System.clearProperty(LocalRegion.EXPIRY_MS_PROPERTY);
    if (this.c != null) {
      this.c.close();
      this.c = null;
    }
    if (this.ds != null) {
      this.ds.disconnect();
      this.ds = null;
    }
  }

  private CachePerfStats getStats() {
    return ((GemFireCacheImpl) this.c).getCachePerfStats();
  }

  /**
   * last event a cache listener saw
   */
  private CacheEvent clLastEvent;
  /**
   * number of cache listener invocations
   */
  private int clInvokeCount;
  /**
   * true if cache listener close called
   */
  private boolean clClosed;
  /**
   * last event a cache writer saw
   */
  private CacheEvent cwLastEvent;
  /**
   * number of cache writer invocations
   */
  private int cwInvokeCount;
  /**
   * true if cache writer close called
   */
  private boolean cwClosed;

  /**
   * last getEvents() a transaction listener saw
   */
  private List tlLastEvents;
  /**
   * number of transaction listener invocations
   */
  private int tlInvokeCount;
  /**
   * true if transaction listener close called
   */
  private boolean tlClosed;

  /**
   * Clears the all the callback state this test has received.
   */
  private void clearCallbackState() {
    this.clLastEvent = null;
    this.clInvokeCount = 0;
    this.clClosed = false;
    this.cwLastEvent = null;
    this.cwInvokeCount = 0;
    this.cwClosed = false;
    this.tlLastEvents = null;
    this.tlInvokeCount = 0;
    this.tlClosed = false;
  }

  /**
   * Used to check to see if CacheEvent was what was expected
   */
  private abstract class ExpectedCacheEvent implements CacheEvent {
    public Region r;
    public Operation op;
    public Object cbArg;
    public boolean queued;

    public void check(CacheEvent other) {
      if (getRegion() != other.getRegion()) {
        fail("wrong region. Expected " + getRegion() + " but found " + other.getRegion());
      }
      assertEquals(getOperation(), other.getOperation());
      assertEquals(getCallbackArgument(), other.getCallbackArgument());
      assertEquals(isOriginRemote(), other.isOriginRemote());
      assertEquals(getDistributedMember(), other.getDistributedMember());
      assertEquals(isExpiration(), other.getOperation().isExpiration());
      assertEquals(isDistributed(), other.getOperation().isDistributed());
    }

    public Region getRegion() {
      return this.r;
    }

    public Operation getOperation() {
      return this.op;
    }

    public Object getCallbackArgument() {
      return this.cbArg;
    }

    public boolean isCallbackArgumentAvailable() {
      return true;
    }

    public boolean isOriginRemote() {
      return false;
    }

    public DistributedMember getDistributedMember() {
      return c.getDistributedSystem().getDistributedMember();
    }

    public boolean isExpiration() {
      return this.op.isExpiration();
    }

    public boolean isDistributed() {
      return this.op.isDistributed();
    }
  }
  /**
   * Used to check to see if EntryEvent was what was expected
   */
  private class ExpectedEntryEvent extends ExpectedCacheEvent implements EntryEvent {
    public void check(EntryEvent other) {
      super.check(other);
      assertEquals(getKey(), other.getKey());
      assertEquals(getOldValue(), other.getOldValue());
      assertEquals(getNewValue(), other.getNewValue());
      assertEquals(isLocalLoad(), other.getOperation().isLocalLoad());
      assertEquals(isNetLoad(), other.getOperation().isNetLoad());
      assertEquals(isLoad(), other.getOperation().isLoad());
      assertEquals(isNetSearch(), other.getOperation().isNetSearch());
      assertEquals(getTransactionId(), other.getTransactionId());
    }

    public Object key;

    public Object getKey() {
      return this.key;
    }

    public Object getOldValue() {
      return null;
    }

    public boolean isOldValueAvailable() {
      return true;
    }

    public Object newValue;

    public Object getNewValue() {
      return this.newValue;
    }

    public boolean isLocalLoad() {
      return getOperation().isLocalLoad();
    }

    public boolean isNetLoad() {
      return getOperation().isNetLoad();
    }

    public boolean isLoad() {
      return getOperation().isLoad();
    }

    public boolean isNetSearch() {
      return getOperation().isNetSearch();
    }

    public TransactionId txId;

    public TransactionId getTransactionId() {
      return this.txId;
    }

    public boolean isBridgeEvent() {
      return hasClientOrigin();
    }

    public boolean hasClientOrigin() {
      return false;
    }

    public ClientProxyMembershipID getContext() {
      // TODO Auto-generated method stub
      return null;
    }

    public SerializedCacheValue getSerializedOldValue() {
      return null;
    }

    public SerializedCacheValue getSerializedNewValue() {
      return null;
    }
  }
  /**
   * Used to check to see if EntryEvent was what was expected
   */
  private class ExpectedRegionEvent extends ExpectedCacheEvent implements RegionEvent {
    public void check(RegionEvent other) {
      super.check(other);
      assertEquals(isReinitializing(), other.isReinitializing());
    }

    public boolean isReinitializing() {
      return false;
    }
  }

  private void checkCWClosed() {
    assertEquals(true, this.cwClosed);
  }

  private void checkCLClosed() {
    assertEquals(true, this.clClosed);
  }

  private void checkTLClosed() {
    assertEquals(true, this.tlClosed);
  }

  private void checkNoCW() {
    assertEquals(0, this.cwInvokeCount);
  }

  private void checkNoCL() {
    assertEquals(0, this.clInvokeCount);
  }

  private void checkNoTL() {
    assertEquals(0, this.tlInvokeCount);
  }

  private void checkTL(ExpectedCacheEvent expected) {
    assertEquals(1, this.tlInvokeCount);
    assertEquals(1, this.tlLastEvents.size());
    {
      Object old_CA = expected.cbArg;
      // expected.cbArg = null;
      try {
        expected.check((CacheEvent) this.tlLastEvents.get(0));
      } finally {
        expected.cbArg = old_CA;
      }
    }
    checkNoCW();
    // checkNoCL();
    clearCallbackState();
  }

  private void checkCW(ExpectedCacheEvent expected) {
    assertEquals(1, this.cwInvokeCount);
    expected.check(this.cwLastEvent);
  }

  private void checkCL(ExpectedCacheEvent expected) {
    checkCL(expected, true);
  }

  private void checkCL(ExpectedCacheEvent expected, boolean clearCallbackState) {
    assertEquals(1, this.clInvokeCount);
    expected.check(this.clLastEvent);
    if (clearCallbackState) {
      clearCallbackState();
    }
  }

  private void setCallbacks(AttributesFactory af) {
    CacheListener cl1 = new CacheListener() {
      public void afterUpdate(EntryEvent e) {
        clLastEvent = e;
        clInvokeCount++;
      }

      public void afterCreate(EntryEvent e) {
        clLastEvent = e;
        clInvokeCount++;
      }

      public void afterInvalidate(EntryEvent e) {
        clLastEvent = e;
        clInvokeCount++;
      }

      public void afterDestroy(EntryEvent e) {
        clLastEvent = e;
        clInvokeCount++;
      }

      public void afterRegionInvalidate(RegionEvent e) {
        clLastEvent = e;
        clInvokeCount++;
      }

      public void afterRegionDestroy(RegionEvent e) {
        clLastEvent = e;
        clInvokeCount++;
      }

      public void afterRegionClear(RegionEvent e) {
        clLastEvent = e;
        clInvokeCount++;
      }

      public void afterRegionCreate(RegionEvent e) {
        clLastEvent = e;
        clInvokeCount++;
      }

      public void afterRegionLive(RegionEvent e) {
        clLastEvent = e;
        clInvokeCount++;
      }

      public void close() {
        clClosed = true;
      }
    };
    CacheWriter cw = new CacheWriter() {
      public void beforeUpdate(EntryEvent e) throws CacheWriterException {
        cwLastEvent = e;
        cwInvokeCount++;
      }

      public void beforeCreate(EntryEvent e) throws CacheWriterException {
        cwLastEvent = e;
        cwInvokeCount++;
      }

      public void beforeDestroy(EntryEvent e) throws CacheWriterException {
        cwLastEvent = e;
        cwInvokeCount++;
      }

      public void beforeRegionDestroy(RegionEvent e) throws CacheWriterException {
        cwLastEvent = e;
        cwInvokeCount++;
      }

      public void beforeRegionClear(RegionEvent e) throws CacheWriterException {
        cwLastEvent = e;
        cwInvokeCount++;
      }

      public void close() {
        cwClosed = true;
      }
    };
    af.addCacheListener(cl1);
    af.setCacheWriter(cw);
    {
      TransactionListener tl = new TransactionListenerAdapter() {
        public void afterCommit(TransactionEvent e) {
          tlLastEvents = e.getEvents();
          tlInvokeCount++;
        }

        public void close() {
          tlClosed = true;
        };
      };
      CacheTransactionManager ctm = this.c.getCacheTransactionManager();
      ctm.addListener(tl);
    }
  }

  /**
   * Confirms region (non-map) methods
   */
  @Test
  public void testRegionMethods() throws Exception {
    Object cbArg = new Object();
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.EMPTY);
    setCallbacks(af);
    clearCallbackState();
    ExpectedRegionEvent expre = new ExpectedRegionEvent();
    assertEquals(0, getStats().getRegions());
    Region r = this.c.createRegion("r", af.create());
    assertEquals(1, getStats().getRegions());
    expre.r = r;
    expre.op = Operation.REGION_CREATE;
    expre.cbArg = null;
    checkNoCW();
    checkCL(expre);

    assertEquals("r", r.getName());
    assertEquals("/r", r.getFullPath());
    assertEquals(null, r.getParentRegion());
    assertEquals(DataPolicy.EMPTY, r.getAttributes().getDataPolicy());
    r.getAttributesMutator();
    try {
      r.getStatistics();
      fail("expected StatisticsDisabledException");
    } catch (StatisticsDisabledException expected) {
      // because they were not enabled in the region attributes
    }
    r.invalidateRegion();
    expre.op = Operation.REGION_INVALIDATE;
    expre.cbArg = null;
    checkNoCW();
    checkCL(expre);

    r.invalidateRegion(cbArg);
    expre.cbArg = cbArg;
    checkNoCW();
    checkCL(expre);

    r.localInvalidateRegion();
    expre.op = Operation.REGION_LOCAL_INVALIDATE;
    expre.cbArg = null;
    checkNoCW();
    checkCL(expre);

    r.localInvalidateRegion(cbArg);
    expre.cbArg = cbArg;
    checkNoCW();
    checkCL(expre);

    r.destroyRegion();
    assertEquals(true, r.isDestroyed());
    assertEquals(0, getStats().getRegions());
    expre.op = Operation.REGION_DESTROY;
    expre.cbArg = null;
    checkCW(expre);
    checkCL(expre);

    r = this.c.createRegion("r", af.create());
    expre.r = r;
    expre.op = Operation.REGION_CREATE;
    expre.cbArg = null;
    checkNoCW();
    checkCL(expre);

    r.destroyRegion(cbArg);
    assertEquals(0, getStats().getRegions());
    assertEquals(true, r.isDestroyed());
    expre.op = Operation.REGION_DESTROY;
    expre.cbArg = cbArg;
    checkCW(expre);
    checkCL(expre);

    r = this.c.createRegion("r", af.create());
    expre.r = r;
    expre.op = Operation.REGION_CREATE;
    expre.cbArg = null;
    checkNoCW();
    checkCL(expre);

    r.localDestroyRegion();
    assertEquals(0, getStats().getRegions());
    assertEquals(true, r.isDestroyed());
    expre.op = Operation.REGION_LOCAL_DESTROY;
    expre.cbArg = null;
    checkNoCW();
    checkCWClosed();
    checkCLClosed();
    checkCL(expre);

    r = this.c.createRegion("r", af.create());
    expre.r = r;
    expre.op = Operation.REGION_CREATE;
    expre.cbArg = null;
    checkNoCW();
    checkCL(expre);

    r.localDestroyRegion(cbArg);
    assertEquals(0, getStats().getRegions());
    assertEquals(true, r.isDestroyed());
    expre.op = Operation.REGION_LOCAL_DESTROY;
    expre.cbArg = cbArg;
    checkNoCW();
    checkCWClosed();
    checkCLClosed();
    checkCL(expre);

    r = this.c.createRegion("r", af.create());
    expre.r = r;
    expre.op = Operation.REGION_CREATE;
    expre.cbArg = null;
    checkNoCW();
    checkCL(expre);

    r.close();
    assertEquals(0, getStats().getRegions());
    assertEquals(true, r.isDestroyed());
    expre.op = Operation.REGION_CLOSE;
    expre.cbArg = null;
    checkNoCW();
    checkCWClosed();
    checkCLClosed();
    checkCL(expre);


    r = this.c.createRegion("r", af.create());
    assertEquals(1, getStats().getRegions());
    expre.r = r;
    expre.op = Operation.REGION_CREATE;
    expre.cbArg = null;
    checkNoCW();
    checkCL(expre);

    try {
      r.saveSnapshot(System.out);
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
    }
    try {
      r.loadSnapshot(System.in);
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
    }
    {
      Region sr = r.createSubregion("sr", af.create());
      assertEquals(2, getStats().getRegions());
      expre.r = sr;
      expre.op = Operation.REGION_CREATE;
      expre.cbArg = null;
      checkNoCW();
      checkCL(expre);
      assertEquals("sr", sr.getName());
      assertEquals("/r/sr", sr.getFullPath());
      assertEquals(r, sr.getParentRegion());
      assertEquals(sr, r.getSubregion("sr"));
      assertEquals(Collections.singleton(sr), r.subregions(false));
      sr.close();
      assertEquals(1, getStats().getRegions());
      expre.op = Operation.REGION_CLOSE;
      expre.cbArg = null;
      checkNoCW();
      checkCWClosed();
      checkCLClosed();
      checkCL(expre);
      assertEquals(true, sr.isDestroyed());
      assertEquals(null, r.getSubregion("sr"));
      assertEquals(Collections.EMPTY_SET, r.subregions(false));
    }


    ExpectedEntryEvent expee = new ExpectedEntryEvent();
    expee.r = r;
    expee.key = "key";
    int creates = getStats().getCreates();
    // int puts = getStats().getPuts();
    // int updates = getStats().getUpdates();
    int destroys = getStats().getDestroys();
    int invalidates = getStats().getInvalidates();
    int gets = getStats().getGets();
    int misses = getStats().getMisses();

    r.put("key", "value", cbArg);
    expee.op = Operation.CREATE;
    creates++;
    assertEquals(creates, getStats().getCreates());
    expee.cbArg = cbArg;
    expee.newValue = "value";
    checkCW(expee);
    checkCL(expee);

    // note on a non-proxy region create after put fails with EntryExistsException
    r.create("key", "value", cbArg);
    creates++;
    assertEquals(creates, getStats().getCreates());
    expee.op = Operation.CREATE;
    expee.cbArg = cbArg;
    expee.newValue = "value";
    checkCW(expee);
    checkCL(expee);

    assertEquals(null, r.getEntry("key"));
    assertEquals(null, r.get("key", cbArg));
    gets++;
    assertEquals(gets, getStats().getGets());
    misses++;
    assertEquals(misses, getStats().getMisses());
    checkNoCW();
    checkNoCL();

    r.invalidate("key");
    invalidates++;
    assertEquals(invalidates, getStats().getInvalidates());
    expee.op = Operation.INVALIDATE;
    expee.cbArg = null;
    expee.newValue = null;
    checkNoCW();
    checkCL(expee);

    r.invalidate("key", cbArg);
    invalidates++;
    assertEquals(invalidates, getStats().getInvalidates());
    expee.op = Operation.INVALIDATE;
    expee.cbArg = cbArg;
    expee.newValue = null;
    checkNoCW();
    checkCL(expee);

    try {
      r.localInvalidate("key");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException expected) {
    }
    try {
      r.localInvalidate("key", cbArg);
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException expected) {
    }
    assertEquals(invalidates, getStats().getInvalidates());
    checkNoCW();
    checkNoCL();

    r.destroy("key");
    destroys++;
    assertEquals(destroys, getStats().getDestroys());
    expee.op = Operation.DESTROY;
    expee.cbArg = null;
    expee.newValue = null;
    checkCW(expee);
    checkCL(expee);

    r.destroy("key", cbArg);
    destroys++;
    assertEquals(destroys, getStats().getDestroys());
    expee.op = Operation.DESTROY;
    expee.cbArg = cbArg;
    expee.newValue = null;
    checkCW(expee);
    checkCL(expee);

    try {
      r.localDestroy("key");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException expected) {
    }
    try {
      r.localDestroy("key", cbArg);
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException expected) {
    }
    assertEquals(destroys, getStats().getDestroys());
    checkNoCW();
    checkNoCL();

    assertEquals(Collections.EMPTY_SET, r.keySet());
    assertEquals(Collections.EMPTY_SET, r.entrySet(true));
    assertEquals(this.c, r.getCache());
    r.setUserAttribute(cbArg);
    assertEquals(cbArg, r.getUserAttribute());
    checkNoCW();
    checkNoCL();

    r.put("key", "value", cbArg);
    creates++;
    assertEquals(creates, getStats().getCreates());
    expee.op = Operation.CREATE;
    expee.cbArg = cbArg;
    expee.newValue = "value";
    checkCW(expee);
    checkCL(expee);

    assertEquals(false, r.containsValueForKey("key"));
    assertEquals(false, r.existsValue("this = 'value'"));
    {
      SelectResults sr = r.query("this = 'value'");
      assertEquals(Collections.EMPTY_SET, sr.asSet());
    }
    assertEquals(null, r.selectValue("this = 'value'"));
    try {
      r.getRegionDistributedLock();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      // because we are not global
    }
    try {
      r.getDistributedLock("key");
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      // because we are not global
    }
    try {
      r.becomeLockGrantor();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      // because we are not global
    }
    try {
      r.writeToDisk();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      // because we are not configured for disk
    }
    checkNoCW();
    checkNoCL();

    // check to see if a local loader works
    {
      CacheLoader cl = new CacheLoader() {
        public Object load(LoaderHelper helper) throws CacheLoaderException {
          return "loadedValue";
        }

        public void close() {}
      };
      r.getAttributesMutator().setCacheLoader(cl);
      r.get("key", cbArg);
      gets++;
      assertEquals(gets, getStats().getGets());
      misses++;
      assertEquals(misses, getStats().getMisses());
      expee.op = Operation.LOCAL_LOAD_CREATE;
      expee.newValue = "loadedValue";
      checkCW(expee);
      checkCL(expee);
      r.getAttributesMutator().setCacheLoader(null);
    }
  }

  /**
   * Confirms map methods
   */
  @Test
  public void testMapMethods() throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.EMPTY);
    setCallbacks(af);
    clearCallbackState();
    ExpectedRegionEvent expre = new ExpectedRegionEvent();

    Region r = this.c.createRegion("r", af.create());
    expre.r = r;
    expre.cbArg = null;
    expre.op = Operation.REGION_CREATE;
    checkNoCW();
    checkCL(expre);

    int creates = getStats().getCreates();
    // int puts = getStats().getPuts();
    // int updates = getStats().getUpdates();
    int destroys = getStats().getDestroys();
    // int invalidates = getStats().getInvalidates();
    int gets = getStats().getGets();
    int misses = getStats().getMisses();
    ExpectedEntryEvent expee = new ExpectedEntryEvent();
    expee.r = r;
    expee.key = "key";
    expee.cbArg = null;

    assertEquals(null, r.put("key", "value"));
    creates++;
    assertEquals(creates, getStats().getCreates());
    expee.op = Operation.CREATE;
    expee.newValue = "value";
    checkCW(expee);
    checkCL(expee);

    {
      HashMap m = new HashMap();
      m.put("k1", "v1");
      m.put("k2", "v2");
      r.putAll(m);
      assertEquals(0, r.size());
      // @todo darrel: check events
      assertEquals(2, this.cwInvokeCount);
      assertEquals(2, this.clInvokeCount);
      clearCallbackState();
      creates += 2;
      assertEquals(creates, getStats().getCreates());
    }
    assertEquals(false, r.containsKey("key"));
    assertEquals(false, r.containsValue("value"));
    assertEquals(Collections.EMPTY_SET, r.entrySet());
    assertEquals(true, r.isEmpty());
    assertEquals(Collections.EMPTY_SET, r.keySet());
    assertEquals(0, r.size());
    assertEquals(Collections.EMPTY_LIST, new ArrayList(r.values()));
    checkNoCW();
    checkNoCL();

    assertEquals(null, r.get("key"));
    gets++;
    assertEquals(gets, getStats().getGets());
    misses++;
    assertEquals(misses, getStats().getMisses());
    checkNoCW();
    checkNoCL();

    assertEquals(null, r.remove("key"));
    destroys++;
    assertEquals(destroys, getStats().getDestroys());
    expee.op = Operation.DESTROY;
    expee.key = "key";
    expee.newValue = null;
    checkCW(expee);
    checkCL(expee);

    r.localClear();
    expre.op = Operation.REGION_LOCAL_CLEAR;
    checkNoCW();
    checkCL(expre);

    r.clear();
    expre.op = Operation.REGION_CLEAR;
    checkCW(expre);
    checkCL(expre);
  }

  /**
   * Check region ops on a proxy region done from a tx.
   */
  @Test
  public void testAllMethodsWithTX() throws Exception {
    Object cbArg = new Object();
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.EMPTY);
    setCallbacks(af);
    clearCallbackState();
    CacheTransactionManager ctm = this.c.getCacheTransactionManager();
    ExpectedRegionEvent expre = new ExpectedRegionEvent();

    Region r = this.c.createRegion("r", af.create());
    expre.r = r;
    expre.cbArg = null;
    expre.op = Operation.REGION_CREATE;
    checkNoCW();
    checkNoTL();
    checkCL(expre);

    int creates = getStats().getCreates();
    // int puts = getStats().getPuts();
    // int updates = getStats().getUpdates();
    int destroys = getStats().getDestroys();
    int invalidates = getStats().getInvalidates();
    // int gets = getStats().getGets();
    // int misses = getStats().getMisses();
    ExpectedEntryEvent expee = new ExpectedEntryEvent();
    expee.r = r;
    expee.key = "key";

    ctm.begin();
    try {
      r.localInvalidate("key");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException expected) {
    }
    try {
      r.localDestroy("key");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException expected) {
    }
    ctm.rollback();

    ctm.begin();
    expee.txId = ctm.getTransactionId();
    r.put("key", "value", cbArg);
    expee.op = Operation.CREATE;
    expee.cbArg = cbArg;
    expee.newValue = "value";
    checkCW(expee);
    checkNoTL();
    checkNoCL();
    clearCallbackState();
    ctm.commit();
    checkCL(expee, false/* ClearCallbacks */);
    checkTL(expee);
    creates++;
    assertEquals(creates, getStats().getCreates());

    ctm.begin();
    expee.txId = ctm.getTransactionId();
    r.create("key", "value", cbArg);
    expee.op = Operation.CREATE;
    expee.cbArg = cbArg;
    expee.newValue = "value";
    checkCW(expee);
    checkNoTL();
    checkNoCL();
    clearCallbackState();
    ctm.commit();
    checkCL(expee, false/* ClearCallbacks */);
    checkTL(expee);
    creates++;
    assertEquals(creates, getStats().getCreates());

    ctm.begin();
    expee.txId = ctm.getTransactionId();
    r.invalidate("key", cbArg);
    expee.op = Operation.INVALIDATE;
    expee.cbArg = cbArg;
    expee.newValue = null;
    checkNoCW();
    checkNoTL();
    checkNoCL();
    clearCallbackState();
    ctm.commit();
    checkCL(expee, false/* ClearCallbacks */);
    invalidates++;
    assertEquals(invalidates, getStats().getInvalidates());
    checkTL(expee);

    ctm.begin();
    expee.txId = ctm.getTransactionId();
    r.destroy("key", cbArg);
    expee.op = Operation.DESTROY;
    expee.cbArg = cbArg;
    expee.newValue = null;
    checkCW(expee);
    checkNoTL();
    checkNoCL();
    clearCallbackState();
    ctm.commit();
    checkCL(expee, false/* ClearCallbacks */);
    destroys++;
    assertEquals(destroys, getStats().getDestroys());
    checkTL(expee);

    ctm.begin();
    expee.txId = ctm.getTransactionId();
    r.create("key", "value", cbArg);
    r.destroy("key", cbArg);
    clearCallbackState();
    ctm.commit();
    destroys++;
    assertEquals(destroys, getStats().getDestroys());
    expee.op = Operation.DESTROY;
    checkTL(expee);

    // the following confirms that bug 37903 is fixed
    ctm.begin();
    expee.txId = ctm.getTransactionId();
    r.invalidate("key");
    r.localInvalidate("key");
    r.localDestroy("key", cbArg);
    // note that the following would fail on a non-proxy with EntryNotFound
    // so it should also fail on a proxy
    try {
      // note if bug 37903 exists then the next line will throw an AssertionError
      r.destroy("key", cbArg);
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException expected) {
    }
    clearCallbackState();
    ctm.commit();
    destroys++;
    assertEquals(destroys, getStats().getDestroys());
    expee.op = Operation.LOCAL_DESTROY;
    checkTL(expee);
  }

  /**
   * Make sure a proxy region can be lru and that it makes no difference since proxies are always
   * empty
   */
  @Test
  public void testLRU() throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(1));
    CacheListener cl1 = new CacheListenerAdapter() {
      public void afterDestroy(EntryEvent e) {
        clInvokeCount++;
      }
    };
    af.addCacheListener(cl1);

    // now try it with a proxy region which should never to do an eviction.
    {
      af.setDataPolicy(DataPolicy.EMPTY);
      try {
        af.create();
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
      }
    }
  }

  /**
   * Make sure a disk region and proxy play nice.
   */
  @Test
  public void testDiskProxy() throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.EMPTY);
    af.setEvictionAttributes(
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
    try {
      af.create();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
  }

  /**
   * Make sure a CachStatistics work on proxy
   */
  @Test
  public void testCacheStatisticsOnProxy() throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.EMPTY);
    af.setStatisticsEnabled(true);
    Region r = this.c.createRegion("rEMPTY", af.create());
    CacheStatistics stats = r.getStatistics();
    long lastModifiedTime = stats.getLastModifiedTime();
    long lastAccessedTime = stats.getLastAccessedTime();

    waitForSystemTimeChange();
    r.put("k", "v");
    assertTrue(lastModifiedTime != stats.getLastModifiedTime());
    assertTrue(lastAccessedTime != stats.getLastAccessedTime());
    lastModifiedTime = stats.getLastModifiedTime();
    lastAccessedTime = stats.getLastAccessedTime();

    waitForSystemTimeChange();
    r.create("k", "v");
    assertTrue(lastModifiedTime != stats.getLastModifiedTime());
    assertTrue(lastAccessedTime != stats.getLastAccessedTime());
    lastModifiedTime = stats.getLastModifiedTime();
    lastAccessedTime = stats.getLastAccessedTime();

    long missCount = stats.getMissCount();
    long hitCount = stats.getHitCount();
    waitForSystemTimeChange();
    r.get("k");
    assertEquals(lastModifiedTime, stats.getLastModifiedTime());
    assertTrue(lastAccessedTime != stats.getLastAccessedTime());
    assertEquals(hitCount, stats.getHitCount());
    assertEquals(missCount + 1, stats.getMissCount());
  }

  /**
   * Waits (hot) until the system time changes.
   */
  private void waitForSystemTimeChange() {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() == start);
  }
}
