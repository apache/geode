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
package org.apache.geode;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

import javax.transaction.Synchronization;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.FailedSynchronizationException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.TransactionEvent;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.TransactionListenerAdapter;
import org.apache.geode.cache.util.TxEventTestUtil;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.StopWatch;

/**
 * Tests basic transaction functionality
 *
 * @since GemFire 4.0
 */
@SuppressWarnings("deprecated")
public class TXJUnitTest {

  private int cbCount;
  private TransactionEvent te;

  protected int listenerAfterCommit;
  protected int listenerAfterFailedCommit;
  protected int listenerAfterRollback;
  protected int listenerClose;
  protected CacheTransactionManager txMgr;

  protected GemFireCacheImpl cache;
  protected Region<String, String> region;

  @Rule
  public TestName testName = new TestName();

  private boolean isPR() {
    return (this.region instanceof PartitionedRegion);
  }

  protected void createCache() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0"); // loner

    this.cache = (GemFireCacheImpl) CacheFactory.create(DistributedSystem.connect(properties));

    createRegion();
    this.txMgr = this.cache.getCacheTransactionManager();
    this.listenerAfterCommit = 0;
    this.listenerAfterFailedCommit = 0;
    this.listenerAfterRollback = 0;
    this.listenerClose = 0;
  }

  protected void createRegion() throws Exception {
    AttributesFactory<String, String> attributesFactory = new AttributesFactory<>();
    attributesFactory.setScope(Scope.DISTRIBUTED_NO_ACK);
    attributesFactory.setConcurrencyChecksEnabled(false); // test validation expects this behavior
    attributesFactory.setIndexMaintenanceSynchronous(true);

    this.region = this.cache.createRegion(getClass().getSimpleName(), attributesFactory.create());
  }

  protected void closeCache() {
    if (this.cache != null) {
      if (this.txMgr != null) {
        try {
          this.txMgr.rollback();
        } catch (IllegalStateException ignore) {
        }
      }
      this.region = null;
      this.txMgr = null;
      Cache c = this.cache;
      this.cache = null;
      c.close();
    }
  }

  @Before
  public void setUpTXJUnitTest() throws Exception {
    createCache();
  }

  @After
  public void tearDownTXJUnitTest() throws Exception {
    closeCache();
  }

  @AfterClass
  public static void afterClass() {
    InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
    if (ids != null) {
      ids.disconnect();
    }
  }

  private void checkNoTxState() {
    assertEquals(null, this.txMgr.getTransactionId());
    assertTrue(!this.txMgr.exists());
    try {
      this.txMgr.commit();
      fail("expected IllegalStateException");
    } catch (CommitConflictException unexpected) {
      fail("did not expect " + unexpected);
    } catch (IllegalStateException expected) {
    }
    try {
      this.txMgr.rollback();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
  }

  @Test
  public void testSimpleOps() throws CacheException {
    final CachePerfStats stats = this.cache.getCachePerfStats();
    // See if things are ok when no transaction
    checkNoTxState();

    this.txMgr.begin();
    // now that a transaction exists make sure things behave as expected
    assertTrue(this.txMgr.getTransactionId() != null);
    assertTrue(this.txMgr.exists());
    try {
      this.txMgr.begin();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    try {
      this.txMgr.commit();
    } catch (CommitConflictException unexpected) {
      fail("did not expect " + unexpected);
    }
    checkNoTxState();

    this.txMgr.begin();
    this.txMgr.rollback();
    checkNoTxState();

    this.region.put("uaKey", "val");

    {
      Region.Entry cmtre = this.region.getEntry("uaKey");
      cmtre.setUserAttribute("uaValue1");
      assertEquals("uaValue1", cmtre.getUserAttribute());

      int txRollbackChanges = stats.getTxRollbackChanges();
      int txCommitChanges = stats.getTxCommitChanges();
      int txFailureChanges = stats.getTxFailureChanges();
      this.txMgr.begin();
      Region.Entry txre = this.region.getEntry("uaKey");
      assertEquals(this.region, txre.getRegion());
      if (isPR()) {
        this.region.put("1", "one");
        try {
          txre.setUserAttribute("uaValue2");
        } catch (UnsupportedOperationException e) {
          // expected
        }
        try {
          txre.getUserAttribute();
        } catch (UnsupportedOperationException e) {
          // expected
        }
      } else {
        assertEquals("uaValue1", txre.getUserAttribute());
        txre.setUserAttribute("uaValue2");
        assertEquals("uaValue2", txre.getUserAttribute());
      }
      this.txMgr.rollback();
      try {
        txre.getValue();
        fail("expected IllegalStateException");
      } catch (IllegalStateException ok) {
      }
      try {
        txre.isDestroyed();
        fail("expected IllegalStateException");
      } catch (IllegalStateException ok) {
      }
      try {
        txre.getUserAttribute();
        fail("expected IllegalStateException");
      } catch (IllegalStateException ok) {
      }
      try {
        txre.setUserAttribute("foo");
        fail("expected IllegalStateException");
      } catch (IllegalStateException ok) {
      }
      assertEquals(txRollbackChanges + 1, stats.getTxRollbackChanges());
      assertEquals(txCommitChanges, stats.getTxCommitChanges());
      assertEquals(txFailureChanges, stats.getTxFailureChanges());

      assertEquals("uaValue1", cmtre.getUserAttribute());
    }

    {
      int txRollbackChanges = stats.getTxRollbackChanges();
      int txCommitChanges = stats.getTxCommitChanges();
      int txFailureChanges = stats.getTxFailureChanges();
      this.region.create("key1", "value1");
      this.txMgr.begin();
      this.region.invalidate("key1");
      this.txMgr.rollback();
      assertEquals(this.region.get("key1"), "value1");
      assertEquals(txRollbackChanges + 1, stats.getTxRollbackChanges());
      assertEquals(txCommitChanges, stats.getTxCommitChanges());
      assertEquals(txFailureChanges, stats.getTxFailureChanges());

      txRollbackChanges = stats.getTxRollbackChanges();
      this.txMgr.begin();
      this.region.destroy("key1");
      this.txMgr.rollback();
      assertEquals(this.region.get("key1"), "value1");
      assertEquals(txRollbackChanges + 1, stats.getTxRollbackChanges());
      assertEquals(txCommitChanges, stats.getTxCommitChanges());
      assertEquals(txFailureChanges, stats.getTxFailureChanges());

      txRollbackChanges = stats.getTxRollbackChanges();
      this.txMgr.begin();
      this.region.put("key1", "value2");
      this.txMgr.rollback();
      assertEquals(this.region.get("key1"), "value1");
      assertEquals(txRollbackChanges + 1, stats.getTxRollbackChanges());
      assertEquals(txCommitChanges, stats.getTxCommitChanges());
      assertEquals(txFailureChanges, stats.getTxFailureChanges());
    }
  }

  @Test
  public void testWriteOps() throws CacheException {
    this.txMgr.begin();
    this.region.put("key1", "value1");
    this.region.put("key2", "value2");
    assertTrue(this.region.containsKey("key1"));
    assertTrue(this.region.containsValueForKey("key1"));
    assertEquals("key1", this.region.getEntry("key1").getKey());
    assertEquals("value1", this.region.getEntry("key1").getValue());
    assertEquals("value1", this.region.get("key1"));
    assertTrue(this.region.containsKey("key2"));
    assertTrue(this.region.containsValueForKey("key2"));
    assertEquals("key2", this.region.getEntry("key2").getKey());
    assertEquals("value2", this.region.getEntry("key2").getValue());
    assertEquals("value2", this.region.get("key2"));
    this.txMgr.rollback();
    assertTrue(!this.region.containsKey("key1"));
    assertTrue(!this.region.containsKey("key2"));

    this.txMgr.begin();
    this.region.create("key1", "value1");
    assertTrue(this.region.containsKey("key1"));
    assertTrue(this.region.containsValueForKey("key1"));
    assertEquals("key1", this.region.getEntry("key1").getKey());
    assertEquals("value1", this.region.getEntry("key1").getValue());
    assertEquals("value1", this.region.get("key1"));
    this.txMgr.rollback();
    assertTrue(!this.region.containsKey("key1"));

    this.region.create("key1", "value1");
    this.txMgr.begin();
    this.region.put("key1", "value2");
    this.txMgr.rollback();
    assertTrue(this.region.containsKey("key1"));
    assertEquals("value1", this.region.get("key1"));
    this.region.localDestroy("key1");

    this.region.create("key1", "value1");
    this.txMgr.begin();
    this.region.localDestroy("key1");
    assertTrue(!this.region.containsKey("key1"));
    assertTrue(!this.region.containsValueForKey("key1"));
    this.txMgr.rollback();
    assertTrue(this.region.containsKey("key1"));
    this.region.localDestroy("key1");

    this.region.create("key1", "value1");
    this.txMgr.begin();
    this.region.destroy("key1");
    assertTrue(!this.region.containsKey("key1"));
    assertTrue(!this.region.containsValueForKey("key1"));
    this.txMgr.rollback();
    assertTrue(this.region.containsKey("key1"));
    this.region.localDestroy("key1");

    this.region.create("key1", "value1");
    this.txMgr.begin();
    assertTrue(this.region.containsValueForKey("key1"));
    this.region.localInvalidate("key1");
    assertTrue(this.region.containsKey("key1"));
    assertTrue(!this.region.containsValueForKey("key1"));
    assertEquals(null, this.region.get("key1"));
    this.txMgr.rollback();
    assertTrue(this.region.containsValueForKey("key1"));
    this.region.localDestroy("key1");

    this.region.create("key1", "value1");
    this.txMgr.begin();
    assertTrue(this.region.containsValueForKey("key1"));
    this.region.invalidate("key1");
    assertTrue(this.region.containsKey("key1"));
    assertTrue(!this.region.containsValueForKey("key1"));
    assertEquals(null, this.region.get("key1"));
    this.txMgr.rollback();
    assertTrue(this.region.containsValueForKey("key1"));
    this.region.localDestroy("key1");

    // see if commits work
    assertTrue(!this.region.containsKey("key1"));
    assertTrue(!this.region.containsKey("key2"));
    this.txMgr.begin();
    this.region.create("key1", "value1");
    this.region.create("key2", "value2");
    this.txMgr.commit();
    assertTrue(this.region.containsKey("key1"));
    assertTrue(this.region.containsValueForKey("key1"));
    assertEquals("key1", this.region.getEntry("key1").getKey());
    assertEquals("value1", this.region.getEntry("key1").getValue());
    assertEquals("value1", this.region.get("key1"));
    assertTrue(this.region.containsKey("key2"));
    assertTrue(this.region.containsValueForKey("key2"));
    assertEquals("key2", this.region.getEntry("key2").getKey());
    assertEquals("value2", this.region.getEntry("key2").getValue());
    assertEquals("value2", this.region.get("key2"));
    this.region.localDestroy("key1");
    this.region.localDestroy("key2");
  }

  @Test
  public void testTwoRegionTxs() throws CacheException {
    final CachePerfStats stats = this.cache.getCachePerfStats();
    int txCommitChanges;
    TransactionId myTxId;

    AttributesFactory<String, String> attributesFactory = new AttributesFactory<>();
    attributesFactory.setScope(Scope.DISTRIBUTED_NO_ACK);

    Region<String, String> reg1 = this.region;
    Region<String, String> reg2 =
        this.cache.createRegion(getUniqueName(), attributesFactory.create());

    this.txMgr.setListener(new TransactionListener() {
      @Override
      public void afterCommit(TransactionEvent event) {
        listenerAfterCommit = 1;
        te = event;
      }

      @Override
      public void afterFailedCommit(TransactionEvent event) {
        listenerAfterFailedCommit = 1;
        te = event;
      }

      @Override
      public void afterRollback(TransactionEvent event) {
        listenerAfterRollback = 1;
        te = event;
      }

      @Override
      public void close() {
        listenerClose = 1;
      }
    });

    // see if commits work
    txCommitChanges = stats.getTxCommitChanges();
    assertTrue(!reg1.containsKey("key1"));
    assertTrue(!reg2.containsKey("key2"));
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.create("key1", "value1");
    reg2.create("key2", "value2");
    this.txMgr.commit();
    assertTrue(reg1.containsKey("key1"));
    assertTrue(reg1.containsValueForKey("key1"));
    assertEquals("key1", reg1.getEntry("key1").getKey());
    assertEquals("value1", reg1.getEntry("key1").getValue());
    assertEquals("value1", reg1.get("key1"));
    assertTrue(reg2.containsKey("key2"));
    assertTrue(reg2.containsValueForKey("key2"));
    assertEquals("key2", reg2.getEntry("key2").getKey());
    assertEquals("value2", reg2.getEntry("key2").getValue());
    assertEquals("value2", reg2.get("key2"));
    assertEquals(txCommitChanges + 2, stats.getTxCommitChanges());
    {
      List<EntryEvent<String, String>> creates =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(2, creates.size());

      for (EntryEvent ev : creates) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1 || ev.getRegion() == reg2);
        if (ev.getRegion() == reg1) {
          assertEquals("key1", ev.getKey());
          assertEquals("value1", ev.getNewValue());
        } else {
          assertEquals("key2", ev.getKey());
          assertEquals("value2", ev.getNewValue());
        }
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");
    reg2.localDestroy("key2");

    reg2.localDestroyRegion();
  }

  @Test
  public void testTxEvent() throws CacheException {
    TransactionId myTxId;
    Region<String, String> reg1 = this.region;

    this.txMgr.setListener(new TransactionListener() {
      @Override
      public void afterCommit(TransactionEvent event) {
        listenerAfterCommit = 1;
        te = event;
      }

      @Override
      public void afterFailedCommit(TransactionEvent event) {
        listenerAfterFailedCommit = 1;
        te = event;
      }

      @Override
      public void afterRollback(TransactionEvent event) {
        listenerAfterRollback = 1;
        te = event;
      }

      @Override
      public void close() {
        listenerClose = 1;
      }
    });

    // make sure each operation has the correct transaction event
    // check create
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.create("key1", "value1");
    this.txMgr.rollback();
    assertEquals(1, this.te.getEvents().size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    {
      Cache teCache = this.te.getCache();
      assertEquals(teCache, this.cache);
      List<EntryEvent<String, String>> creates =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, creates.size());

      for (EntryEvent ev : creates) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value1", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }

    // check put of existing entry
    reg1.create("key1", "value0");
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.put("key1", "value1");
    this.txMgr.rollback();
    assertEquals(1, this.te.getEvents().size());
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    {
      Cache teCache = this.te.getCache();
      assertEquals(teCache, this.cache);
      List<EntryEvent<String, String>> creates = TxEventTestUtil.getPutEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, creates.size());

      for (EntryEvent ev : creates) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value1", ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check put of non-existent entry
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.put("key1", "value0");
    this.txMgr.rollback();
    assertEquals(1, this.te.getEvents().size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    {
      Cache teCache = this.te.getCache();
      assertEquals(teCache, this.cache);
      List<EntryEvent<String, String>> creates =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, creates.size());

      for (EntryEvent ev : creates) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value0", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }

    // check d invalidate of existing entry
    reg1.create("key1", "value0");
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.invalidate("key1");
    this.txMgr.rollback();
    assertEquals(1, this.te.getEvents().size());
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    {
      Cache teCache = this.te.getCache();
      assertEquals(teCache, this.cache);
      List<EntryEvent<String, String>> creates =
          TxEventTestUtil.getInvalidateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, creates.size());

      for (EntryEvent ev : creates) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals(null, ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check l invalidate of existing entry
    reg1.create("key1", "value0");
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.localInvalidate("key1");
    this.txMgr.rollback();
    assertEquals(1, this.te.getEvents().size());
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    {
      Cache teCache = this.te.getCache();
      assertEquals(teCache, this.cache);
      List<EntryEvent<String, String>> creates =
          TxEventTestUtil.getInvalidateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, creates.size());

      for (EntryEvent ev : creates) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals(null, ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        if (!isPR())
          assertTrue(!ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check d destroy of existing entry
    reg1.create("key1", "value0");
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.destroy("key1");
    this.txMgr.rollback();
    assertEquals(1, this.te.getEvents().size());
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    {
      Cache teCache = this.te.getCache();
      assertEquals(teCache, this.cache);
      List<EntryEvent<String, String>> creates =
          TxEventTestUtil.getDestroyEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, creates.size());

      for (EntryEvent ev : creates) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals(null, ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check l destroy of existing entry
    reg1.create("key1", "value0");
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.localDestroy("key1");
    this.txMgr.rollback();
    assertEquals(1, this.te.getEvents().size());
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    {
      Cache teCache = this.te.getCache();
      assertEquals(teCache, this.cache);
      List<EntryEvent<String, String>> creates =
          TxEventTestUtil.getDestroyEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, creates.size());

      for (EntryEvent ev : creates) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals(null, ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        if (!isPR())
          assertTrue(!ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");
  }

  private static class CountingCallBackValidator {
    List<Object> asserts;
    final String createWriterAssert = "create writer Assert";
    final String createListenerAssert = "create listener Assert";
    final String updateWriterAssert = "update writer Assert";
    final String updateListenerAssert = "update listener Assert";
    final String invalAssert = "invalidate Assert";
    final String destroyWriterAssert = "destroy writer Assert";
    final String destroyListenerAssert = "destroy listener Assert";
    final String localDestroyWriterAssert = "local destroy writer Assert";
    final String localDestroyListenerAssert = "local destroy listener Assert";

    CountingCacheListener cl;
    CountingCacheWriter cw;

    CountingCallBackValidator(CountingCacheListener cl, CountingCacheWriter cw) {
      this.cl = cl;
      this.cw = cw;
      this.asserts = new ArrayList<>(8);
    }

    void assertCreateWriterCnt(int cnt) {
      assertCreateWriterCnt(cnt, true);
    }

    void assertCreateWriterCnt(int cnt, boolean remember) {
      if (remember) {
        this.asserts.add(createWriterAssert);
        this.asserts.add(cnt);
      }
      assertEquals(cnt, this.cw.getBeforeCreateCalls());
    }

    void assertCreateListenerCnt(int cnt) {
      assertCreateListenerCnt(cnt, true);
    }

    void assertCreateListenerCnt(int cnt, boolean remember) {
      if (remember) {
        this.asserts.add(createListenerAssert);
        this.asserts.add(cnt);
      }
      assertEquals(cnt, this.cl.getAfterCreateCalls());
    }

    void assertDestroyWriterCnt(int cnt) {
      assertDestroyWriterCnt(cnt, true);
    }

    void assertDestroyWriterCnt(int cnt, boolean remember) {
      if (remember) {
        this.asserts.add(destroyWriterAssert);
        this.asserts.add(cnt);
      }
      assertEquals(cnt, this.cw.getBeforeDestroyCalls(false));
    }

    void assertDestroyListenerCnt(int cnt) {
      assertDestroyListenerCnt(cnt, true);
    }

    void assertDestroyListenerCnt(int cnt, boolean remember) {
      if (remember) {
        this.asserts.add(destroyListenerAssert);
        this.asserts.add(cnt);
      }
      assertEquals(cnt, this.cl.getAfterDestroyCalls(false));
    }

    void assertLocalDestroyWriterCnt(int cnt) {
      assertLocalDestroyWriterCnt(cnt, true);
    }

    void assertLocalDestroyWriterCnt(int cnt, boolean remember) {
      if (remember) {
        this.asserts.add(localDestroyWriterAssert);
        this.asserts.add(cnt);
      }
      assertEquals(0, this.cw.getBeforeDestroyCalls(true));
    }

    void assertLocalDestroyListenerCnt(int cnt) {
      assertLocalDestroyListenerCnt(cnt, true);
    }

    void assertLocalDestroyListenerCnt(int cnt, boolean remember) {
      if (remember) {
        this.asserts.add(localDestroyListenerAssert);
        this.asserts.add(cnt);
      }
      assertEquals(cnt, this.cl.getAfterDestroyCalls(true));
    }

    void assertUpdateWriterCnt(int cnt) {
      assertUpdateWriterCnt(cnt, true);
    }

    void assertUpdateWriterCnt(int cnt, boolean remember) {
      if (remember) {
        this.asserts.add(updateWriterAssert);
        this.asserts.add(cnt);
      }
      assertEquals(cnt, this.cw.getBeforeUpdateCalls());
    }

    void assertUpdateListenerCnt(int cnt) {
      assertUpdateListenerCnt(cnt, true);
    }

    void assertUpdateListenerCnt(int cnt, boolean remember) {
      if (remember) {
        this.asserts.add(updateListenerAssert);
        this.asserts.add(cnt);
      }
      assertEquals(cnt, this.cl.getAfterUpdateCalls());
    }

    void assertInvalidateCnt(int cnt) {
      assertInvalidateCnt(cnt, true);
    }

    void assertInvalidateCnt(int cnt, boolean remember) {
      if (remember) {
        this.asserts.add(invalAssert);
        this.asserts.add(cnt);
      }
      assertEquals(cnt, this.cl.getAfterInvalidateCalls());
    }

    void reAssert() {
      Iterator assertItr = this.asserts.iterator();
      String assertType;
      Integer count;
      int cnt;
      while (assertItr.hasNext()) {
        assertType = (String) assertItr.next();
        assertTrue("CountingCallBackValidator reassert, did not have an associated count",
            assertItr.hasNext());
        count = (Integer) assertItr.next();
        cnt = count;
        if (assertType.equals(createWriterAssert)) {
          this.assertCreateWriterCnt(cnt, false);
        } else if (assertType.equals(createListenerAssert)) {
          this.assertCreateListenerCnt(cnt, false);
        } else if (assertType.equals(updateWriterAssert)) {
          this.assertUpdateWriterCnt(cnt, false);
        } else if (assertType.equals(updateListenerAssert)) {
          this.assertUpdateListenerCnt(cnt, false);
        } else if (assertType.equals(invalAssert)) {
          this.assertInvalidateCnt(cnt, false);
        } else if (assertType.equals(destroyWriterAssert)) {
          this.assertDestroyWriterCnt(cnt, false);
        } else if (assertType.equals(destroyListenerAssert)) {
          this.assertDestroyListenerCnt(cnt, false);
        } else if (assertType.equals(localDestroyWriterAssert)) {
          this.assertLocalDestroyWriterCnt(cnt, false);
        } else if (assertType.equals(localDestroyListenerAssert)) {
          this.assertLocalDestroyListenerCnt(cnt, false);
        } else {
          fail("CountingCallBackValidator reassert, unknown type");
        }
      }
    }

    void reset() {
      this.cl.reset();
      this.cw.reset();
      this.asserts.clear();
    }
  }

  private interface CountingCacheListener extends CacheListener {
    int getAfterCreateCalls();

    int getAfterUpdateCalls();

    int getAfterInvalidateCalls();

    int getAfterDestroyCalls(boolean fetchLocal);

    void reset();
  }

  private interface CountingCacheWriter extends CacheWriter {
    int getBeforeCreateCalls();

    int getBeforeUpdateCalls();

    int getBeforeDestroyCalls(boolean fetchLocal);

    void reset();
  }

  @Test
  public void testTxAlgebra() throws CacheException {
    TransactionId myTxId;
    Region<String, String> reg1 = this.region;

    this.txMgr.setListener(new TransactionListener() {
      @Override
      public void afterCommit(TransactionEvent event) {
        listenerAfterCommit = 1;
        te = event;
      }

      @Override
      public void afterFailedCommit(TransactionEvent event) {
        listenerAfterFailedCommit = 1;
        te = event;
      }

      @Override
      public void afterRollback(TransactionEvent event) {
        listenerAfterRollback = 1;
        te = event;
      }

      @Override
      public void close() {
        listenerClose = 1;
      }
    });
    AttributesMutator<String, String> mutator = this.region.getAttributesMutator();
    CountingCacheListener cntListener = new CountingCacheListener() {
      volatile int aCreateCalls, aUpdateCalls, aInvalidateCalls, aDestroyCalls, aLocalDestroyCalls;

      @Override
      public void close() {}

      @Override
      public void reset() {
        this.aCreateCalls = this.aUpdateCalls =
            this.aInvalidateCalls = this.aDestroyCalls = this.aLocalDestroyCalls = 0;
      }

      @Override
      public void afterCreate(EntryEvent e) {
        ++this.aCreateCalls;
      }

      @Override
      public void afterUpdate(EntryEvent e) {
        ++this.aUpdateCalls;
      }

      @Override
      public void afterInvalidate(EntryEvent e) {
        ++this.aInvalidateCalls;
      }

      @Override
      public void afterDestroy(EntryEvent e) {
        if (e.getOperation().isDistributed()) {
          ++this.aDestroyCalls;
        } else {
          ++this.aLocalDestroyCalls;
        }
      }

      @Override
      public void afterRegionInvalidate(RegionEvent e) {
        fail("Unexpected afterRegionInvalidate in testTxAlgebra");
      }

      @Override
      public void afterRegionDestroy(RegionEvent e) {
        if (!e.getOperation().isClose()) {
          fail("Unexpected afterRegionDestroy in testTxAlgebra");
        }
      }

      @Override
      public void afterRegionClear(RegionEvent event) {}

      @Override
      public void afterRegionCreate(RegionEvent event) {}

      @Override
      public void afterRegionLive(RegionEvent event) {}

      @Override
      public int getAfterCreateCalls() {
        return this.aCreateCalls;
      }

      @Override
      public int getAfterUpdateCalls() {
        return this.aUpdateCalls;
      }

      @Override
      public int getAfterInvalidateCalls() {
        return this.aInvalidateCalls;
      }

      @Override
      public int getAfterDestroyCalls(boolean fetchLocal) {
        return fetchLocal ? this.aLocalDestroyCalls : this.aDestroyCalls;
      }
    };
    mutator.addCacheListener(cntListener);
    CountingCacheWriter cntWriter = new CountingCacheWriter() {
      int bCreateCalls, bUpdateCalls, bDestroyCalls, bLocalDestroyCalls;

      @Override
      public void close() {}

      @Override
      public void reset() {
        this.bCreateCalls = this.bUpdateCalls = this.bDestroyCalls = this.bLocalDestroyCalls = 0;
      }

      @Override
      public void beforeCreate(EntryEvent e) {
        ++this.bCreateCalls;
      }

      @Override
      public void beforeUpdate(EntryEvent e) {
        ++this.bUpdateCalls;
      }

      @Override
      public void beforeDestroy(EntryEvent e) {
        ++this.bDestroyCalls;
      }

      @Override
      public void beforeRegionDestroy(RegionEvent e) {
        fail("Unexpected beforeRegionDestroy in testTxAlgebra");
      }

      @Override
      public void beforeRegionClear(RegionEvent e) {
        fail("Unexpected beforeRegionClear in testTxAlgebra");
      }

      @Override
      public int getBeforeCreateCalls() {
        return this.bCreateCalls;
      }

      @Override
      public int getBeforeUpdateCalls() {
        return this.bUpdateCalls;
      }

      @Override
      public int getBeforeDestroyCalls(boolean fetchLocal) {
        return fetchLocal ? this.bLocalDestroyCalls : this.bDestroyCalls;
      }
    };
    mutator.setCacheWriter(cntWriter);

    CountingCallBackValidator callbackVal = new CountingCallBackValidator(cntListener, cntWriter);

    // make sure each op sequence has the correct affect transaction event
    // check C + C -> EX
    // check C + P -> C
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.create("key1", "value1");
    callbackVal.assertCreateWriterCnt(1);
    try {
      reg1.create("key1", "value2");
      fail("expected EntryExistsException");
    } catch (EntryExistsException ok) {
    }
    callbackVal.assertCreateWriterCnt(1, /* remember */ false);
    reg1.put("key1", "value2");
    callbackVal.assertUpdateWriterCnt(1);
    assertEquals("value2", reg1.getEntry("key1").getValue());
    // Make sure listener callbacks were not triggered before commit
    callbackVal.assertCreateListenerCnt(0, false);
    callbackVal.assertUpdateListenerCnt(0);
    this.txMgr.commit();
    callbackVal.assertCreateListenerCnt(1);
    callbackVal.reAssert();
    assertEquals("value2", reg1.getEntry("key1").getValue());
    assertEquals(1, this.te.getEvents().size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value2", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // Check C + DI -> C
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.create("key1", "value1");
    callbackVal.assertCreateWriterCnt(1);
    reg1.invalidate("key1");
    callbackVal.assertInvalidateCnt(0, false);
    assertTrue(reg1.containsKey("key1"));
    assertTrue(!reg1.containsValueForKey("key1"));
    callbackVal.assertCreateListenerCnt(0, false);
    this.txMgr.commit();
    callbackVal.assertCreateListenerCnt(1);
    callbackVal.reAssert();
    assertTrue(reg1.containsKey("key1"));
    assertTrue(!reg1.containsValueForKey("key1"));
    assertEquals(1, this.te.getEvents().size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals(null, ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // TODO: mitch implement the following
    // check LI + DI -> NOOP
    // check DI + LI -> NOOP
    // check DI + DI -> NOOP
    // check LI + LI -> NOOP

    // check C + DD -> NOOP
    callbackVal.reset();
    this.txMgr.begin();
    reg1.create("key1", "value0");
    callbackVal.assertCreateWriterCnt(1);
    reg1.destroy("key1");
    callbackVal.assertDestroyWriterCnt(1);
    assertTrue(!reg1.containsKey("key1"));
    callbackVal.assertDestroyListenerCnt(0);
    this.txMgr.commit();
    callbackVal.assertDestroyListenerCnt(0);
    callbackVal.assertCreateListenerCnt(0);
    callbackVal.reAssert();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals(0, this.te.getEvents().size());
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());

    // Check C + LI -> C
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.create("key1", "value1");
    callbackVal.assertCreateWriterCnt(1);
    reg1.localInvalidate("key1");
    callbackVal.assertInvalidateCnt(0);
    assertTrue(reg1.containsKey("key1"));
    assertTrue(!reg1.containsValueForKey("key1"));
    this.txMgr.commit();
    callbackVal.assertCreateListenerCnt(1);
    callbackVal.reAssert();
    assertTrue(reg1.containsKey("key1"));
    assertTrue(!reg1.containsValueForKey("key1"));
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals(null, ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // Check C + LI + C -> EX
    // Check C + LI + P -> C
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.create("key1", "value1");
    callbackVal.assertCreateWriterCnt(1);
    reg1.localInvalidate("key1");
    callbackVal.assertInvalidateCnt(0);
    try {
      reg1.create("key1", "ex");
      fail("expected EntryExistsException");
    } catch (EntryExistsException ok) {
    }
    callbackVal.assertCreateWriterCnt(1, /* remember */ false);
    reg1.put("key1", "value2");
    callbackVal.assertUpdateWriterCnt(1);
    assertTrue(reg1.containsKey("key1"));
    assertEquals("value2", reg1.getEntry("key1").getValue());
    callbackVal.assertUpdateListenerCnt(0);
    callbackVal.assertCreateListenerCnt(0, false);
    this.txMgr.commit();
    callbackVal.assertCreateListenerCnt(1);
    callbackVal.reAssert();
    assertTrue(reg1.containsKey("key1"));
    assertEquals("value2", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value2", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // Check C + LI + LD -> NOOP
    callbackVal.reset();
    this.txMgr.begin();
    reg1.create("key1", "value1");
    callbackVal.assertCreateWriterCnt(1);
    reg1.localInvalidate("key1");
    callbackVal.assertInvalidateCnt(0);
    reg1.localDestroy("key1");
    callbackVal.assertLocalDestroyWriterCnt(1);
    assertTrue(!reg1.containsKey("key1"));
    this.txMgr.commit();
    callbackVal.assertCreateListenerCnt(0);
    callbackVal.assertDestroyListenerCnt(0);
    callbackVal.reAssert();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(0, this.te.getEvents().size());

    // Check C + LI + DD -> NOOP
    callbackVal.reset();
    this.txMgr.begin();
    reg1.create("key1", "value1");
    callbackVal.assertCreateWriterCnt(1);
    reg1.localInvalidate("key1");
    callbackVal.assertInvalidateCnt(0);
    reg1.destroy("key1");
    callbackVal.assertDestroyWriterCnt(1);
    assertTrue(!reg1.containsKey("key1"));
    this.txMgr.commit();
    callbackVal.assertDestroyListenerCnt(0);
    callbackVal.assertCreateListenerCnt(0);
    callbackVal.reAssert();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(0, this.te.getEvents().size());

    // check C + LD -> NOOP
    callbackVal.reset();
    this.txMgr.begin();
    reg1.create("key1", "value0");
    callbackVal.assertCreateWriterCnt(1);
    reg1.localDestroy("key1");
    callbackVal.assertLocalDestroyWriterCnt(1);
    assertTrue(!reg1.containsKey("key1"));
    this.txMgr.commit();
    callbackVal.assertCreateListenerCnt(0);
    callbackVal.assertLocalDestroyListenerCnt(0);
    callbackVal.reAssert();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(0, this.te.getEvents().size());

    // check C + LD + D -> EX
    // check C + LD + I -> EX
    // check C + LD + C -> C
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.create("key1", "value0");
    callbackVal.assertCreateWriterCnt(1, /* remember */ false);
    reg1.localDestroy("key1");
    callbackVal.assertLocalDestroyWriterCnt(1);
    assertTrue(!reg1.containsKey("key1"));
    try {
      reg1.localDestroy("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException ok) {
    }
    callbackVal.assertLocalDestroyWriterCnt(1, /* remember */ false);
    try {
      reg1.destroy("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException ok) {
    }
    callbackVal.assertDestroyWriterCnt(0);
    try {
      reg1.localInvalidate("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException ok) {
    }
    callbackVal.assertInvalidateCnt(0);
    try {
      reg1.invalidate("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException ok) {
    }
    callbackVal.assertInvalidateCnt(0, /* remember */ false);
    reg1.create("key1", "value3");
    callbackVal.assertCreateWriterCnt(2);
    assertEquals("value3", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    callbackVal.assertCreateListenerCnt(1);
    callbackVal.reAssert();
    assertEquals("value3", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value3", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check C + LD + P -> C
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.create("key1", "value0");
    callbackVal.assertCreateWriterCnt(1, /* remember */ false);
    reg1.localDestroy("key1");
    callbackVal.assertLocalDestroyWriterCnt(1);
    reg1.put("key1", "value3");
    callbackVal.assertCreateWriterCnt(2);
    assertEquals("value3", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    callbackVal.assertCreateListenerCnt(1);
    callbackVal.assertUpdateListenerCnt(0);
    callbackVal.reAssert();
    assertEquals("value3", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value3", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check put of existing entry
    // check P + C -> EX
    // check P + P -> P
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.put("key1", "value1");
    callbackVal.assertUpdateWriterCnt(1, /* remember */ false);
    try {
      reg1.create("key1", "value2");
      fail("expected EntryExistsException");
    } catch (EntryExistsException ok) {
    }
    callbackVal.assertUpdateWriterCnt(1, /* remember */ false);
    callbackVal.assertCreateWriterCnt(0);
    reg1.put("key1", "value3");
    callbackVal.assertUpdateWriterCnt(2);
    assertEquals("value3", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    callbackVal.assertCreateListenerCnt(0);
    callbackVal.assertUpdateListenerCnt(1);
    callbackVal.reAssert();
    assertEquals("value3", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events = TxEventTestUtil.getPutEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value3", ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check P + DI -> DI
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.put("key1", "value1");
    callbackVal.assertUpdateWriterCnt(1);
    reg1.invalidate("key1");
    callbackVal.assertInvalidateCnt(0, false);
    assertTrue(reg1.containsKey("key1"));
    assertTrue(!reg1.containsValueForKey("key1"));
    this.txMgr.commit();
    callbackVal.assertInvalidateCnt(1);
    callbackVal.assertUpdateListenerCnt(0);
    callbackVal.reAssert();
    assertTrue(reg1.containsKey("key1"));
    assertTrue(!reg1.containsValueForKey("key1"));
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getInvalidateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals(null, ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check P + DD -> D
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.put("key1", "value1");
    callbackVal.assertUpdateWriterCnt(1);
    reg1.destroy("key1");
    callbackVal.assertDestroyWriterCnt(1);
    assertTrue(!reg1.containsKey("key1"));
    this.txMgr.commit();
    callbackVal.assertUpdateListenerCnt(0);
    callbackVal.assertLocalDestroyListenerCnt(0);
    callbackVal.assertDestroyListenerCnt(1);
    callbackVal.reAssert();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getDestroyEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals(null, ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }

    // check P + LI -> LI
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.put("key1", "value1");
    callbackVal.assertUpdateWriterCnt(1);
    reg1.localInvalidate("key1");
    callbackVal.assertInvalidateCnt(0);
    assertTrue(reg1.containsKey("key1"));
    assertTrue(!reg1.containsValueForKey("key1"));
    this.txMgr.commit();
    callbackVal.assertUpdateListenerCnt(1);
    callbackVal.reAssert();
    assertTrue(reg1.containsKey("key1"));
    assertEquals(null, reg1.getEntry("key1").getValue());
    assertTrue(!reg1.containsValueForKey("key1"));
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getInvalidateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals(null, ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(!ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // Check P + LI + C -> EX
    // Check P + LI + P -> P
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.put("key1", "value1");
    callbackVal.assertUpdateWriterCnt(1, /* remember */ false);
    reg1.localInvalidate("key1");
    callbackVal.assertInvalidateCnt(0);
    try {
      reg1.create("key1", "ex");
      fail("expected EntryExistsException");
    } catch (EntryExistsException ok) {
    }
    callbackVal.assertCreateWriterCnt(0);
    callbackVal.assertUpdateWriterCnt(1, /* remember */ false);
    reg1.put("key1", "value2");
    callbackVal.assertUpdateWriterCnt(2);
    assertTrue(reg1.containsKey("key1"));
    assertEquals("value2", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    callbackVal.assertUpdateListenerCnt(1);
    callbackVal.reAssert();
    assertTrue(reg1.containsKey("key1"));
    assertEquals("value2", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events = TxEventTestUtil.getPutEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value2", ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // Check P + LI + LD -> LD
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.put("key1", "value1");
    callbackVal.assertUpdateWriterCnt(1);
    reg1.localInvalidate("key1");
    callbackVal.assertInvalidateCnt(0);
    reg1.localDestroy("key1");
    callbackVal.assertLocalDestroyWriterCnt(1);
    assertTrue(!reg1.containsKey("key1"));
    this.txMgr.commit();
    callbackVal.assertUpdateListenerCnt(0);
    callbackVal.assertDestroyListenerCnt(0);
    callbackVal.assertLocalDestroyListenerCnt(1);
    callbackVal.reAssert();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getDestroyEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals(null, ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(!ev.getOperation().isDistributed());
      }
    }

    // Check P + LI + DD -> DD
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.put("key1", "value1");
    callbackVal.assertUpdateWriterCnt(1);
    reg1.localInvalidate("key1");
    callbackVal.assertInvalidateCnt(0);
    reg1.destroy("key1");
    callbackVal.assertDestroyWriterCnt(1);
    assertTrue(!reg1.containsKey("key1"));
    this.txMgr.commit();
    callbackVal.assertUpdateListenerCnt(0);
    callbackVal.assertDestroyListenerCnt(1);
    callbackVal.reAssert();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getDestroyEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals(null, ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }

    // check P + LD -> LD
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.put("key1", "value1");
    callbackVal.assertUpdateWriterCnt(1);
    reg1.localDestroy("key1");
    callbackVal.assertLocalDestroyWriterCnt(1);
    assertTrue(!reg1.containsKey("key1"));
    this.txMgr.commit();
    callbackVal.assertUpdateListenerCnt(0);
    callbackVal.assertDestroyListenerCnt(0);
    callbackVal.assertLocalDestroyListenerCnt(1);
    callbackVal.reAssert();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getDestroyEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals(null, ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(!ev.getOperation().isDistributed());
      }
    }

    // check P + LD + D -> EX
    // check P + LD + I -> EX
    // check P + LD + C -> C
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.put("key1", "value1");
    callbackVal.assertUpdateWriterCnt(1);
    reg1.localDestroy("key1");
    callbackVal.assertLocalDestroyWriterCnt(1);
    assertTrue(!reg1.containsKey("key1"));
    try {
      reg1.localDestroy("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException ok) {
    }
    callbackVal.assertLocalDestroyWriterCnt(1, /* remember */ false);
    try {
      reg1.destroy("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException ok) {
    }
    callbackVal.assertDestroyWriterCnt(0);
    try {
      reg1.localInvalidate("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException ok) {
    }
    callbackVal.assertInvalidateCnt(0, /* remember */ false);
    try {
      reg1.invalidate("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException ok) {
    }
    callbackVal.assertInvalidateCnt(0);
    reg1.create("key1", "value3");
    callbackVal.assertCreateWriterCnt(1);
    assertEquals("value3", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    callbackVal.assertLocalDestroyListenerCnt(0);
    callbackVal.assertUpdateListenerCnt(1);
    callbackVal.reAssert();
    assertEquals("value3", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value3", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check P + LD + P -> C
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.put("key1", "value1");
    callbackVal.assertUpdateWriterCnt(1, /* remember */ false);
    reg1.localDestroy("key1");
    callbackVal.assertLocalDestroyWriterCnt(1);
    reg1.put("key1", "value3");
    callbackVal.assertCreateWriterCnt(1);
    assertEquals("value3", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    callbackVal.assertLocalDestroyListenerCnt(0);
    callbackVal.assertUpdateListenerCnt(1);
    callbackVal.reAssert();
    assertEquals("value3", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value3", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check DI + C -> EX
    // check DI + P -> P
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.invalidate("key1");
    callbackVal.assertInvalidateCnt(0);
    try {
      reg1.create("key1", "value1");
      fail("expected EntryExistsException");
    } catch (EntryExistsException ok) {
    }
    callbackVal.assertCreateWriterCnt(0);
    reg1.put("key1", "value2");
    callbackVal.assertUpdateWriterCnt(1);
    assertEquals("value2", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    callbackVal.assertUpdateListenerCnt(1);
    callbackVal.reAssert();
    assertEquals("value2", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events = TxEventTestUtil.getPutEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value2", ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check DI + DD -> D
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.invalidate("key1");
    callbackVal.assertInvalidateCnt(0);
    reg1.destroy("key1");
    callbackVal.assertDestroyWriterCnt(1);
    assertTrue(!reg1.containsKey("key1"));
    this.txMgr.commit();
    callbackVal.assertDestroyListenerCnt(1);
    callbackVal.reAssert();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getDestroyEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals(null, ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }

    // check DI + LD -> LD
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.invalidate("key1");
    callbackVal.assertInvalidateCnt(0);
    reg1.localDestroy("key1");
    callbackVal.assertLocalDestroyWriterCnt(1);
    assertTrue(!reg1.containsKey("key1"));
    this.txMgr.commit();
    callbackVal.assertDestroyListenerCnt(0);
    callbackVal.assertLocalDestroyListenerCnt(1);
    callbackVal.reAssert();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getDestroyEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals(null, ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(!ev.getOperation().isDistributed());
      }
    }

    // check DI + LD + D -> EX
    // check DI + LD + I -> EX
    // check DI + LD + C -> C
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.invalidate("key1");
    callbackVal.assertInvalidateCnt(0, false);
    reg1.localDestroy("key1");
    callbackVal.assertLocalDestroyWriterCnt(1);
    assertTrue(!reg1.containsKey("key1"));
    try {
      reg1.localDestroy("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException ok) {
    }
    callbackVal.assertLocalDestroyWriterCnt(1, /* remember */ false);
    try {
      reg1.destroy("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException ok) {
    }
    callbackVal.assertDestroyWriterCnt(0);
    try {
      reg1.localInvalidate("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException ok) {
    }
    callbackVal.assertInvalidateCnt(0, /* remember */ false);
    try {
      reg1.invalidate("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException ok) {
    }
    callbackVal.assertInvalidateCnt(0, /* remember */ false);
    reg1.create("key1", "value3");
    callbackVal.assertCreateWriterCnt(1);
    assertEquals("value3", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    callbackVal.assertInvalidateCnt(0);
    callbackVal.assertDestroyListenerCnt(0);
    callbackVal.assertLocalDestroyListenerCnt(0);
    callbackVal.assertCreateListenerCnt(0);
    callbackVal.assertUpdateListenerCnt(1);
    callbackVal.reAssert();
    assertEquals("value3", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value3", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check DI + LD + P -> C
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.invalidate("key1", "value1");
    callbackVal.assertInvalidateCnt(0, false);
    reg1.localDestroy("key1");
    callbackVal.assertLocalDestroyWriterCnt(1);
    reg1.put("key1", "value3");
    callbackVal.assertCreateWriterCnt(1);
    assertEquals("value3", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    callbackVal.assertInvalidateCnt(0);
    callbackVal.assertCreateListenerCnt(0);
    callbackVal.assertLocalDestroyListenerCnt(0);
    callbackVal.assertUpdateListenerCnt(1);
    callbackVal.reAssert();
    assertEquals("value3", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value3", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check LI + C -> EX
    // check LI + P -> P
    reg1.create("key1", "value0");
    this.txMgr.begin();
    callbackVal.reset();
    myTxId = this.txMgr.getTransactionId();
    reg1.localInvalidate("key1");
    callbackVal.assertInvalidateCnt(0, false);
    try {
      reg1.create("key1", "value1");
      fail("expected EntryExistsException");
    } catch (EntryExistsException ok) {
    }
    callbackVal.assertCreateWriterCnt(0);
    reg1.put("key1", "value2");
    callbackVal.assertUpdateWriterCnt(1);
    assertEquals("value2", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    callbackVal.assertUpdateListenerCnt(1);
    callbackVal.assertInvalidateCnt(0);
    callbackVal.reAssert();
    assertEquals("value2", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events = TxEventTestUtil.getPutEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value2", ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check LI + DD -> DD
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.localInvalidate("key1");
    callbackVal.assertInvalidateCnt(0, false);
    reg1.destroy("key1");
    callbackVal.assertDestroyWriterCnt(1);
    assertTrue(!reg1.containsKey("key1"));
    this.txMgr.commit();
    callbackVal.assertInvalidateCnt(0);
    callbackVal.assertDestroyListenerCnt(1);
    callbackVal.reAssert();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getDestroyEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals(null, ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }

    // check LI + LD -> LD
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.localInvalidate("key1");
    callbackVal.assertInvalidateCnt(0, false);
    reg1.localDestroy("key1");
    callbackVal.assertLocalDestroyWriterCnt(1);
    assertTrue(!reg1.containsKey("key1"));
    this.txMgr.commit();
    callbackVal.assertInvalidateCnt(0);
    callbackVal.assertLocalDestroyListenerCnt(1);
    callbackVal.reAssert();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getDestroyEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals(null, ev.getNewValue());
        assertEquals("value0", ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(!ev.getOperation().isDistributed());
      }
    }

    // check LI + LD + D -> EX
    // check LI + LD + I -> EX
    // check LI + LD + C -> C
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.localInvalidate("key1");
    callbackVal.assertInvalidateCnt(0, false);
    reg1.localDestroy("key1");
    callbackVal.assertLocalDestroyWriterCnt(1);
    assertTrue(!reg1.containsKey("key1"));
    try {
      reg1.localDestroy("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException ok) {
    }
    callbackVal.assertLocalDestroyWriterCnt(1, /* remember */ false);
    try {
      reg1.destroy("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException ok) {
    }
    callbackVal.assertDestroyWriterCnt(0);
    try {
      reg1.localInvalidate("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException ok) {
    }
    callbackVal.assertInvalidateCnt(0, /* remember */ false);
    try {
      reg1.invalidate("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException ok) {
    }
    callbackVal.assertInvalidateCnt(0, /* remember */ false);
    reg1.create("key1", "value3");
    callbackVal.assertCreateWriterCnt(1);
    assertEquals("value3", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    callbackVal.assertInvalidateCnt(0);
    callbackVal.assertDestroyListenerCnt(0);
    callbackVal.assertLocalDestroyListenerCnt(0);
    callbackVal.assertCreateListenerCnt(0);
    callbackVal.assertUpdateListenerCnt(1);
    callbackVal.reAssert();
    assertEquals("value3", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value3", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check LI + LD + P -> C
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.localInvalidate("key1", "value1");
    callbackVal.assertInvalidateCnt(0, false);
    reg1.localDestroy("key1");
    callbackVal.assertLocalDestroyWriterCnt(1);
    reg1.put("key1", "value3");
    callbackVal.assertCreateWriterCnt(1);
    assertEquals("value3", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    callbackVal.assertLocalDestroyListenerCnt(0);
    callbackVal.assertCreateListenerCnt(0);
    callbackVal.assertInvalidateCnt(0);
    callbackVal.assertUpdateListenerCnt(1);
    callbackVal.reAssert();
    assertEquals("value3", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value3", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check init state LI + P + I -> I token (bug 33073)
    reg1.create("key1", "value0");
    reg1.localInvalidate("key1");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.put("key1", "value1");
    callbackVal.assertUpdateWriterCnt(1);
    assertEquals("value1", reg1.getEntry("key1").getValue());
    reg1.invalidate("key1");
    callbackVal.assertInvalidateCnt(0, false);
    this.txMgr.commit();
    callbackVal.assertUpdateListenerCnt(0);
    callbackVal.assertInvalidateCnt(1);
    callbackVal.reAssert();
    assertNull(reg1.getEntry("key1").getValue());
    {
      // Special check to assert Invaldate token
      LocalRegion.NonTXEntry nonTXe = (LocalRegion.NonTXEntry) reg1.getEntry("key1");
      assertTrue(nonTXe.getRegionEntry().isInvalid());
    }
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getInvalidateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertNull(ev.getNewValue());
        assertNull(ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }

    // check init state I + P + LI -> LI token (bug 33073)
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.put("key1", "value1");
    callbackVal.assertUpdateWriterCnt(1);
    assertEquals("value1", reg1.getEntry("key1").getValue());
    reg1.localInvalidate("key1");
    callbackVal.assertInvalidateCnt(0, false);
    this.txMgr.commit();
    callbackVal.assertInvalidateCnt(0);
    callbackVal.assertUpdateListenerCnt(1);
    callbackVal.assertCreateListenerCnt(0);
    callbackVal.reAssert();
    assertNull(reg1.getEntry("key1").getValue());
    {
      // Special check to assert Local Invaldate token
      LocalRegion.NonTXEntry nonTXe = (LocalRegion.NonTXEntry) reg1.getEntry("key1");
      assertTrue(nonTXe.getRegionEntry().isInvalid());
    }
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getInvalidateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertNull(ev.getNewValue());
        assertNull(ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(!ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check DD + C -> C
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.destroy("key1");
    callbackVal.assertDestroyWriterCnt(1);
    reg1.create("key1", "value1");
    callbackVal.assertCreateWriterCnt(1);
    assertEquals("value1", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    callbackVal.assertDestroyListenerCnt(0);
    callbackVal.assertCreateListenerCnt(0);
    callbackVal.assertUpdateListenerCnt(1);
    callbackVal.reAssert();
    assertEquals("value1", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value1", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check DD + P -> C
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.destroy("key1");
    callbackVal.assertDestroyWriterCnt(1);
    reg1.put("key1", "value1");
    callbackVal.assertCreateWriterCnt(1);
    assertEquals("value1", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    callbackVal.assertDestroyListenerCnt(0);
    callbackVal.assertCreateListenerCnt(0);
    callbackVal.assertUpdateListenerCnt(1);
    callbackVal.reAssert();
    assertEquals("value1", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value1", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check LD + C -> C
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.localDestroy("key1");
    callbackVal.assertLocalDestroyWriterCnt(1);
    reg1.create("key1", "value1");
    callbackVal.assertCreateWriterCnt(1);
    assertEquals("value1", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    callbackVal.assertLocalDestroyListenerCnt(0);
    callbackVal.assertCreateListenerCnt(0);
    callbackVal.assertUpdateListenerCnt(1);
    callbackVal.reAssert();
    assertEquals("value1", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value1", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");

    // check LD + P -> C
    reg1.create("key1", "value0");
    callbackVal.reset();
    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    reg1.localDestroy("key1");
    callbackVal.assertLocalDestroyWriterCnt(1);
    reg1.put("key1", "value1");
    callbackVal.assertCreateWriterCnt(1);
    assertEquals("value1", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    callbackVal.assertLocalDestroyListenerCnt(0);
    callbackVal.assertCreateListenerCnt(0);
    callbackVal.assertUpdateListenerCnt(1);
    callbackVal.reAssert();
    assertEquals("value1", reg1.getEntry("key1").getValue());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(1, this.te.getEvents().size());
    {
      List<EntryEvent<String, String>> events =
          TxEventTestUtil.getCreateEvents(this.te.getEvents());
      assertEquals(myTxId, this.te.getTransactionId());
      assertEquals(1, events.size());

      for (EntryEvent ev : events) {
        assertEquals(myTxId, ev.getTransactionId());
        assertTrue(ev.getRegion() == reg1);
        assertEquals("key1", ev.getKey());
        assertEquals("value1", ev.getNewValue());
        assertEquals(null, ev.getOldValue());
        verifyEventProps(ev);
        assertEquals(null, ev.getCallbackArgument());
        assertEquals(true, ev.isCallbackArgumentAvailable());
        assertTrue(!ev.isOriginRemote());
        assertTrue(!ev.getOperation().isExpiration());
        assertTrue(ev.getOperation().isDistributed());
      }
    }
    reg1.localDestroy("key1");
  }

  private void doNonTxInvalidateRegionOp(CachePerfStats stats) throws Exception {
    int txRollbackChanges = stats.getTxRollbackChanges();
    this.region.create("key1", "value1");
    this.region.create("key2", "value2");
    this.txMgr.begin();
    try {
      this.region.localInvalidateRegion();
      fail("Should have gotten an UnsupportedOperationInTransactionException");
    } catch (UnsupportedOperationInTransactionException ee) {
      // this is expected
    }
    this.txMgr.rollback();
    assertEquals("value1", this.region.get("key1"));
    assertEquals("value2", this.region.get("key2"));
    assertEquals(txRollbackChanges, stats.getTxRollbackChanges());

    this.region.put("key1", "value1");
    this.region.put("key2", "value2");
    this.txMgr.begin();
    try {
      this.region.invalidateRegion();
      fail("Should have gotten an UnsupportedOperationInTransactionException");
    } catch (UnsupportedOperationInTransactionException ee) {
      // this is expected
    }
    this.txMgr.rollback();
    assertEquals("value1", this.region.get("key1"));
    assertEquals("value2", this.region.get("key2"));
    assertEquals(txRollbackChanges, stats.getTxRollbackChanges());

  }

  private void doNonTxDestroyRegionOp(CachePerfStats stats) throws Exception {
    int txRollbackChanges = stats.getTxRollbackChanges();
    this.region.put("key1", "value1");
    this.region.put("key2", "value2");
    this.txMgr.begin();
    try {
      this.region.localDestroyRegion();
      fail("Should have gotten an UnsupportedOperationInTransactionException");
    } catch (UnsupportedOperationInTransactionException ee) {
      // this is expected
    }
    this.txMgr.rollback();
    assertTrue(!this.region.isDestroyed());
    assertEquals(txRollbackChanges, stats.getTxRollbackChanges());

    this.txMgr.begin();
    try {
      this.region.destroyRegion();
      fail("Should have gotten an UnsupportedOperationInTransactionException");
    } catch (UnsupportedOperationInTransactionException ee) {
      // this is expected
    }
    this.txMgr.rollback();
    assertTrue(!this.region.isDestroyed());
    assertEquals(txRollbackChanges, stats.getTxRollbackChanges());
  }

  @Test
  public void testNonTxRegionOps() throws Exception {
    final CachePerfStats stats = this.cache.getCachePerfStats();
    doNonTxInvalidateRegionOp(stats);
    doNonTxDestroyRegionOp(stats);
  }

  @Test
  public void testEntryNotFound() {
    // make sure operations that should fail with EntryNotFoundException
    // do so when done transactionally
    try {
      try {
        this.region.destroy("noEntry");
        fail("expected EntryNotFoundException");
      } catch (EntryNotFoundException expected) {
      }
      try {
        this.region.localDestroy("noEntry");
        fail("expected EntryNotFoundException");
      } catch (EntryNotFoundException expected) {
      }
      try {
        this.region.invalidate("noEntry");
        fail("expected EntryNotFoundException");
      } catch (EntryNotFoundException expected) {
      }
      try {
        this.region.localInvalidate("noEntry");
        fail("expected EntryNotFoundException");
      } catch (EntryNotFoundException expected) {
      }

      this.txMgr.begin();
      try {
        this.region.destroy("noEntry");
        fail("expected EntryNotFoundException");
      } catch (EntryNotFoundException expected) {
      }
      this.txMgr.rollback();
      this.txMgr.begin();
      try {
        this.region.localDestroy("noEntry");
        fail("expected EntryNotFoundException");
      } catch (EntryNotFoundException expected) {
      }
      this.txMgr.rollback();
      this.txMgr.begin();
      try {
        this.region.invalidate("noEntry");
        fail("expected EntryNotFoundException");
      } catch (EntryNotFoundException expected) {
      }
      this.txMgr.rollback();
      this.txMgr.begin();
      try {
        this.region.localInvalidate("noEntry");
        fail("expected EntryNotFoundException");
      } catch (EntryNotFoundException expected) {
      }
      this.txMgr.rollback();

      // check to see if EntryExistsException works in transactions
      this.region.create("key1", "value1");
      try {
        this.region.create("key1", "value2");
        fail("expected EntryExistsException");
      } catch (EntryExistsException expected) {
      }
      this.txMgr.begin();
      try {
        this.region.create("key1", "value2");
        fail("expected EntryExistsException");
      } catch (EntryExistsException expected) {
      }
      this.txMgr.rollback();
    } catch (CacheException ex) {
      fail("unexpected " + ex);
    }
  }

  @Test
  public void testListener() {
    assertTrue(this.txMgr.getListener() == null);
    TransactionListener oldListener = this.txMgr.setListener(new TransactionListener() {
      @Override
      public void afterCommit(TransactionEvent event) {
        listenerAfterCommit = 1;
        te = event;
      }

      @Override
      public void afterFailedCommit(TransactionEvent event) {
        listenerAfterFailedCommit = 1;
        te = event;
      }

      @Override
      public void afterRollback(TransactionEvent event) {
        listenerAfterRollback = 1;
        te = event;
      }

      @Override
      public void close() {
        listenerClose = 1;
      }
    });
    assertTrue(oldListener == null);
    this.txMgr.begin();
    TransactionId myTxId = this.txMgr.getTransactionId();
    assertEquals(0, this.listenerAfterRollback);
    this.txMgr.rollback();
    assertEquals(1, this.listenerAfterRollback);
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(0, this.te.getEvents().size());
    assertEquals(myTxId, this.te.getTransactionId());

    this.txMgr.begin();
    myTxId = this.txMgr.getTransactionId();
    try {
      assertEquals(0, this.listenerAfterCommit);
      this.txMgr.commit();
    } catch (CommitConflictException unexpected) {
      fail("did not expect " + unexpected);
    }
    assertEquals(1, this.listenerAfterCommit);
    assertEquals(0, TxEventTestUtil.getCreateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getPutEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getInvalidateEvents(this.te.getEvents()).size());
    assertEquals(0, TxEventTestUtil.getDestroyEvents(this.te.getEvents()).size());
    assertEquals(0, this.te.getEvents().size());
    assertEquals(myTxId, this.te.getTransactionId());

    assertEquals(0, this.listenerClose);
    oldListener = this.txMgr.setListener(new TransactionListener() {
      @Override
      public void afterCommit(TransactionEvent event) {
        listenerAfterCommit = 2;
        te = event;
      }

      @Override
      public void afterFailedCommit(TransactionEvent event) {
        listenerAfterFailedCommit = 2;
      }

      @Override
      public void afterRollback(TransactionEvent event) {
        listenerAfterRollback = 2;
        te = event;
      }

      @Override
      public void close() {
        listenerClose = 2;
      }
    });
    assertEquals(1, this.listenerClose);

    this.txMgr.begin();
    assertEquals(1, this.listenerAfterRollback);
    this.txMgr.rollback();
    assertEquals(2, this.listenerAfterRollback);
    this.txMgr.begin();
    this.txMgr.setListener(oldListener);
    assertEquals(2, this.listenerClose);
    this.txMgr.rollback();
    assertEquals(1, this.listenerAfterRollback);

    closeCache();
    assertEquals(1, this.listenerClose);
  }

  // make sure standard Cache(Listener,Writer)
  // are not called during rollback
  @Test
  public void testNoCallbacksOnRollback() throws CacheException {
    // install listeners
    AttributesMutator<String, String> mutator = this.region.getAttributesMutator();
    mutator.addCacheListener(new CacheListenerAdapter<String, String>() {
      @Override
      public void close() {
        cbCount++;
      }

      @Override
      public void afterCreate(EntryEvent event) {
        cbCount++;
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        cbCount++;
      }

      @Override
      public void afterInvalidate(EntryEvent event) {
        cbCount++;
      }

      @Override
      public void afterDestroy(EntryEvent event) {
        cbCount++;
      }

      @Override
      public void afterRegionInvalidate(RegionEvent event) {
        cbCount++;
      }

      @Override
      public void afterRegionDestroy(RegionEvent event) {
        cbCount++;
      }
    });
    mutator.setCacheWriter(new CacheWriter<String, String>() {
      @Override
      public void close() {
        cbCount++;
      }

      @Override
      public void beforeUpdate(EntryEvent event) throws CacheWriterException {
        cbCount++;
      }

      @Override
      public void beforeCreate(EntryEvent event) throws CacheWriterException {
        cbCount++;
      }

      @Override
      public void beforeDestroy(EntryEvent event) throws CacheWriterException {
        cbCount++;
      }

      @Override
      public void beforeRegionDestroy(RegionEvent event) throws CacheWriterException {
        cbCount++;
      }

      @Override
      public void beforeRegionClear(RegionEvent event) throws CacheWriterException {
        cbCount++;
      }
    });

    this.txMgr.begin();
    this.region.create("key1", "value1");
    this.cbCount = 0;
    this.txMgr.rollback();
    assertEquals(0, this.cbCount);

    this.cbCount = 0;
    this.region.create("key1", "value1");
    // do a santify check to make sure callbacks are installed
    assertEquals(2, this.cbCount); // 2 -> 1writer + 1listener

    this.txMgr.begin();
    this.region.put("key1", "value2");
    this.cbCount = 0;
    this.txMgr.rollback();
    assertEquals(0, this.cbCount);
    this.region.localDestroy("key1");

    this.region.create("key1", "value1");
    this.txMgr.begin();
    this.region.localDestroy("key1");
    this.cbCount = 0;
    this.txMgr.rollback();
    assertEquals(0, this.cbCount);

    this.region.put("key1", "value1");
    this.txMgr.begin();
    this.region.destroy("key1");
    this.cbCount = 0;
    this.txMgr.rollback();
    assertEquals(0, this.cbCount);

    this.region.put("key1", "value1");
    this.txMgr.begin();
    this.region.localInvalidate("key1");
    this.cbCount = 0;
    this.txMgr.rollback();
    assertEquals(0, this.cbCount);
    this.region.localDestroy("key1");

    this.region.put("key1", "value1");
    this.txMgr.begin();
    this.region.invalidate("key1");
    this.cbCount = 0;
    this.txMgr.rollback();
    assertEquals(0, this.cbCount);

    this.region.localDestroy("key1");
  }

  /**
   * TXCallBackValidator is a container for holding state for validating Cache callbacks
   */
  private class TXCallBackValidator {
    boolean passedValidation;
    boolean suspendValidation;
    int expectedCallCount;
    Object key;
    Object oldVal;
    boolean oldValIdentCheck;
    Object newVal;
    boolean newValIdentCheck;
    TransactionId txId;
    boolean isDistributed;
    boolean isLocalLoad;
    boolean isCreate;
    boolean isUpdate;
    boolean isDestroyed;
    boolean isInvalidate;
    Object callBackArg;

    /**
     * EntryEvent, CallCount validator for callbacks (CacheWriter, CacheListener
     */
    boolean validate(EntryEvent event, int cnt) {
      if (this.isSuspendValidation()) {
        return true;
      }
      this.passedValidation = false;
      assertEquals("Expected Call Count Assertion!", this.expectedCallCount, cnt);

      assertTrue(!event.getOperation().isExpiration());
      assertTrue(!event.getOperation().isNetLoad());
      assertEquals("isLoad Assertion!", this.isLoad(), event.getOperation().isLoad());
      assertEquals("isLocalLoad Assertion!", this.isLoad(), event.getOperation().isLocalLoad());
      assertTrue(!event.getOperation().isNetSearch());
      assertTrue(!event.isOriginRemote());
      assertNotNull(event.getRegion());
      assertNotNull(event.getRegion().getCache());
      assertNotNull(event.getRegion().getCache().getCacheTransactionManager());
      assertEquals(this.getTXId(), event.getTransactionId());

      if (!isPR())
        assertEquals("IsDistributed Assertion!", this.isDistributed(),
            event.getOperation().isDistributed());
      assertEquals(this.getKey(), event.getKey());
      assertSame(this.getCallBackArg(), event.getCallbackArgument());
      if (newValIdentCheck) {
        assertSame(newVal, event.getNewValue());
      } else {
        assertEquals(newVal, event.getNewValue());
      }
      if (oldValIdentCheck) {
        assertSame(oldVal, event.getOldValue());
      } else {
        assertEquals(oldVal, event.getOldValue());
      }
      this.passedValidation = true;
      return true;
    }

    int setExpectedCount(int newVal) {
      int oldVal = this.expectedCallCount;
      this.expectedCallCount = newVal;
      return oldVal;
    }

    void setKey(Object key) {
      this.key = key;
    }

    Object getKey() {
      return this.key;
    }

    void setOldValue(Object val, boolean checkWithIdentity) {
      this.oldVal = val;
      this.oldValIdentCheck = checkWithIdentity;
    }

    Object getOldValue() {
      return this.oldVal;
    }

    void setNewValue(Object val, boolean checkWithIdentity) {
      this.newVal = val;
      this.newValIdentCheck = checkWithIdentity;
    }

    Object getNewValue() {
      return this.newVal;
    }

    TransactionId setTXId(TransactionId txId) {
      TransactionId old = this.txId;
      this.txId = txId;
      return old;
    }

    TransactionId getTXId() {
      return this.txId;
    }

    void setIsDistributed(boolean isDistributed) {
      this.isDistributed = isDistributed;
    }

    Object getCallBackArg() {
      return this.callBackArg;
    }

    void setCallBackArg(Object callBackArg) {
      this.callBackArg = callBackArg;
    }

    boolean isDistributed() {
      return this.isDistributed;
    }

    void setIsCreate(boolean isCreate) {
      this.isCreate = isCreate;
    }

    boolean isCreate() {
      return this.isCreate;
    }

    void setIsUpdate(boolean isUpdate) {
      this.isUpdate = isUpdate;
    }

    boolean isUpdate() {
      return this.isUpdate;
    }

    void setIsDestroy(boolean isDestroyed) {
      this.isDestroyed = isDestroyed;
    }

    boolean isDestroy() {
      return this.isDestroyed;
    }

    void setIsInvalidate(boolean isInvalidate) {
      this.isInvalidate = isInvalidate;
    }

    boolean isInvalidate() {
      return this.isInvalidate;
    }

    void setIsLoad(boolean isLoad) {
      this.isLocalLoad = isLoad;
    }

    boolean isLoad() {
      return this.isLocalLoad;
    }

    boolean suspendValidation(boolean toggle) {
      boolean oldVal = this.suspendValidation;
      this.suspendValidation = toggle;
      return oldVal;
    }

    boolean isSuspendValidation() {
      return this.suspendValidation;
    }

    void setPassedValidation(boolean passedValidation) {
      this.passedValidation = passedValidation;
    }

    boolean passedValidation() {
      return this.passedValidation;
    }
  }

  private interface ValidatableCacheListener extends CacheListener {
    void setValidator(TXCallBackValidator v);

    void validate();

    void validateNoEvents();

    void reset();

    void setExpectedCount(int count);

    int getCallCount();
  }

  private interface ValidatableCacheWriter extends CacheWriter {
    void setValidator(TXCallBackValidator v);

    int getCallCount();

    void localDestroyMakeup(int count);

    void validate();

    void reset();

    void validateNoEvents();
  }

  /**
   * Test to make sure CacheListener callbacks are called in place with the CacheEvents properly
   * constructed
   */
  @Test
  public void testCacheCallbacks() throws CacheException {
    final String key1 = "Key1";
    final String value1 = "value1";
    final String value2 = "value2";
    final String callBackArg = "call back arg";
    // install listeners
    AttributesMutator<String, String> mutator = this.region.getAttributesMutator();

    TXCallBackValidator cbv = new TXCallBackValidator();

    // Cache Listener
    ValidatableCacheListener vCl = new ValidatableCacheListener() {
      TXCallBackValidator v;
      int callCount;
      int prevCallCount;
      EntryEvent lastEvent;

      @Override
      public void validate() {
        this.v.validate(this.lastEvent, this.callCount);
      }

      void validate(EntryEvent event) {
        this.v.validate(event, ++this.callCount);
      }

      @Override
      public void setValidator(TXCallBackValidator v) {
        this.v = v;
      }

      @Override
      public void close() {}

      @Override
      public void afterCreate(EntryEvent event) {
        lastEvent = event;
        if (this.v.isSuspendValidation()) {
          return;
        }
        this.validate(event);
        this.v.setPassedValidation(false);
        assertTrue("IsCreate Assertion!", this.v.isCreate());
        assertTrue(event.getRegion().containsKey(this.v.getKey()));
        assertTrue(event.getRegion().containsValueForKey(this.v.getKey()));
        assertNotNull(event.getRegion().getEntry(event.getKey()).getValue());
        this.v.setPassedValidation(true);
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        lastEvent = event;
        if (this.v.isSuspendValidation()) {
          return;
        }
        validate(event);
        this.v.setPassedValidation(false);
        assertTrue("IsUpdate Assertion!", this.v.isUpdate());
        assertTrue(event.getRegion().containsKey(this.v.getKey()));
        assertTrue(event.getRegion().containsValueForKey(this.v.getKey()));
        assertNotNull(event.getRegion().getEntry(event.getKey()).getValue());
        this.v.setPassedValidation(true);
      }

      @Override
      public void afterInvalidate(EntryEvent event) {
        lastEvent = event;
        if (this.v.isSuspendValidation()) {
          return;
        }
        validate(event);
        this.v.setPassedValidation(false);
        assertTrue("IsInvalidate Assertion!", this.v.isInvalidate());
        assertTrue(event.getRegion().containsKey(this.v.getKey()));
        assertTrue(!event.getRegion().containsValueForKey(this.v.getKey()));
        assertNull(event.getRegion().getEntry(event.getKey()).getValue());
        this.v.setPassedValidation(true);
      }

      @Override
      public void afterDestroy(EntryEvent event) {
        lastEvent = event;
        if (this.v.isSuspendValidation()) {
          return;
        }
        validate(event);
        this.v.setPassedValidation(false);
        assertTrue("IsDestroy Assertion!", this.v.isDestroy());
        assertTrue(!event.getRegion().containsKey(this.v.getKey()));
        assertTrue(!event.getRegion().containsValueForKey(this.v.getKey()));
        assertNull(event.getRegion().getEntry(event.getKey()));
        this.v.setPassedValidation(true);
      }

      @Override
      public void afterRegionInvalidate(RegionEvent event) {
        fail("Unexpected invocation of afterRegionInvalidate");
      }

      @Override
      public void afterRegionDestroy(RegionEvent event) {
        if (!event.getOperation().isClose()) {
          fail("Unexpected invocation of afterRegionDestroy");
        }
      }

      @Override
      public void afterRegionClear(RegionEvent event) {}

      @Override
      public void afterRegionCreate(RegionEvent event) {}

      @Override
      public void afterRegionLive(RegionEvent event) {}

      @Override
      public void reset() {
        lastEvent = null;
        prevCallCount = callCount;
      }

      @Override
      public void validateNoEvents() {
        assertNull("Did not expect listener callback", lastEvent);
        assertEquals(prevCallCount, callCount);
      }

      @Override
      public void setExpectedCount(int count) {
        callCount = count;
      }

      @Override
      public int getCallCount() {
        return callCount;
      }
    };

    vCl.setValidator(cbv);
    mutator.addCacheListener(vCl);

    // CacheWriter
    ValidatableCacheWriter vCw = new ValidatableCacheWriter() {
      TXCallBackValidator v;
      int callCount;
      int prevCallCount;
      EntryEvent lastEvent;

      @Override
      public int getCallCount() {
        return this.callCount;
      }

      @Override
      public void localDestroyMakeup(int count) {
        this.callCount += count;
      }

      @Override
      public void validate() {
        this.v.validate(this.lastEvent, this.callCount);
      }

      void validate(EntryEvent event) {
        this.v.validate(event, ++this.callCount);
      }

      @Override
      public void setValidator(TXCallBackValidator v) {
        this.v = v;
      }

      @Override
      public void close() {}

      @Override
      public void beforeCreate(EntryEvent event) {
        lastEvent = event;
        if (this.v.isSuspendValidation()) {
          return;
        }
        validate(event);
        this.v.setPassedValidation(false);
        assertTrue("IsCreate Assertion!", this.v.isCreate());
        assertTrue(!event.getRegion().containsKey(this.v.getKey()));
        assertTrue(!event.getRegion().containsValueForKey(this.v.getKey()));
        assertNull(event.getRegion().getEntry(event.getKey()));
        this.v.setPassedValidation(true);
      }

      @Override
      public void beforeUpdate(EntryEvent event) {
        lastEvent = event;
        if (this.v.isSuspendValidation()) {
          return;
        }
        validate(event);
        this.v.setPassedValidation(false);
        assertTrue("IsUpdate Assertion!", this.v.isUpdate());
        assertTrue(event.getRegion().containsKey(this.v.getKey()));
        // Can not assert the following line, as the value being update may be invalid
        // assertTrue(event.getRegion().containsValueForKey(this.v.getKey()));
        this.v.setPassedValidation(true);
      }

      @Override
      public void beforeDestroy(EntryEvent event) {
        lastEvent = event;
        if (this.v.isSuspendValidation()) {
          return;
        }
        validate(event);
        this.v.setPassedValidation(false);
        assertTrue("IsDestroy Assertion!", this.v.isDestroy());
        assertTrue(event.getRegion().containsKey(this.v.getKey()));
        this.v.setPassedValidation(true);
      }

      @Override
      public void beforeRegionDestroy(RegionEvent event) {
        fail("Unexpected invocation of beforeRegionDestroy");
      }

      @Override
      public void beforeRegionClear(RegionEvent event) {
        fail("Unexpected invocation of beforeRegionClear");
      }

      @Override
      public void reset() {
        lastEvent = null;
        prevCallCount = callCount;
      }

      @Override
      public void validateNoEvents() {
        assertNull("Did not expect a writer event", lastEvent);
        assertEquals(prevCallCount, callCount);
      }
    };
    vCw.setValidator(cbv);
    mutator.setCacheWriter(vCw);

    // Cache Loader
    mutator.setCacheLoader(new CacheLoader() {
      int count = 0;

      @Override
      public Object load(LoaderHelper helper) throws CacheLoaderException {
        return count++;
      }

      @Override
      public void close() {}
    });

    // Use this to track the number of callout method invocations
    int appCallCount = 1;

    // Create => beforeCreate/afterCreate tests
    cbv.setKey(key1);
    cbv.setCallBackArg(callBackArg);
    cbv.setNewValue(value1, false);
    cbv.setOldValue(null, true);
    cbv.setIsDistributed(true);
    cbv.setIsLoad(false);
    cbv.setIsCreate(true);
    cbv.setIsUpdate(false);
    // Test non-transactional create expecting beforeCreate/afterCreate call
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    this.region.create(key1, value1, callBackArg);
    assertTrue("Non-TX Create Validation Assertion", cbv.passedValidation());
    cbv.suspendValidation(true);
    this.region.localDestroy(key1);
    cbv.suspendValidation(false);
    // Test transactional create expecting afterCreate call
    this.txMgr.begin();
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    this.region.create(key1, value1, callBackArg);
    this.txMgr.commit();
    assertTrue("TX Create Validation Assertion", cbv.passedValidation());
    cbv.suspendValidation(true);
    this.region.localDestroy(key1);

    // Put => afterCreate tests
    cbv.suspendValidation(false);
    cbv.setNewValue(value2, false);
    cbv.setOldValue(null, true);
    cbv.setIsDistributed(true);
    cbv.setIsLoad(false);
    cbv.setIsCreate(true);
    cbv.setIsUpdate(false);
    // Test non-transactional put expecting afterCreate call due to no
    // previous Entry
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    this.region.put(key1, value2, callBackArg);
    assertTrue("Non-TX Put->Create Validation Assertion", cbv.passedValidation());
    cbv.suspendValidation(true);
    this.region.localDestroy(key1);
    cbv.suspendValidation(false);
    // Test transactional put expecting afterCreate call due to no
    // previous Entry
    this.txMgr.begin();
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    this.region.put(key1, value2, callBackArg);
    this.txMgr.commit();
    assertTrue("TX Put->Create Validation Assertion", cbv.passedValidation());

    // Put => afterUpdate tests
    cbv.setNewValue(value1, false);
    cbv.setOldValue(value2, false);
    cbv.setIsDistributed(true);
    cbv.setIsLoad(false);
    cbv.setIsCreate(false);
    cbv.setIsUpdate(true);
    // Test non-transactional put expecting afterUpdate call due to
    // previous Entry
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    this.region.put(key1, value1, callBackArg);
    assertTrue("Non-TX Put->Update Validation Assertion", cbv.passedValidation());
    cbv.suspendValidation(true);
    this.region.localDestroy(key1);
    this.region.put(key1, value2);
    cbv.suspendValidation(false);
    // Test transactional put expecting afterUpdate call due to
    // previous Entry
    this.txMgr.begin();
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    this.region.put(key1, value1, callBackArg);
    this.txMgr.commit();
    assertTrue("TX Put->Update Validation Assertion", cbv.passedValidation());

    // LocalDestroy => afterDestroy, non-distributed tests
    cbv.setNewValue(null, true);
    cbv.setOldValue(value1, false);
    cbv.setIsDistributed(false);
    cbv.setIsLoad(false);
    cbv.setIsDestroy(true);
    cbv.setIsUpdate(false);
    // Test non-transactional localDestroy, expecting afterDestroy,
    // non-distributed
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    this.region.localDestroy(key1, callBackArg);
    if (!isPR())
      vCw.localDestroyMakeup(1); // Account for cacheWriter not begin called
    assertTrue("Non-TX LocalDestroy Validation Assertion", cbv.passedValidation());
    cbv.suspendValidation(true);
    this.region.create(key1, value1);
    cbv.suspendValidation(false);
    // Test transactional localDestroy expecting afterDestroy,
    // non-distributed
    this.txMgr.begin();
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    this.region.localDestroy(key1, callBackArg);
    if (!isPR())
      vCw.localDestroyMakeup(1); // Account for cacheWriter not begin called
    this.txMgr.commit();
    assertTrue("TX LocalDestroy Validation Assertion", cbv.passedValidation());

    // Destroy => afterDestroy, distributed tests
    cbv.setNewValue(null, true);
    cbv.setOldValue(value1, false);
    cbv.setIsDistributed(true);
    cbv.setIsLoad(false);
    cbv.setIsDestroy(true);
    cbv.suspendValidation(true);
    this.region.create(key1, value1);
    cbv.suspendValidation(false);
    // Test non-transactional Destroy, expecting afterDestroy,
    // distributed
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    this.region.destroy(key1, callBackArg);
    assertTrue("Non-TX Destroy Validation Assertion", cbv.passedValidation());
    cbv.suspendValidation(true);
    this.region.create(key1, value1);
    cbv.suspendValidation(false);
    // Test transactional Destroy, expecting afterDestroy,
    // distributed
    this.txMgr.begin();
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    this.region.destroy(key1, callBackArg);
    this.txMgr.commit();
    assertTrue("TX Destroy Validation Assertion", cbv.passedValidation());

    // localInvalidate => afterInvalidate, non-distributed tests
    cbv.setNewValue(null, true);
    cbv.setOldValue(value1, false);
    cbv.setIsDistributed(false);
    cbv.setIsLoad(false);
    cbv.setIsInvalidate(true);
    cbv.setIsDestroy(false);
    cbv.suspendValidation(true);
    this.region.create(key1, value1);
    cbv.suspendValidation(false);
    // Test non-transactional localInvalidate, expecting afterInvalidate
    // non-distributed
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    this.region.localInvalidate(key1, callBackArg);
    assertTrue("Non-TX LocalInvalidate Validation Assertion", cbv.passedValidation());
    vCw.localDestroyMakeup(1); // Account for cacheWriter not begin called
    cbv.suspendValidation(true);
    this.region.put(key1, value1);
    cbv.suspendValidation(false);
    // Test transactional localInvalidate, expecting afterInvalidate
    // non-distributed
    this.txMgr.begin();
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    this.region.localInvalidate(key1, callBackArg);
    this.txMgr.commit();
    assertTrue("TX LocalInvalidate Validation Assertion", cbv.passedValidation());
    vCw.localDestroyMakeup(1); // Account for cacheWriter not begin called
    cbv.suspendValidation(true);
    this.region.localDestroy(key1);

    // Invalidate => afterInvalidate, distributed tests
    cbv.setNewValue(null, true);
    cbv.setOldValue(value1, false);
    cbv.setIsDistributed(true);
    cbv.setIsLoad(false);
    cbv.suspendValidation(true);
    this.region.create(key1, value1);
    cbv.suspendValidation(false);
    // Test non-transactional Invalidate, expecting afterInvalidate
    // distributed
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    this.region.invalidate(key1, callBackArg);
    vCw.localDestroyMakeup(1); // Account for cacheWriter not begin called
    assertTrue("Non-TX Invalidate Validation Assertion", cbv.passedValidation());
    cbv.suspendValidation(true);
    this.region.put(key1, value1);
    cbv.suspendValidation(false);
    // Test transactional Invalidate, expecting afterInvalidate
    // distributed
    this.txMgr.begin();
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    this.region.invalidate(key1, callBackArg);
    this.txMgr.commit();
    vCw.localDestroyMakeup(1); // Account for cacheWriter not begin called
    assertTrue("TX Invalidate Validation Assertion", cbv.passedValidation());
    cbv.suspendValidation(true);
    this.region.localDestroy(key1);
    cbv.suspendValidation(false);

    // Create load Event tests
    int loaderValCheck = 0;
    cbv.setNewValue(loaderValCheck++, false);
    cbv.setCallBackArg(null);
    cbv.setOldValue(null, false);
    cbv.setIsDistributed(true);
    cbv.setIsCreate(true);
    cbv.setIsUpdate(false);
    cbv.setIsLoad(true);
    // Test non-transactional load, expecting afterCreate distributed
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    this.region.get(key1);
    assertTrue("Non-TX Invalidate Validation Assertion", cbv.passedValidation());
    vCl.validate();
    vCw.validate();
    cbv.suspendValidation(true);
    this.region.localDestroy(key1);
    cbv.suspendValidation(false);
    // Test transactional load, expecting afterCreate distributed
    vCl.reset();
    this.txMgr.begin();
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setNewValue(loaderValCheck++, false);
    cbv.setExpectedCount(appCallCount++);
    this.region.get(key1);
    this.txMgr.rollback();
    assertTrue("TX Invalidate Validation Assertion", cbv.passedValidation());
    vCw.validate();
    vCl.validateNoEvents();
    vCl.setExpectedCount(vCl.getCallCount() + 1);

    this.txMgr.begin();
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setNewValue(loaderValCheck++, false);
    cbv.setExpectedCount(appCallCount++);
    this.region.get(key1);
    vCw.validate();
    vCw.reset();
    this.txMgr.commit();
    vCw.validateNoEvents();
    assertTrue("TX Invalidate Validation Assertion", cbv.passedValidation());
    vCl.validate();
    cbv.suspendValidation(true);
    this.region.localDestroy(key1);
    cbv.suspendValidation(false);

    // Update load Event tests
    cbv.suspendValidation(true);
    this.region.create(key1, null);
    cbv.suspendValidation(false);
    assertTrue(this.region.containsKey(key1));
    assertTrue(!this.region.containsValueForKey(key1));
    cbv.setNewValue(loaderValCheck++, false);
    cbv.setOldValue(null, false);
    cbv.setIsDistributed(true);
    cbv.setCallBackArg(null);
    cbv.setIsCreate(false);
    cbv.setIsUpdate(true);
    cbv.setIsLoad(true);
    // Test non-transactional load, expecting afterUpdate distributed
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    this.region.get(key1);
    assertTrue("Non-TX Invalidate Validation Assertion", cbv.passedValidation());
    vCw.validate();
    vCl.validate();
    cbv.suspendValidation(true);
    this.region.invalidate(key1);
    cbv.suspendValidation(false);
    assertTrue(this.region.containsKey(key1));
    assertTrue(!this.region.containsValueForKey(key1));
    // Test transactional load, expecting afterUpdate distributed
    this.txMgr.begin();
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    cbv.setNewValue(loaderValCheck++, false);
    this.region.get(key1);
    vCw.validate();
    vCw.reset();
    this.txMgr.commit();
    vCw.validateNoEvents();
    vCl.validate();

    cbv.suspendValidation(true);
    this.region.invalidate(key1);
    cbv.suspendValidation(false);
    vCl.reset();
    this.txMgr.begin();
    cbv.setTXId(txMgr.getTransactionId());
    cbv.setExpectedCount(appCallCount++);
    cbv.setNewValue(loaderValCheck++, false);
    this.region.get(key1);
    this.txMgr.rollback();
    assertTrue("TX Invalidate Validation Assertion", cbv.passedValidation());
    vCw.validate();
    vCl.validateNoEvents();
  }

  @Test
  public void testEntryCount() throws CacheException {
    LocalRegion reg1 = (LocalRegion) this.region;

    assertEquals(0, reg1.entryCount());
    reg1.create("key1", "value1");
    assertEquals(1, reg1.entryCount());
    reg1.create("key2", "value2");
    assertEquals(2, reg1.entryCount());
    reg1.localDestroy("key2");
    assertEquals(1, reg1.entryCount());

    reg1.create("key3", "value3");
    assertEquals(2, reg1.entryCount());

    this.txMgr.begin();
    this.region.create("key2", "value2");
    assertEquals(3, reg1.entryCount());
    reg1.localDestroy("key2");
    assertEquals(2, reg1.entryCount());

    this.region.create("key2", "value2");
    assertEquals(3, reg1.entryCount());
    reg1.destroy("key2");
    assertEquals(2, reg1.entryCount());

    reg1.localInvalidate("key1");
    assertEquals(2, reg1.entryCount());
    reg1.put("key1", "value2");
    assertEquals(2, reg1.entryCount());
    this.region.create("key2", "value2");
    assertEquals(3, reg1.entryCount());

    reg1.localDestroy("key3");
    assertEquals(2, reg1.entryCount());
    reg1.create("key3", "value3");
    assertEquals(3, reg1.entryCount());

    this.txMgr.rollback();

    assertEquals(2, reg1.entryCount());
  }

  private void checkCollectionSize(int expectedSize) {
    checkCollectionSize(expectedSize, expectedSize, expectedSize);
  }

  private void checkCollectionSize(int expectedSize, int expectedRecSize) {
    checkCollectionSize(expectedSize, expectedRecSize, expectedSize);
  }

  private void checkCollectionSize(int expectedSize, int expectedRecSize, int expectedValuesSize) {
    int size = 0;
    for (Object key : this.region.entrySet(false)) {
      size++;
    }
    assertEquals(expectedSize, size);
    assertEquals(expectedSize, this.region.keySet().size());
    assertEquals(expectedValuesSize, this.region.values().size());
    assertEquals(expectedSize, this.region.entrySet(false).size());
    assertEquals(expectedRecSize, this.region.entrySet(true).size());
  }

  @Test
  public void testCollections() throws CacheException {
    Region<String, String> reg1 = this.region;

    checkSubRegionCollection(reg1);

    {
      Collection nonTxKeys = reg1.keySet();
      Collection nonTxValues = reg1.values();
      this.txMgr.begin();
      reg1.create("key1", "value1");
      Collection txKeys = reg1.keySet();
      Collection txValues = reg1.values();
      /*
       * [sumedh] No longer fail this scenario to avoid the overhead of ThreadLocal lookup in every
       * iteration. Besides does not look to be a harmful usage in any case.
       */
      try {
        nonTxKeys.size();
        fail();
      } catch (IllegalStateException expected) {
        TransactionId txid = this.txMgr.getTransactionId();
        assertEquals(
            String.format(
                "The Region collection is not transactional but is being used in a transaction %s.",
                txid),
            expected.getMessage());
      }
      assertEquals(1, txKeys.size());
      try {
        nonTxValues.size();
        fail();
      } catch (IllegalStateException expected) {
        TransactionId txid = this.txMgr.getTransactionId();
        assertEquals(
            String.format(
                "The Region collection is not transactional but is being used in a transaction %s.",
                txid),
            expected.getMessage());
      }
      assertEquals(1, txValues.size());
      assertTrue(txKeys.contains("key1"));
      {
        Iterator txIt = txKeys.iterator();
        assertTrue(txIt.hasNext());
        assertTrue(txIt.hasNext());
        assertEquals("key1", txIt.next());
        assertTrue(!txIt.hasNext());
      }
      assertTrue(txValues.contains("value1"));
      {
        Iterator txIt = txValues.iterator();
        assertTrue(txIt.hasNext());
        assertTrue(txIt.hasNext());
        assertEquals("value1", txIt.next());
        assertTrue(!txIt.hasNext());
      }
      reg1.invalidate("key1");
      assertEquals(1, txKeys.size());
      assertEquals(0, txValues.size());
      assertTrue(txKeys.contains("key1"));
      assertTrue(!txValues.contains("value1"));
      reg1.create("key2", "value2");
      reg1.create("key3", "value3");
      assertEquals(3, txKeys.size());
      assertEquals(2, txValues.size());
      reg1.put("key1", "value1");
      assertEquals(3, txKeys.size());
      assertEquals(3, txValues.size());
      reg1.localInvalidate("key2");
      assertEquals(2, txValues.size());
      reg1.invalidate("key1");
      assertEquals(1, txValues.size());
      reg1.destroy("key2");
      reg1.destroy("key3");
      assertEquals(1, txKeys.size());

      reg1.destroy("key1");
      assertEquals(0, txKeys.size());
      assertTrue(!txKeys.contains("key1"));
      Iterator txIt = txKeys.iterator();
      assertTrue(!txIt.hasNext());
      this.txMgr.rollback();
      try {
        txKeys.size();
        fail("expected IllegalStateException");
      } catch (IllegalStateException ok) {
      }
      try {
        txKeys.isEmpty();
        fail("expected IllegalStateException");
      } catch (IllegalStateException ok) {
      }
      try {
        txKeys.contains("key1");
        fail("expected IllegalStateException");
      } catch (IllegalStateException ok) {
      }
      try {
        txKeys.iterator();
        fail("expected IllegalStateException");
      } catch (IllegalStateException ok) {
      }
      txIt.hasNext();
    }
    {
      this.txMgr.begin();
      reg1.create("key1", "value1");
      Collection txValues = reg1.values();
      assertEquals(1, txValues.size());
      assertTrue(txValues.contains("value1"));
      {
        Iterator txIt = txValues.iterator();
        assertTrue(txIt.hasNext());
        assertEquals("value1", txIt.next());
        assertTrue(!txIt.hasNext());
      }
      reg1.destroy("key1");
      assertEquals(0, txValues.size());
      assertTrue(!txValues.contains("value1"));
      assertTrue(!txValues.iterator().hasNext());
      assertTrue(!txValues.iterator().hasNext());
      this.txMgr.rollback();
    }
    {
      Collection nonTxEntries = reg1.entrySet(false);
      this.txMgr.begin();
      reg1.create("key1", "value1");
      Collection txEntries = reg1.entrySet(false);
      // non-TX collections can now be used in a transactional context
      try {
        nonTxEntries.size();
        fail();
      } catch (IllegalStateException expected) {
        TransactionId txid = this.txMgr.getTransactionId();
        assertEquals(
            String.format(
                "The Region collection is not transactional but is being used in a transaction %s.",
                txid),
            expected.getMessage());
      }
      assertEquals(1, txEntries.size());
      assertTrue(txEntries.contains(reg1.getEntry("key1")));
      {
        Iterator txIt = txEntries.iterator();
        assertTrue(txIt.hasNext());
        assertTrue(txIt.hasNext());
        assertEquals(reg1.getEntry("key1"), txIt.next());
        assertTrue(!txIt.hasNext());
        assertTrue(!txIt.hasNext());
      }
      reg1.destroy("key1");
      assertEquals(0, txEntries.size());
      assertTrue(!txEntries.iterator().hasNext());
      this.txMgr.rollback();
    }
    {
      Collection nonTxKeys = reg1.keySet();
      Collection nonTxValues = reg1.values();

      assertEquals(0, nonTxKeys.size());
      assertEquals(0, nonTxValues.size());
      reg1.create("key1", "value1");
      assertEquals(1, nonTxKeys.size());
      assertEquals(1, nonTxValues.size());
      reg1.invalidate("key1");
      assertEquals(1, nonTxKeys.size());
      assertEquals(0, nonTxValues.size());

      this.txMgr.begin();
      Collection txKeys = reg1.keySet();
      Collection txValues = reg1.values();
      assertEquals(1, txKeys.size());
      assertEquals(0, txValues.size());
      reg1.put("key1", "txValue1");
      assertEquals(1, txKeys.size());
      assertEquals(1, txValues.size());
      assertTrue(txValues.iterator().hasNext());
      assertEquals("txValue1", txValues.iterator().next());
      // non-TX collections can now be used in a transactional context
      try {
        nonTxValues.iterator().hasNext();
      } catch (IllegalStateException expected) {
        TransactionId txid = this.txMgr.getTransactionId();
        assertEquals(
            String.format(
                "The Region collection is not transactional but is being used in a transaction %s.",
                txid),
            expected.getMessage());
      }

      reg1.localInvalidate("key1");
      assertEquals(0, txValues.size());
      assertTrue(!txValues.iterator().hasNext());
      this.txMgr.rollback();
    }
  }

  protected void checkSubRegionCollection(Region<String, String> reg1) {
    AttributesFactory<String, String> attributesFactory = new AttributesFactory<>();
    attributesFactory.setScope(Scope.DISTRIBUTED_NO_ACK);

    Region<String, String> sub1 =
        this.region.createSubregion("collectionSub1", attributesFactory.create());

    attributesFactory = new AttributesFactory<>();

    Region<String, String> sub2 =
        this.region.createSubregion("collectionSub2", attributesFactory.create());

    attributesFactory = new AttributesFactory<>();
    attributesFactory.setScope(Scope.LOCAL);

    Region<String, String> sub2_1 =
        sub2.createSubregion("collectionSub2_1", attributesFactory.create());

    checkCollectionSize(0);
    try {
      this.region.keySet().iterator().next();
      fail();
    } catch (NoSuchElementException expected) {
      assertNull(expected.getMessage());
    }
    try {
      this.region.values().iterator().next();
      fail();
    } catch (NoSuchElementException expected) {
      assertNull(expected.getMessage());
    }
    try {
      this.region.entrySet().iterator().next();
      fail();
    } catch (NoSuchElementException expected) {
      assertNull(expected.getMessage());
    }
    reg1.create("key1", "value1");
    checkCollectionSize(1);
    {
      Iterator it = this.region.keySet().iterator();
      it.next();
      try {
        it.next();
        fail();
      } catch (NoSuchElementException expected) {
        assertNull(expected.getMessage());
      }
    }
    {
      Iterator it = this.region.values().iterator();
      it.next();
      try {
        it.next();
        fail();
      } catch (NoSuchElementException expected) {
        assertNull(expected.getMessage());
      }
    }
    {
      Iterator it = this.region.entrySet().iterator();
      it.next();
      try {
        it.next();
        fail();
      } catch (NoSuchElementException expected) {
        assertNull(expected.getMessage());
      }
    }

    reg1.create("key2", "value2");
    checkCollectionSize(2);
    reg1.localInvalidate("key2");
    checkCollectionSize(2, 2, 1);
    reg1.localInvalidate("key1");
    checkCollectionSize(2, 2, 0);
    reg1.localDestroy("key2");
    checkCollectionSize(1, 1, 0);
    reg1.localDestroy("key1");
    checkCollectionSize(0);


    // Non-TX recursive checks
    sub2_1.create("key6", "value6");
    checkCollectionSize(0, 1);
    assertEquals(0, sub2.entrySet(false).size());
    assertEquals(1, sub2.entrySet(true).size());
    assertEquals(1, sub2_1.entrySet(true).size());
    sub2.create("key5", "value5");
    checkCollectionSize(0, 2);
    assertEquals(1, sub2.entrySet(false).size());
    assertEquals(2, sub2.entrySet(true).size());
    sub1.create("key4", "value4");
    checkCollectionSize(0, 3);
    assertEquals(1, sub1.entrySet(false).size());
    assertEquals(1, sub1.entrySet(true).size());
    reg1.put("key1", "value1");
    checkCollectionSize(1, 4);
    sub2.localDestroy("key5");
    checkCollectionSize(1, 3);
    assertEquals(0, sub2.entrySet(false).size());
    assertEquals(1, sub2.entrySet(true).size());
    assertEquals(1, sub2_1.entrySet(false).size());
    assertEquals(1, sub2_1.entrySet(true).size());
    sub2_1.localDestroy("key6");
    checkCollectionSize(1, 2);
    assertEquals(0, sub2.entrySet(false).size());
    assertEquals(0, sub2.entrySet(true).size());
    assertEquals(0, sub2_1.entrySet(false).size());
    sub1.localDestroy("key4");
    checkCollectionSize(1, 1);
    assertEquals(0, sub1.entrySet(false).size());
    assertEquals(0, sub1.entrySet(true).size());

    reg1.create("key3", "value3");
    sub1.create("key4", "value4");
    sub2.create("key5", "value5");
    sub2_1.create("key6", "value6");
    checkCollectionSize(2, 5);

    this.txMgr.begin();
    this.region.create("key2", "value2");
    checkCollectionSize(3, 6);
    reg1.localDestroy("key2");
    checkCollectionSize(2, 5);
    this.region.create("key2", "value2");
    checkCollectionSize(3, 6);
    reg1.destroy("key2");
    checkCollectionSize(2, 5);
    reg1.put("key1", "value2");
    checkCollectionSize(2, 5);
    this.region.create("key2", "value2");
    checkCollectionSize(3, 6);
    reg1.localDestroy("key3");
    checkCollectionSize(2, 5);
    reg1.create("key3", "value3");
    checkCollectionSize(3, 6);

    // TX recursive checks
    sub2.destroy("key5");
    checkCollectionSize(3, 5);
    assertEquals(1, sub1.entrySet(false).size());
    assertEquals(1, sub1.entrySet(true).size());
    assertEquals(0, sub2.entrySet(false).size());
    assertEquals(1, sub2.entrySet(true).size());
    assertEquals(1, sub2_1.entrySet(false).size());
    assertEquals(1, sub2_1.entrySet(true).size());
    sub2_1.destroy("key6");
    checkCollectionSize(3, 4);
    assertEquals(1, sub1.entrySet(false).size());
    assertEquals(1, sub1.entrySet(true).size());
    assertEquals(0, sub2.entrySet(false).size());
    assertEquals(0, sub2.entrySet(true).size());
    assertEquals(0, sub2_1.entrySet(false).size());
    assertEquals(0, sub2_1.entrySet(true).size());
    sub1.localDestroy("key4");
    checkCollectionSize(3, 3);
    assertEquals(0, sub1.entrySet(false).size());
    assertEquals(0, sub1.entrySet(true).size());
    assertEquals(0, sub2.entrySet(false).size());
    assertEquals(0, sub2.entrySet(true).size());
    assertEquals(0, sub2_1.entrySet(false).size());
    assertEquals(0, sub2_1.entrySet(true).size());
    sub2.put("key5", "value5");
    checkCollectionSize(3, 4);
    assertEquals(0, sub1.entrySet(false).size());
    assertEquals(0, sub1.entrySet(true).size());
    assertEquals(1, sub2.entrySet(false).size());
    assertEquals(1, sub2.entrySet(true).size());
    assertEquals(0, sub2_1.entrySet(false).size());
    assertEquals(0, sub2_1.entrySet(true).size());
    sub2_1.put("key6", "value6");
    checkCollectionSize(3, 5);
    assertEquals(0, sub1.entrySet(false).size());
    assertEquals(0, sub1.entrySet(true).size());
    assertEquals(1, sub2.entrySet(false).size());
    assertEquals(2, sub2.entrySet(true).size());
    assertEquals(1, sub2_1.entrySet(false).size());
    assertEquals(1, sub2_1.entrySet(true).size());
    sub1.put("key4", "value4");
    checkCollectionSize(3, 6);
    assertEquals(1, sub1.entrySet(false).size());
    assertEquals(1, sub1.entrySet(true).size());
    assertEquals(1, sub2.entrySet(false).size());
    assertEquals(2, sub2.entrySet(true).size());
    assertEquals(1, sub2_1.entrySet(false).size());
    assertEquals(1, sub2_1.entrySet(true).size());
    sub2_1.put("key7", "value7");
    checkCollectionSize(3, 7);
    assertEquals(1, sub1.entrySet(false).size());
    assertEquals(1, sub1.entrySet(true).size());
    assertEquals(1, sub2.entrySet(false).size());
    assertEquals(3, sub2.entrySet(true).size());
    assertEquals(2, sub2_1.entrySet(false).size());
    assertEquals(2, sub2_1.entrySet(true).size());

    this.txMgr.rollback();
    checkCollectionSize(2, 5);

    // disabling these in a TX because they throw and don't work now!
    // this.txMgr.begin();
    sub2.destroyRegion();
    checkCollectionSize(2, 3);
    sub1.destroyRegion();
    checkCollectionSize(2);

    reg1.localDestroy("key1");
    reg1.localDestroy("key3");
    checkCollectionSize(0);
  }

  @Test
  public void testLoader() throws CacheException {
    AttributesMutator<String, String> mutator = this.region.getAttributesMutator();
    mutator.setCacheLoader(new CacheLoader<String, String>() {
      int count = 0;

      @Override
      public String load(LoaderHelper helper) throws CacheLoaderException {
        count++;
        return "LV " + count;
      }

      @Override
      public void close() {}
    });
    LocalRegion reg1 = (LocalRegion) this.region;
    if (isPR())
      ((PartitionedRegion) reg1).setHaveCacheLoader();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals("LV 1", reg1.get("key1"));
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 1", reg1.getEntry("key1").getValue());
    reg1.localDestroy("key1");

    // TX load: only TX
    this.txMgr.begin();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals("LV 2", reg1.get("key1"));
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 2", reg1.getEntry("key1").getValue());
    this.txMgr.rollback();
    assertTrue(!reg1.containsKey("key1"));
    // assertIndexDetailsEquals("LV 2", reg1.getEntry("key1").getValue());
    // reg1.localDestroy("key1");
    // TX load: commit check
    this.txMgr.begin();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals("LV 3", reg1.get("key1"));
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 3", reg1.getEntry("key1").getValue());
    this.txMgr.commit();
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 3", reg1.getEntry("key1").getValue());
    reg1.localDestroy("key1");
    // TX load YES conflict: no-initial state, tx create, committed load
    {
      final TXManagerImpl txMgrImpl = (TXManagerImpl) this.txMgr;
      TXStateProxy tx;
      this.txMgr.begin();
      reg1.create("key1", "txValue");
      assertEquals("txValue", reg1.getEntry("key1").getValue());
      tx = txMgrImpl.pauseTransaction();
      assertTrue(!reg1.containsKey("key1"));
      assertEquals("LV 4", reg1.get("key1"));
      assertTrue(reg1.containsKey("key1"));
      txMgrImpl.unpauseTransaction(tx);
      assertEquals("txValue", reg1.getEntry("key1").getValue());
      assertEquals("txValue", reg1.get("key1"));
      try {
        this.txMgr.commit();
        fail("Should have thrown a commit conflict");
      } catch (CommitConflictException cce) {
        // this is what we want
      }
      assertEquals("LV 4", reg1.getEntry("key1").getValue());
      assertEquals("LV 4", reg1.get("key1"));
      reg1.localDestroy("key1");
    }
    // TX load no conflict: load initial state, tx update
    assertEquals("LV 5", reg1.get("key1"));
    this.txMgr.begin();
    reg1.put("key1", "txValue");
    assertEquals("txValue", reg1.get("key1"));
    assertEquals("txValue", reg1.getEntry("key1").getValue());
    this.txMgr.commit(); // no conflict! Make sure committed value overrode initial state
    assertEquals("txValue", reg1.getEntry("key1").getValue());
    assertEquals("txValue", reg1.get("key1"));
    reg1.localDestroy("key1");
    // TX load no conflict: load initial state, tx load
    assertEquals("LV 6", reg1.get("key1"));
    this.txMgr.begin();
    reg1.localInvalidate("key1");
    assertEquals("LV 7", reg1.get("key1"));
    assertEquals("LV 7", reg1.getEntry("key1").getValue());
    this.txMgr.commit(); // no conflict! Make sure committed value overrode initial state
    assertEquals("LV 7", reg1.getEntry("key1").getValue());
    assertEquals("LV 7", reg1.get("key1"));
    reg1.localDestroy("key1");

    // TX load no conflict: no initial state, tx load, committed create
    {
      final TXManagerImpl txMgrImpl = (TXManagerImpl) this.txMgr;
      TXStateProxy tx;
      this.txMgr.begin();
      assertEquals("LV 8", reg1.get("key1"));
      assertEquals("LV 8", reg1.getEntry("key1").getValue());
      tx = txMgrImpl.pauseTransaction();
      assertTrue(!reg1.containsKey("key1"));
      reg1.create("key1", "txValue");
      assertTrue(reg1.containsKey("key1"));
      assertEquals("txValue", reg1.get("key1"));
      txMgrImpl.unpauseTransaction(tx);
      assertEquals("LV 8", reg1.getEntry("key1").getValue());
      try {
        this.txMgr.commit(); // should conflict
        fail("Should have thrown cce");
      } catch (CommitConflictException cce) {
        // this is what we want
      }
      assertEquals("txValue", reg1.getEntry("key1").getValue());
      assertEquals("txValue", reg1.get("key1"));
      reg1.localDestroy("key1");
    }
    // TX load conflict: no-inital state, tx load->update, committed update
    {
      final TXManagerImpl txMgrImpl = (TXManagerImpl) this.txMgr;
      TransactionId txId = null;
      this.txMgr.begin();
      reg1.create("key1", "txValue");
      txId = txMgrImpl.suspend();
      assertTrue(!reg1.containsKey("key1"));
      // new transaction, load(create) + put
      this.txMgr.begin();
      assertEquals("LV 9", reg1.get("key1"));
      assertEquals("LV 9", reg1.getEntry("key1").getValue());
      reg1.put("key1", "txValue2");
      assertEquals("txValue2", reg1.get("key1"));
      assertEquals("txValue2", reg1.getEntry("key1").getValue());
      this.txMgr.commit();
      assertTrue(reg1.containsKey("key1"));
      assertEquals("txValue2", reg1.get("key1"));
      assertEquals("txValue2", reg1.getEntry("key1").getValue());
      txMgrImpl.resume(txId);
      assertEquals("txValue", reg1.getEntry("key1").getValue());
      assertEquals("txValue", reg1.get("key1"));
      try {
        this.txMgr.commit();
        fail("expected CommitConflictException!");
      } catch (CommitConflictException expected) {
      }
      assertTrue(reg1.containsKey("key1"));
      assertEquals("txValue2", reg1.get("key1"));
      assertEquals("txValue2", reg1.getEntry("key1").getValue());
      reg1.localDestroy("key1");
    }

    // TX load repeat: no-initial state, tx load->get
    this.txMgr.begin();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals("LV 10", reg1.get("key1")); // first invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 10", reg1.getEntry("key1").getValue());
    assertEquals("LV 10", reg1.get("key1")); // second invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 10", reg1.getEntry("key1").getValue());
    this.txMgr.rollback();

    // TX load repeat: no-initial state, tx load->localDestory->load
    this.txMgr.begin();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals("LV 11", reg1.get("key1")); // first invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 11", reg1.getEntry("key1").getValue());
    reg1.localDestroy("key1");
    assertEquals("LV 12", reg1.get("key1")); // second invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 12", reg1.getEntry("key1").getValue());
    this.txMgr.rollback();

    // TX load repeat: no-initial state: tx load->destroy->load
    this.txMgr.begin();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals("LV 13", reg1.get("key1")); // first invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 13", reg1.getEntry("key1").getValue());
    reg1.destroy("key1");
    assertEquals("LV 14", reg1.get("key1")); // second invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 14", reg1.getEntry("key1").getValue());
    this.txMgr.rollback();

    // TX load repeat: no-initial state, tx load->localInvalidate->load
    this.txMgr.begin();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals("LV 15", reg1.get("key1")); // first invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 15", reg1.getEntry("key1").getValue());
    reg1.localInvalidate("key1");
    assertEquals("LV 16", reg1.get("key1")); // second invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 16", reg1.getEntry("key1").getValue());
    this.txMgr.rollback();

    // TX load repeat: no-initial, tx load->invalidate->load
    this.txMgr.begin();
    assertTrue(!reg1.containsKey("key1"));
    assertEquals("LV 17", reg1.get("key1")); // first invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 17", reg1.getEntry("key1").getValue());
    reg1.invalidate("key1");
    assertEquals("LV 18", reg1.get("key1")); // second invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 18", reg1.getEntry("key1").getValue());
    this.txMgr.rollback();

    // TX load repeat: invalid entry initial state, tx load->get
    reg1.create("key1", null);
    this.txMgr.begin();
    assertTrue(reg1.containsKey("key1"));
    assertNull(reg1.getEntry("key1").getValue());
    assertEquals("LV 19", reg1.get("key1")); // first invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 19", reg1.getEntry("key1").getValue());
    assertEquals("LV 19", reg1.get("key1")); // second invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 19", reg1.getEntry("key1").getValue());
    this.txMgr.rollback();

    // TX load repeat: invalid entry initial state, tx load->localDestory->load
    this.txMgr.begin();
    assertTrue(reg1.containsKey("key1"));
    assertNull(reg1.getEntry("key1").getValue());
    assertEquals("LV 20", reg1.get("key1")); // first invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 20", reg1.getEntry("key1").getValue());
    reg1.localDestroy("key1");
    assertEquals("LV 21", reg1.get("key1")); // second invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 21", reg1.getEntry("key1").getValue());
    this.txMgr.rollback();

    // TX load repeat: invalid entry initial state: tx load->destroy->load
    this.txMgr.begin();
    assertTrue(reg1.containsKey("key1"));
    assertNull(reg1.getEntry("key1").getValue());
    assertEquals("LV 22", reg1.get("key1")); // first invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 22", reg1.getEntry("key1").getValue());
    reg1.destroy("key1");
    assertEquals("LV 23", reg1.get("key1")); // second invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 23", reg1.getEntry("key1").getValue());
    this.txMgr.rollback();

    // TX load repeat: invalid entry initial state, tx load->localInvalidate->load
    this.txMgr.begin();
    assertTrue(reg1.containsKey("key1"));
    assertNull(reg1.getEntry("key1").getValue());
    assertEquals("LV 24", reg1.get("key1")); // first invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 24", reg1.getEntry("key1").getValue());
    reg1.localInvalidate("key1");
    assertEquals("LV 25", reg1.get("key1")); // second invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 25", reg1.getEntry("key1").getValue());
    this.txMgr.rollback();

    // TX load repeat: invalid entry initial state, tx load->invalidate->load
    this.txMgr.begin();
    assertTrue(reg1.containsKey("key1"));
    assertNull(reg1.getEntry("key1").getValue());
    assertEquals("LV 26", reg1.get("key1")); // first invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 26", reg1.getEntry("key1").getValue());
    reg1.invalidate("key1");
    assertEquals("LV 27", reg1.get("key1")); // second invocation
    assertTrue(reg1.containsKey("key1"));
    assertEquals("LV 27", reg1.getEntry("key1").getValue());
    this.txMgr.rollback();

    // Make sure a load does not conflict with the region being destroyed
    this.txMgr.begin();
    assertEquals("LV 28", reg1.get("key2"));

    this.txMgr.commit();
    reg1.localDestroyRegion(); // non-tx region op
    // reg1 is now destroyed
  }

  @Test
  public void testStats() throws CacheException {
    final int SLEEP_MS = 250;
    // final int OP_TIME = 0; // ns // changed form 10 to 0 because on fater platforms
    // and low resolution clocks this test will fail.
    final CachePerfStats stats = this.cache.getCachePerfStats();

    class statsValidator {
      private long txSuccessLifeTime;
      private long txFailedLifeTime;
      private long txRollbackLifeTime;
      private int txCommits;
      private int txFailures;
      private int txRollbacks;
      private long txCommitTime;
      private long txFailureTime;
      private long txRollbackTime;
      private int txCommitChanges;
      private int txFailureChanges;
      private int txRollbackChanges;

      private CachePerfStats stats;

      private statsValidator(CachePerfStats stats) {
        this.stats = stats;
      }

      private void reset() {
        this.txSuccessLifeTime = this.stats.getTxSuccessLifeTime();
        this.txFailedLifeTime = this.stats.getTxFailedLifeTime();
        this.txRollbackLifeTime = this.stats.getTxRollbackLifeTime();
        this.txCommits = this.stats.getTxCommits();
        this.txFailures = this.stats.getTxFailures();
        this.txRollbacks = this.stats.getTxRollbacks();
        this.txCommitTime = this.stats.getTxCommitTime();
        this.txFailureTime = this.stats.getTxFailureTime();
        this.txRollbackTime = this.stats.getTxRollbackTime();
        this.txCommitChanges = this.stats.getTxCommitChanges();
        this.txFailureChanges = this.stats.getTxFailureChanges();
        this.txRollbackChanges = this.stats.getTxRollbackChanges();
      }

      private void setTxSuccessLifeTime(long txSuccessLifeTime) {
        this.txSuccessLifeTime = txSuccessLifeTime;
      }

      private void setTxFailedLifeTime(long txFailedLifeTime) {
        this.txFailedLifeTime = txFailedLifeTime;
      }

      private void setTxRollbackLifeTime(long txRollbackLifeTime) {
        this.txRollbackLifeTime = txRollbackLifeTime;
      }

      private void setTxCommits(int txCommits) {
        this.txCommits = txCommits;
      }

      private void setTxFailures(int txFailures) {
        this.txFailures = txFailures;
      }

      private void setTxRollbacks(int txRollbacks) {
        this.txRollbacks = txRollbacks;
      }

      private void setTxCommitTime(long txCommitTime) {
        this.txCommitTime = txCommitTime;
      }

      private void setTxFailureTime(long txFailureTime) {
        this.txFailureTime = txFailureTime;
      }

      private void setTxRollbackTime(long txRollbackTime) {
        this.txRollbackTime = txRollbackTime;
      }

      private void setTxCommitChanges(int txCommitChanges) {
        this.txCommitChanges = txCommitChanges;
      }

      private void setTxFailureChanges(int txFailureChanges) {
        this.txFailureChanges = txFailureChanges;
      }

      private void setTxRollbackChanges(int txRollbackChanges) {
        this.txRollbackChanges = txRollbackChanges;
      }

      private void assertValid() {
        assertEquals(this.txRollbacks, this.stats.getTxRollbacks());
        assertEquals(this.txRollbackChanges, this.stats.getTxRollbackChanges());
        if (Boolean
            .getBoolean(DistributionConfig.GEMFIRE_PREFIX + "cache.enable-time-statistics")) {
          assertTrue(this.txRollbackTime <= this.stats.getTxRollbackTime());
          // assertTrue(this.txRollbackLifeTime+((SLEEP_MS-10)*1000000) <=
          // this.stats.getTxRollbackLifeTime());
          assertTrue(
              "RollbackLifeTime " + this.txRollbackLifeTime + " is not <= "
                  + this.stats.getTxRollbackLifeTime(),
              this.txRollbackLifeTime <= this.stats.getTxRollbackLifeTime());
          assertTrue(this.txCommitTime <= this.stats.getTxCommitTime());
          assertTrue(this.txSuccessLifeTime <= this.stats.getTxSuccessLifeTime());
          assertTrue(this.txFailureTime <= this.stats.getTxFailureTime());
          assertTrue(
              "FailedLifeTime " + this.txFailedLifeTime + " is not <= "
                  + this.stats.getTxFailedLifeTime(),
              this.txFailedLifeTime <= this.stats.getTxFailedLifeTime());
        }

        assertEquals(this.txCommits, this.stats.getTxCommits());
        assertEquals(this.txCommitChanges, this.stats.getTxCommitChanges());

        assertEquals(this.txFailures, this.stats.getTxFailures());
        assertEquals(this.txFailureChanges, this.stats.getTxFailureChanges());
      }
    }

    statsValidator statsVal = new statsValidator(stats);
    // Zero and non-zero rollback stats test
    int i;
    long testRollbackLifeTime = 0, testTotalTx = 0;
    for (i = 0; i < 2; ++i) {
      statsVal.reset();
      statsVal.setTxRollbacks(stats.getTxRollbacks() + 1);
      statsVal.setTxRollbackLifeTime(stats.getTxRollbackLifeTime() + ((SLEEP_MS - 20) * 1000000));
      final long beforeBegin = NanoTimer.getTime();
      this.txMgr.begin();
      final long afterBegin = NanoTimer.getTime();
      pause(SLEEP_MS);
      if (i > 0) {
        statsVal.setTxRollbackChanges(stats.getTxRollbackChanges() + 2);
        this.region.put("stats1", "stats rollback1");
        this.region.put("stats2", "stats rollback2");
      }
      statsVal.setTxRollbackTime(stats.getTxRollbackTime());
      final long beforeRollback = NanoTimer.getTime();
      this.txMgr.rollback();
      final long afterRollback = NanoTimer.getTime();
      final long statsRollbackLifeTime = stats.getTxRollbackLifeTime();
      testRollbackLifeTime += beforeRollback - afterBegin;
      // bruce - time based stats are disabled by default
      String p = (String) cache.getDistributedSystem().getProperties()
          .get(DistributionConfig.GEMFIRE_PREFIX + "enable-time-statistics");
      if (p != null && Boolean.getBoolean(p)) {
        assertTrue("Local RollbackLifeTime assertion:  " + testRollbackLifeTime + " is not <= "
            + statsRollbackLifeTime, testRollbackLifeTime <= statsRollbackLifeTime);
      }
      testTotalTx += afterRollback - beforeBegin;
      final long totalTXMinusRollback = testTotalTx - stats.getTxRollbackTime();
      if (Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "cache.enable-time-statistics")) {
        assertTrue("Total Tx Minus Rollback assertion:  " + totalTXMinusRollback + " is not >= "
            + statsRollbackLifeTime, totalTXMinusRollback >= statsRollbackLifeTime);
      }
      statsVal.assertValid();
    }

    // Zero and non-zero commit stats test
    for (i = 0; i < 2; ++i) {
      statsVal.reset();
      statsVal.setTxCommits(stats.getTxCommits() + 1);
      statsVal.setTxSuccessLifeTime(stats.getTxSuccessLifeTime() + ((SLEEP_MS - 10) * 1000000));
      this.txMgr.begin();
      pause(SLEEP_MS);
      if (i > 0) {
        statsVal.setTxCommitChanges(stats.getTxCommitChanges() + 2);
        this.region.put("stats1", "commit1");
        this.region.put("stats2", "commit2");
      }
      try {
        statsVal.setTxCommitTime(stats.getTxCommitTime());
        this.txMgr.commit();
      } catch (CommitConflictException ex) {
        fail("unexpected " + ex);
      }
      statsVal.assertValid();
    }

    // Non-zero failed commit stats
    TXManagerImpl txMgrImpl = (TXManagerImpl) this.txMgr;
    statsVal.reset();
    statsVal.setTxFailures(stats.getTxFailures() + 1);
    statsVal.setTxFailureChanges(stats.getTxFailureChanges() + 2);
    statsVal.setTxFailedLifeTime(stats.getTxFailedLifeTime() + ((SLEEP_MS - 20) * 1000000));
    this.region.put("stats3", "stats fail3");
    this.txMgr.begin();
    this.region.put("stats1", "stats fail1");
    this.region.put("stats2", "stats fail2");
    try {
      this.region.create("stats3", "try stats3");
      fail("expected EntryExistsException");
    } catch (EntryExistsException ok) {
    }
    // begin other tx simulation
    TXStateProxy tx = txMgrImpl.pauseTransaction();
    this.region.put("stats1", "stats success1");
    this.region.put("stats2", "stats success2");
    txMgrImpl.unpauseTransaction(tx);
    // end other tx simulation
    pause(SLEEP_MS);
    try {
      statsVal.setTxFailureTime(stats.getTxFailureTime());
      this.txMgr.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
      // expected failure
    }
    statsVal.assertValid();
  }

  @Test
  public void testCacheStats() throws CacheException {
    CachePerfStats cacheStats = this.cache.getCachePerfStats();
    // quick sanity check to make sure perf stats work non-tx
    int creates;
    int destroys;
    int puts;
    int invalidates;

    creates = cacheStats.getCreates();
    destroys = cacheStats.getDestroys();
    puts = cacheStats.getPuts();
    invalidates = cacheStats.getInvalidates();
    this.region.create("key1", "value1");
    assertEquals(creates + 1, cacheStats.getCreates());
    assertEquals(destroys, cacheStats.getDestroys());
    assertEquals(puts + 1, cacheStats.getPuts());
    assertEquals(invalidates, cacheStats.getInvalidates());

    creates = cacheStats.getCreates();
    destroys = cacheStats.getDestroys();
    puts = cacheStats.getPuts();
    invalidates = cacheStats.getInvalidates();
    this.region.put("key1", "value2");
    assertEquals(creates, cacheStats.getCreates());
    assertEquals(destroys, cacheStats.getDestroys());
    assertEquals(puts + 1, cacheStats.getPuts());
    assertEquals(invalidates, cacheStats.getInvalidates());

    creates = cacheStats.getCreates();
    destroys = cacheStats.getDestroys();
    puts = cacheStats.getPuts();
    invalidates = cacheStats.getInvalidates();
    this.region.invalidate("key1");
    assertEquals(creates, cacheStats.getCreates());
    assertEquals(destroys, cacheStats.getDestroys());
    assertEquals(puts, cacheStats.getPuts());
    assertEquals(invalidates + 1, cacheStats.getInvalidates());

    creates = cacheStats.getCreates();
    destroys = cacheStats.getDestroys();
    puts = cacheStats.getPuts();
    invalidates = cacheStats.getInvalidates();
    this.region.destroy("key1");
    assertEquals(creates, cacheStats.getCreates());
    assertEquals(destroys + 1, cacheStats.getDestroys());
    assertEquals(puts, cacheStats.getPuts());
    assertEquals(invalidates, cacheStats.getInvalidates());

    // now make sure they do not change from tx ops and from rollbacks
    creates = cacheStats.getCreates();
    destroys = cacheStats.getDestroys();
    puts = cacheStats.getPuts();
    invalidates = cacheStats.getInvalidates();
    this.txMgr.begin();
    this.region.create("key1", "value1");
    this.region.put("key1", "value2");
    this.region.invalidate("key1");
    this.region.put("key1", "value3");
    this.region.localInvalidate("key1");
    this.region.put("key1", "value4");
    this.region.localDestroy("key1");
    this.region.put("key1", "value5");
    this.region.destroy("key1");
    this.txMgr.rollback();
    assertEquals(creates, cacheStats.getCreates());
    assertEquals(destroys, cacheStats.getDestroys());
    assertEquals(puts, cacheStats.getPuts());
    assertEquals(invalidates, cacheStats.getInvalidates());

    // now make sure they do change when a commit is done
    creates = cacheStats.getCreates();
    destroys = cacheStats.getDestroys();
    puts = cacheStats.getPuts();
    invalidates = cacheStats.getInvalidates();
    this.txMgr.begin();
    this.region.create("key1", "value1");
    assertEquals(creates, cacheStats.getCreates());
    assertEquals(puts, cacheStats.getPuts());
    this.txMgr.commit();
    assertEquals(creates + 1, cacheStats.getCreates());
    assertEquals(destroys, cacheStats.getDestroys());
    assertEquals(puts + 1, cacheStats.getPuts());
    assertEquals(invalidates, cacheStats.getInvalidates());

    creates = cacheStats.getCreates();
    destroys = cacheStats.getDestroys();
    puts = cacheStats.getPuts();
    invalidates = cacheStats.getInvalidates();
    this.txMgr.begin();
    this.region.put("key1", "value1");
    assertEquals(puts, cacheStats.getPuts());
    this.txMgr.commit();
    assertEquals(creates, cacheStats.getCreates());
    assertEquals(destroys, cacheStats.getDestroys());
    assertEquals(puts + 1, cacheStats.getPuts());
    assertEquals(invalidates, cacheStats.getInvalidates());

    creates = cacheStats.getCreates();
    destroys = cacheStats.getDestroys();
    puts = cacheStats.getPuts();
    invalidates = cacheStats.getInvalidates();
    this.txMgr.begin();
    this.region.localInvalidate("key1");
    assertEquals(invalidates, cacheStats.getInvalidates());
    this.txMgr.commit();
    assertEquals(creates, cacheStats.getCreates());
    assertEquals(destroys, cacheStats.getDestroys());
    assertEquals(puts, cacheStats.getPuts());
    assertEquals(invalidates + 1, cacheStats.getInvalidates());

    creates = cacheStats.getCreates();
    destroys = cacheStats.getDestroys();
    puts = cacheStats.getPuts();
    invalidates = cacheStats.getInvalidates();
    this.txMgr.begin();
    this.region.localDestroy("key1");
    assertEquals(destroys, cacheStats.getDestroys());
    this.txMgr.commit();
    assertEquals(creates, cacheStats.getCreates());
    assertEquals(destroys + 1, cacheStats.getDestroys());
    assertEquals(puts, cacheStats.getPuts());
    assertEquals(invalidates, cacheStats.getInvalidates());
  }

  private void pause(int msWait) {
    try {
      Thread.sleep(msWait);
    } catch (InterruptedException ignore) {
      fail("interrupted");
    }
  }

  @Test
  public void testPauseUnpause() {
    TXManagerImpl txMgrImpl = (TXManagerImpl) this.txMgr;
    assertTrue(!this.txMgr.exists());
    assertEquals(null, txMgrImpl.pauseTransaction());
    TXStateProxy txProxy = null;
    txMgrImpl.unpauseTransaction(txProxy);
    assertTrue(!this.txMgr.exists());

    this.txMgr.begin();
    TransactionId origId = this.txMgr.getTransactionId();
    assertTrue(this.txMgr.exists());
    {
      TXStateProxy tx = txMgrImpl.pauseTransaction();
      assertTrue(!this.txMgr.exists());
      assertThatThrownBy(() -> this.txMgr.begin()).isInstanceOf(IllegalStateException.class);
      assertTrue(!this.txMgr.exists());
      txMgrImpl.unpauseTransaction(tx);
    }
    assertTrue(this.txMgr.exists());
    assertEquals(origId, this.txMgr.getTransactionId());
    this.txMgr.rollback();
  }

  @Test
  public void testSuspendResume() {
    TXManagerImpl txMgrImpl = (TXManagerImpl) this.txMgr;
    assertTrue(!this.txMgr.exists());
    assertEquals(null, txMgrImpl.pauseTransaction());
    TXStateProxy txProxy = null;
    txMgrImpl.unpauseTransaction(txProxy);
    assertTrue(!this.txMgr.exists());

    this.txMgr.begin();
    TransactionId origId = this.txMgr.getTransactionId();
    assertTrue(this.txMgr.exists());
    {
      TXStateProxy tx = txMgrImpl.internalSuspend();
      assertTrue(!this.txMgr.exists());
      this.txMgr.begin();
      try {
        txMgrImpl.internalResume(tx);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
        LogService.getLogger().info("expected ", expected);
      }
      this.txMgr.rollback();
      assertTrue(!this.txMgr.exists());
      txMgrImpl.internalResume(tx);
    }
    assertTrue(this.txMgr.exists());
    assertEquals(origId, this.txMgr.getTransactionId());
    this.txMgr.rollback();
  }

  @Test
  public void testPublicSuspendResume() {
    CacheTransactionManager txMgr = this.txMgr;
    assertTrue(!this.txMgr.exists());
    assertEquals(null, txMgr.suspend());
    TransactionId txId = null;
    try {
      txMgr.resume(txId);
      fail("expected IllegalStateException");
    } catch (IllegalStateException e) {
    }
    assertTrue(!this.txMgr.exists());

    this.txMgr.begin();
    TransactionId origId = this.txMgr.getTransactionId();
    assertTrue(this.txMgr.exists());
    {
      TransactionId tx = txMgr.suspend();
      assertTrue(!this.txMgr.exists());
      this.txMgr.begin();
      try {
        txMgr.resume(tx);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
      }
      this.txMgr.rollback();
      assertTrue(!this.txMgr.exists());
      txMgr.resume(tx);
    }
    assertTrue(this.txMgr.exists());
    assertEquals(origId, this.txMgr.getTransactionId());
    this.txMgr.rollback();

  }

  @Test
  public void testCheckNoTX() {
    {
      AttributesFactory<String, String> af = new AttributesFactory<>();
      af.setScope(Scope.GLOBAL);
      Region<String, String> gr = null;
      try {
        gr = this.cache.createRegion("GLOBALTXTest", af.create());
      } catch (CacheException ex) {
        fail("did not expect " + ex);
      }
      try {
        gr.put("foo", "bar1");
      } catch (Exception ex) {
        fail("did not expect " + ex);
      }
      this.txMgr.begin();
      try {
        gr.put("foo", "bar2");
        fail("expected UnsupportedOperationException");
      } catch (UnsupportedOperationException expected) {
      } catch (Exception ex) {
        fail("did not expect " + ex);
      }
      this.txMgr.rollback();
    }


    {
      DiskStoreFactory dsf = this.cache.createDiskStoreFactory();
      dsf.create("testCheckNoTX");
      AttributesFactory<String, String> af = new AttributesFactory<>();
      af.setScope(Scope.LOCAL);
      af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      af.setDiskStoreName("testCheckNoTX");
      Region<String, String> dr = null;
      try {
        dr = this.cache.createRegion("DiskTXTest", af.create());
      } catch (CacheException ex) {
        fail("did not expect " + ex);
      }
      try {
        try {
          dr.put("foo", "bar1");
        } catch (Exception ex) {
          fail("did not expect " + ex);
        }
        this.txMgr.begin();
        try {
          dr.put("foo", "bar2");
          fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        } catch (Exception ex) {
          fail("did not expect " + ex);
        }
        this.txMgr.rollback();
      } finally {
        dr.localDestroyRegion();
      }
    }
  }

  @Test
  public void testRepeatableRead() throws CacheException {
    final TXManagerImpl txMgrImpl = (TXManagerImpl) this.txMgr;
    TXStateProxy tx;

    // try repeating a get and make sure it doesn't cause a conflict
    this.region.put("key1", "value1"); // non-tx
    txMgrImpl.begin();
    assertEquals("value1", this.region.get("key1"));
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // non-tx
    txMgrImpl.unpauseTransaction(tx);

    assertEquals("value1", this.region.get("key1"));
    txMgrImpl.commit();

    // try repeating a get and modify the entry and make sure it causes a conflict
    this.region.put("key1", "value1"); // non-tx
    txMgrImpl.begin();
    assertEquals("value1", this.region.get("key1"));
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    assertEquals("value1", this.region.get("key1"));
    this.region.put("key1", "value3");
    assertEquals("value3", this.region.get("key1"));
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }

    // try repeating a getEntry and make sure it doesn't cause a conflict
    this.region.put("key1", "value1"); // non-tx
    txMgrImpl.begin();
    this.region.getEntry("key1");
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // non-tx
    txMgrImpl.unpauseTransaction(tx);

    assertEquals("value1", this.region.get("key1"));
    txMgrImpl.commit();

    // try repeating a getEntry and modify the entry and make sure it causes a conflict
    this.region.put("key1", "value1"); // non-tx
    txMgrImpl.begin();
    this.region.getEntry("key1");
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    this.region.put("key1", "value3");
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }

    // try RR when entry fetched using entrySet
    this.region.put("key1", "value1"); // non-tx
    txMgrImpl.begin();
    this.region.get("key1"); // bootstrap the tx, entrySet does not
    this.region.entrySet(false).iterator().next();
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // non-tx
    txMgrImpl.unpauseTransaction(tx);

    assertEquals("value1", this.region.get("key1"));
    txMgrImpl.commit();

    // try RRW->CONFLICT when entry fetched using entrySet
    this.region.put("key1", "value1"); // non-tx
    txMgrImpl.begin();
    this.region.get("key1"); // bootstrap the tx, entrySet does not
    this.region.entrySet(false).iterator().next();
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    assertEquals("value1", this.region.get("key1"));
    this.region.put("key1", "value3");
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }

    // try containsKey
    this.region.put("key1", "value1"); // non-tx
    txMgrImpl.begin();
    assertEquals(true, this.region.containsKey("key1"));
    tx = txMgrImpl.pauseTransaction();
    this.region.remove("key1"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    assertEquals(true, this.region.containsKey("key1"));
    txMgrImpl.commit();
    this.region.put("key1", "value1"); // non-tx
    txMgrImpl.begin();
    assertEquals(true, this.region.containsKey("key1"));
    tx = txMgrImpl.pauseTransaction();
    this.region.remove("key1"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    assertEquals(true, this.region.containsKey("key1"));
    this.region.put("key1", "value3");
    assertEquals(true, this.region.containsKey("key1"));
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }
    // try containsValueForKey
    this.region.put("key1", "value1"); // non-tx
    txMgrImpl.begin();
    assertEquals(true, this.region.containsValueForKey("key1"));
    tx = txMgrImpl.pauseTransaction();
    this.region.remove("key1"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    assertEquals(true, this.region.containsValueForKey("key1"));
    txMgrImpl.commit();
    this.region.put("key1", "value1"); // non-tx
    txMgrImpl.begin();
    assertEquals(true, this.region.containsValueForKey("key1"));
    tx = txMgrImpl.pauseTransaction();
    this.region.remove("key1"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    assertEquals(true, this.region.containsValueForKey("key1"));
    this.region.put("key1", "value3");
    assertEquals(true, this.region.containsValueForKey("key1"));
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }

    // now try the same things but with no entry in committed state at
    // the time of the first read
    // try repeating a get and make sure it doesn't cause a conflict
    this.region.remove("key1"); // non-tx
    txMgrImpl.begin();
    assertEquals(null, this.region.get("key1"));
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // non-tx
    txMgrImpl.unpauseTransaction(tx);

    assertEquals(null, this.region.get("key1"));
    txMgrImpl.commit();

    // try repeating a get and modify the entry and make sure it causes a conflict
    this.region.remove("key1"); // non-tx
    txMgrImpl.begin();
    assertEquals(null, this.region.get("key1"));
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    assertEquals(null, this.region.get("key1"));
    this.region.put("key1", "value3");
    assertEquals("value3", this.region.get("key1"));
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }

    // try repeating a getEntry and make sure it doesn't cause a conflict
    this.region.remove("key1"); // non-tx
    txMgrImpl.begin();
    assertEquals(null, this.region.getEntry("key1"));
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // non-tx
    txMgrImpl.unpauseTransaction(tx);

    assertEquals(null, this.region.getEntry("key1"));
    txMgrImpl.commit();

    // try repeating a getEntry and modify the entry and make sure it causes a conflict
    this.region.remove("key1"); // non-tx
    txMgrImpl.begin();
    assertEquals(null, this.region.getEntry("key1"));
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    assertEquals(null, this.region.getEntry("key1"));
    this.region.put("key1", "value3");
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }

    // try containsKey
    this.region.remove("key1"); // non-tx
    txMgrImpl.begin();
    assertEquals(false, this.region.containsKey("key1"));
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    assertEquals(false, this.region.containsKey("key1"));
    txMgrImpl.commit();
    this.region.remove("key1"); // non-tx
    txMgrImpl.begin();
    assertEquals(false, this.region.containsKey("key1"));
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    assertEquals(false, this.region.containsKey("key1"));
    this.region.put("key1", "value3");
    assertEquals(true, this.region.containsKey("key1"));
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }

    // try containsValueForKey
    this.region.remove("key1"); // non-tx
    txMgrImpl.begin();
    assertEquals(false, this.region.containsValueForKey("key1"));
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    assertEquals(false, this.region.containsValueForKey("key1"));
    txMgrImpl.commit();
    this.region.remove("key1"); // non-tx
    txMgrImpl.begin();
    assertEquals(false, this.region.containsValueForKey("key1"));
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    assertEquals(false, this.region.containsValueForKey("key1"));
    this.region.put("key1", "value3");
    assertEquals(true, this.region.containsValueForKey("key1"));
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }

    // try an invalidate of an already invalid entry
    this.region.remove("key1"); // non-tx
    this.region.create("key1", null); // non-tx
    txMgrImpl.begin();
    this.region.get("key1");
    this.region.localInvalidate("key1"); // should be a noop since it is already invalid
    tx = txMgrImpl.pauseTransaction();
    this.region.remove("key1"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    txMgrImpl.commit();
    assertEquals(false, this.region.containsKey("key1"));

    // make sure a noop invalidate is repeatable read
    this.region.remove("key1"); // non-tx
    this.region.create("key1", null); // non-tx
    txMgrImpl.begin();
    this.region.localInvalidate("key1"); // should be a noop since it is already invalid
    tx = txMgrImpl.pauseTransaction();
    this.region.remove("key1"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    assertEquals(true, this.region.containsKey("key1"));
    assertEquals(false, this.region.containsValueForKey("key1"));
    txMgrImpl.commit();
    assertEquals(false, this.region.containsKey("key1"));

    // make sure a destroy that throws entryNotFound is repeatable read
    this.region.remove("key1"); // non-tx
    txMgrImpl.begin();
    try {
      this.region.localDestroy("key1");
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException expected) {
    }
    tx = txMgrImpl.pauseTransaction();
    this.region.create("key1", "value1"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    assertEquals(false, this.region.containsKey("key1"));
    txMgrImpl.commit();
    assertEquals(true, this.region.containsKey("key1"));
    this.region.remove("key1"); // non-tx

    // make sure a create that throws entryExists is repeatable read
    this.region.create("key1", "non-tx-value1"); // non-tx
    txMgrImpl.begin();
    try {
      this.region.create("key1", "value1");
      fail("expected EntryExistsException");
    } catch (EntryExistsException expected) {
    }
    tx = txMgrImpl.pauseTransaction();
    this.region.remove("key1"); // non-tx
    txMgrImpl.unpauseTransaction(tx);
    assertEquals(true, this.region.containsKey("key1"));
    txMgrImpl.commit();
    assertEquals(false, this.region.containsKey("key1"));
  }

  @Test
  public void testConflicts() throws CacheException {
    final TXManagerImpl txMgrImpl = (TXManagerImpl) this.txMgr;
    TXStateProxy tx;
    // try a put with no conflict to show that commit works
    txMgrImpl.begin();
    this.region.put("key1", "value1");
    txMgrImpl.commit();
    assertEquals("value1", this.region.get("key1"));
    this.region.localDestroy("key1");

    // now try a put with a conflict and make sure it is detected
    txMgrImpl.begin();
    this.region.put("key1", "value1");
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // do a non-tx put to force conflict
    txMgrImpl.unpauseTransaction(tx);
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }
    assertEquals("value2", this.region.get("key1"));
    this.region.localDestroy("key1");

    // slightly difference version where value already exists in cmt state
    this.region.put("key1", "value0");
    txMgrImpl.begin();
    this.region.put("key1", "value1");
    txMgrImpl.commit();
    assertEquals("value1", this.region.get("key1"));
    this.region.localDestroy("key1");

    // now the conflict
    this.region.put("key1", "value0");
    txMgrImpl.begin();
    this.region.put("key1", "value1");
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // do a non-tx put to force conflict
    txMgrImpl.unpauseTransaction(tx);
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }
    assertEquals("value2", this.region.get("key1"));
    this.region.localDestroy("key1");

    // now test create
    txMgrImpl.begin();
    this.region.create("key1", "value1");
    txMgrImpl.commit();
    assertEquals("value1", this.region.get("key1"));
    this.region.localDestroy("key1");

    // now try a create with a conflict and make sure it is detected
    txMgrImpl.begin();
    this.region.create("key1", "value1");
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // do a non-tx put to force conflict
    txMgrImpl.unpauseTransaction(tx);
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }
    assertEquals("value2", this.region.get("key1"));
    this.region.localDestroy("key1");

    // test localInvalidate
    this.region.put("key1", "value0");
    txMgrImpl.begin();
    this.region.localInvalidate("key1");
    txMgrImpl.commit();
    assertTrue(this.region.containsKey("key1") && !this.region.containsValueForKey("key1"));
    this.region.localDestroy("key1");

    // now the conflict
    this.region.put("key1", "value0");
    txMgrImpl.begin();
    this.region.localInvalidate("key1");
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // do a non-tx put to force conflict
    txMgrImpl.unpauseTransaction(tx);
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }
    assertEquals("value2", this.region.get("key1"));
    this.region.localDestroy("key1");

    // test invalidate
    this.region.put("key1", "value0");
    txMgrImpl.begin();
    this.region.invalidate("key1");
    txMgrImpl.commit();
    assertTrue(this.region.containsKey("key1") && !this.region.containsValueForKey("key1"));
    this.region.localDestroy("key1");

    // now the conflict
    this.region.put("key1", "value0");
    txMgrImpl.begin();
    this.region.invalidate("key1");
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // do a non-tx put to force conflict
    txMgrImpl.unpauseTransaction(tx);
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }
    assertEquals("value2", this.region.get("key1"));
    this.region.localDestroy("key1");

    // check C + DD is a NOOP that still gets conflict if non-tx entry created */
    this.txMgr.begin();
    this.region.create("newKey", "valueTX");
    tx = txMgrImpl.pauseTransaction();
    this.region.create("newKey", "valueNONTX");
    txMgrImpl.unpauseTransaction(tx);
    this.region.destroy("newKey");
    assertTrue(!this.region.containsKey("key1"));
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }
    assertEquals("valueNONTX", this.region.get("newKey"));
    this.region.localDestroy("newKey");

    // check C + LD is a NOOP that still gets conflict if non-tx entry created */
    this.txMgr.begin();
    this.region.create("newKey", "valueTX");
    tx = txMgrImpl.pauseTransaction();
    this.region.create("newKey", "valueNONTX");
    txMgrImpl.unpauseTransaction(tx);
    this.region.localDestroy("newKey");
    assertTrue(!this.region.containsKey("key1"));
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }
    assertEquals("valueNONTX", this.region.get("newKey"));
    this.region.localDestroy("newKey");

    // test localDestroy
    this.region.put("key1", "value0");
    txMgrImpl.begin();
    this.region.localDestroy("key1");
    txMgrImpl.commit();
    assertTrue(!this.region.containsKey("key1"));

    // now the conflict
    this.region.put("key1", "value0");
    txMgrImpl.begin();
    this.region.localDestroy("key1");
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // do a non-tx put to force conflict
    txMgrImpl.unpauseTransaction(tx);
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }
    assertEquals("value2", this.region.get("key1"));
    this.region.localDestroy("key1");

    // test destroy
    this.region.put("key1", "value0");
    txMgrImpl.begin();
    this.region.destroy("key1");
    txMgrImpl.commit();
    assertTrue(!this.region.containsKey("key1"));

    // now the conflict
    this.region.put("key1", "value0");
    txMgrImpl.begin();
    this.region.destroy("key1");
    tx = txMgrImpl.pauseTransaction();
    this.region.put("key1", "value2"); // do a non-tx put to force conflict
    txMgrImpl.unpauseTransaction(tx);
    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (CommitConflictException ex) {
    }
    assertEquals("value2", this.region.get("key1"));
    this.region.localDestroy("key1");

    checkUserAttributeConflict(txMgrImpl);

    // make sure non-tx local-invalidate followed by invalidate
    // does not cause conflict
    this.region.create("key1", "val1");
    this.region.localInvalidate("key1");
    txMgrImpl.begin();
    this.region.put("key1", "txVal1");
    tx = txMgrImpl.pauseTransaction();
    this.region.invalidate("key1");
    txMgrImpl.unpauseTransaction(tx);
    txMgrImpl.commit();
    assertEquals("txVal1", this.region.getEntry("key1").getValue());
    this.region.destroy("key1");

    // now try a put and a region destroy.
    txMgrImpl.begin();
    this.region.create("key1", "value1");
    TXStateProxy tis = txMgrImpl.pauseTransaction();
    this.region.localDestroyRegion(); // non-tx
    txMgrImpl.unpauseTransaction(tis);

    try {
      txMgrImpl.commit();
      fail("expected CommitConflictException");
    } catch (TransactionException ex) {
    }
  }

  protected void checkUserAttributeConflict(final CacheTransactionManager txMgrImpl) {
    { // now check entry user attribute conflict checking
      this.region.put("key1", "value0");
      Region.Entry cmtre = this.region.getEntry("key1");
      assertEquals(null, cmtre.getUserAttribute());
      txMgrImpl.begin();
      Region.Entry txre = this.region.getEntry("key1");
      txre.setUserAttribute("uaValue1");
      txMgrImpl.commit();
      assertEquals("uaValue1", cmtre.getUserAttribute());
      this.region.localDestroy("key1");

      this.region.put("key1", "value0");
      cmtre = this.region.getEntry("key1");
      assertEquals("value0", cmtre.getValue());
      assertEquals(null, cmtre.getUserAttribute());
      this.txMgr.begin();
      txre = this.region.getEntry("key1");
      assertEquals("value0", txre.getValue());
      this.region.put("key1", "valueTX");
      assertEquals("valueTX", txre.getValue());
      assertEquals("value0", cmtre.getValue());
      assertEquals(null, txre.getUserAttribute());
      txre.setUserAttribute("uaValue1");
      assertEquals("uaValue1", txre.getUserAttribute());
      assertEquals(null, cmtre.getUserAttribute());
      cmtre.setUserAttribute("uaValue2");
      assertEquals("uaValue2", cmtre.getUserAttribute());
      assertEquals("uaValue1", txre.getUserAttribute());
      try {
        txMgrImpl.commit();
        fail("expected CommitConflictException");
      } catch (CommitConflictException ex) {
      }
      try {
        txre.getValue();
        fail("expected IllegalStateException");
      } catch (IllegalStateException ok) {
      }
      try {
        txre.isDestroyed();
        fail("expected IllegalStateException");
      } catch (IllegalStateException ok) {
      }
      try {
        txre.getUserAttribute();
        fail("expected IllegalStateException");
      } catch (IllegalStateException ok) {
      }
      try {
        txre.setUserAttribute("foo");
        fail("expected IllegalStateException");
      } catch (IllegalStateException ok) {
      }
      assertEquals("uaValue2", cmtre.getUserAttribute());
      assertEquals("value0", cmtre.getValue());
      this.region.localDestroy("key1");
    }
  }

  @Test
  public void testNoopInvalidates() throws CacheException {
    final CachePerfStats stats = this.cache.getCachePerfStats();
    TransactionListener tl = new TransactionListenerAdapter() {
      @Override
      public void afterRollback(TransactionEvent event) {
        te = event;
      }
    };
    this.txMgr.addListener(tl);
    // Make sure invalidates done on invalid entries are noops

    { // distributed invalidate
      // first make sure invalidate is counted as a change
      int txRollbackChanges = stats.getTxRollbackChanges();
      this.region.create("key1", "value1");
      this.txMgr.begin();
      this.region.invalidate("key1");
      this.txMgr.rollback();
      assertEquals(txRollbackChanges + 1, stats.getTxRollbackChanges());
      assertEquals(1, te.getEvents().size());
      this.region.destroy("key1");

      this.region.create("key1", "value1");
      this.txMgr.begin();
      this.region.invalidate("key1");
      this.txMgr.commit();
      assertEquals(1, te.getEvents().size());
      this.region.destroy("key1");

      // now make sure a committed entry that is invalid is not counted as a change
      txRollbackChanges = stats.getTxRollbackChanges();
      this.region.create("key1", "value1");
      this.region.invalidate("key1");
      this.txMgr.begin();
      this.region.invalidate("key1");
      this.txMgr.rollback();
      assertEquals(txRollbackChanges, stats.getTxRollbackChanges());
      assertEquals(0, te.getEvents().size());
      this.region.destroy("key1");

      this.region.create("key1", "value1");
      this.region.invalidate("key1");
      this.txMgr.begin();
      this.region.invalidate("key1");
      this.txMgr.commit();
      assertEquals(0, te.getEvents().size());
      this.region.destroy("key1");

      // now make sure that multiple invalidates of same entry are a single change
      txRollbackChanges = stats.getTxRollbackChanges();
      this.region.create("key1", "value1");
      this.txMgr.begin();
      this.region.invalidate("key1");
      this.region.invalidate("key1");
      this.region.invalidate("key1");
      this.txMgr.rollback();
      assertEquals(txRollbackChanges + 1, stats.getTxRollbackChanges());
      assertEquals(1, te.getEvents().size());
      this.region.destroy("key1");

      this.region.create("key1", "value1");
      this.txMgr.begin();
      this.region.invalidate("key1");
      this.region.invalidate("key1");
      this.region.invalidate("key1");
      this.txMgr.commit();
      assertEquals(1, te.getEvents().size());
      this.region.destroy("key1");
    }

    { // local invalidate
      // first make sure invalidate is counted as a change
      int txRollbackChanges = stats.getTxRollbackChanges();
      this.region.create("key1", "value1");
      this.txMgr.begin();
      this.region.localInvalidate("key1");
      this.txMgr.rollback();
      assertEquals(txRollbackChanges + 1, stats.getTxRollbackChanges());
      this.region.destroy("key1");

      // now make sure a committed entry that is invalid is not counted as a change
      txRollbackChanges = stats.getTxRollbackChanges();
      this.region.create("key1", "value1");
      this.region.localInvalidate("key1");
      this.txMgr.begin();
      this.region.localInvalidate("key1");
      this.txMgr.rollback();
      assertEquals(txRollbackChanges, stats.getTxRollbackChanges());
      this.region.destroy("key1");

      // now make sure that multiple localInvalidates of same entry are a single change
      txRollbackChanges = stats.getTxRollbackChanges();
      this.region.create("key1", "value1");
      this.txMgr.begin();
      this.region.localInvalidate("key1");
      this.region.localInvalidate("key1");
      this.region.localInvalidate("key1");
      this.txMgr.rollback();
      assertEquals(txRollbackChanges + 1, stats.getTxRollbackChanges());
      this.region.destroy("key1");
    }
  }

  private static void clearRegion(Region r) throws TimeoutException {
    Iterator kI = r.keySet().iterator();
    try {
      while (kI.hasNext()) {
        r.destroy(kI.next());
      }
    } catch (CacheException ce) {
      fail("clearRegion operation failed");
    }
  }

  private static final int LRUENTRY_NULL = 0;
  private static final int LRUENTRY_STRING = 1;
  private static final int LRUENTRY_INTEGER = 2;
  private static final int LRUENTRY_LONG = 3;
  private static final int LRUENTRY_DOUBLE = 4;

  private static void assertLRUEntries(Set entries, int size, String keyPrefix, int instanceId) {
    assertEquals(size, entries.size());
    Iterator entItr = entries.iterator();
    while (entItr.hasNext()) {
      Region.Entry re = (Region.Entry) entItr.next();
      switch (instanceId) {
        case LRUENTRY_NULL:
          assertNull(re.getValue());
          break;
        case LRUENTRY_STRING:
          assertTrue(re.getValue() instanceof String);
          break;
        case LRUENTRY_INTEGER:
          assertTrue(re.getValue() instanceof Integer);
          break;
        case LRUENTRY_LONG:
          assertTrue(re.getValue() instanceof Long);
          break;
        case LRUENTRY_DOUBLE:
          assertTrue(re.getValue() instanceof Double);
          break;
        default:
          fail("Unknown instance type in assertLRUEntries: " + instanceId);
      }
      String reKey = (String) re.getKey();
      assertTrue("expected " + reKey + " to start with " + keyPrefix, reKey.startsWith(keyPrefix));
    }
  }

  @Test
  public void testEviction() throws CacheException {
    final int lruSize = 8;
    AttributesFactory<String, Object> af = new AttributesFactory<>();
    af.setEvictionAttributes(
        EvictionAttributes.createLRUEntryAttributes(lruSize, EvictionAction.LOCAL_DESTROY));
    af.setScope(Scope.LOCAL);
    Region<String, Object> lruRegion = this.cache.createRegion(getUniqueName(), af.create());

    // Non-TX LRU verification
    assertEquals(0, lruRegion.entrySet(false).size());
    int numToPut = lruSize + 2;
    for (int i = 0; i < numToPut; ++i) {
      lruRegion.put("key" + i, new Integer(i));
    }
    assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);
    clearRegion(lruRegion);

    // TX LRU verification
    assertEquals(0, lruRegion.entrySet(false).size());
    numToPut = lruSize + 2;
    this.txMgr.begin();
    for (int i = 0; i < numToPut; ++i) {
      lruRegion.put("key" + i, new Long(i));
    }
    assertLRUEntries(lruRegion.entrySet(false), numToPut, "key", LRUENTRY_LONG);
    this.txMgr.commit();
    assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_LONG);
    clearRegion(lruRegion);

    // TX/non-TX no conflict verification w/ initial state
    // full+2, all committed entries have TX refs
    {
      final TXManagerImpl txMgrImpl = (TXManagerImpl) this.txMgr;
      TXStateProxy tx;
      numToPut = lruSize + 2;
      assertEquals(0, lruRegion.entrySet(false).size());
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.create("key" + i, new Integer(i));
      }
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);

      this.txMgr.begin();
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.put("key" + i, new Long(i));
      }
      assertLRUEntries(lruRegion.entrySet(false), numToPut, "key", LRUENTRY_LONG);
      tx = txMgrImpl.pauseTransaction();

      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.put("non-tx key" + i, new Integer(i));
      }
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);
      assertNull(lruRegion.get("non-tx key0"));

      txMgrImpl.unpauseTransaction(tx);
      this.txMgr.commit();
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_LONG);
    }
    clearRegion(lruRegion);

    // TX/non-TX no conflict verification w/ invalid initial state
    // full+2, all committed entries have TX refs using a loader
    {
      AttributesMutator<String, Object> mutator = lruRegion.getAttributesMutator();
      mutator.setCacheLoader(new CacheLoader() {
        // int count = 0;
        public Object load(LoaderHelper helper) throws CacheLoaderException {
          return "value" + helper.getArgument();
        }

        public void close() {}
      });
      final TXManagerImpl txMgrImpl = (TXManagerImpl) this.txMgr;
      TXStateProxy tx;
      numToPut = lruSize + 2;
      assertEquals(0, lruRegion.entrySet(false).size());
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.create("key" + i, null);
      }
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_NULL);
      // assertIndexDetailsEquals(lruSize, lruRegion.entrySet(false).size());
      this.txMgr.begin();
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.get("key" + i, new Integer(i));
      }
      assertLRUEntries(lruRegion.entrySet(false), numToPut, "key", LRUENTRY_STRING);
      tx = txMgrImpl.pauseTransaction();

      assertEquals(lruSize, lruRegion.entrySet(false).size());
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_NULL);
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.get("non-tx key" + i, new Integer(i));
      }
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_NULL);
      assertNull(lruRegion.getEntry("non-tx key0"));

      txMgrImpl.unpauseTransaction(tx);
      this.txMgr.commit();
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_STRING);
      Iterator it = lruRegion.keySet().iterator();
      while (it.hasNext()) {
        lruRegion.localInvalidate(it.next(), null);
      }
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_NULL);
      mutator.setCacheLoader(null);
    }
    clearRegion(lruRegion);

    // TX/TX/non-TX no conflict verification w/ initial state full, TX
    // add lruLimit+4, existing committed have TX 2 refs, force non-TX
    // eviction, force TX eviction
    {
      final TXManagerImpl txMgrImpl = (TXManagerImpl) this.txMgr;
      TransactionId txId1, txId2;
      numToPut = lruSize + 4;
      assertEquals(0, lruRegion.entrySet(false).size());
      // Create entries
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.create("key" + i, new Integer(i));
      }
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);

      this.txMgr.begin();
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);
      // Add a TX reference to committed entries, add a few on top of that
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.put("key" + i, new Long(i));
      }
      assertLRUEntries(lruRegion.entrySet(false), numToPut, "key", LRUENTRY_LONG);
      txId1 = txMgrImpl.suspend();

      this.txMgr.begin();
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);
      // Add another TX reference to committed entries, add a few on top of that
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.put("key" + i, new Double(i));
      }
      assertLRUEntries(lruRegion.entrySet(false), numToPut, "key", LRUENTRY_DOUBLE);
      txId2 = txMgrImpl.suspend();

      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);

      // Force the Non-Tx "put" to remove each attempt since region is full
      // and all the committed entries are currently part of a TX
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.put("non-tx key" + i, new Integer(i));
      }
      assertTrue(numToPut > lruSize);
      assertNull(lruRegion.get("non-tx key0"));
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);

      txMgrImpl.resume(txId1);
      assertLRUEntries(lruRegion.entrySet(false), numToPut, "key", LRUENTRY_LONG);
      // Check to make sure no conflict was caused by non-TX put evictions
      // This should remove all references for each committed entry
      this.txMgr.commit();
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_LONG);
      txMgrImpl.resume(txId2);
      assertLRUEntries(lruRegion.entrySet(false), numToPut, "key", LRUENTRY_DOUBLE);
      this.txMgr.rollback();
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_LONG);

      // Test to make sure we can evict something that has been rolled back
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.put("key" + i, "value" + i);
      }
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_STRING);
    }
    clearRegion(lruRegion);

    // TX/non-TX no conflict verification w/ initial state full, TX
    // add lruLimit+4, force non-TX eviction, then rolls back, make
    // sure that non-TX eviction works properly
    {
      final TXManagerImpl txMgrImpl = (TXManagerImpl) this.txMgr;
      TXStateProxy tx;
      numToPut = lruSize + 4;
      assertEquals(0, lruRegion.entrySet(false).size());
      // Create entries
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.create("key" + i, new Integer(i));
      }
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);

      this.txMgr.begin();
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);
      // Add a TX reference to committed entries, add a few on top of that
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.put("key" + i, new Long(i));
      }
      assertLRUEntries(lruRegion.entrySet(false), numToPut, "key", LRUENTRY_LONG);
      tx = txMgrImpl.pauseTransaction();

      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);
      // Force the Non-Tx "put" to remove each attempt since region is full
      // and all the committed entries are currently part of a TX
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.put("non-tx key" + i, new Integer(i));
      }
      assertNull(lruRegion.get("non-tx key0"));
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);

      txMgrImpl.unpauseTransaction(tx);
      assertLRUEntries(lruRegion.entrySet(false), numToPut, "key", LRUENTRY_LONG);
      // This should remove all references for each committed entry
      this.txMgr.rollback();
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);

      // Test to make sure we can evict something that has been rolled back
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.put("key" + i, "value" + i);
      }
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_STRING);
    }
    clearRegion(lruRegion);

    // TX/TX conflict with initial state full, TX
    // add lruLimit+4, after failed commit make
    // sure that non-TX eviction works properly
    {
      final TXManagerImpl txMgrImpl = (TXManagerImpl) this.txMgr;
      TXStateProxy tx;
      numToPut = lruSize + 4;
      assertEquals(0, lruRegion.entrySet(false).size());
      // Create entries
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.create("key" + i, new Integer(i));
      }
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);

      this.txMgr.begin();
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);
      // Add a TX reference to committed entries, add a few on top of that
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.put("key" + i, new Long(i));
      }
      assertLRUEntries(lruRegion.entrySet(false), numToPut, "key", LRUENTRY_LONG);
      tx = txMgrImpl.pauseTransaction();

      // Cause a conflict
      lruRegion.put("key" + (numToPut - 1), new Integer(numToPut - 1));

      txMgrImpl.unpauseTransaction(tx);
      assertLRUEntries(lruRegion.entrySet(false), numToPut, "key", LRUENTRY_LONG);
      // This should remove all references for each committed entry
      try {
        this.txMgr.commit();
        fail("Expected CommitConflictException during commit LRU Eviction test!");
      } catch (CommitConflictException ok) {
      }

      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_INTEGER);

      // Test to make sure we can evict something that has been rolled back
      for (int i = 0; i < numToPut; ++i) {
        lruRegion.put("key" + i, "value" + i);
      }
      assertLRUEntries(lruRegion.entrySet(false), lruSize, "key", LRUENTRY_STRING);
    }

    lruRegion.localDestroyRegion();
  }

  @Test
  public void testJTASynchronization()
      throws CacheException, javax.transaction.NotSupportedException,
      javax.transaction.RollbackException, javax.transaction.SystemException,
      javax.transaction.HeuristicMixedException, javax.transaction.HeuristicRollbackException {

    javax.transaction.TransactionManager jtaTxMgr = this.cache.getJTATransactionManager();
    TransactionListener tl = new TransactionListener() {
      @Override
      public void afterCommit(TransactionEvent event) {
        ++listenerAfterCommit;
        te = event;
      }

      @Override
      public void afterFailedCommit(TransactionEvent event) {
        ++listenerAfterFailedCommit;
        te = event;
      }

      @Override
      public void afterRollback(TransactionEvent event) {
        ++listenerAfterRollback;
        te = event;
      }

      @Override
      public void close() {
        ++listenerClose;
      }
    };

    this.txMgr.addListener(tl);
    Synchronization gfTXSync;

    // Test successful JTA commit
    jtaTxMgr.begin();
    this.txMgr.begin();
    {
      TXManagerImpl gfTxMgrImpl = (TXManagerImpl) this.txMgr;
      gfTXSync = gfTxMgrImpl.getTXState();
    }
    jtaTxMgr.getTransaction().registerSynchronization(gfTXSync);
    assertEquals(0, this.listenerAfterCommit);
    this.cache.getLogger().info("SWAP:doingCreate");
    this.region.create("syncKey1", "syncVal1");
    jtaTxMgr.commit();
    assertEquals(1, this.listenerAfterCommit);
    assertEquals("syncVal1", this.region.getEntry("syncKey1").getValue());

    try {
      this.txMgr.commit();
      fail("JTA Cache Manager should have called commit!");
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable expected) {
    }

    // Test JTA rollback
    jtaTxMgr.begin();
    this.txMgr.begin();
    {
      TXManagerImpl gfTxMgrImpl = (TXManagerImpl) this.txMgr;
      gfTXSync = gfTxMgrImpl.getTXState();
    }
    jtaTxMgr.getTransaction().registerSynchronization(gfTXSync);
    assertEquals(0, this.listenerAfterRollback);
    this.region.put("syncKey2", "syncVal2");
    jtaTxMgr.rollback();
    assertEquals(1, this.listenerAfterRollback);
    assertTrue(!this.region.containsKey("syncKey2"));

    // Test failed JTA commit with suspend
    jtaTxMgr.begin();
    this.txMgr.begin();
    {
      TXManagerImpl gfTxMgrImpl = (TXManagerImpl) this.txMgr;
      gfTXSync = gfTxMgrImpl.getTXState();
      jtaTxMgr.getTransaction().registerSynchronization(gfTXSync);
      assertEquals(0, this.listenerAfterFailedCommit);
      this.region.put("syncKey3", "syncVal3");
      assertEquals("syncVal3", this.region.getEntry("syncKey3").getValue());

      TXStateProxy gfTx = gfTxMgrImpl.pauseTransaction();
      javax.transaction.Transaction jtaTx = jtaTxMgr.suspend();
      assertNull(jtaTxMgr.getTransaction());
      this.region.put("syncKey3", "syncVal4");
      assertEquals("syncVal4", this.region.getEntry("syncKey3").getValue());
      gfTxMgrImpl.unpauseTransaction(gfTx);
      try {
        jtaTxMgr.resume(jtaTx);
      } catch (Exception failure) {
        fail("JTA resume failed");
      }
      assertNotNull(jtaTxMgr.getTransaction());
    }
    assertEquals("syncVal3", this.region.getEntry("syncKey3").getValue());
    try {
      jtaTxMgr.commit();
      fail("Expected JTA manager conflict exception!");
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable expected) {
    }
    assertEquals(1, this.listenerAfterFailedCommit);
    assertEquals("syncVal4", this.region.getEntry("syncKey3").getValue());


    // Test failed JTA commit with a new thread
    jtaTxMgr.begin();
    this.txMgr.begin();
    {
      TXManagerImpl gfTxMgrImpl = (TXManagerImpl) this.txMgr;
      gfTXSync = gfTxMgrImpl.getTXState();
      jtaTxMgr.getTransaction().registerSynchronization(gfTXSync);
      assertEquals(1, this.listenerAfterFailedCommit);
      this.region.put("syncKey4", "syncVal3");
      assertEquals("syncVal3", this.region.getEntry("syncKey4").getValue());

      // Create a new thread and have it update the same key, causing
      // a conflict
      final int signal[] = {0};
      Thread t = new Thread("non-TX conflict generator") {
        @Override
        public void run() {
          try {
            region.put("syncKey4", "syncVal4");
            while (true)
              synchronized (signal) {
                signal[0] = 1;
                signal.notify();
                signal.wait();
                if (signal[0] == 0) {
                  break;
                }
              }
          } catch (Exception error) {
            fail("Non-tx thread failure due to: " + error);
          }
        }
      };
      t.start();
      try {
        while (true)
          synchronized (signal) {
            if (signal[0] == 1) {
              signal[0] = 0;
              signal.notify();
              break;
            } else {
              signal.wait();
            }
          }
      } catch (InterruptedException dangit) {
        fail("Tx thread waiting for non-tx thread failed due to : " + dangit);
      }
      assertEquals("syncVal3", this.region.getEntry("syncKey4").getValue());
    }
    try {
      jtaTxMgr.commit();
      fail("Expected JTA manager conflict exception!");
    } catch (javax.transaction.HeuristicRollbackException expected) {
    } catch (javax.transaction.RollbackException alsoExpected) {
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable yuk) {
      fail("Did not expect this throwable from JTA commit: " + yuk);
    }
    assertEquals(2, this.listenerAfterFailedCommit);
    assertEquals("syncVal4", this.region.getEntry("syncKey4").getValue());

    this.txMgr.removeListener(tl);
  }

  @Test
  public void testJTAEnlistment() throws CacheException, javax.transaction.NotSupportedException,
      javax.transaction.RollbackException, javax.transaction.SystemException,
      javax.transaction.HeuristicMixedException, javax.transaction.HeuristicRollbackException {

    TransactionListener tl = new TransactionListener() {
      @Override
      public void afterCommit(TransactionEvent event) {
        ++listenerAfterCommit;
        te = event;
      }

      @Override
      public void afterFailedCommit(TransactionEvent event) {
        ++listenerAfterFailedCommit;
        te = event;
      }

      @Override
      public void afterRollback(TransactionEvent event) {
        ++listenerAfterRollback;
        te = event;
      }

      @Override
      public void close() {
        ++listenerClose;
      }
    };

    this.txMgr.addListener(tl);

    javax.transaction.UserTransaction userTx = null;
    try {
      userTx = (javax.transaction.UserTransaction) this.cache.getJNDIContext()
          .lookup("java:/UserTransaction");
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable badDog) {
      fail("Expected to get a healthy UserTransaction!");
    }

    // Test enlistment for put
    // Test enlisted rollback
    // Test prevention of rollback/commit for enlisted transaction
    assertEquals(0, this.listenerAfterRollback);
    userTx.begin();
    this.region.put("enlistKey", "enlistVal");
    assertEquals("enlistVal", this.region.getEntry("enlistKey").getValue());
    assertNotNull(this.txMgr.getTransactionId());
    try {
      this.txMgr.rollback();
      fail("Should not allow a CacheTransactionManager.rollback call once the GF Tx is enlisted");
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable ok) {
    }

    try {
      this.txMgr.commit();
      fail("Should not allow a CacheTransactionManager.commit() call once the GF Tx is enlisted");
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable alsoOk) {
    }
    userTx.rollback();
    assertNull(this.txMgr.getTransactionId());
    assertTrue(!this.region.containsKey("enlistKey"));
    assertEquals(1, this.listenerAfterRollback);

    // Test enlistment for create
    // Test commit
    assertEquals(0, this.listenerAfterCommit);
    userTx.begin();
    this.region.create("enlistKey", "enlistVal");
    assertEquals("enlistVal", this.region.getEntry("enlistKey").getValue());
    assertNotNull(this.txMgr.getTransactionId());
    userTx.commit();
    assertNull(this.txMgr.getTransactionId());
    assertTrue(this.region.containsKey("enlistKey"));
    assertEquals("enlistVal", this.region.getEntry("enlistKey").getValue());
    assertEquals(1, this.listenerAfterCommit);

    // Test enlistment for get
    assertEquals(1, this.listenerAfterCommit);
    userTx.begin();
    assertEquals("enlistVal", this.region.get("enlistKey"));
    assertNotNull(this.txMgr.getTransactionId());
    userTx.commit();
    assertNull(this.txMgr.getTransactionId());
    assertEquals(2, this.listenerAfterCommit);

    // Test enlistment for invalidate
    assertEquals(2, this.listenerAfterCommit);
    userTx.begin();
    this.region.invalidate("enlistKey");
    assertTrue(this.region.containsKey("enlistKey"));
    assertTrue(!this.region.containsValueForKey("enlistKey"));
    assertNotNull(this.txMgr.getTransactionId());
    userTx.commit();
    assertNull(this.txMgr.getTransactionId());
    assertTrue(this.region.containsKey("enlistKey"));
    assertTrue(!this.region.containsValueForKey("enlistKey"));
    assertEquals(3, this.listenerAfterCommit);

    // Test enlistment for destroy
    assertEquals(3, this.listenerAfterCommit);
    userTx.begin();
    this.region.destroy("enlistKey");
    assertTrue(!this.region.containsKey("enlistKey"));
    assertNotNull(this.txMgr.getTransactionId());
    userTx.commit();
    assertNull(this.txMgr.getTransactionId());
    assertTrue(!this.region.containsKey("enlistKey"));
    assertEquals(4, this.listenerAfterCommit);

    // Test enlistment for load
    AttributesMutator<String, String> mutator = this.region.getAttributesMutator();
    mutator.setCacheLoader(new CacheLoader<String, String>() {
      int count = 0;

      @Override
      public String load(LoaderHelper helper) throws CacheLoaderException {
        return String.valueOf(count++);
      }

      @Override
      public void close() {}
    });
    assertEquals(4, this.listenerAfterCommit);
    userTx.begin();
    assertEquals("0", this.region.get("enlistKey"));
    assertNotNull(this.txMgr.getTransactionId());
    userTx.commit();
    assertNull(this.txMgr.getTransactionId());
    assertTrue(this.region.containsKey("enlistKey"));
    assertEquals("0", this.region.getEntry("enlistKey").getValue());
    assertEquals(5, this.listenerAfterCommit);
    mutator.setCacheLoader(null);

    // Test enlisted failed commit
    assertEquals(0, this.listenerAfterFailedCommit);
    userTx.begin();
    this.region.put("enlistKey", "enlistVal");
    assertEquals("enlistVal", this.region.get("enlistKey"));
    assertNotNull(this.txMgr.getTransactionId());
    {
      TXManagerImpl gfTxMgrImpl = (TXManagerImpl) this.txMgr;
      TXStateProxy gfTx = gfTxMgrImpl.pauseTransaction();

      javax.transaction.TransactionManager jtaTxMgr = this.cache.getJTATransactionManager();
      javax.transaction.Transaction jtaTx = jtaTxMgr.suspend();

      this.region.put("enlistKey", "conflictVal");
      assertEquals("conflictVal", this.region.get("enlistKey"));

      try {
        jtaTxMgr.resume(jtaTx);
      } catch (Exception failure) {
        fail("JTA resume failed");
      }
      gfTxMgrImpl.unpauseTransaction(gfTx);
    }
    assertEquals("enlistVal", this.region.get("enlistKey"));
    try {
      userTx.commit();
      fail("Expected JTA commit exception!");
    } catch (javax.transaction.HeuristicRollbackException expected) {
    } catch (javax.transaction.RollbackException alsoExpected) {
    } catch (Exception yuk) {
      fail("Did not expect this exception from JTA commit: " + yuk);
    }
    assertNull(this.txMgr.getTransactionId());
    assertEquals("conflictVal", this.region.getEntry("enlistKey").getValue());
    assertEquals(1, this.listenerAfterFailedCommit);

    // Test rollbackOnly UserTransaction enlistment
    userTx.begin();
    assertNull(this.txMgr.getTransactionId());
    userTx.setRollbackOnly();
    assertEquals(javax.transaction.Status.STATUS_MARKED_ROLLBACK, userTx.getStatus());
    try {
      this.region.put("enlistKey", "enlistVal2");
      fail("Expected to get a FailedSynchronizationException!");
    } catch (FailedSynchronizationException okay) {
    }
    assertNull(this.txMgr.getTransactionId());
    try {
      assertEquals("conflictVal", this.region.getEntry("enlistKey").getValue());
      fail("Expected to get a FailedSynchronizationException!");
    } catch (FailedSynchronizationException okay) {
    }
    assertTrue(!this.region.containsKey("enlistKey2"));
    try {
      this.region.put("enlistKey2", "enlistVal3");
      fail("Expected to get a FailedSynchronizationException!");
    } catch (FailedSynchronizationException okay) {
    }
    assertNull(this.txMgr.getTransactionId());
    try {
      assertEquals("conflictVal", this.region.getEntry("enlistKey").getValue());
      fail("Expected to get a FailedSynchronizationException!");
    } catch (FailedSynchronizationException okay) {
    }
    assertTrue(!this.region.containsKey("enlistKey2"));
    userTx.rollback();
    assertEquals("conflictVal", this.region.getEntry("enlistKey").getValue());
    assertTrue(!this.region.containsKey("enlistKey2"));

    this.txMgr.removeListener(tl);
  }

  private static void waitForUpdates(final Index idx, final int expectedUpdates) {
    // DistributedTestCase.WaitCriterion wc = new DistributedTestCase.WaitCriterion() {
    // String excuse;
    // public boolean done() {
    // return idx.getStatistics().getNumUpdates() == expectedUpdates;
    // }
    //
    // public String description() {
    // return "expectedUpdates " + expectedUpdates + " but got this "
    // + idx.getStatistics().getNumUpdates();
    // }
    // };
    // DistributedTestCase.waitForCriterion(wc, 15 * 1000, 20, true);

    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done
          && time.elapsedTimeMillis() < 15 * 1000; done =
              (idx.getStatistics().getNumUpdates() == expectedUpdates)) {
        Thread.sleep(20);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue("expectedUpdates " + expectedUpdates + " but got this "
        + idx.getStatistics().getNumUpdates(), done);
  }

  private static void waitForKeys(final Index idx, final int expectedKeys) {
    // DistributedTestCase.WaitCriterion wc = new DistributedTestCase.WaitCriterion() {
    // String excuse;
    // public boolean done() {
    // return idx.getStatistics().getNumberOfKeys() == expectedKeys;
    // }
    //
    // public String description() {
    // return "expectedKeys " + expectedKeys + " but got this "
    // + idx.getStatistics().getNumberOfKeys();
    // }
    // };
    // DistributedTestCase.waitForCriterion(wc, 15 * 1000, 20, true);

    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done
          && time.elapsedTimeMillis() < 15 * 1000; done =
              (idx.getStatistics().getNumberOfKeys() == expectedKeys)) {
        Thread.sleep(20);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue(
        "expectedKeys " + expectedKeys + " but got this " + idx.getStatistics().getNumberOfKeys(),
        done);
  }

  @Test
  public void testTXAndQueries() throws CacheException, QueryException {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    try {
      final QueryService qs = this.cache.getQueryService();

      final String fromClause = this.region.getFullPath() + " value ";
      final String qstr = "SELECT DISTINCT * FROM " + fromClause;

      // Create a region with async index updates
      AttributesFactory af = new AttributesFactory(this.region.getAttributes());
      af.setIndexMaintenanceSynchronous(false);
      final Region aIregion = this.cache.createRegion(getUniqueName(), af.create());
      final String aIfromClause = aIregion.getFullPath() + " value ";
      final String aIqstr = "SELECT DISTINCT * FROM " + aIfromClause;

      // Confirm base functionality for query results
      this.region.put("qkey0", "qval0");
      this.region.put("qkey1", "qval01");
      Query q = qs.newQuery(qstr);
      SelectResults res = (SelectResults) q.execute();
      assertEquals(2, res.size());
      String val;
      for (Iterator resI = res.iterator(); resI.hasNext();) {
        val = (String) resI.next();
        assertTrue("Value: " + val + " does not start with qval", val.startsWith("qval"));
      }
      q = qs.newQuery(qstr + " where value.length > 6");
      res = (SelectResults) q.execute();
      assertEquals(0, res.size());

      aIregion.put("qkey0", "qval0");
      aIregion.put("qkey1", "qval01");
      q = qs.newQuery(aIqstr);
      res = (SelectResults) q.execute();
      assertEquals(2, res.size());
      for (Iterator resI = res.iterator(); resI.hasNext();) {
        val = (String) resI.next();
        assertTrue("Value: " + val + " does not start with qval", val.startsWith("qval"));
      }
      q = qs.newQuery(qstr + " where value.length > 6");
      res = (SelectResults) q.execute();
      assertEquals(0, res.size());

      // Test query results in a transaction
      this.txMgr.begin();
      Query q1 = qs.newQuery(qstr);
      this.region.put("noQkey2", "noQval2");
      res = (SelectResults) q1.execute();
      assertEquals(2, res.size());
      for (Iterator resI = res.iterator(); resI.hasNext();) {
        val = (String) resI.next();
        assertTrue("Value: " + val + " does not start with qval", val.startsWith("qval"));
      }
      Query aIq1 = qs.newQuery(aIqstr);
      aIregion.put("noQkey2", "noQval2");
      res = (SelectResults) aIq1.execute();
      assertEquals(2, res.size());
      for (Iterator resI = res.iterator(); resI.hasNext();) {
        val = (String) resI.next();
        assertTrue("Value: " + val + " does not start with qval", val.startsWith("qval"));
      }
      Query q2 = qs.newQuery(qstr + " where value.length > 6");
      res = (SelectResults) q2.execute();
      assertEquals(0, res.size());
      Query aIq2 = qs.newQuery(aIqstr + " where value.length > 6");
      res = (SelectResults) aIq2.execute();
      assertEquals(0, res.size());
      this.txMgr.commit();
      res = (SelectResults) q1.execute();
      assertEquals(3, res.size());
      res = (SelectResults) q2.execute();
      assertEquals(1, res.size());
      res = (SelectResults) aIq1.execute();
      assertEquals(3, res.size());
      res = (SelectResults) aIq2.execute();
      assertEquals(1, res.size());

      this.region.destroy("noQkey2");
      aIregion.destroy("noQkey2");

      // Confirm base functionality for index creation
      Index index0 = qs.createIndex("TXIndex0", IndexType.FUNCTIONAL, "value.length", fromClause);
      assertEquals(2, index0.getStatistics().getNumberOfKeys());
      assertEquals(2, index0.getStatistics().getNumberOfValues());
      assertEquals(2, index0.getStatistics().getNumUpdates()); // Shouldn't this be zero?
      Index aIindex0 =
          qs.createIndex("aITXIndex0", IndexType.FUNCTIONAL, "value.length", aIfromClause);
      assertEquals(2, aIindex0.getStatistics().getNumberOfKeys());
      assertEquals(2, aIindex0.getStatistics().getNumberOfValues());
      assertEquals(2, aIindex0.getStatistics().getNumUpdates()); // Shouldn't this be zero?
      q = qs.newQuery(qstr);
      res = (SelectResults) q.execute();
      assertEquals(2, res.size());
      assertEquals(0, index0.getStatistics().getTotalUses());
      aIq1 = qs.newQuery(aIqstr);
      res = (SelectResults) aIq1.execute();
      assertEquals(2, res.size());
      assertEquals(0, aIindex0.getStatistics().getTotalUses());

      final String val2 = "qval000002";
      this.region.put("qkey2", val2);
      assertEquals(3, index0.getStatistics().getNumUpdates()); // Shouldn't this be 1?
      assertEquals(3, index0.getStatistics().getNumberOfKeys());
      assertEquals(3, index0.getStatistics().getNumberOfValues());
      aIregion.put("qkey2", val2);
      final IndexManager.IndexUpdaterThread upThread =
          ((AbstractRegion) aIregion).getIndexManager().getUpdaterThread();
      while (!upThread.isDone()) {
        pause(20);
      }
      // @todo asif: for some reason the value returned by getNumberOfKeys is unstable.
      // Even when the code waits for it to be the expected value it intermittently
      // will fail because it never gets to be the expected value.
      // This stat (in RangeIndex at least) is only updated when we add a new key
      // to the valueToEntriesMap. I do not see a place that we ever remove from
      // this map (even when we do a removeMapping).
      waitForUpdates(aIindex0, 3);
      waitForKeys(aIindex0, 3);
      assertEquals(3, aIindex0.getStatistics().getNumUpdates()); // Shouldn't this be 1?
      assertEquals(3, aIindex0.getStatistics().getNumberOfKeys());
      assertEquals(3, aIindex0.getStatistics().getNumberOfValues());
      q = qs.newQuery("ELEMENT(" + qstr + " where value.length > 6)");
      assertEquals(val2, (String) q.execute());
      assertEquals(1, index0.getStatistics().getTotalUses());
      aIq1 = qs.newQuery("ELEMENT(" + aIqstr + " where value.length > 6)");
      assertEquals(val2, (String) aIq1.execute());

      this.region.destroy("qkey2");
      waitForKeys(index0, 2);
      assertEquals(2, index0.getStatistics().getNumberOfKeys()); // Shouldn't this be 1, again?
      assertEquals(2, index0.getStatistics().getNumberOfValues());
      assertEquals(4, index0.getStatistics().getNumUpdates());
      aIregion.destroy("qkey2");
      while (!upThread.isDone()) {
        pause(20);
      }
      waitForUpdates(aIindex0, 4);
      // waitForKeys(aIindex0, 3);
      // assertIndexDetailsEquals(3, aIindex0.getStatistics().getNumberOfKeys()); // Shouldn't this
      // be 1, again?
      assertEquals(2, aIindex0.getStatistics().getNumberOfValues());
      assertEquals(4, aIindex0.getStatistics().getNumUpdates());

      // Test index creation
      this.txMgr.begin();
      this.region.destroy("qkey1");
      this.region.put("noQkey3", "noQval3");
      Index index1 = qs.createIndex("TXIndex1", IndexType.FUNCTIONAL, "value", fromClause);
      assertEquals(2, index1.getStatistics().getNumberOfKeys());
      assertEquals(2, index1.getStatistics().getNumberOfValues());
      assertEquals(2, index1.getStatistics().getNumUpdates());
      assertEquals(2, index0.getStatistics().getNumberOfKeys());
      assertEquals(2, index0.getStatistics().getNumberOfValues());
      assertEquals(4, index0.getStatistics().getNumUpdates());

      aIregion.destroy("qkey1");
      aIregion.put("noQkey3", "noQval3");
      Index aIindex1 = qs.createIndex("aITXIndex1", IndexType.FUNCTIONAL, "value", aIfromClause);
      while (!upThread.isDone()) {
        pause(20);
      }
      waitForUpdates(aIindex0, 4);
      waitForUpdates(aIindex1, 2);
      // waitForKeys(aIindex0, 3);
      // waitForKeys(aIindex1, 2);
      assertEquals(2, aIindex1.getStatistics().getNumberOfKeys());
      assertEquals(2, aIindex1.getStatistics().getNumberOfValues());
      assertEquals(2, aIindex1.getStatistics().getNumUpdates());
      // assertIndexDetailsEquals(3, aIindex0.getStatistics().getNumberOfKeys());
      assertEquals(2, aIindex0.getStatistics().getNumberOfValues());
      assertEquals(4, aIindex0.getStatistics().getNumUpdates());


      q = qs.newQuery(qstr);
      res = (SelectResults) q.execute();
      assertEquals(2, res.size());
      assertEquals(0, index1.getStatistics().getTotalUses());
      assertEquals(1, index0.getStatistics().getTotalUses());

      aIq1 = qs.newQuery(aIqstr);
      res = (SelectResults) aIq1.execute();
      assertEquals(2, res.size());
      assertEquals(0, aIindex1.getStatistics().getTotalUses());
      assertEquals(1, aIindex0.getStatistics().getTotalUses());

      q = qs.newQuery(qstr + " where value < 'q'");
      res = (SelectResults) q.execute();
      assertEquals(1, index1.getStatistics().getTotalUses());
      assertEquals(0, res.size());

      aIq1 = qs.newQuery(aIqstr + " where value < 'q'");
      res = (SelectResults) aIq1.execute();
      assertEquals(1, aIindex1.getStatistics().getTotalUses());
      assertEquals(0, res.size());

      this.region.put("noQkey4", "noQval4");
      assertEquals(2, index1.getStatistics().getNumberOfKeys());
      assertEquals(2, index1.getStatistics().getNumberOfValues());
      assertEquals(2, index1.getStatistics().getNumUpdates());
      assertEquals(2, index0.getStatistics().getNumberOfKeys());
      assertEquals(2, index0.getStatistics().getNumberOfValues());
      assertEquals(4, index0.getStatistics().getNumUpdates());

      aIregion.put("noQkey4", "noQval4");
      while (!upThread.isDone()) {
        pause(20);
      }
      waitForUpdates(aIindex0, 4);
      waitForUpdates(aIindex1, 2);
      waitForKeys(aIindex0, 2);
      // waitForKeys(aIindex1, 2);
      // assertIndexDetailsEquals(2, aIindex1.getStatistics().getNumberOfKeys());
      assertEquals(2, aIindex1.getStatistics().getNumberOfValues());
      assertEquals(2, aIindex1.getStatistics().getNumUpdates());
      assertEquals(2, aIindex0.getStatistics().getNumberOfKeys());
      assertEquals(2, aIindex0.getStatistics().getNumberOfValues());
      assertEquals(4, aIindex0.getStatistics().getNumUpdates());

      q = qs.newQuery(qstr + " where value < 'q'");
      res = (SelectResults) q.execute();
      assertEquals(2, index1.getStatistics().getTotalUses());
      assertEquals(0, res.size());

      aIq1 = qs.newQuery(aIqstr + " where value <'q'");
      res = (SelectResults) aIq1.execute();
      assertEquals(2, aIindex1.getStatistics().getTotalUses());
      assertEquals(0, res.size());

      q = qs.newQuery(qstr + " where value.length > 6");
      res = (SelectResults) q.execute();
      assertEquals(2, index0.getStatistics().getTotalUses());
      assertEquals(0, res.size());

      aIq1 = qs.newQuery(aIqstr + " where value.length > 6");
      res = (SelectResults) aIq1.execute();
      assertEquals(2, aIindex0.getStatistics().getTotalUses());
      assertEquals(0, res.size());

      this.txMgr.commit();
      assertEquals(3, index1.getStatistics().getNumberOfKeys());
      assertEquals(3, index1.getStatistics().getNumberOfValues()); // Shouldn't this be 4?
      assertEquals(5, index1.getStatistics().getNumUpdates());
      assertEquals(2, index0.getStatistics().getNumberOfKeys());
      assertEquals(3, index0.getStatistics().getNumberOfValues()); // Shouldn't this be 4?
      assertEquals(7, index0.getStatistics().getNumUpdates());

      while (!upThread.isDone()) {
        pause(20);
      }
      waitForUpdates(aIindex0, 7);
      waitForUpdates(aIindex1, 5);
      // waitForKeys(aIindex0, 4); // sometimes 3 sometimes 4
      // waitForKeys(aIindex1, 3);
      assertEquals(3, aIindex1.getStatistics().getNumberOfKeys());
      assertEquals(3, aIindex1.getStatistics().getNumberOfValues()); // Shouldn't this be 4?
      assertEquals(5, aIindex1.getStatistics().getNumUpdates());
      // assertIndexDetailsEquals(4, aIindex0.getStatistics().getNumberOfKeys());
      assertEquals(3, aIindex0.getStatistics().getNumberOfValues()); // Shouldn't this be 4?
      assertEquals(7, aIindex0.getStatistics().getNumUpdates());

      q = qs.newQuery(qstr + " where value <'q'");
      res = (SelectResults) q.execute();
      assertEquals(3, index1.getStatistics().getTotalUses());
      assertEquals(2, res.size());

      aIq1 = qs.newQuery(aIqstr + " where value < 'q'");
      res = (SelectResults) aIq1.execute();
      assertEquals(3, aIindex1.getStatistics().getTotalUses());
      assertEquals(2, res.size());

      q = qs.newQuery(qstr + " where value.length > 6");
      res = (SelectResults) q.execute();
      assertEquals(3, index0.getStatistics().getTotalUses());
      assertEquals(2, res.size());

      aIq1 = qs.newQuery(aIqstr + " where value.length > 6");
      res = (SelectResults) aIq1.execute();
      assertEquals(3, aIindex0.getStatistics().getTotalUses());
      assertEquals(2, res.size());
    } finally {
      IndexManager.TEST_RANGEINDEX_ONLY = false;
    }
  }

  /**
   * make sure that we do not expose BucketRegion on transactionListener events
   *
   */
  @Test
  public void testInternalRegionNotExposed() throws Exception {
    TransactionListenerForRegionTest tl = new TransactionListenerForRegionTest();
    CacheTransactionManager ctm = this.cache.getCacheTransactionManager();
    ctm.addListener(tl);
    CacheListenerForRegionTest cl = new CacheListenerForRegionTest();
    AttributesFactory af = new AttributesFactory();
    PartitionAttributes pa =
        new PartitionAttributesFactory().setRedundantCopies(0).setTotalNumBuckets(1).create();
    af.setPartitionAttributes(pa);
    af.addCacheListener(cl);
    Region pr = this.cache.createRegion("testTxEventForRegion", af.create());
    pr.put(2, "tw");
    pr.put(3, "three");
    pr.put(4, "four");
    ctm.begin();
    pr.put(1, "one");
    pr.put(2, "two");
    pr.invalidate(3);
    pr.destroy(4);
    ctm.commit();
    assertFalse(tl.exceptionOccurred);
    assertFalse(cl.exceptionOccurred);
  }



  /**
   * make sure that we throw an UnsupportedOperationInTransactionException
   *
   */
  @Test
  public void testPutAllSupported() throws Exception {
    TXManagerImpl ctm = this.cache.getTxManager();
    AttributesFactory af = new AttributesFactory();
    Region r = this.cache.createRegion("dRegion", af.create());
    PartitionAttributes pa =
        new PartitionAttributesFactory().setRedundantCopies(0).setTotalNumBuckets(1).create();
    af.setPartitionAttributes(pa);
    Region pr = this.cache.createRegion("prRegion", af.create());
    Map map = new HashMap();
    map.put("stuff", "junk");
    map.put("stuff2", "junk2");
    ctm.begin();
    pr.putAll(map);
    r.putAll(map);
    TXStateProxy tx = ctm.pauseTransaction();
    assertTrue(!pr.containsKey("stuff"));
    assertTrue(!r.containsKey("stuff"));
    ctm.unpauseTransaction(tx);
    ctm.commit();
    assertTrue(pr.containsKey("stuff"));
    assertTrue(r.containsKey("stuff"));
  }


  /**
   * make sure that we throw an UnsupportedOperationInTransactionException
   *
   */
  @Test
  public void testGetAllSupported() throws Exception {
    CacheTransactionManager ctm = this.cache.getCacheTransactionManager();
    AttributesFactory af = new AttributesFactory();
    Region r = this.cache.createRegion("dRegion", af.create());
    PartitionAttributes pa =
        new PartitionAttributesFactory().setRedundantCopies(0).setTotalNumBuckets(1).create();
    af.setPartitionAttributes(pa);
    Region pr = this.cache.createRegion("prRegion", af.create());
    List list = new ArrayList();
    list.add("stuff");
    list.add("stuff2");
    ctm.begin();
    pr.getAll(list);
    r.getAll(list);
    ctm.commit();
    // now we aren't in tx so these shouldn't throw
    pr.getAll(list);
    r.getAll(list);
  }



  /**
   * make sure that we throw an UnsupportedOperationInTransactionException
   *
   */
  @Test
  public void testDestroyRegionNotSupported() throws Exception {
    CacheTransactionManager ctm = this.cache.getCacheTransactionManager();
    AttributesFactory af = new AttributesFactory();
    Region r = this.cache.createRegion("dRegion", af.create());
    PartitionAttributes pa =
        new PartitionAttributesFactory().setRedundantCopies(0).setTotalNumBuckets(1).create();
    af.setPartitionAttributes(pa);
    Region pr = this.cache.createRegion("prRegion", af.create());
    List list = new ArrayList();
    list.add("stuff");
    list.add("stuff2");
    ctm.begin();
    try {
      pr.destroyRegion();
      fail("Should have thrown UnsupportedOperationInTransactionException during destroyRegion");
    } catch (UnsupportedOperationInTransactionException ee) {
      // expected
    }
    try {
      pr.localDestroyRegion();
      fail(
          "Should have thrown UnsupportedOperationInTransactionException during localDestroyRegion");
    } catch (UnsupportedOperationInTransactionException ee) {
      // expected
    }
    try {
      r.destroyRegion();
      fail("Should have thrown UnsupportedOperationInTransactionException during destroyRegion");
    } catch (UnsupportedOperationInTransactionException ee) {
      // expected
    }

    try {
      r.localDestroyRegion();
      fail(
          "Should have thrown UnsupportedOperationInTransactionException during localDestroyRegion");
    } catch (UnsupportedOperationInTransactionException ee) {
      // expected
    }
    assertTrue(!pr.isDestroyed());
    assertTrue(!r.isDestroyed());

    ctm.commit();
    // now we aren't in tx so these shouldn't throw
    pr.destroyRegion();
    r.destroyRegion();

  }

  /**
   * make sure that we throw an UnsupportedOperationInTransactionException
   *
   */
  @Test
  public void testInvalidateRegionNotSupported() throws Exception {
    CacheTransactionManager ctm = this.cache.getCacheTransactionManager();
    AttributesFactory af = new AttributesFactory();
    Region r = this.cache.createRegion("dRegion", af.create());
    PartitionAttributes pa =
        new PartitionAttributesFactory().setRedundantCopies(0).setTotalNumBuckets(1).create();
    af.setPartitionAttributes(pa);
    Region pr = this.cache.createRegion("prRegion", af.create());
    ctm.begin();
    try {
      pr.invalidateRegion();
      fail("Should have thrown UnsupportedOperationInTransactionException during invalidateRegion");
    } catch (UnsupportedOperationInTransactionException ee) {
      // expected
    }
    try {
      pr.localInvalidateRegion();
      fail(
          "Should have thrown UnsupportedOperationInTransactionException during localInvalidateRegion");
    } catch (UnsupportedOperationInTransactionException ee) {
      // expected
    }
    try {
      r.invalidateRegion();
      fail("Should have thrown UnsupportedOperationInTransactionException during invalidateRegion");
    } catch (UnsupportedOperationInTransactionException ee) {
      // expected
    }

    try {
      r.localInvalidateRegion();
      fail(
          "Should have thrown UnsupportedOperationInTransactionException during localInvalidateRegion");
    } catch (UnsupportedOperationInTransactionException ee) {
      // expected
    }
    ctm.commit();
    // now we aren't in tx so these shouldn't throw
    pr.invalidateRegion();
    r.invalidateRegion();

  }

  /**
   * make sure that we throw an UnsupportedOperationInTransactionException
   *
   */
  @Test
  public void testClearRegionNotSupported() throws Exception {
    CacheTransactionManager ctm = this.cache.getCacheTransactionManager();
    AttributesFactory af = new AttributesFactory();
    Region r = this.cache.createRegion("dRegion", af.create());
    PartitionAttributes pa =
        new PartitionAttributesFactory().setRedundantCopies(0).setTotalNumBuckets(1).create();
    af.setPartitionAttributes(pa);
    Region pr = this.cache.createRegion("prRegion", af.create());
    ctm.begin();
    try {
      pr.clear();
      fail("Should have thrown UnsupportedOperation during invalidateRegion");
    } catch (UnsupportedOperationException ee) {
      // expected
    }
    try {
      pr.localClear();
      fail("Should have thrown UnsupportedOperation during localInvalidateRegion");
    } catch (UnsupportedOperationException ee) {
      // expected
    }
    try {
      r.clear();
      fail("Should have thrown UnsupportedOperationInTransactionException during invalidateRegion");
    } catch (UnsupportedOperationInTransactionException ee) {
      // expected
    }

    try {
      r.localClear();
      fail(
          "Should have thrown UnsupportedOperationInTransactionException during localInvalidateRegion");
    } catch (UnsupportedOperationInTransactionException ee) {
      // expected
    }
    ctm.commit();
    // now we aren't in tx so these shouldn't throw
    pr.invalidateRegion();
    r.invalidateRegion();

  }

  @Test
  public void testBug51781() {
    AttributesFactory<Integer, String> af = new AttributesFactory<Integer, String>();
    af.setDataPolicy(DataPolicy.NORMAL);
    Region<Integer, String> r = this.cache.createRegion(getUniqueName(), af.create());
    CacheTransactionManager mgr = this.cache.getCacheTransactionManager();
    r.put(1, "value1");
    r.put(2, "value2");
    assertEquals(2, r.size());
    mgr.begin();
    r.put(3, "value3");
    r.destroy(3);
    mgr.commit();
    assertEquals(2, r.size());
  }

  private String getUniqueName() {
    return getClass().getSimpleName() + "_" + testName.getMethodName();
  }

  private static class TransactionListenerForRegionTest extends TransactionListenerAdapter {
    private boolean exceptionOccurred = false;

    @Override
    public void afterCommit(TransactionEvent event) {
      List<CacheEvent<?, ?>> events = event.getEvents();
      for (CacheEvent<?, ?> e : events) {
        if (!"/testTxEventForRegion".equals(e.getRegion().getFullPath())) {
          exceptionOccurred = true;
        }
      }
    }
  }

  private static class CacheListenerForRegionTest extends CacheListenerAdapter {
    private boolean exceptionOccurred = false;

    @Override
    public void afterCreate(EntryEvent event) {
      verifyRegion(event);
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      verifyRegion(event);
    }

    private void verifyRegion(EntryEvent event) {
      if (!"/testTxEventForRegion".equals(event.getRegion().getFullPath())) {
        exceptionOccurred = true;
      }
    }
  }

  private void verifyEventProps(EntryEvent ev) {
    assertTrue(!ev.getOperation().isLocalLoad());
    assertTrue(!ev.getOperation().isNetLoad());
    assertTrue(!ev.getOperation().isLoad());
    assertTrue(!ev.getOperation().isNetSearch());
  }

}
