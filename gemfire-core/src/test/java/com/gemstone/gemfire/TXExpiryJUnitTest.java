/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire;

import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.cache.*;
import com.gemstone.gemfire.internal.util.StopWatch;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests transaction expiration functionality
 *
 * @author Mitch Thomas
 * @since 4.0
 *
 */
@SuppressWarnings("deprecation")
@Category(IntegrationTest.class)
public class TXExpiryJUnitTest {

  protected GemFireCacheImpl cache;
  protected CacheTransactionManager txMgr;

  protected void createCache() throws CacheException {
    Properties p = new Properties();
    p.setProperty("mcast-port", "0"); // loner
    this.cache = (GemFireCacheImpl)CacheFactory.create(DistributedSystem.connect(p));
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.DISTRIBUTED_NO_ACK);
    this.txMgr = this.cache.getCacheTransactionManager();
  }
  private void closeCache() {
    if (this.cache != null) {
      if (this.txMgr != null) {
        try {
          this.txMgr.rollback();
        } catch (IllegalStateException ignore) {
        }
      }
      // this.region = null;
      this.txMgr = null;
      Cache c = this.cache;
      this.cache = null;
      c.close();
    }
  }

  @Before
  public void setUp() throws Exception {
    createCache();
  }

  @After
  public void tearDown() throws Exception {
    closeCache();
    InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
    if (ids != null) {
      ids.disconnect();
    }
  }

  @Test
  public void testEntryTTLExpiration() throws CacheException {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.DISTRIBUTED_NO_ACK);
    af.setStatisticsEnabled(true);
    af.setEntryTimeToLive(new ExpirationAttributes(2, ExpirationAction.DESTROY));
    Region exprReg = this.cache.createRegion("TXEntryTTL", af.create());
    generalEntryExpirationTest(exprReg, 2);
    AttributesMutator mutator = exprReg.getAttributesMutator();
    mutator.setEntryTimeToLive(new ExpirationAttributes(1, ExpirationAction.DESTROY));
    generalEntryExpirationTest(exprReg, 1);
  } 

  @Test
  public void testEntryIdleExpiration() throws CacheException {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.DISTRIBUTED_NO_ACK);
    af.setStatisticsEnabled(true);
    af.setEntryIdleTimeout(new ExpirationAttributes(2, ExpirationAction.DESTROY));
    Region exprReg = this.cache.createRegion("TXEntryIdle", af.create());
//    exprReg.getCache().getLogger().info("invoking expiration test with 2");
    generalEntryExpirationTest(exprReg, 2);
    AttributesMutator mutator = exprReg.getAttributesMutator();
    mutator.setEntryIdleTimeout(new ExpirationAttributes(1, ExpirationAction.DESTROY));
//    exprReg.getCache().getLogger().info("invoking expiration test with 1 after setting idle timeout of 1 second");
    generalEntryExpirationTest(exprReg, 1);
  } 
  
  private void waitDance(boolean list[], int waitMs) {
    synchronized(list) {
      if (!list[0]) {
        try {
          list.wait(waitMs);
        }
        catch (InterruptedException e) {
          fail("Interrupted");
        }
        if (list[0]) {
          fail("Cache listener detected a destroy... bad!");
        }
      } else {
        fail("Cache listener detected a destroy oh man that is bad!");
      }
    }  
  }
  
  @SuppressWarnings("deprecation")
  public void generalEntryExpirationTest(final Region exprReg, 
                                         final int exprTime) 
    throws CacheException 
  {
    final int waitMs = exprTime * 1500;
    final int patientWaitMs = exprTime * 90000;
    final boolean wasDestroyed[] = {false};
    AttributesMutator mutator = exprReg.getAttributesMutator();
    final AtomicInteger ac = new AtomicInteger();
    final AtomicInteger au = new AtomicInteger();
    final AtomicInteger ai = new AtomicInteger();
    final AtomicInteger ad = new AtomicInteger();
    
//    exprReg.getCache().getLogger().info("generalEntryExpirationTest invoked with exprTime " + exprTime);

    mutator.setCacheListener(new CacheListenerAdapter() {
        public void close() {}
        public void afterCreate(EntryEvent e) {
//          e.getRegion().getCache().getLogger().info("invoked afterCreate for " + e);
          ac.incrementAndGet();
        }
        public void afterUpdate(EntryEvent e) {
//          e.getRegion().getCache().getLogger().info("invoked afterUpdate for " + e);
          au.incrementAndGet();
        }
        public void afterInvalidate(EntryEvent e) {
//          e.getRegion().getCache().getLogger().info("invoked afterInvalidate for " + e);
          ai.incrementAndGet();
        }
        public void afterDestroy(EntryEvent e) {
//          e.getRegion().getCache().getLogger().info("invoked afterDestroy for " + e);
          ad.incrementAndGet();
          if (e.getKey().equals("key0")) {
            synchronized(wasDestroyed) {
              wasDestroyed[0] = true;
              wasDestroyed.notifyAll();
            }
          }
        }
        public void afterRegionInvalidate(RegionEvent event) {
          fail("Unexpected invokation of afterRegionInvalidate");
        }
        public void afterRegionDestroy(RegionEvent event) {
          if (!event.getOperation().isClose()) {
            fail("Unexpected invokation of afterRegionDestroy");
          }
        }
      });
    
    // Test to ensure an expriation does not cause a conflict
    for(int i=0; i<2; i++) {
      exprReg.put("key" + i, "value" + i);
    }
    try {  Thread.sleep(500); } catch (InterruptedException ie) {fail("interrupted");}
    this.txMgr.begin();
//    exprReg.getCache().getLogger().info("transactional update of key0");
    exprReg.put("key0", "value");
//    exprReg.getCache().getLogger().info("waiting for " + waitMs);
    waitDance(wasDestroyed, waitMs);
    assertEquals("value", exprReg.getEntry("key0").getValue());
//    exprReg.getCache().getLogger().info("committing transaction");
    try {
      this.txMgr.commit();
    } catch (CommitConflictException error) {
      fail("Expiration should not cause commit to fail");
    }
    assertEquals("value", exprReg.getEntry("key0").getValue());
    try {
      synchronized(wasDestroyed) {
        if (!wasDestroyed[0]) {
//          exprReg.getCache().getLogger().info("waiting for wasDestroyed to be set by listener");
          long start = System.currentTimeMillis();
          wasDestroyed.wait(patientWaitMs);
          long took = System.currentTimeMillis()-start;
          if (!wasDestroyed[0]) {
//            exprReg.getCache().getLogger().info("wasDestroyed was never set by the listener");
            OSProcess.printStacks(0, false);
            fail("Cache listener did not detect a destroy in " + patientWaitMs + " ms! actuallyWaited "+took+"ms ac="+ac.get()+" au:"+au.get()+" ai:"+ai.get()+" ad:"+ad.get());
          }
        }
      }
    } catch (InterruptedException ie) {
      fail("Caught InterruptedException while waiting for eviction");
    }
    assertTrue(!exprReg.containsKey("key0"));
    // key1 is the canary for the rest of the entries
    assertTrue(!exprReg.containsKey("key1"));

    // rollback and failed commit test, ensure expiration continues
    for(int j=0; j<2; j++) {
      synchronized(wasDestroyed) {
        wasDestroyed[0] = false;
      }
      for(int i=0; i<2; i++) {
        exprReg.put("key" + i, "value" + i);
      }
      try {  Thread.sleep(500); } catch (InterruptedException ie) {fail("interrupted");}
      this.txMgr.begin();
      exprReg.put("key0", "value");
      waitDance(wasDestroyed, waitMs);
      assertEquals("value", exprReg.getEntry("key0").getValue());
      String checkVal;
      if (j==0) {
        checkVal = "value0";
        this.txMgr.rollback();
      } else {
        checkVal = "conflictVal";
        final TXManagerImpl txMgrImpl = (TXManagerImpl)this.txMgr;
        TXStateProxy tx = txMgrImpl.internalSuspend();
        exprReg.put("key0", checkVal);
        txMgrImpl.resume(tx);
        try {
          this.txMgr.commit();
          fail("Expected CommitConflictException!");
        } catch (CommitConflictException expected) {}
      }
//      exprReg.getCache().getLogger().info("waiting for listener to be invoked.  iteration = " + j);
      try {
        synchronized(wasDestroyed) {
          if (!wasDestroyed[0]) {
            Object value = exprReg.get("key0");
            if (value == null) { // destroy in progress?
              wasDestroyed.wait(waitMs);
              assertTrue(wasDestroyed[0]);
            }
            else {
              assertEquals(checkVal, value);
              long start = System.currentTimeMillis();
              wasDestroyed.wait(patientWaitMs);
              long took = System.currentTimeMillis()-start;
              if (!wasDestroyed[0]) {
                Map m = new HashMap(exprReg);
                fail("Cache listener did not detect a destroy in " + patientWaitMs + " ms! actuallyWaited:"+took+"ms ac="+ac.get()+" au:"+au.get()+" ai:"+ai.get()+" ad:"+ad.get()+" j="+j+" region="+m);
              }
            }
          } 
        }
      } catch (InterruptedException ie) {
        fail("Caught InterruptedException while waiting for expiration");
      }
      assertTrue(!exprReg.containsKey("key0"));
      // key1 is the canary for the rest of the entries
      assertTrue(!exprReg.containsKey("key1"));
    }
  }

  @Test
  public void testRegionIdleExpiration() throws CacheException {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.DISTRIBUTED_NO_ACK);
    af.setStatisticsEnabled(true);
    af.setRegionIdleTimeout(new ExpirationAttributes(2, ExpirationAction.INVALIDATE));
    Region exprReg = this.cache.createRegion("TXRegionIdle", af.create());
    generalRegionExpirationTest(exprReg, 2, null, false);
    generalRegionExpirationTest(exprReg, 1, new ExpirationAttributes(1, ExpirationAction.INVALIDATE), false);
    generalRegionExpirationTest(exprReg, 2, new ExpirationAttributes(2, ExpirationAction.INVALIDATE), false);
    generalRegionExpirationTest(exprReg, 1, new ExpirationAttributes(1, ExpirationAction.DESTROY), false);
  } 

  @Test
  public void testRegionTTLExpiration() throws CacheException {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.DISTRIBUTED_NO_ACK);
    af.setStatisticsEnabled(true);
    af.setRegionTimeToLive(new ExpirationAttributes(1, ExpirationAction.INVALIDATE));
    Region exprReg = this.cache.createRegion("TXRegionTTL", af.create());
    generalRegionExpirationTest(exprReg, 1, null, true);
    generalRegionExpirationTest(exprReg, 2, new ExpirationAttributes(2, ExpirationAction.INVALIDATE), true);
    generalRegionExpirationTest(exprReg, 1, new ExpirationAttributes(1, ExpirationAction.INVALIDATE), true);
    generalRegionExpirationTest(exprReg, 1, new ExpirationAttributes(1, ExpirationAction.DESTROY), true);
  } 
  
  private void generalRegionExpirationTest(final Region exprReg, 
                                          final int exprTime, 
                                          ExpirationAttributes exprAtt,
                                          boolean useTTL) 
    throws CacheException 
  {
    final int waitMs = exprTime * 1500;
    final int patientWaitMs = exprTime * 90000;
    final boolean regionExpiry[] = {false};
    AttributesMutator mutator = exprReg.getAttributesMutator();
    mutator.setCacheListener(new CacheListenerAdapter() {
        public void close() {}
        public void afterCreate(EntryEvent e) {}
        public void afterUpdate(EntryEvent e) {}
        public void afterInvalidate(EntryEvent e) {}
        public void afterDestroy(EntryEvent e) {}
        public void afterRegionInvalidate(RegionEvent event) {
          synchronized(regionExpiry) {
            regionExpiry[0] = true;
            regionExpiry.notifyAll();
          }
        }
        public void afterRegionDestroy(RegionEvent event) {
          if (!event.getOperation().isClose()) {
            synchronized(regionExpiry) {
              regionExpiry[0] = true;
              regionExpiry.notifyAll();
            }
          }
        }
      });

    // Create some keys and age them, I wish we could fake/force the age
    // instead of having to actually wait
    for(int i=0; i<2; i++) {
      exprReg.put("key" + i, "value" + i);
    }

    ExpirationAction action;
    if (exprAtt!=null) {
      action = exprAtt.getAction();
      if (useTTL) {
        mutator.setRegionTimeToLive(exprAtt);
      } else {
        mutator.setRegionIdleTimeout(exprAtt);
      }
    } else {
      if (useTTL) {
        action = exprReg.getAttributes().getRegionTimeToLive().getAction();
      } else {
        action = exprReg.getAttributes().getRegionIdleTimeout().getAction();
      }
    }

    // Potential race condition at this point if the Region operation
    // is destroy i.e. we may not get to the transaction block
    // before the destroy timer fires.

    String regName = exprReg.getName();
    // Test to ensure a region expriation does not cause a conflict    
    this.txMgr.begin();
    exprReg.put("key0", "value");
    waitDance(regionExpiry, waitMs);
    assertEquals("value", exprReg.getEntry("key0").getValue());
    try {
      this.txMgr.commit();
    } catch (CommitConflictException error) {
      fail("Expiration should not cause commit to fail");
    }
    try {
      synchronized(regionExpiry) {
        if (!regionExpiry[0]) {
          assertEquals("value", exprReg.getEntry("key0").getValue());
          regionExpiry.wait(patientWaitMs);
          if (!regionExpiry[0]) {
            fail("Cache listener did not detect a region expiration in " + patientWaitMs + " ms!");
          }
        }
      }
    } catch (InterruptedException ie) {
      fail("Caught InterruptedException while waiting for eviction");
    }
    if (action == ExpirationAction.DESTROY) {
      assertNull("listener saw Region expiration, expected a destroy operation!", 
                 this.cache.getRegion(regName));
    } else {
      assertTrue("listener saw Region expriation, expected invalidation", 
                 !exprReg.containsValueForKey("key0"));
    }

    // @todo mitch test rollback and failed expiration
  }
}
