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

import org.apache.geode.cache.*;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.*;
import org.apache.geode.internal.cache.ExpiryTask.ExpiryTaskListener;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.IntegrationTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.*;

/**
 * Tests transaction expiration functionality
 *
 * @since GemFire 4.0
 */
@Category(IntegrationTest.class)
public class TXExpiryJUnitTest {

  protected GemFireCacheImpl cache;
  protected CacheTransactionManager txMgr;

  protected void createCache() throws CacheException {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0"); // loner
    this.cache = (GemFireCacheImpl) (new CacheFactory(p)).create();
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
  }

  @Test
  public void testEntryTTLExpiration() throws CacheException {
    generalEntryExpirationTest(createRegion("TXEntryTTL"),
        new ExpirationAttributes(1, ExpirationAction.DESTROY), true);
  }

  @Test
  public void testEntryIdleExpiration() throws CacheException {
    generalEntryExpirationTest(createRegion("TXEntryIdle"),
        new ExpirationAttributes(1, ExpirationAction.DESTROY), false);
  }

  private Region<String, String> createRegion(String name) {
    RegionFactory<String, String> rf = this.cache.createRegionFactory();
    rf.setScope(Scope.DISTRIBUTED_NO_ACK);
    rf.setStatisticsEnabled(true);
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      return rf.create(name);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }

  public void generalEntryExpirationTest(final Region<String, String> exprReg,
      ExpirationAttributes exprAtt, boolean useTTL) throws CacheException {
    final LocalRegion lr = (LocalRegion) exprReg;
    final boolean wasDestroyed[] = {false};
    AttributesMutator<String, String> mutator = exprReg.getAttributesMutator();
    final AtomicInteger ac = new AtomicInteger();
    final AtomicInteger au = new AtomicInteger();
    final AtomicInteger ai = new AtomicInteger();
    final AtomicInteger ad = new AtomicInteger();

    if (useTTL) {
      mutator.setEntryTimeToLive(exprAtt);
    } else {
      mutator.setEntryIdleTimeout(exprAtt);
    }
    final CacheListener<String, String> cl = new CacheListenerAdapter<String, String>() {
      public void afterCreate(EntryEvent<String, String> e) {
        ac.incrementAndGet();
      }

      public void afterUpdate(EntryEvent<String, String> e) {
        au.incrementAndGet();
      }

      public void afterInvalidate(EntryEvent<String, String> e) {
        ai.incrementAndGet();
      }

      public void afterDestroy(EntryEvent<String, String> e) {
        ad.incrementAndGet();
        if (e.getKey().equals("key0")) {
          synchronized (wasDestroyed) {
            wasDestroyed[0] = true;
            wasDestroyed.notifyAll();
          }
        }
      }

      public void afterRegionInvalidate(RegionEvent<String, String> event) {
        fail("Unexpected invocation of afterRegionInvalidate");
      }

      public void afterRegionDestroy(RegionEvent<String, String> event) {
        if (!event.getOperation().isClose()) {
          fail("Unexpected invocation of afterRegionDestroy");
        }
      }
    };
    mutator.addCacheListener(cl);
    try {

      ExpiryTask.suspendExpiration();
      // Test to ensure an expiration does not cause a conflict
      for (int i = 0; i < 2; i++) {
        exprReg.put("key" + i, "value" + i);
      }
      this.txMgr.begin();
      exprReg.put("key0", "value");
      waitForEntryExpiration(lr, "key0");
      assertEquals("value", exprReg.getEntry("key0").getValue());
      try {
        ExpiryTask.suspendExpiration();
        this.txMgr.commit();
      } catch (CommitConflictException error) {
        fail("Expiration should not cause commit to fail");
      }
      assertEquals("value", exprReg.getEntry("key0").getValue());
      waitForEntryExpiration(lr, "key0");
      synchronized (wasDestroyed) {
        assertEquals(true, wasDestroyed[0]);
      }
      assertTrue(!exprReg.containsKey("key0"));
      // key1 is the canary for the rest of the entries
      waitForEntryToBeDestroyed(exprReg, "key1");

      // rollback and failed commit test, ensure expiration continues
      for (int j = 0; j < 2; j++) {
        synchronized (wasDestroyed) {
          wasDestroyed[0] = false;
        }
        ExpiryTask.suspendExpiration();
        for (int i = 0; i < 2; i++) {
          exprReg.put("key" + i, "value" + i);
        }
        this.txMgr.begin();
        exprReg.put("key0", "value");
        waitForEntryExpiration(lr, "key0");
        assertEquals("value", exprReg.getEntry("key0").getValue());
        String checkVal;
        ExpiryTask.suspendExpiration();
        if (j == 0) {
          checkVal = "value0";
          this.txMgr.rollback();
        } else {
          checkVal = "conflictVal";
          final TXManagerImpl txMgrImpl = (TXManagerImpl) this.txMgr;
          TXStateProxy tx = txMgrImpl.internalSuspend();
          exprReg.put("key0", checkVal);
          txMgrImpl.resume(tx);
          try {
            this.txMgr.commit();
            fail("Expected CommitConflictException!");
          } catch (CommitConflictException expected) {
          }
        }
        waitForEntryExpiration(lr, "key0");
        synchronized (wasDestroyed) {
          assertEquals(true, wasDestroyed[0]);
        }
        assertTrue(!exprReg.containsKey("key0"));
        // key1 is the canary for the rest of the entries
        waitForEntryToBeDestroyed(exprReg, "key1");
      }
    } finally {
      mutator.removeCacheListener(cl);
      ExpiryTask.permitExpiration();
    }
  }

  private void waitForEntryToBeDestroyed(final Region r, final String key) {
    WaitCriterion waitForExpire = new WaitCriterion() {
      public boolean done() {
        return r.getEntry(key) == null;
      }

      public String description() {
        return "never saw entry destroy of " + key;
      }
    };
    Wait.waitForCriterion(waitForExpire, 3000, 10, true);
  }

  public static void waitForEntryExpiration(LocalRegion lr, String key) {
    try {
      ExpirationDetector detector;
      do {
        detector = new ExpirationDetector(lr.getEntryExpiryTask(key));
        ExpiryTask.expiryTaskListener = detector;
        ExpiryTask.permitExpiration();
        Wait.waitForCriterion(detector, 3000, 2, true);
      } while (!detector.hasExpired() && detector.wasRescheduled());
    } finally {
      ExpiryTask.expiryTaskListener = null;
    }
  }

  private void waitForRegionExpiration(LocalRegion lr, boolean ttl) {
    try {
      ExpirationDetector detector;
      do {
        detector = new ExpirationDetector(
            ttl ? lr.getRegionTTLExpiryTask() : lr.getRegionIdleExpiryTask());
        ExpiryTask.expiryTaskListener = detector;
        ExpiryTask.permitExpiration();
        Wait.waitForCriterion(detector, 3000, 2, true);
      } while (!detector.hasExpired() && detector.wasRescheduled());
    } finally {
      ExpiryTask.expiryTaskListener = null;
    }
  }


  /**
   * Used to detect that a particular ExpiryTask has expired.
   */
  public static class ExpirationDetector implements ExpiryTaskListener, WaitCriterion {
    private volatile boolean ran = false;
    private volatile boolean expired = false;
    private volatile boolean rescheduled = false;
    public final ExpiryTask et;

    public ExpirationDetector(ExpiryTask et) {
      assertNotNull(et);
      this.et = et;
    }

    @Override
    public void afterCancel(ExpiryTask et) {}

    @Override
    public void afterSchedule(ExpiryTask et) {}

    @Override
    public void afterReschedule(ExpiryTask et) {
      if (et == this.et) {
        if (!hasExpired()) {
          ExpiryTask.suspendExpiration();
        }
        this.rescheduled = true;
      }
    }

    @Override
    public void afterExpire(ExpiryTask et) {
      if (et == this.et) {
        this.expired = true;
      }
    }

    @Override
    public void afterTaskRan(ExpiryTask et) {
      if (et == this.et) {
        this.ran = true;
      }
    }

    @Override
    public boolean done() {
      return this.ran;
    }

    @Override
    public String description() {
      return "the expiry task " + this.et + " never ran";
    }

    public boolean wasRescheduled() {
      return this.rescheduled;
    }

    public boolean hasExpired() {
      return this.expired;
    }
  }

  @Category(FlakyTest.class) // GEODE-845: time sensitive, expiration, eats exceptions (1 fixed),
                             // waitForCriterion, 3 second timeout
  @Test
  public void testRegionIdleExpiration() throws CacheException {
    Region<String, String> exprReg = createRegion("TXRegionIdle");
    generalRegionExpirationTest(exprReg, new ExpirationAttributes(1, ExpirationAction.INVALIDATE),
        false);
    generalRegionExpirationTest(exprReg, new ExpirationAttributes(1, ExpirationAction.DESTROY),
        false);
  }

  @Test
  public void testRegionTTLExpiration() throws CacheException {
    Region<String, String> exprReg = createRegion("TXRegionTTL");
    generalRegionExpirationTest(exprReg, new ExpirationAttributes(1, ExpirationAction.INVALIDATE),
        true);
    generalRegionExpirationTest(exprReg, new ExpirationAttributes(1, ExpirationAction.DESTROY),
        true);
  }

  private void generalRegionExpirationTest(final Region<String, String> exprReg,
      ExpirationAttributes exprAtt, boolean useTTL) throws CacheException {
    final LocalRegion lr = (LocalRegion) exprReg;
    final ExpirationAction action = exprAtt.getAction();
    final boolean regionExpiry[] = {false};
    AttributesMutator<String, String> mutator = exprReg.getAttributesMutator();
    final CacheListener<String, String> cl = new CacheListenerAdapter<String, String>() {
      public void afterRegionInvalidate(RegionEvent<String, String> event) {
        synchronized (regionExpiry) {
          regionExpiry[0] = true;
          regionExpiry.notifyAll();
        }
      }

      public void afterRegionDestroy(RegionEvent<String, String> event) {
        if (!event.getOperation().isClose()) {
          synchronized (regionExpiry) {
            regionExpiry[0] = true;
            regionExpiry.notifyAll();
          }
        }
      }
    };
    mutator.addCacheListener(cl);
    // Suspend before enabling region expiration to prevent
    // it from happening before we do the put.
    ExpiryTask.suspendExpiration();
    try {
      if (useTTL) {
        mutator.setRegionTimeToLive(exprAtt);
      } else {
        mutator.setRegionIdleTimeout(exprAtt);
      }

      // Create some keys and age them, I wish we could fake/force the age
      // instead of having to actually wait
      for (int i = 0; i < 2; i++) {
        exprReg.put("key" + i, "value" + i);
      }

      String regName = exprReg.getName();
      // Test to ensure a region expiration does not cause a conflict
      this.txMgr.begin();
      exprReg.put("key0", "value");
      waitForRegionExpiration(lr, useTTL);
      assertEquals("value", exprReg.getEntry("key0").getValue());
      try {
        ExpiryTask.suspendExpiration();
        this.txMgr.commit();
      } catch (CommitConflictException error) {
        Assert.fail("Expiration should not cause commit to fail", error);
      }
      assertEquals("value", exprReg.getEntry("key0").getValue());
      waitForRegionExpiration(lr, useTTL);
      synchronized (regionExpiry) {
        assertEquals(true, regionExpiry[0]);
      }
      if (action == ExpirationAction.DESTROY) {
        assertNull("listener saw Region expiration, expected a destroy operation!",
            this.cache.getRegion(regName));
      } else {
        assertTrue("listener saw Region expiration, expected invalidation",
            !exprReg.containsValueForKey("key0"));
      }

    } finally {
      if (!exprReg.isDestroyed()) {
        mutator.removeCacheListener(cl);
      }
      ExpiryTask.permitExpiration();
    }

    // @todo mitch test rollback and failed expiration
  }
}
