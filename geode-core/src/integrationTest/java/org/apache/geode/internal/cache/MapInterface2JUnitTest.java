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
/*
 * Created on Aug 23, 2005
 *
 *
 */
package org.apache.geode.internal.cache;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.util.StopWatch;
import org.apache.geode.test.dunit.ThreadUtils;

public class MapInterface2JUnitTest {

  protected boolean afterClearCallbackOccurred = false;
  boolean mainThreadProceed = false;

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      region.put("" + i, new Portfolio(i));
    }
  }

  @After
  public void tearDown() {
    CacheUtils.closeCache();
  }

  @Test
  public void testBasicMapClearNonTrnxn() {
    Region rgn = CacheUtils.getRegion("Portfolios");
    int size = rgn.size();
    assertTrue("MapInterface2JUnitTest::basicMapClearNonTranxn: The init size of region is zero",
        size > 0);
    rgn.clear();
    if (rgn.size() != 0) {
      fail("The region size is non zerio even after issuing clear");
    }
  }

  @Test
  public void testBasicMapClearTrnxn() {
    Region rgn = CacheUtils.getRegion("Portfolios");
    int size = rgn.size();
    assertTrue("MapInterface2JUnitTest::basicMapClearNonTranxn: The init size of region is zero",
        size > 0);
    CacheTransactionManager tm = CacheUtils.getCacheTranxnMgr();
    tm.begin();
    rgn.put("6", new Portfolio(6));
    assertTrue(rgn.size() == 5);
    try {
      rgn.clear();
      fail("expected exception not thrownn");
    } catch (UnsupportedOperationInTransactionException e) {
    }
    if (rgn.size() == 0) {
      fail("The region size is zero even after throwing exception");
    }
    try {
      tm.commit();
    } catch (CommitConflictException cce) {
      // Ignore
    }
    if (rgn.size() == 0) {
      fail("The region size is zero even after throwing exception");
    }
  }

  @Test
  public void testBasicMapAfterClearCalback() {
    Region rgn = CacheUtils.getRegion("Portfolios");
    AttributesMutator atm = rgn.getAttributesMutator();
    atm.addCacheListener(new CacheListenerAdapter() {

      @Override
      public void afterRegionClear(RegionEvent event) {
        synchronized (MapInterface2JUnitTest.this) {
          event.getRegion().getCache().getLogger().info("afterRegionClear call back " + event);
          afterClearCallbackOccurred = true;
          MapInterface2JUnitTest.this.notify();
        }
      }
    });
    int size = rgn.size();
    assertTrue("MapInterface2JUnitTest::basicMapClearNonTranxn: The init size of region is zero",
        size > 0);
    rgn.clear();
    if (rgn.size() != 0) {
      fail("The region size is non zero even after issuing clear");
    }
    if (rgn.size() != 0) {
      fail("The region size is non zero even after issuing clear");
    }
    try {
      synchronized (this) {
        if (!afterClearCallbackOccurred) {
          wait(10000);
        }
      }
    } catch (InterruptedException ie) {
      fail(ie.toString());
    }
    if (!afterClearCallbackOccurred) {
      fail("afterClear Callback not issued");
    }
  }

  @Test
  public void testBlockGlobalScopeInSingleVM() {
    CacheUtils.getCache().setLockLease(40);
    CacheUtils.getCache().setLockTimeout(5);
    final Region region = CacheUtils.createRegion("Global", String.class, Scope.GLOBAL);
    for (int i = 0; i < 10; i++) {
      region.put("" + i, "" + i);
    }

    final Object callbackSync = new Object();
    final boolean[] canCallbackProceed = new boolean[] {false};
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      @Override
      public void afterRegionClear(RegionEvent event) {
        // Allow main thread to proceed just before sleeping

        try {
          synchronized (MapInterface2JUnitTest.this) {
            mainThreadProceed = true;
            MapInterface2JUnitTest.this.notify();
          }
          event.getRegion().getCache().getLogger().info("*******Main THread Notified *********");
          synchronized (callbackSync) {
            long maxWait = 20000;
            StopWatch timer = new StopWatch(true);
            while (!canCallbackProceed[0]) {
              long timeLeft = maxWait - timer.elapsedTimeMillis();
              if (timeLeft > 0) {
                callbackSync.wait(timeLeft);
              } else {
                fail("testBlockGlobalScopeInSingleVM attempted to wait too long");
              }
            }
          }
          event.getRegion().getCache().getLogger().info("******* Callback complete *********");
        } catch (InterruptedException ie) {
          ie.printStackTrace();
        }
      }
    });
    Thread th = new Thread(new Runnable() {
      @Override
      public void run() {
        region.clear();
      }

    });

    th.start();
    try {
      synchronized (this) {
        if (!mainThreadProceed) {
          region.getCache().getLogger().info("*******Main THread is going in wait********");
          wait();
        }
      }
      region.getCache().getLogger().info("*******Main THread coming out of wait*********");
      region.put("test", "test");
      fail("The put operation should not have succeeded");
    } catch (org.apache.geode.cache.TimeoutException cwe) {
      assertTrue("The test correctly encounetred a TimeoutException" + cwe, true);
    } catch (InterruptedException ie) {
      fail("The main thread experienced Interruption" + ie);
    } finally {
      synchronized (callbackSync) {
        canCallbackProceed[0] = true;
        callbackSync.notify();
      }
    }
    ThreadUtils.join(th, 30 * 1000);
  }


  @Test
  public void testSuccessGlobalScopeInSingleVM() {
    CacheUtils.getCache().setLockLease(10);
    CacheUtils.getCache().setLockTimeout(15);
    final Region region = CacheUtils.createRegion("Global", String.class, Scope.GLOBAL);
    for (int i = 0; i < 10; i++) {
      region.put("" + i, "" + i);
    }

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      @Override
      public void afterRegionClear(RegionEvent event) {
        // Allwo main thread to proceed just before sleeping

        try {
          synchronized (MapInterface2JUnitTest.this) {
            mainThreadProceed = true;
            MapInterface2JUnitTest.this.notify();
          }
          event.getRegion().getCache().getLogger().info("*******Main THread Notified *********");
          Thread.sleep(1000);
          event.getRegion().getCache().getLogger().info("******* After Sleeping 5000 *********");
        } catch (InterruptedException ie) {
          fail("interrupted");
        }
      }
    });
    Thread th = new Thread(new Runnable() {
      @Override
      public void run() {
        region.clear();
      }

    });

    th.start();
    try {
      synchronized (this) {
        if (!mainThreadProceed) {
          region.getCache().getLogger().info("*******Main THread is going in wait********");
          wait();
        }
      }
      region.getCache().getLogger().info("*******Main THread coming out of wait*********");
      region.put("test", "test");
      String str = (String) region.get("test");
      assertNotNull("The put operation has succeeded", str);
    } catch (Exception cwe) {
      fail("The test experienced exception " + cwe);
    }
    ThreadUtils.join(th, 30 * 1000);
  }


}
