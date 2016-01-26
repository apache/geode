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
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.internal.cache.CacheObserver;
import com.gemstone.gemfire.internal.cache.CacheObserverAdapter;
import com.gemstone.gemfire.internal.cache.CacheObserverHolder;
import com.gemstone.gemfire.internal.cache.DiskEntry;
import com.gemstone.gemfire.internal.cache.DiskRegionHelperFactory;
import com.gemstone.gemfire.internal.cache.DiskRegionProperties;
import com.gemstone.gemfire.internal.cache.DiskRegionTestingBase;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * This JUnit test tests concurrent rolling and normal region operations
 * put,get,clear,destroy in both sync and async mode
 * 
 * A region operation is done on the same key that is about to be rolled or has
 * just been rolled and the region operation is verified to have been correctly
 * executed.
 * 
 * @author Mitul Bid
 *  
 */
@Category(IntegrationTest.class)
public class ConcurrentRollingAndRegionOperationsJUnitTest extends
    DiskRegionTestingBase
{

  protected volatile boolean hasBeenNotified;

  protected int rollingCount = 0;

  protected boolean encounteredFailure = false;


  @Before
  public void setUp() throws Exception
  {
    this.hasBeenNotified = false;
    super.setUp();
  }

  void putBeforeRoll(final Region region)
  {
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void beforeGoingToCompact()
      {
        region.put("Key", "Value2");
      }

      public void afterHavingCompacted()
      {
        synchronized (region) {
          region.notify();
          hasBeenNotified = true;
        }
      }
    });
    region.put("Key", "Value1");
    switchOplog(region);
    synchronized (region) {
      if (!hasBeenNotified) {
        try {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        }
        catch (InterruptedException e) {
          fail("exception not expected here");
        }
      }
    }
    try {
      Assert.assertEquals("Value2", getValueOnDisk(region));
    }
    catch (EntryNotFoundException e) {
      logWriter.error("Exception occured", e);
      fail("Entry not found although was supposed to be there");
    }
  }

  void getBeforeRoll(final Region region)
  {
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void beforeGoingToCompact()
      {
        region.get("Key");
      }

      public void afterHavingCompacted()
      {
        synchronized (region) {
          region.notify();
          hasBeenNotified = true;
        }
      }
    });
    region.put("Key", "Value1");
    switchOplog(region);
    synchronized (region) {
      if (!hasBeenNotified) {
        try {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        }
        catch (InterruptedException e) {
          fail("exception not expected here");
        }
      }
    }
    try {
      Assert.assertEquals("Value1", getValueOnDisk(region));
      Assert.assertEquals("Value1", getValueInHTree(region));
    }
    catch (EntryNotFoundException e) {
      logWriter.error("Exception occured", e);
      fail("Entry not found although was supposed to be there");
    }
  }

  void delBeforeRoll(final Region region)
  {
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void beforeGoingToCompact()
      {
        synchronized (region) {
          region.notify();
          hasBeenNotified = true;
        }
      }
    });
    region.put("Key", "Value1");
    switchOplog(region);
    synchronized (region) {
      if (!hasBeenNotified) {
        try {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        }
        catch (InterruptedException e) {
          fail("exception not expected here");
        }
      }
    }
    try {
      region.destroy("Key");
    }
    catch (Exception e) {
      logWriter.error("Exception occured", e);
      fail("failed while trying to destroy due to " + e);
    }
    boolean entryNotFound = false;
    try {
      getValueOnDisk(region);
    }
    catch (EntryNotFoundException e) {
      entryNotFound = true;
    }
    if (!entryNotFound) {
      fail("EntryNotFoundException was expected but did not get it");
    }
    entryNotFound = false;
    Object obj = ((LocalRegion)region).basicGetEntry("Key");
    if (obj == null) {
      entryNotFound = true;
    }
    if (!entryNotFound) {
      fail("EntryNotFoundException was expected but did not get it");
    }
  }

  void clearBeforeRoll(final Region region)
  {
    this.hasBeenNotified = false;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void beforeGoingToCompact()
      {
        synchronized (region) {
          region.notify();
          hasBeenNotified = true;
        }
      }
    });
    region.put("Key", "Value1");
    switchOplog(region);
    synchronized (region) {
      if (!hasBeenNotified) {
        try {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        }
        catch (InterruptedException e) {
          fail("exception not expected here");
        }
      }
    }
    region.clear();
    boolean entryNotFound = false;
    try {
      getValueOnDisk(region);
    }
    catch (EntryNotFoundException e) {
      entryNotFound = true;
    }
    if (!entryNotFound) {
      fail("EntryNotFoundException was expected but did not get it");
    }
    entryNotFound = false;
    Object obj = ((LocalRegion)region).basicGetEntry("Key");
    if (obj == null) {
      entryNotFound = true;
    }
    if (!entryNotFound) {
      fail("EntryNotFoundException was expected but did not get it");
    }
  }

  void putAfterRoll(final Region region)
  {
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void beforeGoingToCompact()
      {
        region.put("Key", "Value1");
      }

      public void afterHavingCompacted()
      {
        synchronized (region) {
          region.notify();
          hasBeenNotified = true;
        }
      }
    });
    region.put("makeNonEmpty", "needSomethingSoIt can be compacted");
    switchOplog(region);
    synchronized (region) {
      if (!hasBeenNotified) {
        try {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        }
        catch (InterruptedException e) {
          fail("exception not expected here");
        }
      }
    }
    region.put("Key", "Value2");
    try {
      Assert.assertEquals("Value2", getValueOnDisk(region));
    }
    catch (EntryNotFoundException e) {
      logWriter.error("Exception occured", e);
      fail("Entry not found although was supposed to be there");
    }
  }

  void getAfterRoll(final Region region)
  {
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void afterHavingCompacted()
      {
        synchronized (region) {
          region.notify();
          hasBeenNotified = true;
        }
      }
    });
    region.put("Key", "Value1");
    switchOplog(region);
    synchronized (region) {
      if (!hasBeenNotified) {
        try {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        }
        catch (InterruptedException e) {
          fail("exception not expected here");
        }
      }
    }
    region.get("Key");
    try {
      Assert.assertEquals("Value1", getValueOnDisk(region));
      Assert.assertEquals("Value1", getValueInHTree(region));
    }
    catch (EntryNotFoundException e) {
      logWriter.error("Exception occured", e);
      fail("Entry not found although was supposed to be there");
    }
  }

  void delAfterRoll(final Region region)
  {
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void afterHavingCompacted()
      {
        synchronized (region) {
          region.notify();
          hasBeenNotified = true;
        }
      }
    });
    region.put("Key", "Value1");
    switchOplog(region);
    synchronized (region) {
      if (!hasBeenNotified) {
        try {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        }
        catch (InterruptedException e) {
          fail("exception not expected here");
        }
      }
    }
    try {
      region.destroy("Key");
    }
    catch (Exception e1) {
      logWriter.error("Exception occured", e1);
      fail("encounter exception when not expected " + e1);
    }
    boolean entryNotFound = false;
    try {
      getValueOnDisk(region);
    }
    catch (EntryNotFoundException e) {
      entryNotFound = true;
    }
    if (!entryNotFound) {
      fail("EntryNotFoundException was expected but did not get it");
    }
    entryNotFound = false;
    Object obj = ((LocalRegion)region).basicGetEntry("Key");
    if (obj == null) {
      entryNotFound = true;
    }
    if (!entryNotFound) {
      fail("EntryNotFoundException was expected but did not get it");
    }
  }

  void clearAfterRoll(final Region region)
  {
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void afterHavingCompacted()
      {
        synchronized (region) {
          region.notify();
          hasBeenNotified = true;
        }
      }
    });
    region.put("Key", "Value1");
    switchOplog(region);
    synchronized (region) {
      if (!hasBeenNotified) {
        try {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        }
        catch (InterruptedException e) {
          fail("exception not expected here");
        }
      }
    }
    region.clear();
    boolean entryNotFound = false;
    try {
      getValueOnDisk(region);
    }
    catch (EntryNotFoundException e) {
      entryNotFound = true;
    }
    if (!entryNotFound) {
      fail("EntryNotFoundException was expected but did not get it");
    }
    entryNotFound = false;
    Object obj = ((LocalRegion)region).basicGetEntry("Key");
    if (obj == null) {
      entryNotFound = true;
    }
    if (!entryNotFound) {
      fail("EntryNotFoundException was expected but did not get it");
    }
  }

  private void switchOplog(Region region)
  {
    ((LocalRegion)region).getDiskRegion().forceFlush();
    region.forceRolling();
  }

  Object getValueOnDisk(Region region) throws EntryNotFoundException
  {
    ((LocalRegion)region).getDiskRegion().forceFlush();
    return ((LocalRegion)region).getValueOnDisk("Key");
  }

  Object getValueInHTree(Region region)
  {
    RegionEntry re = ((LocalRegion)region).basicGetEntry("Key");
    return ((LocalRegion)region).getDiskRegion().getNoBuffer(
        ((DiskEntry)re).getDiskId());
  }

  public void DARREL_DISABLE_testSyncPutBeforeRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskRegionProperties, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    putBeforeRoll(region);
    region.destroyRegion();
  }

  public void DARREL_DISABLE_testAsyncPutBeforeRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache,
        diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    putBeforeRoll(region);
    region.destroyRegion();
  }

  public void DARREL_DISABLE_testSyncPutAfterRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskRegionProperties, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    putAfterRoll(region);
    region.destroyRegion();
  }

  public void DARREL_DISABLE_testAsyncPutAfterRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache,
        diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    putAfterRoll(region);
    region.destroyRegion();
  }

  public void DARREL_DISABLE_testSyncGetBeforeRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskRegionProperties, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    getBeforeRoll(region);
    region.destroyRegion();
  }

  public void DARREL_DISABLE_testAsyncGetBeforeRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache,
        diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    getBeforeRoll(region);
    region.destroyRegion();
  }

  public void DARREL_DISABLE_testSyncGetAfterRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskRegionProperties, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    getAfterRoll(region);
    region.destroyRegion();
  }

  public void DARREL_DISABLE_testAsyncGetAfterRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache,
        diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    getAfterRoll(region);
    region.destroyRegion();
  }

  public void DARREL_DISABLE_testSyncClearBeforeRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskRegionProperties, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    clearBeforeRoll(region);
    region.destroyRegion();
  }

  public void DARREL_DISABLE_testAsyncClearBeforeRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache,
        diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    clearBeforeRoll(region);
    region.destroyRegion();
  }

  public void DARREL_DISABLE_testSyncClearAfterRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskRegionProperties, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    clearAfterRoll(region);
    region.destroyRegion();
  }

  public void DARREL_DISABLE_testAsyncClearAfterRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache,
        diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    clearAfterRoll(region);
    region.destroyRegion();
  }

  public void DARREL_DISABLE_testSyncDelBeforeRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskRegionProperties, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    delBeforeRoll(region);
    region.destroyRegion();
  }

  public void DARREL_DISABLE_testAsyncDelBeforeRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache,
        diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    delBeforeRoll(region);
    region.destroyRegion();
  }

  public void DARREL_DISABLE_testSyncDelAfterRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskRegionProperties, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    delAfterRoll(region);
    region.destroyRegion();
  }

  public void DARREL_DISABLE_testAsyncDelAfterRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache,
        diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    delAfterRoll(region);
    region.destroyRegion();
  }

  public void DARREL_DISABLE_testCloseBeforeRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskRegionProperties, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    closeBeforeRoll(region);
  }

  public void DARREL_DISABLE_testCloseAfterRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskRegionProperties, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    closeAfterRoll(region);
    //Asif :Recreate the region so it gets destroyed in tear down
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskRegionProperties, Scope.LOCAL);
  }

  public void DARREL_DISABLE_testconcurrentPutAndRoll()
  {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskRegionProperties, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    concurrentPutAndRoll(region);
    region.destroyRegion();
  }

  private void concurrentPutAndRoll(final Region region)
  {
    hasBeenNotified = false;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      long startTime, endTime, totalTime = 0;

      boolean localHasBeenNotified = false;

      public void beforeGoingToCompact()
      {
        final Object obj = new Object();
        Thread thread1 = new Thread() {

          public void run()
          {
            RegionEntry re = ((LocalRegion)region).basicGetEntry("Key");

            synchronized (re) {
              synchronized (obj) {
                obj.notify();
                localHasBeenNotified = true;
              }
              try {
                Thread.sleep(2000);
              }
              catch (InterruptedException e) {
                testFailed = true;
                failureCause = "Exception occured when it was not supposed to occur, Exception is "
                    + e + "in concurrentPutAndRoll";
                fail("exception not expected here");
              }
            }
          }
        };
        thread1.start();
        synchronized (obj) {
          try {
            if (!localHasBeenNotified) {
              obj.wait(10000);
              assertTrue(localHasBeenNotified);
            }
          }
          catch (InterruptedException e) {
            testFailed = true;
            failureCause = "Exception occured when it was not supposed to occur, Exception is "
                + e + "in concurrentPutAndRoll";
            fail("exception not expected here");
          }
        }
        startTime = System.currentTimeMillis();
      }

      public void afterHavingCompacted()
      {
        endTime = System.currentTimeMillis();
        totalTime = endTime - startTime;
        setTotalTime(totalTime);
        synchronized (region) {
          region.notify();
          hasBeenNotified = true;
        }
      }
    });
    region.put("Key", "Value1");
    switchOplog(region);
    synchronized (region) {
      try {
        if (!hasBeenNotified) {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        }
      }
      catch (InterruptedException e) {
        fail("exception not expected here");
      }
    }
    if (this.totalTime < 2000) {
      fail(" It should have taken more than 2000 millisecs but it took "
          + totalTime);
    }
    assertFalse(failureCause, testFailed);
  }

  /**
   * Check if the roller thread cant skip rolling the entry & if a get is done
   * on that entry , it is possible for the get operation to get the Oplog which
   * is not yet destroyed but by the time a basicGet is done,the oplog gets
   * destroyed & the get operation sees the file length zero or it may encounter
   * null pointer exception while retrieving the oplog.
   * 
   * @author Asif
   *  
   */
  @Test
  public void testConcurrentRollingAndGet()
  {
    final int MAX_OPLOG_SIZE = 1000*2;
    DiskRegionProperties diskProps = new DiskRegionProperties();
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setCompactionThreshold(100);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(false);
    final int TOTAL_SWITCHING = 200;
    final int TOTAL_KEYS = 20;

    final List threads = new ArrayList();
    final byte[] val = new byte[100];
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;

    CacheObserver old = CacheObserverHolder
        .setInstance(new CacheObserverAdapter() {
          public void beforeGoingToCompact()
          {
            for (int k = 0; k < TOTAL_KEYS; ++k) {
              final int num = k;
              Thread th = new Thread(new Runnable() {
                public void run()
                {

                  byte[] val_on_disk = null;
                  try {
                    val_on_disk = (byte[])((LocalRegion)region)
                        .getValueOnDisk("key" + (num + 1));
                    assertTrue(
                        "byte  array was not of right size  as its size was "
                            + val_on_disk.length, val_on_disk.length == 100);

                  }
                  catch (Exception e) {
                    encounteredFailure = true;
                    logWriter.error("Test encountered exception ", e);
                    fail(" Test failed as could not obtain value from disk.Exception = "
                        + e);
                  }

                }

              });
              threads.add(th);
            }
            for (int j = 0; j < TOTAL_KEYS; ++j) {
              ((Thread)threads.get(rollingCount++)).start();
            }

          }

        });

    for (int i = 0; i < TOTAL_SWITCHING; ++i) {
      for (int j = 0; j < TOTAL_KEYS; ++j) {
        region.put("key" + (j + 1), val);

      }
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      try {
        Thread.sleep(100);
      }
      catch (InterruptedException e) {
        logWriter.error("Main thread encountered exception ", e);

        fail(" Test failed as main thread encountered exception = " + e);

      }
    }

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    for (int i = 0; i < threads.size(); ++i) {
      Thread th = (Thread)threads.get(i);
      if (th != null) {
        DistributedTestCase.join(th, 30 * 1000, null);
      }
    }
    assertTrue(
        "The test will fail as atleast one  thread doing get operation encounetred exception",
        !encounteredFailure);
    CacheObserverHolder.setInstance(old);
    closeDown();

  }

  private volatile long totalTime = 0;

  protected void setTotalTime(long time)
  {
    this.totalTime = time;
  }

  void closeAfterRoll(final Region region)
  {
    hasBeenNotified = false;
    final Close th = new Close(region);
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void afterHavingCompacted()
      {
        LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
        try {
          th.start();
          Thread.sleep(3000);
        }
        catch (Exception e) {
          logWriter.error("Exception occured", e);
          fail("Exception occured when it was not supposed to occur");
        }
      }
    });
    region.put("Key", "Value1");
    switchOplog(region);
    synchronized (region) {
      try {
        if (!hasBeenNotified) {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        }
      }
      catch (InterruptedException e) {
        fail("exception not expected here");
      }
    }
    try {
      th.join(5000);
    } catch (InterruptedException ignore) {
      fail("exception not expected here");
    }
    assertFalse(th.isAlive());
    assertFalse(failureCause, testFailed);
  }

  void closeBeforeRoll(final Region region)
  {
    hasBeenNotified = false;
    final Close th = new Close(region);
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void beforeGoingToCompact()
      {
        LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
        try {
          th.start();
          Thread.sleep(3000);
        }
        catch (Exception e) {
          logWriter.error("Exception occured", e);
          fail("Exception occured when it was not supposed to occur");
        }
      }
    });
    region.put("Key", "Value1");
    switchOplog(region);
    synchronized (region) {
      try {
        if (!hasBeenNotified) {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        }
      }
      catch (InterruptedException e) {
        fail("exception not expected here");
      }
    }
    try {
      th.join(5000);
    } catch (InterruptedException ignore) {
      fail("exception not expected here");
    }
    assertFalse(th.isAlive());
    assertFalse(failureCause, testFailed);
  }

  class Close extends Thread
  {

    private Region region;

    Close(Region region) {
      this.region = region;
    }

    public void run()
    {
      try {
        region.close();
        synchronized (region) {
          region.notify();
          hasBeenNotified = true;
        }
      }
      catch (Exception e) {
        logWriter.error("Exception occured", e);
        testFailed = true;
        failureCause = "Exception occured when it was not supposed to occur, due to "
            + e;
        fail("Exception occured when it was not supposed to occur, due to " + e);
      }
    }
  }
}
