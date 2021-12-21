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
package org.apache.geode.internal.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;

import org.junit.Test;

import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.eviction.EvictionCounters;

/**
 * Miscellaneous disk tests
 */
public class DiskRegOplogSwtchingAndRollerJUnitTest extends DiskRegionTestingBase {

  private static File[] dirs1 = null;

  private static int[] diskDirSize1 = null;

  private volatile boolean hasBeenNotified = false;

  private final DiskRegionProperties diskProps = new DiskRegionProperties();

  private final boolean encounteredException = false;

  private final Object forWaitNotify = new Object();

  private boolean gotNotification = false;

  private Object getValOnDsk = null;

  /**
   * tests non occurrence of DiskAccessException
   */
  @Test
  public void testSyncPersistRegionDAExp() {
    File testingDirectory1 = new File("testingDirectory1");
    testingDirectory1.mkdir();
    testingDirectory1.deleteOnExit();
    File file1 = new File("testingDirectory1/" + "testSyncPersistRegionDAExp" + "1");
    file1.mkdir();
    file1.deleteOnExit();
    dirs1 = new File[1];
    dirs1[0] = file1;
    diskDirSize1 = new int[1];
    diskDirSize1[0] = 2048;

    diskProps.setDiskDirsAndSizes(dirs1, diskDirSize1);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setMaxOplogSize(100000000);
    diskProps.setRegionName("region_SyncPersistRegionDAExp");
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    DiskStore ds = cache.findDiskStore(((LocalRegion) region).getDiskStoreName());
    // int[] diskSizes1 = ((LocalRegion)region).getDiskDirSizes();
    assertTrue(ds != null);
    int[] diskSizes1 = null;
    diskSizes1 = ds.getDiskDirSizes();
    assertEquals(1, diskDirSize1.length);
    assertTrue("diskSizes != 2048 ", diskSizes1[0] == 2048);

    diskAccessExpHelpermethod(region);

    // region.close(); // closes disk file which will flush all buffers
    closeDown();

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;

  }// end of testSyncPersistRegionDAExp

  @Test
  public void testAsyncPersistRegionDAExp() {
    File testingDirectory1 = new File("testingDirectory1");
    testingDirectory1.mkdir();
    testingDirectory1.deleteOnExit();
    File file1 = new File("testingDirectory1/" + "testAsyncPersistRegionDAExp" + "1");
    file1.mkdir();
    file1.deleteOnExit();
    dirs1 = new File[1];
    dirs1[0] = file1;
    diskDirSize1 = new int[1];
    diskDirSize1[0] = 2048;

    diskProps.setDiskDirsAndSizes(dirs1, diskDirSize1);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setMaxOplogSize(100000000);
    diskProps.setRegionName("region_AsyncPersistRegionDAExp");
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    DiskStore ds = cache.findDiskStore(((LocalRegion) region).getDiskStoreName());
    // int[] diskSizes1 = ((LocalRegion)region).getDiskDirSizes();
    assertTrue(ds != null);
    int[] diskSizes1 = null;
    diskSizes1 = ds.getDiskDirSizes();
    assertEquals(1, diskDirSize1.length);
    assertTrue("diskSizes != 2048 ", diskSizes1[0] == 2048);

    diskAccessExpHelpermethod(region);

    // region.close(); // closes disk file which will flush all buffers
    closeDown();
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
  }// end of testAsyncPersistRegionDAExp

  private void diskAccessExpHelpermethod(final Region region) {
    final byte[] value = new byte[990];
    Arrays.fill(value, (byte) 77);
    try {
      for (int i = 0; i < 2; i++) {
        region.put("" + i, value);
      }
    } catch (DiskAccessException e) {
      logWriter.error("exception not expected", e);
      fail("FAILED::" + e);
    }
    try {
      region.put("" + 2, value);
    } catch (DiskAccessException e) {
      fail("FAILED::DiskAccessException is NOT expected here !!");
    }
  }

  /**
   * DiskRegionRollingJUnitTest :
   */
  @Test
  public void testSyncRollingHappening() {
    try {
      DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
      diskRegionProperties.setDiskDirs(dirs);
      diskRegionProperties.setMaxOplogSize(512);
      diskRegionProperties.setRolling(true);
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      Region region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskRegionProperties,
          Scope.LOCAL);

      CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        @Override
        public void beforeGoingToCompact() {
          synchronized (forWaitNotify) {
            forWaitNotify.notify();
            gotNotification = true;
          }
        }
      });

      // This will end up 50% garbage
      // 1 live create
      // 2 live tombstones
      // 2 garbage creates and 1 garbage modify
      region.put("k1", "v1");
      region.put("k2", "v2");
      region.put("k2", "v3");
      region.put("k3", "v3");
      region.remove("k1");
      region.remove("k2");


      region.forceRolling();

      synchronized (forWaitNotify) {
        if (!gotNotification) {
          forWaitNotify.wait(10000);
        }
      }

      if (!gotNotification) {
        fail("Expected rolling to have happened but did not happen");
      }

    } catch (Exception e) {
      fail(" tests failed due to " + e);
    }
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
  }

  @Test
  public void testSyncRollingNotHappening() {
    try {
      DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
      diskRegionProperties.setDiskDirs(dirs);
      diskRegionProperties.setMaxOplogSize(512);
      diskRegionProperties.setRolling(false);
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      Region region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskRegionProperties,
          Scope.LOCAL);

      CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        @Override
        public void beforeGoingToCompact() {
          synchronized (forWaitNotify) {
            forWaitNotify.notify();
            gotNotification = true;
          }
        }
      });

      region.forceRolling();

      synchronized (forWaitNotify) {
        if (!gotNotification) {
          forWaitNotify.wait(3000);
        }
      }

      if (gotNotification) {
        fail("Expected rolling not to have happened but it did happen");
      }

    } catch (Exception e) {
      fail(" tests failed due to " + e);
    }

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
  }

  @Test
  public void testAsyncRollingHappening() {
    try {
      DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
      diskRegionProperties.setDiskDirs(dirs);
      diskRegionProperties.setMaxOplogSize(512);
      diskRegionProperties.setRolling(true);
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      Region region =
          DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskRegionProperties);

      CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        @Override
        public void beforeGoingToCompact() {
          synchronized (forWaitNotify) {
            forWaitNotify.notify();
            gotNotification = true;
          }
        }
      });


      // This will end up 50% garbage
      // 1 live create
      // 2 live tombstones
      // 2 garbage creates and 1 garbage modify
      region.put("k1", "v1");
      region.put("k2", "v2");
      region.put("k3", "v3");
      ((LocalRegion) region).forceFlush(); // so that the next modify doesn't conflate
      region.put("k2", "v3");
      ((LocalRegion) region).forceFlush(); // so that the next 2 removes don't conflate
      region.remove("k1");
      region.remove("k2");
      ((LocalRegion) region).forceFlush();

      region.forceRolling();

      synchronized (forWaitNotify) {
        if (!gotNotification) {
          forWaitNotify.wait(10000);
        }
      }

      if (!gotNotification) {
        fail("Expected rolling to have happened but did not happen");
      }

    } catch (Exception e) {
      fail(" tests failed due to " + e);
    }

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
  }

  @Test
  public void testAsyncRollingNotHappening() {
    try {
      DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
      diskRegionProperties.setDiskDirs(dirs);
      diskRegionProperties.setMaxOplogSize(512);
      diskRegionProperties.setRolling(false);
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      Region region =
          DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskRegionProperties);

      CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        @Override
        public void beforeGoingToCompact() {
          synchronized (forWaitNotify) {
            forWaitNotify.notify();
            gotNotification = true;
          }
        }
      });

      region.forceRolling();

      synchronized (forWaitNotify) {
        if (!gotNotification) {
          forWaitNotify.wait(3000);
        }
      }

      if (gotNotification) {
        fail("Expected rolling not to have happened but it did happen");
      }

    } catch (Exception e) {
      fail(" tests failed due to " + e);
    }
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
  }

  /**
   * DiskRegOplog1OverridingOplog2JUnitTest: Disk Region test : oplog1 flush overriding oplog2 flush
   *
   * This test will hold the flush of oplog1 and flush oplog2 before it. After that oplog1 is
   * allowed to flush. A get of an entry which was first put in oplog1 and then in oplog2 should
   * result in the get being done from oplog2.
   */
  @Test
  public void testOplog1FlushOverridingOplog2Flush() {
    hasBeenNotified = false;
    diskProps.setDiskDirs(dirs);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setBytesThreshold(100000000);
    diskProps.setTimeInterval(4000);
    diskProps.setRegionName("region_Oplog1FlushOverridingOplog2Flush");
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      boolean callOnce = false;

      @Override
      public void goingToFlush() {
        synchronized (this) {

          if (!callOnce) {
            try {
              region.put("1", "2");
            } catch (Exception e) {
              logWriter.error("exception not expected", e);
              failureCause = "FAILED::" + e;
              testFailed = true;
              fail("FAILED::" + e);
            }

            Thread th = new Thread(new DoesFlush(region));
            th.setName("TestingThread");
            th.start();
            Thread.yield();
          }
          callOnce = true;
        }

      }
    });

    try {
      region.put("1", "1");
    } catch (Exception e) {
      logWriter.error("exception not expected", e);
      fail("FAILED::" + e);
    }

    region.forceRolling();

    synchronized (region) {
      if (!hasBeenNotified) {
        try {
          region.wait();
        } catch (InterruptedException e) {
          logWriter.error("exception not expected", e);
          fail("Failed:" + e);
        }
      }
    }

    ((LocalRegion) region).getDiskRegion().flushForTesting();

    try {

      getValOnDsk = ((LocalRegion) region).getValueOnDisk("1");
    } catch (EntryNotFoundException e1) {
      logWriter.error("exception not expected", e1);
      fail("Failed:" + e1);
    }

    assertTrue(getValOnDsk.equals("2"));
    assertFalse(failureCause, testFailed);
    // region.close(); // closes disk file which will flush all buffers
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;

  }// end of testOplog1FlushOverridingOplog2Flush

  private class DoesFlush implements Runnable {

    private final Region region;

    public DoesFlush(Region region) {
      this.region = region;
    }

    @Override
    public void run() {
      ((LocalRegion) region).getDiskRegion().flushForTesting();
      synchronized (region) {
        region.notify();
        hasBeenNotified = true;
      }
    }
  }

  /**
   * OplogRoller should correctly add those entries created in an Oplog , but by the time rolling
   * has started , the entry exists in the current oplog
   */
  @Test
  public void testEntryExistsinCurrentOplog() {
    hasBeenNotified = false;
    diskProps.setDiskDirs(dirs);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setCompactionThreshold(100);
    diskProps.setRegionName("testEntryExistsinCurrentOplog");
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      boolean callOnce = false;

      @Override
      public void beforeGoingToCompact() {
        synchronized (this) {
          if (!callOnce) {
            try {
              for (int i = 0; i < 100; i++) {
                region.put(new Integer(i), "newVal" + i);
              }
            } catch (Exception e) {
              logWriter.error("exception not expected", e);
              failureCause = "FAILED::" + e;
              testFailed = true;
              fail("FAILED::" + e);
            }
          }
          callOnce = true;
        }

      }

      @Override
      public void afterHavingCompacted() {
        synchronized (region) {
          region.notify();
          hasBeenNotified = true;
        }
      }
    });

    try {
      for (int i = 0; i < 100; i++) {
        region.put(new Integer(i), new Integer(i));
      }
    } catch (Exception e) {
      logWriter.error("exception not expected", e);
      fail("FAILED::" + e);
    }

    for (int i = 0; i < 100; i++) {
      assertTrue(region.get(new Integer(i)).equals(new Integer(i)));
    }
    region.forceRolling();

    synchronized (region) {
      if (!hasBeenNotified) {
        try {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        } catch (InterruptedException e) {
          logWriter.error("exception not expected", e);
          fail("interrupted");
        }
      }
    }

    for (int i = 0; i < 100; i++) {
      assertEquals("newVal" + i, region.get(new Integer(i)));
    }

    region.close();

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

    for (int i = 0; i < 100; i++) {
      assertTrue(region.containsKey(new Integer(i)));
      assertEquals("newVal" + i, region.get(new Integer(i)));
    }
    assertFalse(failureCause, testFailed);
    // region.close();
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
  }// end of testEntryExistsinCurrentOplog

  /**
   * Entries deleted in current Oplog are recorded correctly during the rolling of that oplog
   */
  @Test
  public void testEntryDeletedinCurrentOplog() {
    hasBeenNotified = false;
    diskProps.setDiskDirs(dirs);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setCompactionThreshold(100);
    diskProps.setMaxOplogSize(100000);
    diskProps.setRegionName("testEntryDeletedinCurrentOplog1");
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      boolean callOnce = false;

      @Override
      public void beforeGoingToCompact() {
        synchronized (this) {

          if (!callOnce) {
            try {

              region.destroy(new Integer(10));
              region.destroy(new Integer(20));
              region.destroy(new Integer(30));
              region.destroy(new Integer(40));
              region.destroy(new Integer(50));

            } catch (Exception e) {
              logWriter.error("exception not expected", e);
              failureCause = "FAILED::" + e;
              testFailed = true;
              fail("FAILED::" + e);
            }
          }
          callOnce = true;
        }

      }

      @Override
      public void afterHavingCompacted() {
        synchronized (region) {
          region.notify();
          hasBeenNotified = true;
        }
      }

    });

    try {
      for (int i = 0; i < 100; i++) {
        region.put(new Integer(i), new Integer(i));
      }
    } catch (Exception e) {
      logWriter.error("exception not expected", e);
      fail("FAILED::" + e);
    }

    for (int i = 0; i < 100; i++) {
      assertTrue(region.get(new Integer(i)).equals(new Integer(i)));
    }
    region.forceRolling();

    synchronized (region) {
      if (!hasBeenNotified) {
        try {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        } catch (InterruptedException e) {
          logWriter.error("exception not expected", e);
          fail("interrupted");
        }
      }
    }

    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    Oplog oplog = dr.testHook_getChild();

    // Set set = oplog.getEntriesDeletedInThisOplog();

    // assertTrue(set.size() == 5);

    region.close();

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

    for (int i = 0; i < 100; i++) {
      if (i == 10 || i == 20 || i == 30 || i == 40 || i == 50) {
        assertTrue(" failed on key " + i, !region.containsKey(new Integer(i)));
      } else {
        assertTrue(region.get(new Integer(i)).equals(new Integer(i)));
      }
    }
    assertFalse(failureCause, testFailed);
    // region.close();
  }// end of testEntryDeletedinCurrentOplog

  private EvictionCounters getLRUStats(Region region) {
    return ((LocalRegion) region).getEvictionController().getCounters();
  }

  /**
   * to validate the get operation performed on a byte array.
   */
  private boolean getByteArrVal(Long key, Region region) {
    Object val = null;
    byte[] val2 = new byte[1024];
    Arrays.fill(val2, (byte) 77);
    try {
      // val = region.get(key);
      val = ((LocalRegion) region).getValueOnDisk(key);
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to get the value on disk");
    }
    // verify that the retrieved byte[] equals to the value put initially.
    boolean result = false;
    byte[] x = null;
    x = (byte[]) val;
    Arrays.fill(x, (byte) 77);
    for (int i = 0; i < x.length; i++) {
      result = (x[i] == val2[i]);
    }
    if (!result) {
      fail(
          "The val of byte[] put at 100th key obtained from disk is not euqal to the value put initially");
    }
    return result;
  }

  /**
   * DiskRegOplogRollerWaitingForAsyncWriterJUnitTest: Disk Region test : Oplog Roller should wait
   * for asynch writer to terminate if asynch flush is going on , before deleting the oplog
   */
  private boolean afterWritingBytes = false;

  @Test
  public void testOplogRollerWaitingForAsyncWriter() {
    hasBeenNotified = false;
    diskProps.setDiskDirs(dirs);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setBytesThreshold(100000000);
    diskProps.setTimeInterval(4000);
    diskProps.setRegionName("region_OplogRollerWaitingForAsyncWriter");
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      boolean callOnce = false;

      @Override
      public void afterWritingBytes() {

        synchronized (this) {

          if (!callOnce) {
            try {
              region.put("1", "2");
            } catch (Exception e) {
              logWriter.error("exception not expected", e);
              fail("FAILED::" + e);
              failureCause = "FAILED::" + e.toString();
              testFailed = true;
            }

            Thread th = new Thread(new DoesFlush1(region));
            th.setName("TestingThread");
            th.start();
            Thread.yield();

          }
          callOnce = true;
          afterWritingBytes = true;
        }

      }

      @Override
      public void afterHavingCompacted() {
        if (!afterWritingBytes) {
          fail("Roller didnt wait for Async writer to terminate!");
          failureCause = "Roller didnt wait for Async writer to terminate!";
          testFailed = true;
        }
      }

    });

    try {
      region.put("1", "1");
    } catch (Exception e) {
      logWriter.error("exception not expected", e);
      fail("FAILED::" + e);
    }

    region.forceRolling();

    synchronized (region) {
      if (!hasBeenNotified) {
        try {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        } catch (InterruptedException e) {
          logWriter.error("exception not expected", e);
          fail("interrupted");
        }
      }
    }

    ((LocalRegion) region).getDiskRegion().flushForTesting();

    try {
      assertTrue((((LocalRegion) region).getValueOnDisk("1")).equals("2"));
    } catch (EntryNotFoundException e1) {
      e1.printStackTrace();
      fail("Failed:" + e1);
    }
    assertFalse(failureCause, testFailed);
    // region.close(); // closes disk file which will flush all buffers

  }// end of testOplogRollerWaitingForAsyncWriter

  private class DoesFlush1 implements Runnable {

    private final Region region;

    public DoesFlush1(Region region) {
      this.region = region;
    }

    @Override
    public void run() {
      ((LocalRegion) region).getDiskRegion().flushForTesting();
      synchronized (region) {
        region.notify();
        hasBeenNotified = true;
      }
    }
  }

  /**
   * Task 125: Ensuring that retrieval of evicted entry data for rolling purposes is correct & does
   * not cause any eviction sort of things
   */
  @Test
  public void testGetEvictedEntry() throws EntryNotFoundException {
    hasBeenNotified = false;
    diskProps.setDiskDirs(dirs);
    diskProps.setPersistBackup(false);
    diskProps.setRolling(false);
    diskProps.setCompactionThreshold(100);
    diskProps.setAllowForceCompaction(true);
    diskProps.setMaxOplogSize(1000000000);
    diskProps.setOverflow(true);
    diskProps.setOverFlowCapacity(1);
    diskProps.setRegionName("region_testGetEvictedEntry");
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    DiskRegionStats stats = ((LocalRegion) region).getDiskRegion().getStats();
    // LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    // CacheObserverHolder.setInstance(new CacheObserverAdapter() {
    // boolean callOnce = false;

    // public void afterHavingCompacted()
    // {
    // synchronized (this) {

    // if (!callOnce) {
    // synchronized (region) {
    // region.notify();
    // hasBeenNotified = true;
    // }
    // }
    // }
    // callOnce = true;
    // afterWritingBytes = true;
    // }

    // });

    try {
      for (int i = 0; i < 100; i++) {
        region.put("key" + i, "val" + i);
      }
    } catch (Exception e) {
      logWriter.error("exception not expected", e);
      fail("FAILED::" + e);
    }

    // Check region stats
    assertTrue("before ForcRolling getNumKeys != 100", region.size() == 100);
    assertTrue("before ForcRolling getNumEntriesInVM != 1", stats.getNumEntriesInVM() == 1);
    assertTrue("before ForcRolling getNumOverflowOnDisk != 99", stats.getNumOverflowOnDisk() == 99);

    // region.forceRolling();
    // region.forceCompaction();

    // synchronized (region) {
    // if (!hasBeenNotified) {
    // try {
    // region.wait(10000);
    // assertTrue(hasBeenNotified);
    // }
    // catch (InterruptedException e) {
    // logWriter.error("exception not expected", e);
    // fail("interrupted");
    // }
    // }
    // }

    // // Check region stats
    // assertTrue("after ForcRolling getNumKeys != 100", region.size() == 100);
    // assertTrue("after ForcRolling getNumEntriesInVM != 1", stats
    // .getNumEntriesInVM() == 1);
    // assertTrue("after ForcRolling getNumOverflowOnDisk != 99", stats
    // .getNumOverflowOnDisk() == 99);

    for (int i = 0; i < 99; i++) {
      assertTrue(((LocalRegion) region).getValueOnDisk("key" + i).equals("val" + i));
    }

    ((LocalRegion) region).getDiskRegion().flushForTesting();

    // Values in VM for entries 0 to 98 must be null
    for (int i = 0; i < 99; i++) {
      assertTrue(((LocalRegion) region).getValueInVM("key" + i) == null);
    }
    assertTrue(((LocalRegion) region).getValueInVM("key99").equals("val99"));
    // Values on disk for entries 0 to 98 must have their values
    for (int i = 0; i < 99; i++) {
      assertTrue(((LocalRegion) region).getValueOnDisk("key" + i).equals("val" + i));
    }
    assertTrue(((LocalRegion) region).getValueOnDisk("key99") == null);

    // region.close(); // closes disk file which will flush all buffers
  }// end of testGetEvictedEntry

  /**
   * DiskRegNoDiskAccessExceptionTest: tests that while rolling is set to true DiskAccessException
   * doesn't occur even when amount of put data exceeds the max dir sizes.
   */
  @Test
  public void testDiskFullExcep() {
    boolean exceptionOccurred = false;
    int[] diskDirSize1 = new int[4];
    diskDirSize1[0] = 1048576;
    diskDirSize1[1] = 1048576;
    diskDirSize1[2] = 1048576;
    diskDirSize1[3] = 1048576;

    diskProps.setDiskDirsAndSizes(dirs, diskDirSize1);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setMaxOplogSize(1000000000);
    diskProps.setBytesThreshold(1000000);
    diskProps.setTimeInterval(1500000);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps);
    DiskStore ds = cache.findDiskStore(((LocalRegion) region).getDiskStoreName());
    // int[] diskSizes1 = ((LocalRegion)region).getDiskDirSizes();
    assertTrue(ds != null);
    int[] diskSizes1 = null;
    diskSizes1 = ds.getDiskDirSizes();

    assertTrue("diskSizes != 1048576 ", diskSizes1[0] == 1048576);
    assertTrue("diskSizes != 1048576 ", diskSizes1[1] == 1048576);
    assertTrue("diskSizes != 1048576 ", diskSizes1[2] == 1048576);
    assertTrue("diskSizes != 1048576 ", diskSizes1[3] == 1048576);

    final byte[] value = new byte[1024];
    Arrays.fill(value, (byte) 77);
    try {
      for (int i = 0; i < 7000; i++) {
        region.put("" + i, value);
      }
    } catch (DiskAccessException e) {
      logWriter.error("exception not expected", e);
      exceptionOccurred = true;
    }
    if (exceptionOccurred) {
      fail("FAILED::DiskAccessException is Not expected here !!");
    }

    // region.close(); // closes disk file which will flush all buffers

  }// end of testDiskFullExcep

}// end of test class
