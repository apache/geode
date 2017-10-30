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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.awaitility.Awaitility;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.Oplog.OPLOG_TYPE;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Testing Oplog API's
 */
@Category(IntegrationTest.class)
public class OplogJUnitTest extends DiskRegionTestingBase {

  private boolean proceed = false;

  private final DiskRegionProperties diskProps = new DiskRegionProperties();

  private long delta;

  volatile private boolean assertDone = false;

  private boolean failure = false;

  @Override
  protected final void postSetUp() throws Exception {
    diskProps.setDiskDirs(dirs);
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
  }

  @Override
  protected final void postTearDown() throws Exception {
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;
  }

  /**
   * Test method for 'org.apache.geode.internal.cache.Oplog.isBackup()'
   */
  @Test
  public void testIsBackup() {

    InternalRegion overFlowAndPersistRegionRegion =
        (InternalRegion) DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    assertTrue("Not correctly setup for overflow and persist",
        overFlowAndPersistRegionRegion.getDiskRegion().isBackup());
    closeDown(overFlowAndPersistRegionRegion);

    InternalRegion overFlowOnlyRegion =
        (InternalRegion) DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    assertFalse("Not correctly setup for overflow only mode",
        overFlowOnlyRegion.getDiskRegion().isBackup());
    closeDown(overFlowOnlyRegion);

    InternalRegion persistOnlyRegion = (InternalRegion) DiskRegionHelperFactory
        .getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    assertTrue("Not correctly setup for  persist only mode",
        persistOnlyRegion.getDiskRegion().isBackup());
    closeDown(persistOnlyRegion);
  }

  /*
   * Test method for 'org.apache.geode.internal.cache.Oplog.useSyncWrites()'
   */
  @Test
  public void testUseSyncWritesWhenSet() {
    diskProps.setSynchronous(true);
    InternalRegion syncOverFlowAndPersistRegion =
        (InternalRegion) DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    assertTrue(syncOverFlowAndPersistRegion.getAttributes().isDiskSynchronous());
    closeDown(syncOverFlowAndPersistRegion);

    InternalRegion syncOverFlowOnlyRegion =
        (InternalRegion) DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    assertTrue(syncOverFlowOnlyRegion.getAttributes().isDiskSynchronous());
    closeDown(syncOverFlowOnlyRegion);

    InternalRegion syncPersistOnlyRegion = (InternalRegion) DiskRegionHelperFactory
        .getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    assertTrue(syncPersistOnlyRegion.getAttributes().isDiskSynchronous());
    closeDown(syncPersistOnlyRegion);
  }

  @Test
  public void testNotUseSyncWritesWhenNotSet() {
    diskProps.setSynchronous(false);
    InternalRegion asyncOverFlowAndPersistRegion =
        (InternalRegion) DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache, diskProps);
    assertFalse(asyncOverFlowAndPersistRegion.getAttributes().isDiskSynchronous());
    closeDown(asyncOverFlowAndPersistRegion);

    InternalRegion asyncOverFlowOnlyRegion =
        (InternalRegion) DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache, diskProps);
    assertFalse(asyncOverFlowOnlyRegion.getAttributes().isDiskSynchronous());
    closeDown(asyncOverFlowOnlyRegion);

    InternalRegion asyncPersistOnlyRegion =
        (InternalRegion) DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps);
    assertFalse(asyncPersistOnlyRegion.getAttributes().isDiskSynchronous());
    closeDown(asyncPersistOnlyRegion);
  }

  /**
   * Test method for 'org.apache.geode.internal.cache.Oplog.clear(File)'
   */
  @Test
  public void testClear() {
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    putTillOverFlow(region);
    region.clear();
    region.close();
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    assertTrue(" failed in get OverflowAndPersist ", region.get(0) == null);
    closeDown();

    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    putTillOverFlow(region);
    region.clear();
    region.close();
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    assertTrue(" failed in get OverflowOnly ", region.get(0) == null);
    closeDown();

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    put100Int();
    region.clear();
    region.close();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    assertTrue(" failed in get PersistOnly ", region.get(0) == null);
    closeDown();
  }

  /**
   * Test method for 'org.apache.geode.internal.cache.Oplog.close()'
   */
  @Test
  public void testClose() {
    {
      deleteFiles();
      region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
      DiskRegion dr = ((LocalRegion) region).getDiskRegion();
      Oplog oplog = dr.testHook_getChild();
      long id = oplog.getOplogId();
      oplog.close();

      StatisticsFactory factory = cache.getDistributedSystem();
      Oplog newOplog =
          new Oplog(id, dr.getOplogSet(), new DirectoryHolder(factory, dirs[0], 1000, 0));
      dr.getOplogSet().setChild(newOplog);
      closeDown();
    }
    {
      deleteFiles();
      region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
      DiskRegion dr = ((LocalRegion) region).getDiskRegion();
      dr.testHookCloseAllOverflowOplogs();
      checkIfContainsFile("OVERFLOW");
      closeDown();
    }
    {
      deleteFiles();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      DiskRegion dr = ((LocalRegion) region).getDiskRegion();
      Oplog oplog = dr.testHook_getChild();
      long id = oplog.getOplogId();
      oplog.close();
      StatisticsFactory factory = cache.getDistributedSystem();
      Oplog newOplog =
          new Oplog(id, dr.getOplogSet(), new DirectoryHolder(factory, dirs[0], 1000, 2));
      dr.setChild(newOplog);
      closeDown();
    }

  }

  private void closeDown(InternalRegion region) {
    super.closeDown(region);
    DiskRegion diskRegion = region != null ? region.getDiskRegion() : null;
    if (diskRegion != null) {
      diskRegion.getDiskStore().close();
      ((InternalCache) cache).removeDiskStore(diskRegion.getDiskStore());
    }
  }

  @Override
  protected void closeDown() {
    DiskRegion dr = null;
    if (region != null) {
      dr = ((LocalRegion) region).getDiskRegion();
    }
    super.closeDown();
    if (dr != null) {
      dr.getDiskStore().close();
      ((LocalRegion) region).getGemFireCache().removeDiskStore(dr.getDiskStore());
    }
  }

  private void checkIfContainsFile(String fileExtension) {
    for (File dir : dirs) {
      File[] files = dir.listFiles();
      for (File file : files) {
        if (file.getAbsolutePath().endsWith(fileExtension)) {
          fail("file " + file + " still exists after oplog.close()");
        }
      }
    }
  }

  /**
   * Test method for 'org.apache.geode.internal.cache.Oplog.destroy()'
   */
  @Test
  public void testDestroy() {
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    put100Int();
    putTillOverFlow(region);
    try {
      region.destroy(0);
    } catch (EntryNotFoundException e1) {
      logWriter.error("Exception occurred", e1);
      fail(" Entry not found when it was expected to be there");
    }
    region.close();
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    assertTrue(" failed in get OverflowAndPersist ", region.get(0) == null);
    closeDown();

    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    put100Int();
    putTillOverFlow(region);
    try {
      region.destroy(0);
    } catch (EntryNotFoundException e1) {
      logWriter.error("Exception occurred", e1);
      fail(" Entry not found when it was expected to be there");
    }
    region.close();
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    assertTrue(" failed in get OverflowOnly ", region.get(0) == null);

    closeDown();

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    put100Int();
    try {
      region.destroy(0);
    } catch (EntryNotFoundException e1) {
      logWriter.error("Exception occurred", e1);
      fail(" Entry not found when it was expected to be there");
    }
    region.close();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    assertTrue(" failed in get PersistOnly ", region.get(0) == null);
    closeDown();

  }

  /**
   * Test method for 'org.apache.geode.internal.cache.Oplog.remove(long)'
   */
  @Test
  public void testRemove() {
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    putTillOverFlow(region);
    region.remove(0);
    region.close();
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    assertTrue(" failed in get OverflowAndPersist ", region.get(0) == null);
    closeDown();

    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    putTillOverFlow(region);
    region.remove(0);
    assertTrue(" failed in get OverflowOnly ", region.get(0) == null);
    region.close();
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    closeDown();

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    put100Int();
    region.remove(0);
    assertTrue(" failed in get PersistOnly ", region.get(0) == null);
    region.close();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    closeDown();

  }

  /**
   * Tests whether the data is written in the right format on the disk
   *
   */
  @Test
  public void testFaultInOfValuesFromDisk() {
    try {
      // Asif First create a persist only disk region which is of aysnch
      // & switch of OplOg type
      diskProps.setMaxOplogSize(1000);

      diskProps.setPersistBackup(true);
      diskProps.setRolling(false);
      diskProps.setSynchronous(true);
      diskProps.setTimeInterval(-1);
      diskProps.setOverflow(false);

      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      byte[] val = new byte[10];
      for (int i = 0; i < val.length; ++i) {
        val[i] = (byte) i;
      }
      region.put(1, val);

      DiskEntry entry = ((DiskEntry) ((LocalRegion) region).basicGetEntry(1));
      DiskRegion dr = ((LocalRegion) region).getDiskRegion();

      val = (byte[]) dr.getNoBuffer(entry.getDiskId());
      for (int i = 0; i < val.length; ++i) {
        if (val[i] != (byte) i) {
          fail("Test for fault in from disk failed");
        }
      }
      val = (byte[]) DiskStoreImpl.convertBytesAndBitsIntoObject(
          dr.getBytesAndBitsWithoutLock(entry.getDiskId(), true, false));
      for (int i = 0; i < val.length; ++i) {
        if (val[i] != (byte) i) {
          fail("Test for fault in from disk failed");
        }
      }
      region.invalidate(1);
      assertTrue(dr.getNoBuffer(entry.getDiskId()) == Token.INVALID);

    } catch (Exception e) {
      logWriter.error("Exception occurred", e);
      fail(e.toString());
    }
    closeDown();
  }

  /**
   * Tests the original ByteBufferPool gets transferred to the new Oplog for synch mode
   *
   */
  @Test
  public void testByteBufferPoolTransferForSynchMode() {
    diskProps.setMaxOplogSize(1024);
    diskProps.setBytesThreshold(0);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setSynchronous(true);
    diskProps.setTimeInterval(10000);
    diskProps.setOverflow(false);

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    // Populate data just below the switch over threshhold
    byte[] val = new byte[5];
    for (int i = 0; i < val.length; ++i) {
      val[i] = (byte) i;
    }

    region.put(1, val);

    ((LocalRegion) region).basicGetEntry(1);
    Oplog old = dr.testHook_getChild();
    ByteBuffer oldWriteBuf = old.getWriteBuf();
    dr.forceRolling();
    region.put(2, val);
    Oplog switched = dr.testHook_getChild();
    assertTrue(old != switched);
    assertEquals(dr.getDiskStore().persistentOplogs.getChild(2), switched);
    assertEquals(oldWriteBuf, switched.getWriteBuf());
    assertEquals(null, old.getWriteBuf());
    closeDown();

  }

  /**
   * Tests the bug which arises in case of asynch mode during oplog switching caused by conflation
   * of create/destroy operation.The bug occurs if a create operation is followed by destroy but
   * before destroy proceeds some other operation causes oplog switching
   *
   */
  @Test
  public void testBug34615() {
    final int MAX_OPLOG_SIZE = 100;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setSynchronous(false);
    diskProps.setOverflow(false);
    diskProps.setBytesThreshold(150);

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    final byte[] val = new byte[50];
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps);
    final CacheObserver old = CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      @Override
      public void afterConflation(ByteBuffer orig, ByteBuffer conflated) {
        Thread th = new Thread(() -> region.put("2", new byte[75]));
        assertNull(conflated);
        th.start();
        ThreadUtils.join(th, 30 * 1000);
        LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;

      }
    });

    region.put("1", val);
    region.remove("1");
    assertFalse(failureCause, testFailed);
    CacheObserverHolder.setInstance(old);
    closeDown();

  }

  /**
   */
  @Test
  public void testConflation() throws Exception {

    final int MAX_OPLOG_SIZE = 1000;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setSynchronous(false);
    diskProps.setOverflow(false);
    diskProps.setBytesThreshold(1500);

    final byte[] val = new byte[50];
    final byte[][] bb = new byte[2][];
    bb[0] = new byte[5];
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps);
    try {
      region.put("1", val);
      region.put("1", new byte[10]);
      region.put("2", val);
      region.put("2", new byte[100]);
      region.create("3", null);
      region.put("3", new byte[10]);
      region.create("4", null);
      region.put("4", new byte[0]);

      // tests for byte[][]
      region.create("5", bb);
      region.put("6", val);
      region.put("6", bb);
      region.create("7", null);
      region.put("7", bb);

      region.create("8", new byte[9]);
      region.invalidate("8");
      region.create("9", new byte[0]);
      region.invalidate("9");

      region.create("10", new byte[9]);
      region.localInvalidate("10");
      region.create("11", new byte[0]);
      region.localInvalidate("11");

      DiskRegion dr = ((LocalRegion) region).getDiskRegion();
      dr.flushForTesting();
      byte[] val_1 = ((byte[]) ((LocalRegion) region).getValueOnDisk("1"));
      assertEquals(val_1.length, 10);
      byte[] val_2 = ((byte[]) ((LocalRegion) region).getValueOnDisk("2"));
      assertEquals(val_2.length, 100);
      byte[] val_3 = ((byte[]) ((LocalRegion) region).getValueOnDisk("3"));
      assertEquals(val_3.length, 10);
      byte[] val_4 = ((byte[]) ((LocalRegion) region).getValueOnDisk("4"));
      assertEquals(val_4.length, 0);
      byte[][] val_5 = (byte[][]) ((LocalRegion) region).getValueOnDisk("5");
      assertEquals(val_5.length, 2);
      assertEquals(val_5[0].length, 5);
      assertNull(val_5[1]);
      byte[][] val_6 = (byte[][]) ((LocalRegion) region).getValueOnDisk("6");
      assertEquals(val_6.length, 2);
      assertEquals(val_6[0].length, 5);
      assertNull(val_6[1]);
      byte[][] val_7 = (byte[][]) ((LocalRegion) region).getValueOnDisk("7");
      assertEquals(val_7.length, 2);
      assertEquals(val_7[0].length, 5);
      assertNull(val_7[1]);
      Object val_8 = ((LocalRegion) region).getValueOnDisk("8");
      assertEquals(val_8, Token.INVALID);
      Object val_9 = ((LocalRegion) region).getValueOnDisk("9");
      assertEquals(val_9, Token.INVALID);
      Object val_10 = ((LocalRegion) region).getValueOnDisk("10");
      assertEquals(val_10, Token.LOCAL_INVALID);
      Object val_11 = ((LocalRegion) region).getValueOnDisk("11");
      assertEquals(val_11, Token.LOCAL_INVALID);

    } finally {
      closeDown();
    }
  }

  /**
   * This tests the retrieval of empty byte array when present in asynch buffers
   *
   */
  @Test
  public void testGetEmptyByteArrayInAsynchBuffer() {
    final int MAX_OPLOG_SIZE = 1000;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setSynchronous(false);
    diskProps.setOverflow(false);
    diskProps.setBytesThreshold(1500);

    final byte[] val = new byte[50];
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps);
    try {
      region.put("1", val);
      region.put("1", new byte[0]);
      byte[] val_1 = ((byte[]) ((LocalRegion) region).getValueOnDiskOrBuffer("1"));
      assertEquals(val_1.length, 0);
    } catch (Exception e) {
      logWriter.error("Exception occurred", e);
      fail("The test failed due to exception = " + e);
    }
    closeDown();
  }

  /**
   * This tests the retrieval of empty byte array in synch mode
   *
   */
  @Test
  public void testGetEmptyByteArrayInSynchMode() {
    final int MAX_OPLOG_SIZE = 1000;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(false);
    diskProps.setBytesThreshold(1500);

    final byte[] val = new byte[50];
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    try {
      region.put("1", val);
      region.put("1", new byte[0]);
      byte[] val_1 = ((byte[]) ((LocalRegion) region).getValueOnDiskOrBuffer("1"));
      assertEquals(val_1.length, 0);
    } catch (Exception e) {
      logWriter.error("Exception occurred", e);
      fail("The test failed due to exception = " + e);
    }
    closeDown();
  }

  /**
   * This tests the bug which caused the oplogRoller to attempt to roll a removed entry whose value
   * is Token.Removed This bug can occur if a remove operation causes oplog switching & hence roller
   * thread gets notified, & the roller thread obtains the iterator of the concurrent region map
   * before the remove
   *
   */
  @Test
  public void testBug34702() {
    final int MAX_OPLOG_SIZE = 500 * 2;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(false);
    final byte[] val = new byte[200];
    proceed = false;

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    region.put("key1", val);
    region.put("key2", val);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    final CacheObserver old = CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      @Override
      public void afterSettingOplogOffSet(long offset) {
        ((LocalRegion) region).getDiskRegion().forceRolling();
        // Let the operation thread yield to the Roller so that
        // it is able to obtain the iterator of the concurrrent region map
        // & thus get the reference to the entry which will contain
        // value as Token.Removed as the entry though removed from
        // concurrent
        // map still will be available to the roller
        Thread.yield();
        // Sleep for some time
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          testFailed = true;
          failureCause = "No guarantee that test is succesful";
          fail("No guarantee that test is succesful");
        }
      }

      @Override
      public void afterHavingCompacted() {
        proceed = true;
        synchronized (OplogJUnitTest.this) {
          OplogJUnitTest.this.notify();
        }
      }
    });
    try {
      region.destroy("key1");
      region.destroy("key2");
    } catch (Exception e1) {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(old);
      fail("Test failed as entry deletion threw exception. Exception = " + e1);
    }
    // Wait for some time & check if the after having rolled callabck
    // is issued sucessfully or not.
    if (!proceed) {
      synchronized (this) {
        if (!proceed) {
          try {
            this.wait(20000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // The test will automatically fail due to proceed flag
          }
        }
      }
    }
    assertFalse(failureCause, testFailed);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    CacheObserverHolder.setInstance(old);

    if (!proceed) {
      fail("Test failed as afterHavingCompacted callabck not issued even after sufficient wait");
    }
    closeDown();

  }

  /**
   * tests a potential deadlock situation if the operation causing a swithcing of Oplog is waiting
   * for roller to free space. The problem can arise if the operation causing Oplog switching is
   * going on an Entry , which already has its oplog ID referring to the Oplog being switched. In
   * such case, when the roller will try to roll the entries referencing the current oplog , it will
   * not be able to acquire the lock on the entry as the switching thread has already taken a lock
   * on it.
   *
   */
  @Test
  public void testRollingDeadlockSituation() {
    final int MAX_OPLOG_SIZE = 2000;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(false);
    diskProps.setDiskDirsAndSizes(new File[] {dirs[0]}, new int[] {1400});
    final byte[] val = new byte[500];
    proceed = false;
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    region.put("key1", val);
    region.put("key1", val);
    try {
      region.put("key1", val);
    } catch (DiskAccessException dae) {
      logWriter.error("Exception occurred", dae);
      fail(
          "Test failed as DiskAccessException was encountered where as the operation should ideally have proceeded without issue . exception = "
              + dae);
    }
  }

  /**
   * This tests whether an empty byte array is correctly writtem to the disk as a zero value length
   * operation & hence the 4 bytes field for recording the value length is absent & also since the
   * value length is zero no byte for it should also get added. Similary during recover from HTree
   * as well as Oplog , the empty byte array should be read correctly
   *
   */
  @Test
  public void testEmptyByteArrayPutAndRecovery() {
    CacheObserver old = CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      @Override
      public void afterConflation(ByteBuffer origBB, ByteBuffer conflatedBB) {
        if ((2 + 4 + 1 + EntryEventImpl.serialize("key1").length) != origBB.capacity()) {
          failureCause =
              "For a backup region, addition of an empty array should result in an offset of 6 bytes where as actual offset is ="
                  + origBB.capacity();
          testFailed = true;
        }
        Assert.assertTrue(
            "For a backup region, addition of an empty array should result in an offset of 6 bytes where as actual offset is ="
                + origBB.capacity(),
            (2 + 4 + 1 + EntryEventImpl.serialize("key1").length) == origBB.capacity());

      }
    });
    try {
      final int MAX_OPLOG_SIZE = 2000;
      diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
      diskProps.setPersistBackup(true);
      diskProps.setSynchronous(true);
      diskProps.setOverflow(false);
      diskProps.setDiskDirsAndSizes(new File[] {dirs[0]}, new int[] {1400});
      final byte[] val = new byte[0];
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      region.put("key1", val);
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      byte[] _val = (byte[]) region.get("key1");
      assertTrue(
          "value of key1 after restarting the region is not an empty byte array. This may indicate problem in reading from Oplog",
          _val.length == 0);
      if (this.logWriter.infoEnabled()) {
        this.logWriter.info(
            "After first region close & opening again no problems encountered & hence Oplog has been read successfully.");
        this.logWriter.info(
            "Closing the region again without any operation done, would indicate that next time data will be loaded from HTree .");
      }
      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      _val = (byte[]) region.get("key1");
      assertTrue(
          "value of key1 after restarting the region is not an empty byte array. This may indicate problem in reading from HTRee",
          _val.length == 0);
      assertFalse(failureCause, testFailed);
      // region.close();

    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(old);

    }
  }

  /**
   * This is used to test bug 35012 where a remove operation on a key gets unrecorded due to
   * switching of Oplog if it happens just after the remove operation has destroyed the in memory
   * entry & is about to acquire the readlock in DiskRegion to record the same. If the Oplog has
   * switched during that duration , the bug would appear
   *
   */

  @Test
  public void testBug35012() {
    final int MAX_OPLOG_SIZE = 500;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(false);
    final byte[] val = new byte[200];
    try {

      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

      region.put("key1", val);
      region.put("key2", val);
      region.put("key3", val);
      final Thread th = new Thread(() -> region.remove("key1"));
      // main thread acquires the write lock
      ((LocalRegion) region).getDiskRegion().acquireWriteLock();
      try {
        th.start();
        Thread.yield();
        DiskRegion dr = ((LocalRegion) region).getDiskRegion();
        dr.testHook_getChild().forceRolling(dr);
      } finally {
        ((LocalRegion) region).getDiskRegion().releaseWriteLock();
      }
      ThreadUtils.join(th, 30 * 1000);
      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      assertEquals(region.size(), 2);
    } catch (Exception e) {
      this.logWriter.error("Exception occurred ", e);
      fail("The test could not be completed because of exception .Exception=" + e);
    }
    closeDown();

  }

  /**
   * Tests if buffer size & time are not set , the asynch writer gets awakened on time basis of
   * default 1 second
   *
   */
  @Test
  public void testAsynchWriterAttribBehaviour1() throws Exception {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    ((DiskStoreFactoryImpl) dsf).setMaxOplogSizeInBytes(10000);
    File dir = new File("testingDirectoryDefault");
    dir.mkdir();
    dir.deleteOnExit();
    File[] dirs = {dir};
    dsf.setDiskDirs(dirs);
    RegionFactory<Object, Object> factory =
        cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
    final long t1 = System.currentTimeMillis();
    DiskStore ds = dsf.create("test");
    factory.setDiskSynchronous(false);
    factory.setDiskStoreName(ds.getName());
    factory.setScope(Scope.LOCAL);
    region = factory.create("test");

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    CacheObserver old = CacheObserverHolder.setInstance(

        new CacheObserverAdapter() {
          private long t2;

          @Override
          public void goingToFlush() {
            t2 = System.currentTimeMillis();
            delta = t2 - t1;
            LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
            synchronized (OplogJUnitTest.this) {
              OplogJUnitTest.this.notify();
            }
          }
        });

    region.put("key1", "111111111111");
    synchronized (this) {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        this.wait(10000);
        assertFalse(LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER);
      }
    }
    CacheObserverHolder.setInstance(old);
    // Windows clock has an accuracy of 15 ms. Accounting for the same.
    assertTrue("delta is in miilliseconds=" + delta, delta >= 985);
    closeDown();
  }

  /**
   * Tests if buffer size is set but time is not set , the asynch writer gets awakened on buffer
   * size basis
   */
  @Ignore("TODO:DARREL_DISABLE: test is disabled")
  @Test
  public void testAsynchWriterAttribBehaviour2() throws Exception {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    ((DiskStoreFactoryImpl) dsf).setMaxOplogSizeInBytes(10000);
    dsf.setQueueSize(2);
    File dir = new File("testingDirectoryDefault");
    dir.mkdir();
    dir.deleteOnExit();
    File[] dirs = {dir};
    dsf.setDiskDirs(dirs);
    RegionFactory<Object, Object> factory =
        cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
    DiskStore ds = dsf.create("test");
    factory.setDiskSynchronous(false);
    factory.setDiskStoreName(ds.getName());
    factory.setScope(Scope.LOCAL);
    region = factory.create("test");

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    CacheObserver old = CacheObserverHolder.setInstance(

        new CacheObserverAdapter() {

          @Override
          public void goingToFlush() {
            synchronized (OplogJUnitTest.this) {
              LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
              OplogJUnitTest.this.notify();
            }
          }
        });

    region.put("key1", new byte[25]);
    Thread.sleep(1000);
    assertTrue(LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER);
    region.put("key2", new byte[25]);
    synchronized (this) {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        OplogJUnitTest.this.wait(10000);
        assertFalse(LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER);
      }
    }
    CacheObserverHolder.setInstance(old);
    closeDown();
  }

  /**
   * Tests if buffer size & time interval are explicitly set to zero then the flush will occur due
   * to asynchForceFlush or due to switching of Oplog
   *
   */
  @Test
  public void testAsynchWriterAttribBehaviour3() throws Exception {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    ((DiskStoreFactoryImpl) dsf).setMaxOplogSizeInBytes(500);
    dsf.setQueueSize(0);
    dsf.setTimeInterval(0);
    File dir = new File("testingDirectoryDefault");
    dir.mkdir();
    dir.deleteOnExit();
    File[] dirs = {dir};
    dsf.setDiskDirs(dirs);
    RegionFactory<Object, Object> factory =
        cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
    DiskStore ds = dsf.create("test");
    factory.setDiskSynchronous(false);
    factory.setDiskStoreName(ds.getName());
    factory.setScope(Scope.LOCAL);
    region = factory.create("test");

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    CacheObserver old = CacheObserverHolder.setInstance(

        new CacheObserverAdapter() {

          @Override
          public void goingToFlush() {
            LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
            synchronized (OplogJUnitTest.this) {
              OplogJUnitTest.this.notify();
            }
          }
        });
    region.put("key1", new byte[100]);
    region.put("key2", new byte[100]);
    region.put("key3", new byte[100]);
    region.put("key4", new byte[100]);
    region.put("key5", new byte[100]);
    Thread.sleep(1000);
    assertTrue(LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER);


    ((LocalRegion) region).getDiskRegion().forceRolling();
    synchronized (this) {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        OplogJUnitTest.this.wait(10000);
      }
    }
    assertFalse(LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER);
    CacheObserverHolder.setInstance(old);
    closeDown();
  }

  /**
   * Tests if the byte buffer pool in asynch mode tries to contain the pool size
   *
   */
  @Test
  public void testByteBufferPoolContainment() throws Exception {

    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setMaxOplogSize(1024 * 1024);
    diskProps.setSynchronous(false);
    diskProps.setOverflow(false);
    diskProps.setBytesThreshold(10); // this is now item count
    diskProps.setTimeInterval(0);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps);
    final byte[] val = new byte[1000];
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      @Override
      public void goingToFlush() { // Delay flushing

        assertEquals(10, region.size());
        for (int i = 10; i < 20; ++i) {
          region.put("" + i, val);
        }
        synchronized (OplogJUnitTest.this) {
          OplogJUnitTest.this.notify();
          LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
        }

      }
    });
    for (int i = 0; i < 10; ++i) {
      region.put("" + i, val);
    }
    synchronized (OplogJUnitTest.this) {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        OplogJUnitTest.this.wait(9000);
        assertEquals(false, LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER);
      }
    }
  }

  /**
   * tests async stats are correctly updated
   */
  @Test
  public void testAsyncStats() throws InterruptedException {
    diskProps.setBytesThreshold(101);
    diskProps.setTimeInterval(1000000);
    region = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache, diskProps);
    final DiskStoreStats dss = ((LocalRegion) region).getDiskRegion().getDiskStore().getStats();

    assertEquals(0, dss.getQueueSize());
    put100Int();
    Awaitility.await().pollInterval(10, TimeUnit.MILLISECONDS).pollDelay(10, TimeUnit.MILLISECONDS)
        .timeout(10, TimeUnit.SECONDS).until(() -> assertEquals(100, dss.getQueueSize()));

    assertEquals(0, dss.getFlushes());

    DiskRegion diskRegion = ((LocalRegion) region).getDiskRegion();
    diskRegion.getDiskStore().flush();
    Awaitility.await().pollInterval(10, TimeUnit.MILLISECONDS).pollDelay(10, TimeUnit.MILLISECONDS)
        .timeout(10, TimeUnit.SECONDS).until(() -> assertEquals(0, dss.getQueueSize()));
    Awaitility.await().pollInterval(10, TimeUnit.MILLISECONDS).pollDelay(10, TimeUnit.MILLISECONDS)
        .timeout(10, TimeUnit.SECONDS).until(() -> assertEquals(100, dss.getFlushes()));
    put100Int();
    Awaitility.await().pollInterval(10, TimeUnit.MILLISECONDS).pollDelay(10, TimeUnit.MILLISECONDS)
        .timeout(10, TimeUnit.SECONDS).until(() -> assertEquals(100, dss.getQueueSize()));
    diskRegion.getDiskStore().flush();
    Awaitility.await().pollInterval(10, TimeUnit.MILLISECONDS).pollDelay(10, TimeUnit.MILLISECONDS)
        .timeout(10, TimeUnit.SECONDS).until(() -> assertEquals(0, dss.getQueueSize()));
    Awaitility.await().pollInterval(10, TimeUnit.MILLISECONDS).pollDelay(10, TimeUnit.MILLISECONDS)
        .timeout(10, TimeUnit.SECONDS).until(() -> assertEquals(200, dss.getFlushes()));
    closeDown();
  }

  /**
   * Tests delayed creation of DiskID in overflow only mode
   *
   */
  @Test
  public void testDelayedDiskIdCreationInOverflowOnlyMode() {
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setMaxOplogSize(1024 * 1024);
    diskProps.setSynchronous(false);
    diskProps.setOverflow(true);
    diskProps.setBytesThreshold(10000);
    diskProps.setTimeInterval(0);
    diskProps.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache, diskProps);
    final byte[] val = new byte[1000];
    region.put("1", val);
    DiskEntry entry = (DiskEntry) ((LocalRegion) region).basicGetEntry("1");
    assertTrue(entry instanceof AbstractDiskLRURegionEntry);
    assertNull(entry.getDiskId());
    region.put("2", val);
    assertNotNull(entry.getDiskId());
    entry = (DiskEntry) ((LocalRegion) region).basicGetEntry("2");
    assertTrue(entry instanceof AbstractDiskLRURegionEntry);
    assertNull(entry.getDiskId());
  }

  /**
   * Tests immediate creation of DiskID in overflow With Persistence mode
   *
   */
  @Test
  public void testImmediateDiskIdCreationInOverflowWithPersistMode() {
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setMaxOplogSize(1024 * 1024);
    diskProps.setSynchronous(false);
    diskProps.setOverflow(true);
    diskProps.setBytesThreshold(10000);
    diskProps.setTimeInterval(0);
    diskProps.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache, diskProps);
    final byte[] val = new byte[1000];
    region.put("1", val);
    DiskEntry entry = (DiskEntry) ((LocalRegion) region).basicGetEntry("1");
    assertTrue(entry instanceof AbstractDiskLRURegionEntry);
    assertNotNull(entry.getDiskId());
    region.put("2", val);
    assertNotNull(entry.getDiskId());
    entry = (DiskEntry) ((LocalRegion) region).basicGetEntry("2");
    assertTrue(entry instanceof AbstractDiskLRURegionEntry);
    assertNotNull(entry.getDiskId());
  }

  /**
   * An entry which is evicted to disk will have the flag already written to disk, appropriately set
   *
   */
  @Test
  public void testEntryAlreadyWrittenIsCorrectlyUnmarkedForOverflowOnly() throws Exception {
    diskProps.setPersistBackup(false);
    diskProps.setRolling(false);
    diskProps.setMaxOplogSize(1024 * 1024);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(true);
    diskProps.setBytesThreshold(10000);
    diskProps.setTimeInterval(0);
    diskProps.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    final byte[] val = new byte[1000];
    region.put("1", val);
    region.put("2", val);
    // "1" should now be on disk
    region.get("1");
    // "2" should now be on disk
    DiskEntry entry1 = (DiskEntry) ((LocalRegion) region).basicGetEntry("1");
    DiskId did1 = entry1.getDiskId();
    DiskId.isInstanceofOverflowIntOplogOffsetDiskId(did1);
    assertTrue(!did1.needsToBeWritten());
    region.put("1", "3");
    assertTrue(did1.needsToBeWritten());
    region.put("2", val);
    DiskEntry entry2 = (DiskEntry) ((LocalRegion) region).basicGetEntry("2");
    DiskId did2 = entry2.getDiskId();
    assertTrue(!did2.needsToBeWritten() || !did1.needsToBeWritten());
    tearDown();
    setUp();
    diskProps.setPersistBackup(false);
    diskProps.setRolling(false);
    long opsize = Integer.MAX_VALUE;
    opsize += 100L;
    diskProps.setMaxOplogSize(opsize);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(true);
    diskProps.setBytesThreshold(10000);
    diskProps.setTimeInterval(0);
    diskProps.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    region.put("1", val);
    region.put("2", val);
    region.get("1");
    entry1 = (DiskEntry) ((LocalRegion) region).basicGetEntry("1");
    did1 = entry1.getDiskId();
    DiskId.isInstanceofOverflowOnlyWithLongOffset(did1);
    assertTrue(!did1.needsToBeWritten());
    region.put("1", "3");
    assertTrue(did1.needsToBeWritten());
    region.put("2", "3");
    did2 = entry2.getDiskId();
    assertTrue(!did2.needsToBeWritten() || !did1.needsToBeWritten());
  }


  /**
   * An persistent or overflow with persistence entry which is evicted to disk, will have the flag
   * already written to disk, appropriately set
   *
   */
  @Test
  public void testEntryAlreadyWrittenIsCorrectlyUnmarkedForOverflowWithPersistence() {
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setMaxOplogSize(1024 * 1024);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(true);
    diskProps.setBytesThreshold(10000);
    diskProps.setTimeInterval(0);
    diskProps.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    final byte[] val = new byte[1000];
    region.put("1", val);
    DiskEntry entry1 = (DiskEntry) ((LocalRegion) region).basicGetEntry("1");
    DiskId did1 = entry1.getDiskId();
    DiskId.isInstanceofPersistIntOplogOffsetDiskId(did1);
    assertTrue(!did1.needsToBeWritten());
    region.put("2", val);
    assertTrue(!did1.needsToBeWritten());
  }

  /**
   * Tests the various DiskEntry.Helper APIs for correctness as there is now delayed creation of
   * DiskId and accessing OplogkeyId will throw UnsupportedException
   */
  @Test
  public void testHelperAPIsForOverflowOnlyRegion() {
    diskProps.setPersistBackup(false);
    diskProps.setRolling(false);
    diskProps.setMaxOplogSize(1024 * 1024);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(true);
    diskProps.setBytesThreshold(10000);
    diskProps.setTimeInterval(0);
    diskProps.setOverFlowCapacity(2);
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    final byte[] val = new byte[1000];
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    region.put("1", val);
    region.put("2", val);
    region.put("3", val);
    DiskEntry entry1 = (DiskEntry) ((LocalRegion) region).basicGetEntry("1");
    DiskEntry entry2 = (DiskEntry) ((LocalRegion) region).basicGetEntry("2");
    DiskEntry entry3 = (DiskEntry) ((LocalRegion) region).basicGetEntry("3");
    assertNull(entry2.getDiskId());
    assertNull(entry3.getDiskId());
    assertNotNull(entry1.getDiskId());
    assertNull(DiskEntry.Helper.getValueOnDisk(entry3, dr));
    assertNull(DiskEntry.Helper.getValueOnDisk(entry2, dr));
    assertNotNull(DiskEntry.Helper.getValueOnDisk(entry1, dr));
    assertNull(DiskEntry.Helper.getValueOnDisk(entry3, dr));
    assertNull(DiskEntry.Helper.getValueOnDisk(entry2, dr));

    assertNull(entry2.getDiskId());
    assertNull(entry3.getDiskId());
    assertNotNull(entry1.getDiskId());
    assertNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry3, dr, (LocalRegion) region));
    assertNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry2, dr, (LocalRegion) region));
    assertNotNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry1, dr, (LocalRegion) region));
    assertNull(DiskEntry.Helper.getValueOnDisk(entry3, dr));
    assertNull(DiskEntry.Helper.getValueOnDisk(entry2, dr));
  }

  /**
   * Tests the various DiskEntry.Helper APIs for correctness as there is now delayed creation of
   * DiskId and accessing OplogkeyId will throw UnsupportedException
   */
  @Test
  public void testHelperAPIsForOverflowWithPersistenceRegion() {
    helperAPIsForPersistenceWithOrWithoutOverflowRegion(true /* should overflow */);
  }

  /**
   * Tests the various DiskEntry.Helper APIs for correctness as there is now delayed creation of
   * DiskId and accessing OplogkeyId will throw UnsupportedException
   */
  @Test
  public void testHelperAPIsForPersistenceRegion() {
    helperAPIsForPersistenceWithOrWithoutOverflowRegion(false /* should overflow */);
  }

  /**
   * Tests the various DiskEntry.Helper APIs for correctness as there is now delayed creation of
   * DiskId and accessing OplogkeyId will throw UnsupportedException
   */
  private void helperAPIsForPersistenceWithOrWithoutOverflowRegion(boolean overflow) {
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setMaxOplogSize(1024 * 1024);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(overflow);
    diskProps.setBytesThreshold(10000);
    diskProps.setTimeInterval(0);
    diskProps.setOverFlowCapacity(2);
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    final byte[] val = new byte[1000];
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    region.put("1", val);
    region.put("2", val);
    region.put("3", val);
    DiskEntry entry1 = (DiskEntry) ((LocalRegion) region).basicGetEntry("1");
    DiskEntry entry2 = (DiskEntry) ((LocalRegion) region).basicGetEntry("2");
    DiskEntry entry3 = (DiskEntry) ((LocalRegion) region).basicGetEntry("3");
    assertNotNull(entry2.getDiskId());
    assertNotNull(entry3.getDiskId());
    assertNotNull(entry1.getDiskId());
    assertNotNull(DiskEntry.Helper.getValueOnDisk(entry3, dr));
    assertNotNull(DiskEntry.Helper.getValueOnDisk(entry2, dr));
    assertNotNull(DiskEntry.Helper.getValueOnDisk(entry1, dr));

    assertNotNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry3, dr, (LocalRegion) region));
    assertNotNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry2, dr, (LocalRegion) region));
    assertNotNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry1, dr, (LocalRegion) region));

    region.close();
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    dr = ((LocalRegion) region).getDiskRegion();
    entry1 = (DiskEntry) ((LocalRegion) region).basicGetEntry("1");
    entry2 = (DiskEntry) ((LocalRegion) region).basicGetEntry("2");
    entry3 = (DiskEntry) ((LocalRegion) region).basicGetEntry("3");

    assertNotNull(entry2.getDiskId());
    assertNotNull(entry3.getDiskId());
    assertNotNull(entry1.getDiskId());

    assertNotNull(DiskEntry.Helper.getValueOnDisk(entry3, dr));
    assertNotNull(DiskEntry.Helper.getValueOnDisk(entry2, dr));
    assertNotNull(DiskEntry.Helper.getValueOnDisk(entry1, dr));

    assertNotNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry3, dr, (LocalRegion) region));
    assertNotNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry2, dr, (LocalRegion) region));
    assertNotNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry1, dr, (LocalRegion) region));
  }

  /**
   * Tests the condition when a 'put' is in progress and concurrent 'clear' and 'put'(on the same
   * key) occur. Thus if after Htree ref was set (in 'put'), the region got cleared (and same key
   * re-'put'), the entry will get recorded in the new Oplog without a corresponding create (
   * because the Oplogs containing create have already been deleted due to the clear operation).
   * This put should not proceed. Also, Region creation after closing should not give an exception.
   */
  @Test
  public void testPutClearPut() throws Exception {
    try {
      // Create a persist only region with rolling true
      diskProps.setPersistBackup(true);
      diskProps.setRolling(true);
      diskProps.setMaxOplogSize(1024);
      diskProps.setSynchronous(true);
      this.proceed = false;
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      final Thread clearOp = new Thread(() -> {
        try {
          LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
          region.clear();
          region.put("key1", "value3");
        } catch (Exception e) {
          testFailed = true;
          failureCause = "Encountered Exception=" + e;
        }
      });
      region.getAttributesMutator().setCacheWriter(new CacheWriterAdapter() {
        @Override
        public void beforeUpdate(EntryEvent event) throws CacheWriterException {
          clearOp.start();
        }
      });
      try {
        ThreadUtils.join(clearOp, 30 * 1000);
      } catch (Exception e) {
        testFailed = true;
        failureCause = "Encountered Exception=" + e;
        e.printStackTrace();
      }
      region.create("key1", "value1");
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      region.put("key1", "value2");
      if (!testFailed) {
        region.close();
        region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      } else {
        fail(failureCause);
      }
    } finally {
      testFailed = false;
      proceed = false;
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    }
  }

  /**
   * Tests the condition when a 'put' on an alreay created entry and concurrent 'clear' are
   * happening. Thus if after HTree ref was set (in 'put'), the region got cleared (and same key
   * re-'put'), the entry will actually become a create in the VM The new Oplog should record it as
   * a create even though the Htree ref in ThreadLocal will not match with the current Htree Ref.
   * But the operation is valid & should get recorded in Oplog
   *
   */
  @Test
  public void testPutClearCreate() throws Exception {
    failure = false;
    try {
      // Create a persist only region with rolling true
      diskProps.setPersistBackup(true);
      diskProps.setRolling(true);
      diskProps.setMaxOplogSize(1024);
      diskProps.setSynchronous(true);

      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

      region.create("key1", "value1");
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        @Override
        public void afterSettingDiskRef() {
          Thread clearTh = new Thread(() -> region.clear());
          clearTh.start();
          try {
            ThreadUtils.join(clearTh, 120 * 1000);
            failure = clearTh.isAlive();
            failureCause = "Clear Thread still running !";
          } catch (Exception e) {
            failure = true;
            failureCause = e.toString();
          }
        }
      });
      region.put("key1", "value2");
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      assertFalse(failureCause, failure);
      assertEquals(1, region.size());
      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      assertEquals(1, region.size());
      assertEquals("value2", region.get("key1"));
    } finally {
      testFailed = false;
      proceed = false;
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(new CacheObserverAdapter());
      failure = false;
    }
  }

  /**
   * Tests if 'destroy' transaction is working correctly for sync-overflow-only disk region entry
   */
  @Test
  public void testOverFlowOnlySyncDestroyTx() {
    diskProps.setMaxOplogSize(20480);
    diskProps.setOverFlowCapacity(1);
    diskProps.setDiskDirs(dirs);
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    assertNotNull(region);
    region.put("key", "createValue");
    region.put("key1", "createValue1");
    cache.getCacheTransactionManager().begin();
    region.destroy("key");
    cache.getCacheTransactionManager().commit();
    assertNull("The deleted entry should have been null",
        ((LocalRegion) region).entries.getEntry("key"));
  }

  /**
   * Test to force a recovery to follow the path of switchOutFilesForRecovery and ensuring that
   * IOExceptions do not come as a result. This is also a bug test for bug 37682
   */
  @Test
  public void testSwitchFilesForRecovery() throws Exception {
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, null, Scope.LOCAL);
    put100Int();
    ((LocalRegion) region).getDiskRegion().forceRolling();
    Thread.sleep(2000);
    put100Int();
    int sizeOfRegion = region.size();
    region.close();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, null, Scope.LOCAL);

    if (sizeOfRegion != region.size()) {
      fail(" Expected region size to be " + sizeOfRegion + " after recovery but it is "
          + region.size());
    }
  }

  /**
   * tests directory stats are correctly updated in case of single directory (for bug 37531)
   */
  @Test
  public void testPersist1DirStats() {
    final AtomicBoolean freezeRoller = new AtomicBoolean();
    CacheObserver old = CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      private volatile boolean didBeforeCall = false;

      @Override
      public void beforeGoingToCompact() {
        this.didBeforeCall = true;
        synchronized (freezeRoller) {
          if (!assertDone) {
            try {
              // Here, we are not allowing the Roller thread to roll the old oplog into htree
              while (!freezeRoller.get()) {
                freezeRoller.wait();
              }
              freezeRoller.set(false);
            } catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }
      }

      @Override
      public void afterHavingCompacted() {
        if (this.didBeforeCall) {
          this.didBeforeCall = false;
          LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
          checkDiskStats();
        }
      }
    });
    try {
      final int MAX_OPLOG_SIZE = 500;
      diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
      diskProps.setPersistBackup(true);
      diskProps.setRolling(true);
      diskProps.setSynchronous(true);
      diskProps.setOverflow(false);
      diskProps.setDiskDirsAndSizes(new File[] {dirs[0]}, new int[] {4000});
      final byte[] val = new byte[200];
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      region.put("key1", val);
      checkDiskStats();
      region.put("key2", val);
      checkDiskStats();
      // This put will cause a switch as max-oplog size (500) will be exceeded (600)
      region.put("key3", val);
      synchronized (freezeRoller) {
        checkDiskStats();
        assertDone = true;
        freezeRoller.set(true);
        freezeRoller.notifyAll();
      }

      region.close();
      closeDown();
      // Stop rolling to get accurate estimates:
      diskProps.setRolling(false);

      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

      // On recreating the region after closing, old Oplog file gets rolled into htree
      // "Disk space usage zero when region recreated"
      checkDiskStats();
      region.put("key4", val);
      checkDiskStats();
      region.put("key5", val);
      checkDiskStats();
      assertDone = false;
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      region.put("key6", val);
      // again we expect a switch in oplog here
      synchronized (freezeRoller) {
        checkDiskStats();
        assertDone = true;
        freezeRoller.set(true);
        freezeRoller.notifyAll();
      }
      region.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to exception" + e);
    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(old);
      synchronized (freezeRoller) {
        assertDone = true;
        freezeRoller.set(true);
        freezeRoller.notifyAll();
      }
    }
  }

  /**
   * Tests reduction in size of disk stats when the oplog is rolled.
   */
  @Category(FlakyTest.class) // GEODE-527: jvm sizing sensitive, non-thread-safe test hooks, time
                             // sensitive
  @Test
  public void testStatsSizeReductionOnRolling() throws Exception {
    final int MAX_OPLOG_SIZE = 500 * 2;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setCompactionThreshold(100);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(false);
    diskProps.setDiskDirsAndSizes(new File[] {dirs[0]}, new int[] {4000});
    final byte[] val = new byte[333];
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    final DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    final Object lock = new Object();
    final boolean[] exceptionOccurred = new boolean[] {true};
    final boolean[] okToExit = new boolean[] {false};
    final boolean[] switchExpected = new boolean[] {false};

    // calculate sizes
    final int extra_byte_num_per_entry =
        InternalDataSerializer.calculateBytesForTSandDSID(getDSID((LocalRegion) region));
    final int key3_size =
        DiskOfflineCompactionJUnitTest.getSize4Create(extra_byte_num_per_entry, "key3", val);
    final int tombstone_key1 =
        DiskOfflineCompactionJUnitTest.getSize4TombstoneWithKey(extra_byte_num_per_entry, "key1");
    final int tombstone_key2 =
        DiskOfflineCompactionJUnitTest.getSize4TombstoneWithKey(extra_byte_num_per_entry, "key2");

    // TODO: move static methods from DiskOfflineCompactionJUnitTest to shared util class

    CacheObserver old = CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      private long before = -1;
      private DirectoryHolder dh = null;
      private long oplogsSize = 0;

      @Override
      public void beforeSwitchingOplog() {
        cache.getLogger().info("beforeSwitchingOplog");
        if (!switchExpected[0]) {
          fail("unexpected oplog switch");
        }
        if (before == -1) {
          // only want to call this once; before the 1st oplog destroy
          this.dh = dr.getNextDir();
          this.before = this.dh.getDirStatsDiskSpaceUsage();
        }
      }

      @Override
      public void beforeDeletingCompactedOplog(Oplog oplog) {
        cache.getLogger().info("beforeDeletingCompactedOplog");
        oplogsSize += oplog.getOplogSize();
      }

      @Override
      public void afterHavingCompacted() {
        cache.getLogger().info("afterHavingCompacted");
        if (before > -1) {
          synchronized (lock) {
            okToExit[0] = true;
            long after = this.dh.getDirStatsDiskSpaceUsage();
            // after compaction, in _2.crf, key3 is an create-entry,
            // key1 and key2 are tombstones.
            // _2.drf contained a rvvgc with drMap.size()==1
            int expected_drf_size = Oplog.OPLOG_DISK_STORE_REC_SIZE + Oplog.OPLOG_MAGIC_SEQ_REC_SIZE
                + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE
                + DiskOfflineCompactionJUnitTest.getRVVSize(1, new int[] {0}, true);
            int expected_crf_size = Oplog.OPLOG_DISK_STORE_REC_SIZE + Oplog.OPLOG_MAGIC_SEQ_REC_SIZE
                + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE
                + DiskOfflineCompactionJUnitTest.getRVVSize(1, new int[] {1}, false)
                + Oplog.OPLOG_NEW_ENTRY_BASE_REC_SIZE + key3_size + tombstone_key1 + tombstone_key2;
            int oplog_2_size = expected_drf_size + expected_crf_size;
            if (after != oplog_2_size) {
              cache.getLogger().info(
                  "test failed before=" + before + " after=" + after + " oplogsSize=" + oplogsSize);
              exceptionOccurred[0] = true;
            } else {
              exceptionOccurred[0] = false;
            }
            LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
            lock.notify();
          }
        }
      }
    });
    try {

      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      cache.getLogger().info("putting key1");
      region.put("key1", val);
      checkDiskStats();
      cache.getLogger().info("putting key2");
      region.put("key2", val);
      checkDiskStats();

      cache.getLogger().info("removing key1");
      region.remove("key1");
      cache.getLogger().info("removing key2");
      region.remove("key2");

      // This put will cause a switch as max-oplog size (900) will be exceeded (999)
      switchExpected[0] = true;
      cache.getLogger().info("putting key3");
      region.put("key3", val);
      cache.getLogger().info("waiting for compaction");
      synchronized (lock) {
        if (!okToExit[0]) {
          lock.wait(9000);
          assertTrue(okToExit[0]);
        }
        assertFalse(exceptionOccurred[0]);
      }

      region.close();
    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(old);
    }
  }

  @Test
  public void testUnPreblowOnRegionCreate() throws Exception {
    final int MAX_OPLOG_SIZE = 20000;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(false);
    diskProps.setDiskDirsAndSizes(new File[] {dirs[0]}, new int[] {40000});
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

    try {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      for (int i = 0; i < 10; ++i) {
        region.put("key-" + i, "value-");
      }

      assertEquals(18000, getOplogFileSizeSum(dirs[0], ".crf"));
      assertEquals(2000, getOplogFileSizeSum(dirs[0], ".drf"));

      // make a copy of inflated crf. use this to replace compacted crf to
      // simulate incomplete diskStore close
      File[] files = dirs[0].listFiles();
      for (File file : files) {
        if (file.getName().endsWith(".crf") || file.getName().endsWith(".drf")) {
          File inflated = new File(file.getAbsolutePath() + "_inflated");
          FileUtils.copyFile(file, inflated);
        }
      }

      cache.close();
      assertTrue(500 > getOplogFileSizeSum(dirs[0], ".crf"));
      assertTrue(100 > getOplogFileSizeSum(dirs[0], ".drf"));

      // replace compacted crf with inflated crf and remove krf
      files = dirs[0].listFiles();
      for (File file : files) {
        String name = file.getName();
        if (name.endsWith(".krf") || name.endsWith(".crf") || name.endsWith(".drf")) {
          file.delete();
        }
      }
      for (File file : files) {
        String name = file.getName();
        if (name.endsWith("_inflated")) {
          assertTrue(file.renameTo(new File(file.getAbsolutePath().replace("_inflated", ""))));
        }
      }
      assertEquals(18000, getOplogFileSizeSum(dirs[0], ".crf"));
      assertEquals(2000, getOplogFileSizeSum(dirs[0], ".drf"));

      createCache();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      for (int i = 10; i < 20; ++i) {
        region.put("key-" + i, "value-");
      }

      int sizeCrf = getOplogFileSizeSum(dirs[0], ".crf");
      assertTrue("crf too big:" + sizeCrf, sizeCrf < 18000 + 500);
      assertTrue("crf too small:" + sizeCrf, sizeCrf > 18000);
      int sizeDrf = getOplogFileSizeSum(dirs[0], ".drf");
      assertTrue("drf too big:" + sizeDrf, sizeDrf < 2000 + 100);
      assertTrue("drf too small:" + sizeDrf, sizeDrf > 2000);

      // test that region recovery does not cause unpreblow
      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

      assertEquals(sizeCrf, getOplogFileSizeSum(dirs[0], ".crf"));
      assertEquals(sizeDrf, getOplogFileSizeSum(dirs[0], ".drf"));
    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    }
  }

  private int getOplogFileSizeSum(File dir, String type) {
    int sum = 0;
    File[] files = dir.listFiles();
    for (File file : files) {
      String name = file.getName();
      if (name.endsWith(type)) {
        sum += file.length();
      }
    }
    return sum;
  }

  @Test
  public void testMagicSeqPresence() throws Exception {
    final int MAX_OPLOG_SIZE = 200;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(false);
    diskProps.setDiskDirsAndSizes(new File[] {dirs[0]}, new int[] {4000});
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

    // 3 types of oplog files will be verified
    verifyOplogHeader(dirs[0], ".if", ".crf", ".drf");

    try {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      IntStream.range(0, 20).forEach(i -> region.put("key-" + i, "value-" + i));
      // krf is created, so 4 types of oplog files will be verified
      verifyOplogHeader(dirs[0], ".if", ".crf", ".drf", ".krf");

      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

      verifyOplogHeader(dirs[0], ".if", ".crf", ".drf", ".krf");
      region.close();
    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    }
  }

  private void verifyOplogHeader(File dir, String... oplogTypes) throws IOException {

    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
      List<String> types = new ArrayList<>(Arrays.asList(oplogTypes));
      Arrays.stream(dir.listFiles()).map(File::getName).map(f -> f.substring(f.indexOf(".")))
          .forEach(types::remove);
      return types.isEmpty();
    });

    File[] files = dir.listFiles();
    HashSet<String> verified = new HashSet<>();
    for (File file : files) {
      String name = file.getName();
      byte[] expect = new byte[Oplog.OPLOG_MAGIC_SEQ_REC_SIZE];
      if (name.endsWith(".crf")) {
        expect[0] = Oplog.OPLOG_MAGIC_SEQ_ID;
        System.arraycopy(OPLOG_TYPE.CRF.getBytes(), 0, expect, 1, OPLOG_TYPE.getLen());
        verified.add(".crf");
      } else if (name.endsWith(".drf")) {
        expect[0] = Oplog.OPLOG_MAGIC_SEQ_ID;
        System.arraycopy(OPLOG_TYPE.DRF.getBytes(), 0, expect, 1, OPLOG_TYPE.getLen());
        verified.add(".drf");
      } else if (name.endsWith(".krf")) {
        expect[0] = Oplog.OPLOG_MAGIC_SEQ_ID;
        System.arraycopy(OPLOG_TYPE.KRF.getBytes(), 0, expect, 1, OPLOG_TYPE.getLen());
        verified.add(".krf");
      } else if (name.endsWith(".if")) {
        expect[0] = DiskInitFile.OPLOG_MAGIC_SEQ_ID;
        System.arraycopy(OPLOG_TYPE.IF.getBytes(), 0, expect, 1, OPLOG_TYPE.getLen());
        verified.add(".if");
      } else {
        System.out.println("Ignored: " + file);
        continue;
      }
      expect[expect.length - 1] = 21; // EndOfRecord

      byte[] buf = new byte[Oplog.OPLOG_MAGIC_SEQ_REC_SIZE];

      FileInputStream fis = new FileInputStream(file);
      int count = fis.read(buf, 0, 8);
      fis.close();

      System.out.println("Verifying: " + file);
      assertEquals("expected a read to return 8 but it returned " + count + " for file " + file, 8,
          count);
      assertTrue(Arrays.equals(expect, buf));
    }

    assertEquals(oplogTypes.length, verified.size());
  }

  /**
   * Tests if without rolling the region size before close is same as after recreation
   */
  @Test
  public void testSizeStatsAfterRecreationInAsynchMode() throws Exception {
    final int MAX_OPLOG_SIZE = 1000;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setSynchronous(false);
    diskProps.setBytesThreshold(800);
    diskProps.setOverflow(false);
    diskProps.setDiskDirsAndSizes(new File[] {dirs[0], dirs[1]}, new int[] {4000, 4000});
    final byte[] val = new byte[25];
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps);
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();

    try {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      for (int i = 0; i < 42; ++i) {
        region.put("key" + i, val);
      }
      // need to wait for writes to happen before getting size
      dr.flushForTesting();
      long size1 = 0;
      for (DirectoryHolder dh : dr.getDirectories()) {
        size1 += dh.getDirStatsDiskSpaceUsage();
      }
      System.out.println("Size before close = " + size1);
      region.close();
      diskProps.setSynchronous(true);
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      dr = ((LocalRegion) region).getDiskRegion();
      long size2 = 0;
      for (DirectoryHolder dh : dr.getDirectories()) {
        size2 += dh.getDirStatsDiskSpaceUsage();
      }
      System.out.println("Size after recreation= " + size2);
      assertEquals(size1, size2);
      region.close();
    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    }
  }

  @Test
  public void testAsynchModeStatsBehaviour() throws Exception {
    final int MAX_OPLOG_SIZE = 1000;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setSynchronous(false);
    diskProps.setBytesThreshold(800);
    diskProps.setTimeInterval(Long.MAX_VALUE);
    diskProps.setOverflow(false);
    diskProps.setDiskDirsAndSizes(new File[] {dirs[0]}, new int[] {4000});
    final byte[] val = new byte[25];
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps);
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();

    try {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      for (int i = 0; i < 4; ++i) {
        region.put("key" + i, val);
      }
      // This test now has a race condition in it since
      // async puts no longer increment disk space.
      // It is not until a everything is flushed that we will know the disk size.
      dr.flushForTesting();

      checkDiskStats();
      long size1 = 0;
      for (DirectoryHolder dh : dr.getDirectories()) {
        size1 += dh.getDirStatsDiskSpaceUsage();
      }
      System.out.println("Size before close = " + size1);
      region.close();
      diskProps.setSynchronous(true);
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
      dr = ((LocalRegion) region).getDiskRegion();
      long size2 = 0;
      for (DirectoryHolder dh : dr.getDirectories()) {
        size2 += dh.getDirStatsDiskSpaceUsage();
      }
      System.out.println("Size after recreation= " + size2);
      assertEquals(size1, size2);
      region.close();
    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    }
  }

  private long diskSpaceUsageStats() {
    return ((LocalRegion) region).getDiskRegion().getInfoFileDir().getDirStatsDiskSpaceUsage();
  }

  private long calculatedDiskSpaceUsageStats() {
    return oplogSize();
  }

  private void checkDiskStats() {
    long actualDiskStats = diskSpaceUsageStats();
    long computedDiskStats = calculatedDiskSpaceUsageStats();
    int tries = 0;
    while (actualDiskStats != computedDiskStats && tries++ <= 100) {
      // race conditions exist in which the stats change
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignore) {
      }
      actualDiskStats = diskSpaceUsageStats();
      computedDiskStats = calculatedDiskSpaceUsageStats();
    }
    assertEquals(computedDiskStats, actualDiskStats);
  }

  private long oplogSize() {
    long size = ((LocalRegion) region).getDiskRegion().getDiskStore().undeletedOplogSize.get();
    Oplog[] opArray =
        ((LocalRegion) region).getDiskRegion().getDiskStore().persistentOplogs.getAllOplogs();
    if (opArray != null) {
      for (Oplog log : opArray) {
        size += log.getOplogSize();
      }
    }
    return size;
  }

  private int getDSID(LocalRegion lr) {
    return lr.getDistributionManager().getDistributedSystemId();
  }
}
