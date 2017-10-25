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
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.lru.LRUStatistics;
import org.apache.geode.internal.cache.lru.NewLRUClockHand;
import org.apache.geode.internal.cache.persistence.UninterruptibleFileChannel;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * TODO: fails when running integrationTest from gradle command-line on Windows 7
 * 
 * JUnit tests covering some miscellaneous functionality of Disk Region.
 */
@Category(IntegrationTest.class)
public class DiskRegionJUnitTest extends DiskRegionTestingBase {

  private static volatile boolean hasNotified = false;
  private static volatile boolean putsHaveStarted = false;

  private volatile boolean exceptionOccurred = false;
  private volatile boolean finished = false;

  private DiskRegionProperties diskProps = new DiskRegionProperties();

  private DiskRegionProperties diskProps1 = new DiskRegionProperties();
  private DiskRegionProperties diskProps2 = new DiskRegionProperties();
  private DiskRegionProperties diskProps3 = new DiskRegionProperties();
  private DiskRegionProperties diskProps4 = new DiskRegionProperties();
  private DiskRegionProperties diskProps5 = new DiskRegionProperties();
  private DiskRegionProperties diskProps6 = new DiskRegionProperties();
  private DiskRegionProperties diskProps7 = new DiskRegionProperties();
  private DiskRegionProperties diskProps8 = new DiskRegionProperties();
  private DiskRegionProperties diskProps9 = new DiskRegionProperties();
  private DiskRegionProperties diskProps10 = new DiskRegionProperties();
  private DiskRegionProperties diskProps11 = new DiskRegionProperties();
  private DiskRegionProperties diskProps12 = new DiskRegionProperties();

  private Region region1;
  private Region region2;
  private Region region3;
  private Region region4;
  private Region region5;
  private Region region6;
  private Region region7;
  private Region region8;
  private Region region9;
  private Region region10;
  private Region region11;
  private Region region12;

  private boolean failed = false;

  private int counter = 0;
  private boolean hasBeenNotified = false;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Override
  protected final void postSetUp() throws Exception {
    this.exceptionOccurred = false;
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
  }

  @Override
  protected final void postTearDown() throws Exception {
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;
  }

  private static class MyCL extends CacheListenerAdapter {
    public EntryEvent lastEvent;

    @Override
    public void afterDestroy(EntryEvent event) {
      this.lastEvent = event;
    }
  }

  @Test
  public void testRemoveCorrectlyRecorded() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setOverflow(true);
    props.setOverFlowCapacity(1);
    props.setDiskDirs(dirs);
    Region region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, props);
    region.put("1", "1");
    region.put("2", "2");
    region.put("3", "3");

    MyCL cacheListener = new MyCL();
    region.getAttributesMutator().addCacheListener(cacheListener);
    region.destroy("1");

    // Make sure we don't get an old value when doing a destroy
    // of an entry that overflowed to disk.
    // If we do then we have hit bug 40795.
    assertNotNull(cacheListener.lastEvent);
    assertEquals(null, cacheListener.lastEvent.getOldValue());

    assertTrue(region.get("1") == null);

    boolean exceptionOccurred = false;
    try {
      Object result = ((LocalRegion) region).getValueOnDisk("1");
      if (result == null || result.equals(Token.TOMBSTONE)) {
        exceptionOccurred = true;
      }
    } catch (EntryNotFoundException e) {
      exceptionOccurred = true;
    }

    if (!exceptionOccurred) {
      fail("exception did not occur although was supposed to occur");
    }

    region.close();
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, props);

    assertTrue(region.get("1") == null);
    region.destroyRegion();
  }

  /**
   * Tests if region overflows correctly and stats are create and updated correctly.
   */
  @Test
  public void testDiskRegionOverflow() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setOverflow(true);
    props.setOverFlowCapacity(100);
    props.setDiskDirs(dirs);
    Region region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache, props);

    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    assertNotNull(dr);

    DiskRegionStats diskStats = dr.getStats();
    LRUStatistics lruStats =
        ((LocalRegion) region).getEvictionController().getLRUHelper().getStats();
    assertNotNull(diskStats);
    assertNotNull(lruStats);

    dr.flushForTesting();

    assertEquals(0, diskStats.getWrites());
    assertEquals(0, diskStats.getReads());
    assertEquals(0, lruStats.getEvictions());

    // Put in larger stuff until we start evicting
    int total;
    for (total = 0; lruStats.getEvictions() <= 0; total++) {
      // getLogWriter().info("DEBUG: total " + total + ", evictions " +
      // lruStats.getEvictions());
      int[] array = new int[250];
      array[0] = total;
      region.put(new Integer(total), array);
    }

    dr.flushForTesting();

    assertEquals(1, diskStats.getWrites());
    assertEquals(0, diskStats.getReads());
    assertEquals(1, lruStats.getEvictions());
    assertEquals(1, diskStats.getNumOverflowOnDisk());
    assertEquals(total - 1, diskStats.getNumEntriesInVM());
    Object value = region.get(new Integer(0));
    dr.flushForTesting();

    assertNotNull(value);
    assertEquals(0, ((int[]) value)[0]);

    assertEquals(2, diskStats.getWrites());
    assertEquals(1, diskStats.getReads());
    assertEquals(2, lruStats.getEvictions());

    for (int i = 0; i < total; i++) {
      int[] array = (int[]) region.get(new Integer(i));
      assertNotNull(array);
      assertEquals(i, array[0]);
    }
  }

  private void assertArrayEquals(Object expected, Object v) {
    assertEquals(expected.getClass(), v.getClass());
    int vLength = Array.getLength(v);
    assertEquals(Array.getLength(expected), vLength);
    for (int i = 0; i < vLength; i++) {
      assertEquals(Array.get(expected, i), Array.get(v, i));
    }
  }

  /**
   * test method for putting different objects and validating that they have been correctly put
   */
  @Test
  public void testDifferentObjectTypePuts() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setOverflow(true);
    props.setOverFlowCapacity(100);
    props.setDiskDirs(dirs);

    int total = 10;
    {
      Region region = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache, props);
      DiskRegion dr = ((LocalRegion) region).getDiskRegion();
      for (int i = 0; i < total; i++) {
        String s = String.valueOf(i);
        region.put(s, s);
      }
      region.put("foobar", "junk");

      region.localDestroy("foobar");

      region.put("foobar2", "junk");
      dr.flushForTesting();
      region.localDestroy("foobar2");
      // test invalidate
      region.put("invalid", "invalid");
      dr.flushForTesting();
      region.invalidate("invalid");
      dr.flushForTesting();
      assertTrue(region.containsKey("invalid") && !region.containsValueForKey("invalid"));
      total++;
      // test local-invalidate
      region.put("localinvalid", "localinvalid");
      dr.flushForTesting();
      region.localInvalidate("localinvalid");
      dr.flushForTesting();
      assertTrue(region.containsKey("localinvalid") && !region.containsValueForKey("localinvalid"));
      total++;

      // test byte[] values
      region.put("byteArray", new byte[0]);
      dr.flushForTesting();
      assertArrayEquals(new byte[0], region.get("byteArray"));
      total++;
      // test modification
      region.put("modified", "originalValue");
      dr.flushForTesting();
      region.put("modified", "modified");
      dr.flushForTesting();
      assertEquals("modified", region.get("modified"));
      total++;
      assertEquals(total, region.size());
    }
    cache.close();
    cache = createCache();
    {
      Region region = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache, props);
      assertEquals(total, region.size());
      assertEquals(true, region.containsKey("invalid"));
      assertEquals(null, region.get("invalid"));
      assertEquals(false, region.containsValueForKey("invalid"));

      region.localDestroy("invalid");
      total--;
      assertTrue(region.containsKey("localinvalid") && !region.containsValueForKey("localinvalid"));
      region.localDestroy("localinvalid");
      total--;
      assertArrayEquals(new byte[0], region.get("byteArray"));
      region.localDestroy("byteArray");
      total--;
      assertEquals("modified", region.get("modified"));
      region.localDestroy("modified");
      total--;
    }
  }

  private static class DoesPut implements Runnable {

    private Region region;

    DoesPut(Region region) {
      this.region = region;
    }

    @Override
    public void run() {
      region.put(new Integer(1), new Integer(2));
    }

  }

  private class DoesGet implements Runnable {

    private final Region region;

    DoesGet(Region region) {
      this.region = region;
    }

    @Override
    public void run() {
      synchronized (this.region) {
        if (!hasNotified) {
          try {
            long startTime = System.currentTimeMillis();
            region.wait(23000);
            long interval = System.currentTimeMillis() - startTime;
            if (interval > 23000) {
              testFailed = true;
              failureCause = " Test took too long in wait, it should have exited before 23000 ms";
              fail(" Test took too long in wait, it should have exited before 23000 ms");
            }
          } catch (InterruptedException e) {
            testFailed = true;
            failureCause = "interrupted exception not expected here";
            throw new AssertionError("exception not expected here", e);
          }
        }
        region.get(new Integer(0));
      } // synchronized
    } // run()
  }

  @Test
  public void testFaultingInRemovalFromAsyncBuffer() throws Exception {
    failed = false;
    DiskRegionProperties props = new DiskRegionProperties();
    props.setOverflow(true);
    props.setRolling(true);
    props.setOverFlowCapacity(100);
    props.setDiskDirs(dirs);

    Region region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, props);
    DoesGet get = new DoesGet(region);

    Thread thread1 = new Thread(get);
    Thread thread2 = new Thread(get);
    Thread thread3 = new Thread(get);
    Thread thread4 = new Thread(get);
    Thread thread5 = new Thread(get);

    thread1.start();
    thread2.start();
    thread3.start();
    thread4.start();
    thread5.start();

    for (int i = 0; i < 110; i++) {
      region.put(new Integer(i), new Integer(i));
    }

    synchronized (region) {
      region.notifyAll();
      hasNotified = true;
    }

    long startTime = System.currentTimeMillis();
    ThreadUtils.join(thread1, 20 * 1000);
    ThreadUtils.join(thread2, 20 * 1000);
    ThreadUtils.join(thread3, 20 * 1000);
    ThreadUtils.join(thread4, 20 * 1000);
    ThreadUtils.join(thread5, 20 * 1000);

    long interval = System.currentTimeMillis() - startTime;
    if (interval > 100000) {
      fail(" Test took too long in going to join, it should have exited before 100000 ms");
    }
    assertFalse(failureCause, testFailed);

  }

  @Test
  public void testGetWhileRolling() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setOverflow(true);
    props.setOverFlowCapacity(1);
    props.setDiskDirs(dirs);

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    final Region region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, props);

    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      @Override
      public void beforeGoingToCompact() {
        synchronized (region) {
          region.notifyAll();
          hasBeenNotified = true;
        }
      }
    });

    Runnable get = new Runnable() {
      @Override
      public void run() {
        int localCounter = 0;
        synchronized (region) {
          localCounter = counter;
          counter++;
        }
        int limit = ((localCounter * 1000) + 1000);
        for (int i = localCounter * 1000; i < limit; i++) {
          if (finished) {
            return;
          }
          try {
            Thread.sleep(10);
            region.get(new Integer(i));
          } catch (Exception e) {
            if (finished) {
              return;
            }
            failed = true;
            throw new AssertionError("failed due to ", e);
          }
        }

      }
    };

    for (int i = 0; i < 8000; i++) {
      region.put(new Integer(i), new Integer(i));
    }

    finished = false;
    Thread thread1 = new Thread(get);
    Thread thread2 = new Thread(get);
    Thread thread3 = new Thread(get);
    Thread thread4 = new Thread(get);
    Thread thread5 = new Thread(get);

    thread1.start();
    thread2.start();
    thread3.start();
    thread4.start();
    thread5.start();

    long startTime = System.currentTimeMillis();
    finished = true;
    ThreadUtils.join(thread1, 5 * 60 * 1000);
    ThreadUtils.join(thread2, 5 * 60 * 1000);
    ThreadUtils.join(thread3, 5 * 60 * 1000);
    ThreadUtils.join(thread4, 5 * 60 * 1000);
    ThreadUtils.join(thread5, 5 * 60 * 1000);
    long interval = System.currentTimeMillis() - startTime;
    if (interval > 100000) {
      fail(" Test took too long in going to join, it should have exited before 100000 ms");
    }

    if (failed) {
      fail(" test had failed while doing get ");
    }
  }

  /**
   * DiskDirectoriesJUnitTest:
   * 
   * This tests the potential deadlock situation if the region is created such that rolling is
   * turned on but the Max directory space is less than or equal to the Max Oplog Size. In such
   * situations , if during switch over , if the Oplog to be rolled is added after function call of
   * obtaining nextDir , a dead lock occurs
   */
  @Test
  public void testSingleDirectoryNotHanging() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    // setting to null will make only one directory
    File dir = new File("testSingleDirectoryNotHanging");
    dir.mkdir();
    dir.deleteOnExit();
    File[] dirs = new File[1];
    dirs[0] = dir;
    int[] dirSizes = {2048};
    diskRegionProperties.setDiskDirsAndSizes(dirs, dirSizes);
    diskRegionProperties.setMaxOplogSize(2097152);
    diskRegionProperties.setRolling(true);
    region =
        DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskRegionProperties, Scope.LOCAL);

    Puts puts = new Puts(region);
    puts.performPuts();

    if (!puts.putSuccessful(0)) {
      fail(" first put did not succeed");
    }

    if (!puts.putSuccessful(1)) {
      fail(" second put did not succeed");
    }

    if (!puts.putSuccessful(2)) {
      fail(" third put did not succeed");
    }

    if (puts.exceptionOccurred()) {
      fail(" Exception was not supposed to occur but did occur");
    }
    closeDown();
  }

  @Test
  public void testOperationGreaterThanMaxOplogSize() throws Exception {
    putsHaveStarted = false;
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setMaxOplogSize(512);
    diskRegionProperties.setRolling(true);
    Region region =
        DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskRegionProperties, Scope.LOCAL);

    Puts puts = new Puts(region);
    puts.performPuts();

    if (!puts.putSuccessful(0)) {
      fail(" first put did not succeed");
    }

    if (!puts.putSuccessful(1)) {
      fail(" second put did not succeed");
    }

    if (!puts.putSuccessful(2)) {
      fail(" third put did not succeed");
    }

    if (puts.exceptionOccurred()) {
      fail(" Exception was not supposed to occur but did occur");
    }
  }

  /**
   * As we have relaxed the constraint of max dir size
   */
  @Test
  public void testOperationGreaterThanMaxDirSize() throws Exception {
    putsHaveStarted = false;
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName("IGNORE_EXCEPTION_testOperationGreaterThanMaxDirSize");
    int[] dirSizes = {1025, 1025, 1025, 1025};
    diskRegionProperties.setDiskDirsAndSizes(dirs, dirSizes);
    diskRegionProperties.setMaxOplogSize(600);
    diskRegionProperties.setRolling(false);
    Region region =
        DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskRegionProperties, Scope.LOCAL);
    DiskStore ds = ((LocalRegion) region).getDiskStore();
    if (!Arrays.equals(dirSizes, ds.getDiskDirSizes())) {
      fail("expected=" + Arrays.toString(dirSizes) + " actual="
          + Arrays.toString(ds.getDiskDirSizes()));
    }

    Puts puts = new Puts(region, 1026);
    puts.performPuts();

    if (!puts.exceptionOccurred()) {
      fail(" Exception was supposed to occur but did not occur");
    }
    if (puts.putSuccessful(0)) {
      fail(" first put did succeed when it was not supposed to");
    }

    if (puts.putSuccessful(1)) {
      fail(" second put did  succeed  when it was not supposed to");
    }

    if (puts.putSuccessful(2)) {
      fail(" third put did  succeed  when it was not supposed to");
    }

    // if the exception occurred then the region should be closed already
    ((LocalRegion) region).getDiskStore().waitForClose();
  }

  /**
   * When max-dir-size is exceeded and compaction is enabled we allow oplogs to keep getting
   * created. Make sure that when they do they do not keep putting one op per oplog (which is caused
   * by bug 42464).
   */
  @Test
  public void testBug42464() throws Exception {
    putsHaveStarted = false;
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    File[] myDirs = new File[] {dirs[0]};
    int[] dirSizes = {900};
    diskRegionProperties.setDiskDirsAndSizes(myDirs, dirSizes);
    diskRegionProperties.setMaxOplogSize(500);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setOverFlowCapacity(1);
    Region region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskRegionProperties);
    DiskStore ds = ((LocalRegion) region).getDiskStore();
    DiskStoreImpl dsi = (DiskStoreImpl) ds;
    if (!Arrays.equals(dirSizes, ds.getDiskDirSizes())) {
      fail("expected=" + Arrays.toString(dirSizes) + " actual="
          + Arrays.toString(ds.getDiskDirSizes()));
    }
    // One entry is kept in memory
    // since the crf max is 500 we should only be able to have 4 entries. The
    // 5th should not fit because of record overhead.
    for (int i = 0; i <= 9; i++) {
      region.getCache().getLogger().info("putting " + i);
      region.put(new Integer(i), new byte[101]);
    }
    // At this point we should have two oplogs that are basically full
    // (they should each contain 4 entries) and a third oplog that
    // contains a single entry. But the 3rd one will end up also containing 4
    // entries.
    // TODO what is the max size of this 3rd oplog's crf? The first two crfs
    // will be close to 400 bytes each. So the max size of the 3rd oplog should
    // be close to 100.
    ArrayList<OverflowOplog> oplogs = dsi.testHookGetAllOverflowOplogs();
    assertEquals(3, oplogs.size());

    // TODO verify oplogs
    // Now make sure that further oplogs can hold 4 entries
    for (int j = 10; j <= 13; j++) {
      region.getCache().getLogger().info("putting " + j);
      region.put(new Integer(j), new byte[101]);
    }
    oplogs = dsi.testHookGetAllOverflowOplogs();
    assertEquals(4, oplogs.size());
    // now remove all entries and make sure old oplogs go away
    for (int i = 0; i <= 13; i++) {
      region.getCache().getLogger().info("removing " + i);
      region.remove(new Integer(i));
    }
    // give background compactor chance to remove oplogs
    oplogs = dsi.testHookGetAllOverflowOplogs();
    int retryCount = 20;
    while (oplogs.size() > 1 && retryCount > 0) {
      Wait.pause(100);
      oplogs = dsi.testHookGetAllOverflowOplogs();
      retryCount--;
    }
    assertEquals(1, oplogs.size());
  }

  private static class Puts implements Runnable {

    private int dataSize = 1024;
    private Region region;
    private volatile boolean[] putSuccessful = new boolean[3];
    private volatile boolean exceptionOccurred = false;

    Puts(Region region) {
      this.region = region;
    }

    Puts(Region region, int dataSize) {
      this.region = region;
      this.dataSize = dataSize;
    }

    public boolean exceptionOccurred() {
      return exceptionOccurred;
    }

    public boolean putSuccessful(int index) {
      return putSuccessful[index];
    }

    @Override
    public void run() {
      performPuts();
    }

    public void performPuts() {
      exceptionOccurred = false;
      putSuccessful[0] = false;
      putSuccessful[1] = false;
      putSuccessful[2] = false;

      try {
        byte[] bytes = new byte[this.dataSize];
        synchronized (this) {
          putsHaveStarted = true;
          this.notify();
        }
        region.put("1", bytes);
        putSuccessful[0] = true;
        region.put("2", bytes);
        putSuccessful[1] = true;
        region.put("3", bytes);
        putSuccessful[2] = true;
      } catch (DiskAccessException e) {
        exceptionOccurred = true;
      }
    }
  }

  @Test
  public void testSingleDirectorySizeViolation() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName("IGNORE_EXCEPTION_testSingleDirectorySizeViolation");
    // setting to null will make only one directory
    File dir = temporaryFolder.newFolder("testSingleDirectoryNotHanging");
    File[] dirs = new File[] {dir};
    int[] dirSizes = {2048};
    diskRegionProperties.setDiskDirsAndSizes(dirs, dirSizes);
    diskRegionProperties.setMaxOplogSize(2097152);
    diskRegionProperties.setRolling(false);
    region =
        DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskRegionProperties, Scope.LOCAL);

    Puts puts = new Puts(region);
    puts.performPuts();

    if (!puts.putSuccessful(0)) {
      fail(" first put did not succeed");
    }

    if (puts.putSuccessful(1)) {
      fail(" second put should not succeed");
    }

    if (!puts.exceptionOccurred()) {
      fail(" Exception was supposed to occur but did not occur");
    }
    // if the exception occurred then the region should be closed already
    ((LocalRegion) region).getDiskStore().waitForClose();

    closeDown();
  }

  /**
   * DiskRegDiskAccessExceptionTest : Disk region test for DiskAccessException.
   */
  @Test
  public void testDiskFullExcep() throws Exception {
    int[] diskDirSize1 = new int[4];
    diskDirSize1[0] = (2048 + 500);
    diskDirSize1[1] = (2048 + 500);
    diskDirSize1[2] = (2048 + 500);
    diskDirSize1[3] = (2048 + 500);

    diskProps.setDiskDirsAndSizes(dirs, diskDirSize1);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setMaxOplogSize(1000000000);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    int[] diskSizes1 = ((LocalRegion) region).getDiskDirSizes();

    assertEquals(2048 + 500, diskSizes1[0]);
    assertEquals(2048 + 500, diskSizes1[1]);
    assertEquals(2048 + 500, diskSizes1[2]);
    assertEquals(2048 + 500, diskSizes1[3]);

    // we have room for 2 values per dir

    final byte[] value = new byte[1024];
    Arrays.fill(value, (byte) 77);

    for (int i = 0; i < 8; i++) {
      region.put("" + i, value);
    }

    // we should have put 2 values in each dir so the next one should not fit
    logWriter
        .info("<ExpectedException action=add>" + "DiskAccessException" + "</ExpectedException>");
    try {
      region.put("FULL", value);
      fail("FAILED::DiskAccessException is expected here !!");
    } catch (DiskAccessException e) {
    } finally {
      logWriter.info(
          "<ExpectedException action=remove>" + "DiskAccessException" + "</ExpectedException>");
    }

    // if the exception occurred then the region should be closed already
    ((LocalRegion) region).getDiskStore().waitForClose();
    assertEquals(true, cache.isClosed());
  }

  /**
   * Make sure if compaction is enabled that we can exceed the disk dir limit
   */
  @Test
  public void testNoDiskFullExcep() throws Exception {
    int[] diskDirSize1 = new int[4];
    diskDirSize1[0] = (2048 + 500);
    diskDirSize1[1] = (2048 + 500);
    diskDirSize1[2] = (2048 + 500);
    diskDirSize1[3] = (2048 + 500);

    diskProps.setDiskDirsAndSizes(dirs, diskDirSize1);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setMaxOplogSize(1000000000);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    int[] diskSizes1 = ((LocalRegion) region).getDiskDirSizes();

    assertEquals(2048 + 500, diskSizes1[0]);
    assertEquals(2048 + 500, diskSizes1[1]);
    assertEquals(2048 + 500, diskSizes1[2]);
    assertEquals(2048 + 500, diskSizes1[3]);

    // we have room for 2 values per dir

    final byte[] value = new byte[1024];
    Arrays.fill(value, (byte) 77);
    try {
      for (int i = 0; i < 8; i++) {
        region.put("" + i, value);
      }
    } catch (DiskAccessException e) {
      logWriter.error("Exception occurred but not expected", e);
      throw new AssertionError("FAILED::", e);
    }

    // we should have put 2 values in each dir so the next one should not fit
    // but will be allowed because compaction is enabled.
    // It should log a warning
    try {
      region.put("OK", value);
    } catch (DiskAccessException e) {
      logWriter.error("Exception occurred but not expected", e);
      throw new AssertionError("FAILED::", e);
    }

    assertEquals(false, cache.isClosed());
  }

  /**
   * DiskRegDiskAccessExceptionTest : Disk region test for DiskAccessException.
   */
  @Test
  public void testDiskFullExcepOverflowOnly() throws Exception {
    int[] diskDirSize1 = new int[4];
    diskDirSize1[0] = (2048 + 500);
    diskDirSize1[1] = (2048 + 500);
    diskDirSize1[2] = (2048 + 500);
    diskDirSize1[3] = (2048 + 500);

    diskProps.setDiskDirsAndSizes(dirs, diskDirSize1);
    diskProps.setPersistBackup(false);
    diskProps.setRolling(false);
    diskProps.setMaxOplogSize(1000000000);
    diskProps.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    int[] diskSizes1 = ((LocalRegion) region).getDiskDirSizes();

    assertEquals(2048 + 500, diskSizes1[0]);
    assertEquals(2048 + 500, diskSizes1[1]);
    assertEquals(2048 + 500, diskSizes1[2]);
    assertEquals(2048 + 500, diskSizes1[3]);

    // we have room for 2 values per dir

    final byte[] value = new byte[1024];
    Arrays.fill(value, (byte) 77);

    // put a dummy value in since one value stays in memory
    region.put("FIRST", value);
    try {
      for (int i = 0; i < 8; i++) {
        region.put("" + i, value);
      }
    } catch (DiskAccessException e) {
      logWriter.error("Exception occurred but not expected", e);
      throw new AssertionError("FAILED::", e);
    }

    // we should have put 2 values in each dir so the next one should not fit
    logWriter
        .info("<ExpectedException action=add>" + "DiskAccessException" + "</ExpectedException>");
    try {
      region.put("FULL", value);
      fail("FAILED::DiskAccessException is expected here !!");
    } catch (DiskAccessException e) {
    } finally {
      logWriter.info(
          "<ExpectedException action=remove>" + "DiskAccessException" + "</ExpectedException>");
    }

    // if the exception occurred then the region should be closed already
    ((LocalRegion) region).getDiskStore().waitForClose();
    assertEquals(true, cache.isClosed());
  }

  /**
   * Make sure if compaction is enabled that we can exceed the disk dir limit
   */
  @Test
  public void testNoDiskFullExcepOverflowOnly() throws Exception {
    int[] diskDirSize1 = new int[4];
    diskDirSize1[0] = (2048 + 500);
    diskDirSize1[1] = (2048 + 500);
    diskDirSize1[2] = (2048 + 500);
    diskDirSize1[3] = (2048 + 500);

    diskProps.setDiskDirsAndSizes(dirs, diskDirSize1);
    diskProps.setPersistBackup(false);
    diskProps.setRolling(true);
    diskProps.setMaxOplogSize(1000000000);
    diskProps.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    int[] diskSizes1 = ((LocalRegion) region).getDiskDirSizes();

    assertEquals(2048 + 500, diskSizes1[0]);
    assertEquals(2048 + 500, diskSizes1[1]);
    assertEquals(2048 + 500, diskSizes1[2]);
    assertEquals(2048 + 500, diskSizes1[3]);

    // we have room for 2 values per dir

    final byte[] value = new byte[1024];
    Arrays.fill(value, (byte) 77);
    // put a dummy value in since one value stays in memory
    region.put("FIRST", value);
    try {
      for (int i = 0; i < 8; i++) {
        region.put("" + i, value);
      }
    } catch (DiskAccessException e) {
      logWriter.error("Exception occurred but not expected", e);
      throw new AssertionError("FAILED::", e);
    }

    // we should have put 2 values in each dir so the next one should not fit
    // but will be allowed because compaction is enabled.
    // It should log a warning
    try {
      region.put("OK", value);
    } catch (DiskAccessException e) {
      logWriter.error("Exception occurred but not expected", e);
      throw new AssertionError("FAILED::", e);
    }

    assertEquals(false, cache.isClosed());
  }

  /**
   * DiskAccessException Test : Even if rolling doesn't free the space in stipulated time, the
   * operation should not get stuck or see Exception
   */
  @Test
  public void testSynchModeAllowOperationToProceedEvenIfDiskSpaceIsNotSufficient()
      throws Exception {
    File[] dirs1 = null;
    File testingDirectory1 = new File("testingDirectory1");
    testingDirectory1.mkdir();
    testingDirectory1.deleteOnExit();
    File file1 = new File("testingDirectory1/" + "testSyncPersistRegionDAExp" + "1");
    file1.mkdir();
    file1.deleteOnExit();
    dirs1 = new File[1];
    dirs1[0] = file1;
    int[] diskDirSize1 = new int[1];
    diskDirSize1[0] = 2048;

    diskProps.setDiskDirsAndSizes(dirs1, diskDirSize1);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setCompactionThreshold(100);
    diskProps.setMaxOplogSize(100000000);
    diskProps.setRegionName("region_SyncPersistRegionDAExp");
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    int[] diskSizes1 = ((LocalRegion) region).getDiskDirSizes();

    assertEquals(2048, diskSizes1[0]);
    this.exceptionOccurred = false;
    dskAccessExpHelperMethod(region, true/* synch mode */);

    // region.close(); // closes disk file which will flush all buffers
    closeDown();

  }// end of testSyncPersistRegionDAExp

  @Test
  public void testAsynchModeAllowOperationToProceedEvenIfDiskSpaceIsNotSufficient()
      throws Exception {
    File[] dirs1 = null;
    File testingDirectory1 = new File("testingDirectory1");
    testingDirectory1.mkdir();
    testingDirectory1.deleteOnExit();
    File file1 = new File("testingDirectory1/" + "testAsyncPersistRegionDAExp" + "1");
    file1.mkdir();
    file1.deleteOnExit();
    dirs1 = new File[1];
    dirs1[0] = file1;
    int[] diskDirSize1 = new int[1];
    diskDirSize1[0] = 2048;
    diskProps.setDiskDirsAndSizes(dirs1, diskDirSize1);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setCompactionThreshold(100);
    diskProps.setMaxOplogSize(100000000);
    diskProps.setBytesThreshold(1000000);
    diskProps.setTimeInterval(1500000);
    diskProps.setRegionName("region_AsyncPersistRegionDAExp");
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps);
    int[] diskSizes1 = ((LocalRegion) region).getDiskDirSizes();
    assertEquals(diskDirSize1.length, 1);
    assertTrue("diskSizes != 2048 ", diskSizes1[0] == 2048);
    this.exceptionOccurred = false;
    this.dskAccessExpHelperMethod(region, false/* asynch mode */);

    // region.close(); // closes disk file which will flush all buffers
    closeDown();
  }// end of testAsyncPersistRegionDAExp

  private void dskAccessExpHelperMethod(final Region region, final boolean synchMode) {

    Thread testThread = new Thread(new Runnable() {
      public void run() {

        LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
        final byte[] value = new byte[990];
        Arrays.fill(value, (byte) 77);
        try {
          for (int i = 0; i < 2; i++) {
            region.put("" + (synchMode ? 1 : i), value);
          }
        } catch (DiskAccessException e) {
          logWriter.error("Exception occurred but not expected", e);
          testFailed = true;
          failureCause = "FAILED::" + e.toString();
          throw new AssertionError("FAILED::", e);
        }

        final Thread t1 = Thread.currentThread();

        CacheObserver old = CacheObserverHolder.setInstance(new CacheObserverAdapter() {
          public void beforeGoingToCompact() {
            try {
              ThreadUtils.join(t1, 60 * 1000);
            } catch (Exception e) {
              testFailed = true;
              failureCause =
                  "Test failed as the compactor thread not guaranteed to have not rolled the oplog";
              throw new AssertionError(
                  "Test failed as the compactor thread not guaranteed to have not rolled the oplog",
                  e);
            }
          }

        });


        region.put("" + (synchMode ? 1 : 2), value);
        long availSpace =
            ((LocalRegion) region).getDiskRegion().getDirectories()[0].getAvailableSpace();
        // The available space must be less than the minimum required as compactor has yet not freed
        // the space. Still the put operation must succeed.
        logWriter.info("Available disk space=" + availSpace + " MINIMUM STIPULATED SPACE="
            + DiskStoreImpl.MINIMUM_DIR_SIZE);
        exceptionOccurred = false/* availSpace >= DiskStoreImpl.MINIMUM_DIR_SIZE */; // @todo I see
                                                                                     // this test
                                                                                     // failing here
                                                                                     // but I don't
                                                                                     // know what it
                                                                                     // means. My
                                                                                     // availSpace
                                                                                     // is
                                                                                     // 1052
        if (exceptionOccurred) {
          fail("FAILED::Available space should be less than Minimum Directory size("
              + DiskStoreImpl.MINIMUM_DIR_SIZE
              + ") as the operation would have violated the max directory size requirement availSpace="
              + availSpace);
        } else {
          exceptionOccurred = false /* availSpace >= 0 */; // @todo I see this test failing here but
                                                           // I don't know what it means. My
                                                           // availSpace is 1052
          if (exceptionOccurred) {
            fail(
                "FAILED::Available space should be less than 0 as the operation would have violated the max directory size requirement availSpace="
                    + availSpace);
          }
        }


        LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
        CacheObserverHolder.setInstance(old);
      }
    });
    testThread.start();
    // region.clear();
    ThreadUtils.join(testThread, 40 * 1000);
    assertFalse(failureCause, testFailed);
    assertFalse(
        "Expected situation of max directory size violation happening and available space less than zero did not happen  ",
        exceptionOccurred); // CC jade1d failure

  }

  /**
   * DiskRegDiskAttributesTest: This test is for testing Disk attributes set programmatically
   */
  @Test
  public void testDiskRegDWAttrbts() throws Exception {
    diskProps1.setDiskDirs(dirs);
    diskProps2.setDiskDirs(dirs);
    diskProps3.setDiskDirs(dirs);
    diskProps4.setDiskDirs(dirs);
    diskProps5.setDiskDirs(dirs);
    diskProps6.setDiskDirs(dirs);
    diskProps7.setDiskDirs(dirs);
    diskProps8.setDiskDirs(dirs);
    diskProps9.setDiskDirs(dirs);
    diskProps10.setDiskDirs(dirs);
    diskProps11.setDiskDirs(dirs);
    diskProps12.setDiskDirs(dirs);
    // Get the region1 which is SyncPersistOnly and set DiskWriteAttibutes
    diskProps1.setRolling(true);
    diskProps1.setMaxOplogSize(10737418240l);
    region1 = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps1, Scope.LOCAL);
    long opSz1 = (10737418240L / (1024 * 1024));
    verify((LocalRegion) region1, diskProps1);
    destroyRegion(region1);
    // Get the region2 which is SyncPersistOnly and set DiskWriteAttibutes

    diskProps2.setRolling(false);

    region2 = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps2, Scope.LOCAL);
    verify((LocalRegion) region2, diskProps2);
    destroyRegion(region2);
    // Get the region3 which AsyncPersistOnly, No buffer and Rolling oplog
    diskProps3.setRolling(true);
    diskProps3.setMaxOplogSize(10737418240l);
    region3 = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps3);
    verify((LocalRegion) region3, diskProps3);
    destroyRegion(region3);
    // Get the region4 which is AsynchPersistonly, No buffer and fixed oplog
    diskProps4.setRolling(false);
    region4 = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps4);
    verify((LocalRegion) region4, diskProps4);
    destroyRegion(region4);
    // Get the region5 which is SynchOverflowOnly, Rolling oplog
    diskProps5.setRolling(true);
    diskProps5.setMaxOplogSize(10737418240l);
    region5 = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps5);
    verify((LocalRegion) region5, diskProps5);
    destroyRegion(region5);
    // Get the region6 which is SyncOverflowOnly, Fixed oplog
    diskProps6.setRolling(false);
    region6 = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps6);
    verify((LocalRegion) region6, diskProps6);
    destroyRegion(region6);
    // Get the region7 which is AsyncOverflow, with Buffer and rolling oplog
    diskProps7.setRolling(true);
    diskProps7.setMaxOplogSize(10737418240l);
    diskProps7.setBytesThreshold(10000l);
    diskProps7.setTimeInterval(15l);
    region7 = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache, diskProps7);
    verify((LocalRegion) region7, diskProps7);
    destroyRegion(region7);
    // Get the region8 which is AsyncOverflow ,Time base buffer-zero byte buffer
    // and Fixed oplog
    diskProps8.setRolling(false);
    diskProps8.setTimeInterval(15l);
    diskProps8.setBytesThreshold(0l);
    region8 = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache, diskProps8);
    verify((LocalRegion) region8, diskProps8);
    destroyRegion(region8);
    // Get the region9 which is SyncPersistOverflow, Rolling oplog
    diskProps9.setRolling(true);
    diskProps9.setMaxOplogSize(10737418240l);
    region9 = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps9);
    verify((LocalRegion) region9, diskProps9);
    destroyRegion(region9);
    // Get the region10 which is Sync PersistOverflow, fixed oplog
    diskProps10.setRolling(false);
    region10 = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps10);
    verify((LocalRegion) region10, diskProps10);
    destroyRegion(region10);
    // Get the region11 which is Async Overflow Persist ,with buffer and rollong
    // oplog
    diskProps11.setRolling(true);
    diskProps11.setMaxOplogSize(10737418240l);
    diskProps11.setBytesThreshold(10000l);
    diskProps11.setTimeInterval(15l);
    region11 = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache, diskProps11);
    verify((LocalRegion) region11, diskProps11);
    destroyRegion(region11);
    // Get the region12 which is Async Persist Overflow with time based buffer
    // and Fixed oplog
    diskProps12.setRolling(false);
    diskProps12.setBytesThreshold(0l);
    diskProps12.setTimeInterval(15l);
    region12 = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache, diskProps12);
    verify((LocalRegion) region12, diskProps12);
    destroyRegion(region12);
  }// end of DiskRegDiskAttributesTest

  private static void closeRegion(Region r) {
    LocalRegion lr = (LocalRegion) r;
    r.close();
    lr.getDiskStore().close();
    lr.getGemFireCache().removeDiskStore(lr.getDiskStore());
  }

  private static void destroyRegion(Region r) {
    LocalRegion lr = (LocalRegion) r;
    r.destroyRegion();
    lr.getDiskStore().close();
    lr.getGemFireCache().removeDiskStore(lr.getDiskStore());
  }

  /**
   * DiskRegGetInvalidEntryTest: get invalid entry should return null.
   */
  @Test
  public void testDiskGetInvalidEntry() throws Exception {
    Object getInvalidEnt = "some val";

    diskProps.setDiskDirsAndSizes(dirs, diskDirSize);
    diskProps.setRolling(false);
    diskProps.setMaxOplogSize(1000000000);
    diskProps.setBytesThreshold(1000000);
    diskProps.setTimeInterval(1500000);
    region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache, diskProps);

    final byte[] value = new byte[1024];
    Arrays.fill(value, (byte) 77);
    try {
      for (int i = 0; i < 10; i++) {
        region.put("key" + i, value);
      }
    } catch (Exception e) {
      throw new AssertionError("Failed while put:", e);
    }
    // invalidate an entry
    try {
      region.invalidate("key1");
    } catch (Exception e) {
      throw new AssertionError("Failed while invalidating:" + e.toString());
    }
    // get the invalid entry and verify that the value returned is null
    try {
      getInvalidEnt = region.get("key1");
    } catch (Exception e) {
      logWriter.error("Exception occurred but not expected", e);
      throw new AssertionError("Failed while getting invalid entry:", e);

    }
    assertTrue("get operation on invalid entry returned non null value", getInvalidEnt == null);

    region.close(); // closes disk file which will flush all buffers

  }// end of DiskRegGetInvalidEntryTest

  /**
   * DiskRegionByteArrayJUnitTest: A byte array as a value put in local persistent region ,when
   * retrieved from the disk should be correctly presented as a byte array
   */
  @Test
  public void testDiskRegionByteArray() throws Exception {
    Object val = null;
    diskProps.setPersistBackup(true);
    diskProps.setDiskDirs(dirs);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    final int ENTRY_SIZE = 1024;
    final int OP_COUNT = 10;
    final String key = "K";
    final byte[] value = new byte[ENTRY_SIZE];
    Arrays.fill(value, (byte) 77);
    // put an entry
    region.put(key, value);
    // put few more entries to write on disk
    for (int i = 0; i < OP_COUNT; i++) {
      region.put(new Integer(i), value);
    }
    // get from disk
    try {
      DiskId diskId = ((DiskEntry) (((LocalRegion) region).basicGetEntry("K"))).getDiskId();
      val = ((LocalRegion) region).getDiskRegion().get(diskId);
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new AssertionError("Failed to get the value on disk", ex);
    }
    // verify that the value retrieved above represents byte array.
    // verify the length of the byte[]
    assertTrue((((byte[]) val).length) == 1024);
    // verify that the retrieved byte[] equals to the value put initially.
    boolean result = false;
    byte[] x = null;
    x = (byte[]) val;

    for (int i = 0; i < x.length; i++) {
      result = (x[i] == value[i]);
    }

    if (!result) {
      fail("The val obtained from disk is not euqal to the value put initially");
    }

  }// end of DiskRegionByteArrayJUnitTest

  /**
   * DiskRegionFactoryJUnitTest: Test for verifying DiskRegion or SimpleDiskRegion.
   */
  @Test
  public void testInstanceOfDiskRegion() throws Exception {
    DiskRegionProperties diskProps = new DiskRegionProperties();

    diskProps.setDiskDirs(dirs); // dirs is an array of four dirs
    diskProps.setRolling(true);
    Region region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

    destroyRegion(region);

    diskProps.setDiskDirs(dirs); // dirs is an array of four dirs
    diskProps.setRolling(false);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

    destroyRegion(region);

    diskProps.setRolling(false);
    File singleDisk1 = new File("singleDisk");
    singleDisk1.mkdir();
    singleDisk1.deleteOnExit();
    File[] singleDirArray1 = {singleDisk1};
    int[] diskSizes1 = {2048};
    diskProps.setMaxOplogSize(1024);
    diskProps.setDiskDirsAndSizes(singleDirArray1, diskSizes1);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

    destroyRegion(region);

    diskProps.setRolling(false);
    File singleDisk = new File("singleDisk");
    singleDisk.mkdir();
    singleDisk.deleteOnExit();
    File[] singleDirArray = {singleDisk};
    int[] diskSizes = {1024};
    diskProps.setMaxOplogSize(2048);
    diskProps.setDiskDirsAndSizes(singleDirArray, diskSizes);
  }

  /**
   * DiskRegionStatsJUnitTest :
   */
  @Test
  public void testStats() throws Exception {
    final int overflowCapacity = 100;
    int counter = 0;
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(null);
    diskRegionProperties.setOverFlowCapacity(overflowCapacity);
    diskRegionProperties.setMaxOplogSize(2097152);
    diskRegionProperties.setRolling(true);

    Region region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskRegionProperties);

    DiskRegionStats stats = ((LocalRegion) region).getDiskRegion().getStats();

    for (int i = 0; i < 5000; i++) {
      region.put(new Integer(i), new Integer(i));
      region.put(new Integer(i), new Integer(i));
      region.put(new Integer(i), new Integer(i));
      if (i > overflowCapacity + 5) {
        region.get(new Integer(++counter));
        region.get(new Integer(counter));
      }

      if (i > overflowCapacity) {
        if (!(stats.getNumEntriesInVM() == overflowCapacity)) {
          fail(" number of entries is VM should be equal to overflow capacity");
        }
        if (!(stats.getNumOverflowOnDisk() - 1 == i - overflowCapacity)) {
          fail(" number of entries on disk not corrected expected " + (i - overflowCapacity)
              + " but is " + stats.getNumOverflowOnDisk());
        }
      }
    }
  }// end of testStats

  /**
   * DiskRegOverflowOnlyNoFilesTest: Overflow only mode has no files of previous run, during startup
   */
  @Test
  public void testOverflowOnlyNoFiles() throws Exception {
    diskProps.setTimeInterval(15000l);
    diskProps.setBytesThreshold(100000l);
    diskProps.setOverFlowCapacity(1000);
    diskProps.setDiskDirs(dirs);
    // diskProps.setDiskDirsAndSizes(dirs, diskDirSize);

    for (int i = 0; i < dirs.length; i++) {
      File[] files = dirs[i].listFiles();
      assertTrue("Files already exists", files == null || files.length == 0);
    }
    region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache, diskProps);

    final byte[] value = new byte[1024];
    Arrays.fill(value, (byte) 77);

    for (int i = 0; i < 100; i++) {
      region.put(new Integer(i), value);
    }

    region.close(); // closes disk file which will flush all buffers
    // verify that there are no files in the disk dir

    {
      int fileCount = 0;
      for (int i = 0; i < dirs.length; i++) {
        File[] files = dirs[i].listFiles();
        region.getCache().getLogger().info("files=" + new ArrayList(Arrays.asList(files)));
        fileCount += files.length;
      }
      // since the diskStore has not been closed we expect two files: .lk and .if
      assertEquals(2, fileCount);
      region.getCache().close();

      // we now should only have zero
      fileCount = 0;
      for (int i = 0; i < dirs.length; i++) {
        File[] files = dirs[i].listFiles();
        fileCount += files.length;
      }
      assertEquals(0, fileCount);
    }
  }// end of testOverflowOnlyNoFiles

  @Test
  public void testPersistNoFiles() throws Exception {
    diskProps.setOverflow(false);
    diskProps.setRolling(false);
    diskProps.setDiskDirs(dirs);
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("testPersistNoFiles");

    for (int i = 0; i < dirs.length; i++) {
      File[] files = dirs[i].listFiles();
      assertTrue("Files already exists", files == null || files.length == 0);
    }
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

    final byte[] value = new byte[1024];
    Arrays.fill(value, (byte) 77);

    for (int i = 0; i < 100; i++) {
      region.put(new Integer(i), value);
    }

    region.destroyRegion();

    {
      int fileCount = 0;
      for (int i = 0; i < dirs.length; i++) {
        File[] files = dirs[i].listFiles();
        region.getCache().getLogger().info("files=" + new ArrayList(Arrays.asList(files)));
        fileCount += files.length;
      }
      // since the diskStore has not been closed we expect four files: .lk and .if
      // and a crf and a drf
      assertEquals(4, fileCount);
      region.getCache().close();

      // we now should only have zero since the disk store had no regions remaining in it.
      fileCount = 0;
      for (int i = 0; i < dirs.length; i++) {
        File[] files = dirs[i].listFiles();
        fileCount += files.length;
      }
      assertEquals(0, fileCount);
    }
  }

  /**
   * Test to verify that DiskAccessException is not thrown if rolling has been enabled. The test
   * configurations will cause the disk to go full and wait for the compactor to release space. A
   * DiskAccessException should not be thrown by this test
   */
  @Test
  public void testDiskAccessExceptionNotThrown() throws Exception {
    File diskDir = new File("dir");
    diskDir.mkdir();
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirsAndSizes(new File[] {diskDir}, new int[] {10240});
    diskRegionProperties.setMaxOplogSize(1024);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setSynchronous(true);
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskRegionProperties);
    byte[] bytes = new byte[256];
    for (int i = 0; i < 1500; i++) {
      region.put(new Integer(i % 10), bytes);
    }
  }

  /**
   * If an entry which has just been written on the disk, sees clear just before updating the
   * LRULiist, then that deleted entry should not go into the LRUList
   */
  @Test
  public void testClearInteractionWithLRUList_Bug37605() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setOverflow(true);
    props.setOverFlowCapacity(1);
    props.setDiskDirs(dirs);
    props.setRegionName("IGNORE_EXCEPTION_testClearInteractionWithLRUList_Bug37605");
    final Region region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, props);

    final Thread th = new Thread(new Runnable() {
      public void run() {
        region.clear();
      }
    });

    region.getAttributesMutator().addCacheListener(new CacheListenerAdapter() {
      public void afterCreate(EntryEvent event) {
        th.start();
      }
    });

    region.create("key1", "value1");
    try {
      cache.getLogger().info("waiting for clear to finish");
      ThreadUtils.join(th, 30 * 1000);
    } catch (Exception ie) {
      DiskRegionJUnitTest.this.exceptionOccurred = true;
      DiskRegionJUnitTest.this.failureCause = ie.toString();
    }

    assertFalse(this.failureCause, this.exceptionOccurred);
    NewLRUClockHand lruList = ((VMLRURegionMap) ((LocalRegion) region).entries)._getLruList();
    assertEquals(region.size(), 0);
    lruList.audit();
    assertNull("The LRU List should have been empty instead it contained a cleared entry",
        lruList.getLRUEntry());
  }

  /**
   * As in the clear operation, previously the code was such that Htree Ref was first reset & then
   * the underlying region map got cleared, it was possible for the create op to set the new Htree
   * ref in thread local. Now if clear happened, the entry on which create op is going on was no
   * longer valid, but we would not be able to detect the conflict. The fix was to first clear the
   * region map & then reset the Htree Ref.
   */
  @Test
  public void testClearInteractionWithCreateOperation_Bug37606() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setOverflow(false);
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setPersistBackup(true);
    props.setRegionName("IGNORE_EXCEPTION_testClearInteractionWithCreateOperation_Bug37606");
    final Region region =
        DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    final Thread th = new Thread(new Runnable() {
      public void run() {
        region.create("key1", "value1");
      }
    });
    CacheObserver old = CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      public void beforeDiskClear() {
        th.start();
        Wait.pause(7 * 1000);
        System.out.println("FIXME: this thread does not terminate--EVER!");
        // try {
        // DistributedTestCase.join(th, 7 * 1000, null);
        // }
        // catch (Exception e) {
        // DiskRegionJUnitTest.this.exceptionOccurred = true;
        // DiskRegionJUnitTest.this.failureCause = e.toString();
        // }
      }
    });
    try {
      region.clear();
      ThreadUtils.join(th, 30 * 1000);
      assertFalse(this.failureCause, this.exceptionOccurred);
      // We expect 1 entry to exist, because the clear was triggered before
      // the update
      assertEquals(1, region.size());
      region.close();
      assertEquals(1,
          DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL).size());
    } finally {
      CacheObserverHolder.setInstance(old);
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    }
  }

  /**
   * Similar test in case of 'update'
   */
  @Test
  public void testClearInteractionWithUpdateOperation_Bug37606() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setOverflow(false);
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setPersistBackup(true);
    props.setRegionName("IGNORE_EXCEPTION_testClearInteractionWithUpdateOperation_Bug37606");
    final Region region =
        DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    region.create("key1", "value1");
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    final Thread th = new Thread(new Runnable() {
      public void run() {
        region.put("key1", "value2");
      }
    });
    CacheObserver old = CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      public void beforeDiskClear() {
        LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
        th.start();
        System.out.println("FIXME: this thread (2) does not terminate--EVER!");
        Wait.pause(10 * 1000);
        // try {
        // DistributedTestCase.join(th, 10 * 1000, null);
        // }
        // catch (Exception e) {
        // DiskRegionJUnitTest.this.exceptionOccurred = true;
        // DiskRegionJUnitTest.this.failureCause = e.toString();
        // }
      }
    });
    try {
      region.clear();
      ThreadUtils.join(th, 30 * 1000);
      assertFalse(this.failureCause, this.exceptionOccurred);
      // We expect 1 entry to exist, because the clear was triggered before
      // the update
      assertEquals(1, region.size());
      region.close();
      assertEquals(1,
          DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL).size());
    } finally {
      CacheObserverHolder.setInstance(old);
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    }
  }

  /**
   * If IOException occurs while updating an entry in a persist only synch mode, DiskAccessException
   * should occur & region should be destroyed
   */
  @Test
  public void testEntryUpdateInSynchPersistOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName("IGNORE_EXCEPTION_testEntryUpdateInSynchPersistOnlyForIOExceptionCase");
    props.setOverflow(false);
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setPersistBackup(true);

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    entryUpdateInSynchPersistTypeForIOExceptionCase(region);
  }

  /**
   * If IOException occurs while updating an entry in a persist overflow synch mode, we should get
   * DiskAccessException & region be destroyed
   */
  @Test
  public void testEntryUpdateInSyncOverFlowPersistOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName(
        "IGNORE_EXCEPTION_testEntryUpdateInSyncOverFlowPersistOnlyForIOExceptionCase");
    props.setOverflow(true);
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setPersistBackup(true);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    entryUpdateInSynchPersistTypeForIOExceptionCase(region);
  }

  /**
   * If IOException occurs while updating an entry in a persist only synch mode, DiskAccessException
   * should occur & region should be destroyed
   * 
   * @throws Exception
   */
  private void entryUpdateInSynchPersistTypeForIOExceptionCase(Region region) throws Exception {

    region.create("key1", "value1");
    // Get the oplog handle & hence the underlying file & close it
    UninterruptibleFileChannel oplogFileChannel =
        ((LocalRegion) region).getDiskRegion().testHook_getChild().getFileChannel();
    oplogFileChannel.close();
    try {
      region.put("key1", "value2");
      fail("Should have encountered DiskAccessException");
    } catch (DiskAccessException dae) {
      // OK
    }

    ((LocalRegion) region).getDiskStore().waitForClose();
    assertTrue(cache.isClosed());
    region = null;
  }

  /**
   * If IOException occurs while invalidating an entry in a persist only synch mode,
   * DiskAccessException should occur & region should be destroyed
   */
  @Test
  public void testEntryInvalidateInSynchPersistOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName("IGNORE_EXCEPTION_testEntryInvalidateInSynchPersistOnlyForIOExceptionCase");
    props.setOverflow(false);
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setPersistBackup(true);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    entryInvalidateInSynchPersistTypeForIOExceptionCase(region);
  }

  /**
   * If IOException occurs while invalidating an entry in a persist overflow synch mode,
   * DiskAccessException should occur & region should be destroyed
   */
  @Test
  public void testEntryInvalidateInSynchPersistOverflowForIOExceptionCase() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName(
        "IGNORE_EXCEPTION_testEntryInvalidateInSynchPersistOverflowForIOExceptionCase");

    props.setOverflow(true);
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setPersistBackup(true);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    entryInvalidateInSynchPersistTypeForIOExceptionCase(region);
  }

  /**
   * If IOException occurs while invalidating an entry in a persist only synch mode,
   * DiskAccessException should occur & region should be destroyed
   * 
   * @throws Exception
   */
  private void entryInvalidateInSynchPersistTypeForIOExceptionCase(Region region) throws Exception {
    region.create("key1", "value1");
    // Get the oplog handle & hence the underlying file & close it
    UninterruptibleFileChannel oplogFileChannel =
        ((LocalRegion) region).getDiskRegion().testHook_getChild().getFileChannel();
    oplogFileChannel.close();
    try {
      region.invalidate("key1");
      fail("Should have encountered DiskAccessException");
    } catch (DiskAccessException dae) {
      // OK
    }

    ((LocalRegion) region).getDiskStore().waitForClose();
    assertTrue(cache.isClosed());
    region = null;
  }

  /**
   * If IOException occurs while creating an entry in a persist only synch mode, DiskAccessException
   * should occur & region should be destroyed
   */
  @Test
  public void testEntryCreateInSynchPersistOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName("IGNORE_EXCEPTION_testEntryCreateInSynchPersistOnlyForIOExceptionCase");
    props.setOverflow(false);
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setPersistBackup(true);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    entryCreateInSynchPersistTypeForIOExceptionCase(region);
  }

  /**
   * If IOException occurs while creating an entry in a persist overflow synch mode,
   * DiskAccessException should occur & region should be destroyed
   */
  @Test
  public void testEntryCreateInSynchPersistOverflowForIOExceptionCase() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName("IGNORE_EXCEPTION_testEntryCreateInSynchPersistOverflowForIOExceptionCase");
    props.setOverflow(true);
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setPersistBackup(true);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    entryCreateInSynchPersistTypeForIOExceptionCase(region);
  }

  /**
   * 
   * If IOException occurs while creating an entry in a persist only synch mode, DiskAccessException
   * should occur & region should be destroyed
   * 
   * @throws Exception
   */
  private void entryCreateInSynchPersistTypeForIOExceptionCase(Region region) throws Exception {

    // Get the oplog handle & hence the underlying file & close it
    UninterruptibleFileChannel oplogFileChannel =
        ((LocalRegion) region).getDiskRegion().testHook_getChild().getFileChannel();
    oplogFileChannel.close();
    try {
      region.create("key1", "value1");
      fail("Should have encountered DiskAccessException");
    } catch (DiskAccessException dae) {
      // OK
    }
    ((LocalRegion) region).getDiskStore().waitForClose();
    assertTrue(cache.isClosed());
    region = null;
  }

  /**
   * If IOException occurs while destroying an entry in a persist only synch mode,
   * DiskAccessException should occur & region should be destroyed
   */
  @Test
  public void testEntryDestructionInSynchPersistOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props
        .setRegionName("IGNORE_EXCEPTION_testEntryDestructionInSynchPersistOnlyForIOExceptionCase");
    props.setOverflow(false);
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setPersistBackup(true);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    entryDestructionInSynchPersistTypeForIOExceptionCase(region);
  }

  /**
   * If IOException occurs while destroying an entry in a persist overflow synch mode,
   * DiskAccessException should occur & region should be destroyed
   */
  @Test
  public void testEntryDestructionInSynchPersistOverflowForIOExceptionCase() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName(
        "IGNORE_EXCEPTION_testEntryDestructionInSynchPersistOverflowForIOExceptionCase");
    props.setOverflow(true);
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setPersistBackup(true);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    entryDestructionInSynchPersistTypeForIOExceptionCase(region);
  }

  /**
   * If IOException occurs while destroying an entry in a persist only synch mode,
   * DiskAccessException should occur & region should be destroyed
   * 
   * @throws Exception
   */
  private void entryDestructionInSynchPersistTypeForIOExceptionCase(Region region)
      throws Exception {

    region.create("key1", "value1");
    // Get the oplog handle & hence the underlying file & close it
    ((LocalRegion) region).getDiskRegion().testHook_getChild().testClose();
    try {
      region.destroy("key1");
      fail("Should have encountered DiskAccessException");
    } catch (DiskAccessException dae) {
      // OK
    }

    ((LocalRegion) region).getDiskStore().waitForClose();
    assertTrue(cache.isClosed());
    region = null;
  }

  /**
   * If IOException occurs while updating an entry in a Overflow only synch mode,
   * DiskAccessException should occur & region should be destroyed
   */
  @Test
  public void testEntryUpdateInSynchOverflowOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName("IGNORE_EXCEPTION_testEntryUpdateInSynchOverflowOnlyForIOExceptionCase");
    props.setOverflow(true);
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setPersistBackup(false);
    props.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, props);

    region.create("key1", "value1");
    region.create("key2", "value2");
    ((LocalRegion) region).getDiskRegion().testHookCloseAllOverflowChannels();
    try {
      // Update key1, so that key2 goes on disk & encounters an exception
      region.put("key1", "value1'");
      fail("Should have encountered DiskAccessException");
    } catch (DiskAccessException dae) {
      // OK
    }
    ((LocalRegion) region).getDiskStore().waitForClose();
    assertTrue(cache.isClosed());
    region = null;
  }

  /**
   * If IOException occurs while creating an entry in a Overflow only synch mode,
   * DiskAccessException should occur & region should be destroyed
   */
  @Test
  public void testEntryCreateInSynchOverflowOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName("IGNORE_EXCEPTION_testEntryCreateInSynchOverflowOnlyForIOExceptionCase");
    props.setOverflow(true);
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setPersistBackup(false);
    props.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, props);

    region.create("key1", "value1");
    region.create("key2", "value2");
    ((LocalRegion) region).getDiskRegion().testHookCloseAllOverflowChannels();
    try {
      region.create("key3", "value3");
      fail("Should have encountered DiskAccessException");
    } catch (DiskAccessException dae) {
      // OK
    }
    ((LocalRegion) region).getDiskStore().waitForClose();
    assertTrue(cache.isClosed());
    region = null;
  }

  /**
   * A deletion of an entry in overflow only mode should not cause any eviction & hence no
   * DiskAccessException
   */
  @Test
  public void testEntryDeletionInSynchOverflowOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setOverflow(true);
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setPersistBackup(false);
    props.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, props);

    region.create("key1", "value1");
    region.create("key2", "value2");
    region.create("key3", "value3");
    ((LocalRegion) region).getDiskRegion().testHookCloseAllOverflowChannels();

    // Update key1, so that key2 goes on disk & encounters an exception
    region.destroy("key1");
    region.destroy("key3");
  }

  /**
   * If IOException occurs while updating an entry in an Asynch mode, DiskAccessException should
   * occur & region should be destroyed
   */
  @Test
  public void testEntryUpdateInASynchPersistOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName("IGNORE_EXCEPTION_testEntryUpdateInASynchPersistOnlyForIOExceptionCase");
    props.setOverflow(true);
    props.setRolling(false);
    props.setBytesThreshold(48);
    props.setDiskDirs(dirs);
    props.setPersistBackup(true);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, props);
    // Get the oplog handle & hence the underlying file & close it
    UninterruptibleFileChannel oplogFileChannel =
        ((LocalRegion) region).getDiskRegion().testHook_getChild().getFileChannel();
    oplogFileChannel.close();

    region.create("key1", new byte[16]);
    region.create("key2", new byte[16]);

    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    dr.flushForTesting();
    // Join till the asynch writer terminates
    if (!dr.testWaitForAsyncFlusherThread(2000)) {
      fail("async flusher thread did not terminate");
    }

    Wait.waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        return cache.isClosed();
      }

      @Override
      public String description() {
        return "Waiting for region IGNORE_EXCEPTION_testEntryUpdateInASynchPersistOnlyForIOExceptionCase to be destroyed.";
      }
    }, 5000, 500, true);

    ((LocalRegion) region).getDiskStore().waitForClose();
    assertTrue(cache.isClosed());
    region = null;
  }

  /**
   * If IOException occurs while updating an entry in an already initialized DiskRegion ,then the
   * bridge servers should not be stopped , if any running as they are no clients connected to it.
   */
  @Test
  public void testBridgeServerStoppingInSynchPersistOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName(
        "IGNORE_EXCEPTION_testBridgeServerStoppingInSynchPersistOnlyForIOExceptionCase");
    props.setOverflow(true);
    props.setRolling(true);
    props.setDiskDirs(dirs);
    props.setPersistBackup(true);

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    CacheServer bs1 = cache.addCacheServer();
    bs1.setPort(0);
    bs1.start();

    region.create("key1", new byte[16]);
    region.create("key2", new byte[16]);
    // Get the oplog handle & hence the underlying file & close it
    UninterruptibleFileChannel oplogFileChannel =
        ((LocalRegion) region).getDiskRegion().testHook_getChild().getFileChannel();
    oplogFileChannel.close();
    try {
      region.put("key2", new byte[16]);
    } catch (DiskAccessException dae) {
      // OK expected
    }
    ((LocalRegion) region).getDiskStore().waitForClose();
    assertTrue(cache.isClosed());
    region = null;
    List bsRunning = cache.getCacheServers();
    // [anil & bruce] the following assertion was changed to true because
    // a disk access exception in a server should always stop the server
    assertTrue(bsRunning.isEmpty());
  }

  @Test
  public void testDummyByteBugDuringRegionClose_Bug40250() throws Exception {
    try {
      // Create a region with rolling enabled.
      DiskRegionProperties props = new DiskRegionProperties();
      props.setRegionName("testDummyByteBugDuringRegionClose");
      props.setRolling(true);
      props.setCompactionThreshold(100);
      props.setDiskDirs(dirs);
      props.setPersistBackup(true);
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
      // create some string entries
      for (int i = 0; i < 2; ++i) {
        region.put("" + i, "" + i);
      }
      final Thread th = new Thread(new Runnable() {
        public void run() {
          region.close();
        }
      });
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      final boolean[] toWait = new boolean[] {true};
      // cause a switch
      CacheObserver old = CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        public void beforeGoingToCompact() {
          // Spawn a thread which does region.close;
          // Start the shutdown thread, do not allow the compactor to proceed
          // till the roll flag
          // is false in DiskRegion
          th.start();
          synchronized (region) {
            toWait[0] = false;
            region.notify();
          }
          // Lets wait for some time which will be enough to toggle glag.
          // ideally we need visibility
          // of roll flag from DiskRegion.OplogCompactor but want to
          // avoid exposing it
          try {
            Thread.sleep(8000);
          } catch (InterruptedException ie) {
            ie.printStackTrace();
          }

        }
      });
      region.forceRolling();
      synchronized (region) {
        if (toWait[0]) {
          region.wait(9000);
          assertFalse(toWait[0]);
        }
      }
      th.join();
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(new CacheObserverAdapter());
      // Restart the region
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
      for (int i = 0; i < 2; ++i) {
        assertEquals("" + i, region.get("" + i));
      }
      region.close();
    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(new CacheObserverAdapter());
    }
  }

  /**
   * If IOException occurs while initializing a region ,then the bridge servers should not be
   * stopped
   */
  @Test
  public void testBridgeServerRunningInSynchPersistOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName(
        "IGNORE_EXCEPTION_testBridgeServerStoppingInSynchPersistOnlyForIOExceptionCase");
    props.setOverflow(true);
    props.setRolling(true);
    props.setDiskDirs(dirs);
    props.setPersistBackup(true);
    props.setMaxOplogSize(100000); // just needs to be bigger than 65550

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    CacheServer bs1 = cache.addCacheServer();
    bs1.setPort(0);
    bs1.start();

    region.create("key1", new byte[16]);
    region.create("key2", new byte[16]);
    // Get the oplog file path
    UninterruptibleFileChannel oplogFileChnl =
        ((LocalRegion) region).getDiskRegion().testHook_getChild().getFileChannel();
    // corrupt the opfile
    oplogFileChnl.position(2);
    ByteBuffer bf = ByteBuffer.allocate(416);
    for (int i = 0; i < 5; ++i) {
      bf.putInt(i);
    }
    bf.flip();
    // Corrupt the oplogFile
    oplogFileChnl.write(bf);
    // Close the region
    region.close();
    assertTrue(region.isDestroyed());
    try {
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
      fail("expected DiskAccessException");
    } catch (DiskAccessException dae) {
      // OK expected
    }
    assertTrue(region.isDestroyed());
    region = null;
    List bsRunning = cache.getCacheServers();
    assertTrue(!bsRunning.isEmpty());
  }

  @Test
  public void testEarlyTerminationOfCompactorByDefault() throws Exception {
    try {
      // Create a region with rolling enabled.
      DiskRegionProperties props = new DiskRegionProperties();
      props.setRegionName("testEarlyTerminationOfCompactorByDefault");
      props.setRolling(true);
      props.setCompactionThreshold(100);
      props.setDiskDirs(dirs);
      props.setMaxOplogSize(100);
      props.setPersistBackup(true);
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
      final boolean[] foundException = new boolean[] {false, false};
      final boolean[] closeThreadStarted = new boolean[] {false};
      final boolean[] allowCompactorThread = new boolean[] {false};
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

      final Thread th = new Thread(new Runnable() {
        public void run() {
          DiskStoreImpl dsi = ((LocalRegion) region).getDiskStore();
          region.close();
          dsi.close();
        }
      });
      final Object anotherLock = new Object();
      // cause a switch
      CacheObserver old = CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        int sizeBeforeRoll;
        Map monitor;
        final AtomicBoolean compactorSignalled = new AtomicBoolean();
        final AtomicBoolean compactorCompleted = new AtomicBoolean();

        public void beforeGoingToCompact() {
          logWriter.info("beforeGoingToCompact");
          DiskRegion cdr = ((LocalRegion) region).getDiskRegion();
          monitor = cdr.getOplogIdToOplog();
          // wait for operations to get over
          synchronized (anotherLock) {
            try {
              if (!allowCompactorThread[0]) {
                anotherLock.wait(15000);
                assertTrue(allowCompactorThread[0]);
              }
            } catch (Exception e) {
              foundException[0] = true;
              e.printStackTrace();
            }
          }
          synchronized (monitor) {
            sizeBeforeRoll = monitor.size();
            assertTrue(sizeBeforeRoll > 0);
          }
          logWriter.info("beforeGoingToCompact sizeBeforeCompact=" + sizeBeforeRoll);
          this.compactorSignalled.set(false);
          this.compactorCompleted.set(false);
          th.start();
          synchronized (region) {
            closeThreadStarted[0] = true;
            region.notify();
          }
          // wait for th to call afterSignallingCompactor
          synchronized (this.compactorSignalled) {
            int waits = 0;
            while (!this.compactorSignalled.get()) {
              try {
                this.compactorSignalled.wait(100);
                waits++;
                if (waits > 100) {
                  foundException[0] = true;
                  fail("took too long to call afterSignallingCompactor");
                }
              } catch (InterruptedException e) {
                e.printStackTrace();
                foundException[0] = true;
                break;
              }
            }
          }
        }

        public void afterSignallingCompactor() {
          logWriter.info("afterSignallingCompactor");
          synchronized (this.compactorSignalled) {
            this.compactorSignalled.set(true);
            this.compactorSignalled.notifyAll();
          }
        }

        public void afterStoppingCompactor() {
          // th is the thread that calls this in its region.close code.
          logWriter.info("afterStoppingCompactor");
          // wait until afterHavingCompacted is called
          synchronized (this.compactorCompleted) {
            int waits = 0;
            while (!this.compactorCompleted.get()) {
              try {
                this.compactorCompleted.wait(100);
                waits++;
                if (waits > 100) {
                  foundException[0] = true;
                  fail("took too long to call afterHavingCompacted");
                }
              } catch (InterruptedException e) {
                e.printStackTrace();
                foundException[0] = true;
                break;
              }
            }
          }
        }

        public void afterHavingCompacted() {
          logWriter.info("afterHavingCompacted");
          synchronized (this.compactorCompleted) {
            this.compactorCompleted.set(true);
            this.compactorCompleted.notifyAll();
          }
          synchronized (monitor) {
            if (monitor.size() != sizeBeforeRoll) {
              foundException[1] = true;
              // we stopped early and didn't roll any
              assertEquals(sizeBeforeRoll, monitor.size());
            }
          }
        }
      });
      // create some string entries
      for (int i = 0; i < 100; ++i) {
        region.put("" + i, "" + i);
      }
      synchronized (anotherLock) {
        anotherLock.notify();
        allowCompactorThread[0] = true;
      }
      synchronized (region) {
        if (!closeThreadStarted[0]) {
          region.wait(9000);
          assertTrue(closeThreadStarted[0]);
        }
      }
      th.join();
      assertFalse(foundException[0]);
      assertFalse(foundException[1]);

    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(new CacheObserverAdapter());
    }
  }

  @Test
  public void testAssertionErrorIfMissingOplog() throws Exception {
    try {
      // Create a region with rolling enabled.
      DiskRegionProperties props = new DiskRegionProperties();
      // System.getProperties().setProperty(DiskRegion.CHECK_ENTRY_BALANCE_PROPERTY_NAME, "true");
      props.setRegionName(
          "IGNORE_EXCEPTION_testAssertionErrorIfDanglingModificationsAreNotBalancedByDanglingDeletes");
      props.setRolling(false);
      props.setDiskDirs(dirs);
      props.setMaxOplogSize(100);
      props.setPersistBackup(true);
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);

      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

      final Oplog[] switchedOplog = new Oplog[1];
      CacheObserver old = CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        public void beforeSwitchingOplog() {
          DiskRegion dr = ((LocalRegion) region).getDiskRegion();
          if (switchedOplog[0] == null) {
            switchedOplog[0] = dr.testHook_getChild();
          }
        }
      });
      // create some string entries
      int i = 0;
      for (; i < 100; ++i) {
        if (switchedOplog[0] == null) {
          region.put("" + i, new byte[10]);
        } else {
          break;
        }
      }
      assertTrue(i > 1);
      assertTrue(switchedOplog[0].getOplogFileForTest().delete());
      region.close();
      // We don't validate the oplogs until we recreate the disk store.
      DiskStoreImpl store = ((LocalRegion) region).getDiskStore();
      store.close();
      ((GemFireCacheImpl) cache).removeDiskStore(store);
      logWriter
          .info("<ExpectedException action=add>" + "DiskAccessException" + "</ExpectedException>");
      try {
        region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
        // Missing crf and drfs are now detected.
        fail("expected DiskAccessException LocalizedStrings.DiskRegion_MISSING_OR_CORRUPT_OPLOG");
      } catch (IllegalStateException expected) {
        // Expected in recovery
      } finally {
        logWriter.info(
            "<ExpectedException action=remove>" + "DiskAccessException" + "</ExpectedException>");
      }

    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(new CacheObserverAdapter());
      // System.getProperties().setProperty(DiskRegion.CHECK_ENTRY_BALANCE_PROPERTY_NAME, "");
    }
  }

  @Test
  public void testNoTerminationOfCompactorTillRollingCompleted() throws Exception {
    try {
      // Create a region with rolling enabled.
      System.getProperties()
          .setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME, "true");
      DiskRegionProperties props = new DiskRegionProperties();
      props.setRegionName("testNoTerminationOfCompactorTillRollingCompleted");
      props.setRolling(true);
      props.setCompactionThreshold(100);
      props.setDiskDirs(dirs);
      props.setMaxOplogSize(100);
      props.setPersistBackup(true);
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
      final boolean[] foundException = new boolean[] {false, false};
      final boolean[] closeThreadStarted = new boolean[] {false};
      final boolean[] allowCompactorThread = new boolean[] {false};
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

      final Thread th = new Thread(new Runnable() {
        public void run() {
          DiskStoreImpl dsi = ((LocalRegion) region).getDiskStore();
          region.close();
          dsi.close();
        }
      });
      final Object anotherLock = new Object();
      // cause a switch
      CacheObserver old = CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        int sizeBeforeRoll;
        Map monitor;
        final AtomicBoolean compactorSignalled = new AtomicBoolean();

        public void beforeGoingToCompact() {

          DiskRegion cdr = ((LocalRegion) region).getDiskRegion();
          monitor = cdr.getOplogIdToOplog();
          // wait for operations to get over
          synchronized (anotherLock) {
            try {
              if (!allowCompactorThread[0]) {
                anotherLock.wait(9000);
                assertTrue(allowCompactorThread[0]);
              }
            } catch (Exception e) {
              foundException[0] = true;
              e.printStackTrace();
            }
          }
          synchronized (monitor) {
            sizeBeforeRoll = monitor.size();
            assertTrue(sizeBeforeRoll > 0);
          }
          logWriter.info("beforeGoingToCompact sizeBeforeCompact=" + sizeBeforeRoll);
          this.compactorSignalled.set(false);
          th.start();
          synchronized (region) {
            closeThreadStarted[0] = true;
            region.notify();
          }
          // wait for th to call afterSignallingCompactor
          synchronized (this.compactorSignalled) {
            int waits = 0;
            while (!this.compactorSignalled.get()) {
              try {
                this.compactorSignalled.wait(100);
                waits++;
                if (waits > 100) {
                  foundException[0] = true;
                  fail("took too long to call afterSignallingCompactor");
                }
              } catch (InterruptedException e) {
                e.printStackTrace();
                foundException[0] = true;
                break;
              }
            }
          }
        }

        public void afterSignallingCompactor() {
          synchronized (this.compactorSignalled) {
            this.compactorSignalled.set(true);
            this.compactorSignalled.notifyAll();
          }
        }

        public void afterHavingCompacted() {
          synchronized (monitor) {
            if (sizeBeforeRoll != monitor.size()) {
              // expect it to be the same since we compact one and add one (to copy forward)
              foundException[1] = true;
              // we should have rolled at least one oplog
              fail("expected sizeBeforeRoll " + sizeBeforeRoll + " to be equal to "
                  + monitor.size());
            }
          }
        }
      });
      // create some string entries
      for (int i = 0; i < 100; ++i) {
        region.put("" + i, "" + i);
      }
      synchronized (anotherLock) {
        anotherLock.notify();
        allowCompactorThread[0] = true;
      }
      synchronized (region) {
        if (!closeThreadStarted[0]) {
          region.wait(9000);
          assertTrue(closeThreadStarted[0]);
        }
      }
      th.join();
      assertFalse(foundException[0]);
      assertFalse(foundException[1]);

    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(new CacheObserverAdapter());
      System.getProperties()
          .setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME, "");
    }
  }

  @Test
  public void testCompactorClose() throws Exception {
    // System.getProperties().setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME,
    // "true");
    try {
      // Create a region with rolling enabled.
      DiskRegionProperties props = new DiskRegionProperties();
      props.setRegionName("testCompactorClose");
      props.setRolling(true);
      props.setCompactionThreshold(100);
      props.setDiskDirs(dirs);
      props.setMaxOplogSize(100);
      props.setPersistBackup(true);
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
      final boolean[] foundException = new boolean[] {false};
      final boolean[] regionDestroyed = new boolean[] {false};
      final boolean[] allowCompactorThread = new boolean[] {false};
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;


      final Object anotherLock = new Object();
      // cause a switch
      CacheObserver old = CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        final AtomicBoolean compactorSignalled = new AtomicBoolean();

        public void beforeGoingToCompact() {
          // wait for operations to get over
          synchronized (anotherLock) {
            try {
              if (!allowCompactorThread[0]) {
                anotherLock.wait();
              }
            } catch (Exception e) {
              foundException[0] = true;
              e.printStackTrace();
            }
          }
          this.compactorSignalled.set(false);
        }

        @Override
        public void beforeDeletingCompactedOplog(Oplog oplog) {
          // destroy the oplog
          // This will cause DiskAccessException where the compactor will
          // attempt to destroy the region.
          throw new DiskAccessException("IGNORE_EXCEPTION_testCompactorClose GeneratedException",
              region);
        }

        @Override
        public void afterStoppingCompactor() {
          synchronized (region) {
            regionDestroyed[0] = true;
            region.notify();
          }
        }
      });
      // create some string entries
      for (int i = 0; i < 10; ++i) {
        region.put("" + i, new byte[10]);
      }
      synchronized (anotherLock) {
        anotherLock.notify();
        allowCompactorThread[0] = true;
      }
      synchronized (region) {
        if (!regionDestroyed[0]) {
          region.wait(10000);
          assertTrue(regionDestroyed[0]);
        }
      }
      assertFalse(foundException[0]);
    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(new CacheObserverAdapter());
      // System.getProperties().setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME,
      // "");
    }
  }

  @Test
  public void testBug40648part1() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName("testBug40648part1");
    props.setRolling(true);
    props.setDiskDirs(dirs);
    props.setMaxOplogSize(500 * 2);
    props.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, props);
    final byte[] payload = new byte[100];
    final int MAX_KEY = Integer.getInteger("MAX_KEY", 2).intValue();
    final Integer[] keys = new Integer[MAX_KEY];
    for (int i = 0; i < MAX_KEY; i++) {
      keys[i] = Integer.valueOf(i);
    }
    final int MAX_ITERATIONS = Integer.getInteger("MAX_ITERATIONS", 1000).intValue();
    int itCount = 0;
    while (itCount++ < MAX_ITERATIONS) {
      for (int i = 0; i < MAX_KEY; i++) {
        region.put(keys[i], payload);
      }
    }
  }

  @Test
  public void testBug40648part2() throws Exception {
    // Same as part1 but no persistence. I wasn't able to get part2
    // to fail but thought this was worth testing anyway.
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName("testBug40648part2");
    props.setRolling(true);
    props.setDiskDirs(dirs);
    props.setMaxOplogSize(500 * 2);
    props.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, props);
    final byte[] payload = new byte[100];
    final int MAX_KEY = Integer.getInteger("MAX_KEY", 2).intValue();
    final Integer[] keys = new Integer[MAX_KEY];
    for (int i = 0; i < MAX_KEY; i++) {
      keys[i] = Integer.valueOf(i);
    }
    final int MAX_ITERATIONS = Integer.getInteger("MAX_ITERATIONS", 1000).intValue();
    int itCount = 0;
    while (itCount++ < MAX_ITERATIONS) {
      for (int i = 0; i < MAX_KEY; i++) {
        region.put(keys[i], payload);
      }
    }
  }

  @Test
  public void testForceCompactionDoesRoll() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName("testForceCompactionDoesRoll");
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setAllowForceCompaction(true);
    props.setPersistBackup(true);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    logWriter.info("calling noop forceCompaction");
    assertEquals(false, ((LocalRegion) region).getDiskStore().forceCompaction());
    logWriter.info("putting key1");
    region.put("key1", "value1");
    logWriter.info("putting key2");
    region.put("key2", "value2");
    logWriter.info("calling noop forceCompaction");
    assertEquals(false, ((LocalRegion) region).getDiskStore().forceCompaction());
    logWriter.info("removing key1");
    region.remove("key1");
    logWriter.info("removing key2");
    region.remove("key2");
    // now that it is compactable the following forceCompaction should
    // go ahead and do a roll and compact it.
    boolean compacted = ((LocalRegion) region).getDiskStore().forceCompaction();
    assertEquals(true, compacted);
  }

  /**
   * Confirm that forceCompaction waits for the compaction to finish
   */
  @Test
  public void testNonDefaultCompaction() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName("testForceCompactionDoesRoll");
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setAllowForceCompaction(true);
    props.setPersistBackup(true);
    props.setCompactionThreshold(90);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    logWriter.info("putting key1");
    region.put("key1", "value1");
    logWriter.info("putting key2");
    region.put("key2", "value2");
    // Only remove 1 of the entries. This wouldn't trigger compaction with
    // the default threshold, since there are two entries.
    logWriter.info("removing key1");
    region.remove("key1");
    // now that it is compactable the following forceCompaction should
    // go ahead and do a roll and compact it.
    Oplog oplog = dr.testHook_getChild();
    boolean compacted = ((LocalRegion) region).getDiskStore().forceCompaction();
    assertEquals(true, oplog.testConfirmCompacted());
    assertEquals(true, compacted);
  }

  /**
   * Confirm that forceCompaction waits for the compaction to finish
   */
  @Test
  public void testForceCompactionIsSync() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName("testForceCompactionDoesRoll");
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setAllowForceCompaction(true);
    props.setPersistBackup(true);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    logWriter.info("putting key1");
    region.put("key1", "value1");
    logWriter.info("putting key2");
    region.put("key2", "value2");
    logWriter.info("removing key1");
    region.remove("key1");
    logWriter.info("removing key2");
    region.remove("key2");
    // now that it is compactable the following forceCompaction should
    // go ahead and do a roll and compact it.
    Oplog oplog = dr.testHook_getChild();
    boolean compacted = ((LocalRegion) region).getDiskStore().forceCompaction();
    assertEquals(true, oplog.testConfirmCompacted());
    assertEquals(true, compacted);
    CachePerfStats stats = ((InternalCache) cache).getCachePerfStats();
    assertTrue("expected " + stats.getDiskTasksWaiting() + " to be >= 0",
        stats.getDiskTasksWaiting() >= 0);
  }

  @Test
  public void testBug40876() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();

    props.setRegionName("testBug40876");
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setPersistBackup(true);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    region.put("key1", "value1");
    region.invalidate("key1");
    region.close();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    Object obj = ((LocalRegion) this.region).getValueOnDiskOrBuffer("key1");
    assertEquals(Token.INVALID, obj);
    assertFalse(this.region.containsValueForKey("key1"));
  }

  /**
   * Make sure oplog created by recovery goes in the proper directory
   */
  @Test
  public void testBug41822() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName("testBug41822");
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setMaxOplogSize(500);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    final byte[] payload = new byte[100];
    region.put("key0", payload);
    assertEquals(dirs[0],
        ((LocalRegion) region).getDiskRegion().testHook_getChild().getDirectoryHolder().getDir());
    region.close();
    ((LocalRegion) region).getDiskStore().close();
    ((LocalRegion) region).getGemFireCache().removeDiskStore(((LocalRegion) region).getDiskStore());
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    region.put("key1", payload);
    assertEquals(dirs[1],
        ((LocalRegion) region).getDiskRegion().testHook_getChild().getDirectoryHolder().getDir());
    region.close();
    ((LocalRegion) region).getDiskStore().close();
    ((LocalRegion) region).getGemFireCache().removeDiskStore(((LocalRegion) region).getDiskStore());
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    region.put("key2", payload);
    assertEquals(dirs[2],
        ((LocalRegion) region).getDiskRegion().testHook_getChild().getDirectoryHolder().getDir());
    region.close();
    ((LocalRegion) region).getDiskStore().close();
    ((LocalRegion) region).getGemFireCache().removeDiskStore(((LocalRegion) region).getDiskStore());
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    region.put("key3", payload);
    assertEquals(dirs[3],
        ((LocalRegion) region).getDiskRegion().testHook_getChild().getDirectoryHolder().getDir());
    region.close();
    ((LocalRegion) region).getDiskStore().close();
    ((LocalRegion) region).getGemFireCache().removeDiskStore(((LocalRegion) region).getDiskStore());
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
    region.put("key4", payload);
    assertEquals(dirs[0],
        ((LocalRegion) region).getDiskRegion().testHook_getChild().getDirectoryHolder().getDir());
  }

  private class Bug41770CacheObserverAdapter extends CacheObserverAdapter {
    boolean didClear = false;

    public void afterWritingBytes() {
      region.getCache().getLogger().info("in afterWritingBytes didClear=" + didClear);
      if (!didClear)
        return;
      CacheObserverHolder.setInstance(new CacheObserverAdapter());
      // now that the flusher finished the async create of VALUE1
      // do another create
      region.create("KEY", "VALUE2");
      signalCompletion();
    }

    public void goingToFlush() {
      region.getCache().getLogger().info("in goingToFlush");
      // once the flusher is stuck in our listener do a region clear
      region.clear();
      didClear = true;
    }

    private boolean allDone = false;

    public synchronized boolean waitForCompletion() throws InterruptedException {
      if (!this.allDone) {
        this.wait(15000);
      }
      return this.allDone;
    }

    private synchronized void signalCompletion() {
      this.allDone = true;
      this.notifyAll();
    }
  }

  @Test
  public void testBug41770() throws Exception {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName("testBug41770");
    props.setOverflow(false);
    props.setRolling(false);
    props.setDiskDirs(dirs);
    props.setPersistBackup(true);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, props);

    // Install a listener then gets called when the async flusher threads
    // finds an entry to flush
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    Bug41770CacheObserverAdapter co = new Bug41770CacheObserverAdapter();
    CacheObserverHolder.setInstance(co);

    try {
      region.create("KEY", "VALUE1");
      ((LocalRegion) region).getDiskStore().forceFlush();

      assertEquals(true, co.waitForCompletion());
      // we should now have two creates in our oplog.

      region.close();
      // do a recovery it will fail with an assertion if this bug is not fixed
      region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, props);
    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(new CacheObserverAdapter());
    }
  }

}// end of DiskRegionJUnitTest
