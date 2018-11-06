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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.cache.EvictionAction.OVERFLOW_TO_DISK;
import static org.apache.geode.cache.EvictionAttributes.DEFAULT_ENTRIES_MAXIMUM;
import static org.apache.geode.cache.EvictionAttributes.createLRUEntryAttributes;
import static org.apache.geode.cache.RegionShortcut.LOCAL;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.eviction.EvictionCounters;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.UninterruptibleFileChannel;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

/**
 * Integration tests covering some miscellaneous functionality of Disk Region.
 */
public class DiskRegionJUnitTest {

  private static final long MAX_OPLOG_SIZE_IN_BYTES = 1024 * 1024 * 1024 * 10L;

  private Properties config;
  private InternalCache cache;
  private EvictionAttributes heapEvictionAttributes;

  private File[] diskDirs;
  private int[] diskDirSizes;

  private String uniqueName;
  private String regionName;
  private String diskStoreName;

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    diskStoreName = uniqueName + "_diskStore";

    config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");

    cache = (InternalCache) new CacheFactory(config).create();

    heapEvictionAttributes = EvictionAttributes.createLRUHeapAttributes(ObjectSizer.DEFAULT,
        OVERFLOW_TO_DISK);

    diskDirs = new File[4];
    diskDirs[0] = createDirectory(temporaryFolder.getRoot(), testName.getMethodName() + "1");
    diskDirs[1] = createDirectory(temporaryFolder.getRoot(), testName.getMethodName() + "2");
    diskDirs[2] = createDirectory(temporaryFolder.getRoot(), testName.getMethodName() + "3");
    diskDirs[3] = createDirectory(temporaryFolder.getRoot(), testName.getMethodName() + "4");

    // set default values of disk dir sizes here
    diskDirSizes = new int[4];
    diskDirSizes[0] = Integer.MAX_VALUE;
    diskDirSizes[1] = Integer.MAX_VALUE;
    diskDirSizes[2] = Integer.MAX_VALUE;
    diskDirSizes[3] = Integer.MAX_VALUE;

    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (cache != null) {
        cache.close();
      }
    } finally {
      CacheObserverHolder.setInstance(null);
      DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    }
  }

  @Test
  public void testRemoveCorrectlyRecorded() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setOverflow(true);
    diskRegionProperties.setOverFlowCapacity(1);
    diskRegionProperties.setDiskDirs(diskDirs);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, true, 1);

    region.put("1", "1");
    region.put("2", "2");
    region.put("3", "3");

    AfterDestroyListener<String, String> cacheListener = new AfterDestroyListener<>();
    region.getAttributesMutator().addCacheListener(cacheListener);

    region.destroy("1");

    // Make sure we don't get an old value when doing a destroy of an entry that overflowed to disk.
    // If we do then we have hit bug 40795.
    assertThat(cacheListener.getLastEvent()).isNotNull();
    assertThat(cacheListener.getLastEvent().getOldValue()).isNull();

    assertThat(region.get("1")).isNull();

    // does the following ever throw EntryNotFoundException? that would be ok
    Object valueOnDisk = ((InternalRegion) region).getValueOnDisk("1");
    assertThat(valueOnDisk == null || valueOnDisk.equals(Token.TOMBSTONE)).isTrue();

    region.close();

    Region<String, String> region2 =
        createRegion(regionName, diskStoreName, true, true, false, true, 1);
    assertThat(region2.get("1")).isNull();
  }

  /**
   * Tests if region overflows correctly and stats are create and updated correctly.
   */
  @Test
  public void testDiskRegionOverflow() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setOverflow(true);
    diskRegionProperties.setOverFlowCapacity(5);
    diskRegionProperties.setDiskDirs(diskDirs);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<Integer, Object> region =
        createRegion(regionName, diskStoreName, false, false, false, true, 5);

    DiskRegion diskRegion = getDiskRegion(region);

    DiskRegionStats diskStats = diskRegion.getStats();
    EvictionCounters evictionCounters =
        ((InternalRegion) region).getEvictionController().getCounters();

    assertThat(diskStats).isNotNull();
    assertThat(evictionCounters).isNotNull();

    diskRegion.flushForTesting();

    assertThat(diskStats.getWrites()).isEqualTo(0);
    assertThat(diskStats.getReads()).isEqualTo(0);
    assertThat(evictionCounters.getEvictions()).isEqualTo(0);

    // Put in larger stuff until we start evicting
    int total;
    for (total = 0; evictionCounters.getEvictions() <= 0; total++) {
      int[] array = new int[250];
      array[0] = total;
      region.put(total, array);
    }

    diskRegion.flushForTesting();

    assertThat(diskStats.getWrites()).isEqualTo(1);
    assertThat(diskStats.getReads()).isEqualTo(0);
    assertThat(diskStats.getNumEntriesInVM()).isEqualTo(total - 1);
    assertThat(diskStats.getNumOverflowOnDisk()).isEqualTo(1);
    assertThat(evictionCounters.getEvictions()).isEqualTo(1);

    Object value = region.get(0);
    assertThat(value).isNotNull().isInstanceOf(int[].class);
    assertThat(((int[]) value)[0]).isEqualTo(0);

    diskRegion.flushForTesting();

    assertThat(diskStats.getWrites()).isEqualTo(2);
    assertThat(diskStats.getReads()).isEqualTo(1);
    assertThat(evictionCounters.getEvictions()).isEqualTo(2);

    for (int i = 0; i < total; i++) {
      int[] array = (int[]) region.get(i);
      assertThat(array).isNotNull().isInstanceOf(int[].class);
      assertThat(array[0]).isEqualTo(i);
    }
  }

  /**
   * test method for putting different objects and validating that they have been correctly put
   */
  @Test
  public void testDifferentObjectTypePuts() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setOverflow(true);
    diskRegionProperties.setOverFlowCapacity(100);
    diskRegionProperties.setDiskDirs(diskDirs);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, Object> region =
        createRegion(regionName, diskStoreName, true, false, false, true, 100);

    DiskRegion diskRegion = getDiskRegion(region);

    int total = 10;
    for (int i = 0; i < total; i++) {
      region.put(String.valueOf(i), String.valueOf(i));
    }
    region.put("foobar", "junk");

    region.localDestroy("foobar");

    region.put("foobar2", "junk");
    diskRegion.flushForTesting();
    region.localDestroy("foobar2");

    // test invalidate
    region.put("invalid", "invalid");
    diskRegion.flushForTesting();
    region.invalidate("invalid");
    diskRegion.flushForTesting();
    assertThat(region.containsKey("invalid")).isTrue();
    assertThat(region.containsValueForKey("invalid")).isFalse();
    total++;

    // test local-invalidate
    region.put("localinvalid", "localinvalid");
    diskRegion.flushForTesting();
    region.localInvalidate("localinvalid");
    diskRegion.flushForTesting();
    assertThat(region.containsKey("localinvalid")).isTrue();
    assertThat(region.containsValueForKey("localinvalid")).isFalse();
    total++;

    // test byte[] values
    region.put("byteArray", new byte[0]);
    diskRegion.flushForTesting();
    assertThatArrayEquals(new byte[0], region.get("byteArray"));
    total++;

    // test modification
    region.put("modified", "originalValue");
    diskRegion.flushForTesting();
    region.put("modified", "modified");
    diskRegion.flushForTesting();
    assertThat(region.get("modified")).isEqualTo("modified");
    total++;

    assertThat(region.size()).isEqualTo(total);

    cache.close();
    cache = (InternalCache) new CacheFactory(config).create();

    diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    region = createRegion(regionName, diskStoreName, true, false, false, true, 100);

    assertThat(region.size()).isEqualTo(total);
    assertThat(region.containsKey("invalid")).isTrue();
    assertThat(region.get("invalid")).isNull();
    assertThat(region.containsValueForKey("invalid")).isFalse();

    region.localDestroy("invalid");
    total--;

    assertThat(region.containsKey("localinvalid")).isTrue();
    assertThat(region.containsValueForKey("localinvalid")).isFalse();
    region.localDestroy("localinvalid");
    total--;

    assertThatArrayEquals(new byte[0], region.get("byteArray"));
    region.localDestroy("byteArray");
    total--;

    assertThat(region.get("modified")).isEqualTo("modified");
    region.localDestroy("modified");
    total--;

    assertThat(region.size()).isEqualTo(total);
  }

  @Test
  public void testFaultingInRemovalFromAsyncBuffer() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setOverflow(true);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setOverFlowCapacity(100);
    diskRegionProperties.setDiskDirs(diskDirs);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<Integer, Integer> region =
        createRegion(regionName, diskStoreName, false, true, false, true, 100);

    CountDownLatch doLatch = new CountDownLatch(1);
    DelayedGet get = new DelayedGet(doLatch, region);

    CompletableFuture<Void> getter1 = executorServiceRule.runAsync(get);
    CompletableFuture<Void> getter2 = executorServiceRule.runAsync(get);
    CompletableFuture<Void> getter3 = executorServiceRule.runAsync(get);
    CompletableFuture<Void> getter4 = executorServiceRule.runAsync(get);
    CompletableFuture<Void> getter5 = executorServiceRule.runAsync(get);

    for (int i = 0; i < 110; i++) {
      region.put(i, i);
    }

    doLatch.countDown();

    CompletableFuture.allOf(getter1, getter2, getter3, getter4, getter5).get(2, MINUTES);
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
  public void testSingleDirectoryNotHanging() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirsAndSizes(new File[] {diskDirs[0]}, new int[] {2048});
    diskRegionProperties.setMaxOplogSize(2097152);
    diskRegionProperties.setRolling(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, byte[]> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    Puts puts = new Puts(region);
    puts.performPuts();

    assertThat(puts.putSuccessful(0)).as(" first put did not succeed").isTrue();
    assertThat(puts.putSuccessful(1)).as(" second put did not succeed").isTrue();
    assertThat(puts.putSuccessful(2)).as(" third put did not succeed").isTrue();

    assertThat(puts.diskAccessExceptionOccurred())
        .as(" Exception was not supposed to occur but did occur").isFalse();
  }

  @Test
  public void testOperationGreaterThanMaxOplogSize() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setMaxOplogSize(512);
    diskRegionProperties.setRolling(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, byte[]> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    Puts puts = new Puts(region);
    puts.performPuts();

    assertThat(puts.putSuccessful(0)).as(" first put did not succeed").isTrue();
    assertThat(puts.putSuccessful(1)).as(" second put did not succeed").isTrue();
    assertThat(puts.putSuccessful(2)).as(" third put did not succeed").isTrue();

    assertThat(puts.diskAccessExceptionOccurred())
        .as(" Exception was not supposed to occur but did occur").isFalse();
  }

  /**
   * As we have relaxed the constraint of max dir size
   */
  @Test
  public void testOperationGreaterThanMaxDirSize() {
    int[] diskDirSizes = {1025, 1025, 1025, 1025};

    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setDiskDirsAndSizes(diskDirs, diskDirSizes);
    diskRegionProperties.setMaxOplogSize(600);
    diskRegionProperties.setRolling(false);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    DiskStore diskStore = createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, byte[]> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    assertThat(diskStore.getDiskDirSizes())
        .as("expected=" + Arrays.toString(diskDirSizes) + " actual="
            + Arrays.toString(diskStore.getDiskDirSizes()))
        .isEqualTo(diskDirSizes);

    Puts puts = new Puts(region, 1026);
    puts.performPuts();

    assertThat(puts.diskAccessExceptionOccurred())
        .as(" Exception was not supposed to occur but did occur").isTrue();

    assertThat(puts.putSuccessful(0)).as(" first put did not succeed").isFalse();
    assertThat(puts.putSuccessful(1)).as(" second put did not succeed").isFalse();
    assertThat(puts.putSuccessful(2)).as(" third put did not succeed").isFalse();

    // if the exception occurred then the region should be closed already
    verifyClosedDueToDiskAccessException(region);
  }

  /**
   * Regression test for TRAC #42464: overfilling disk directory causes rampant oplog creation
   *
   * <p>
   * When max-dir-size is exceeded and compaction is enabled we allow oplogs to keep getting
   * created. Make sure that when they do they do not keep putting one op per oplog (which is caused
   * by bug 42464).
   */
  @Test
  public void testBug42464() {
    int[] dirSizes = new int[] {900};

    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirsAndSizes(new File[] {diskDirs[0]}, dirSizes);
    diskRegionProperties.setMaxOplogSize(500);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setOverFlowCapacity(1);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    DiskStore diskStore = createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 500);

    Region<Integer, Object> region =
        createRegion(regionName, diskStoreName, false, true, false, true, 1);

    assertThat(diskStore.getDiskDirSizes()).as("expected=" + Arrays.toString(dirSizes) + " actual="
        + Arrays.toString(diskStore.getDiskDirSizes())).isEqualTo(dirSizes);

    // One entry is kept in memory
    // since the crf max is 500 we should only be able to have 4 entries. The
    // 5th should not fit because of record overhead.
    for (int i = 0; i <= 9; i++) {
      region.getCache().getLogger().info("putting " + i);
      region.put(i, new byte[101]);
    }

    // At this point we should have two oplogs that are basically full
    // (they should each contain 4 entries) and a third oplog that
    // contains a single entry. But the 3rd one will end up also containing 4
    // entries.
    // TODO what is the max size of this 3rd oplog's crf? The first two crfs
    // will be close to 400 bytes each. So the max size of the 3rd oplog should
    // be close to 100.
    ArrayList<OverflowOplog> oplogs = toDiskStoreImpl(diskStore).testHookGetAllOverflowOplogs();
    assertThat(oplogs).hasSize(3);

    // TODO verify oplogs
    // Now make sure that further oplogs can hold 4 entries
    for (int j = 10; j <= 13; j++) {
      region.put(j, new byte[101]);
    }
    oplogs = toDiskStoreImpl(diskStore).testHookGetAllOverflowOplogs();
    assertThat(oplogs).hasSize(4);

    // now remove all entries and make sure old oplogs go away
    for (int i = 0; i <= 13; i++) {
      region.remove(i);
    }

    // give background compactor chance to remove oplogs
    oplogs = toDiskStoreImpl(diskStore).testHookGetAllOverflowOplogs();
    int retryCount = 20;
    while (oplogs.size() > 1 && retryCount > 0) {
      Wait.pause(100);
      oplogs = toDiskStoreImpl(diskStore).testHookGetAllOverflowOplogs();
      retryCount--;
    }
    assertThat(oplogs).hasSize(1);
  }

  @Test
  public void testSingleDirectorySizeViolation() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setDiskDirsAndSizes(new File[] {diskDirs[0]}, new int[] {2048});
    diskRegionProperties.setMaxOplogSize(2097152);
    diskRegionProperties.setRolling(false);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, byte[]> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    Puts puts = new Puts(region);
    puts.performPuts();

    assertThat(puts.putSuccessful(0)).as(" first put did not succeed").isTrue();
    assertThat(puts.putSuccessful(1)).as(" second put should have failed").isFalse();
    assertThat(puts.putSuccessful(2)).as(" third put should have failed").isFalse();

    assertThat(puts.diskAccessExceptionOccurred())
        .as(" Exception was supposed to occur but did not occur").isTrue();

    verifyClosedDueToDiskAccessException(region);
  }

  /**
   * DiskRegDiskAccessExceptionTest : Disk region test for DiskAccessException.
   */
  @Test
  public void testDiskFullExcep() {
    int[] diskDirSizes = new int[4];
    diskDirSizes[0] = 2048 + 500;
    diskDirSizes[1] = 2048 + 500;
    diskDirSizes[2] = 2048 + 500;
    diskDirSizes[3] = 2048 + 500;

    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirsAndSizes(diskDirs, diskDirSizes);
    diskRegionProperties.setPersistBackup(true);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setMaxOplogSize(1000000000);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 1_000_000_000);

    Region<Object, Object> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    int[] actualDiskDirSizes = ((InternalRegion) region).getDiskDirSizes();

    assertThat(actualDiskDirSizes[0]).isEqualTo(2048 + 500);
    assertThat(actualDiskDirSizes[1]).isEqualTo(2048 + 500);
    assertThat(actualDiskDirSizes[2]).isEqualTo(2048 + 500);
    assertThat(actualDiskDirSizes[3]).isEqualTo(2048 + 500);

    // we have room for 2 values per dir

    byte[] value = new byte[1024];
    Arrays.fill(value, (byte) 77);

    for (int i = 0; i < 8; i++) {
      region.put(String.valueOf(i), value);
    }

    // we should have put 2 values in each dir so the next one should not fit
    try (IgnoredException ie = addIgnoredException(DiskAccessException.class)) {
      Throwable thrown = catchThrowable(() -> region.put("FULL", value));
      assertThat(thrown).isInstanceOf(DiskAccessException.class);
    }

    verifyClosedDueToDiskAccessException(region);
  }

  /**
   * Make sure if compaction is enabled that we can exceed the disk dir limit
   */
  @Test
  public void testNoDiskFullExcep() {
    int[] diskDirSizes = new int[4];
    diskDirSizes[0] = 2048 + 500;
    diskDirSizes[1] = 2048 + 500;
    diskDirSizes[2] = 2048 + 500;
    diskDirSizes[3] = 2048 + 500;

    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirsAndSizes(diskDirs, diskDirSizes);
    diskRegionProperties.setPersistBackup(true);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setMaxOplogSize(1000000000);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 1_000_000_000);

    Region<Object, Object> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    int[] actualDiskDirSizes = ((InternalRegion) region).getDiskDirSizes();

    assertThat(actualDiskDirSizes[0]).isEqualTo(2048 + 500);
    assertThat(actualDiskDirSizes[1]).isEqualTo(2048 + 500);
    assertThat(actualDiskDirSizes[2]).isEqualTo(2048 + 500);
    assertThat(actualDiskDirSizes[3]).isEqualTo(2048 + 500);

    // we have room for 2 values per dir

    byte[] value = new byte[1024];
    Arrays.fill(value, (byte) 77);

    for (int i = 0; i < 8; i++) {
      region.put(String.valueOf(i), value);
    }

    // we should have put 2 values in each dir so the next one should not fit but will be allowed
    // because compaction is enabled. It should log a warning.
    region.put("OK", value);

    assertThat(cache.isClosed()).isFalse();
  }

  /**
   * DiskRegDiskAccessExceptionTest : Disk region test for DiskAccessException.
   */
  @Test
  public void testDiskFullExcepOverflowOnly() {
    int[] diskDirSizes = new int[4];
    diskDirSizes[0] = 2048 + 500;
    diskDirSizes[1] = 2048 + 500;
    diskDirSizes[2] = 2048 + 500;
    diskDirSizes[3] = 2048 + 500;

    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirsAndSizes(diskDirs, diskDirSizes);
    diskRegionProperties.setPersistBackup(false);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setMaxOplogSize(1000000000);
    diskRegionProperties.setOverFlowCapacity(1);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 1_000_000_000);

    Region<Object, Object> region =
        createRegion(regionName, diskStoreName, false, true, false, true, 1);

    int[] actualDiskDirSizes = ((InternalRegion) region).getDiskDirSizes();

    assertThat(actualDiskDirSizes[0]).isEqualTo(2048 + 500);
    assertThat(actualDiskDirSizes[1]).isEqualTo(2048 + 500);
    assertThat(actualDiskDirSizes[2]).isEqualTo(2048 + 500);
    assertThat(actualDiskDirSizes[3]).isEqualTo(2048 + 500);

    // we have room for 2 values per dir

    byte[] value = new byte[1024];
    Arrays.fill(value, (byte) 77);

    // put a dummy value in since one value stays in memory
    region.put("FIRST", value);

    for (int i = 0; i < 8; i++) {
      region.put(String.valueOf(i), value);
    }

    // we should have put 2 values in each dir so the next one should not fit
    try (IgnoredException ie = addIgnoredException(DiskAccessException.class)) {
      Throwable thrown = catchThrowable(() -> region.put("FULL", value));
      assertThat(thrown).isInstanceOf(DiskAccessException.class);
    }

    verifyClosedDueToDiskAccessException(region);
  }

  /**
   * Make sure if compaction is enabled that we can exceed the disk dir limit
   */
  @Test
  public void testNoDiskFullExcepOverflowOnly() {
    int[] diskDirSizes = new int[4];
    diskDirSizes[0] = 2048 + 500;
    diskDirSizes[1] = 2048 + 500;
    diskDirSizes[2] = 2048 + 500;
    diskDirSizes[3] = 2048 + 500;

    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirsAndSizes(diskDirs, diskDirSizes);
    diskRegionProperties.setPersistBackup(false);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setMaxOplogSize(1000000000);
    diskRegionProperties.setOverFlowCapacity(1);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 1_000_000_000);

    Region<Object, Object> region =
        createRegion(regionName, diskStoreName, false, true, false, true, 1);

    int[] actualDiskDirSizes = ((InternalRegion) region).getDiskDirSizes();

    assertThat(actualDiskDirSizes[0]).isEqualTo(2048 + 500);
    assertThat(actualDiskDirSizes[1]).isEqualTo(2048 + 500);
    assertThat(actualDiskDirSizes[2]).isEqualTo(2048 + 500);
    assertThat(actualDiskDirSizes[3]).isEqualTo(2048 + 500);

    // we have room for 2 values per dir

    byte[] value = new byte[1024];
    Arrays.fill(value, (byte) 77);

    // put a dummy value in since one value stays in memory
    region.put("FIRST", value);

    for (int i = 0; i < 8; i++) {
      region.put(String.valueOf(i), value);
    }

    // we should have put 2 values in each dir so the next one should not fit
    // but will be allowed because compaction is enabled. It should log a warning.
    region.put("OK", value);

    assertThat(cache.isClosed()).isFalse();
  }

  /**
   * DiskAccessException Test : Even if rolling doesn't free the space in stipulated time, the
   * operation should not get stuck or see Exception
   */
  @Test
  public void testSyncModeAllowOperationToProceedEvenIfDiskSpaceIsNotSufficient() {
    int[] diskDirSizes = {2048};

    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirsAndSizes(new File[] {diskDirs[0]}, diskDirSizes);
    diskRegionProperties.setPersistBackup(true);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    diskRegionProperties.setMaxOplogSize(100000000);
    diskRegionProperties.setRegionName(regionName);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 100_000_000);

    Region<Integer, Object> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    int[] actualDiskSizes = ((InternalRegion) region).getDiskDirSizes();
    assertThat(actualDiskSizes).isEqualTo(diskDirSizes);

    // puts should not fail even after surpassing disk dir sizes
    byte[] value = new byte[990];
    Arrays.fill(value, (byte) 77);
    region.put(1, value);
    region.put(1, value);
    region.put(1, value);

    region.close();
  }// end of testSyncPersistRegionDAExp

  @Test
  public void testAsyncModeAllowOperationToProceedEvenIfDiskSpaceIsNotSufficient() {
    int[] dirSizes = {2048};

    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirsAndSizes(new File[] {diskDirs[0]}, dirSizes);
    diskRegionProperties.setPersistBackup(true);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    diskRegionProperties.setMaxOplogSize(100000000);
    diskRegionProperties.setBytesThreshold(1000000);
    diskRegionProperties.setTimeInterval(1500000);
    diskRegionProperties.setRegionName(regionName);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    DiskStore diskStore =
        createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 100_000_000);

    Region<Object, Object> region =
        createRegion(regionName, diskStoreName, true, false, false, false, 0);

    assertThat(diskStore.getDiskDirSizes()).isEqualTo(dirSizes);

    // puts should not fail even after surpassing disk dir sizes
    byte[] value = new byte[990];
    Arrays.fill(value, (byte) 77);
    region.put(0, value);
    region.put(1, value);
    region.put(2, value);

    region.close();
  }// end of testAsyncPersistRegionDAExp

  /**
   * DiskRegGetInvalidEntryTest: get invalid entry should return null.
   */
  @Test
  public void testDiskGetInvalidEntry() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirsAndSizes(diskDirs, diskDirSizes);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setMaxOplogSize(1000000000);
    diskRegionProperties.setBytesThreshold(1000000);
    diskRegionProperties.setTimeInterval(1500000);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 1_000_000_000);

    Region<Object, Object> region =
        createRegion(regionName, diskStoreName, false, false, false, true, 1000);

    byte[] value = new byte[1024];
    Arrays.fill(value, (byte) 77);
    for (int i = 0; i < 10; i++) {
      region.put("key" + i, value);
    }

    // invalidate an entry
    region.invalidate("key1");

    // get the invalid entry and verify that the value returned is null
    assertThat(region.get("key1")).isNull();

    region.close(); // closes disk file which will flush all buffers

  }// end of DiskRegGetInvalidEntryTest

  /**
   * DiskRegionByteArrayJUnitTest: A byte array as a value put in local persistent region ,when
   * retrieved from the disk should be correctly presented as a byte array
   */
  @Test
  public void testDiskRegionByteArray() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setPersistBackup(true);
    diskRegionProperties.setDiskDirs(diskDirs);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<Object, Object> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    int ENTRY_SIZE = 1024;
    int OP_COUNT = 10;
    String key = "K";
    byte[] value = new byte[ENTRY_SIZE];
    Arrays.fill(value, (byte) 77);

    // put an entry
    region.put(key, value);

    // put few more entries to write on disk
    for (int i = 0; i < OP_COUNT; i++) {
      region.put(i, value);
    }

    // get from disk
    DiskId diskId = ((DiskEntry) ((InternalRegion) region).basicGetEntry("K")).getDiskId();
    Object val = getDiskRegion(region).get(diskId);

    // verify that the value retrieved above represents byte array.
    // verify the length of the byte[]
    assertThat((byte[]) val).hasSize(1024);

    // verify that the retrieved byte[] equals to the value put initially.
    byte[] x = (byte[]) val;
    boolean result = false;
    for (int i = 0; i < x.length; i++) {
      result = x[i] == value[i];
    }

    assertThat(result).as("The val obtained from disk is not euqal to the value put initially")
        .isTrue();
  }// end of DiskRegionByteArrayJUnitTest

  /**
   * DiskRegionFactoryJUnitTest: Test for verifying DiskRegion or SimpleDiskRegion.
   */
  @Test
  public void testInstanceOfDiskRegion() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(diskDirs); // diskDirs is an array of four diskDirs
    diskRegionProperties.setRolling(true);

    DiskStoreFactory diskStoreFactory2 = toDiskStoreFactory(diskRegionProperties);

    DiskStore diskStore = createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory2);

    Region<?, ?> region = createRegion(regionName, diskStoreName, true, true, false, false, 0);

    region.destroyRegion();
    closeDiskStore(diskStore);

    diskRegionProperties.setDiskDirs(diskDirs); // diskDirs is an array of four diskDirs
    diskRegionProperties.setRolling(false);

    DiskStoreFactory diskStoreFactory1 = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory1);

    region = createRegion(regionName, diskStoreName, true, true, false, false, 0);

    region.destroyRegion();
    closeDiskStore(diskStore);

    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirsAndSizes(new File[] {newFolder(uniqueName + "1")},
        new int[] {2048});

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    region = createRegion(regionName, diskStoreName, true, true, false, false, 0);

    region.destroyRegion();
    closeDiskStore(diskStore);

    diskRegionProperties.setRolling(false);
    diskRegionProperties.setMaxOplogSize(2048);
    File dir = newFolder(uniqueName + "2");
    diskRegionProperties.setDiskDirsAndSizes(new File[] {dir}, new int[] {1024});

    // TODO: test just rolls along? not sure of the value
  }

  /**
   * DiskRegionStatsJUnitTest :
   */
  @Test
  public void testStats() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirsAndSizes(new File[] {diskDirs[0]}, new int[] {diskDirSizes[0]});
    diskRegionProperties.setMaxOplogSize(2097152);
    diskRegionProperties.setOverFlowCapacity(100);
    diskRegionProperties.setRolling(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 2_097_152);

    int overflowCapacity = 100;
    Region<Integer, Integer> region =
        createRegion(regionName, diskStoreName, false, true, false, true, overflowCapacity);

    DiskRegionStats stats = getDiskRegion(region).getStats();

    // TODO: there are no assertions for counter
    int counter = 0;
    for (int i = 0; i < 5000; i++) {
      region.put(i, i);
      region.put(i, i);
      region.put(i, i);

      if (i > overflowCapacity + 5) {
        region.get(++counter);
        region.get(counter);
      }

      if (i > overflowCapacity) {
        assertThat(stats.getNumEntriesInVM()).isEqualTo(overflowCapacity);
        assertThat(stats.getNumOverflowOnDisk() - 1)
            .as(" number of entries on disk not corrected expected " + (i - overflowCapacity)
                + " but is " + stats.getNumOverflowOnDisk())
            .isEqualTo(i - overflowCapacity);
      }
    }
  }// end of testStats

  /**
   * DiskRegOverflowOnlyNoFilesTest: Overflow only mode has no files of previous run, during startup
   */
  @Test
  public void testOverflowOnlyNoFiles() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setTimeInterval(15000);
    diskRegionProperties.setBytesThreshold(100000);
    diskRegionProperties.setOverFlowCapacity(1000);
    diskRegionProperties.setDiskDirs(diskDirs);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<Integer, Object> region =
        createRegion(regionName, diskStoreName, false, false, false, true, 1_000);

    byte[] value = new byte[1024];
    Arrays.fill(value, (byte) 77);

    for (int i = 0; i < 100; i++) {
      region.put(i, value);
    }

    region.close(); // closes disk file which will flush all buffers

    // verify that there are no files in the disk dir
    int fileCount = 0;
    for (File diskDir : diskDirs) {
      File[] files = diskDir.listFiles();
      fileCount += files.length;
    }

    // since the diskStore has not been closed we expect two files: .lk and .if
    assertThat(fileCount).isEqualTo(2);
    cache.close();

    // we now should only have zero
    fileCount = 0;
    for (File diskDir : diskDirs) {
      File[] files = diskDir.listFiles();
      fileCount += files.length;
    }
    assertThat(fileCount).isEqualTo(0);
  }// end of testOverflowOnlyNoFiles

  @Test
  public void testPersistNoFiles() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setOverflow(false);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);
    diskRegionProperties.setRegionName(regionName);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<Integer, Object> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    byte[] value = new byte[1024];
    Arrays.fill(value, (byte) 77);

    for (int i = 0; i < 100; i++) {
      region.put(i, value);
    }

    region.destroyRegion();

    int fileCount = 0;
    for (File diskDir : diskDirs) {
      File[] files = diskDir.listFiles();
      fileCount += files.length;
    }

    // since the diskStore has not been closed we expect four files: .lk and .if and a crf and a drf
    assertThat(fileCount).isEqualTo(4);

    cache.close();

    // we now should only have zero since the disk store had no regions remaining in it.
    fileCount = 0;
    for (File diskDir : diskDirs) {
      File[] files = diskDir.listFiles();
      fileCount += files.length;
    }
    assertThat(fileCount).isEqualTo(0);
  }

  /**
   * Test to verify that DiskAccessException is not thrown if rolling has been enabled. The test
   * configurations will cause the disk to go full and wait for the compactor to release space. A
   * DiskAccessException should not be thrown by this test
   */
  @Test
  public void testDiskAccessExceptionNotThrown() {
    File diskDir = newFolder(uniqueName);

    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirsAndSizes(new File[] {diskDir}, new int[] {10240});
    diskRegionProperties.setMaxOplogSize(1024);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setSynchronous(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 1024);

    Region<Integer, Object> region =
        createRegion(regionName, diskStoreName, true, true, false, true, DEFAULT_ENTRIES_MAXIMUM);

    byte[] bytes = new byte[256];

    for (int i = 0; i < 1500; i++) {
      region.put(i % 10, bytes);
    }
  }

  /**
   * Regression test for TRAC #37605: The entry which has already been removed from the system by
   * clear operation may still get appended to the LRUList
   *
   * <p>
   * If an entry which has just been written on the disk, sees clear just before updating the
   * EvictionList, then that deleted entry should not go into the EvictionList
   */
  @Test
  public void testClearInteractionWithLRUList_Bug37605() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setOverflow(true);
    diskRegionProperties.setOverFlowCapacity(1);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setRegionName(regionName);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory,
        diskRegionProperties.getMaxOplogSize());

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, true, 1);

    AtomicReference<Future<Void>> doClearFuture = new AtomicReference<>();
    region.getAttributesMutator().addCacheListener(new CacheListenerAdapter<String, String>() {
      @Override
      public void afterCreate(EntryEvent<String, String> event) {
        doClearFuture.set(executorServiceRule.runAsync(() -> {
          try {
            region.clear();
          } catch (AssertionError | Exception e) {
            errorCollector.addError(e);
          }
        }));
      }
    });

    region.create("key1", "value1");
    awaitFuture(doClearFuture);
    assertThat(region.isEmpty()).isTrue();

    VMLRURegionMap vmlruRegionMap = getVMLRURegionMap(region);
    assertThat(vmlruRegionMap.getEvictionList().getEvictableEntry()).isNull();
  }

  /**
   * Regression test for TRAC #37606: A cleared entry may still get logged into the Oplog due to a
   * very small window of race condition
   *
   * <p>
   * As in the clear operation, previously the code was such that Htree Ref was first reset & then
   * the underlying region map got cleared, it was possible for the create op to set the new Htree
   * ref in thread local. Now if clear happened, the entry on which create op is going on was no
   * longer valid, but we would not be able to detect the conflict. The fix was to first clear the
   * region map & then reset the Htree Ref.
   */
  @Test
  public void testClearInteractionWithCreateOperation_Bug37606() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setOverflow(false);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);
    diskRegionProperties.setRegionName(regionName);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    AtomicReference<Future<Void>> doCreateFuture = new AtomicReference<>();
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      @Override
      public void beforeDiskClear() {
        LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
        doCreateFuture.set(executorServiceRule.runAsync(() -> {
          try {
            region.create("key1", "value1");
          } catch (AssertionError | Exception e) {
            errorCollector.addError(e);
          }
        }));
      }
    });

    region.clear();
    awaitFuture(doCreateFuture);

    // We expect 1 entry to exist, because the clear was triggered before the update
    assertThat(region.size()).isEqualTo(1);

    // close and recreate region and the entry should still exist
    region.close();
    Region<String, String> region2 =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);
    assertThat(region2.size()).isEqualTo(1);
  }

  /**
   * Regression test for TRAC #37606: A cleared entry may still get logged into the Oplog due to a
   * very small window of race condition
   *
   * <p>
   * Similar test in case of 'update'
   */
  @Test
  public void testClearInteractionWithUpdateOperation_Bug37606() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setOverflow(false);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);
    diskRegionProperties.setRegionName(regionName);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    region.create("key1", "value1");

    AtomicReference<Future<Void>> doPutFuture = new AtomicReference<>();
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      @Override
      public void beforeDiskClear() {
        LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
        doPutFuture.set(executorServiceRule.runAsync(() -> {
          try {
            region.put("key1", "value1");
          } catch (AssertionError | Exception e) {
            errorCollector.addError(e);
          }
        }));
      }
    });

    region.clear();
    awaitFuture(doPutFuture);

    // We expect 1 entry to exist, because the clear was triggered before the update
    assertThat(region.size()).isEqualTo(1);

    // close and recreate region and the entry should still exist
    region.close();
    Region<String, String> region2 =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);
    assertThat(region2.size()).isEqualTo(1);
  }

  /**
   * If IOException occurs while updating an entry in a persist only sync mode, DiskAccessException
   * should occur & region should be destroyed
   */
  @Test
  public void testEntryUpdateInSyncPersistOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setOverflow(false);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    region.create("key1", "value1");

    closeOplogFileChannel(region);

    Throwable thrown = catchThrowable(() -> region.put("key1", "value2"));
    assertThat(thrown).isInstanceOf(DiskAccessException.class);

    verifyClosedDueToDiskAccessException(region);
  }

  /**
   * If IOException occurs while updating an entry in a persist overflow sync mode, we should get
   * DiskAccessException & region be destroyed
   */
  @Test
  public void testEntryUpdateInSyncOverFlowPersistOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setOverflow(true);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, true, DEFAULT_ENTRIES_MAXIMUM);

    region.create("key1", "value1");

    closeOplogFileChannel(region);

    Throwable thrown = catchThrowable(() -> region.put("key1", "value2"));
    assertThat(thrown).isInstanceOf(DiskAccessException.class);

    verifyClosedDueToDiskAccessException(region);
  }

  /**
   * If IOException occurs while invalidating an entry in a persist only sync mode,
   * DiskAccessException should occur & region should be destroyed
   */
  @Test
  public void testEntryInvalidateInSyncPersistOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setOverflow(false);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    region.create("key1", "value1");

    closeOplogFileChannel(region);

    Throwable thrown = catchThrowable(() -> region.invalidate("key1"));
    assertThat(thrown).isInstanceOf(DiskAccessException.class);

    verifyClosedDueToDiskAccessException(region);
  }

  /**
   * If IOException occurs while invalidating an entry in a persist overflow sync mode,
   * DiskAccessException should occur & region should be destroyed
   */
  @Test
  public void testEntryInvalidateInSyncPersistOverflowForIOExceptionCase() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setOverflow(true);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, true, DEFAULT_ENTRIES_MAXIMUM);

    region.create("key1", "value1");

    closeOplogFileChannel(region);

    Throwable thrown = catchThrowable(() -> region.invalidate("key1"));
    assertThat(thrown).isInstanceOf(DiskAccessException.class);

    verifyClosedDueToDiskAccessException(region);
  }

  /**
   * If IOException occurs while creating an entry in a persist only sync mode, DiskAccessException
   * should occur & region should be destroyed
   */
  @Test
  public void testEntryCreateInSyncPersistOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setOverflow(false);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    closeOplogFileChannel(region);

    Throwable thrown = catchThrowable(() -> region.create("key1", "value1"));
    assertThat(thrown).isInstanceOf(DiskAccessException.class);

    verifyClosedDueToDiskAccessException(region);
  }

  /**
   * If IOException occurs while creating an entry in a persist overflow sync mode,
   * DiskAccessException should occur & region should be destroyed
   */
  @Test
  public void testEntryCreateInSyncPersistOverflowForIOExceptionCase() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setOverflow(true);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, true, DEFAULT_ENTRIES_MAXIMUM);

    closeOplogFileChannel(region);

    Throwable thrown = catchThrowable(() -> region.create("key1", "value1"));
    assertThat(thrown).isInstanceOf(DiskAccessException.class);

    verifyClosedDueToDiskAccessException(region);
  }

  /**
   * If IOException occurs while destroying an entry in a persist only sync mode,
   * DiskAccessException should occur & region should be destroyed
   */
  @Test
  public void testEntryDestructionInSyncPersistOnlyForIOExceptionCase() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setOverflow(false);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    region.create("key1", "value1");

    // Get the oplog handle & hence the underlying file & close it
    getDiskRegion(region).testHook_getChild().testClose();

    Throwable thrown = catchThrowable(() -> region.destroy("key1"));
    assertThat(thrown).isInstanceOf(DiskAccessException.class);

    verifyClosedDueToDiskAccessException(region);
  }

  /**
   * If IOException occurs while destroying an entry in a persist overflow sync mode,
   * DiskAccessException should occur & region should be destroyed
   */
  @Test
  public void testEntryDestructionInSyncPersistOverflowForIOExceptionCase() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setOverflow(true);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, true, DEFAULT_ENTRIES_MAXIMUM);

    region.create("key1", "value1");

    // Get the oplog handle & hence the underlying file & close it
    getDiskRegion(region).testHook_getChild().testClose();

    Throwable thrown = catchThrowable(() -> region.destroy("key1"));
    assertThat(thrown).isInstanceOf(DiskAccessException.class);

    verifyClosedDueToDiskAccessException(region);
  }

  /**
   * If IOException occurs while updating an entry in a Overflow only sync mode,
   * DiskAccessException should occur & region should be destroyed
   */
  @Test
  public void testEntryUpdateInSyncOverflowOnlyForIOExceptionCase() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setOverflow(true);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(false);
    diskRegionProperties.setOverFlowCapacity(1);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, false, true, false, true, 1);

    region.create("key1", "value1");
    region.create("key2", "value2");

    getDiskRegion(region).testHookCloseAllOverflowChannels();

    // Update key1, so that key2 goes on disk & encounters an exception
    Throwable thrown = catchThrowable(() -> region.put("key1", "value1'"));
    assertThat(thrown).isInstanceOf(DiskAccessException.class);

    verifyClosedDueToDiskAccessException(region);
  }

  /**
   * If IOException occurs while creating an entry in a Overflow only sync mode,
   * DiskAccessException should occur & region should be destroyed
   */
  @Test
  public void testEntryCreateInSyncOverflowOnlyForIOExceptionCase() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setOverflow(true);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(false);
    diskRegionProperties.setOverFlowCapacity(1);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, false, true, false, true, 1);

    region.create("key1", "value1");
    region.create("key2", "value2");

    getDiskRegion(region).testHookCloseAllOverflowChannels();

    Throwable thrown = catchThrowable(() -> region.create("key3", "value3"));
    assertThat(thrown).isInstanceOf(DiskAccessException.class);

    verifyClosedDueToDiskAccessException(region);
  }

  /**
   * A deletion of an entry in overflow only mode should not cause any eviction & hence no
   * DiskAccessException
   */
  @Test
  public void testEntryDeletionInSyncOverflowOnlyForIOExceptionCase() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setOverflow(true);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(false);
    diskRegionProperties.setOverFlowCapacity(1);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, false, true, false, true, 1);

    region.create("key1", "value1");
    region.create("key2", "value2");
    region.create("key3", "value3");

    getDiskRegion(region).testHookCloseAllOverflowChannels();

    // Update key1, so that key2 goes on disk & encounters an exception
    region.destroy("key1");
    region.destroy("key3");
  }

  /**
   * If IOException occurs while updating an entry in an Asynch mode, DiskAccessException should
   * occur & region should be destroyed
   */
  @Test
  public void testEntryUpdateInASyncPersistOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setOverflow(true);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setBytesThreshold(48);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<Object, Object> region =
        createRegion(regionName, diskStoreName, true, false, false, true, DEFAULT_ENTRIES_MAXIMUM);

    closeOplogFileChannel(region);

    region.create("key1", new byte[16]);
    region.create("key2", new byte[16]);

    DiskRegion diskRegion = getDiskRegion(region);
    diskRegion.flushForTesting();

    verifyClosedDueToDiskAccessException(region);
  }

  /**
   * If IOException occurs while updating an entry in an already initialized DiskRegion ,then the
   * cache servers should not be stopped , if any running as they are no clients connected to it.
   */
  @Test
  public void testBridgeServerStoppingInSyncPersistOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setOverflow(true);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<Object, Object> region =
        createRegion(regionName, diskStoreName, true, true, false, true, DEFAULT_ENTRIES_MAXIMUM);

    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();

    region.create("key1", new byte[16]);
    region.create("key2", new byte[16]);

    closeOplogFileChannel(region);

    Throwable thrown = catchThrowable(() -> region.put("key2", new byte[16]));
    assertThat(thrown).isInstanceOf(DiskAccessException.class);

    verifyClosedDueToDiskAccessException(region);

    // a disk access exception in a server should always stop the server
    List<CacheServer> cacheServers = cache.getCacheServers();
    assertThat(cacheServers).isEmpty();
  }

  /**
   * Regression test for TRAC #40250: If roller is active at the time of region.close it can end up
   * writing a dummy byte & thus lose the original value
   */
  @Test
  public void testDummyByteBugDuringRegionClose_Bug40250() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    // create some string entries
    for (int i = 0; i < 2; ++i) {
      region.put(String.valueOf(i), String.valueOf(i));
    }

    AtomicReference<Future<Void>> closeRegionFuture = new AtomicReference<>();

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      @Override
      public void beforeGoingToCompact() {
        closeRegionFuture.set(executorServiceRule.runAsync(() -> {
          try {
            region.close();
          } catch (AssertionError | Exception e) {
            errorCollector.addError(e);
          }
        }));

        // await until the roll flag DiskRegion.OplogCompactor is false (not currently exposed)
        try {
          Thread.sleep(8000);
        } catch (AssertionError | Exception e) {
          errorCollector.addError(e);
        }
      }
    });

    getDiskRegion(region).forceRolling();

    awaitFuture(closeRegionFuture);

    // Restart the region
    Region<String, String> region2 =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    for (int i = 0; i < 2; ++i) {
      assertThat(region2.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
    }
  }

  /**
   * If IOException occurs while initializing a region, then the cache servers should not be
   * stopped
   */
  @Test
  public void testBridgeServerRunningInSyncPersistOnlyForIOExceptionCase() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setOverflow(true);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);
    diskRegionProperties.setMaxOplogSize(100000); // just needs to be bigger than 65550

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 100_000);

    Region<String, Object> region =
        createRegion(regionName, diskStoreName, true, true, false, true, DEFAULT_ENTRIES_MAXIMUM);

    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();

    region.create("key1", new byte[16]);
    region.create("key2", new byte[16]);

    // Get the oplog file path
    UninterruptibleFileChannel oplogFileChannel =
        getDiskRegion(region).testHook_getChild().getFileChannel();

    // corrupt the opfile
    oplogFileChannel.position(2);
    ByteBuffer byteBuffer = ByteBuffer.allocate(416);
    for (int i = 0; i < 5; ++i) {
      byteBuffer.putInt(i);
    }
    byteBuffer.flip();

    // Corrupt the oplogFile
    oplogFileChannel.write(byteBuffer);

    // Close the region
    region.close();
    assertThat(region.isDestroyed()).isTrue();

    Throwable thrown = catchThrowable(() -> createRegion(regionName, diskStoreName, true, true,
        false, true, DEFAULT_ENTRIES_MAXIMUM));
    assertThat(thrown).isInstanceOf(DiskAccessException.class);

    List cacheServers = cache.getCacheServers();
    assertThat(cacheServers).isNotEmpty();
  }

  @Test
  public void testEarlyTerminationOfCompactorByDefault() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setMaxOplogSize(100);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 100);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    AtomicReference<Future<Void>> closeRegionFuture = new AtomicReference<>();
    CountDownLatch closingRegionLatch = new CountDownLatch(1);
    CountDownLatch allowCompactorLatch = new CountDownLatch(1);

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      private final CountDownLatch compactorSignalledLatch = new CountDownLatch(1);
      private final CountDownLatch compactorCompletedLatch = new CountDownLatch(1);
      private volatile int oplogSizeBeforeRolling;

      @Override
      public void beforeGoingToCompact() {
        try {
          // wait for operations to get over
          awaitLatch(allowCompactorLatch);

          DiskRegion diskRegion = getDiskRegion(region);
          oplogSizeBeforeRolling = diskRegion.getOplogIdToOplog().size();
          assertThat(oplogSizeBeforeRolling).isGreaterThan(0);

          closeRegionFuture.set(executorServiceRule.runAsync(() -> {
            closingRegionLatch.countDown();
            DiskStoreImpl diskStoreImpl = getDiskStore(region);
            region.close();
            diskStoreImpl.close();
          }));

          // wait for th to call afterSignallingCompactor
          awaitLatch(compactorSignalledLatch);
        } catch (AssertionError | Exception e) {
          errorCollector.addError(e);
        }
      }

      @Override
      public void afterSignallingCompactor() {
        try {
          compactorSignalledLatch.countDown();
        } catch (AssertionError | Exception e) {
          errorCollector.addError(e);
        }
      }

      @Override
      public void afterStoppingCompactor() {
        try {
          awaitLatch(compactorCompletedLatch);
        } catch (AssertionError | Exception e) {
          errorCollector.addError(e);
        }
      }

      @Override
      public void afterHavingCompacted() {
        try {
          compactorCompletedLatch.countDown();
          DiskRegion diskRegion = getDiskRegion(region);
          assertThat(diskRegion.getOplogIdToOplog()).hasSize(oplogSizeBeforeRolling);
        } catch (AssertionError | Exception e) {
          errorCollector.addError(e);
        }
      }
    });

    // create some string entries
    for (int i = 0; i < 100; ++i) {
      region.put(String.valueOf(i), String.valueOf(i));
    }

    allowCompactorLatch.countDown();
    awaitLatch(closingRegionLatch);
    awaitFuture(closeRegionFuture);
  }

  @Test
  public void throwsIllegalStateExceptionIfMissingOplogDrf() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    DiskStore diskStore = createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<Object, Object> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    region.close();
    closeDiskStore(diskStore);

    Path oplogFile = Files.list(diskDirs[0].toPath())
        .filter(path -> path.toString().endsWith(".drf")).findFirst().get();

    Files.delete(oplogFile);

    String expectedMessage =
        "The following required files could not be found: *.drf files with these ids:";

    Throwable thrown = catchThrowable(() -> createDiskStoreWithSizeInBytes(diskStoreName,
        toDiskStoreFactory(diskRegionProperties)));
    assertThat(thrown).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(expectedMessage).hasNoCause();
  }

  @Test
  public void throwsIllegalStateExceptionIfMissingOplogCrf() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    DiskStore diskStore = createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<Object, Object> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    region.close();
    closeDiskStore(diskStore);

    Path oplogFile = Files.list(diskDirs[0].toPath())
        .filter(path -> path.toString().endsWith(".crf")).findFirst().get();

    Files.delete(oplogFile);

    String expectedMessage =
        "The following required files could not be found: *.crf files with these ids:";

    Throwable thrown = catchThrowable(() -> createDiskStoreWithSizeInBytes(diskStoreName,
        toDiskStoreFactory(diskRegionProperties)));
    assertThat(thrown).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(expectedMessage).hasNoCause();
  }

  @Test
  public void testNoTerminationOfCompactorTillRollingCompleted() throws Exception {
    System.setProperty(DiskStoreImpl.COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME, "true");

    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setCompactionThreshold(100);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setMaxOplogSize(100);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 100);

    Region<Object, Object> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    AtomicReference<Future<Void>> closeRegionFuture = new AtomicReference<>();
    CountDownLatch closeThreadStartedLatch = new CountDownLatch(1);
    CountDownLatch allowCompactorLatch = new CountDownLatch(1);

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      private final CountDownLatch compactorSignalledLatch = new CountDownLatch(1);
      private volatile int oplogSizeBeforeRolling;

      @Override
      public void beforeGoingToCompact() {
        try {
          // wait for operations to get over
          awaitLatch(allowCompactorLatch);

          DiskRegion diskRegion = getDiskRegion(region);
          oplogSizeBeforeRolling = diskRegion.getOplogIdToOplog().size();
          assertThat(oplogSizeBeforeRolling).isGreaterThan(0);

          closeRegionFuture.set(executorServiceRule.runAsync(() -> {
            DiskStoreImpl diskStore = getDiskStore(region);
            region.close();
            diskStore.close();
          }));

          closeThreadStartedLatch.countDown();

          // wait for th to call afterSignallingCompactor
          awaitLatch(compactorSignalledLatch);
        } catch (AssertionError | Exception e) {
          errorCollector.addError(e);
        }
      }

      @Override
      public void afterSignallingCompactor() {
        try {
          compactorSignalledLatch.countDown();
        } catch (AssertionError | Exception e) {
          errorCollector.addError(e);
        }
      }

      @Override
      public void afterHavingCompacted() {
        try {
          DiskRegion diskRegion = getDiskRegion(region);
          assertThat(diskRegion.getOplogIdToOplog()).hasSize(oplogSizeBeforeRolling);
        } catch (AssertionError | Exception e) {
          errorCollector.addError(e);
        }
      }
    });

    // create some string entries
    for (int i = 0; i < 100; ++i) {
      region.put(String.valueOf(i), String.valueOf(i));
    }

    allowCompactorLatch.countDown();
    awaitLatch(closeThreadStartedLatch);
    awaitFuture(closeRegionFuture);
  }

  /**
   * Regression test for TRAC #40648: oplog rolling fails reading with Bad file descriptor
   */
  @Test
  public void testBug40648part1() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setMaxOplogSize(500 * 2);
    diskRegionProperties.setOverFlowCapacity(1);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 1000);

    Region<Integer, Object> region =
        createRegion(regionName, diskStoreName, true, true, false, true, 1);

    // long loop and no assertions just to verify nothing throws

    byte[] payload = new byte[100];
    for (int i = 0; i < 1000; i++) {
      region.put(0, payload);
      region.put(1, payload);
    }
  }

  /**
   * Regression test for TRAC #40648: oplog rolling fails reading with Bad file descriptor
   *
   * <p>
   * Same as part1 but no persistence.
   */
  @Test
  public void testBug40648part2() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setMaxOplogSize(500 * 2);
    diskRegionProperties.setOverFlowCapacity(1);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 1000);

    Region<Integer, Object> region =
        createRegion(regionName, diskStoreName, false, true, false, true, 1);

    // long loop and no assertions just to verify nothing throws

    byte[] payload = new byte[100];
    for (int i = 0; i < 1000; i++) {
      region.put(0, payload);
      region.put(1, payload);
    }
  }

  @Test
  public void testForceCompactionDoesRoll() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setAllowForceCompaction(true);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    DiskStore diskStore = getDiskStore(region);

    assertThat(diskStore.forceCompaction()).isFalse();

    region.put("key1", "value1");
    region.put("key2", "value2");

    assertThat(diskStore.forceCompaction()).isFalse();

    region.remove("key1");
    region.remove("key2");

    // the following forceCompaction should go ahead and do a roll and compact it
    boolean compacted = diskStore.forceCompaction();
    assertThat(compacted).isTrue();
  }

  /**
   * Confirm that forceCompaction waits for the compaction to finish
   */
  @Test
  public void testNonDefaultCompaction() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setAllowForceCompaction(true);
    diskRegionProperties.setPersistBackup(true);
    diskRegionProperties.setCompactionThreshold(90);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    region.put("key1", "value1");
    region.put("key2", "value2");

    // Only remove 1 of the entries. This wouldn't trigger compaction with the default threshold,
    // since there are two entries.
    region.remove("key1");

    // the following forceCompaction should go ahead and do a roll and compact it
    Oplog oplog = getDiskRegion(region).testHook_getChild();
    boolean compacted = getDiskStore(region).forceCompaction();

    assertThat(oplog.testConfirmCompacted()).isTrue();
    assertThat(compacted).isTrue();
  }

  /**
   * Confirm that forceCompaction waits for the compaction to finish
   */
  @Test
  public void testForceCompactionIsSync() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setAllowForceCompaction(true);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    region.put("key1", "value1");
    region.put("key2", "value2");
    region.remove("key1");
    region.remove("key2");

    // now that it is compactable the following forceCompaction should
    // go ahead and do a roll and compact it.
    Oplog oplog = getDiskRegion(region).testHook_getChild();
    boolean compacted = getDiskStore(region).forceCompaction();

    assertThat(oplog.testConfirmCompacted()).isTrue();
    assertThat(compacted).isTrue();

    CachePerfStats stats = cache.getCachePerfStats();
    assertThat(stats.getDiskTasksWaiting()).isGreaterThanOrEqualTo(0);
  }

  /**
   * Regression test for TRAC #40876: diskReg tests (disk persistence and overflow-to-disk) fail
   * when an invalidated entry is recovered from disk
   */
  @Test
  public void testBug40876() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    region.put("key1", "value1");
    region.invalidate("key1");
    region.close();

    region = createRegion(regionName, diskStoreName, true, true, false, false, 0);

    Object value = ((InternalPersistentRegion) region).getValueOnDiskOrBuffer("key1");
    assertThat(value).isEqualTo(Token.INVALID);
    assertThat(region.containsValueForKey("key1")).isFalse();
  }

  /**
   * Regression test for TRAC #41822: Only one disk directory is being used when multiple
   * directories are specified
   *
   * <p>
   * Make sure oplog created by recovery goes in the proper directory
   */
  @Test
  public void testBug41822() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setMaxOplogSize(500);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    DiskStore diskStore = createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 500);

    Region<String, Object> region =
        createRegion(regionName, diskStoreName, true, true, false, false, 0);

    byte[] payload = new byte[100];

    region.put("key0", payload);
    assertThat(getDiskRegion(region).testHook_getChild().getDirectoryHolder().getDir())
        .isEqualTo(diskDirs[0]);

    region.close();
    closeDiskStore(diskStore);

    diskStore = createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 500);
    region = createRegion(regionName, diskStoreName, true, true, false, false, 0);
    region.put("key1", payload);

    assertThat(getDiskRegion(region).testHook_getChild().getDirectoryHolder().getDir())
        .isEqualTo(diskDirs[1]);

    region.close();
    closeDiskStore(diskStore);

    diskStore = createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 500);
    region = createRegion(regionName, diskStoreName, true, true, false, false, 0);
    region.put("key2", payload);

    assertThat(getDiskRegion(region).testHook_getChild().getDirectoryHolder().getDir())
        .isEqualTo(diskDirs[2]);

    region.close();
    closeDiskStore(diskStore);

    diskStore = createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 500);
    region = createRegion(regionName, diskStoreName, true, true, false, false, 0);
    region.put("key3", payload);

    assertThat(getDiskRegion(region).testHook_getChild().getDirectoryHolder().getDir())
        .isEqualTo(diskDirs[3]);

    region.close();
    closeDiskStore(diskStore);

    diskStore = createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 500);
    region = createRegion(regionName, diskStoreName, true, true, false, false, 0);
    region.put("key4", payload);

    assertThat(getDiskRegion(region).testHook_getChild().getDirectoryHolder().getDir())
        .isEqualTo(diskDirs[0]);
  }

  /**
   * Regression test for TRAC #41770: ConcurrentRegionOperationsJUnitTest fails with duplicate
   * create in an oplog
   */
  @Test
  public void testBug41770() throws Exception {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setRegionName(regionName);
    diskRegionProperties.setOverflow(false);
    diskRegionProperties.setRolling(false);
    diskRegionProperties.setDiskDirs(diskDirs);
    diskRegionProperties.setPersistBackup(true);

    DiskStoreFactory diskStoreFactory = toDiskStoreFactory(diskRegionProperties);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory);

    Region<String, String> region =
        createRegion(regionName, diskStoreName, true, false, false, false, 0);

    // Install a listener then gets called when the async flusher threads finds an entry to flush
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    DoRegionClearDuringDiskStoreFlush doRegionClearDuringDiskStoreFlush =
        new DoRegionClearDuringDiskStoreFlush(region);
    CacheObserverHolder.setInstance(doRegionClearDuringDiskStoreFlush);

    region.create("KEY", "VALUE1");
    getDiskStore(region).forceFlush();

    doRegionClearDuringDiskStoreFlush.waitForCompletion();
    // we should now have two creates in our oplog.

    region.close();

    // do a recovery it will fail with an assertion if this bug is not fixed
    Throwable thrown =
        catchThrowable(() -> createRegion(regionName, diskStoreName, true, false, false, false, 0));
    assertThat(thrown).isNull();
  }

  private void awaitLatch(CountDownLatch latch) throws InterruptedException {
    assertThat(latch.await(5, MINUTES)).as("Timed out awaiting latch").isTrue();
  }

  private void awaitFuture(AtomicReference<Future<Void>> voidFuture)
      throws InterruptedException, ExecutionException, TimeoutException {
    await().until(() -> voidFuture.get() != null);
    awaitFuture(voidFuture.get());
  }

  private void awaitFuture(Future<Void> voidFuture)
      throws InterruptedException, ExecutionException, TimeoutException {
    voidFuture.get(5, MINUTES);
  }

  private File newFolder(String folder) {
    try {
      return temporaryFolder.newFolder(folder);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private File createDirectory(File parentDirectory, String name) {
    File file = new File(parentDirectory, name);
    assertThat(file.mkdir()).isTrue();
    return file;
  }

  private <K, V> Region<K, V> createRegion(String regionName,
      String diskStoreName,
      boolean isDataPolicyPersistent,
      boolean isDataSynchronous, boolean isHeapEviction,
      boolean isOverflowEviction,
      int overflowCapacity) {
    RegionFactory<K, V> regionFactory = cache.createRegionFactory(LOCAL);

    if (isDataPolicyPersistent) {
      regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    }

    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(isDataSynchronous);

    if (isHeapEviction) {
      regionFactory.setEvictionAttributes(heapEvictionAttributes);
    } else if (isOverflowEviction) {
      regionFactory.setEvictionAttributes(createOverflowEvictionAttributes(overflowCapacity));
    }

    return regionFactory.create(regionName);
  }

  private EvictionAttributes createOverflowEvictionAttributes(int overflowCapacity) {
    return createLRUEntryAttributes(overflowCapacity, OVERFLOW_TO_DISK);
  }

  private DiskStoreFactory toDiskStoreFactory(DiskRegionProperties diskRegionProperties) {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();

    diskStoreFactory.setAllowForceCompaction(diskRegionProperties.getAllowForceCompaction());
    diskStoreFactory.setAutoCompact(diskRegionProperties.isRolling());
    diskStoreFactory.setCompactionThreshold(diskRegionProperties.getCompactionThreshold());

    if (diskRegionProperties.getDiskDirSizes() == null) {
      int[] diskDirSizes = new int[diskRegionProperties.getDiskDirs().length];
      Arrays.fill(diskDirSizes, Integer.MAX_VALUE);
      diskStoreFactory.setDiskDirsAndSizes(diskRegionProperties.getDiskDirs(), diskDirSizes);
    } else {
      diskStoreFactory.setDiskDirsAndSizes(diskRegionProperties.getDiskDirs(),
          diskRegionProperties.getDiskDirSizes());
    }

    if (diskRegionProperties.getBytesThreshold() > Integer.MAX_VALUE) {
      diskStoreFactory.setQueueSize(Integer.MAX_VALUE);
    } else {
      diskStoreFactory.setQueueSize((int) diskRegionProperties.getBytesThreshold());
    }

    if (diskRegionProperties.getTimeInterval() != -1) {
      diskStoreFactory.setTimeInterval(diskRegionProperties.getTimeInterval());
    }

    return diskStoreFactory;
  }

  private DiskStore createDiskStoreWithSizeInBytes(String diskStoreName,
      DiskStoreFactory diskStoreFactory) {
    return createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, MAX_OPLOG_SIZE_IN_BYTES);
  }

  private DiskStore createDiskStoreWithSizeInBytes(String diskStoreName,
      DiskStoreFactory diskStoreFactory,
      long maxOplogSizeInBytes) {
    ((DiskStoreFactoryImpl) diskStoreFactory).setMaxOplogSizeInBytes(maxOplogSizeInBytes);
    DirectoryHolder.SET_DIRECTORY_SIZE_IN_BYTES_FOR_TESTING_PURPOSES = true;
    try {
      return diskStoreFactory.create(diskStoreName);
    } finally {
      DirectoryHolder.SET_DIRECTORY_SIZE_IN_BYTES_FOR_TESTING_PURPOSES = false;
    }
  }

  private DiskStoreImpl toDiskStoreImpl(DiskStore diskStore) {
    return (DiskStoreImpl) diskStore;
  }

  private DiskRegion getDiskRegion(Region region) {
    return ((InternalRegion) region).getDiskRegion();
  }

  private DiskStoreImpl getDiskStore(Region region) {
    return ((DiskRecoveryStore) region).getDiskStore();
  }

  private InternalRegion toInternalRegion(Region region) {
    return (InternalRegion) region;
  }

  private VMLRURegionMap getVMLRURegionMap(Region<?, ?> region) {
    return (VMLRURegionMap) toInternalRegion(region).getRegionMap();
  }

  private void closeDiskStore(DiskStore diskStore) {
    DiskStoreImpl internalDiskStore = (DiskStoreImpl) diskStore;
    internalDiskStore.close();

    cache.removeDiskStore(internalDiskStore);
  }

  private void closeOplogFileChannel(Region<?, ?> region) throws IOException {
    // Get the oplog handle & hence the underlying file & close it
    UninterruptibleFileChannel oplogFileChannel =
        getDiskRegion(region).testHook_getChild().getFileChannel();
    oplogFileChannel.close();
  }

  private void assertThatArrayEquals(Object expected, Object actual) {
    assertThat(actual).isInstanceOf(expected.getClass());

    int actualLength = Array.getLength(actual);
    assertThat(actualLength).isEqualTo(Array.getLength(expected));

    for (int i = 0; i < actualLength; i++) {
      assertThat(Array.get(actual, i)).isEqualTo(Array.get(expected, i));
    }
  }

  private void verifyClosedDueToDiskAccessException(Region<?, ?> region) {
    await().until(() -> getDiskStore(region).isClosed());
    await().until(() -> cache.isClosed());
    Throwable thrown = catchThrowable(() -> cache.createRegionFactory().create(regionName));
    assertThat(thrown).isInstanceOf(CacheClosedException.class)
        .hasCauseInstanceOf(DiskAccessException.class);
  }

  private class DelayedGet implements Runnable {

    private final CountDownLatch goLatch;
    private final Region<Integer, ?> region;

    DelayedGet(CountDownLatch goLatch, Region<Integer, ?> region) {
      this.goLatch = goLatch;
      this.region = region;
    }

    @Override
    public void run() {
      try {
        awaitLatch(goLatch);
        region.get(0);
      } catch (AssertionError | Exception e) {
        errorCollector.addError(e);
      }
    }
  }

  private static class AfterDestroyListener<K, V> extends CacheListenerAdapter<K, V> {

    private volatile EntryEvent<K, V> lastEvent;

    @Override
    public void afterDestroy(EntryEvent<K, V> event) {
      lastEvent = event;
    }

    EntryEvent<K, V> getLastEvent() {
      return lastEvent;
    }
  }

  private class Puts implements Runnable {

    private final int dataSize;
    private final Region<String, byte[]> region;
    private final AtomicBoolean[] putSuccessful;

    private volatile boolean diskAccessExceptionOccurred;

    Puts(Region<String, byte[]> region) {
      this(region, 1024);
    }

    Puts(Region<String, byte[]> region, int dataSize) {
      this.region = region;
      this.dataSize = dataSize;
      putSuccessful =
          new AtomicBoolean[] {new AtomicBoolean(), new AtomicBoolean(), new AtomicBoolean()};
    }

    @Override
    public void run() {
      try {
        performPuts();
      } catch (AssertionError | Exception e) {
        errorCollector.addError(e);
      }
    }

    boolean diskAccessExceptionOccurred() {
      return diskAccessExceptionOccurred;
    }

    boolean putSuccessful(int index) {
      return putSuccessful[index].get();
    }

    void performPuts() {
      diskAccessExceptionOccurred = false;
      putSuccessful[0].set(false);
      putSuccessful[1].set(false);
      putSuccessful[2].set(false);

      try {
        byte[] bytes = new byte[dataSize];
        region.put("1", bytes);
        putSuccessful[0].set(true);
        region.put("2", bytes);
        putSuccessful[1].set(true);
        region.put("3", bytes);
        putSuccessful[2].set(true);
      } catch (DiskAccessException e) {
        diskAccessExceptionOccurred = true;
      }
    }
  }

  /**
   * Performs Region clear during DiskStore flush and then performs create after flushing.
   *
   * <p>
   * TRAC #41770: ConcurrentRegionOperationsJUnitTest fails with duplicate create in an oplog
   */
  private class DoRegionClearDuringDiskStoreFlush extends CacheObserverAdapter {

    private final CountDownLatch completedLatch = new CountDownLatch(1);
    private final Region<String, String> region;

    private boolean cleared;

    DoRegionClearDuringDiskStoreFlush(Region<String, String> region) {
      this.region = region;
    }

    @Override
    public synchronized void afterWritingBytes() {
      try {
        if (cleared) {
          CacheObserverHolder.setInstance(null);
          // now that the flusher finished the async create of VALUE1 do another create
          region.create("KEY", "VALUE2");
          completedLatch.countDown();
        }
      } catch (AssertionError | Exception e) {
        errorCollector.addError(e);
      }
    }

    @Override
    public synchronized void goingToFlush() {
      try {
        if (!cleared) {
          // once the flusher is stuck in our listener do a region clear
          region.clear();
          cleared = true;
        }
      } catch (AssertionError | Exception e) {
        errorCollector.addError(e);
      }
    }

    synchronized void waitForCompletion() throws InterruptedException {
      awaitLatch(completedLatch);
    }
  }
}
