/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;

/**
 * Verifies that the unnecessary memory is cleared when operational log is compacted.
 */
public class DiskRegionCompactorClearsObjectThatAreNoLongerNeededIntegrationTest {

  private final Properties config = new Properties();
  private Cache cache;

  private File[] diskDirs;
  private int[] diskDirSizes;

  private String regionName;
  private String diskStoreName;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  private static final int ENTRY_RANGE_0_299 = 300;
  private static final int ENTRY_RANGE_300_599 = 600;

  @Before
  public void setUp() throws Exception {
    String uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    diskStoreName = uniqueName + "_diskStore";

    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");

    cache = new CacheFactory(config).create();

    diskDirs = new File[1];
    diskDirs[0] = createDirectory(temporaryFolder.getRoot(), testName.getMethodName());
    diskDirSizes = new int[1];
    Arrays.fill(diskDirSizes, Integer.MAX_VALUE);

    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
    TombstoneService.EXPIRED_TOMBSTONE_LIMIT = 1;
    TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT = 1;
  }

  @After
  public void tearDown() throws Exception {
    try {
      cache.close();
    } finally {
      DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;
      disconnectAllFromDS();
    }
  }

  /**
   * Verifies that the unnecessary memory is cleared when operational log (.crf adn .drf) is
   * compacted.
   * This test case covers the following scenario:
   *
   * 1. Create several Oplog files (.crf, .drf and .krf) by executing put operations
   * 2. Execute destroy operation for every fifth entry, and each time add new entry. This will
   * result with few additional Oplog files. Compaction threshold will not be reached.
   * 3. Destroy all operations created in step 2. This will trigger compaction of files that
   * were created in step 2. Compaction will delete only .crf and .krf files, but will not
   * delete .drf files because they contain destroy operations for events located in
   * .crf files crated in step 1.
   * 4. Check that unnecessary objects are cleared for theOplog that represents orphaned .drf
   * file (no accompanying .crf and .krf file)
   **/
  @Test
  public void testCompactorRegionMapDeletedForOnlyDrfOplogAfterCompactionIsDone()
      throws InterruptedException {

    createDiskStore(30, 10000);
    Region<Object, Object> region = createRegion();
    DiskStoreImpl diskStore = ((LocalRegion) region).getDiskStore();

    // Create several oplog files (.crf and .drf) by executing put operations in defined range
    executePutsInRange0_299(region);
    await().untilAsserted(() -> assertThat(getCurrentNumberOfOplogs(diskStore)).isEqualTo(5));

    // Destroy every fifth entry from previous range and each time put new entry in new range.
    // This will create additional oplog files (.crf and .drf), but compaction will not be triggered
    // as threshold will not be reached. Oplog files (.drf) created in this step will contain
    // destroys for events that are located in .crf files from previous range.
    destroyEveryFifthElementInRange0_299AndEachTimePutInRange300_599(region);
    await().untilAsserted(() -> assertThat(getCurrentNumberOfOplogs(diskStore)).isEqualTo(7));

    // Destroy all events created in previous step in order to trigger automatic compaction.
    // This will trigger compaction for the files that were created in previous step.
    // Compaction will delete .crf and .krf file, but will leave .drf file because it contains
    // destroy operation for the events that are located in some older .crf files.
    destroyEveryFifthElementInRange300_599(region);

    // wait for all Oplog's to be compacted
    await().untilAsserted(() -> assertThat(isOplogToBeCompactedAvailable(diskStore)).isFalse());

    await().untilAsserted(
        () -> assertThat(areAllUnnecessaryObjectClearedForOnlyDrfOplog(diskStore)).isTrue());
  }

  /**
   * Verifies that the unnecessary memory is cleared when operational log (.crf adn .drf) is
   * compacted. This test case covers the following scenario:
   *
   * 1. Create several Oplog files (.crf, .drf and .krf) by executing put operations
   * 2. Execute destroy operation for every fifth entry from previous step, and each time add and
   * destroy new entry. This will result with Oplogs that have 0 live entries at the moment when
   * they are rolled out. These Oplogs will be closed early since compaction doesn't have to be
   * done for them. When Oplog is closed early the .crf files will be deleted, but .drf files will
   * not because they contain deletes.
   * 4. Check that unnecessary objects are cleared for theOplog that represents orphaned .drf
   * file (no accompanying .crf and .krf file)
   **/
  @Test
  public void testCompactorRegionMapDeletedForOnlyDrfOplogAfterCompactionNotDoneDueToLiveCountZero()
      throws InterruptedException {

    createDiskStore(5, 10000);
    Region<Object, Object> region = createRegion();
    DiskStoreImpl diskStore = ((LocalRegion) region).getDiskStore();

    // Create several oplog files (.crf and .drf) by executing put operations in defined range
    executePutsInRange0_299(region);
    await().untilAsserted(() -> assertThat(getCurrentNumberOfOplogs(diskStore)).isEqualTo(5));

    // Do multiple put and destroy operations to hit the scenario in which there are 0
    // live entries are in Oplog just before compaction is triggered.
    destroyEveryFifthElementInRange0_299AndEachTimePutAndDestroyInRange300_599(region);

    await().untilAsserted(() -> assertThat(isOplogToBeCompactedAvailable(diskStore)).isFalse());

    await().untilAsserted(
        () -> assertThat(areAllUnnecessaryObjectClearedForOnlyDrfOplog(diskStore)).isTrue());
  }

  /**
   * Verifies that the unnecessary memory is cleared when operational log (.crf and .drf) is
   * compacted.This is special scenario were creation of .krf file is cancelled by ongoing
   * compaction. This usually happens when new oplog is rolled out and previous oplog is
   * immediately marked as eligible for compaction. Compaction and .krf creation start at the
   * similar time and compactor cancels creation of .krf if it is executed first.
   *
   * This test case covers the following scenario:
   *
   * 1. Create several Oplog files (.crf, .drf and .krf) by executing put operations.
   * 2. Execute destroy operation for every fifth entry, and each time add new entry. When
   * it is time for oplog to roll out, the previous oplog will be immediately marked as ready
   * for compaction because compaction threshold is set to high value in this case. This way
   * we force that compaction cancel creation of .krf file. Compaction will
   * delete only .crf file (.krf was not created at all), but will not delete .drf files because
   * they contain destroy operations for events located in .crf files created in step 1.
   * 3. Check that unnecessary objects are cleared for theOplog that represents orphaned .drf
   * file (no accompanying .crf and .krf file)
   **/
  @Test
  public void testCompactorRegionMapDeletedAfterCompactionForOnlyDrfOplogAndKrfCreationCanceledByCompactionIsDone() {

    createDiskStore(70, 10000);
    Region<Object, Object> region = createRegion();
    DiskStoreImpl diskStore = ((LocalRegion) region).getDiskStore();

    // Create several oplog files (.crf and .drf) by executing put operations in defined range
    executePutsInRange0_299(region);
    await().untilAsserted(() -> assertThat(getCurrentNumberOfOplogs(diskStore)).isEqualTo(5));

    destroyEveryFifthElementInRange0_299AndEachTimePutInRange300_599(region);
    await().untilAsserted(() -> assertThat(isOplogToBeCompactedAvailable(diskStore)).isFalse());

    await().untilAsserted(
        () -> assertThat(areAllUnnecessaryObjectClearedForOnlyDrfOplog(diskStore)).isTrue());
  }

  /**
   * Verifies that the region is recovered from Oplog's (including .drf only oplog's) when region is
   * closed and then recreated again in order to trigger recovery.
   * This test case covers the following scenario:
   *
   * 1. Create several Oplog files (.crf, .drf and .krf) by executing put operations
   * 2. Execute destroy operation for every fifth entry, and each time add new entry. This will
   * result with few additional Oplog files. Compaction threshold will not be reached.
   * 3. Destroy all operations created in step 2. This will trigger compaction of files that
   * were created in step 2. Compaction will delete only .crf and .krf files, but will not
   * delete .drf files because they contain destroy operations for events located in
   * .crf files created in step 1. Check that unnecessary objects are cleared for the
   * Oplog that represents orphaned .drf file (no accompanying .crf and .krf file)
   * 4. At this step there will be Oplog objects from step 1. that will contain all files (.crf,
   * .drf and .krf), and Oplog objects from step 2. that will contain only .drf files.
   * In order to test recovery, close the region and then recreate it again.
   * 5. Check that region is recovered correctly. Check that only events that have never been
   * destroyed are recovered from Oplog files.
   * 6. Check that unnecessary objects are cleared for theOplog that represents orphaned .drf
   * file (no accompanying .crf and .krf file)
   **/
  @Test
  public void testCompactorRegionMapDeletedForOnlyDrfOplogAfterCompactionAndRecoveryAfterRegionClose()
      throws InterruptedException {

    createDiskStore(30, 10000);
    Region<Object, Object> region = createRegion();
    DiskStoreImpl diskStore = ((LocalRegion) region).getDiskStore();

    // Create several oplog files (.crf and .drf) by executing put operations in defined range
    executePutsInRange0_299(region);
    await().untilAsserted(() -> assertThat(getCurrentNumberOfOplogs(diskStore)).isEqualTo(5));

    // Destroy every fifth entry from previous range and each time put new entry in new range.
    // This will create additional oplog files (.crf and .drf), but compaction will not be triggered
    // as threshold will not be reached. Oplog files (.drf) created in this step will contain
    // destroys for events that are located in .crf files from previous range.
    destroyEveryFifthElementInRange0_299AndEachTimePutInRange300_599(region);
    await().untilAsserted(() -> assertThat(getCurrentNumberOfOplogs(diskStore)).isEqualTo(7));

    // Destroy all events created in previous step in order to trigger automatic compaction.
    // This will trigger compaction for the files that were created in previous step.
    // Compaction will delete .crf and .krf file, but will leave .drf file because it contains
    // destroy operation for the events that are located in some older .crf files.
    destroyEveryFifthElementInRange300_599(region);

    // wait for all Oplog's to be compacted
    await().untilAsserted(() -> assertThat(isOplogToBeCompactedAvailable(diskStore)).isFalse());

    // close the region and then recreate it to trigger recovery from oplog files
    region.close();
    region = createRegion();

    // every fifth element is destroyed from range ENTRY_RANGE_1, so it is expected to have
    // ENTRY_RANGE_1 - (ENTRY_RANGE_1/5) of elements, also just in case check that every
    // fifth get operation return null
    checkThatAllRegionDataIsRecoveredFromOplogFiles(region);

    // check that unnecessary data is cleared from Oplog's
    await().untilAsserted(
        () -> assertThat(areAllUnnecessaryObjectClearedForOnlyDrfOplog(diskStore)).isTrue());
  }

  /**
   * Verifies that the region is recovered from Oplog's (including .drf only oplog's) when cache is
   * closed and then recreated again in order to trigger recovery.
   * This test case covers the following scenario:
   *
   * 1. Create several Oplog files (.crf, .drf and .krf) by executing put operations
   * 2. Execute destroy operation for every fifth entry, and each time add new entry. This will
   * result with few additional Oplog files. Compaction threshold will not be reached.
   * 3. Destroy all operations created in step 2. This will trigger compaction of files that
   * were created in step 2. Compaction will delete only .crf and .krf files, but will not
   * delete .drf files because they contain destroy operations for events located in
   * .crf files created in step 1.
   * 4. At this step there will be Oplog objects from step 1. that will contain all files (.crf,
   * .drf and .krf), and Oplog objects from step 2. that will contain only .drf files.
   * In order to test recovery, close the cache and then recreate it again.
   * 5. Check that region is recovered correctly. Check that only events that have never been
   * destroyed are recovered from Oplog files.
   * 6. Check that unnecessary objects are cleared for theOplog that represents orphaned .drf
   * file (no accompanying .crf and .krf file)
   **/
  @Test
  public void testCompactorRegionMapDeletedForOnlyDrfOplogAfterCompactionAndRecoveryAfterCacheClosed()
      throws InterruptedException {

    createDiskStore(30, 10000);
    Region<Object, Object> region = createRegion();
    DiskStoreImpl diskStore = ((LocalRegion) region).getDiskStore();

    // Create several oplog files (.crf and .drf) by executing put operations in defined range
    executePutsInRange0_299(region);
    await().untilAsserted(() -> assertThat(getCurrentNumberOfOplogs(diskStore)).isEqualTo(5));

    // Destroy every fifth entry from previous range and each time put new entry in new range.
    // This will create additional oplog files (.crf and .drf), but compaction will not be triggered
    // as threshold will not be reached. Oplog files (.drf) created in this step will contain
    // destroys for events that are located in .crf files from previous range.
    destroyEveryFifthElementInRange0_299AndEachTimePutInRange300_599(region);
    await().untilAsserted(() -> assertThat(getCurrentNumberOfOplogs(diskStore)).isEqualTo(7));

    // Destroy all events created in previous step in order to trigger automatic compaction.
    // This will trigger compaction for the files that were created in previous step.
    // Compaction will delete .crf and .krf file, but will leave .drf file because it contains
    // destroy operation for the events that are located in some older .crf files.
    destroyEveryFifthElementInRange300_599(region);

    // wait for all Oplog's to be compacted
    await().untilAsserted(() -> assertThat(isOplogToBeCompactedAvailable(diskStore)).isFalse());

    // close the cache and then recreate it again to trigger recovery from oplog files
    cache.close();
    cache = new CacheFactory(config).create();
    createDiskStore(30, 10000);
    region = createRegion();

    // every fifth element is destroyed from range ENTRY_RANGE_1, so it is expected to have
    // ENTRY_RANGE_1 - (ENTRY_RANGE_1/5) of elements, also just in case check that every
    // fifth get operation return null
    checkThatAllRegionDataIsRecoveredFromOplogFiles(region);

    await().untilAsserted(
        () -> assertThat(areAllUnnecessaryObjectClearedForOnlyDrfOplog(diskStore)).isTrue());
  }

  void checkThatAllRegionDataIsRecoveredFromOplogFiles(Region<Object, Object> region) {
    int objectCount = 0;
    for (int i = 0; i < ENTRY_RANGE_0_299; i++) {
      Object object = region.get(i);
      if (i % 5 == 0) {
        assertThat(object).isNull();
      }
      if (object != null) {
        objectCount++;
      }
    }
    assertThat(objectCount).isEqualTo(ENTRY_RANGE_0_299 - (ENTRY_RANGE_0_299 / 5));
  }

  void executePutsInRange0_299(Region<Object, Object> region) {
    for (int i = 0; i < ENTRY_RANGE_0_299; i++) {
      region.put(i, new byte[100]);
    }
  }

  void destroyEveryFifthElementInRange0_299AndEachTimePutAndDestroyInRange300_599(
      Region<Object, Object> region)
      throws InterruptedException {
    TombstoneService tombstoneService = ((InternalCache) cache).getTombstoneService();
    for (int i = ENTRY_RANGE_0_299; i < ENTRY_RANGE_300_599; i++) {
      region.put(i, new byte[300]);
      region.destroy(i);
      assertThat(tombstoneService.forceBatchExpirationForTests(1)).isTrue();
      if (i % 5 == 0) {
        region.destroy(i - ENTRY_RANGE_0_299);
        assertThat(tombstoneService.forceBatchExpirationForTests(1)).isTrue();
      }
    }
  }

  void destroyEveryFifthElementInRange300_599(Region<Object, Object> region)
      throws InterruptedException {
    TombstoneService tombstoneService = ((InternalCache) cache).getTombstoneService();
    int key = ENTRY_RANGE_0_299;
    while (key < ENTRY_RANGE_300_599) {
      region.destroy(key);
      assertThat(tombstoneService.forceBatchExpirationForTests(1)).isTrue();
      key = key + 5;
    }
  }

  void destroyEveryFifthElementInRange0_299AndEachTimePutInRange300_599(
      Region<Object, Object> region) {
    TombstoneService tombstoneService = ((InternalCache) cache).getTombstoneService();
    int key = 0;
    while (key < ENTRY_RANGE_0_299) {
      region.destroy(key);
      // It is necessary to force tombstone expiration, otherwise event won't be stored in .drf file
      // and total live count won't be decreased
      await().untilAsserted(
          () -> assertThat(tombstoneService.forceBatchExpirationForTests(1)).isTrue());
      region.put(key + ENTRY_RANGE_0_299, new byte[300]);
      key = key + 5;
    }
  }

  boolean areAllUnnecessaryObjectClearedForOnlyDrfOplog(DiskStoreImpl diskStore) {
    boolean isClear = true;
    for (Oplog oplog : diskStore.getAllOplogsForBackup()) {
      if (oplog.getHasDeletes() && oplog.isDeleted() && oplog.hasNoLiveValues()) {
        if (oplog.getRegionMapSize() != 0)
          isClear = false;
      }
    }
    return isClear;
  }

  void createDiskStore(int compactionThreshold, int maxOplogSizeInBytes) {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setAutoCompact(true);
    diskStoreFactory.setCompactionThreshold(compactionThreshold);
    diskStoreFactory.setDiskDirsAndSizes(diskDirs, diskDirSizes);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, maxOplogSizeInBytes);
  }

  Region<Object, Object> createRegion() {
    RegionFactory<Object, Object> regionFactory =
        cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT);
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(true);
    return regionFactory.create(regionName);
  }

  boolean isOplogToBeCompactedAvailable(DiskStoreImpl ds) {
    if (ds.getOplogToBeCompacted() == null) {
      return false;
    }
    return ds.getOplogToBeCompacted().length > 0;
  }

  int getCurrentNumberOfOplogs(DiskStoreImpl ds) {
    return ds.getAllOplogsForBackup().length;
  }

  private File createDirectory(File parentDirectory, String name) {
    File file = new File(parentDirectory, name);
    assertThat(file.mkdir()).isTrue();
    return file;
  }

  private void createDiskStoreWithSizeInBytes(String diskStoreName,
      DiskStoreFactory diskStoreFactory,
      long maxOplogSizeInBytes) {
    ((DiskStoreFactoryImpl) diskStoreFactory).setMaxOplogSizeInBytes(maxOplogSizeInBytes);
    ((DiskStoreFactoryImpl) diskStoreFactory).setDiskDirSizesUnit(DiskDirSizesUnit.BYTES);
    diskStoreFactory.create(diskStoreName);
  }
}
