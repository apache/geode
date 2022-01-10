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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;

/**
 * Verifies that automatic compaction works after cache recovered from oplogs
 */
public class DiskRegionCompactorClearOplogAfterRecoveryIntegrationTest {

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

  private static final int ENTRY_RANGE = 350;

  @Before
  public void setUp() throws Exception {
    String uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    diskStoreName = uniqueName + "_diskStore";

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
    }
  }

  /**
   * Verifies that compaction works as expected after region is recovered
   **/
  @Test
  public void testThatCompactionWorksAfterRegionIsClosedAndThenRecovered()
      throws InterruptedException {

    createDiskStore(30, 10000);
    Region<Object, Object> region = createRegion();
    DiskStoreImpl diskStore = ((InternalRegion) region).getDiskStore();

    // Create several oplog files (.crf and .drf) by executing put operations in defined range
    executePutOperations(region);
    await().untilAsserted(() -> assertThat(getCurrentNumberOfOplogs(diskStore)).isEqualTo(5));

    Set<Long> oplogIds = getAllOplogIds(diskStore);

    region.close();
    region = createRegion();

    // Execute destroy operations for all entries created in previous step. All oplogs created
    // in previous step will not contain live entries, so they must be compacted.
    executeDestroyOperations(region);

    await().untilAsserted(
        () -> assertThat(areOplogsCompacted(oplogIds, diskStore)).isTrue());
  }

  boolean areOplogsCompacted(Set<Long> oplogIds, DiskStoreImpl diskStore) {
    Set<Long> currentOplogId = getAllOplogIds(diskStore);
    return currentOplogId.stream().noneMatch(oplogIds::contains);
  }

  Set<Long> getAllOplogIds(DiskStoreImpl diskStore) {
    Set<Long> oplogIds = new HashSet<>();
    for (Oplog oplog : diskStore.getAllOplogsForBackup()) {
      oplogIds.add(oplog.getOplogId());
    }
    return oplogIds;
  }


  void executePutOperations(Region<Object, Object> region) {
    for (int i = 0; i < ENTRY_RANGE; i++) {
      region.put(i, new byte[100]);
    }
  }

  void executeDestroyOperations(Region<Object, Object> region) throws InterruptedException {
    TombstoneService tombstoneService = ((InternalCache) cache).getTombstoneService();
    for (int i = 0; i < ENTRY_RANGE; i++) {
      region.destroy(i);
      assertThat(tombstoneService.forceBatchExpirationForTests(1)).isTrue();
    }
  }

  void createDiskStore(int compactionThreshold, int maxOplogSizeInBytes) {
    DiskStoreFactoryImpl diskStoreFactory = (DiskStoreFactoryImpl) cache.createDiskStoreFactory();
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

  int getCurrentNumberOfOplogs(DiskStoreImpl ds) {
    return ds.getAllOplogsForBackup().length;
  }

  private File createDirectory(File parentDirectory, String name) {
    File file = new File(parentDirectory, name);
    assertThat(file.mkdir()).isTrue();
    return file;
  }

  private void createDiskStoreWithSizeInBytes(String diskStoreName,
      DiskStoreFactoryImpl diskStoreFactory,
      long maxOplogSizeInBytes) {
    diskStoreFactory.setMaxOplogSizeInBytes(maxOplogSizeInBytes);
    diskStoreFactory.setDiskDirSizesUnit(DiskDirSizesUnit.BYTES);
    diskStoreFactory.create(diskStoreName);
  }
}
