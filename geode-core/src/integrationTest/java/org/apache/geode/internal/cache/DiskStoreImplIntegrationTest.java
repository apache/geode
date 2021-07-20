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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.backup.BackupService;
import org.apache.geode.management.internal.ManagementConstants;


public class DiskStoreImplIntegrationTest {
  private static final String DISK_STORE_NAME = "testDiskStore";
  private static final String REGION_NAME = "testRegion";
  private final int TIME_INTERVAL = 300000;

  @Rule
  public TemporaryFolder temporaryDirectory = new TemporaryFolder();

  private Cache cache;
  private Region aRegion;
  private DiskStoreStats diskStoreStats;
  final int NUM_ENTRIES = 100000;

  @Before
  public void setup() {
    cache = createCache();
  }

  @After
  public void tearDown() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
    }
  }

  @Test
  public void cleansUpOrphanedBackupFilesOnDiskStoreCreation() throws Exception {
    File baseDir = temporaryDirectory.newFolder();
    createRegionWithDiskStore(baseDir);
    DiskStore diskStore = cache.findDiskStore(DISK_STORE_NAME);

    List<Path> tempDirs = new ArrayList<>();
    for (File diskDir : diskStore.getDiskDirs()) {
      Path tempDir =
          diskDir.toPath().resolve(BackupService.TEMPORARY_DIRECTORY_FOR_BACKUPS + "testing");
      Files.createDirectories(tempDir);
      tempDirs.add(tempDir);
    }

    cache.close();
    cache = createCache();
    createRegionWithDiskStore(baseDir);

    tempDirs.forEach(tempDir -> assertThat(Files.exists(tempDir)).isFalse());
  }

  @Test
  public void queueSizeStatIncrementedAfterAsyncFlush() throws Exception {
    File baseDir = temporaryDirectory.newFolder();
    final int QUEUE_SIZE = 50;
    createRegionWithDiskStoreAndAsyncQueue(baseDir, QUEUE_SIZE);
    await().until(() -> diskStoreStats.getQueueSize() == 0);

    putEntries(QUEUE_SIZE - 1);
    await()
        .until(() -> diskStoreStats.getQueueSize() == QUEUE_SIZE - 1);

    putEntries(1);
    await().until(() -> diskStoreStats.getQueueSize() == 0);
  }

  @Test
  public void givenDiskStoreIsCreatedWithMaxSizeThenDiskUsagePercentagesShouldBeZeroAndDiskFreePercentagesShouldBe100()
      throws Exception {

    final int ALLOWED_MARGIN = 1;
    File[] diskDirs = new File[2];
    diskDirs[0] = temporaryDirectory.newFolder("dir1");
    diskDirs[1] = temporaryDirectory.newFolder("dir2");

    int[] diskDirSizes = new int[2];
    diskDirSizes[0] = 20;
    diskDirSizes[1] = 20;

    cache.createDiskStoreFactory().setMaxOplogSize(2).setDiskDirsAndSizes(diskDirs, diskDirSizes)
        .create(DISK_STORE_NAME);
    DiskStore diskStore = cache.findDiskStore(DISK_STORE_NAME);

    assertEquals(0, ((DiskStoreImpl) diskStore).getDiskUsagePercentage(), 0);
    assertEquals(100, ((DiskStoreImpl) diskStore).getDiskFreePercentage(),
        ALLOWED_MARGIN);
  }

  @Test
  public void givenDiskStoreIsCreatedWithMaxSizeWhenDataIsStoredThenDiskPercentagesShouldBeModified()
      throws Exception {

    final int ESTIMATED_USAGE_PERCENTAGE = 7; // Estimated disk percentage for NUM_ENTRIES
    final int ESTIMATED_FREE_PERCENTAGE = 100 - ESTIMATED_USAGE_PERCENTAGE;
    final int ALLOWED_MARGIN = 2;

    File[] diskDirs = new File[2];
    diskDirs[0] = temporaryDirectory.newFolder("dir1");
    diskDirs[1] = temporaryDirectory.newFolder("dir2");

    int[] diskDirSizes = new int[2];
    diskDirSizes[0] = 20;
    diskDirSizes[1] = 20;

    cache.createDiskStoreFactory().setMaxOplogSize(2).setDiskDirsAndSizes(diskDirs, diskDirSizes)
        .create(DISK_STORE_NAME);
    Region region = cache.<String, String>createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
        .setDiskStoreName(DISK_STORE_NAME).create(REGION_NAME);
    DiskStore diskStore = cache.findDiskStore(DISK_STORE_NAME);

    putEntries(region, NUM_ENTRIES);

    assertNotEquals(0, ((DiskStoreImpl) diskStore).getDiskUsagePercentage());

    assertEquals(ESTIMATED_USAGE_PERCENTAGE, ((DiskStoreImpl) diskStore).getDiskUsagePercentage(),
        ALLOWED_MARGIN);
    assertEquals(ESTIMATED_FREE_PERCENTAGE, ((DiskStoreImpl) diskStore).getDiskFreePercentage(),
        ALLOWED_MARGIN);
  }

  @Test
  public void givenDiskStoreIsCreatedWithDefaultSizeThenDiskPercentagesShouldNotBeAvailable()
      throws Exception {

    File[] diskDirs = new File[2];
    diskDirs[0] = temporaryDirectory.newFolder("dir1");
    diskDirs[1] = temporaryDirectory.newFolder("dir2");

    cache.createDiskStoreFactory().setDiskDirs(diskDirs)
        .create(DISK_STORE_NAME);
    DiskStore diskStore = cache.findDiskStore(DISK_STORE_NAME);

    assertEquals(ManagementConstants.NOT_AVAILABLE_FLOAT,
        ((DiskStoreImpl) diskStore).getDiskUsagePercentage(), 0);
    assertEquals(ManagementConstants.NOT_AVAILABLE_FLOAT,
        ((DiskStoreImpl) diskStore).getDiskFreePercentage(), 0);
  }

  @Test
  public void givenDiskStoreIsCreatedWithDefaultSizeWhenDataIsStoredThenDiskPercentagesShouldNotBeAvailable()
      throws Exception {

    File[] diskDirs = new File[2];
    diskDirs[0] = temporaryDirectory.newFolder("dir1");
    diskDirs[1] = temporaryDirectory.newFolder("dir2");

    cache.createDiskStoreFactory().setDiskDirs(diskDirs)
        .create(DISK_STORE_NAME);
    DiskStore diskStore = cache.findDiskStore(DISK_STORE_NAME);
    Region region = cache.<String, String>createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
        .setDiskStoreName(DISK_STORE_NAME).create(REGION_NAME);

    putEntries(region, NUM_ENTRIES);

    assertEquals(ManagementConstants.NOT_AVAILABLE_FLOAT,
        ((DiskStoreImpl) diskStore).getDiskUsagePercentage(), 0);
    assertEquals(ManagementConstants.NOT_AVAILABLE_FLOAT,
        ((DiskStoreImpl) diskStore).getDiskFreePercentage(), 0);
  }

  @Test
  public void givenDiskStoreIsCreatedWithAtLeastOneDefaultSizeThenDiskPercentagesShouldNotBeAvailable()
      throws Exception {

    File[] diskDirs = new File[3];
    diskDirs[0] = temporaryDirectory.newFolder("dir1");
    diskDirs[1] = temporaryDirectory.newFolder("dir2");
    diskDirs[2] = temporaryDirectory.newFolder("dir3");

    int[] diskDirSizes;

    for (int i = 0; i < 3; i++) {
      cache = createCache();
      diskDirSizes = new int[] {10, 10, 10};
      diskDirSizes[i] = Integer.MAX_VALUE;

      cache.createDiskStoreFactory().setDiskDirsAndSizes(diskDirs, diskDirSizes)
          .create(DISK_STORE_NAME);
      DiskStore diskStore = cache.findDiskStore(DISK_STORE_NAME);

      assertEquals(ManagementConstants.NOT_AVAILABLE_FLOAT,
          ((DiskStoreImpl) diskStore).getDiskUsagePercentage(), 0);
      assertEquals(ManagementConstants.NOT_AVAILABLE_FLOAT,
          ((DiskStoreImpl) diskStore).getDiskFreePercentage(), 0);
      cache.close();
    }
  }

  @Test
  public void givenDiskStoreIsCreatedWithAtLeastOneDefaultSizeWhenDataIsStoredThenDiskPercentagesShouldNotBeAvailable()
      throws Exception {

    File[] diskDirs = new File[2];
    diskDirs[0] = temporaryDirectory.newFolder("dir1");
    diskDirs[1] = temporaryDirectory.newFolder("dir2");

    int[] diskDirSizes = new int[2];
    diskDirSizes[0] = 10;
    diskDirSizes[1] = Integer.MAX_VALUE;

    cache.createDiskStoreFactory().setDiskDirsAndSizes(diskDirs, diskDirSizes)
        .create(DISK_STORE_NAME);
    DiskStore diskStore = cache.findDiskStore(DISK_STORE_NAME);
    Region region = cache.<String, String>createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
        .setDiskStoreName(DISK_STORE_NAME).create(REGION_NAME);

    putEntries(region, NUM_ENTRIES);

    assertEquals(ManagementConstants.NOT_AVAILABLE_FLOAT,
        ((DiskStoreImpl) diskStore).getDiskUsagePercentage(), 0);
    assertEquals(ManagementConstants.NOT_AVAILABLE_FLOAT,
        ((DiskStoreImpl) diskStore).getDiskFreePercentage(), 0);
  }

  @Test
  public void whenMaxOplogFileDoesNotFitInDirThenNextDirIsSelected() throws Exception {

    File[] diskDirs = new File[3];
    diskDirs[0] = temporaryDirectory.newFolder("dir1");
    diskDirs[1] = temporaryDirectory.newFolder("dir2");
    diskDirs[2] = temporaryDirectory.newFolder("dir3");
    final int NUM_ENTRIES = 450000;
    int[] diskDirSizes;

    cache = createCache();
    diskDirSizes = new int[] {10, 5, 10};

    cache.createDiskStoreFactory().setMaxOplogSize(2).setDiskDirsAndSizes(diskDirs, diskDirSizes)
        .create(DISK_STORE_NAME);
    Region region = cache.<String, String>createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
        .setDiskStoreName(DISK_STORE_NAME).create(REGION_NAME);
    DiskStore diskStore = cache.findDiskStore(DISK_STORE_NAME);

    putEntries(region, NUM_ENTRIES);

    File[] dirs = diskStore.getDiskDirs();

    /**
     * 8 oplog files should be created.
     * The eighth one does not fit in dir1, so that directory should
     * be skipped, and the file should be stored in dir2.
     */
    assertThat(oplogFileIsInDir(1, dirs[0])).isTrue();
    assertThat(oplogFileIsInDir(2, dirs[1])).isTrue();
    assertThat(oplogFileIsInDir(3, dirs[2])).isTrue();

    assertThat(oplogFileIsInDir(4, dirs[0])).isTrue();
    assertThat(oplogFileIsInDir(5, dirs[1])).isTrue();
    assertThat(oplogFileIsInDir(6, dirs[2])).isTrue();

    assertThat(oplogFileIsInDir(7, dirs[0])).isTrue();
    assertThat(oplogFileIsInDir(8, dirs[1])).isFalse();
    assertThat(oplogFileIsInDir(8, dirs[2])).isTrue();

    assertThat(oplogFileIsInDir(9, dirs[0])).isFalse();
  }

  @Test
  public void oplogWriteBufferSizeIsEqualsToDiskStoreWriteBufferSize() throws Exception {
    File[] diskDirs = new File[1];
    int expectedWriteBufferSize = 12345;
    diskDirs[0] = temporaryDirectory.newFolder("dir1");
    cache = createCache();
    cache.createDiskStoreFactory().setWriteBufferSize(expectedWriteBufferSize).setDiskDirs(diskDirs)
        .create(DISK_STORE_NAME);
    Region region = cache.<String, String>createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
        .setDiskStoreName(DISK_STORE_NAME).create(REGION_NAME);
    putEntries(region, 1);
    DiskStore diskStore = cache.findDiskStore(DISK_STORE_NAME);
    assertThat(diskStore.getWriteBufferSize()).isEqualTo(expectedWriteBufferSize);
    Oplog oplog = ((DiskStoreImpl) diskStore).getPersistentOplogs().getChild();
    assertThat(oplog.getWriteBuf().capacity()).isEqualTo(expectedWriteBufferSize);
  }

  /**
   * Returns true if the files of the given oplog file are in the
   * given directory.
   */
  private boolean oplogFileIsInDir(int oplogFileIndex, File dir) {
    String[] files = dir.list();
    String pattern = "BACKUP" + DISK_STORE_NAME + "_" + oplogFileIndex;
    for (String file : files) {
      if (file.contains(pattern)) {
        return true;
      }
    }
    return false;
  }

  private void putEntries(int numToPut) {
    putEntries(aRegion, numToPut);
  }

  private void putEntries(Region region, int numToPut) {
    for (int i = 1; i <= numToPut; i++) {
      region.put(i, i);
    }
  }

  private void createRegionWithDiskStore(File baseDir) {
    cache.createDiskStoreFactory().setDiskDirs(new File[] {baseDir}).create(DISK_STORE_NAME);
    cache.<String, String>createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
        .setDiskStoreName(DISK_STORE_NAME).create(REGION_NAME);
  }

  private void createRegionWithDiskStoreAndAsyncQueue(File baseDir, int queueSize) {
    createDiskStoreWithQueue(baseDir, queueSize, TIME_INTERVAL);

    RegionFactory regionFactory =
        cache.<String, String>createRegionFactory(RegionShortcut.PARTITION_PERSISTENT);
    regionFactory.setDiskSynchronous(false);
    regionFactory.setDiskStoreName(DISK_STORE_NAME);
    aRegion = regionFactory.create(REGION_NAME);
  }

  private void createDiskStoreWithQueue(File baseDir, int queueSize, long timeInterval) {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(new File[] {baseDir});
    diskStoreFactory.setQueueSize(queueSize);
    diskStoreFactory.setTimeInterval(timeInterval);
    DiskStore diskStore = diskStoreFactory.create(DISK_STORE_NAME);
    diskStoreStats = ((DiskStoreImpl) diskStore).getStats();
  }

  private Cache createCache() {
    // Setting MCAST port explicitly is currently required due to default properties set in gradle
    return new CacheFactory().set(ConfigurationProperties.MCAST_PORT, "0").create();
  }
}
