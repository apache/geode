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

import static org.apache.geode.cache.EvictionAction.OVERFLOW_TO_DISK;
import static org.apache.geode.cache.EvictionAttributes.createLRUEntryAttributes;
import static org.apache.geode.cache.RegionShortcut.LOCAL;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

/**
 * Extracted and exploded {@code testDiskRegDWAttrbts} from {@link DiskRegionJUnitTest}.
 */
public class DiskRegionAttributesIntegrationTest {

  private static final long MAX_OPLOG_SIZE_IN_BYTES = 1024 * 1024 * 1024 * 10L;

  private InternalCache cache;
  private EvictionAttributes evictionAttributes;

  private File[] diskDirs;
  private int[] diskDirSizes;

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
    String uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    diskStoreName = uniqueName + "_diskStore";

    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");

    cache = (InternalCache) new CacheFactory(config).create();

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

    evictionAttributes = createLRUEntryAttributes(1000, OVERFLOW_TO_DISK);

    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (cache != null) {
        cache.close();
      }
    } finally {
      DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;
    }
  }

  @Test
  public void syncPersistentWithAutoCompact() {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setAutoCompact(true);
    diskStoreFactory.setDiskDirsAndSizes(diskDirs, diskDirSizes);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, MAX_OPLOG_SIZE_IN_BYTES);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(LOCAL);
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(true);

    Region region = regionFactory.create(regionName);

    assertThat(region.getAttributes().isDiskSynchronous()).isEqualTo(true);
    verifyRegionAndDiskStoreAttributes(true, MAX_OPLOG_SIZE_IN_BYTES, 0, -1);
  }

  @Test
  public void syncPersistent() {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setAutoCompact(false);
    diskStoreFactory.setDiskDirsAndSizes(diskDirs, diskDirSizes);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, MAX_OPLOG_SIZE_IN_BYTES);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(LOCAL);
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);

    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(true);

    Region region = regionFactory.create(regionName);

    assertThat(region.getAttributes().isDiskSynchronous()).isEqualTo(true);
    verifyRegionAndDiskStoreAttributes(false, MAX_OPLOG_SIZE_IN_BYTES, 0, -1);
  }

  @Test
  public void asyncPersistentWithAutoCompact() {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setAutoCompact(true);
    diskStoreFactory.setDiskDirsAndSizes(diskDirs, diskDirSizes);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, MAX_OPLOG_SIZE_IN_BYTES);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(LOCAL);
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(false);

    Region region = regionFactory.create(regionName);

    assertThat(region.getAttributes().isDiskSynchronous()).isEqualTo(false);
    verifyRegionAndDiskStoreAttributes(true, MAX_OPLOG_SIZE_IN_BYTES, 0, -1);
  }

  @Test
  public void asyncPersistent() {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setAutoCompact(false);
    diskStoreFactory.setDiskDirsAndSizes(diskDirs, diskDirSizes);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, MAX_OPLOG_SIZE_IN_BYTES);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(LOCAL);
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(false);

    Region region = regionFactory.create(regionName);

    assertThat(region.getAttributes().isDiskSynchronous()).isEqualTo(false);
    verifyRegionAndDiskStoreAttributes(false, MAX_OPLOG_SIZE_IN_BYTES, 0, -1);
  }

  @Test
  public void syncOverflowWithAutoCompact() {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setAutoCompact(true);
    diskStoreFactory.setDiskDirsAndSizes(diskDirs, diskDirSizes);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, MAX_OPLOG_SIZE_IN_BYTES);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(LOCAL);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(true);
    regionFactory.setEvictionAttributes(evictionAttributes);

    Region region = regionFactory.create(regionName);

    assertThat(region.getAttributes().isDiskSynchronous()).isEqualTo(true);
    verifyRegionAndDiskStoreAttributes(true, MAX_OPLOG_SIZE_IN_BYTES, 0, -1);
  }

  @Test
  public void syncOverflow() {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setAutoCompact(false);
    diskStoreFactory.setDiskDirsAndSizes(diskDirs, diskDirSizes);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, MAX_OPLOG_SIZE_IN_BYTES);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(LOCAL);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(true);
    regionFactory.setEvictionAttributes(evictionAttributes);

    Region region = regionFactory.create(regionName);

    assertThat(region.getAttributes().isDiskSynchronous()).isEqualTo(true);
    verifyRegionAndDiskStoreAttributes(false, MAX_OPLOG_SIZE_IN_BYTES, 0, -1);
  }

  @Test
  public void asyncOverflowWithAutoCompact() {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setAutoCompact(true);
    diskStoreFactory.setDiskDirsAndSizes(diskDirs, diskDirSizes);
    diskStoreFactory.setQueueSize(10_000);
    diskStoreFactory.setTimeInterval(15);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, MAX_OPLOG_SIZE_IN_BYTES);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(LOCAL);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(false);

    regionFactory.setEvictionAttributes(evictionAttributes);

    Region region = regionFactory.create(regionName);

    assertThat(region.getAttributes().isDiskSynchronous()).isEqualTo(false);
    verifyRegionAndDiskStoreAttributes(true, MAX_OPLOG_SIZE_IN_BYTES, 10_000, 15);
  }

  @Test
  public void asyncOverflowWithEviction() {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setAutoCompact(false);
    diskStoreFactory.setDiskDirsAndSizes(diskDirs, diskDirSizes);
    diskStoreFactory.setTimeInterval(15);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, MAX_OPLOG_SIZE_IN_BYTES);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(LOCAL);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(false);

    regionFactory.setEvictionAttributes(evictionAttributes);

    Region region = regionFactory.create(regionName);

    assertThat(region.getAttributes().isDiskSynchronous()).isEqualTo(false);
    verifyRegionAndDiskStoreAttributes(false, MAX_OPLOG_SIZE_IN_BYTES, 0, 15);
  }

  @Test
  public void syncPersistentWithOverflowAndAutoCompact() {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setAutoCompact(true);
    diskStoreFactory.setDiskDirsAndSizes(diskDirs, diskDirSizes);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, MAX_OPLOG_SIZE_IN_BYTES);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(LOCAL);
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(true);
    regionFactory.setEvictionAttributes(evictionAttributes);

    Region region = regionFactory.create(regionName);

    assertThat(region.getAttributes().isDiskSynchronous()).isEqualTo(true);
    verifyRegionAndDiskStoreAttributes(true, MAX_OPLOG_SIZE_IN_BYTES, 0, -1);
  }

  @Test
  public void syncPersistentWithOverflow() {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setAutoCompact(false);
    diskStoreFactory.setDiskDirsAndSizes(diskDirs, diskDirSizes);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, MAX_OPLOG_SIZE_IN_BYTES);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(LOCAL);
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(true);
    regionFactory.setEvictionAttributes(evictionAttributes);

    Region region = regionFactory.create(regionName);

    assertThat(region.getAttributes().isDiskSynchronous()).isEqualTo(true);
    verifyRegionAndDiskStoreAttributes(false, MAX_OPLOG_SIZE_IN_BYTES, 0, -1);
  }

  @Test
  public void asyncPersistentWithOverflowAndAutoCompactAndBuffer() {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setAutoCompact(true);
    diskStoreFactory.setDiskDirsAndSizes(diskDirs, diskDirSizes);
    diskStoreFactory.setQueueSize(10_000);
    diskStoreFactory.setTimeInterval(15);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, MAX_OPLOG_SIZE_IN_BYTES);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(LOCAL);
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(false);
    regionFactory.setEvictionAttributes(evictionAttributes);

    Region region = regionFactory.create(regionName);

    assertThat(region.getAttributes().isDiskSynchronous()).isEqualTo(false);
    verifyRegionAndDiskStoreAttributes(true, MAX_OPLOG_SIZE_IN_BYTES, 10_000, 15);
  }

  @Test
  public void asyncPersistentWithOverflowAndBuffer() {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setAutoCompact(false);
    diskStoreFactory.setDiskDirsAndSizes(diskDirs, diskDirSizes);
    diskStoreFactory.setTimeInterval(15);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, MAX_OPLOG_SIZE_IN_BYTES);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(LOCAL);
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(false);
    regionFactory.setEvictionAttributes(evictionAttributes);

    Region region = regionFactory.create(regionName);

    assertThat(region.getAttributes().isDiskSynchronous()).isEqualTo(false);
    verifyRegionAndDiskStoreAttributes(false, MAX_OPLOG_SIZE_IN_BYTES, 0, 15);
  }

  private File createDirectory(File parentDirectory, String name) {
    File file = new File(parentDirectory, name);
    assertThat(file.mkdir()).isTrue();
    return file;
  }

  private void createDiskStoreWithSizeInBytes(String diskStoreName,
      DiskStoreFactory diskStoreFactory, long maxOplogSizeInBytes) {
    ((DiskStoreFactoryImpl) diskStoreFactory).setMaxOplogSizeInBytes(maxOplogSizeInBytes);
    DirectoryHolder.SET_DIRECTORY_SIZE_IN_BYTES_FOR_TESTING_PURPOSES = true;
    try {
      diskStoreFactory.create(diskStoreName);
    } finally {
      DirectoryHolder.SET_DIRECTORY_SIZE_IN_BYTES_FOR_TESTING_PURPOSES = false;
    }
  }

  private void verifyRegionAndDiskStoreAttributes(boolean autoCompact, long maxOplogSizeInBytes,
      int bytesThreshold, int timeInterval) {
    DiskStore diskStore = cache.findDiskStore(diskStoreName);

    assertThat(diskStore.getAutoCompact()).isEqualTo(autoCompact);

    int expectedDiskDirsCount = diskDirs.length;
    int actualDiskDirsCount = diskStore.getDiskDirs().length;
    assertThat(actualDiskDirsCount).isEqualTo(expectedDiskDirsCount);

    int[] expectedDiskDirSizes = diskDirSizes;
    if (expectedDiskDirSizes == null) {
      expectedDiskDirSizes = new int[expectedDiskDirsCount];
      Arrays.fill(expectedDiskDirSizes, Integer.MAX_VALUE);
    }

    int[] actualDiskDirSizes = diskStore.getDiskDirSizes();
    for (int i = 0; i < expectedDiskDirsCount; i++) {
      assertThat(actualDiskDirSizes[i]).isEqualTo(expectedDiskDirSizes[i]);
    }

    assertThat(diskStore.getDiskUsageWarningPercentage())
        .isEqualTo(DiskStoreFactory.DEFAULT_DISK_USAGE_WARNING_PERCENTAGE);
    assertThat(diskStore.getDiskUsageCriticalPercentage())
        .isEqualTo(DiskStoreFactory.DEFAULT_DISK_USAGE_CRITICAL_PERCENTAGE);
    assertThat(diskStore.getMaxOplogSize()).isEqualTo(maxOplogSizeInBytes / (1024 * 1024));
    assertThat(diskStore.getQueueSize()).isEqualTo(bytesThreshold);

    if (timeInterval != -1) {
      assertThat(diskStore.getTimeInterval()).isEqualTo(timeInterval);
    } else {
      assertThat(diskStore.getTimeInterval()).isEqualTo(DiskStoreFactory.DEFAULT_TIME_INTERVAL);
    }
  }
}
