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

import static org.apache.geode.cache.DiskStoreFactory.DEFAULT_DISK_DIR_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;

/**
 * Tests DiskStoreFactory
 */
public class DiskStoreFactoryIntegrationTest {

  private static Cache cache = null;
  private static final Properties props = new Properties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  static {
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(LOG_LEVEL, "config"); // to keep diskPerf logs smaller
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
  }

  @Before
  public void setUp() throws Exception {
    props.setProperty(STATISTIC_ARCHIVE_FILE,
        temporaryFolder.getRoot().getAbsolutePath() + File.separator + "stats.gfs");
    createCache();
  }

  private Cache createCache() {
    cache = new CacheFactory(props).create();

    return cache;
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  /**
   * Test method for 'org.apache.geode.cache.DiskWriteAttributes.getDefaultInstance()'
   */
  @Test
  public void testGetDefaultInstance() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testGetDefaultInstance";
    assertThat(cache.findDiskStore(name)).isNull();
    DiskStore ds = dsf.create(name);

    assertThat(cache.findDiskStore(name)).isEqualTo(ds);
    assertThat(ds.getName()).isEqualTo(name);
    assertThat(ds.getAutoCompact()).isEqualTo(DiskStoreFactory.DEFAULT_AUTO_COMPACT);
    assertThat(ds.getCompactionThreshold())
        .isEqualTo(DiskStoreFactory.DEFAULT_COMPACTION_THRESHOLD);
    assertThat(ds.getAllowForceCompaction())
        .isEqualTo(DiskStoreFactory.DEFAULT_ALLOW_FORCE_COMPACTION);
    assertThat(ds.getMaxOplogSize()).isEqualTo(DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE);
    assertThat(ds.getTimeInterval()).isEqualTo(DiskStoreFactory.DEFAULT_TIME_INTERVAL);
    assertThat(ds.getWriteBufferSize()).isEqualTo(DiskStoreFactory.DEFAULT_WRITE_BUFFER_SIZE);
    assertThat(ds.getQueueSize()).isEqualTo(DiskStoreFactory.DEFAULT_QUEUE_SIZE);
    assertThat(Arrays.equals(ds.getDiskDirs(), DiskStoreFactory.DEFAULT_DISK_DIRS))
        .as("expected=" + Arrays.toString(DiskStoreFactory.DEFAULT_DISK_DIRS) + " had="
            + Arrays.toString(ds.getDiskDirs()))
        .isTrue();
    assertThat(Arrays.equals(ds.getDiskDirSizes(), DiskStoreFactory.DEFAULT_DISK_DIR_SIZES))
        .as("expected=" + Arrays.toString(DiskStoreFactory.DEFAULT_DISK_DIR_SIZES) + " had="
            + Arrays.toString(ds.getDiskDirSizes()))
        .isTrue();
  }

  @Test
  public void testNonDefaults() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testNonDefaults";
    DiskStore ds = dsf.setAutoCompact(!DiskStoreFactory.DEFAULT_AUTO_COMPACT)
        .setCompactionThreshold(DiskStoreFactory.DEFAULT_COMPACTION_THRESHOLD / 2)
        .setAllowForceCompaction(!DiskStoreFactory.DEFAULT_ALLOW_FORCE_COMPACTION)
        .setMaxOplogSize(DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE + 1)
        .setTimeInterval(DiskStoreFactory.DEFAULT_TIME_INTERVAL + 1)
        .setWriteBufferSize(DiskStoreFactory.DEFAULT_WRITE_BUFFER_SIZE + 1)
        .setQueueSize(DiskStoreFactory.DEFAULT_QUEUE_SIZE + 1).create(name);

    assertThat(ds.getAutoCompact()).isEqualTo(!DiskStoreFactory.DEFAULT_AUTO_COMPACT);
    assertThat(ds.getCompactionThreshold())
        .isEqualTo(DiskStoreFactory.DEFAULT_COMPACTION_THRESHOLD / 2);
    assertThat(ds.getAllowForceCompaction())
        .isEqualTo(!DiskStoreFactory.DEFAULT_ALLOW_FORCE_COMPACTION);
    assertThat(ds.getMaxOplogSize()).isEqualTo(DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE + 1);
    assertThat(ds.getTimeInterval()).isEqualTo(DiskStoreFactory.DEFAULT_TIME_INTERVAL + 1);
    assertThat(ds.getWriteBufferSize()).isEqualTo(DiskStoreFactory.DEFAULT_WRITE_BUFFER_SIZE + 1);
    assertThat(ds.getQueueSize()).isEqualTo(DiskStoreFactory.DEFAULT_QUEUE_SIZE + 1);
  }

  @Test
  public void testCompactionThreshold() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testCompactionThreshold1";
    DiskStore ds = dsf.setCompactionThreshold(0).create(name);
    assertThat(ds.getCompactionThreshold()).isEqualTo(0);
    name = "testCompactionThreshold2";
    ds = dsf.setCompactionThreshold(100).create(name);
    assertThat(ds.getCompactionThreshold()).isEqualTo(100);

    // check illegal stuff
    assertThatThrownBy(() -> dsf.setCompactionThreshold(-1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> dsf.setCompactionThreshold(101))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testQueueSize() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testQueueSize";
    DiskStore ds = dsf.setQueueSize(0).create(name);
    assertThat(ds.getQueueSize()).isEqualTo(0);
    name = "testQueueSize2";
    ds = dsf.setQueueSize(Integer.MAX_VALUE).create(name);
    assertThat(ds.getQueueSize()).isEqualTo(Integer.MAX_VALUE);

    // check illegal stuff
    assertThatThrownBy(() -> dsf.setQueueSize(-1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testWriteBufferSize() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testWriteBufferSize";
    DiskStore ds = dsf.setWriteBufferSize(0).create(name);
    assertThat(ds.getWriteBufferSize()).isEqualTo(0);
    name = "testWriteBufferSize2";
    ds = dsf.setWriteBufferSize(Integer.MAX_VALUE).create(name);
    assertThat(ds.getWriteBufferSize()).isEqualTo(Integer.MAX_VALUE);

    // check illegal stuff
    assertThatThrownBy(() -> dsf.setWriteBufferSize(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testTimeInterval() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testTimeInterval";
    DiskStore ds = dsf.setTimeInterval(0).create(name);
    assertThat(ds.getTimeInterval()).isEqualTo(0);
    name = "testTimeInterval2";
    ds = dsf.setTimeInterval(Long.MAX_VALUE).create(name);
    assertThat(ds.getTimeInterval()).isEqualTo(Long.MAX_VALUE);

    // check illegal stuff
    assertThatThrownBy(() -> dsf.setTimeInterval(-1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testMaxOplogSize() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testMaxOplogSize";
    DiskStore ds = dsf.setMaxOplogSize(0).create(name);
    assertThat(ds.getMaxOplogSize()).isEqualTo(0);
    name = "testMaxOplogSize2";
    long max = Long.MAX_VALUE / (1024 * 1024);
    ds = dsf.setMaxOplogSize(max).create(name);
    assertThat(ds.getMaxOplogSize()).isEqualTo(max);

    // check illegal stuff
    assertThatThrownBy(() -> dsf.setMaxOplogSize(-1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> dsf.setMaxOplogSize(max + 1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testFlush() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testFlush";
    DiskStore ds = dsf.create(name);
    ds.flush();
  }

  @Test
  public void testForceRoll() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testForceRoll";
    DiskStore ds = dsf.create(name);
    ds.forceRoll();
  }

  @Test
  public void testDestroyWithPersistentRegion() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testDestroy";
    DiskStore ds = dsf.create(name);
    Region region = cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT)
        .setDiskStoreName("testDestroy").create("region");
    assertThatThrownBy(ds::destroy).isInstanceOf(IllegalStateException.class);

    // This should now work
    region.destroyRegion();
    assertThatCode(ds::destroy).doesNotThrowAnyException();
  }

  @Test
  public void testDestroyWithClosedRegion() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testDestroy";
    DiskStore ds = dsf.create(name);
    Region region = cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT)
        .setDiskStoreName("testDestroy").create("region");

    // This should now work
    region.close();
    assertThatCode(ds::destroy).doesNotThrowAnyException();
  }

  @Test
  public void testDestroyWithOverflowRegion() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testDestroy";
    DiskStore ds = dsf.create(name);

    Region region = cache.createRegionFactory(RegionShortcut.LOCAL_OVERFLOW)
        .setDiskStoreName("testDestroy").create("region");
    assertThatThrownBy(ds::destroy).isInstanceOf(IllegalStateException.class);

    // The destroy should now work.
    region.close();
    assertThatCode(ds::destroy).doesNotThrowAnyException();
  }

  @Test
  public void testForceCompaction() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAllowForceCompaction(true);
    String name = "testForceCompaction";
    DiskStore ds = dsf.create(name);
    assertThat(ds.forceCompaction()).isFalse();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testMissingInitFile() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testMissingInitFile";
    DiskStore diskStore = dsf.create(name);
    File ifFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + DiskInitFile.IF_FILE_EXT);
    assertThat(ifFile.exists()).isTrue();
    AttributesFactory<String, String> af = new AttributesFactory<>();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    cache.createRegion("r", af.create());
    cache.close();
    assertThat(ifFile.exists()).isTrue();
    assertThat(ifFile.delete()).isTrue();
    assertThat(ifFile.exists()).isFalse();
    cache = createCache();
    dsf = cache.createDiskStoreFactory();
    assertThat(cache.findDiskStore(name)).isNull();

    try {
      dsf.create(name);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    // if test passed clean up files
    removeFiles(diskStore);
  }

  private void removeFiles(DiskStore diskStore) {
    final String diskStoreName = diskStore.getName();
    File[] dirs = diskStore.getDiskDirs();

    for (File dir : dirs) {
      File[] files = dir.listFiles((file, name) -> name.startsWith("BACKUP" + diskStoreName));
      assertThat(files).isNotNull();
      Arrays.stream(files).forEach(file -> assertThat(file.delete()).isTrue());
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testMissingCrfFile() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testMissingCrfFile";
    DiskStore diskStore = dsf.create(name);
    File crfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.crf");
    AttributesFactory<String, String> af = new AttributesFactory<>();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region<String, String> r = cache.createRegion("r", af.create());
    r.put("key", "value");
    assertThat(crfFile.exists()).isTrue();
    cache.close();
    assertThat(crfFile.exists()).isTrue();
    assertThat(crfFile.delete()).isTrue();
    assertThat(crfFile.exists()).isFalse();
    cache = createCache();
    dsf = cache.createDiskStoreFactory();
    assertThat(cache.findDiskStore(name)).isNull();

    try {
      dsf.create(name);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    // if test passed clean up files
    removeFiles(diskStore);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testMissingDrfFile() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testMissingDrfFile";
    DiskStore diskStore = dsf.create(name);
    File drfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.drf");
    AttributesFactory<String, String> af = new AttributesFactory<>();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region<String, String> r = cache.createRegion("r", af.create());
    r.put("key", "value");
    assertThat(drfFile.exists()).isTrue();
    cache.close();
    assertThat(drfFile.exists()).isTrue();
    assertThat(drfFile.delete()).isTrue();
    assertThat(drfFile.exists()).isFalse();
    cache = createCache();
    dsf = cache.createDiskStoreFactory();
    assertThat(cache.findDiskStore(name)).isNull();

    try {
      dsf.create(name);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    // if test passed clean up files
    removeFiles(diskStore);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testRedefiningDefaultDiskStore() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAutoCompact(!DiskStoreFactory.DEFAULT_AUTO_COMPACT);
    assertThat(cache.findDiskStore(DiskStoreFactory.DEFAULT_DISK_STORE_NAME)).isNull();
    DiskStore diskStore = dsf.create(DiskStoreFactory.DEFAULT_DISK_STORE_NAME);
    AttributesFactory<String, String> af = new AttributesFactory<>();
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region<String, String> r = cache.createRegion("r", af.create());
    r.put("key", "value");
    DiskStore ds = ((LocalRegion) r).getDiskStore();
    assertThat(cache.findDiskStore(DiskStoreFactory.DEFAULT_DISK_STORE_NAME)).isEqualTo(ds);
    assertThat(ds.getName()).isEqualTo(DiskStoreFactory.DEFAULT_DISK_STORE_NAME);
    assertThat(ds.getAutoCompact()).isEqualTo(!DiskStoreFactory.DEFAULT_AUTO_COMPACT);
    cache.close();
    // if test passed clean up files
    removeFiles(diskStore);
  }

  @Test
  public void failedDiskStoreInitialRecoveryCleansUpDiskStore() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    DiskStoreImpl diskStore = mock(DiskStoreImpl.class);
    doThrow(RuntimeException.class).when(diskStore).doInitialRecovery();
    boolean threwException = false;
    try {
      ((DiskStoreFactoryImpl) dsf).initializeDiskStore(diskStore);
    } catch (RuntimeException e) {
      threwException = true;
    }
    assertThat(threwException).isTrue();
    verify(diskStore, times(1)).close();
  }

  @Test
  public void checkIfDirectoriesExistShouldCreateDirectoriesWhenTheyDoNotExist()
      throws IOException {
    File[] diskDirs = new File[3];
    diskDirs[0] = new File(
        temporaryFolder.getRoot().getAbsolutePath() + File.separator + "randomNonExistingDiskDir");
    diskDirs[1] = temporaryFolder.newFolder("existingDiskDir");
    diskDirs[2] = new File(temporaryFolder.getRoot().getAbsolutePath() + File.separator
        + "anotherRandomNonExistingDiskDir");

    assertThat(Files.exists(diskDirs[0].toPath())).isFalse();
    assertThat(Files.exists(diskDirs[1].toPath())).isTrue();
    assertThat(Files.exists(diskDirs[2].toPath())).isFalse();
    DiskStoreFactoryImpl.checkIfDirectoriesExist(diskDirs);
    Arrays.stream(diskDirs).forEach(diskDir -> assertThat(Files.exists(diskDir.toPath())).isTrue());
  }

  @Test
  public void setDiskDirsShouldInitializeInternalMetadataAndCreateDirectoriesWhenTheyDoNotExist()
      throws IOException {
    File[] diskDirs = new File[3];
    diskDirs[0] =
        new File(temporaryFolder.getRoot().getAbsolutePath() + File.separator + "randomDiskDir");
    diskDirs[1] = temporaryFolder.newFolder("existingDiskDir");
    diskDirs[2] = new File(
        temporaryFolder.getRoot().getAbsolutePath() + File.separator + "anotherRandomDiskDir");
    assertThat(Files.exists(diskDirs[0].toPath())).isFalse();
    assertThat(Files.exists(diskDirs[1].toPath())).isTrue();
    assertThat(Files.exists(diskDirs[2].toPath())).isFalse();

    DiskStoreFactoryImpl factoryImpl =
        (DiskStoreFactoryImpl) cache.createDiskStoreFactory().setDiskDirs(diskDirs);
    Arrays.stream(diskDirs).forEach(diskDir -> assertThat(Files.exists(diskDir.toPath())).isTrue());
    assertThat(factoryImpl.getDiskStoreAttributes().diskDirs).isEqualTo(diskDirs);
    assertThat(factoryImpl.getDiskStoreAttributes().diskDirSizes)
        .isEqualTo(new int[] {DEFAULT_DISK_DIR_SIZE, DEFAULT_DISK_DIR_SIZE, DEFAULT_DISK_DIR_SIZE});
  }
}
