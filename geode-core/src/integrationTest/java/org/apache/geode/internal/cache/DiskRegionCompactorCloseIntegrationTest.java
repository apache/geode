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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.cache.RegionShortcut.LOCAL;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;

/**
 * Extracted {@code testCompactorClose} from {@link DiskRegionJUnitTest}.
 */
public class DiskRegionCompactorCloseIntegrationTest {

  private final CountDownLatch beforeGoingToCompactLatch = new CountDownLatch(1);
  private final AtomicBoolean afterStoppingCompactor = new AtomicBoolean();

  private final Properties config = new Properties();
  private Cache cache;

  private File[] diskDirs;
  private int[] diskDirSizes;

  private String uniqueName;
  private String regionName;
  private String diskStoreName;

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    diskStoreName = uniqueName + "_diskStore";

    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");

    cache = new CacheFactory(config).create();

    diskDirs = new File[4];
    diskDirs[0] = createDirectory(temporaryFolder.getRoot(), testName.getMethodName() + "1");
    diskDirs[1] = createDirectory(temporaryFolder.getRoot(), testName.getMethodName() + "2");
    diskDirs[2] = createDirectory(temporaryFolder.getRoot(), testName.getMethodName() + "3");
    diskDirs[3] = createDirectory(temporaryFolder.getRoot(), testName.getMethodName() + "4");

    diskDirSizes = new int[4];
    Arrays.fill(diskDirSizes, Integer.MAX_VALUE);

    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
  }

  @After
  public void tearDown() throws Exception {
    try {
      beforeGoingToCompactLatch.countDown();
      cache.close();
    } finally {
      CacheObserverHolder.setInstance(null);
      DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      disconnectAllFromDS();
    }
  }

  @Test
  public void testCompactorClose() {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setAutoCompact(true);
    diskStoreFactory.setCompactionThreshold(100);
    diskStoreFactory.setDiskDirsAndSizes(diskDirs, diskDirSizes);

    createDiskStoreWithSizeInBytes(diskStoreName, diskStoreFactory, 100);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(LOCAL);
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(true);

    Region<Object, Object> region = regionFactory.create(regionName);

    CacheObserverHolder.setInstance(new CompactorCacheObserver(region));

    for (int i = 0; i < 10; ++i) {
      region.put(i, new byte[10]);
    }

    beforeGoingToCompactLatch.countDown();

    await().untilAsserted(() -> assertThat(afterStoppingCompactor).isTrue());

    assertThat(region.isDestroyed()).isTrue();
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
    DirectoryHolder.SET_DIRECTORY_SIZE_IN_BYTES_FOR_TESTING_PURPOSES = true;
    try {
      diskStoreFactory.create(diskStoreName);
    } finally {
      DirectoryHolder.SET_DIRECTORY_SIZE_IN_BYTES_FOR_TESTING_PURPOSES = false;
    }
  }

  private class CompactorCacheObserver extends CacheObserverAdapter {

    private final Region<?, ?> region;

    CompactorCacheObserver(Region<?, ?> region) {
      this.region = region;
    }

    @Override
    public void beforeGoingToCompact() {
      try {
        beforeGoingToCompactLatch.await(5, MINUTES);
      } catch (Exception e) {
        errorCollector.addError(e);
      }
    }

    @Override
    public void beforeDeletingCompactedOplog(Oplog compactedOplog) {
      // compactor will attempt to destroy the region
      throw new DiskAccessException(uniqueName + "_IGNORE_EXCEPTION", region);
    }

    @Override
    public void afterStoppingCompactor() {
      // compactor destroyed the region and stopped
      afterStoppingCompactor.set(true);
    }
  }
}
