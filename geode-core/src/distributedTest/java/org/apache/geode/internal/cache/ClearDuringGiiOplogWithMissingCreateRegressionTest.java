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

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.entries.VersionedThinDiskRegionEntryHeapObjectKey;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Regression test: The Clear operation during a GII in progress can leave a Entry in the Oplog
 * due to a race condition wherein the clearFlag getting set after the entry gets written to the
 * disk, The Test verifies the existence of the scenario.
 *
 * <p>
 * Bug: Clear operation with GII in progress may result in a deleted entry to be logged in
 * the oplog without accompanying create
 */
public class ClearDuringGiiOplogWithMissingCreateRegressionTest extends CacheTestCase {

  private static final int PUT_COUNT = 10000;

  private String uniqueName;
  private String regionName;
  private File[] foldersForServer1;
  private File[] foldersForServer2;

  private VM server1;
  private VM server2;

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    server1 = getHost(0).getVM(0);
    server2 = getHost(0).getVM(1);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName;

    File server1Disk1 = temporaryFolder.newFolder(uniqueName + "_server1_disk1");
    foldersForServer1 = new File[] {server1Disk1};

    File server2Disk1 = temporaryFolder.newFolder(uniqueName + "_server2_disk1");
    foldersForServer2 = new File[] {server2Disk1};
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
  }

  /**
   * The Clear operation during a GII in progress can leave a Entry in the Oplog due to a race
   * condition wherein the clearFlag getting set after the entry gets written to the disk, The Test
   * verifies the existence of the scenario.
   */
  @Test
  public void clearDuringGiiShouldOplogCreateAndDelete() {
    server1.invoke(() -> createCacheForVM0());
    server1.invoke(() -> {
      Region<Integer, Integer> region = getCache().getRegion(regionName);
      for (int i = 0; i < PUT_COUNT; i++) {
        region.put(i, i);
      }
    });

    server2.invoke(() -> createCacheForVM1());

    server1.invoke(() -> {
      getCache().getRegion(regionName).localDestroyRegion();
      getCache().close();
    });

    server2.invoke(() -> getCache().close());

    server2.invoke(() -> createCacheForVM1());
    server2.invoke(() -> assertThatRegionSizeIsZero());
  }

  /**
   * This method is used to create Cache in VM0
   */
  private void createCacheForVM0() {
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(foldersForServer1);

    DiskStore diskStore = dsf.create(uniqueName);

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setDiskSynchronous(false);
    factory.setDiskStoreName(diskStore.getName());

    getCache().createRegion(regionName, factory.create());
  }

  /**
   * This method is used to create Cache in VM1
   */
  private void createCacheForVM1() throws IOException, ClassNotFoundException {
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(foldersForServer2);

    DiskStore diskStore = dsf.create(uniqueName);

    InternalRegionFactory factory =
        getCache().createInternalRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
    factory.setDiskSynchronous(false);
    factory.setDiskStoreName(diskStore.getName());

    DistributedRegion distRegion =
        new DistributedRegion(regionName, factory.getCreateAttributes(), null,
            getCache(),
            new InternalRegionArguments().setDestroyLockFlag(true).setRecreateFlag(false)
                .setSnapshotInputStream(null).setImageTarget(null),
            disabledClock());

    distRegion.entries.setEntryFactory(new TestableDiskRegionEntryFactory());
    factory.setInternalMetaRegion(distRegion).setDestroyLockFlag(true)
        .setSnapshotInputStream(null).setImageTarget(null);
    factory.create(regionName);
  }

  /**
   * This method clears the region and notifies the other member when complete
   */
  private void invokeRemoteClearAndWait() {
    server1.invoke(() -> {
      Region region = getCache().getRegion(regionName);
      region.clear();
    });
  }

  private void assertThatRegionSizeIsZero() {
    assertThat(getCache().getRegion(regionName).size()).isZero();
  }

  private class TestableDiskRegionEntry extends VersionedThinDiskRegionEntryHeapObjectKey {

    TestableDiskRegionEntry(RegionEntryContext context, Object key, Object value) {
      super(context, key, value);
    }

    /**
     * Overridden setValue method to call clear Region before actually writing the entry
     */
    @Override
    public boolean initialImageInit(final InternalRegion region, final long lastModified,
        final Object newValue, final boolean create, final boolean wasRecovered,
        final boolean acceptedVersionTag) throws RegionClearedException {

      invokeRemoteClearAndWait();

      // Continue GII processing, which should throw RegionClearedException after the clear
      boolean result;
      try {
        result = super.initialImageInit(region, lastModified, newValue, create, wasRecovered,
            acceptedVersionTag);
      } catch (RegionClearedException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException("initialImageInit threw " + e.getClass().getSimpleName(), e);
      }

      return result;
    }
  }

  private class TestableDiskRegionEntryFactory implements RegionEntryFactory {

    @Override
    public RegionEntry createEntry(RegionEntryContext r, Object key, Object value) {
      return new TestableDiskRegionEntry(r, key, value);
    }

    @Override
    public Class getEntryClass() {
      return TestableDiskRegionEntry.class;
    }

    @Override
    public RegionEntryFactory makeVersioned() {
      return this;
    }

    @Override
    public RegionEntryFactory makeOnHeap() {
      return this;
    }
  }
}
