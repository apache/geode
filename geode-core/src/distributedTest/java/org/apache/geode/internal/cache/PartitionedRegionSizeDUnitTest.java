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

import static org.apache.geode.cache.EvictionAction.OVERFLOW_TO_DISK;
import static org.apache.geode.cache.EvictionAttributes.createLRUEntryAttributes;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

/**
 * This test verifies the size API for 100 thousand put operations (done synch/asynch) on
 * PartitionedRegions with different combinations of Scope and Redundancy (Scope DIST_ACK,
 * Redundancy 1 AND Scope DIST_NO_ACK, Redundancy 0).
 */

public class PartitionedRegionSizeDUnitTest extends CacheTestCase {

  private static final String DISK_STORE_NAME = "DISKSTORE";
  private static final String REGION_NAME = "PR";
  private static final int CNT = 100;
  private static final int TOTAL_NUMBER_OF_BUCKETS = 5;

  private File overflowDirectory;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() throws Exception {
    vm0 = VM.getVM(0);
    vm1 = VM.getVM(1);
    vm2 = VM.getVM(2);
    vm3 = VM.getVM(3);

    overflowDirectory = temporaryFolder.newFolder("overflowDir");
  }

  /**
   * This test method invokes methods doing size validation on PRs.
   */
  @Test
  public void testSize() throws Exception {
    // Create PRs with dataStore on 3 VMs
    vm0.invoke(() -> createPartitionedRegion(200));
    vm1.invoke(() -> createPartitionedRegion(200));
    vm2.invoke(() -> createPartitionedRegion(200));

    // Create only accessor on 4th VM
    vm3.invoke(() -> createPartitionedRegion(0));

    // Do put operations on PR synchronously.
    vm3.invoke(() -> {
      Region<Integer, Integer> region =
          getCache().getRegion(PartitionedRegionSizeDUnitTest.REGION_NAME);
      for (int k = 0; k < CNT; k++) {
        region.put(k, k);
      }
    });

    // Validate the size against the total put operations
    vm3.invoke(() -> {
      Region region = getCache().getRegion(PartitionedRegionSizeDUnitTest.REGION_NAME);
      await().until(() -> region.size() == CNT);
    });
  }

  /**
   * Regression test for TRAC #39868
   *
   * <p>
   * TRAC #39868: PartitionMemberDetails.getSize() reports negative PR sizes when redundancy is 0
   */
  @Test
  public void testBug39868() throws Exception {
    vm0.invoke(() -> createPartitionedRegion(200));

    vm0.invoke(() -> {
      Region<Integer, byte[]> region = getRegion(REGION_NAME);
      for (int i = 0; i < 100; i++) {
        region.put(i * TOTAL_NUMBER_OF_BUCKETS, new byte[100]);
      }
    });

    vm1.invoke(() -> createPartitionedRegion(200));

    vm0.invoke(() -> {
      Region<Integer, byte[]> region = getRegion(REGION_NAME);
      for (int i = 0; i < 100; i++) {
        region.destroy(i * TOTAL_NUMBER_OF_BUCKETS);
      }
    });

    vm1.invoke(() -> {
      PartitionedRegion partitionedRegion = getPartitionedRegion(REGION_NAME);
      long bytes = partitionedRegion.getDataStore().currentAllocatedMemory();
      assertThat(bytes).isEqualTo(0);
    });
  }

  @Test
  public void testByteSize() throws Exception {
    vm0.invoke(() -> createPartitionedRegion(200));
    vm1.invoke(() -> createPartitionedRegion(200));

    long bucketSizeWithOneEntry = vm0.invoke(() -> {
      Region<Integer, byte[]> region = getRegion(REGION_NAME);
      region.put(0, new byte[100]);

      PartitionedRegion partitionedRegion = (PartitionedRegion) region;
      PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();
      long size = dataStore.getBucketSize(0);

      for (int i = 1; i < 100; i++) {
        region.put(i * TOTAL_NUMBER_OF_BUCKETS, new byte[100]);
      }
      await().until(() -> dataStore.getBucketsManaged() == (short) 1);

      // make sure the size is proportional to the amount of data
      await().until(() -> dataStore.getBucketSize(0) == 100 * size);

      // destroy and invalidate entries and make sure the size goes down
      for (int i = 0; i < 25; i++) {
        region.destroy(i * TOTAL_NUMBER_OF_BUCKETS);
      }

      for (int i = 25; i < 50; i++) {
        region.invalidate(i * TOTAL_NUMBER_OF_BUCKETS);
      }

      await().until(() -> dataStore.getBucketSize(0) == 50 * size);

      // put some larger values in and make sure the size goes up
      for (int i = 50; i < 75; i++) {
        region.put(i * TOTAL_NUMBER_OF_BUCKETS, new byte[150]);
      }

      // Now put in some smaller values and see if the size balances out
      for (int i = 75; i < 100; i++) {
        region.put(i * TOTAL_NUMBER_OF_BUCKETS, new byte[50]);
      }

      await().until(() -> dataStore.getBucketSize(0) == 50 * size);

      return size;
    });

    vm1.invoke(() -> {
      PartitionedRegion partitionedRegion = getPartitionedRegion(REGION_NAME);
      await().until(
          () -> partitionedRegion.getDataStore().getBucketSize(0) == 50 * bucketSizeWithOneEntry);
    });

    vm1.invoke(() -> {
      PartitionedRegion partitionedRegion = getPartitionedRegion(REGION_NAME);
      PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();
      await().until(() -> dataStore.currentAllocatedMemory() == 50 * bucketSizeWithOneEntry);
    });
  }

  @Test
  public void testByteSizeWithEviction() throws Exception {
    vm0.invoke(this::createPartitionedRegionWithOverflow);

    long bucketSizeWithOneEntry = vm0.invoke(() -> {
      Region<Integer, byte[]> region = getRegion(REGION_NAME);
      region.put(0, new byte[100]);

      PartitionedRegion partitionedRegion = (PartitionedRegion) region;
      PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();
      long size = dataStore.getBucketSize(0);

      for (int i = 1; i < 100; i++) {
        region.put(i * TOTAL_NUMBER_OF_BUCKETS, new byte[100]);
      }
      await().until(() -> dataStore.getBucketsManaged() == (short) 1);
      return size;
    });

    vm0.invoke(() -> {
      Region<Integer, byte[]> region = getRegion(REGION_NAME);
      PartitionedRegion partitionedRegion = (PartitionedRegion) region;
      PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();

      // there should only be 2 items in memory
      await().until(() -> dataStore.currentAllocatedMemory() == 2 * bucketSizeWithOneEntry);

      // fault something else into memory and check again.
      region.get(82 * TOTAL_NUMBER_OF_BUCKETS);
      await().until(() -> dataStore.currentAllocatedMemory() == 2 * bucketSizeWithOneEntry);
    });
  }

  private void createPartitionedRegionWithOverflow() {
    Cache cache = getCache();

    File[] diskDirs = new File[] {overflowDirectory};

    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(diskDirs);
    DiskStore diskStore = diskStoreFactory.create(DISK_STORE_NAME);

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setLocalMaxMemory(200);
    paf.setTotalNumBuckets(TOTAL_NUMBER_OF_BUCKETS);

    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setDiskStoreName(diskStore.getName());
    regionFactory.setDiskSynchronous(true);
    regionFactory.setEvictionAttributes(createLRUEntryAttributes(2, OVERFLOW_TO_DISK));
    regionFactory.setPartitionAttributes(paf.create());

    regionFactory.create(REGION_NAME);
  }

  private void createPartitionedRegion(final int localMaxMemory) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setLocalMaxMemory(localMaxMemory);
    paf.setTotalNumBuckets(TOTAL_NUMBER_OF_BUCKETS);

    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    regionFactory.create(REGION_NAME);
  }

  private <K, V> Region<K, V> getRegion(String regionName) {
    return getCache().getRegion(regionName);
  }

  private PartitionedRegion getPartitionedRegion(String regionName) {
    return (PartitionedRegion) getCache().getRegion(regionName);
  }
}
