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
package org.apache.geode.internal.cache.partitioned;

import static org.apache.geode.cache.EvictionAction.OVERFLOW_TO_DISK;
import static org.apache.geode.cache.EvictionAttributes.createLRUEntryAttributes;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Moving a bucket during rebalancing should update overflow stats (numEntriesInVM and
 * numOverflowOnDisk).
 *
 * <p>
 * GEODE-3566: Moving a bucket during rebalancing does not update overflow stats
 */

@SuppressWarnings("serial")
public class BucketRebalanceStatRegressionTest implements Serializable {

  private static final String REGION_NAME = "TestRegion";

  private static final int TOTAL_NUMBER_BUCKETS = 2;
  private static final int LRU_ENTRY_COUNT = 4;
  private static final int ENTRIES_IN_REGION = 20;
  private static final int BYTES_SIZE = 100;

  private VM vm0;
  private VM vm1;

  @ClassRule
  public static DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    vm0 = getVM(0);
    vm1 = getVM(1);
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
  }

  @Test
  public void statsUpdatedAfterRebalancePersistentOverflowPR() throws Exception {
    initializeRegions(RegionShortcut.PARTITION_PERSISTENT, true);

    validateInitialOverflowStats();
    validateInitialRegion();
    validateStatsUpdatedAfterRebalance();
  }

  @Test
  public void statsUpdatedAfterRebalanceOverflowPR() throws Exception {
    initializeRegions(RegionShortcut.PARTITION, true);

    validateInitialOverflowStats();
    validateInitialRegion();
    validateStatsUpdatedAfterRebalance();
  }

  @Test
  public void statsUpdatedAfterRebalancePersistentPR() throws Exception {
    initializeRegions(RegionShortcut.PARTITION_PERSISTENT, false);

    validateInitialRegion();
    validateStatsUpdatedAfterRebalance();
  }

  /**
   * Verify that overflow stats are updated when a bucket moves due to rebalancing.
   */
  private void validateStatsUpdatedAfterRebalance() {
    vm0.invoke(() -> rebalance());
    assertThat(vm0.invoke(() -> cacheRule.getCache().getRegion(REGION_NAME).size()))
        .isEqualTo(ENTRIES_IN_REGION);
    assertThat(vm1.invoke(() -> cacheRule.getCache().getRegion(REGION_NAME).size()))
        .isEqualTo(ENTRIES_IN_REGION);
    assertThat(vm0.invoke(() -> ((PartitionedRegion) (cacheRule.getCache().getRegion(REGION_NAME)))
        .getLocalBucketsListTestOnly().size())).isEqualTo(TOTAL_NUMBER_BUCKETS / 2);
    assertThat(vm1.invoke(() -> ((PartitionedRegion) (cacheRule.getCache().getRegion(REGION_NAME)))
        .getLocalBucketsListTestOnly().size())).isEqualTo(TOTAL_NUMBER_BUCKETS / 2);
    validateOverflowStats(vm0, "vm0");
    validateOverflowStats(vm1, "vm1");
  }

  /**
   * Initialize region on the distributed members.
   *
   * @param shortcut The region shortcut to use to create the region.
   * @param overflow If true, use overflow on the region, false otherwise.
   */
  private void initializeRegions(final RegionShortcut shortcut, final boolean overflow) {
    // arrange: create regions and data
    vm0.invoke(() -> createRegion(shortcut, overflow));
    vm0.invoke(() -> loadRegion());
    vm1.invoke(() -> createRegion(shortcut, overflow));
  }

  /**
   * Do validation on the initial region before rebalancing. It is expected that all buckets and
   * data live on vm0; vm1 does not host any buckets.
   */
  private void validateInitialRegion() {
    assertThat(vm0.invoke(() -> cacheRule.getCache().getRegion(REGION_NAME).size()))
        .isEqualTo(ENTRIES_IN_REGION);
    assertThat(vm1.invoke(() -> cacheRule.getCache().getRegion(REGION_NAME).size()))
        .isEqualTo(ENTRIES_IN_REGION);
    assertThat(vm0.invoke(() -> ((PartitionedRegion) (cacheRule.getCache().getRegion(REGION_NAME)))
        .getLocalBucketsListTestOnly().size())).isEqualTo(TOTAL_NUMBER_BUCKETS);
    assertThat(vm1.invoke(() -> ((PartitionedRegion) (cacheRule.getCache().getRegion(REGION_NAME)))
        .getLocalBucketsListTestOnly().size())).isEqualTo(0);
  }

  /**
   * Do validation the initial region for the member containing all the data
   */
  private void validateInitialOverflowStats() {
    assertThat(vm0.invoke(() -> ((PartitionedRegion) (cacheRule.getCache().getRegion(REGION_NAME)))
        .getDiskRegionStats().getNumEntriesInVM())).isEqualTo(LRU_ENTRY_COUNT);
    assertThat(vm0.invoke(() -> ((PartitionedRegion) (cacheRule.getCache().getRegion(REGION_NAME)))
        .getDiskRegionStats().getNumOverflowOnDisk()))
            .isEqualTo(ENTRIES_IN_REGION - LRU_ENTRY_COUNT);
  }

  /**
   * Validate that the overflow stats are as expected on the given member.
   */
  private void validateOverflowStats(final VM vm, final String vmName) {
    long[] overflowStats = vm.invoke(() -> getOverflowStats());
    long[] overflowEntries = vm.invoke(() -> getActualOverflowEntries());

    long statEntriesInVM = overflowStats[0];
    long statEntriesOnDisk = overflowStats[1];
    long actualEntriesInVM = overflowEntries[0];
    long actualEntriesOnDisk = overflowEntries[1];

    assertThat(actualEntriesInVM).as("entriesInVM for " + vmName).isEqualTo(statEntriesInVM);
    assertThat(actualEntriesOnDisk).as("entriesOnDisk for " + vmName).isEqualTo(statEntriesOnDisk);
  }

  /**
   * Rebalance the region, waiting for the rebalance operation to complete
   */
  private void rebalance() throws Exception {
    ResourceManager resourceManager = cacheRule.getCache().getResourceManager();
    RebalanceFactory rebalanceFactory = resourceManager.createRebalanceFactory();
    RebalanceOperation rebalanceOperation = rebalanceFactory.start();

    // wait for rebalance to complete
    assertThat(rebalanceOperation.getResults()).isNotNull();
  }

  /**
   * Load the region with some data
   *
   */
  private void loadRegion() {
    Region<Integer, byte[]> region = cacheRule.getCache().getRegion(REGION_NAME);
    for (int i = 1; i <= ENTRIES_IN_REGION; i++) {
      region.put(i, new byte[BYTES_SIZE]);
    }
  }

  /**
   * Return stats from the region's disk statistics, specifically the numEntriesInVM stat and the
   * numOverflowOnDisk stat.
   *
   * @return [0] numEntriesInVM stat [1] numOverflowOnDisk stat
   */
  private long[] getOverflowStats() {
    Region<Integer, byte[]> region = cacheRule.getCache().getRegion(REGION_NAME);
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;

    long numEntriesInVM = partitionedRegion.getDiskRegionStats().getNumEntriesInVM();
    long numOverflowOnDisk = partitionedRegion.getDiskRegionStats().getNumOverflowOnDisk();
    return new long[] {numEntriesInVM, numOverflowOnDisk};
  }

  /**
   * Return the actual values for entries in the jvm (in memory) and entries on disk. These values
   * are the sum of all buckets in the current member.
   *
   * @return [0] total entries in VM [1] total entries on disk
   */
  private long[] getActualOverflowEntries() {
    Region<Integer, byte[]> region = cacheRule.getCache().getRegion(REGION_NAME);
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;

    int totalBucketEntriesInVM = 0;
    int totalBucketEntriesOnDisk = 0;
    Set<Entry<Integer, BucketRegion>> buckets =
        partitionedRegion.getDataStore().getAllLocalBuckets();
    for (Map.Entry<Integer, BucketRegion> entry : buckets) {
      BucketRegion bucket = entry.getValue();
      if (bucket != null) {
        totalBucketEntriesInVM += bucket.testHookGetValuesInVM();
        totalBucketEntriesOnDisk += bucket.testHookGetValuesOnDisk();
      }
    }

    return new long[] {totalBucketEntriesInVM, totalBucketEntriesOnDisk};
  }

  private void createRegion(final RegionShortcut shortcut, final boolean overflow)
      throws IOException {
    Cache cache = cacheRule.getOrCreateCache();
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(getDiskDirs());
    DiskStore diskStore = diskStoreFactory.create(testName.getMethodName());

    RegionFactory<Integer, byte[]> regionFactory = cache.createRegionFactory(shortcut);
    regionFactory.setDiskStoreName(diskStore.getName());
    regionFactory.setDiskSynchronous(true);

    if (overflow) {
      regionFactory
          .setEvictionAttributes(createLRUEntryAttributes(LRU_ENTRY_COUNT, OVERFLOW_TO_DISK));
    }

    PartitionAttributesFactory<Integer, byte[]> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(0);
    paf.setTotalNumBuckets(TOTAL_NUMBER_BUCKETS);
    regionFactory.setPartitionAttributes(paf.create());

    regionFactory.create(REGION_NAME);
  }

  private File[] getDiskDirs() throws IOException {
    File dir = temporaryFolder.newFolder("disk" + VM.getCurrentVMNum()).getAbsoluteFile();
    return new File[] {dir};
  }
}
