/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({DistributedTest.class})
public class BucketRebalanceStatRegressionTest extends CacheTestCase {

  private static final int LRU_ENTRY_COUNT = 4;
  private static final int ENTRIES_IN_REGION = 20;
  private static final int TOTAL_NUMBER_BUCKETS = 2;
  private static final String REGION_NAME = "TestRegion";
  private final VM vm0 = Host.getHost(0).getVM(0);
  private final VM vm1 = Host.getHost(0).getVM(1);

  @Before
  public void setUp() throws Exception {
    getSystem();
    getCache();
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
    statsUpdatedAfterRebalance();
  }

  @Test
  public void statsUpdatedAfterRebalanceOverflowPR() throws Exception {
    initializeRegions(RegionShortcut.PARTITION, true);
    validateInitialOverflowStats();
    validateInitialRegion();
    statsUpdatedAfterRebalance();
  }

  @Test
  public void statsUpdatedAfterRebalancePersistentPR() throws Exception {
    initializeRegions(RegionShortcut.PARTITION_PERSISTENT, false);
    validateInitialRegion();
    statsUpdatedAfterRebalance();
  }

  /**
   * Verify that overflow stats are updated when a bucket moves due to rebalancing.
   * 
   * @param shortcut The region shortcut to use to create the region.
   * @param overflow If true the region is configured for overflow to disk, false otherwise.
   * @throws Exception
   */
  private void statsUpdatedAfterRebalance() throws Exception {
    vm0.invoke(() -> rebalance());
    assertThat(vm0.invoke(() -> getCache().getRegion(REGION_NAME).size()))
        .isEqualTo(ENTRIES_IN_REGION);
    assertThat(vm1.invoke(() -> getCache().getRegion(REGION_NAME).size()))
        .isEqualTo(ENTRIES_IN_REGION);
    assertThat(vm0.invoke(() -> ((PartitionedRegion) (getCache().getRegion(REGION_NAME)))
        .getLocalBucketsListTestOnly().size())).isEqualTo(TOTAL_NUMBER_BUCKETS / 2);
    assertThat(vm1.invoke(() -> ((PartitionedRegion) (getCache().getRegion(REGION_NAME)))
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
  private void initializeRegions(RegionShortcut shortcut, boolean overflow) {
    // arrange: create regions and data
    vm0.invoke(() -> {
      createRegion(shortcut, overflow);
    });
    vm0.invoke(() -> loadRegion());
    vm1.invoke(() -> {
      createRegion(shortcut, overflow);
    });
  }

  /**
   * Do validation on the initial region before rebalancing. It is expected that all buckets and
   * data live on vm0; vm1 does not host any buckets.
   * 
   * @param overflow If true the region is set for overflow to disk, false otherwise.
   * @param vm0 One of the members hosting the partitioned region under test.
   * @param vm1 Another member hosting the partitioned region under test.
   */
  private void validateInitialRegion() {
    assertThat(vm0.invoke(() -> getCache().getRegion(REGION_NAME).size()))
        .isEqualTo(ENTRIES_IN_REGION);
    assertThat(vm1.invoke(() -> getCache().getRegion(REGION_NAME).size()))
        .isEqualTo(ENTRIES_IN_REGION);
    assertThat(vm0.invoke(() -> ((PartitionedRegion) (getCache().getRegion(REGION_NAME)))
        .getLocalBucketsListTestOnly().size())).isEqualTo(TOTAL_NUMBER_BUCKETS);
    assertThat(vm1.invoke(() -> ((PartitionedRegion) (getCache().getRegion(REGION_NAME)))
        .getLocalBucketsListTestOnly().size())).isEqualTo(0);
  }

  /**
   * Do validation the initial region for the member containing all the data
   * 
   */
  private void validateInitialOverflowStats() {
    assertThat(vm0.invoke(() -> ((PartitionedRegion) (getCache().getRegion(REGION_NAME)))
        .getDiskRegionStats().getNumEntriesInVM())).isEqualTo(LRU_ENTRY_COUNT);
    assertThat(vm0.invoke(() -> ((PartitionedRegion) (getCache().getRegion(REGION_NAME)))
        .getDiskRegionStats().getNumOverflowOnDisk()))
            .isEqualTo(ENTRIES_IN_REGION - LRU_ENTRY_COUNT);
  }

  /**
   * Validate that the overflow stats are as expected on the given member.
   * 
   * @param vm The member to check stats on.
   * @param vmName The name of the member.
   */
  private void validateOverflowStats(VM vm, String vmName) {
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
   * 
   * @throws Exception
   */
  private void rebalance() throws Exception {
    ResourceManager resMan = getCache().getResourceManager();
    RebalanceFactory factory = resMan.createRebalanceFactory();
    RebalanceOperation rebalanceOp = factory.start();
    RebalanceResults results = rebalanceOp.getResults(); // wait for rebalance to complete
  }

  /**
   * Load the region with some data
   * 
   */
  private void loadRegion() {
    Region aRegion = getCache().getRegion(REGION_NAME);
    for (int i = 1; i <= ENTRIES_IN_REGION; i++) {
      aRegion.put(i, new byte[100]);
    }
  }

  /**
   * Return stats from the region's disk statistics, specifically the numEntriesInVM stat and the
   * numOverflowOnDisk stat.
   * 
   * @return [0] numEntriesInVM stat [1] numOverflowOnDisk stat
   */
  private long[] getOverflowStats() {
    Region testRegion = getCache().getRegion(REGION_NAME);
    PartitionedRegion partitionedRegion = (PartitionedRegion) testRegion;
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
    Region testRegion = getCache().getRegion(REGION_NAME);
    PartitionedRegion pr = (PartitionedRegion) testRegion;
    int totalBucketEntriesInVM = 0;
    int totalBucketEntriesOnDisk = 0;
    Set<Entry<Integer, BucketRegion>> buckets = pr.getDataStore().getAllLocalBuckets();
    for (Map.Entry<Integer, BucketRegion> entry : buckets) {
      BucketRegion bucket = entry.getValue();
      if (bucket != null) {
        totalBucketEntriesInVM += bucket.testHookGetValuesInVM();
        totalBucketEntriesOnDisk += bucket.testHookGetValuesOnDisk();
      }
    }
    return new long[] {totalBucketEntriesInVM, totalBucketEntriesOnDisk};
  }

  /**
   * Create a PartitionedRegion
   */
  private Region<?, ?> createRegion(RegionShortcut shortcut, boolean overflow) {
    Cache cache = getCache();
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    File[] diskDirs = getDiskDirs();
    diskStoreFactory.setDiskDirs(diskDirs);
    DiskStore diskStore = diskStoreFactory.create(getUniqueName());

    RegionFactory<String, String> regionFactory = cache.createRegionFactory(shortcut);
    regionFactory.setDiskStoreName(diskStore.getName());
    regionFactory.setDiskSynchronous(true);
    if (overflow) {
      EvictionAttributes evAttr = EvictionAttributes.createLRUEntryAttributes(LRU_ENTRY_COUNT,
          EvictionAction.OVERFLOW_TO_DISK);
      regionFactory.setEvictionAttributes(evAttr);
    }

    PartitionAttributesFactory prFactory = new PartitionAttributesFactory();
    prFactory.setTotalNumBuckets(TOTAL_NUMBER_BUCKETS);
    prFactory.setRedundantCopies(0);
    regionFactory.setPartitionAttributes(prFactory.create());

    return regionFactory.create(REGION_NAME);
  }

}

