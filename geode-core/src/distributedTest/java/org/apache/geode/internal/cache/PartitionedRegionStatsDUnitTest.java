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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;


public class PartitionedRegionStatsDUnitTest extends CacheTestCase {

  private static final int CNT = 10;
  private static final int PUT = 1;
  private static final int GET = 2;
  private static final int CONTAINS_KEY = 3;
  private static final int CONTAINS_VALUE_FOR_KEY = 4;
  private static final int INVALIDATE = 5;
  private static final int DESTROY = 6;
  private static final int CREATE = 7;
  private static final int GET_ENTRY = 8;

  private static final int DEFAULT_NUM_BUCKETS = 113;
  public static final String STAT_CONFIGURED_REDUNDANT_COPIES = "configuredRedundantCopies";
  public static final String STAT_ACTUAL_REDUNDANT_COPIES = "actualRedundantCopies";

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  private String regionName;

  @Before
  public void setUp() throws Exception {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);
    vm3 = getHost(0).getVM(3);

    regionName = "region";
  }

  @Test
  public void testClose() throws Exception {
    // Create PRs on 3 VMs and accessors on 1 VM
    vm0.invoke(() -> createPartitionedRegion(regionName, 200, 1, 5));
    vm1.invoke(() -> createPartitionedRegion(regionName, 200, 1, 5));
    vm2.invoke(() -> createPartitionedRegion(regionName, 200, 1, 5));
    vm3.invoke(() -> createPartitionedRegion(regionName, 0, 1, 5));

    // Do Region operations.
    vm0.invoke(() -> doOpsOnRegion(regionName, PUT));
    vm0.invoke(() -> doOpsOnRegion(regionName, GET));
    vm0.invoke(() -> doOpsOnRegion(regionName, GET_ENTRY));
    vm0.invoke(() -> doOpsOnRegion(regionName, CONTAINS_KEY));
    vm0.invoke(() -> doOpsOnRegion(regionName, CONTAINS_VALUE_FOR_KEY));
    vm0.invoke(() -> doOpsOnRegion(regionName, INVALIDATE));
    vm0.invoke(() -> doOpsOnRegion(regionName, DESTROY));

    vm0.invoke(() -> validatePartitionedRegionOpsStats(regionName));

    vm0.invoke(() -> validateRedundantCopiesStats(regionName));
  }

  /**
   * Ok, first problem, GC'd tombstone is counted as an entry To test - modifying a tombstone -
   * modifying and doing tombstone GC?
   */
  @Test
  public void testDataStoreEntryCountWithRebalance() throws Exception {
    vm0.invoke(() -> createPartitionedRegionWithRebalance(regionName, 0));

    vm0.invoke(() -> {
      Cache cache = getCache();
      Region<Integer, Integer> region = cache.getRegion(regionName);
      region.put(0, 0);
      region.put(1, 1);
      region.put(113, 113);
      region.put(114, 114);
      region.destroy(0);
      region.destroy(1);
    });

    vm1.invoke(() -> createPartitionedRegionWithRebalance(regionName, 0));

    vm0.invoke(() -> validateEntryCount(regionName, 1));
    vm1.invoke(() -> validateEntryCount(regionName, 1));
  }

  @Test
  public void testDataStoreEntryCount2WithRebalance() throws Exception {
    vm0.invoke(() -> createPartitionedRegionWithRebalance(regionName, 0));

    vm0.invoke(() -> {
      Cache cache = getCache();
      Region<Integer, Integer> region = cache.getRegion(regionName);
      for (int i = 0; i <= 5; i++) {
        region.put(i, i);
      }
    });

    vm1.invoke(() -> createPartitionedRegionWithRebalance(regionName, 0));

    vm0.invoke(() -> validateEntryCount(regionName, 3));
    vm1.invoke(() -> validateEntryCount(regionName, 3));
  }

  /**
   * Test to make sure the datastore entry count is accurate.
   *
   * Ok, first problem, GC'd tombstone is counted as an entry To test - modifying a tombstone -
   * modifying and doing tombstone GC?
   */
  @Test
  public void testDataStoreEntryCount() throws Exception {
    vm0.invoke(() -> createPartitionedRegionWithRedundantCopies(regionName));
    vm1.invoke(() -> createPartitionedRegionWithRedundantCopies(regionName));

    vm0.invoke(() -> putDataInRegion(regionName));

    vm0.invoke(() -> validateEntryCount(regionName, 4));
    vm1.invoke(() -> validateEntryCount(regionName, 4));

    // Do a destroy
    vm0.invoke(() -> {
      Cache cache = getCache();
      Region<Integer, Integer> region = cache.getRegion(regionName);
      region.destroy(0);
    });

    // We expect the tombstone won't be recorded as part of the entry count
    vm0.invoke(() -> validateEntryCount(regionName, 3));
    vm1.invoke(() -> validateEntryCount(regionName, 3));

    // Destroy and modify a tombstone
    vm0.invoke(() -> {
      Cache cache = getCache();
      Region<Integer, Integer> region = cache.getRegion(regionName);
      region.destroy(113);
      region.put(113, 113);
    });

    vm0.invoke(() -> validateEntryCount(regionName, 3));
    vm1.invoke(() -> validateEntryCount(regionName, 3));

    // After GII (which might include the tombstone), a new members should still see only 2 live
    // entries.
    vm2.invoke(() -> createPartitionedRegionWithRedundantCopies(regionName));

    // Wait for redundancy to be restored. Once it is the entry count should be 2
    vm2.invoke(() -> {
      Cache cache = getCache();
      PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
      PartitionedRegionStats prStats = region.getPrStats();
      await()
          .untilAsserted(() -> assertThat(prStats.getLowRedundancyBucketCount()).isEqualTo(0));
    });

    vm2.invoke(() -> validateEntryCount(regionName, 3));

    // A tombstone GC shouldn't affect the count.
    vm0.invoke(() -> {
      InternalCache cache = getCache();
      TombstoneService tombstoneService = cache.getTombstoneService();
      tombstoneService.forceBatchExpirationForTests(1);
    });

    vm0.invoke(() -> validateEntryCount(regionName, 3));
    vm1.invoke(() -> validateEntryCount(regionName, 3));
    vm2.invoke(() -> validateEntryCount(regionName, 3));
  }

  @Test
  public void testTotalNumBuckets() {
    vm0.invoke(() -> createPartitionedRegionWithRedundantCopies(regionName));
    vm1.invoke(() -> createPartitionedRegionWithRedundantCopies(regionName));

    vm0.invoke(() -> putDataInRegion(regionName));
    vm1.invoke(() -> putDataInRegion(regionName));

    vm0.invoke(() -> validateTotalNumBucketsCount(regionName, DEFAULT_NUM_BUCKETS));
    vm1.invoke(() -> validateTotalNumBucketsCount(regionName, DEFAULT_NUM_BUCKETS));
  }

  private void putDataInRegion(final String regionName) {
    Cache cache = getCache();
    Region<Integer, Integer> region = cache.getRegion(regionName);
    region.put(0, 0);
    region.put(1, 1);
    region.put(113, 113);
    region.put(226, 114);
  }

  private void createPartitionedRegionWithRedundantCopies(final String regionName) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(2);

    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    regionFactory.create(regionName);
  }

  private void createPartitionedRegion(final String regionName, final int localMaxMemory,
      final int redundancy, int totalNumBuckets) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setLocalMaxMemory(localMaxMemory);
    paf.setRedundantCopies(redundancy);
    paf.setTotalNumBuckets(totalNumBuckets);

    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    regionFactory.create(regionName);
  }

  private void createPartitionedRegionWithRebalance(final String regionName, int redundancy)
      throws InterruptedException, TimeoutException {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundancy);

    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    regionFactory.create(regionName);

    RebalanceOperation op = getCache().getResourceManager().createRebalanceFactory().start();

    op.getResults(2, MINUTES);
  }

  private void validateEntryCount(final String regionName, final int expectedCount) {
    Cache cache = getCache();
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
    PartitionedRegionStats stats = region.getPrStats();
    CachePerfStats cachePerfStats = region.getCachePerfStats();

    assertThat(stats.getDataStoreEntryCount()).isEqualTo(expectedCount);
    assertThat(cachePerfStats.getEntries()).isEqualTo(expectedCount);
  }

  private void validateTotalNumBucketsCount(final String regionName, final int expectedCount) {
    Cache cache = getCache();
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
    PartitionedRegionStats stats = region.getPrStats();

    assertThat(stats.getTotalNumBuckets()).isEqualTo(expectedCount);
  }

  private void doOpsOnRegion(final String regionName, final int opType) {
    doOpsOnRegion(getCache().getRegion(regionName), opType);
  }

  private void doOpsOnRegion(final Region<Integer, Integer> region, final int opType) {
    switch (opType) {
      case PUT:
        for (int k = 0; k < CNT; k++) {
          region.put(k, k);
        }
        break;
      case GET:
        for (int k = 0; k < CNT; k++) {
          region.get(k);
        }
        break;
      case CONTAINS_KEY:
        for (int k = 0; k < CNT; k++) {
          region.containsKey(k);
        }
        break;
      case CONTAINS_VALUE_FOR_KEY:
        for (int k = 0; k < CNT; k++) {
          region.containsValueForKey(k);
        }
        break;
      case INVALIDATE:
        for (int k = 0; k < CNT; k++) {
          region.invalidate(k);
        }
        break;
      case DESTROY:
        for (int k = 0; k < CNT; k++) {
          region.destroy(k);
        }
        break;
      case CREATE:
        for (int k = 0; k < CNT; k++) {
          region.create(k, k);
        }
        break;
      case GET_ENTRY:
        for (int k = 0; k < CNT; k++) {
          region.getEntry(k);
        }
    }
  }

  private void validatePartitionedRegionOpsStats(final String regionName) {
    Cache cache = getCache();
    PartitionedRegion partitionedRegion = (PartitionedRegion) cache.getRegion(regionName);

    assertThat(partitionedRegion).isNotNull();

    Statistics stats = partitionedRegion.getPrStats().getStats();

    int putsCompleted = stats.get("putsCompleted").intValue();
    int getsCompleted = stats.get("getsCompleted").intValue();
    int getEntryCompleted = stats.get("getEntryCompleted").intValue();
    int createsCompleted = stats.get("createsCompleted").intValue();
    int containsKeyCompleted = stats.get("containsKeyCompleted").intValue();
    int containsValueForKeyCompleted = stats.get("containsValueForKeyCompleted").intValue();
    int invalidatesCompleted = stats.get("invalidatesCompleted").intValue();
    int destroysCompleted = stats.get("destroysCompleted").intValue();

    assertThat(putsCompleted).isEqualTo(CNT);
    assertThat(getsCompleted).isEqualTo(CNT);
    assertThat(getEntryCompleted).isEqualTo(CNT);
    assertThat(createsCompleted).isEqualTo(0);
    assertThat(containsKeyCompleted).isEqualTo(CNT);
    assertThat(containsValueForKeyCompleted).isEqualTo(CNT);
    assertThat(invalidatesCompleted).isEqualTo(CNT);
    assertThat(destroysCompleted).isEqualTo(CNT);

    // TODO: Need to validate that bucket stats add up....
    // partitionedRegion.getDataStore().getCachePerfStats().getGets(); // etc...
  }

  private void validateRedundantCopiesStats(final String regionName) {
    Cache cache = getCache();
    PartitionedRegion partitionedRegion = (PartitionedRegion) cache.getRegion(regionName);

    assertThat(partitionedRegion).isNotNull();

    Statistics stats = partitionedRegion.getPrStats().getStats();
    int configuredRedundantCopies = stats.get(STAT_CONFIGURED_REDUNDANT_COPIES).intValue();
    int actualRedundantCopies = stats.get(STAT_ACTUAL_REDUNDANT_COPIES).intValue();

    assertThat(configuredRedundantCopies).isEqualTo(1);
    assertThat(actualRedundantCopies).isEqualTo(1);
  }
}
