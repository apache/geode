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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.concurrent.TimeoutException;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

@SuppressWarnings("serial")
public class PartitionedRegionStatsDistributedTest implements Serializable {

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

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  private final String regionName = "region";

  @Test
  public void whenOperationsAreDoneOnEntriesThenStatsMustBeCorrect() {
    MemberVM locator1 = clusterStartupRule.startLocatorVM(0);
    int locator1Port = locator1.getPort();

    MemberVM server1 =
        clusterStartupRule.startServerVM(2,
            s -> s.withConnectionToLocator(locator1Port));
    MemberVM server2 =
        clusterStartupRule.startServerVM(3,
            s -> s.withConnectionToLocator(locator1Port));
    MemberVM server3 =
        clusterStartupRule.startServerVM(4,
            s -> s.withConnectionToLocator(locator1Port));
    MemberVM server4 =
        clusterStartupRule.startServerVM(5,
            s -> s.withConnectionToLocator(locator1Port));

    // Create PRs on 3 VMs and accessors on 1 VM
    server1.invoke(() -> createPartitionedRegion(regionName, 200, 1, 5));
    server2.invoke(() -> createPartitionedRegion(regionName, 200, 1, 5));
    server3.invoke(() -> createPartitionedRegion(regionName, 200, 1, 5));
    server4.invoke(() -> createPartitionedRegion(regionName, 0, 1, 5));

    // Do Region operations.
    server1.invoke(() -> doOpsOnRegion(regionName, PUT));
    server1.invoke(() -> doOpsOnRegion(regionName, GET));
    server1.invoke(() -> doOpsOnRegion(regionName, GET_ENTRY));
    server1.invoke(() -> doOpsOnRegion(regionName, CONTAINS_KEY));
    server1.invoke(() -> doOpsOnRegion(regionName, CONTAINS_VALUE_FOR_KEY));
    server1.invoke(() -> doOpsOnRegion(regionName, INVALIDATE));
    server1.invoke(() -> doOpsOnRegion(regionName, DESTROY));

    server1.invoke(() -> validatePartitionedRegionOpsStats(regionName));

    server1.invoke(() -> validateRedundantCopiesStats(regionName));
  }

  /**
   * Test to ensure that tombstone entries are not counted as a part of region entries
   */
  @Test
  public void whenEntryIsDestroyedThenTombstoneShouldNotBePartOfEntries() {
    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 =
        clusterStartupRule.startServerVM(1, s -> s.withConnectionToLocator(locatorPort));
    MemberVM server2 =
        clusterStartupRule.startServerVM(2, s -> s.withConnectionToLocator(locatorPort));
    server1.invoke(() -> createPartitionedRegionWithRebalance(regionName, 0));

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region<Integer, Integer> region = cache.getRegion(regionName);
      region.put(0, 0);
      region.put(1, 1);
      region.put(113, 113);
      region.put(114, 114);
      region.destroy(0);
      region.destroy(1);
    });

    server2.invoke(() -> createPartitionedRegionWithRebalance(regionName, 0));

    server1.invoke(() -> validateEntryCount(regionName, 1));
    server2.invoke(() -> validateEntryCount(regionName, 1));
  }

  @Test
  public void whenRegionPutsAreDoneAndRebalanceCompletedThenStatsShouldBeCorrect() {
    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 =
        clusterStartupRule.startServerVM(1, s -> s.withConnectionToLocator(locatorPort));
    MemberVM server2 =
        clusterStartupRule.startServerVM(2, s -> s.withConnectionToLocator(locatorPort));
    server1.invoke(() -> createPartitionedRegionWithRebalance(regionName, 0));

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region<Integer, Integer> region = cache.getRegion(regionName);
      for (int i = 0; i <= 5; i++) {
        region.put(i, i);
      }
    });

    server2.invoke(() -> createPartitionedRegionWithRebalance(regionName, 0));

    server1.invoke(() -> validateEntryCount(regionName, 3));
    server2.invoke(() -> validateEntryCount(regionName, 3));
  }

  /**
   * Test to make sure the datastore entry count is accurate.
   * Region entry stats are validated before and after entry destroy.
   * Ensure that tombstones and tombstone GC are not affecting the region
   * entry count.
   */
  @Test
  public void tombstoneAndGCShouldNotAffectTheEntryCount() {
    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server0 =
        clusterStartupRule.startServerVM(1, s -> s.withConnectionToLocator(locatorPort));
    MemberVM server1 =
        clusterStartupRule.startServerVM(2, s -> s.withConnectionToLocator(locatorPort));
    MemberVM server2 =
        clusterStartupRule.startServerVM(3, s -> s.withConnectionToLocator(locatorPort));
    server0.invoke(() -> createPartitionedRegionWithRedundantCopies(regionName));
    server1.invoke(() -> createPartitionedRegionWithRedundantCopies(regionName));

    server0.invoke(() -> putDataInRegion(regionName));

    server0.invoke(() -> validateEntryCount(regionName, 4));
    server1.invoke(() -> validateEntryCount(regionName, 4));

    // Do a destroy
    server0.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region<Integer, Integer> region = cache.getRegion(regionName);
      region.destroy(0);
    });

    // We expect the tombstone won't be recorded as part of the entry count
    server0.invoke(() -> validateEntryCount(regionName, 3));
    server1.invoke(() -> validateEntryCount(regionName, 3));

    // Destroy and modify a tombstone
    server0.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region<Integer, Integer> region = cache.getRegion(regionName);
      region.destroy(113);
      region.put(113, 113);
    });

    server0.invoke(() -> validateEntryCount(regionName, 3));
    server1.invoke(() -> validateEntryCount(regionName, 3));

    // After GII (which might include the tombstone), a new members should still see only 2 live
    // entries.
    server2.invoke(() -> createPartitionedRegionWithRedundantCopies(regionName));

    // Wait for redundancy to be restored. Once it is the entry count should be 2
    server2.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
      PartitionedRegionStats prStats = region.getPrStats();
      await()
          .untilAsserted(() -> assertThat(prStats.getLowRedundancyBucketCount()).isEqualTo(0));
    });

    server2.invoke(() -> validateEntryCount(regionName, 3));

    // A tombstone GC shouldn't affect the count.
    server0.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      TombstoneService tombstoneService = cache.getTombstoneService();
      tombstoneService.forceBatchExpirationForTests(1);
    });

    server0.invoke(() -> validateEntryCount(regionName, 3));
    server1.invoke(() -> validateEntryCount(regionName, 3));
    server2.invoke(() -> validateEntryCount(regionName, 3));
  }

  @Test
  public void testThatServersHaveTheCorrectNumberOfBucketsAfterPuts() {
    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server0 =
        clusterStartupRule.startServerVM(1, s -> s.withConnectionToLocator(locatorPort));
    MemberVM server1 =
        clusterStartupRule.startServerVM(2, s -> s.withConnectionToLocator(locatorPort));
    server0.invoke(() -> createPartitionedRegionWithRedundantCopies(regionName));
    server1.invoke(() -> createPartitionedRegionWithRedundantCopies(regionName));

    server0.invoke(() -> putDataInRegion(regionName));
    server1.invoke(() -> putDataInRegion(regionName));

    server0.invoke(() -> validateTotalNumBucketsCount(regionName, DEFAULT_NUM_BUCKETS));
    server1.invoke(() -> validateTotalNumBucketsCount(regionName, DEFAULT_NUM_BUCKETS));
  }

  private void putDataInRegion(final String regionName) {
    Cache cache = ClusterStartupRule.getCache();
    Region<Integer, Integer> region = cache.getRegion(regionName);
    region.put(0, 0);
    region.put(1, 1);
    region.put(113, 113);
    region.put(226, 114);
  }

  private void createPartitionedRegionWithRedundantCopies(final String regionName) {
    PartitionAttributesFactory<Integer, Integer> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(2);

    RegionFactory<Integer, Integer> regionFactory =
        ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    regionFactory.create(regionName);
  }

  private void createPartitionedRegion(final String regionName, final int localMaxMemory,
      final int redundancy, int totalNumBuckets) {
    PartitionAttributesFactory<Integer, Integer> paf = new PartitionAttributesFactory<>();
    paf.setLocalMaxMemory(localMaxMemory);
    paf.setRedundantCopies(redundancy);
    paf.setTotalNumBuckets(totalNumBuckets);

    RegionFactory<Integer, Integer> regionFactory =
        ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());
    regionFactory.create(regionName);
  }

  private void createPartitionedRegionWithRebalance(final String regionName, int redundancy)
      throws InterruptedException, TimeoutException {
    PartitionAttributesFactory<Integer, Integer> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(redundancy);

    RegionFactory<Integer, Integer> regionFactory =
        ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    regionFactory.create(regionName);

    RebalanceOperation op =
        ClusterStartupRule.getCache().getResourceManager().createRebalanceFactory().start();

    op.getResults(2, MINUTES);
  }

  private void validateEntryCount(final String regionName, final int expectedCount) {
    Cache cache = ClusterStartupRule.getCache();
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
    PartitionedRegionStats stats = region.getPrStats();
    CachePerfStats cachePerfStats = region.getCachePerfStats();

    assertThat(stats.getDataStoreEntryCount()).isEqualTo(expectedCount);
    long actualCount = cachePerfStats.stats.getLong(CachePerfStats.entryCountId);
    assertThat(actualCount).isEqualTo(expectedCount);
  }

  private void validateTotalNumBucketsCount(final String regionName, final int expectedCount) {
    Cache cache = ClusterStartupRule.getCache();
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
    PartitionedRegionStats stats = region.getPrStats();

    assertThat(stats.getTotalNumBuckets()).isEqualTo(expectedCount);
  }

  private void doOpsOnRegion(final String regionName, final int opType) {
    doOpsOnRegion(ClusterStartupRule.getCache().getRegion(regionName), opType);
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
    Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(regionName);

    assertThat(region).isNotNull();
    Statistics stats = ((PartitionedRegion) region).getPrStats().getStats();

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

    Region<Object, Object> region = ClusterStartupRule.getCache().getRegion(regionName);
    assertThat(region).isNotNull();

    Statistics stats = ((PartitionedRegion) region).getPrStats().getStats();
    int configuredRedundantCopies = stats.get(STAT_CONFIGURED_REDUNDANT_COPIES).intValue();
    int actualRedundantCopies = stats.get(STAT_ACTUAL_REDUNDANT_COPIES).intValue();

    assertThat(configuredRedundantCopies).isEqualTo(1);
    assertThat(actualRedundantCopies).isEqualTo(1);
  }
}
