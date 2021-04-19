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

import static org.apache.geode.internal.util.ArrayUtils.asList;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionedRegionPartialClearException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper;
import org.apache.geode.internal.cache.versions.RegionVersionHolder;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

/**
 * Tests to verify that {@link PartitionedRegion#clear()} operation can be executed multiple times
 * on the same region while other cache operations are being executed concurrently and members are
 * added or removed.
 */
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class PartitionedRegionClearWithConcurrentOperationsDUnitTest implements Serializable {

  private static final int BUCKETS = 13;
  private static final String REGION_NAME = "PartitionedRegion";
  private static final String TEST_CASE_NAME =
      "[{index}] {method}(Coordinator:{0}, RegionType:{1})";

  @Rule
  public DistributedRule distributedRule = new DistributedRule(3);
  @Rule
  public CacheRule cacheRule = CacheRule.builder().createCacheInAll().build();

  private VM server1;
  private VM server2;
  private VM accessor;

  @SuppressWarnings("unused")
  static TestVM[] coordinators() {
    return new TestVM[] {
        TestVM.SERVER1, TestVM.ACCESSOR
    };
  }

  @SuppressWarnings("unused")
  static Object[] coordinatorsAndRegionTypes() {
    List<Object[]> parameters = new ArrayList<>();
    RegionShortcut[] regionShortcuts = regionTypes();

    Arrays.stream(regionShortcuts).forEach(regionShortcut -> {
      parameters.add(new Object[] {TestVM.SERVER1, regionShortcut});
      parameters.add(new Object[] {TestVM.ACCESSOR, regionShortcut});
    });

    return parameters.toArray();
  }

  private static RegionShortcut[] regionTypes() {
    return new RegionShortcut[] {
        RegionShortcut.PARTITION, RegionShortcut.PARTITION_REDUNDANT
    };
  }

  @Before
  public void setUp() throws Exception {
    server1 = getVM(TestVM.SERVER1.vmNumber);
    server2 = getVM(TestVM.SERVER2.vmNumber);
    accessor = getVM(TestVM.ACCESSOR.vmNumber);
  }

  private void initAccessor(RegionShortcut regionShortcut) {
    PartitionAttributes<String, String> attrs = new PartitionAttributesFactory<String, String>()
        .setTotalNumBuckets(BUCKETS)
        .setLocalMaxMemory(0)
        .create();

    cacheRule.getCache().createRegionFactory(regionShortcut)
        .setPartitionAttributes(attrs)
        .create(REGION_NAME);

  }

  private void initDataStore(RegionShortcut regionShortcut) {
    PartitionAttributes<String, String> attrs = new PartitionAttributesFactory<String, String>()
        .setTotalNumBuckets(BUCKETS)
        .create();

    cacheRule.getCache().createRegionFactory(regionShortcut)
        .setPartitionAttributes(attrs)
        .create(REGION_NAME);
  }

  private void parametrizedSetup(RegionShortcut regionShortcut) {
    server1.invoke(() -> initDataStore(regionShortcut));
    server2.invoke(() -> initDataStore(regionShortcut));
    accessor.invoke(() -> initAccessor(regionShortcut));
  }

  private void waitForSilence() {
    DMStats dmStats = cacheRule.getSystem().getDistributionManager().getStats();
    PartitionedRegion region = (PartitionedRegion) cacheRule.getCache().getRegion(REGION_NAME);
    PartitionedRegionStats partitionedRegionStats = region.getPrStats();

    await().untilAsserted(() -> {
      assertThat(dmStats.getReplyWaitsInProgress()).isEqualTo(0);
      assertThat(partitionedRegionStats.getVolunteeringInProgress()).isEqualTo(0);
      assertThat(partitionedRegionStats.getBucketCreatesInProgress()).isEqualTo(0);
      assertThat(partitionedRegionStats.getPrimaryTransfersInProgress()).isEqualTo(0);
      assertThat(partitionedRegionStats.getRebalanceBucketCreatesInProgress()).isEqualTo(0);
      assertThat(partitionedRegionStats.getRebalancePrimaryTransfersInProgress()).isEqualTo(0);
    });
  }

  /**
   * Populates the region and verifies the data on the selected VMs.
   */
  private void populateRegion(VM feeder, int entryCount, Iterable<VM> vms) {
    feeder.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
      IntStream.range(0, entryCount).forEach(i -> region.put(String.valueOf(i), "Value_" + i));
    });

    vms.forEach(vm -> vm.invoke(() -> {
      waitForSilence();
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);

      IntStream.range(0, entryCount)
          .forEach(i -> assertThat(region.get(String.valueOf(i))).isEqualTo("Value_" + i));
    }));
  }

  /**
   * Asserts that the RegionVersionVectors for both buckets are consistent.
   *
   * @param bucketId Id of the bucket to compare.
   * @param bucketDump1 First bucketDump.
   * @param bucketDump2 Second bucketDump.
   */
  private void assertRegionVersionVectorsConsistency(int bucketId, BucketDump bucketDump1,
      BucketDump bucketDump2) {
    RegionVersionVector<?> rvv1 = bucketDump1.getRvv();
    RegionVersionVector<?> rvv2 = bucketDump2.getRvv();

    if (rvv1 == null) {
      assertThat(rvv2)
          .as("Bucket " + bucketId + " has an RVV on member " + bucketDump2.getMember()
              + ", but does not on member " + bucketDump1.getMember())
          .isNull();
    }

    if (rvv2 == null) {
      assertThat(rvv1)
          .as("Bucket " + bucketId + " has an RVV on member " + bucketDump1.getMember()
              + ", but does not on member " + bucketDump2.getMember())
          .isNull();
    }

    assertThat(rvv1).isNotNull();
    assertThat(rvv2).isNotNull();
    Map<VersionSource<?>, RegionVersionHolder<?>> rvv2Members =
        new HashMap<>(rvv1.getMemberToVersion());
    Map<VersionSource<?>, RegionVersionHolder<?>> rvv1Members =
        new HashMap<>(rvv1.getMemberToVersion());
    for (Map.Entry<VersionSource<?>, RegionVersionHolder<?>> entry : rvv1Members.entrySet()) {
      VersionSource<?> memberId = entry.getKey();
      RegionVersionHolder<?> versionHolder1 = entry.getValue();
      RegionVersionHolder<?> versionHolder2 = rvv2Members.remove(memberId);
      assertThat(versionHolder1)
          .as("RegionVersionVector for bucket " + bucketId + " on member " + bucketDump1.getMember()
              + " is not consistent with member " + bucketDump2.getMember())
          .isEqualTo(versionHolder2);
    }
  }

  /**
   * Asserts that the region data is consistent across buckets.
   */
  private void assertRegionBucketsConsistency() throws ForceReattemptException {
    PartitionedRegion region = (PartitionedRegion) cacheRule.getCache().getRegion(REGION_NAME);
    // Redundant copies + 1 primary.
    int expectedCopies = region.getRedundantCopies() + 1;

    for (int bId = 0; bId < BUCKETS; bId++) {
      final int bucketId = bId;
      List<BucketDump> bucketDumps = region.getAllBucketEntries(bucketId);
      assertThat(bucketDumps.size())
          .as("Bucket " + bucketId + " should have " + expectedCopies + " copies, but has "
              + bucketDumps.size())
          .isEqualTo(expectedCopies);

      // Check that all copies of the bucket have the same data.
      if (bucketDumps.size() > 1) {
        BucketDump firstDump = bucketDumps.get(0);

        for (int j = 1; j < bucketDumps.size(); j++) {
          BucketDump otherDump = bucketDumps.get(j);
          assertRegionVersionVectorsConsistency(bucketId, firstDump, otherDump);

          await().untilAsserted(() -> assertThat(otherDump.getValues())
              .as("Values for bucket " + bucketId + " on member " + otherDump.getMember()
                  + " are not consistent with member " + firstDump.getMember())
              .isEqualTo(firstDump.getValues()));

          await().untilAsserted(() -> assertThat(otherDump.getVersions())
              .as("Versions for bucket " + bucketId + " on member " + otherDump.getMember()
                  + " are not consistent with member " + firstDump.getMember())
              .isEqualTo(firstDump.getVersions()));
        }
      }
    }
  }

  /**
   * Continuously execute get operations on the PartitionedRegion for the given durationInMillis.
   */
  private void executeGets(final int numEntries, final long durationInMillis) {
    Cache cache = cacheRule.getCache();
    Region<String, String> region = cache.getRegion(REGION_NAME);
    Instant finishTime = Instant.now().plusMillis(durationInMillis);

    while (Instant.now().isBefore(finishTime)) {
      // Region might have been cleared in between, that's why we check for null.
      IntStream.range(0, numEntries).forEach(i -> {
        Optional<String> nullableValue = Optional.ofNullable(region.get(String.valueOf(i)));
        nullableValue.ifPresent(value -> assertThat(value).isEqualTo("Value_" + i));
      });
    }
  }

  /**
   * Continuously execute put operations on the PartitionedRegion for the given durationInMillis.
   */
  private void executePuts(final int numEntries, final long durationInMillis) {
    Cache cache = cacheRule.getCache();
    Region<String, String> region = cache.getRegion(REGION_NAME);
    Instant finishTime = Instant.now().plusMillis(durationInMillis);

    while (Instant.now().isBefore(finishTime)) {
      IntStream.range(0, numEntries).forEach(i -> region.put(String.valueOf(i), "Value_" + i));
    }
  }

  /**
   * Continuously execute putAll operations on the PartitionedRegion for the given
   * durationInMillis.
   */
  private void executePutAlls(final int startKey, final int finalKey, final long durationInMillis) {
    Cache cache = cacheRule.getCache();
    Map<String, String> valuesToInsert = new HashMap<>();
    Region<String, String> region = cache.getRegion(REGION_NAME);
    IntStream.range(startKey, finalKey)
        .forEach(i -> valuesToInsert.put(String.valueOf(i), "Value_" + i));
    Instant finishTime = Instant.now().plusMillis(durationInMillis);

    while (Instant.now().isBefore(finishTime)) {
      region.putAll(valuesToInsert);
    }
  }

  /**
   * Continuously execute remove operations on the PartitionedRegion for the given
   * durationInMillis.
   */
  private void executeRemoves(final int numEntries, final long durationInMillis) {
    Cache cache = cacheRule.getCache();
    Region<String, String> region = cache.getRegion(REGION_NAME);
    Instant finishTime = Instant.now().plusMillis(durationInMillis);

    while (Instant.now().isBefore(finishTime)) {
      // Region might have been cleared in between, that's why we check for null.
      IntStream.range(0, numEntries).forEach(i -> {
        Optional<String> nullableValue = Optional.ofNullable(region.remove(String.valueOf(i)));
        nullableValue.ifPresent(value -> assertThat(value).isEqualTo("Value_" + i));
      });
    }
  }

  /**
   * Continuously execute removeAll operations on the PartitionedRegion for the given
   * durationInMillis.
   */
  private void executeRemoveAlls(final int startKey, final int finalKey,
      final long durationInMillis) {
    Cache cache = cacheRule.getCache();
    List<String> keysToRemove = new ArrayList<>();
    Region<String, String> region = cache.getRegion(REGION_NAME);
    IntStream.range(startKey, finalKey).forEach(i -> keysToRemove.add(String.valueOf(i)));
    Instant finishTime = Instant.now().plusMillis(durationInMillis);

    while (Instant.now().isBefore(finishTime)) {
      region.removeAll(keysToRemove);
    }
  }

  /**
   * Execute the clear operation and retry until success.
   */
  private void executeClearWithRetry(VM coordinator) {
    coordinator.invoke(() -> {
      boolean retry;

      do {
        retry = false;

        try {
          cacheRule.getCache().getRegion(REGION_NAME).clear();
        } catch (PartitionedRegionPartialClearException pce) {
          retry = true;
        }

      } while (retry);
    });
  }

  /**
   * Continuously execute clear operations on the PartitionedRegion every periodInMillis for the
   * given durationInMillis.
   */
  private void executeClears(final long durationInMillis, final long periodInMillis) {
    Cache cache = cacheRule.getCache();
    Region<String, String> region = cache.getRegion(REGION_NAME);
    long minimumInvocationCount = durationInMillis / periodInMillis;

    for (int invocationCount = 0; invocationCount < minimumInvocationCount; invocationCount++) {
      region.clear();
    }
  }

  /**
   * The test does the following (clear coordinator and regionType are parametrized):
   * - Launches one thread per VM to continuously execute removes, puts and gets for a given time.
   * - Clears the Partition Region continuously every X milliseconds for a given time.
   * - Asserts that, after the clears have finished, the Region Buckets are consistent across
   * members.
   */
  @Test
  @TestCaseName(TEST_CASE_NAME)
  @Parameters(method = "coordinatorsAndRegionTypes")
  public void clearWithConcurrentPutGetRemoveShouldWorkCorrectly(TestVM coordinatorVM,
      RegionShortcut regionShortcut) throws InterruptedException {
    parametrizedSetup(regionShortcut);

    // Let all VMs continuously execute puts and gets for 60 seconds.
    final int workMillis = 60000;
    final int entries = 15000;
    List<AsyncInvocation<Void>> asyncInvocationList = Arrays.asList(
        server1.invokeAsync(() -> executePuts(entries, workMillis)),
        server2.invokeAsync(() -> executeGets(entries, workMillis)),
        accessor.invokeAsync(() -> executeRemoves(entries, workMillis)));

    // Clear the region every second for 60 seconds.
    getVM(coordinatorVM.vmNumber).invoke(() -> executeClears(workMillis, 1000));

    // Let asyncInvocations finish.
    for (AsyncInvocation<Void> asyncInvocation : asyncInvocationList) {
      asyncInvocation.await();
    }

    // Assert Region Buckets are consistent.
    asList(accessor, server1, server2).forEach(vm -> vm.invoke(this::waitForSilence));
    accessor.invoke(this::assertRegionBucketsConsistency);
  }

  /**
   * The test does the following (clear coordinator and regionType are parametrized):
   * - Launches two threads per VM to continuously execute putAll and removeAll for a given time.
   * - Clears the Partition Region continuously every X milliseconds for a given time.
   * - Asserts that, after the clears have finished, the Region Buckets are consistent across
   * members.
   */
  @Test
  @TestCaseName(TEST_CASE_NAME)
  @Parameters(method = "coordinatorsAndRegionTypes")
  public void clearWithConcurrentPutAllRemoveAllShouldWorkCorrectly(TestVM coordinatorVM,
      RegionShortcut regionShortcut) throws InterruptedException {
    parametrizedSetup(regionShortcut);

    // Let all VMs continuously execute putAll and removeAll for 15 seconds.
    final int workMillis = 15000;
    List<AsyncInvocation<Void>> asyncInvocationList = Arrays.asList(
        server1.invokeAsync(() -> executePutAlls(0, 2000, workMillis)),
        server1.invokeAsync(() -> executeRemoveAlls(0, 2000, workMillis)),
        server2.invokeAsync(() -> executePutAlls(2000, 4000, workMillis)),
        server2.invokeAsync(() -> executeRemoveAlls(2000, 4000, workMillis)),
        accessor.invokeAsync(() -> executePutAlls(4000, 6000, workMillis)),
        accessor.invokeAsync(() -> executeRemoveAlls(4000, 6000, workMillis)));

    // Clear the region every half second for 15 seconds.
    getVM(coordinatorVM.vmNumber).invoke(() -> executeClears(workMillis, 500));

    // Let asyncInvocations finish.
    for (AsyncInvocation<Void> asyncInvocation : asyncInvocationList) {
      asyncInvocation.await();
    }

    // Assert Region Buckets are consistent.
    asList(accessor, server1, server2).forEach(vm -> vm.invoke(this::waitForSilence));
    accessor.invoke(this::assertRegionBucketsConsistency);
  }

  /**
   * The test does the following (regionType is parametrized):
   * - Populates the Partition Region.
   * - Verifies that the entries are synchronized on all members.
   * - Sets the {@link MemberKiller} as a {@link DistributionMessageObserver} to stop the
   * coordinator VM while the clear is in progress.
   * - Clears the Partition Region (at this point the coordinator is restarted).
   * - Asserts that, after the member joins again, the Region Buckets are consistent.
   */
  @Test
  @TestCaseName("[{index}] {method}(RegionType:{0})")
  @Parameters(method = "regionTypes")
  public void clearShouldFailWhenCoordinatorMemberIsBounced(RegionShortcut regionShortcut) {
    parametrizedSetup(regionShortcut);
    final int entries = 1000;
    populateRegion(accessor, entries, asList(accessor, server1, server2));

    // Set the CoordinatorMemberKiller and try to clear the region
    server1.invoke(() -> {
      DistributionMessageObserver.setInstance(new MemberKiller(true));
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
      assertThatThrownBy(region::clear)
          .isInstanceOf(DistributedSystemDisconnectedException.class)
          .hasCauseInstanceOf(ForcedDisconnectException.class);
    });

    // Wait for member to get back online and assign all buckets.
    server1.invoke(() -> {
      cacheRule.createCache();
      initDataStore(regionShortcut);
      await().untilAsserted(
          () -> assertThat(InternalDistributedSystem.getConnectedInstance()).isNotNull());
      PartitionRegionHelper.assignBucketsToPartitions(cacheRule.getCache().getRegion(REGION_NAME));
    });

    // Assert Region Buckets are consistent.
    asList(accessor, server1, server2).forEach(vm -> vm.invoke(this::waitForSilence));
    accessor.invoke(this::assertRegionBucketsConsistency);
  }

  /**
   * The test does the following (clear coordinator is chosen through parameters):
   * - Populates the Partition Region.
   * - Verifies that the entries are synchronized on all members.
   * - Sets the {@link MemberKiller} as a {@link DistributionMessageObserver} to stop a
   * non-coordinator VM while the clear is in progress (the member has primary buckets, though, so
   * participates on the clear operation).
   * - Launches two threads per VM to continuously execute gets, puts and removes for a given time.
   * - Clears the Partition Region (at this point the non-coordinator is restarted).
   * - Asserts that, after the clear has finished, the Region Buckets are consistent across members.
   */
  @Test
  @Parameters(method = "coordinators")
  @TestCaseName("[{index}] {method}(Coordinator:{0})")
  public void clearOnRedundantPartitionRegionWithConcurrentPutGetRemoveShouldWorkCorrectlyWhenNonCoordinatorMembersAreBounced(
      TestVM coordinatorVM) throws InterruptedException {
    parametrizedSetup(RegionShortcut.PARTITION_REDUNDANT);
    final int entries = 7500;
    populateRegion(accessor, entries, asList(accessor, server1, server2));
    server2.invoke(() -> DistributionMessageObserver.setInstance(new MemberKiller(false)));

    // Let all VMs (except the one to kill) continuously execute gets, put and removes for 30"
    final int workMillis = 30000;
    List<AsyncInvocation<Void>> asyncInvocationList = Arrays.asList(
        server1.invokeAsync(() -> executeGets(entries, workMillis)),
        server1.invokeAsync(() -> executePuts(entries, workMillis)),
        accessor.invokeAsync(() -> executeGets(entries, workMillis)),
        accessor.invokeAsync(() -> executeRemoves(entries, workMillis)));

    // Retry the clear operation on the region until success (server2 will go down, but other
    // members will eventually become primary for those buckets previously hosted by server2).
    executeClearWithRetry(getVM(coordinatorVM.vmNumber));

    // Wait for member to get back online.
    server2.invoke(() -> {
      cacheRule.createCache();
      initDataStore(RegionShortcut.PARTITION_REDUNDANT);
      await().untilAsserted(
          () -> assertThat(InternalDistributedSystem.getConnectedInstance()).isNotNull());
    });

    // Let asyncInvocations finish.
    for (AsyncInvocation<Void> asyncInvocation : asyncInvocationList) {
      asyncInvocation.await();
    }

    // Assert Region Buckets are consistent.
    asList(accessor, server1, server2).forEach(vm -> vm.invoke(this::waitForSilence));
    accessor.invoke(this::assertRegionBucketsConsistency);
  }

  /**
   * The test does the following (clear coordinator is chosen through parameters):
   * - Populates the Partition Region.
   * - Verifies that the entries are synchronized on all members.
   * - Sets the {@link MemberKiller} as a {@link DistributionMessageObserver} to stop a
   * non-coordinator VM while the clear is in progress (the member has primary buckets, though, so
   * participates on the clear operation).
   * - Launches two threads per VM to continuously execute gets, puts and removes for a given time.
   * - Clears the Partition Region (at this point the non-coordinator is restarted).
   * - Asserts that the clear operation failed with PartitionedRegionPartialClearException (primary
   * buckets on the the restarted members are not available).
   */
  @Test
  @Parameters(method = "coordinators")
  @TestCaseName("[{index}] {method}(Coordinator:{0})")
  public void clearOnNonRedundantPartitionRegionWithConcurrentPutGetRemoveShouldFailWhenNonCoordinatorMembersAreBounced(
      TestVM coordinatorVM) throws InterruptedException {
    parametrizedSetup(RegionShortcut.PARTITION);
    final int entries = 7500;
    populateRegion(accessor, entries, asList(accessor, server1, server2));
    server2.invoke(() -> DistributionMessageObserver.setInstance(new MemberKiller(false)));

    // Let all VMs (except the one to kill) continuously execute gets, put and removes for 30"
    final int workMillis = 30000;
    List<AsyncInvocation<Void>> asyncInvocationList = Arrays.asList(
        server1.invokeAsync(() -> executeGets(entries, workMillis)),
        server1.invokeAsync(() -> executePuts(entries, workMillis)),
        accessor.invokeAsync(() -> executeGets(entries, workMillis)),
        accessor.invokeAsync(() -> executeRemoves(entries, workMillis)));

    // Clear the region.
    getVM(coordinatorVM.vmNumber).invoke(() -> {
      assertThatThrownBy(() -> cacheRule.getCache().getRegion(REGION_NAME).clear())
          .isInstanceOf(PartitionedRegionPartialClearException.class);
    });

    // Let asyncInvocations finish.
    for (AsyncInvocation<Void> asyncInvocation : asyncInvocationList) {
      asyncInvocation.await();
    }
  }

  /**
   * The test does the following (clear coordinator is chosen through parameters):
   * - Sets the {@link MemberKiller} as a {@link DistributionMessageObserver} to stop a
   * non-coordinator VM while the clear is in progress (the member has primary buckets, though, so
   * participates on the clear operation).
   * - Launches one thread per VM to continuously execute putAll/removeAll for a given time.
   * - Clears the Partition Region (at this point the non-coordinator is restarted).
   * - Asserts that, after the clear has finished, the Region Buckets are consistent across members.
   */
  @Test
  @Parameters(method = "coordinators")
  @TestCaseName("[{index}] {method}(Coordinator:{0})")
  public void clearOnRedundantPartitionRegionWithConcurrentPutAllRemoveAllShouldWorkCorrectlyWhenNonCoordinatorMembersAreBounced(
      TestVM coordinatorVM) throws InterruptedException {
    parametrizedSetup(RegionShortcut.PARTITION_REDUNDANT);
    server2.invoke(() -> DistributionMessageObserver.setInstance(new MemberKiller(false)));

    // Let all VMs continuously execute putAll/removeAll for 30 seconds.
    final int workMillis = 30000;
    List<AsyncInvocation<Void>> asyncInvocationList = Arrays.asList(
        server1.invokeAsync(() -> executePutAlls(0, 6000, workMillis)),
        accessor.invokeAsync(() -> executeRemoveAlls(2000, 4000, workMillis)));

    // Retry the clear operation on the region until success (server2 will go down, but other
    // members will eventually become primary for those buckets previously hosted by server2).
    executeClearWithRetry(getVM(coordinatorVM.vmNumber));

    // Wait for member to get back online.
    server2.invoke(() -> {
      cacheRule.createCache();
      initDataStore(RegionShortcut.PARTITION_REDUNDANT);
      await().untilAsserted(
          () -> assertThat(InternalDistributedSystem.getConnectedInstance()).isNotNull());
    });

    // Let asyncInvocations finish.
    for (AsyncInvocation<Void> asyncInvocation : asyncInvocationList) {
      asyncInvocation.await();
    }

    // Assert Region Buckets are consistent.
    asList(accessor, server1, server2).forEach(vm -> vm.invoke(this::waitForSilence));
    accessor.invoke(this::assertRegionBucketsConsistency);
  }

  /**
   * The test does the following (clear coordinator is chosen through parameters):
   * - Sets the {@link MemberKiller} as a {@link DistributionMessageObserver} to stop a
   * non-coordinator VM while the clear is in progress (the member has primary buckets, though, so
   * participates on the clear operation).
   * - Launches one thread per VM to continuously execute putAll/removeAll for a given time.
   * - Clears the Partition Region (at this point the non-coordinator is restarted).
   * - Asserts that the clear operation failed with PartitionedRegionPartialClearException (primary
   * buckets on the the restarted members are not available).
   */
  @Test
  @Parameters(method = "coordinators")
  @TestCaseName("[{index}] {method}(Coordinator:{0})")
  public void clearOnNonRedundantPartitionRegionWithConcurrentPutAllRemoveAllShouldFailWhenNonCoordinatorMembersAreBounced(
      TestVM coordinatorVM) throws InterruptedException {
    parametrizedSetup(RegionShortcut.PARTITION);
    server2.invoke(() -> DistributionMessageObserver.setInstance(new MemberKiller(false)));

    final int workMillis = 30000;
    List<AsyncInvocation<Void>> asyncInvocationList = Arrays.asList(
        server1.invokeAsync(() -> executePutAlls(0, 6000, workMillis)),
        accessor.invokeAsync(() -> executeRemoveAlls(2000, 4000, workMillis)));

    // Clear the region.
    getVM(coordinatorVM.vmNumber).invoke(() -> {
      assertThatThrownBy(() -> cacheRule.getCache().getRegion(REGION_NAME).clear())
          .isInstanceOf(PartitionedRegionPartialClearException.class);
    });

    // Let asyncInvocations finish.
    for (AsyncInvocation<Void> asyncInvocation : asyncInvocationList) {
      asyncInvocation.await();
    }
  }

  private enum TestVM {
    ACCESSOR(0), SERVER1(1), SERVER2(2);

    final int vmNumber;

    TestVM(int vmNumber) {
      this.vmNumber = vmNumber;
    }
  }

  /**
   * Shutdowns a coordinator member while the clear operation is in progress.
   */
  private static class MemberKiller extends DistributionMessageObserver {

    private final boolean coordinator;

    private MemberKiller(boolean coordinator) {
      this.coordinator = coordinator;
    }

    /**
     * Shutdowns the VM whenever the message is an instance of
     * {@link PartitionedRegionClearMessage}.
     */
    private void shutdownMember(DistributionMessage message) {
      if (message instanceof PartitionedRegionClearMessage) {
        if (((PartitionedRegionClearMessage) message)
            .getOperationType() == PartitionedRegionClearMessage.OperationType.OP_PR_CLEAR) {
          DistributionMessageObserver.setInstance(null);
          InternalDistributedSystem.getConnectedInstance().stopReconnectingNoDisconnect();
          MembershipManagerHelper
              .crashDistributedSystem(InternalDistributedSystem.getConnectedInstance());
          await().untilAsserted(
              () -> assertThat(InternalDistributedSystem.getConnectedInstance()).isNull());
        }
      }
    }

    /**
     * Invoked only on clear coordinator VM.
     *
     * @param dm the distribution manager that received the message
     * @param message The message itself
     */
    @Override
    public void beforeSendMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (coordinator) {
        shutdownMember(message);
      } else {
        super.beforeSendMessage(dm, message);
      }
    }

    /**
     * Invoked only on non clear coordinator VM.
     *
     * @param dm the distribution manager that received the message
     * @param message The message itself
     */
    @Override
    public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (!coordinator) {
        shutdownMember(message);
      } else {
        super.beforeProcessMessage(dm, message);
      }
    }
  }
}
