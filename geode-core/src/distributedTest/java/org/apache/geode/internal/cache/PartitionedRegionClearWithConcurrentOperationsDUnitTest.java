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

import static java.time.Duration.ofMillis;
import static org.apache.geode.cache.partition.PartitionRegionHelper.assignBucketsToPartitions;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.util.ArrayUtils.asList;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.AsyncInvocation.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.rules.DistributedRule.getLocators;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionedRegionPartialClearException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper;
import org.apache.geode.internal.cache.PartitionedRegionClearMessage.OperationType;
import org.apache.geode.internal.cache.versions.RegionVersionHolder;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedReference;
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
  private static final Duration DURATION = Duration.ofSeconds(30);
  private static final AtomicBoolean DONE = new AtomicBoolean();

  @Rule
  public DistributedRule distributedRule = new DistributedRule(3);
  @Rule
  public DistributedReference<InternalCache> cache = new DistributedReference<>();

  private VM server1;
  private VM server2;
  private VM accessor;

  @Before
  public void setUp() {
    server1 = getVM(TestVM.SERVER1.getVmId());
    server2 = getVM(TestVM.SERVER2.getVmId());
    accessor = getVM(TestVM.ACCESSOR.getVmId());

    asList(accessor, server1, server2).forEach(vm -> vm.invoke(() -> {
      cache.set((InternalCache) new CacheFactory().set(LOCATORS, getLocators()).create());
    }));
  }

  @After
  public void tearDown() {
    asList(accessor, server1, server2).forEach(vm -> vm.invoke(() -> DONE.set(true)));
  }

  /**
   * The test does the following (clear coordinator and regionType are parametrized):
   * - Launches one thread per VM to continuously execute removes, puts and gets for a given time.
   * - Clears the Partition Region continuously every X milliseconds for a given time.
   * - Asserts that, after the clears have finished, the Region Buckets are consistent across
   * members.
   */
  @Test
  @Parameters({"SERVER1,PARTITION", "ACCESSOR,PARTITION",
      "SERVER1,PARTITION_REDUNDANT", "ACCESSOR,PARTITION_REDUNDANT"})
  @TestCaseName("[{index}] {method}(Coordinator:{0}, RegionType:{1})")
  public void clearWithConcurrentPutGetRemoveShouldWorkCorrectly(TestVM coordinatorVM,
      RegionShortcut regionShortcut) throws InterruptedException {
    createRegions(regionShortcut);

    // Let all VMs continuously execute puts and gets for 60 seconds.
    final int entries = 15000;
    List<AsyncInvocation<Void>> asyncInvocations = Arrays.asList(
        server1.invokeAsync(() -> executePuts(entries)),
        server2.invokeAsync(() -> executeGets(entries)),
        accessor.invokeAsync(() -> executeRemoves(entries)),
        // Clear the region every second for 60 seconds.
        getVM(coordinatorVM.getVmId()).invokeAsync(() -> executeClears(ofMillis(1000))));

    // Let asyncInvocations finish.
    await(asyncInvocations);

    // Assert Region Buckets are consistent.
    accessor.invoke(() -> await().untilAsserted(this::validatePartitionedRegionConsistency));
  }

  /**
   * The test does the following (clear coordinator and regionType are parametrized):
   * - Launches two threads per VM to continuously execute putAll and removeAll for a given time.
   * - Clears the Partition Region continuously every X milliseconds for a given time.
   * - Asserts that, after the clears have finished, the Region Buckets are consistent across
   * members.
   */
  @Test
  @Parameters({"SERVER1,PARTITION", "ACCESSOR,PARTITION",
      "SERVER1,PARTITION_REDUNDANT", "ACCESSOR,PARTITION_REDUNDANT"})
  @TestCaseName("[{index}] {method}(Coordinator:{0}, RegionType:{1})")
  public void clearWithConcurrentPutAllRemoveAllShouldWorkCorrectly(TestVM coordinatorVM,
      RegionShortcut regionShortcut) throws InterruptedException {
    createRegions(regionShortcut);

    // Let all VMs continuously execute putAll and removeAll for 15 seconds.
    List<AsyncInvocation<Void>> asyncInvocations = Arrays.asList(
        server1.invokeAsync(() -> executePutAlls(0, 2000)),
        server1.invokeAsync(() -> executeRemoveAlls(0, 2000)),
        server2.invokeAsync(() -> executePutAlls(2000, 4000)),
        server2.invokeAsync(() -> executeRemoveAlls(2000, 4000)),
        accessor.invokeAsync(() -> executePutAlls(4000, 6000)),
        accessor.invokeAsync(() -> executeRemoveAlls(4000, 6000)),
        // Clear the region every half second for 15 seconds.
        getVM(coordinatorVM.getVmId()).invokeAsync(() -> executeClears(ofMillis(500))));

    // Let asyncInvocations finish.
    await(asyncInvocations);

    // Assert Region Buckets are consistent.
    accessor.invoke(() -> await().untilAsserted(this::validatePartitionedRegionConsistency));
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
  @Parameters({"PARTITION", "PARTITION_REDUNDANT"})
  @TestCaseName("[{index}] {method}(RegionType:{0})")
  public void clearShouldFailWhenCoordinatorMemberIsBounced(RegionShortcut regionShortcut) {
    createRegions(regionShortcut);
    final int entries = 1000;
    accessor.invoke(() -> populateRegion(entries));
    asList(accessor, server1, server2).forEach(vm -> vm.invoke(() -> validateRegion(entries)));

    // Set the CoordinatorMemberKiller and try to clear the region
    server1.invoke(() -> {
      MemberKiller.create(true, this::killMember);
      Region<String, String> region = cache.get().getRegion(REGION_NAME);
      assertThatThrownBy(region::clear)
          .isInstanceOf(DistributedSystemDisconnectedException.class)
          .hasCauseInstanceOf(ForcedDisconnectException.class);
    });

    // Wait for member to get back online and assign all buckets.
    server1.invoke(() -> {
      cache.set((InternalCache) new CacheFactory().set(LOCATORS, getLocators()).create());
      createDataStore(regionShortcut);
      assignBucketsToPartitions(cache.get().getRegion(REGION_NAME));
    });

    // Assert Region Buckets are consistent.
    accessor.invoke(() -> await().untilAsserted(this::validatePartitionedRegionConsistency));
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
  @Parameters({"SERVER1", "ACCESSOR"})
  @TestCaseName("[{index}] {method}(Coordinator:{0})")
  public void clearOnRedundantPartitionRegionWithConcurrentPutGetRemoveShouldWorkCorrectlyWhenNonCoordinatorMembersAreBounced(
      TestVM coordinatorVM) throws InterruptedException {
    createRegions(RegionShortcut.PARTITION_REDUNDANT);
    final int entries = 7500;
    accessor.invoke(() -> populateRegion(entries));
    asList(accessor, server1, server2).forEach(vm -> vm.invoke(() -> validateRegion(entries)));
    server2.invoke(() -> MemberKiller.create(false, this::killMember));

    // Let all VMs (except the one to kill) continuously execute gets, put and removes for 30"
    List<AsyncInvocation<Void>> asyncInvocations = Arrays.asList(
        server1.invokeAsync(() -> executeGets(entries)),
        server1.invokeAsync(() -> executePuts(entries)),
        accessor.invokeAsync(() -> executeGets(entries)),
        accessor.invokeAsync(() -> executeRemoves(entries)));

    // Retry the clear operation on the region until success (server2 will go down, but other
    // members will eventually become primary for those buckets previously hosted by server2).
    getVM(coordinatorVM.getVmId()).invoke(() -> executeClearWithRetry());

    // Wait for member to get back online.
    server2.invoke(() -> {
      cache.set((InternalCache) new CacheFactory().set(LOCATORS, getLocators()).create());
      createDataStore(RegionShortcut.PARTITION_REDUNDANT);
    });

    // Let asyncInvocations finish.
    await(asyncInvocations);

    // Assert Region Buckets are consistent.
    accessor.invoke(() -> await().untilAsserted(this::validatePartitionedRegionConsistency));
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
  @Parameters({"SERVER1", "ACCESSOR"})
  @TestCaseName("[{index}] {method}(Coordinator:{0})")
  public void clearOnNonRedundantPartitionRegionWithConcurrentPutGetRemoveShouldFailWhenNonCoordinatorMembersAreBounced(
      TestVM coordinatorVM) throws InterruptedException {
    createRegions(RegionShortcut.PARTITION);
    final int entries = 7500;
    accessor.invoke(() -> populateRegion(entries));
    asList(accessor, server1, server2).forEach(vm -> vm.invoke(() -> validateRegion(entries)));
    server2.invoke(() -> MemberKiller.create(false, this::killMember));

    // Let all VMs (except the one to kill) continuously execute gets, put and removes for 30"
    List<AsyncInvocation<Void>> asyncInvocations = Arrays.asList(
        server1.invokeAsync(() -> executeGets(entries)),
        server1.invokeAsync(() -> executePuts(entries)),
        accessor.invokeAsync(() -> executeGets(entries)),
        accessor.invokeAsync(() -> executeRemoves(entries)));

    // Clear the region.
    getVM(coordinatorVM.getVmId()).invoke(() -> {
      assertThatThrownBy(() -> cache.get().getRegion(REGION_NAME).clear())
          .isInstanceOf(PartitionedRegionPartialClearException.class);
    });

    // Let asyncInvocations finish.
    await(asyncInvocations);
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
  @Parameters({"SERVER1", "ACCESSOR"})
  @TestCaseName("[{index}] {method}(Coordinator:{0})")
  public void clearOnRedundantPartitionRegionWithConcurrentPutAllRemoveAllShouldWorkCorrectlyWhenNonCoordinatorMembersAreBounced(
      TestVM coordinatorVM) throws InterruptedException {
    createRegions(RegionShortcut.PARTITION_REDUNDANT);
    server2.invoke(() -> MemberKiller.create(false, this::killMember));

    // Let all VMs continuously execute putAll/removeAll for 30 seconds.
    List<AsyncInvocation<Void>> asyncInvocations = Arrays.asList(
        server1.invokeAsync(() -> executePutAlls(0, 6000)),
        accessor.invokeAsync(() -> executeRemoveAlls(2000, 4000)));

    // Retry the clear operation on the region until success (server2 will go down, but other
    // members will eventually become primary for those buckets previously hosted by server2).
    getVM(coordinatorVM.getVmId()).invoke(this::executeClearWithRetry);

    // Wait for member to get back online.
    server2.invoke(() -> {
      cache.set((InternalCache) new CacheFactory().set(LOCATORS, getLocators()).create());
      createDataStore(RegionShortcut.PARTITION_REDUNDANT);
    });

    // Let asyncInvocations finish.
    await(asyncInvocations);

    // Assert Region Buckets are consistent.
    accessor.invoke(() -> await().untilAsserted(this::validatePartitionedRegionConsistency));
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
  @Parameters({"SERVER1", "ACCESSOR"})
  @TestCaseName("[{index}] {method}(Coordinator:{0})")
  public void clearOnNonRedundantPartitionRegionWithConcurrentPutAllRemoveAllShouldFailWhenNonCoordinatorMembersAreBounced(
      TestVM coordinatorVM) throws InterruptedException {
    createRegions(RegionShortcut.PARTITION);
    server2.invoke(() -> MemberKiller.create(false, this::killMember));

    List<AsyncInvocation<Void>> asyncInvocations = Arrays.asList(
        server1.invokeAsync(() -> executePutAlls(0, 6000)),
        accessor.invokeAsync(() -> executeRemoveAlls(2000, 4000)));

    // Clear the region.
    getVM(coordinatorVM.getVmId()).invoke(() -> {
      assertThatThrownBy(() -> cache.get().getRegion(REGION_NAME).clear())
          .isInstanceOf(PartitionedRegionPartialClearException.class);
    });

    // Let asyncInvocations finish.
    await(asyncInvocations);
  }

  private void createAccessor(RegionShortcut regionShortcut) {
    cache.get().createRegionFactory(regionShortcut)
        .setPartitionAttributes(new PartitionAttributesFactory<>()
            .setTotalNumBuckets(BUCKETS)
            .setLocalMaxMemory(0)
            .create())
        .create(REGION_NAME);
  }

  private void createDataStore(RegionShortcut regionShortcut) {
    cache.get().createRegionFactory(regionShortcut)
        .setPartitionAttributes(new PartitionAttributesFactory<>()
            .setTotalNumBuckets(BUCKETS)
            .create())
        .create(REGION_NAME);
  }

  private void createRegions(RegionShortcut regionShortcut) {
    server1.invoke(() -> createDataStore(regionShortcut));
    server2.invoke(() -> createDataStore(regionShortcut));
    accessor.invoke(() -> createAccessor(regionShortcut));
  }

  /**
   * Populates the region and verifies the data on the selected VMs.
   */
  private void populateRegion(int entryCount) {
    Region<String, String> region = cache.get().getRegion(REGION_NAME);
    IntStream.range(0, entryCount).forEach(i -> region.put(String.valueOf(i), "Value_" + i));
  }

  private void validateRegion(int entryCount) {
    Region<String, String> region = cache.get().getRegion(REGION_NAME);
    IntStream.range(0, entryCount)
        .forEach(i -> assertThat(region.get(String.valueOf(i))).isEqualTo("Value_" + i));
  }

  /**
   * Asserts that the RegionVersionVectors for both buckets are consistent.
   *
   * @param bucketId Id of the bucket to compare.
   * @param bucketDump1 First bucketDump.
   * @param bucketDump2 Second bucketDump.
   */
  private void validateRegionVersionVectorsConsistency(int bucketId, BucketDump bucketDump1,
      BucketDump bucketDump2) {
    RegionVersionVector<?> rvv1 = bucketDump1.getRvv();
    RegionVersionVector<?> rvv2 = bucketDump2.getRvv();

    Map<VersionSource<?>, RegionVersionHolder<?>> rvv2Members =
        new HashMap<>(rvv1.getMemberToVersion()); // TODO: getting rvv2Members from rvv1 is wrong
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
  private void validatePartitionedRegionConsistency() throws ForceReattemptException {
    PartitionedRegion region = (PartitionedRegion) cache.get().getRegion(REGION_NAME);
    // Redundant copies + 1 primary.
    int expectedCopies = region.getRedundantCopies() + 1;

    for (int bucketId = 0; bucketId < BUCKETS; bucketId++) {
      List<BucketDump> bucketDumps = region.getAllBucketEntries(bucketId);

      assertThat(bucketDumps.size())
          .as("Bucket " + bucketId + " should have " + expectedCopies + " copies, but has "
              + bucketDumps.size())
          .isEqualTo(expectedCopies);

      // Check that all copies of the bucket have the same data.
      if (!bucketDumps.isEmpty()) {
        validateBucketContents(bucketId, bucketDumps);
      }
    }
  }

  private void validateBucketContents(int bucketId, List<BucketDump> bucketDumps) {
    BucketDump firstDump = bucketDumps.get(0);
    for (BucketDump otherDump : bucketDumps) {
      validateRegionVersionVectorsConsistency(bucketId, firstDump, otherDump);

      assertThat(otherDump.getValues())
          .as("Values for bucket " + bucketId + " on member " + otherDump.getMember()
              + " are not consistent with member " + firstDump.getMember())
          .isEqualTo(firstDump.getValues());

      assertThat(otherDump.getVersions())
          .as("Versions for bucket " + bucketId + " on member " + otherDump.getMember()
              + " are not consistent with member " + firstDump.getMember())
          .isEqualTo(firstDump.getVersions());
    }
  }

  /**
   * Continuously execute get operations on the PartitionedRegion for the given duration.
   */
  private void executeGets(final int entryCount) {
    Region<String, String> region = cache.get().getRegion(REGION_NAME);
    Instant finishTime = Instant.now().plusMillis(DURATION.toMillis());

    while (Instant.now().isBefore(finishTime)) {
      // Region might have been cleared in between, that's why we check for null.
      IntStream.range(0, entryCount).forEach(i -> {
        Optional<String> nullableValue = Optional.ofNullable(region.get(String.valueOf(i)));
        nullableValue.ifPresent(value -> assertThat(value).isEqualTo("Value_" + i));
      });
    }
  }

  /**
   * Continuously execute put operations on the PartitionedRegion for the given duration.
   */
  private void executePuts(final int entryCount) {
    Region<String, String> region = cache.get().getRegion(REGION_NAME);
    Instant finishTime = Instant.now().plusMillis(DURATION.toMillis());

    while (Instant.now().isBefore(finishTime)) {
      IntStream.range(0, entryCount).forEach(i -> region.put(String.valueOf(i), "Value_" + i));
    }
  }

  /**
   * Continuously execute putAll operations on the PartitionedRegion for the given duration.
   */
  private void executePutAlls(final int startKey, final int endKey) {
    Region<String, String> region = cache.get().getRegion(REGION_NAME);
    Map<String, String> valuesToInsert = new HashMap<>();
    IntStream.range(startKey, endKey)
        .forEach(i -> valuesToInsert.put(String.valueOf(i), "Value_" + i));
    Instant finishTime = Instant.now().plusMillis(DURATION.toMillis());

    while (Instant.now().isBefore(finishTime)) {
      region.putAll(valuesToInsert);
    }
  }

  /**
   * Continuously execute remove operations on the PartitionedRegion for the given duration.
   */
  private void executeRemoves(final int entryCount) {
    Region<String, String> region = cache.get().getRegion(REGION_NAME);
    Instant finishTime = Instant.now().plusMillis(DURATION.toMillis());

    while (Instant.now().isBefore(finishTime)) {
      // Region might have been cleared in between, that's why we check for null.
      IntStream.range(0, entryCount).forEach(i -> {
        Optional<String> nullableValue = Optional.ofNullable(region.remove(String.valueOf(i)));
        nullableValue.ifPresent(value -> assertThat(value).isEqualTo("Value_" + i));
      });
    }
  }

  /**
   * Continuously execute removeAll operations on the PartitionedRegion for the given duration.
   */
  private void executeRemoveAlls(final int startKey, final int endKey) {
    List<String> keysToRemove = new ArrayList<>();
    Region<String, String> region = cache.get().getRegion(REGION_NAME);
    IntStream.range(startKey, endKey).forEach(i -> keysToRemove.add(String.valueOf(i)));
    Instant finishTime = Instant.now().plusMillis(DURATION.toMillis());

    while (Instant.now().isBefore(finishTime)) {
      region.removeAll(keysToRemove);
    }
  }

  /**
   * Continuously execute clear operations on the PartitionedRegion every period for the
   * given duration.
   */
  private void executeClears(final Duration period) {
    int count = (int) (DURATION.toMillis() / period.toMillis());
    executeClears(count);
  }

  private void executeClears(int count) {
    Region<String, String> region = cache.get().getRegion(REGION_NAME);
    // for (int invocation = 0; invocation < count; invocation++) {
    while (!DONE.get()) {
      region.clear();
    }
  }

  /**
   * Execute the clear operation and retry until success.
   */
  private void executeClearWithRetry() {
    boolean retry;
    do {
      retry = false;
      try {
        cache.get().getRegion(REGION_NAME).clear();
      } catch (PartitionedRegionPartialClearException pce) {
        retry = true;
      }
    } while (retry);
  }

  private void killMember() {
    InternalDistributedSystem system = cache.get().getInternalDistributedSystem();
    system.stopReconnectingNoDisconnect();
    MembershipManagerHelper.crashDistributedSystem(system);
    await().untilAsserted(() -> assertThat(system.isDisconnected()).isTrue());
  }

  private enum TestVM {
    ACCESSOR(0), SERVER1(1), SERVER2(2);

    private final int vmId;

    TestVM(int vmId) {
      this.vmId = vmId;
    }

    int getVmId() {
      return vmId;
    }
  }

  /**
   * Shutdowns a coordinator member while the clear operation is in progress.
   */
  private static class MemberKiller extends DistributionMessageObserver {

    private final boolean coordinator;
    private final Runnable action;

    private static void create(boolean coordinator, Runnable action) {
      MemberKiller memberKiller = new MemberKiller(coordinator, action);
      DistributionMessageObserver.setInstance(memberKiller);
    }

    private MemberKiller(boolean coordinator, Runnable action) {
      this.coordinator = coordinator;
      this.action = action;
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

    /**
     * Shutdowns the VM whenever the message is an instance of
     * {@link PartitionedRegionClearMessage}.
     */
    private void shutdownMember(DistributionMessage message) {
      if (message instanceof PartitionedRegionClearMessage) {
        PartitionedRegionClearMessage clearMessage = (PartitionedRegionClearMessage) message;
        if (clearMessage.getOperationType() == OperationType.OP_PR_CLEAR) {
          DistributionMessageObserver.setInstance(null);
          action.run();
        }
      }
    }
  }
}
