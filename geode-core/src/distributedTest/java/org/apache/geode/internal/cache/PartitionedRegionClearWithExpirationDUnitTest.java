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

import static org.apache.geode.cache.ExpirationAction.DESTROY;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_PERSISTENT_OVERFLOW;
import static org.apache.geode.internal.util.ArrayUtils.asList;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

/**
 * Tests to verify that {@link PartitionedRegion#clear()} cancels all remaining expiration tasks
 * on the {@link PartitionedRegion} once the operation is executed.
 */
@RunWith(JUnitParamsRunner.class)
public class PartitionedRegionClearWithExpirationDUnitTest implements Serializable {
  private static final Integer BUCKETS = 13;
  private static final Integer EXPIRATION_TIME = 30;
  private static final String REGION_NAME = "PartitionedRegion";

  @Rule
  public DistributedRule distributedRule = new DistributedRule(3);

  @Rule
  public CacheRule cacheRule = CacheRule.builder().createCacheInAll().build();

  @Rule
  public DistributedDiskDirRule distributedDiskDirRule = new DistributedDiskDirRule();

  private VM accessor, server1, server2;

  private enum TestVM {
    ACCESSOR(0), SERVER1(1), SERVER2(2);

    final int vmNumber;

    TestVM(int vmNumber) {
      this.vmNumber = vmNumber;
    }
  }

  @SuppressWarnings("unused")
  static RegionShortcut[] regionTypes() {
    return new RegionShortcut[] {
        PARTITION,
        PARTITION_OVERFLOW,
        PARTITION_REDUNDANT,
        PARTITION_REDUNDANT_OVERFLOW,

        PARTITION_PERSISTENT,
        PARTITION_PERSISTENT_OVERFLOW,
        PARTITION_REDUNDANT_PERSISTENT,
        PARTITION_REDUNDANT_PERSISTENT_OVERFLOW
    };
  }

  @SuppressWarnings("unused")
  static Object[] vmsAndRegionTypes() {
    ArrayList<Object[]> parameters = new ArrayList<>();
    RegionShortcut[] regionShortcuts = regionTypes();

    Arrays.stream(regionShortcuts).forEach(regionShortcut -> {
      parameters.add(new Object[] {TestVM.SERVER1, regionShortcut});
      parameters.add(new Object[] {TestVM.ACCESSOR, regionShortcut});
    });

    return parameters.toArray();
  }

  @Before
  public void setUp() throws Exception {
    server1 = getVM(TestVM.SERVER1.vmNumber);
    server2 = getVM(TestVM.SERVER2.vmNumber);
    accessor = getVM(TestVM.ACCESSOR.vmNumber);
  }

  private RegionShortcut getRegionAccessorShortcut(RegionShortcut dataStoreRegionShortcut) {
    if (dataStoreRegionShortcut.isPersistent()) {
      switch (dataStoreRegionShortcut) {
        case PARTITION_PERSISTENT:
          return PARTITION;
        case PARTITION_PERSISTENT_OVERFLOW:
          return PARTITION_OVERFLOW;
        case PARTITION_REDUNDANT_PERSISTENT:
          return PARTITION_REDUNDANT;
        case PARTITION_REDUNDANT_PERSISTENT_OVERFLOW:
          return PARTITION_REDUNDANT_OVERFLOW;
      }
    }

    return dataStoreRegionShortcut;
  }

  private void initAccessor(RegionShortcut regionShortcut,
      ExpirationAttributes expirationAttributes) {
    RegionShortcut accessorShortcut = getRegionAccessorShortcut(regionShortcut);
    PartitionAttributes<String, String> attributes =
        new PartitionAttributesFactory<String, String>()
            .setTotalNumBuckets(BUCKETS)
            .setLocalMaxMemory(0)
            .create();

    cacheRule.getCache()
        .<String, String>createRegionFactory(accessorShortcut)
        .setPartitionAttributes(attributes)
        .setEntryTimeToLive(expirationAttributes)
        .setEntryIdleTimeout(expirationAttributes)
        .create(REGION_NAME);
  }

  private void initDataStore(RegionShortcut regionShortcut,
      ExpirationAttributes expirationAttributes) {
    PartitionAttributes<String, String> attributes =
        new PartitionAttributesFactory<String, String>()
            .setTotalNumBuckets(BUCKETS)
            .create();

    cacheRule.getCache()
        .<String, String>createRegionFactory(regionShortcut)
        .setPartitionAttributes(attributes)
        .setEntryTimeToLive(expirationAttributes)
        .setEntryIdleTimeout(expirationAttributes)
        .create(REGION_NAME);

    ExpiryTask.expiryTaskListener = new ExpirationListener();
  }

  private void parametrizedSetup(RegionShortcut regionShortcut,
      ExpirationAttributes expirationAttributes) {
    server1.invoke(() -> initDataStore(regionShortcut, expirationAttributes));
    server2.invoke(() -> initDataStore(regionShortcut, expirationAttributes));
    accessor.invoke(() -> initAccessor(regionShortcut, expirationAttributes));
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
  private void populateRegion(VM feeder, int entryCount, List<VM> vms) {
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
   * Asserts that the region is empty on requested VMs.
   */
  private void assertRegionIsEmpty(List<VM> vms) {
    vms.forEach(vm -> vm.invoke(() -> {
      waitForSilence();
      PartitionedRegion region = (PartitionedRegion) cacheRule.getCache().getRegion(REGION_NAME);

      assertThat(region.getLocalSize()).isEqualTo(0);
    }));
  }

  /**
   * Asserts that the region data is consistent across buckets.
   */
  private void assertRegionBucketsConsistency() throws ForceReattemptException {
    waitForSilence();
    List<BucketDump> bucketDumps;
    PartitionedRegion region = (PartitionedRegion) cacheRule.getCache().getRegion(REGION_NAME);
    // Redundant copies + 1 primary.
    int expectedCopies = region.getRedundantCopies() + 1;

    for (int bucketId = 0; bucketId < BUCKETS; bucketId++) {
      bucketDumps = region.getAllBucketEntries(bucketId);
      assertThat(bucketDumps.size()).as("Bucket " + bucketId + " should have " + expectedCopies
          + " copies, but has " + bucketDumps.size()).isEqualTo(expectedCopies);

      // Check that all copies of the bucket have the same data.
      if (bucketDumps.size() > 1) {
        BucketDump firstDump = bucketDumps.get(0);

        for (int j = 1; j < bucketDumps.size(); j++) {
          BucketDump otherDump = bucketDumps.get(j);
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
    }
  }

  /**
   * Register the MemberKiller CacheWriter on the given vms.
   */
  private void registerVMKillerAsCacheWriter(List<VM> vmsToBounce) {
    vmsToBounce.forEach(vm -> vm.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
      region.getAttributesMutator().setCacheWriter(new MemberKiller());
    }));
  }

  /**
   * The test does the following (clear coordinator and region type are parametrized):
   * - Populates the Partition Region (entries have expiration).
   * - Verifies that the entries are synchronized on all members.
   * - Clears the Partition Region once.
   * - Asserts that, after the clear is finished:
   * . No expiration tasks were executed.
   * . All expiration tasks were cancelled.
   * . Map of expiry tasks per bucket is empty.
   * . The Partition Region is empty on all members.
   */
  @Test
  @Parameters(method = "vmsAndRegionTypes")
  @TestCaseName("[{index}] {method}(Coordinator:{0}, RegionType:{1})")
  public void clearShouldRemoveRegisteredExpirationTasks(TestVM coordinatorVM,
      RegionShortcut regionShortcut) {
    final int entries = 500;
    int expirationTime = (int) GeodeAwaitility.getTimeout().getSeconds();
    parametrizedSetup(regionShortcut, new ExpirationAttributes(expirationTime, DESTROY));
    populateRegion(accessor, entries, asList(accessor, server1, server2));

    // Clear the region.
    getVM(coordinatorVM.vmNumber).invoke(() -> {
      Cache cache = cacheRule.getCache();
      cache.getRegion(REGION_NAME).clear();
    });

    // Assert all expiration tasks were cancelled and none were executed.
    asList(server1, server2).forEach(vm -> vm.invoke(() -> {
      ExpirationListener listener = (ExpirationListener) EntryExpiryTask.expiryTaskListener;
      assertThat(listener.tasksRan.get()).isEqualTo(0);
      assertThat(listener.tasksCanceled.get()).isEqualTo(listener.tasksScheduled.get());

      PartitionedRegionDataStore dataStore =
          ((PartitionedRegion) cacheRule.getCache().getRegion(REGION_NAME)).getDataStore();
      Set<BucketRegion> bucketRegions = dataStore.getAllLocalBucketRegions();
      bucketRegions
          .forEach(bucketRegion -> assertThat(bucketRegion.entryExpiryTasks.isEmpty()).isTrue());
    }));

    // Assert Region Buckets are consistent and region is empty,
    accessor.invoke(this::assertRegionBucketsConsistency);
    assertRegionIsEmpty(asList(accessor, server1, server1));
  }

  /**
   * The test does the following (region type is parametrized):
   * - Populates the Partition Region (entries have expiration).
   * - Verifies that the entries are synchronized on all members.
   * - Sets the {@link MemberKiller} as a {@link CacheWriter} to stop the coordinator VM while the
   * clear is in progress.
   * - Clears the Partition Region (at this point the coordinator is restarted).
   * - Asserts that, after the clear is finished and the expiration time is reached:
   * . No expiration tasks were cancelled.
   * . All entries were removed due to the expiration.
   * . The Partition Region Buckets are consistent on all members.
   */
  @Test
  @Parameters(method = "regionTypes")
  @TestCaseName("[{index}] {method}(RegionType:{0})")
  public void clearShouldFailWhenCoordinatorMemberIsBouncedAndExpirationTasksShouldSurvive(
      RegionShortcut regionShortcut) {
    final int entries = 1000;
    ExpirationAttributes expirationAttributes = new ExpirationAttributes(EXPIRATION_TIME, DESTROY);
    parametrizedSetup(regionShortcut, expirationAttributes);
    populateRegion(accessor, entries, asList(accessor, server1, server2));
    registerVMKillerAsCacheWriter(Collections.singletonList(server1));

    // Clear the region (it should fail).
    server1.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
      assertThatThrownBy(region::clear)
          .isInstanceOf(DistributedSystemDisconnectedException.class)
          .hasCauseInstanceOf(ForcedDisconnectException.class);
    });

    // Wait for member to get back online and assign all buckets.
    server1.invoke(() -> {
      cacheRule.createCache();
      initDataStore(regionShortcut, expirationAttributes);
      await().untilAsserted(
          () -> assertThat(InternalDistributedSystem.getConnectedInstance()).isNotNull());
      PartitionRegionHelper.assignBucketsToPartitions(cacheRule.getCache().getRegion(REGION_NAME));
    });

    // Wait until all expiration tasks are executed.
    asList(server1, server2).forEach(vm -> vm.invoke(() -> {
      PartitionedRegionDataStore dataStore =
          ((PartitionedRegion) cacheRule.getCache().getRegion(REGION_NAME)).getDataStore();
      Set<BucketRegion> bucketRegions = dataStore.getAllLocalBucketRegions();
      bucketRegions.forEach(bucketRegion -> await()
          .untilAsserted(() -> assertThat(bucketRegion.entryExpiryTasks.isEmpty()).isTrue()));
    }));

    // At this point the entries should be either invalidated or destroyed (expiration tasks ran).
    asList(accessor, server1, server2).forEach(vm -> vm.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
      IntStream.range(0, entries).forEach(i -> {
        String key = String.valueOf(i);
        assertThat(region.get(key)).isNull();
      });
    }));

    // Assert Region Buckets are consistent.
    accessor.invoke(this::assertRegionBucketsConsistency);
  }

  /**
   * The test does the following (clear coordinator and region type are parametrized):
   * - Populates the Partition Region (entries have expiration).
   * - Verifies that the entries are synchronized on all members.
   * - Sets the {@link MemberKiller} as a {@link CacheWriter} to stop a non-coordinator VM while the
   * clear is in progress (the member has primary buckets, though, so participates on
   * the clear operation).
   * - Clears the Partition Region (at this point the non-coordinator is restarted).
   * - Asserts that, after the clear is finished:
   * . No expiration tasks were executed on the non-restarted members.
   * . All expiration tasks were cancelled on the non-restarted members.
   * . Map of expiry tasks per bucket is empty on the non-restarted members.
   * . All expiration tasks were executed and all expired on the restarted members.
   * . The Partition Region is empty and buckets are consistent across all members.
   */
  @Test
  @Parameters(method = "vmsAndRegionTypes")
  @TestCaseName("[{index}] {method}(Coordinator:{0}, RegionType:{1})")
  public void clearShouldSucceedAndRemoveRegisteredExpirationTasksWhenNonCoordinatorMemberIsBounced(
      TestVM coordinatorVM, RegionShortcut regionShortcut) {
    final int entries = 1500;
    ExpirationAttributes expirationAttributes = new ExpirationAttributes(EXPIRATION_TIME, DESTROY);
    parametrizedSetup(regionShortcut, expirationAttributes);
    registerVMKillerAsCacheWriter(Collections.singletonList(server2));
    populateRegion(accessor, entries, asList(accessor, server1, server2));

    // Clear the region.
    getVM(coordinatorVM.vmNumber).invoke(() -> {
      Cache cache = cacheRule.getCache();
      cache.getRegion(REGION_NAME).clear();
    });

    // Wait for member to get back online and assign buckets.
    server2.invoke(() -> {
      cacheRule.createCache();
      initDataStore(regionShortcut, expirationAttributes);
      await().untilAsserted(
          () -> assertThat(InternalDistributedSystem.getConnectedInstance()).isNotNull());
      PartitionRegionHelper.assignBucketsToPartitions(cacheRule.getCache().getRegion(REGION_NAME));
    });

    // Assert all expiration tasks were cancelled and none were executed (surviving members).
    server1.invoke(() -> {
      PartitionedRegionDataStore dataStore =
          ((PartitionedRegion) cacheRule.getCache().getRegion(REGION_NAME)).getDataStore();
      Set<BucketRegion> bucketRegions = dataStore.getAllLocalBucketRegions();
      bucketRegions
          .forEach(bucketRegion -> assertThat(bucketRegion.entryExpiryTasks.isEmpty()).isTrue());

      ExpirationListener listener = (ExpirationListener) EntryExpiryTask.expiryTaskListener;
      assertThat(listener.tasksRan.get()).isEqualTo(0);
      assertThat(listener.tasksCanceled.get()).isEqualTo(listener.tasksScheduled.get());
    });

    // Assert all expiration tasks were expired as the region is empty (restarted member).
    server2.invoke(() -> {
      PartitionedRegionDataStore dataStore =
          ((PartitionedRegion) cacheRule.getCache().getRegion(REGION_NAME)).getDataStore();
      Set<BucketRegion> bucketRegions = dataStore.getAllLocalBucketRegions();

      // During restart, the member loads the region from disk and automatically registers
      // expiration tasks for each entry. After GII, however, the region is empty due to the
      // clear operation and the tasks will just expire as there are no entries.
      bucketRegions.forEach(bucketRegion -> await()
          .untilAsserted(() -> assertThat(bucketRegion.entryExpiryTasks.isEmpty()).isTrue()));

      ExpirationListener listener = (ExpirationListener) EntryExpiryTask.expiryTaskListener;
      assertThat(listener.tasksExpired.get()).isEqualTo(listener.tasksRan.get());
    });

    // Assert Region Buckets are consistent and region is empty,
    accessor.invoke(this::assertRegionBucketsConsistency);
    assertRegionIsEmpty(asList(accessor, server1, server1));
  }

  /**
   * Tracks expiration tasks lifecycle.
   */
  public static class ExpirationListener implements ExpiryTask.ExpiryTaskListener {
    final AtomicInteger tasksRan = new AtomicInteger(0);
    final AtomicInteger tasksExpired = new AtomicInteger(0);
    final AtomicInteger tasksCanceled = new AtomicInteger(0);
    final AtomicInteger tasksScheduled = new AtomicInteger(0);

    @Override
    public void afterSchedule(ExpiryTask et) {
      tasksScheduled.incrementAndGet();
    }

    @Override
    public void afterTaskRan(ExpiryTask et) {
      tasksRan.incrementAndGet();
    }

    @Override
    public void afterReschedule(ExpiryTask et) {}

    @Override
    public void afterExpire(ExpiryTask et) {
      tasksExpired.incrementAndGet();
    }

    @Override
    public void afterCancel(ExpiryTask et) {
      tasksCanceled.incrementAndGet();
    }
  }

  /**
   * Shutdowns a member while the clear operation is in progress.
   * The writer is only installed on the member the test wants to shutdown, doesn't matter whether
   * it's the clear coordinator or another member holding primary buckets.
   */
  public static class MemberKiller extends CacheWriterAdapter<String, String> {

    @Override
    public synchronized void beforeRegionClear(RegionEvent<String, String> event)
        throws CacheWriterException {
      InternalDistributedSystem.getConnectedInstance().stopReconnectingNoDisconnect();
      MembershipManagerHelper.crashDistributedSystem(
          InternalDistributedSystem.getConnectedInstance());
      await().untilAsserted(
          () -> assertThat(InternalDistributedSystem.getConnectedInstance()).isNull());
    }
  }
}
