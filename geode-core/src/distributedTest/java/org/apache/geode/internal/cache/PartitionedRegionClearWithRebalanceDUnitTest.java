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

import static org.apache.geode.cache.PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_PERSISTENT;
import static org.apache.geode.internal.util.ArrayUtils.asList;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DUnitBlackboard;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

@RunWith(JUnitParamsRunner.class)
public class PartitionedRegionClearWithRebalanceDUnitTest implements Serializable {
  private static final long serialVersionUID = -7183993832801073933L;

  private static final Integer BUCKETS = GLOBAL_MAX_BUCKETS_DEFAULT;
  private static final String REGION_NAME = "PartitionedRegion";
  public static final String DISK_STORE_NAME = "diskStore";
  public static final String BEGIN_CLEAR = "begin-clear";
  private static final int ENTRIES = 10000;

  @Rule
  public DistributedRule distributedRule = new DistributedRule(3);

  @Rule
  public CacheRule cacheRule = CacheRule.builder().createCacheInAll().build();

  @Rule
  public DistributedDiskDirRule distributedDiskDirRule = new DistributedDiskDirRule();

  private static transient DUnitBlackboard blackboard;

  private VM accessor;
  private VM server1;
  private VM server2;

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
        PARTITION_REDUNDANT,
        PARTITION_REDUNDANT_PERSISTENT,
    };
  }

  @SuppressWarnings("unused")
  static Object[] vmsAndRegionTypes() {
    ArrayList<Object[]> parameters = new ArrayList<>();
    RegionShortcut[] regionShortcuts = regionTypes();

    Arrays.stream(regionShortcuts).forEach(regionShortcut -> {
      // {ClearCoordinatorVM, RebalanceVM, regionShortcut}
      parameters.add(new Object[] {TestVM.SERVER1, TestVM.SERVER1, regionShortcut});
      parameters.add(new Object[] {TestVM.SERVER1, TestVM.ACCESSOR, regionShortcut});
      parameters.add(new Object[] {TestVM.ACCESSOR, TestVM.SERVER1, regionShortcut});
      parameters.add(new Object[] {TestVM.ACCESSOR, TestVM.ACCESSOR, regionShortcut});
    });

    return parameters.toArray();
  }

  @Before
  public void setUp() throws Exception {
    getBlackboard().initBlackboard();
    server1 = getVM(TestVM.SERVER1.vmNumber);
    server2 = getVM(TestVM.SERVER2.vmNumber);
    accessor = getVM(TestVM.ACCESSOR.vmNumber);
  }

  private static DUnitBlackboard getBlackboard() {
    if (blackboard == null) {
      blackboard = new DUnitBlackboard();
    }
    return blackboard;
  }

  private RegionShortcut getRegionAccessorShortcut(RegionShortcut dataStoreRegionShortcut) {
    if (dataStoreRegionShortcut.isPersistent()) {
      switch (dataStoreRegionShortcut) {
        case PARTITION_PERSISTENT:
          return PARTITION;
        case PARTITION_REDUNDANT_PERSISTENT:
          return PARTITION_REDUNDANT;
      }
    }

    return dataStoreRegionShortcut;
  }

  private void initAccessor(RegionShortcut regionShortcut) {
    RegionShortcut accessorShortcut = getRegionAccessorShortcut(regionShortcut);
    // StartupRecoveryDelay is set to infinite to prevent automatic rebalancing when creating the
    // region on other members
    PartitionAttributes<String, String> attributes =
        new PartitionAttributesFactory<String, String>()
            .setTotalNumBuckets(BUCKETS)
            .setStartupRecoveryDelay(-1)
            .setLocalMaxMemory(0)
            .create();

    cacheRule.getCache()
        .<String, String>createRegionFactory(accessorShortcut)
        .setPartitionAttributes(attributes)
        .create(REGION_NAME);
  }

  private void initDataStore(RegionShortcut regionShortcut) {
    // StartupRecoveryDelay is set to infinite to prevent automatic rebalancing when creating the
    // region on other members
    PartitionAttributes<String, String> attributes =
        new PartitionAttributesFactory<String, String>()
            .setTotalNumBuckets(BUCKETS)
            .setStartupRecoveryDelay(-1)
            .create();

    RegionFactory<String, String> factory = cacheRule.getCache()
        .<String, String>createRegionFactory(regionShortcut)
        .setPartitionAttributes(attributes);

    if (regionShortcut.isPersistent()) {
      factory.setDiskStoreName(
          cacheRule.getCache().createDiskStoreFactory().create(DISK_STORE_NAME).getName());
    }

    factory.create(REGION_NAME);
  }

  private void parametrizedSetup(RegionShortcut regionShortcut) {
    // Create and populate the region on server1 first, to create an unbalanced distribution of data
    server1.invoke(() -> {
      initDataStore(regionShortcut);
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
      IntStream.range(0, ENTRIES).forEach(i -> region.put("key" + i, "value" + i));
    });
    server2.invoke(() -> initDataStore(regionShortcut));
    accessor.invoke(() -> initAccessor(regionShortcut));
  }

  private AsyncInvocation<Object> setupAndPrepareClear(TestVM clearCoordinatorVM,
      RegionShortcut regionType) {
    parametrizedSetup(regionType);

    return getVM(clearCoordinatorVM.vmNumber).invokeAsync(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
      // Wait for the signal from the blackboard before triggering the clear to start
      getBlackboard().waitForGate(BEGIN_CLEAR, GeodeAwaitility.getTimeout().toMillis(),
          TimeUnit.MILLISECONDS);
      region.clear();
    });
  }

  private RebalanceResults startRebalanceAndGetResults() throws InterruptedException {
    // Start a rebalance and wait until bucket creation for redundancy recovery (the first stage of
    // a rebalance operation) has started before signalling the blackboard
    RebalanceOperation rebalanceOp =
        cacheRule.getCache().getResourceManager().createRebalanceFactory().start();
    await().untilAsserted(() -> assertThat(cacheRule.getCache().getInternalResourceManager()
        .getStats().getRebalanceBucketCreatesCompleted(), greaterThan(0)));
    getBlackboard().signalGate(BEGIN_CLEAR);

    return rebalanceOp.getResults();
  }

  private void waitForSilence() {
    DMStats dmStats = cacheRule.getSystem().getDistributionManager().getStats();
    PartitionedRegion region = (PartitionedRegion) cacheRule.getCache().getRegion(REGION_NAME);
    PartitionedRegionStats partitionedRegionStats = region.getPrStats();

    await().untilAsserted(() -> {
      Assertions.assertThat(dmStats.getReplyWaitsInProgress()).isEqualTo(0);
      Assertions.assertThat(partitionedRegionStats.getVolunteeringInProgress()).isEqualTo(0);
      Assertions.assertThat(partitionedRegionStats.getBucketCreatesInProgress()).isEqualTo(0);
      Assertions.assertThat(partitionedRegionStats.getPrimaryTransfersInProgress()).isEqualTo(0);
      Assertions.assertThat(partitionedRegionStats.getRebalanceBucketCreatesInProgress())
          .isEqualTo(0);
      Assertions.assertThat(partitionedRegionStats.getRebalancePrimaryTransfersInProgress())
          .isEqualTo(0);
    });
  }

  private void assertRegionIsEmpty(List<VM> vms) {
    vms.forEach(vm -> vm.invoke(() -> {
      waitForSilence();
      PartitionedRegion region = (PartitionedRegion) cacheRule.getCache().getRegion(REGION_NAME);

      Assertions.assertThat(region.getLocalSize()).isEqualTo(0);
    }));
  }

  private void registerVMKillerAsCacheWriter(List<VM> vmsToBounce) {
    vmsToBounce.forEach(vm -> vm.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
      region.getAttributesMutator().setCacheWriter(new MemberKiller());
    }));
  }

  @Test
  @Parameters(method = "vmsAndRegionTypes")
  @TestCaseName("[{index}] {method}(ClearCoordinator:{0}, RebalanceCoordinator:{1}, RegionType:{2})")
  public void clearRegionDuringRebalanceClearsRegion(TestVM clearCoordinatorVM,
      TestVM rebalanceVM, RegionShortcut regionType) throws InterruptedException {
    AsyncInvocation<?> clearInvocation = setupAndPrepareClear(clearCoordinatorVM, regionType);

    getVM(rebalanceVM.vmNumber).invoke(() -> {
      RebalanceResults results = startRebalanceAndGetResults();

      // Verify that rebalance did some work
      int combinedResults = results.getTotalBucketTransfersCompleted()
          + results.getTotalBucketCreatesCompleted() + results.getTotalPrimaryTransfersCompleted();
      assertThat(combinedResults, greaterThan(0));

      // Verify that no bucket creates failed during the rebalance
      assertThat(cacheRule.getCache().getInternalResourceManager().getStats()
          .getRebalanceBucketCreatesFailed(), is(0));
    });

    clearInvocation.await();

    // Assert that the region is empty
    assertRegionIsEmpty(asList(accessor, server1, server2));
  }

  @Test
  @Parameters(method = "vmsAndRegionTypes")
  @TestCaseName("[{index}] {method}(ClearCoordinator:{0}, RebalanceCoordinator:{1}, RegionType:{2})")
  public void clearRegionDuringRebalancePrimaryReassignmentClearsRegion(TestVM clearCoordinatorVM,
      TestVM rebalanceVM, RegionShortcut regionType) throws InterruptedException {
    AsyncInvocation<?> clearInvocation = setupAndPrepareClear(clearCoordinatorVM, regionType);

    getVM(rebalanceVM.vmNumber).invoke(() -> {
      // Start a rebalance and wait until primary reassignment has started before signalling the
      // blackboard
      RebalanceOperation rebalanceOp =
          cacheRule.getCache().getResourceManager().createRebalanceFactory().start();
      await().untilAsserted(() -> assertThat(cacheRule.getCache().getInternalResourceManager()
          .getStats().getRebalancePrimaryTransfersCompleted(), greaterThan(0)));
      getBlackboard().signalGate(BEGIN_CLEAR);

      // Verify that rebalance did some work
      RebalanceResults results = rebalanceOp.getResults();
      int combinedResults = results.getTotalBucketTransfersCompleted()
          + results.getTotalBucketCreatesCompleted() + results.getTotalPrimaryTransfersCompleted();
      assertThat(combinedResults, greaterThan(0));

      // Verify that no primary transfers failed during the rebalance
      assertThat(cacheRule.getCache().getInternalResourceManager().getStats()
          .getRebalancePrimaryTransfersFailed(), is(0));
    });

    clearInvocation.await();

    // Assert that the region is empty
    assertRegionIsEmpty(asList(accessor, server1, server2));
  }

  @Test
  @Parameters(method = "vmsAndRegionTypes")
  @TestCaseName("[{index}] {method}(ClearCoordinator:{0}, RebalanceCoordinator:{1}, RegionType:{2})")
  public void clearRegionDuringRebalanceClearsRegionWhenNonCoordinatorIsBounced(
      TestVM clearCoordinatorVM, TestVM rebalanceVM, RegionShortcut regionType)
      throws InterruptedException {
    AsyncInvocation<?> clearInvocation = setupAndPrepareClear(clearCoordinatorVM, regionType);

    // Server 2 is never the clear coordinator
    registerVMKillerAsCacheWriter(Collections.singletonList(server2));

    getVM(rebalanceVM.vmNumber).invoke(() -> {
      // Start a rebalance and wait until bucket creation for redundancy recovery has started before
      // signalling the blackboard
      RebalanceResults results = startRebalanceAndGetResults();

      // Verify that rebalance did some work
      int combinedResults = results.getTotalBucketTransfersCompleted()
          + results.getTotalBucketCreatesCompleted() + results.getTotalPrimaryTransfersCompleted();
      assertThat(combinedResults, greaterThan(0));
    });

    clearInvocation.await();

    // Bring server 2 back online and assign buckets
    server2.invoke(() -> {
      cacheRule.createCache();
      initDataStore(regionType);
      await().untilAsserted(
          () -> Assertions.assertThat(InternalDistributedSystem.getConnectedInstance())
              .isNotNull());
      PartitionRegionHelper.assignBucketsToPartitions(cacheRule.getCache().getRegion(REGION_NAME));
    });

    // Assert that the region is empty
    assertRegionIsEmpty(asList(accessor, server1, server2));
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
          () -> Assertions.assertThat(InternalDistributedSystem.getConnectedInstance()).isNull());
    }
  }

}
