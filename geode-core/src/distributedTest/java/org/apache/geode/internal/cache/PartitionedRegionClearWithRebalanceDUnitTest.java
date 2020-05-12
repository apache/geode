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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DUnitBlackboard;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

@RunWith(JUnitParamsRunner.class)
public class PartitionedRegionClearWithRebalanceDUnitTest implements Serializable {
  private static final long serialVersionUID = -7183993832801073933L;

  private static final Integer BUCKETS = GLOBAL_MAX_BUCKETS_DEFAULT;
  private static final String REGION_NAME = "PartitionedRegion";
  public static final String DISK_STORE_SUFFIX = "DiskStore";
  public static final String BEGIN_CLEAR = "begin-clear";
  private static final int ENTRIES = 10000;
  public static final String COLOCATED_REGION = "childColocatedRegion";

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
  static Object[] vmsAndRegionTypes() {
    ArrayList<Object[]> parameters = new ArrayList<>();

    RegionShortcut[] regionShortcuts = new RegionShortcut[] {
        PARTITION_REDUNDANT,
        PARTITION_REDUNDANT_PERSISTENT
    };

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

  private void initAccessor(RegionShortcut regionShortcut, Collection<String> regionNames) {
    RegionShortcut accessorShortcut = getRegionAccessorShortcut(regionShortcut);
    // StartupRecoveryDelay is set to infinite to prevent automatic rebalancing when creating the
    // region on other members
    regionNames.forEach(regionName -> {
      PartitionAttributesFactory<String, String> attributesFactory =
          new PartitionAttributesFactory<String, String>()
              .setTotalNumBuckets(BUCKETS)
              .setStartupRecoveryDelay(-1)
              .setLocalMaxMemory(0);

      if (regionName.equals(COLOCATED_REGION)) {
        attributesFactory.setColocatedWith(REGION_NAME);
      }

      PartitionAttributes<String, String> attributes = attributesFactory.create();

      cacheRule.getCache()
          .<String, String>createRegionFactory(accessorShortcut)
          .setPartitionAttributes(attributes)
          .create(regionName);
    });
  }

  private void initDataStore(RegionShortcut regionShortcut, Collection<String> regionNames) {
    // StartupRecoveryDelay is set to infinite to prevent automatic rebalancing when creating the
    // region on other members
    regionNames.forEach(regionName -> {
      PartitionAttributesFactory<String, String> attributesFactory =
          new PartitionAttributesFactory<String, String>()
              .setTotalNumBuckets(BUCKETS)
              .setStartupRecoveryDelay(-1);

      if (regionName.equals(COLOCATED_REGION)) {
        attributesFactory.setColocatedWith(REGION_NAME);
      }

      PartitionAttributes<String, String> attributes = attributesFactory.create();

      RegionFactory<String, String> factory = cacheRule.getCache()
          .<String, String>createRegionFactory(regionShortcut)
          .setPartitionAttributes(attributes);

      if (regionShortcut.isPersistent()) {
        factory.setDiskStoreName(
            cacheRule.getCache().createDiskStoreFactory().create(regionName + DISK_STORE_SUFFIX)
                .getName());
      }

      factory.create(regionName);
    });
  }

  private void parametrizedSetup(RegionShortcut regionShortcut, Collection<String> regionNames) {
    // Create and populate the region on server1 first, to create an unbalanced distribution of data
    server1.invoke(() -> {
      initDataStore(regionShortcut, regionNames);
      regionNames.forEach(regionName -> {
        Region<String, String> region = cacheRule.getCache().getRegion(regionName);
        IntStream.range(0, ENTRIES).forEach(i -> region.put("key" + i, "value" + i));
      });
    });
    server2.invoke(() -> initDataStore(regionShortcut, regionNames));
    accessor.invoke(() -> initAccessor(regionShortcut, regionNames));
  }

  private AsyncInvocation<Object> prepareClearAsyncInvocation(
      TestVM clearCoordinatorVM, String regionName) {
    return getVM(clearCoordinatorVM.vmNumber).invokeAsync(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      // Wait for the signal from the blackboard before triggering the clear to start
      getBlackboard().waitForGate(BEGIN_CLEAR, GeodeAwaitility.getTimeout().toMillis(),
          TimeUnit.MILLISECONDS);

      region.clear();
    });
  }

  private AsyncInvocation<Object> setupAndPrepareClear(TestVM clearCoordinatorVM,
      RegionShortcut regionType) {
    parametrizedSetup(regionType, Collections.singleton(REGION_NAME));

    return prepareClearAsyncInvocation(clearCoordinatorVM, REGION_NAME);
  }

  private void doRebalanceAndSignalBlackboard(boolean waitForPrimaryReassignment)
      throws InterruptedException {
    // Start a rebalance and wait until bucket creation for redundancy recovery (the first stage of
    // a rebalance operation) has started before signalling the blackboard
    RebalanceOperation rebalanceOp =
        cacheRule.getCache().getResourceManager().createRebalanceFactory().start();
    if (waitForPrimaryReassignment) {
      await().untilAsserted(() -> assertThat(cacheRule.getCache().getInternalResourceManager()
          .getStats().getRebalancePrimaryTransfersCompleted(), greaterThan(0)));
    } else {
      await().untilAsserted(() -> assertThat(cacheRule.getCache().getInternalResourceManager()
          .getStats().getRebalanceBucketCreatesCompleted(), greaterThan(0)));
    }
    getBlackboard().signalGate(BEGIN_CLEAR);

    rebalanceOp.getResults();
  }

  private void waitForSilenceOnRegion(String regionName) {
    DMStats dmStats = cacheRule.getSystem().getDistributionManager().getStats();
    PartitionedRegion region = (PartitionedRegion) cacheRule.getCache().getRegion(regionName);
    PartitionedRegionStats partitionedRegionStats = region.getPrStats();
    await().untilAsserted(() -> {
      assertThat(dmStats.getReplyWaitsInProgress(), is(0));
      assertThat(partitionedRegionStats.getVolunteeringInProgress(), is(0L));
      assertThat(partitionedRegionStats.getBucketCreatesInProgress(), is(0L));
      assertThat(partitionedRegionStats.getPrimaryTransfersInProgress(), is(0L));
      assertThat(partitionedRegionStats.getRebalanceBucketCreatesInProgress(), is(0L));
      assertThat(partitionedRegionStats.getRebalancePrimaryTransfersInProgress(), is(0L));
    });
  }

  private void assertRegionIsEmpty(List<VM> vms, String regionName) {
    vms.forEach(vm -> vm.invoke(() -> {
      waitForSilenceOnRegion(regionName);
      PartitionedRegion region = (PartitionedRegion) cacheRule.getCache().getRegion(regionName);

      assertThat("Region local size should be 0 for region " + regionName, region.getLocalSize(),
          is(0));
    }));
  }

  private void assertRegionIsNotEmpty(List<VM> vms, String regionName) {
    vms.forEach(vm -> vm.invoke(() -> {
      waitForSilenceOnRegion(regionName);
      PartitionedRegion region = (PartitionedRegion) cacheRule.getCache().getRegion(regionName);

      assertThat("Region size should be " + ENTRIES + " for region " + regionName, region.size(),
          is(ENTRIES));
    }));
  }

  private void assertRebalanceDoesNoWork() {
    server1.invoke(() -> {
      RebalanceResults results =
          cacheRule.getCache().getResourceManager().createRebalanceFactory().start().getResults();

      assertThat("Expected bucket transfers to be zero", results.getTotalBucketTransfersCompleted(),
          is(0));
      assertThat("Expected bucket creates to be zero", results.getTotalBucketCreatesCompleted(),
          is(0));
      assertThat("Expected primary transfers to be zero",
          results.getTotalPrimaryTransfersCompleted(), is(0));
    });
  }

  @Test
  @Parameters(method = "vmsAndRegionTypes")
  @TestCaseName("[{index}] {method}(ClearCoordinator:{0}, RebalanceCoordinator:{1}, RegionType:{2})")
  public void clearRegionDuringRebalanceClearsRegion(TestVM clearCoordinatorVM,
      TestVM rebalanceVM, RegionShortcut regionType) throws InterruptedException {
    AsyncInvocation<?> clearInvocation = setupAndPrepareClear(clearCoordinatorVM, regionType);

    getVM(rebalanceVM.vmNumber)
        .invoke((SerializableRunnableIF) () -> doRebalanceAndSignalBlackboard(false));

    clearInvocation.await();

    // Assert that the region is empty
    assertRegionIsEmpty(asList(accessor, server1, server2), REGION_NAME);

    // Assert that the region was successfully rebalanced (a second rebalance should do no work)
    assertRebalanceDoesNoWork();
  }

  @Test
  @Parameters(method = "vmsAndRegionTypes")
  @TestCaseName("[{index}] {method}(ClearCoordinator:{0}, RebalanceCoordinator:{1}, RegionType:{2})")
  public void clearRegionDuringRebalancePrimaryReassignmentClearsRegion(TestVM clearCoordinatorVM,
      TestVM rebalanceVM, RegionShortcut regionType) throws InterruptedException {
    AsyncInvocation<?> clearInvocation = setupAndPrepareClear(clearCoordinatorVM, regionType);

    getVM(rebalanceVM.vmNumber)
        .invoke((SerializableRunnableIF) () -> doRebalanceAndSignalBlackboard(true));

    clearInvocation.await();

    // Assert that the region is empty
    assertRegionIsEmpty(asList(accessor, server1, server2), REGION_NAME);

    // Assert that the region was successfully rebalanced (a second rebalance should do no work)
    assertRebalanceDoesNoWork();
  }

  @Test
  @Parameters(method = "vmsAndRegionTypes")
  @TestCaseName("[{index}] {method}(ClearCoordinator:{0}, RebalanceCoordinator:{1}, RegionType:{2})")
  public void clearParentColocatedRegionDuringRebalanceOfColocatedRegionsClearsRegionAndDoesNotInterfereWithRebalance(
      TestVM clearCoordinatorVM, TestVM rebalanceVM, RegionShortcut regionType)
      throws InterruptedException {
    parametrizedSetup(regionType, asList(REGION_NAME, COLOCATED_REGION));

    AsyncInvocation<?> clearInvocation = prepareClearAsyncInvocation(clearCoordinatorVM,
        REGION_NAME);

    getVM(rebalanceVM.vmNumber)
        .invoke((SerializableRunnableIF) () -> doRebalanceAndSignalBlackboard(true));

    clearInvocation.await();

    // Assert that the parent region is empty
    assertRegionIsEmpty(asList(accessor, server1, server2), REGION_NAME);

    // Assert that the colocated region is the correct size
    assertRegionIsNotEmpty(asList(accessor, server1, server2), COLOCATED_REGION);

    // Assert that the regions were successfully rebalanced (a second rebalance should do no work)
    assertRebalanceDoesNoWork();
  }

  @Test
  @Parameters(method = "vmsAndRegionTypes")
  @TestCaseName("[{index}] {method}(ClearCoordinator:{0}, RebalanceCoordinator:{1}, RegionType:{2})")
  public void clearChildColocatedRegionDuringRebalanceOfColocatedRegionsClearsRegionAndDoesNotInterfereWithRebalance(
      TestVM clearCoordinatorVM, TestVM rebalanceVM, RegionShortcut regionType)
      throws InterruptedException {
    parametrizedSetup(regionType, asList(REGION_NAME, COLOCATED_REGION));

    AsyncInvocation<?> clearInvocation =
        prepareClearAsyncInvocation(clearCoordinatorVM, COLOCATED_REGION);

    getVM(rebalanceVM.vmNumber)
        .invoke((SerializableRunnableIF) () -> doRebalanceAndSignalBlackboard(true));

    clearInvocation.await();

    // Assert that the colocated region is empty
    assertRegionIsEmpty(asList(accessor, server1, server2), COLOCATED_REGION);

    // Assert that the parent region is the correct size
    assertRegionIsNotEmpty(asList(accessor, server1, server2), REGION_NAME);

    // Assert that the regions were successfully rebalanced (a second rebalance should do no work)
    assertRebalanceDoesNoWork();
  }
}
