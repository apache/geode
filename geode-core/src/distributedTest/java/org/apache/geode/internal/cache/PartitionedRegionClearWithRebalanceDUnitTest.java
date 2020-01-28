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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
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
  private static final String REGION_NAME = "testRegion";
  private static final String COLOCATED_REGION = "childColocatedRegion";
  private static final int ENTRIES = 10000;
  private static final String DISK_STORE_SUFFIX = "DiskStore";
  private static final String REBALANCE_HAS_BEGUN = "rebalance-begun";
  private static final String CLEAR_HAS_BEGUN = "clear-begun";

  @Rule
  public DistributedRule distributedRule = new DistributedRule(4);

  @Rule
  public CacheRule cacheRule = CacheRule.builder().createCacheInAll().build();

  @Rule
  public DistributedDiskDirRule distributedDiskDirRule = new DistributedDiskDirRule();

  private static transient DUnitBlackboard blackboard;

  private VM accessor;
  private VM server1;
  private VM server2;
  private VM server3;

  private enum TestVM {
    ACCESSOR(0), SERVER1(1), SERVER2(2), SERVER3(3);

    final int vmNumber;

    TestVM(int vmNumber) {
      this.vmNumber = vmNumber;
    }
  }

  @SuppressWarnings("unused")
  static Object[] coordinatorVMsAndRegionTypes() {
    return new Object[] {
        // {ClearCoordinatorVM, regionShortcut}
        new Object[] {TestVM.SERVER1, PARTITION_REDUNDANT},
        new Object[] {TestVM.ACCESSOR, PARTITION_REDUNDANT},
        new Object[] {TestVM.SERVER1, PARTITION_REDUNDANT_PERSISTENT},
        new Object[] {TestVM.ACCESSOR, PARTITION_REDUNDANT_PERSISTENT}
    };
  }

  @SuppressWarnings("unused")
  static Object[] coordinatorVMsAndRegionTypesNoAccessor() {
    return new Object[] {
        // {ClearCoordinatorVM, regionShortcut}
        new Object[] {TestVM.SERVER1, PARTITION_REDUNDANT},
        new Object[] {TestVM.SERVER2, PARTITION_REDUNDANT},
        new Object[] {TestVM.SERVER1, PARTITION_REDUNDANT_PERSISTENT},
        new Object[] {TestVM.SERVER2, PARTITION_REDUNDANT_PERSISTENT}
    };
  }

  @Before
  public void setUp() throws Exception {
    getBlackboard().initBlackboard();
    server1 = getVM(TestVM.SERVER1.vmNumber);
    server2 = getVM(TestVM.SERVER2.vmNumber);
    server3 = getVM(TestVM.SERVER3.vmNumber);
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
        default:
          throw new IllegalArgumentException(
              "Invalid RegionShortcut specified: " + dataStoreRegionShortcut);
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

      cacheRule.getCache()
          .<String, String>createRegionFactory(accessorShortcut)
          .setPartitionAttributes(attributesFactory.create())
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

      RegionFactory<String, String> factory = cacheRule.getCache()
          .<String, String>createRegionFactory(regionShortcut)
          .setPartitionAttributes(attributesFactory.create())
          .setCacheWriter(new BlackboardSignaller());

      // Set up the disk store if the region is persistent
      if (regionShortcut.isPersistent()) {
        factory.setDiskStoreName(cacheRule.getCache()
            .createDiskStoreFactory()
            .create(regionName + DISK_STORE_SUFFIX)
            .getName());
      }

      factory.create(regionName);
    });
  }

  private void parametrizedSetup(RegionShortcut regionShortcut, Collection<String> regionNames,
      boolean useAccessor) {
    // Create and populate the region on server1 first, to create an unbalanced distribution of data
    server1.invoke(() -> {
      initDataStore(regionShortcut, regionNames);
      regionNames.forEach(regionName -> {
        Region<String, String> region = cacheRule.getCache().getRegion(regionName);
        IntStream.range(0, ENTRIES).forEach(i -> region.put("key" + i, "value" + i));
      });
    });
    server2.invoke(() -> initDataStore(regionShortcut, regionNames));
    if (useAccessor) {
      accessor.invoke(() -> initAccessor(regionShortcut, regionNames));
    } else {
      server3.invoke(() -> initDataStore(regionShortcut, regionNames));
    }
  }

  private void setBlackboardSignallerCacheWriter(String regionName) {
    cacheRule.getCache().<String, String>getRegion(regionName).getAttributesMutator()
        .setCacheWriter(new BlackboardSignaller());
  }

  private AsyncInvocation<?> startClearAsync(TestVM clearCoordinatorVM, String regionName,
      boolean waitForRebalance) {
    return getVM(clearCoordinatorVM.vmNumber).invokeAsync(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      if (waitForRebalance) {
        // Wait for the signal from the blackboard before triggering the clear to start
        getBlackboard().waitForGate(REBALANCE_HAS_BEGUN, GeodeAwaitility.getTimeout().toMillis(),
            TimeUnit.MILLISECONDS);
      }
      region.clear();
    });
  }

  // Trigger a rebalance and wait until it has started restoring redundancy before signalling the
  // blackboard
  private AsyncInvocation<?> startRebalanceAsyncAndSignalBlackboard(boolean waitForClear) {
    return server1.invokeAsync(() -> {
      RebalanceFactory rebalance =
          cacheRule.getCache().getResourceManager().createRebalanceFactory();
      if (waitForClear) {
        // Wait for the signal from the blackboard before triggering the rebalance to start
        getBlackboard().waitForGate(CLEAR_HAS_BEGUN, GeodeAwaitility.getTimeout().toMillis(),
            TimeUnit.MILLISECONDS);
      }
      RebalanceOperation op = rebalance.start();
      await().untilAsserted(() -> assertThat(cacheRule.getCache().getInternalResourceManager()
          .getStats().getRebalanceBucketCreatesCompleted()).isGreaterThan(0));
      getBlackboard().signalGate(REBALANCE_HAS_BEGUN);
      op.getResults();
    });
  }

  private void executeClearAndRebalanceAsyncInvocations(TestVM clearCoordinatorVM,
      String regionToClear, boolean rebalanceFirst) throws InterruptedException {
    getVM(clearCoordinatorVM.vmNumber)
        .invoke(() -> setBlackboardSignallerCacheWriter(regionToClear));

    AsyncInvocation<?> clearInvocation = startClearAsync(clearCoordinatorVM, regionToClear,
        rebalanceFirst);

    AsyncInvocation<?> rebalanceInvocation =
        startRebalanceAsyncAndSignalBlackboard(!rebalanceFirst);

    clearInvocation.await();
    rebalanceInvocation.await();
  }

  private void prepareMemberToShutdownOnClear() throws TimeoutException, InterruptedException {
    getBlackboard().waitForGate(CLEAR_HAS_BEGUN, GeodeAwaitility.getTimeout().toMillis(),
        TimeUnit.MILLISECONDS);
    InternalDistributedSystem.getConnectedInstance().stopReconnectingNoDisconnect();
    MembershipManagerHelper.crashDistributedSystem(
        InternalDistributedSystem.getConnectedInstance());
    await().untilAsserted(
        () -> assertThat(InternalDistributedSystem.getConnectedInstance()).isNull());
  }

  private void waitForSilenceOnRegion(String regionName) {
    DMStats dmStats = cacheRule.getSystem().getDistributionManager().getStats();
    PartitionedRegion region = (PartitionedRegion) cacheRule.getCache().getRegion(regionName);
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

  private void assertRegionIsEmpty(List<VM> vms, String regionName) {
    vms.forEach(vm -> vm.invoke(() -> {
      waitForSilenceOnRegion(regionName);
      PartitionedRegion region = (PartitionedRegion) cacheRule.getCache().getRegion(regionName);

      assertThat(region.getLocalSize()).as("Region local size should be 0 for region " + regionName)
          .isEqualTo(0);
    }));
  }

  private void assertRegionIsNotEmpty(List<VM> vms, String regionName) {
    vms.forEach(vm -> vm.invoke(() -> {
      waitForSilenceOnRegion(regionName);
      PartitionedRegion region = (PartitionedRegion) cacheRule.getCache().getRegion(regionName);

      assertThat(region.size()).as("Region size should be " + ENTRIES + " for region " + regionName)
          .isEqualTo(ENTRIES);
    }));
  }

  private void assertRebalanceDoesNoWork() {
    server1.invoke(() -> {
      RebalanceResults results =
          cacheRule.getCache().getResourceManager().createRebalanceFactory().start().getResults();

      assertThat(results.getTotalBucketTransfersCompleted())
          .as("Expected bucket transfers to be zero").isEqualTo(0);
      assertThat(results.getTotalBucketCreatesCompleted()).as("Expected bucket creates to be zero")
          .isEqualTo(0);
      assertThat(results.getTotalPrimaryTransfersCompleted())
          .as("Expected primary transfers to be zero").isEqualTo(0);
    });
  }

  @Test
  @Parameters(method = "coordinatorVMsAndRegionTypes")
  @TestCaseName("[{index}] {method}(ClearCoordinator:{0}, RegionType:{1})")
  public void clearRegionStartedAfterRebalanceClearsRegion(TestVM clearCoordinatorVM,
      RegionShortcut regionType) throws InterruptedException {
    parametrizedSetup(regionType, Collections.singleton(REGION_NAME), true);

    executeClearAndRebalanceAsyncInvocations(clearCoordinatorVM, REGION_NAME, true);

    // Assert that the region is empty
    assertRegionIsEmpty(asList(accessor, server1, server2), REGION_NAME);

    // Assert that the region was successfully rebalanced (a second rebalance should do no work)
    assertRebalanceDoesNoWork();
  }

  @Test
  @Parameters(method = "coordinatorVMsAndRegionTypes")
  @TestCaseName("[{index}] {method}(ClearCoordinator:{0}, RegionType:{1})")
  public void clearRegionStartedBeforeRebalanceClearsRegion(TestVM clearCoordinatorVM,
      RegionShortcut regionType) throws InterruptedException {
    parametrizedSetup(regionType, Collections.singleton(REGION_NAME), true);

    executeClearAndRebalanceAsyncInvocations(clearCoordinatorVM, REGION_NAME, false);

    // Assert that the region is empty
    assertRegionIsEmpty(asList(accessor, server1, server2), REGION_NAME);

    // Assert that the region was successfully rebalanced (a second rebalance should do no work)
    assertRebalanceDoesNoWork();
  }

  @Test
  @Parameters(method = "coordinatorVMsAndRegionTypes")
  @TestCaseName("[{index}] {method}(ClearCoordinator:{0}, RegionType:{1})")
  public void clearParentColocatedRegionStartedAfterRebalanceOfColocatedRegionsClearsRegionAndDoesNotInterfereWithRebalance(
      TestVM clearCoordinatorVM, RegionShortcut regionType)
      throws InterruptedException {
    parametrizedSetup(regionType, asList(REGION_NAME, COLOCATED_REGION), true);

    executeClearAndRebalanceAsyncInvocations(clearCoordinatorVM, REGION_NAME, true);

    // Assert that the parent region is empty
    assertRegionIsEmpty(asList(accessor, server1, server2), REGION_NAME);

    // Assert that the colocated region is the correct size
    assertRegionIsNotEmpty(asList(accessor, server1, server2), COLOCATED_REGION);

    // Assert that the regions were successfully rebalanced (a second rebalance should do no work)
    assertRebalanceDoesNoWork();
  }

  @Test
  @Parameters(method = "coordinatorVMsAndRegionTypes")
  @TestCaseName("[{index}] {method}(ClearCoordinator:{0}, RegionType:{1})")
  public void clearParentColocatedRegionStartedBeforeRebalanceOfColocatedRegionsClearsRegionAndDoesNotInterfereWithRebalance(
      TestVM clearCoordinatorVM, RegionShortcut regionType)
      throws InterruptedException {
    parametrizedSetup(regionType, asList(REGION_NAME, COLOCATED_REGION), true);

    executeClearAndRebalanceAsyncInvocations(clearCoordinatorVM, REGION_NAME, false);

    // Assert that the parent region is empty
    assertRegionIsEmpty(asList(accessor, server1, server2), REGION_NAME);

    // Assert that the colocated region is the correct size
    assertRegionIsNotEmpty(asList(accessor, server1, server2), COLOCATED_REGION);

    // Assert that the regions were successfully rebalanced (a second rebalance should do no work)
    assertRebalanceDoesNoWork();
  }

  @Test
  @Parameters(method = "coordinatorVMsAndRegionTypes")
  @TestCaseName("[{index}] {method}(ClearCoordinator:{0}, RegionType:{1})")
  public void clearChildColocatedRegionStartedAfterRebalanceOfColocatedRegionsClearsRegionAndDoesNotInterfereWithRebalance(
      TestVM clearCoordinatorVM, RegionShortcut regionType)
      throws InterruptedException {
    parametrizedSetup(regionType, asList(REGION_NAME, COLOCATED_REGION), true);

    executeClearAndRebalanceAsyncInvocations(clearCoordinatorVM, COLOCATED_REGION, true);

    // Assert that the colocated region is empty
    assertRegionIsEmpty(asList(accessor, server1, server2), COLOCATED_REGION);

    // Assert that the parent region is the correct size
    assertRegionIsNotEmpty(asList(accessor, server1, server2), REGION_NAME);

    // Assert that the regions were successfully rebalanced (a second rebalance should do no work)
    assertRebalanceDoesNoWork();
  }

  @Test
  @Parameters(method = "coordinatorVMsAndRegionTypes")
  @TestCaseName("[{index}] {method}(ClearCoordinator:{0}, RegionType:{1})")
  public void clearChildColocatedRegionStartedBeforeRebalanceOfColocatedRegionsClearsRegionAndDoesNotInterfereWithRebalance(
      TestVM clearCoordinatorVM, RegionShortcut regionType)
      throws InterruptedException {
    parametrizedSetup(regionType, asList(REGION_NAME, COLOCATED_REGION), true);

    executeClearAndRebalanceAsyncInvocations(clearCoordinatorVM, COLOCATED_REGION, false);

    // Assert that the colocated region is empty
    assertRegionIsEmpty(asList(accessor, server1, server2), COLOCATED_REGION);

    // Assert that the parent region is the correct size
    assertRegionIsNotEmpty(asList(accessor, server1, server2), REGION_NAME);

    // Assert that the regions were successfully rebalanced (a second rebalance should do no work)
    assertRebalanceDoesNoWork();
  }

  @Test
  @Parameters(method = "coordinatorVMsAndRegionTypesNoAccessor")
  @TestCaseName("[{index}] {method}(ClearCoordinator:{0}, RegionType:{1})")
  public void clearStartedBeforeRebalanceClearsRegionWhenNonCoordinatorMemberIsKilled(
      TestVM clearCoordinatorVM, RegionShortcut regionType)
      throws InterruptedException {
    parametrizedSetup(regionType, Collections.singleton(REGION_NAME), false);

    getVM(clearCoordinatorVM.vmNumber).invoke(() -> setBlackboardSignallerCacheWriter(REGION_NAME));

    // Make server3 shut down when it receives the signal from the blackboard that clear has started
    AsyncInvocation<?> shutdownInvocation =
        server3.invokeAsync(this::prepareMemberToShutdownOnClear);

    executeClearAndRebalanceAsyncInvocations(clearCoordinatorVM, REGION_NAME, false);

    shutdownInvocation.await();

    // Assert that the region is empty
    assertRegionIsEmpty(asList(server1, server2), REGION_NAME);
  }

  @Test
  @Parameters(method = "coordinatorVMsAndRegionTypesNoAccessor")
  @TestCaseName("[{index}] {method}(ClearCoordinator:{0}, RegionType:{1})")
  public void clearStartedAfterRebalanceClearsRegionWhenNewMemberJoins(TestVM clearCoordinatorVM,
      RegionShortcut regionType) throws InterruptedException {

    // Load the data on server1 before creating the region on other servers, to create an imbalanced
    // system
    server1.invoke(() -> {
      initDataStore(regionType, Collections.singleton(REGION_NAME));
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
      IntStream.range(0, ENTRIES).forEach(i -> region.put("key" + i, "value" + i));
    });
    server2.invoke(() -> initDataStore(regionType, Collections.singleton(REGION_NAME)));

    // Wait for rebalance to start, then create the region on server3
    AsyncInvocation<?> createRegion = server3.invokeAsync(() -> {
      cacheRule.createCache();

      PartitionAttributesFactory<String, String> attributesFactory =
          new PartitionAttributesFactory<String, String>()
              .setTotalNumBuckets(BUCKETS)
              .setStartupRecoveryDelay(-1);

      RegionFactory<String, String> factory = cacheRule.getCache()
          .<String, String>createRegionFactory(regionType)
          .setPartitionAttributes(attributesFactory.create())
          .setCacheWriter(new BlackboardSignaller());

      if (regionType.isPersistent()) {
        factory.setDiskStoreName(cacheRule.getCache()
            .createDiskStoreFactory()
            .create(REGION_NAME + DISK_STORE_SUFFIX)
            .getName());
      }

      getBlackboard().waitForGate(REBALANCE_HAS_BEGUN, GeodeAwaitility.getTimeout().toMillis(),
          TimeUnit.MILLISECONDS);

      factory.create(REGION_NAME);
    });

    executeClearAndRebalanceAsyncInvocations(clearCoordinatorVM, REGION_NAME, true);

    createRegion.await();

    // Assert that the region is empty
    assertRegionIsEmpty(asList(server1, server2, server3), REGION_NAME);
  }


  @Test
  @Parameters(method = "coordinatorVMsAndRegionTypesNoAccessor")
  @TestCaseName("[{index}] {method}(ClearCoordinator:{0}, RegionType:{1})")
  public void clearStartedBeforeRebalanceClearsRegionWhenNewMemberJoins(TestVM clearCoordinatorVM,
      RegionShortcut regionType) throws InterruptedException {

    // Load the data on server1 before creating the region on other servers, to create an imbalanced
    // system
    server1.invoke(() -> {
      initDataStore(regionType, Collections.singleton(REGION_NAME));
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
      IntStream.range(0, ENTRIES).forEach(i -> region.put("key" + i, "value" + i));
    });

    server2.invoke(() -> initDataStore(regionType, Collections.singleton(REGION_NAME)));

    // Wait for clear to start, then create the region on server3
    AsyncInvocation<?> createRegion = server3.invokeAsync(() -> {
      cacheRule.createCache();

      PartitionAttributesFactory<String, String> attributesFactory =
          new PartitionAttributesFactory<String, String>()
              .setTotalNumBuckets(BUCKETS)
              .setStartupRecoveryDelay(-1);

      RegionFactory<String, String> factory = cacheRule.getCache()
          .<String, String>createRegionFactory(regionType)
          .setPartitionAttributes(attributesFactory.create())
          .setCacheWriter(new BlackboardSignaller());

      if (regionType.isPersistent()) {
        factory.setDiskStoreName(cacheRule.getCache()
            .createDiskStoreFactory()
            .create(REGION_NAME + DISK_STORE_SUFFIX)
            .getName());
      }

      getBlackboard().waitForGate(CLEAR_HAS_BEGUN, GeodeAwaitility.getTimeout().toMillis(),
          TimeUnit.MILLISECONDS);

      factory.create(REGION_NAME);
    });

    executeClearAndRebalanceAsyncInvocations(clearCoordinatorVM, REGION_NAME, false);

    createRegion.await();

    // Assert that the region is empty
    assertRegionIsEmpty(asList(server1, server2, server3), REGION_NAME);
  }

  public static class BlackboardSignaller extends CacheWriterAdapter<String, String> {
    @Override
    public synchronized void beforeRegionClear(RegionEvent<String, String> event)
        throws CacheWriterException {
      getBlackboard().signalGate(CLEAR_HAS_BEGUN);
    }
  }
}
