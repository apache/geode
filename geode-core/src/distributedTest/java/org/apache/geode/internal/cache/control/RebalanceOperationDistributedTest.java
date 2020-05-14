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
package org.apache.geode.internal.cache.control;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.cache.EvictionAction.OVERFLOW_TO_DISK;
import static org.apache.geode.cache.EvictionAttributes.createLRUEntryAttributes;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.distributed.ConfigurationProperties.ENFORCE_UNIQUE_HOST;
import static org.apache.geode.distributed.ConfigurationProperties.REDUNDANCY_ZONE;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.partition.PartitionMemberInfo;
import org.apache.geode.cache.partition.PartitionRebalanceInfo;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.partition.PartitionRegionInfo;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.CacheClosingDistributionMessageObserver;
import org.apache.geode.internal.cache.ColocationHelper;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PRHARedundancyProvider;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.SignalBounceOnRequestImageMessageObserver;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import org.apache.geode.internal.cache.partitioned.BucketCountLoadProbe;
import org.apache.geode.internal.cache.partitioned.LoadProbe;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * TODO test colocated regions where buckets aren't created for all subregions
 *
 * <p>
 * TODO test colocated regions where members aren't consistent in which regions they have
 */
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class RebalanceOperationDistributedTest extends CacheTestCase {

  private static final long TIMEOUT_SECONDS = GeodeAwaitility.getTimeout().getSeconds();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @After
  public void tearDown() {
    invokeInEveryVM(() -> {
      InternalResourceManager.setResourceObserver(null);
      DistributionMessageObserver.setInstance(null);
    });
  }

  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}(simulate={0})")
  public void testRecoverRedundancy(boolean simulate) {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    // Create the region in only 1 VM
    vm0.invoke(() -> createPartitionedRegion("region1", 1));

    // Create some buckets
    vm0.invoke(() -> {
      Region<Number, String> region = getCache().getRegion("region1");
      region.put(1, "A");
      region.put(2, "A");
      region.put(3, "A");
      region.put(4, "A");
      region.put(5, "A");
      region.put(6, "A");
    });

    // make sure we can tell that the buckets have low redundancy
    vm0.invoke(() -> validateRedundancy("region1", 6, 0, 6));

    // Create the region in the other VM (should have no effect)
    vm1.invoke(() -> createPartitionedRegion("region1", 1));

    // Make sure we still have low redundancy
    vm0.invoke(() -> validateRedundancy("region1", 6, 0, 6));

    // Now simulate a rebalance
    vm0.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();
      RebalanceResults results = doRebalance(simulate, manager);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(6);
      assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(3);
      assertThat(results.getTotalBucketTransferBytes()).isEqualTo(0);
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(0);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(1);

      PartitionRebalanceInfo details = detailSet.iterator().next();
      assertThat(details.getBucketCreatesCompleted()).isEqualTo(6);
      assertThat(details.getPrimaryTransfersCompleted()).isEqualTo(3);
      assertThat(details.getBucketTransferBytes()).isEqualTo(0);
      assertThat(details.getBucketTransfersCompleted()).isEqualTo(0);

      Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
      assertThat(afterDetails).hasSize(2);

      for (PartitionMemberInfo memberDetails : afterDetails) {
        assertThat(memberDetails.getBucketCount()).isEqualTo(6);
        assertThat(memberDetails.getPrimaryCount()).isEqualTo(3);
      }

      if (!simulate) {
        validateStatistics(manager, results);
      }
    });

    if (simulate) {
      // Make sure the simulation didn't do anything
      vm0.invoke(() -> validateRedundancy("region1", 6, 0, 6));
    } else {
      vm0.invoke(() -> validateRedundancy("region1", 6, 1, 0));
      vm1.invoke(() -> validateRedundancy("region1", 6, 1, 0));
    }
  }

  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}(simulate={0})")
  public void testEnforceIP(boolean simulate) {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        Properties props = new Properties();
        props.setProperty(ENFORCE_UNIQUE_HOST, "true");
        getCache(props);
      });
    }

    // Create the region in only 1 VM
    vm0.invoke(() -> createPartitionedRegion("region1", 1));

    // Create some buckets
    vm0.invoke(() -> {
      Region<Number, String> region = getCache().getRegion("region1");
      region.put(1, "A");
      region.put(2, "A");
      region.put(3, "A");
      region.put(4, "A");
      region.put(5, "A");
      region.put(6, "A");
    });

    // make sure we can tell that the buckets have low redundancy
    vm0.invoke(() -> validateRedundancy("region1", 6, 0, 6));

    // Create the region in the other VM (should have no effect)
    vm1.invoke(() -> createPartitionedRegion("region1", 1));

    // Make sure we still have low redundancy
    vm0.invoke(() -> validateRedundancy("region1", 6, 0, 6));

    // Now simulate a rebalance
    vm0.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();
      RebalanceResults results = doRebalance(simulate, manager);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(0);
      assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(0);

      // We actually *will* transfer buckets, because that improves the balance
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(3);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(1);

      PartitionRebalanceInfo details = detailSet.iterator().next();
      assertThat(details.getBucketCreatesCompleted()).isEqualTo(0);
      assertThat(details.getPrimaryTransfersCompleted()).isEqualTo(0);
      assertThat(details.getBucketTransfersCompleted()).isEqualTo(3);

      if (!simulate) {
        validateStatistics(manager, results);
      }
    });

    // Make sure we still have low redundancy
    vm0.invoke(() -> validateRedundancy("region1", 6, 0, 6));
    vm1.invoke(() -> validateRedundancy("region1", 6, 0, 6));
  }

  /**
   * Test that we correctly use the redundancy-zone property to determine where to place redundant
   * copies of a buckets.
   */
  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}(simulate={0})")
  public void testEnforceZone(boolean simulate) {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    vm0.invoke(() -> setRedundancyZone("A"));
    vm1.invoke(() -> setRedundancyZone("A"));

    DistributedMember zoneBMember = vm2.invoke(() -> {
      setRedundancyZone("B");
      return getCache().getDistributedSystem().getDistributedMember();
    });

    // Create the region in only 1 VM
    vm0.invoke(() -> createPartitionedRegion("region1", 1));

    // Create some buckets
    vm0.invoke(() -> {
      Region<Number, String> region = getCache().getRegion("region1");
      region.put(1, "A");
      region.put(2, "A");
      region.put(3, "A");
      region.put(4, "A");
      region.put(5, "A");
      region.put(6, "A");
    });

    // make sure we can tell that the buckets have low redundancy
    vm0.invoke(() -> validateRedundancy("region1", 6, 0, 6));

    // Create the region in the other VMs (should have no effect)
    vm1.invoke(() -> createPartitionedRegion("region1", 1));
    vm2.invoke(() -> createPartitionedRegion("region1", 1));

    // Make sure we still have low redundancy
    vm0.invoke(() -> validateRedundancy("region1", 6, 0, 6));

    // Now simulate a rebalance
    vm0.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();
      RebalanceResults results = doRebalance(simulate, manager);

      // We expect to satisfy redundancy with the zone B member
      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(6);

      // 2 primaries will go to vm2, leaving vm0 and vm1 with 2 primaries each
      assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(2);

      // We actually *will* transfer 3 buckets to the other member in zone A, because that
      // improves
      // the balance
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(3);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(1);

      PartitionRebalanceInfo details = detailSet.iterator().next();
      assertThat(details.getBucketCreatesCompleted()).isEqualTo(6);
      assertThat(details.getPrimaryTransfersCompleted()).isEqualTo(2);
      assertThat(details.getBucketTransfersCompleted()).isEqualTo(3);

      Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
      for (PartitionMemberInfo info : afterDetails) {
        if (info.getDistributedMember().equals(zoneBMember)) {
          assertThat(info.getBucketCount()).isEqualTo(6);
        } else {
          assertThat(info.getBucketCount()).isEqualTo(3);
        }
        assertThat(info.getPrimaryCount()).isEqualTo(2);
      }

      // assertIndexDetailsEquals(0, details.getBucketTransferBytes());
      if (!simulate) {
        validateStatistics(manager, results);
      }
    });

    if (!simulate) {
      vm0.invoke(() -> validateBucketCount("region1", 3));
      vm1.invoke(() -> validateBucketCount("region1", 3));
      vm2.invoke(() -> validateBucketCount("region1", 6));
    }
  }

  @Test
  public void testEnforceZoneWithMultipleRegions() {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    vm0.invoke(() -> setRedundancyZone("A"));
    vm1.invoke(() -> setRedundancyZone("A"));

    DistributedMember zoneBMember = vm2.invoke(() -> {
      setRedundancyZone("B");
      return getCache().getDistributedSystem().getDistributedMember();
    });

    vm0.invoke(() -> InternalResourceManager.setResourceObserver(new ParallelRecoveryObserver(2)));

    // Create the region in only 1 VM
    vm0.invoke(() -> createTwoRegionsWithParallelRecoveryObserver());

    // Create some buckets
    vm0.invoke(() -> {
      doPuts("region1");
      doPuts("region2");
    });

    // make sure we can tell that the buckets have low redundancy
    vm0.invoke(() -> validateRedundancyOfTwoRegions("region1", "region2", 6, 0, 6));

    // Create the region in the other VMs (should have no effect)
    for (VM vm : toArray(vm1, vm2)) {
      vm.invoke(() -> {
        InternalResourceManager.setResourceObserver(new ParallelRecoveryObserver(2));
        createTwoRegionsWithParallelRecoveryObserver();
      });
    }

    // Make sure we still have low redundancy
    vm0.invoke(() -> validateRedundancyOfTwoRegions("region1", "region2", 6, 0, 6));

    // Now do a rebalance
    vm0.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();
      RebalanceResults results = doRebalance(false, manager);

      // We expect to satisfy redundancy with the zone B member
      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(12);

      // 2 primaries will go to vm2, leaving vm0 and vm1 with 2 primaries each
      assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(4);

      // We actually *will* transfer 3 buckets to the other member in zone A, because that
      // improves the balance
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(6);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(2);

      for (PartitionRebalanceInfo details : detailSet) {
        assertThat(details.getBucketCreatesCompleted()).isEqualTo(6);
        assertThat(details.getPrimaryTransfersCompleted()).isEqualTo(2);
        assertThat(details.getBucketTransfersCompleted()).isEqualTo(3);

        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        for (PartitionMemberInfo info : afterDetails) {
          if (info.getDistributedMember().equals(zoneBMember)) {
            assertThat(info.getBucketCount()).isEqualTo(6);
          } else {
            assertThat(info.getBucketCount()).isEqualTo(3);
          }
          assertThat(info.getPrimaryCount()).isEqualTo(2);
        }
      }

      validateStatistics(manager, results);
    });

    vm0.invoke(() -> {
      assertThat(((ParallelRecoveryObserver) InternalResourceManager.getResourceObserver())
          .isObserverCalled()).isTrue();
    });

    vm0.invoke(() -> validateBucketCount("region1", 3));
    vm1.invoke(() -> validateBucketCount("region1", 3));
    vm2.invoke(() -> validateBucketCount("region1", 6));

    vm0.invoke(() -> validateBucketCount("region2", 3));
    vm1.invoke(() -> validateBucketCount("region2", 3));
    vm2.invoke(() -> validateBucketCount("region2", 6));
  }

  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}(simulate={0})")
  public void testRecoverRedundancyBalancing(boolean simulate) {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    DistributedMember member1 = vm0.invoke(() -> {
      createPartitionedRegion("region1", 1, 200);
      return getCache().getDistributedSystem().getDistributedMember();
    });

    vm0.invoke(() -> {
      Region<Number, String> region = getCache().getRegion("region1");
      for (int i = 0; i < 12; i++) {
        region.put(i, "A");
      }
    });

    vm0.invoke(() -> validateRedundancy("region1", 12, 0, 12));

    // Now create the region in 2 more VMs with half the localMaxMemory
    vm1.invoke(() -> createPartitionedRegion("region1", 1, 100));
    vm2.invoke(() -> createPartitionedRegion("region1", 1, 100));

    vm0.invoke(() -> validateRedundancy("region1", 12, 0, 12));

    // Now simulate a rebalance
    vm0.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();
      RebalanceResults results = doRebalance(simulate, manager);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(12);
      assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(6);
      assertThat(results.getTotalBucketTransferBytes()).isEqualTo(0);
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(0);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(1);

      PartitionRebalanceInfo details = detailSet.iterator().next();
      assertThat(details.getBucketCreatesCompleted()).isEqualTo(12);
      assertThat(details.getPrimaryTransfersCompleted()).isEqualTo(6);
      assertThat(details.getBucketTransferBytes()).isEqualTo(0);
      assertThat(details.getBucketTransfersCompleted()).isEqualTo(0);

      Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
      assertThat(afterDetails).hasSize(3);

      for (PartitionMemberInfo memberDetails : afterDetails) {
        // We have 1 member with a size of 200 and two members with size 100
        if (memberDetails.getDistributedMember().equals(member1)) {
          assertThat(memberDetails.getBucketCount()).isEqualTo(12);
          assertThat(memberDetails.getPrimaryCount()).isEqualTo(6);
        } else {
          assertThat(memberDetails.getBucketCount()).isEqualTo(6);
          assertThat(memberDetails.getPrimaryCount()).isEqualTo(3);
        }
      }

      if (!simulate) {
        validateStatistics(manager, results);
      }
    });

    if (!simulate) {
      for (VM vm : toArray(vm0, vm1, vm2)) {
        vm.invoke(() -> validateRedundancy("region1", 12, 1, 0));
      }
    }
  }

  @Test
  public void testRecoverRedundancyBalancingIfCreateBucketFails() {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    DistributedMember member1 = vm0.invoke(() -> {
      createPartitionedRegion("region1", 1, 100);
      return getCache().getDistributedSystem().getDistributedMember();
    });

    vm0.invoke(() -> {
      Region<Number, String> region = getCache().getRegion("region1");
      for (int i = 0; i < 1; i++) {
        region.put(i, "A");
      }
    });

    vm0.invoke(() -> validateRedundancy("region1", 1, 0, 1));

    // Now create the region in 2 more VMs
    // Let localMaxMemory(VM1) > localMaxMemory(VM2)
    // so that redundant bucket will always be attempted on VM1
    DistributedMember member2 = vm1.invoke(() -> {
      createPartitionedRegion("region1", 1, 100);
      return getCache().getDistributedSystem().getDistributedMember();
    });
    DistributedMember member3 = vm2.invoke(() -> {
      createPartitionedRegion("region1", 1, 90);
      return getCache().getDistributedSystem().getDistributedMember();
    });

    vm0.invoke(() -> validateRedundancy("region1", 1, 0, 1));

    // Inject mock PRHARedundancyProvider to simulate createBucketFailures
    vm0.invoke(() -> {
      InternalCache cache = spy(getCache());

      PartitionedRegion origRegion = (PartitionedRegion) cache.getRegion("region1");
      PartitionedRegion spyRegion = spy(origRegion);
      PRHARedundancyProvider redundancyProvider =
          spy(new PRHARedundancyProvider(spyRegion, mock(InternalResourceManager.class)));

      // return the spied region when ever getPartitionedRegions() is invoked
      Set<PartitionedRegion> parRegions = cache.getPartitionedRegions();
      parRegions.remove(origRegion);
      parRegions.add(spyRegion);

      doReturn(parRegions).when(cache).getPartitionedRegions();
      doReturn(redundancyProvider).when(spyRegion).getRedundancyProvider();

      // simulate create bucket fails on member2 and test if it creates on member3
      doReturn(false).when(redundancyProvider).createBackupBucketOnMember(anyInt(),
          eq((InternalDistributedMember) member2), anyBoolean(), anyBoolean(), any(),
          anyBoolean());

      // Now simulate a rebalance
      // Create operationImpl and not factory as we need spied cache to be passed to operationImpl
      RegionFilter filter = new FilterByPath(null, null);
      RebalanceOperationImpl operation = new RebalanceOperationImpl(cache, false, filter);
      operation.start();

      RebalanceResults results = operation.getResults(TIMEOUT_SECONDS, SECONDS);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(1);
      assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(0);
      assertThat(results.getTotalBucketTransferBytes()).isEqualTo(0);
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(0);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(1);

      PartitionRebalanceInfo details = detailSet.iterator().next();
      assertThat(details.getBucketCreatesCompleted()).isEqualTo(1);
      assertThat(details.getPrimaryTransfersCompleted()).isEqualTo(0);
      assertThat(details.getBucketTransferBytes()).isEqualTo(0);
      assertThat(details.getBucketTransfersCompleted()).isEqualTo(0);

      Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
      assertThat(afterDetails).hasSize(3);
      for (PartitionMemberInfo memberDetails : afterDetails) {
        if (memberDetails.getDistributedMember().equals(member1)) {
          assertThat(memberDetails.getBucketCount()).isEqualTo(1);
          assertThat(memberDetails.getPrimaryCount()).isEqualTo(1);
        } else if (memberDetails.getDistributedMember().equals(member2)) {
          assertThat(memberDetails.getBucketCount()).isEqualTo(0);
          assertThat(memberDetails.getPrimaryCount()).isEqualTo(0);
        } else if (memberDetails.getDistributedMember().equals(member3)) {
          assertThat(memberDetails.getBucketCount()).isEqualTo(1);
          assertThat(memberDetails.getPrimaryCount()).isEqualTo(0);
        }
      }

      ResourceManagerStats stats = cache.getInternalResourceManager().getStats();

      assertThat(stats.getRebalancesInProgress()).isEqualTo(0);
      assertThat(stats.getRebalancesCompleted()).isEqualTo(1);
      assertThat(stats.getRebalanceBucketCreatesInProgress()).isEqualTo(0);
      assertThat(stats.getRebalanceBucketCreatesCompleted())
          .isEqualTo(results.getTotalBucketCreatesCompleted());
      assertThat(stats.getRebalanceBucketCreatesFailed()).isEqualTo(1);
    });

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> validateRedundancy("region1", 1, 1, 0));
    }
  }

  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}(simulate={0})")
  public void testRecoverRedundancyColocatedRegions(boolean simulate) {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    DistributedMember member1 = vm0.invoke(() -> {
      createPartitionedRegion("region1", 1, 200);
      return getCache().getDistributedSystem().getDistributedMember();
    });

    vm0.invoke(() -> createPartitionedRegion("region2", 200, "region1"));

    // Create some buckets.
    vm0.invoke(() -> {
      Region<Number, String> region = getCache().getRegion("region1");
      Region<Number, String> region2 = getCache().getRegion("region2");
      for (int i = 0; i < 12; i++) {
        region.put(i, "A");
        region2.put(i, "A");
      }
    });

    // check to make sure our redundancy is impaired
    vm0.invoke(() -> validateRedundancyOfTwoRegions("region1", "region2", 12, 0, 12));

    // Now create the region in 2 more vms, each which
    // has local max memory of 1/2 that of the original VM.
    vm1.invoke(() -> createPartitionedRegion("region1", 1, 100));
    vm2.invoke(() -> createPartitionedRegion("region1", 1, 100));
    vm1.invoke(() -> createPartitionedRegion("region2", 100, "region1"));
    vm2.invoke(() -> createPartitionedRegion("region2", 100, "region1"));

    vm0.invoke(() -> validateRedundancyOfTwoRegions("region1", "region2", 12, 0, 12));

    // Now simulate a rebalance
    vm0.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();
      RebalanceResults results = doRebalance(simulate, manager);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(24);
      assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(12);
      assertThat(results.getTotalBucketTransferBytes()).isEqualTo(0);
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(0);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(2);

      for (PartitionRebalanceInfo details : detailSet) {
        assertThat(details.getBucketCreatesCompleted()).isEqualTo(12);
        assertThat(details.getPrimaryTransfersCompleted()).isEqualTo(6);
        assertThat(details.getBucketTransferBytes()).isEqualTo(0);
        assertThat(details.getBucketTransfersCompleted()).isEqualTo(0);

        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        assertThat(afterDetails).hasSize(3);

        for (PartitionMemberInfo memberDetails : afterDetails) {
          if (memberDetails.getDistributedMember().equals(member1)) {
            assertThat(memberDetails.getBucketCount()).isEqualTo(12);
            assertThat(memberDetails.getPrimaryCount()).isEqualTo(6);
          } else {
            assertThat(memberDetails.getBucketCount()).isEqualTo(6);
            assertThat(memberDetails.getPrimaryCount()).isEqualTo(3);
          }
        }
        if (!simulate) {
          validateStatistics(manager, results);
        }
      }
    });

    if (!simulate) {
      for (VM vm : toArray(vm0, vm1, vm2)) {
        vm.invoke(() -> {
          PartitionedRegion region1 = (PartitionedRegion) getCache().getRegion("region1");
          PartitionedRegion region2 = (PartitionedRegion) getCache().getRegion("region2");

          PartitionRegionInfo details =
              PartitionRegionHelper.getPartitionRegionInfo(getCache().getRegion("region1"));
          assertThat(details.getCreatedBucketCount()).isEqualTo(12);
          assertThat(details.getActualRedundantCopies()).isEqualTo(1);
          assertThat(details.getLowRedundancyBucketCount()).isEqualTo(0);

          details = PartitionRegionHelper.getPartitionRegionInfo(getCache().getRegion("region2"));
          assertThat(details.getCreatedBucketCount()).isEqualTo(12);
          assertThat(details.getActualRedundantCopies()).isEqualTo(1);
          assertThat(details.getLowRedundancyBucketCount()).isEqualTo(0);

          assertThat(region2.getLocalPrimaryBucketsListTestOnly())
              .isEqualTo(region1.getLocalPrimaryBucketsListTestOnly());

          assertThat(region2.getLocalBucketsListTestOnly())
              .isEqualTo(region1.getLocalBucketsListTestOnly());
        });
      }
    }
  }

  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}(simulate={0})")
  public void testRecoverRedundancyParallelAsyncEventQueue(boolean simulate)
      throws SecurityException {
    if (simulate) {
      invokeInEveryVM(
          () -> System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "LOG_REBALANCE", "true"));
    }

    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    DistributedMember member1 = vm0.invoke(() -> {
      createPRRegionWithAsyncQueue(200);
      return getCache().getDistributedSystem().getDistributedMember();
    });

    // Create some buckets. Put enough data to cause the queue to overflow (more than 1 MB)
    vm0.invoke(() -> {
      Region<Number, String> region = ((Cache) getCache()).getRegion("region1");
      for (int i = 0; i < 12; i++) {
        region.put(i, "A", new byte[1024 * 512]);
      }

      // The async event queue uses asynchronous writes.
      // Flush the default disk store to make sure all values have overflowed
      getCache().findDiskStore(null).flush();
    });

    // check to make sure our redundancy is impaired

    vm0.invoke(() -> {
      Region region1 = getCache().getRegion("region1");
      PartitionedRegion region2 =
          ColocationHelper.getColocatedChildRegions((PartitionedRegion) region1).get(0);

      validateRedundancyOfTwoRegions(region1, region2, 12, 0, 12);

      assertThat(getCache().getAsyncEventQueue("parallelQueue").size()).isEqualTo(12);
    });

    // Create the region on two more members, each with 1/2 of the memory
    vm1.invoke(() -> createPRRegionWithAsyncQueue(100));
    vm2.invoke(() -> createPRRegionWithAsyncQueue(100));

    vm0.invoke(() -> {
      Region region1 = getCache().getRegion("region1");
      PartitionedRegion region2 =
          ColocationHelper.getColocatedChildRegions((PartitionedRegion) region1).get(0);

      validateRedundancyOfTwoRegions(region1, region2, 12, 0, 12);

      assertThat(getCache().getAsyncEventQueue("parallelQueue").size()).isEqualTo(12);
    });

    // Now simulate a rebalance
    vm0.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();
      RebalanceResults results = doRebalance(simulate, manager);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(24);
      assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(12);
      assertThat(results.getTotalBucketTransferBytes()).isEqualTo(0);
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(0);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(2);

      for (PartitionRebalanceInfo details : detailSet) {
        assertThat(details.getBucketCreatesCompleted()).isEqualTo(12);
        assertThat(details.getPrimaryTransfersCompleted()).isEqualTo(6);
        assertThat(details.getBucketTransferBytes()).isEqualTo(0);
        assertThat(details.getBucketTransfersCompleted()).isEqualTo(0);

        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        assertThat(afterDetails).hasSize(3);

        for (PartitionMemberInfo memberDetails : afterDetails) {
          if (memberDetails.getDistributedMember().equals(member1)) {
            assertThat(memberDetails.getBucketCount()).isEqualTo(12);
            assertThat(memberDetails.getPrimaryCount()).isEqualTo(6);
          } else {
            assertThat(memberDetails.getBucketCount()).isEqualTo(6);
            assertThat(memberDetails.getPrimaryCount()).isEqualTo(3);
          }
        }

        if (!simulate) {
          validateStatistics(manager, results);
        }
      }
    });

    if (!simulate) {
      for (VM vm : toArray(vm0, vm1, vm2)) {
        vm.invoke(() -> {
          PartitionedRegion region1 = (PartitionedRegion) getCache().getRegion("region1");
          // Get the async event queue region (It's a colocated region)
          PartitionedRegion region2 = ColocationHelper.getColocatedChildRegions(region1).get(0);

          validateRedundancyOfTwoRegions(region1, region2, 12, 1, 0);

          assertThat(region2.getLocalPrimaryBucketsListTestOnly())
              .isEqualTo(region1.getLocalPrimaryBucketsListTestOnly());

          assertThat(region2.getLocalBucketsListTestOnly())
              .isEqualTo(region1.getLocalBucketsListTestOnly());
        });
      }
    }
  }

  @Test
  public void testCancelOperation() {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    // Create the region in only 1 VM
    vm0.invoke(() -> createPartitionedRegion("region1", 1));

    // Create some buckets
    vm0.invoke(() -> {
      Region<Number, String> region = ((Cache) getCache()).getRegion("region1");
      region.put(1, "A");
    });

    // make sure we can tell that the buckets have low redundancy
    vm0.invoke(() -> validateRedundancy("region1", 1, 0, 1));

    // Create the region in the other VM (should have no effect)
    vm1.invoke(() -> createPartitionedRegion("region1", 1));

    // Make sure we still have low redundancy
    vm0.invoke(() -> validateRedundancy("region1", 1, 0, 1));

    // Now do a rebalance, but cancel it in the middle
    vm0.invoke(() -> {
      CountDownLatch rebalancingCancelled = new CountDownLatch(1);
      CountDownLatch rebalancingFinished = new CountDownLatch(1);

      InternalResourceManager manager = getCache().getInternalResourceManager();

      InternalResourceManager.setResourceObserver(new ResourceObserverAdapter() {

        @Override
        public void rebalancingOrRecoveryStarted(Region region) {
          try {
            rebalancingCancelled.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }

        @Override
        public void rebalancingOrRecoveryFinished(Region region) {
          rebalancingFinished.countDown();
        }
      });

      RebalanceOperation op = manager.createRebalanceFactory().start();
      assertThat(op.isCancelled()).isFalse();
      assertThat(op.isDone()).isFalse();
      assertThat(manager.getRebalanceOperations()).isEqualTo(Collections.singleton(op));

      Throwable thrown = catchThrowable(() -> op.getResults(5, SECONDS));
      assertThat(thrown).isInstanceOf(TimeoutException.class);

      boolean result = op.cancel();
      assertThat(result).isTrue();

      rebalancingCancelled.countDown();

      assertThat(op.isCancelled()).isTrue();
      assertThat(op.isDone()).isTrue();

      rebalancingFinished.await();

      thrown = catchThrowable(() -> op.getResults(60, SECONDS));
      assertThat(thrown).isInstanceOf(CancellationException.class);

      assertThat(manager.getRebalanceOperations()).isEqualTo(Collections.emptySet());
    });

    // We should still have low redundancy, because the rebalancing was cancelled
    vm0.invoke(() -> validateRedundancy("region1", 1, 0, 1));
  }

  /**
   * Test that the rebalancing operation picks up on a concurrent membership change
   */
  @Test
  public void testMembershipChange() {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    // Create the region in only 1 VM
    vm0.invoke(() -> createPartitionedRegion("region1", 0));

    // Create some buckets
    vm0.invoke(() -> {
      Region<Number, String> region = getCache().getRegion("region1");
      region.put(1, "A");
      region.put(2, "A");
      region.put(3, "A");
      region.put(4, "A");
      region.put(5, "A");
      region.put(6, "A");
    });

    // Create the region in the other VM (should have no effect)
    vm1.invoke(() -> createPartitionedRegion("region1", 0));

    // Now do a rebalance, but start another member in the middle
    vm0.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();

      InternalResourceManager.setResourceObserver(new ResourceObserverAdapter() {
        private volatile boolean firstBucket = true;

        @Override
        public void movingBucket(Region region, int bucketId, DistributedMember source,
            DistributedMember target) {
          if (firstBucket) {
            firstBucket = false;
            // NOTE: do not replace createPartitionedRegionRunnable with lambda
            // because ResourceObserverAdapter is NOT serializable
            vm2.invoke(createPartitionedRegionRunnable("region1", 0));
          }
        }
      });

      RebalanceResults results = doRebalance(false, manager);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(0);
      assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(0);
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(4);
      assertThat(results.getTotalBucketTransferBytes()).isGreaterThan(0);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(1);

      PartitionRebalanceInfo details = detailSet.iterator().next();
      assertThat(details.getBucketCreatesCompleted()).isEqualTo(0);
      assertThat(details.getPrimaryTransfersCompleted()).isEqualTo(0);
      assertThat(details.getBucketTransferBytes()).isGreaterThan(0);
      assertThat(details.getBucketTransfersCompleted()).isEqualTo(4);

      Set<PartitionMemberInfo> beforeDetails = details.getPartitionMemberDetailsBefore();
      // there should have only been 2 members when the rebalancing started.
      assertThat(beforeDetails).hasSize(2);

      // if it was done, there should now be 3 members.
      Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
      assertThat(afterDetails).hasSize(3);

      for (PartitionMemberInfo memberDetails : afterDetails) {
        assertThat(memberDetails.getBucketCount()).isEqualTo(2);
        assertThat(memberDetails.getPrimaryCount()).isEqualTo(2);
      }

      validateStatistics(manager, results);

      ResourceManagerStats stats = manager.getStats();
      assertThat(stats.getRebalanceMembershipChanges()).isEqualTo(1);
    });
  }

  /**
   * Check to make sure that we balance buckets between two hosts with no redundancy.
   */
  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}(simulate={0})")
  public void testMoveBucketsNoRedundancy(boolean simulate) {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    // Create the region in only 1 VM
    vm0.invoke(() -> createPartitionedRegion("region1", new NullReturningLoader()));

    // Create some buckets
    vm0.invoke(() -> {
      Region<Number, String> region = getCache().getRegion("region1");
      region.put(1, "A");
      region.put(2, "A");
      region.put(3, "A");
      region.put(4, "A");
      region.put(5, "A");
      region.put(6, "A");
    });

    // Create the region in the other VM (should have no effect)
    vm1.invoke(() -> createPartitionedRegion("region1", new NullReturningLoader()));

    // Now simulate a rebalance
    vm0.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();
      RebalanceResults results = doRebalance(simulate, manager);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(0);
      assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(0);
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(3);
      assertThat(results.getTotalBucketTransferBytes()).isGreaterThan(0);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(1);

      PartitionRebalanceInfo details = detailSet.iterator().next();
      assertThat(details.getBucketCreatesCompleted()).isEqualTo(0);
      assertThat(details.getPrimaryTransfersCompleted()).isEqualTo(0);
      assertThat(details.getBucketTransferBytes()).isGreaterThan(0);
      assertThat(details.getBucketTransfersCompleted()).isEqualTo(3);
      assertThat(details.getNumberOfMembersExecutedOn()).isEqualTo(2);

      Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
      assertThat(afterDetails).hasSize(2);

      for (PartitionMemberInfo memberDetails : afterDetails) {
        assertThat(memberDetails.getBucketCount()).isEqualTo(3);
        assertThat(memberDetails.getPrimaryCount()).isEqualTo(3);
      }

      if (!simulate) {
        validateStatistics(manager, results);
      }
    });

    if (!simulate) {
      for (VM vm : toArray(vm0, vm1)) {
        vm.invoke(() -> {
          Region region = getCache().getRegion("region1");

          PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
          assertThat(details.getCreatedBucketCount()).isEqualTo(6);
          assertThat(details.getActualRedundantCopies()).isEqualTo(0);
          assertThat(details.getLowRedundancyBucketCount()).isEqualTo(0);
          assertThat(details.getPartitionMemberInfo()).hasSize(2);

          for (PartitionMemberInfo memberDetails : details.getPartitionMemberInfo()) {
            assertThat(memberDetails.getBucketCount()).isEqualTo(3);
            assertThat(memberDetails.getPrimaryCount()).isEqualTo(3);
          }

          // check to make sure that moving buckets didn't close the cache loader
          NullReturningLoader loader =
              (NullReturningLoader) getCache().getRegion("region1").getAttributes()
                  .getCacheLoader();
          assertThat(loader.isClosed()).isFalse();
        });
      }

      for (VM vm : toArray(vm0, vm1)) {
        vm.invoke(() -> {
          NullReturningLoader loader =
              (NullReturningLoader) getCache().getRegion("region1").getAttributes()
                  .getCacheLoader();
          assertThat(loader.isClosed()).isFalse();
          // check to make sure that closing the PR closes the cache loader
          getCache().getRegion("region1").close();
          assertThat(loader.isClosed()).isTrue();
        });
      }
    }
  }

  /**
   * Check to make sure that we balance buckets between two hosts with no redundancy.
   */
  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}(simulate={0})")
  public void testFilterRegions(boolean simulate) {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    Set<String> included = new HashSet<>();
    included.add("region0");
    included.add("region1");

    Set<String> excluded = new HashSet<>();
    excluded.add("region0");
    excluded.add("region3");

    Set<String> expectedRebalanced = new HashSet<>();
    expectedRebalanced.add("/region0");
    expectedRebalanced.add("/region1");

    int numRegions = 4;

    // Create the region in only 1 VM
    vm0.invoke(() -> {
      for (int whichRegion = 0; whichRegion < numRegions; whichRegion++) {
        createPartitionedRegion("region" + whichRegion, 0);
      }
    });

    // Create some buckets
    vm0.invoke(() -> {
      for (int whichRegion = 0; whichRegion < numRegions; whichRegion++) {
        Region<Number, String> region = getCache().getRegion("region" + whichRegion);
        for (int key = 0; key < 6; key++) {
          region.put(key, "A");
        }
      }
    });

    // Create the region in the other VM (should have no effect)
    vm1.invoke(() -> {
      for (int whichRegion = 0; whichRegion < numRegions; whichRegion++) {
        createPartitionedRegion("region" + whichRegion, 0);
      }
    });

    // Now simulate a rebalance
    vm0.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();

      RebalanceResults results = doRebalance(simulate, manager, included, excluded);
      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      Set<String> names = new HashSet<>();

      for (PartitionRebalanceInfo details : detailSet) {
        assertThat(details.getBucketCreatesCompleted()).isEqualTo(0);
        assertThat(details.getPrimaryTransfersCompleted()).isEqualTo(0);
        assertThat(details.getBucketTransferBytes()).isGreaterThan(0);
        assertThat(details.getBucketTransfersCompleted()).isEqualTo(3);

        names.add(details.getRegionPath());

        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        assertThat(afterDetails).hasSize(2);

        for (PartitionMemberInfo memberDetails : afterDetails) {
          assertThat(memberDetails.getBucketCount()).isEqualTo(3);
          assertThat(memberDetails.getPrimaryCount()).isEqualTo(3);
        }
      }

      assertThat(names).isEqualTo(expectedRebalanced);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(0);
      assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(0);
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(6);
      assertThat(results.getTotalBucketTransferBytes()).isGreaterThan(0);

      if (!simulate) {
        validateStatistics(manager, results);
      }
    });

    if (!simulate) {
      for (VM vm : toArray(vm0, vm1)) {
        vm.invoke(() -> {
          for (String name : expectedRebalanced) {
            Region region = getCache().getRegion(name);

            PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
            assertThat(details.getCreatedBucketCount()).isEqualTo(6);
            assertThat(details.getActualRedundantCopies()).isEqualTo(0);
            assertThat(details.getLowRedundancyBucketCount()).isEqualTo(0);
            assertThat(details.getPartitionMemberInfo()).hasSize(2);

            for (PartitionMemberInfo memberDetails : details.getPartitionMemberInfo()) {
              assertThat(memberDetails.getBucketCount()).isEqualTo(3);
              assertThat(memberDetails.getPrimaryCount()).isEqualTo(3);
            }
          }

          Region region = getCache().getRegion("region2");

          PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
          assertThat(details.getCreatedBucketCount()).isEqualTo(6);
          assertThat(details.getActualRedundantCopies()).isEqualTo(0);
          assertThat(details.getLowRedundancyBucketCount()).isEqualTo(0);
          assertThat(details.getPartitionMemberInfo()).hasSize(2);

          for (PartitionMemberInfo memberDetails : details.getPartitionMemberInfo()) {
            int bucketCount = memberDetails.getBucketCount();
            int primaryCount = memberDetails.getPrimaryCount();
            assertThat(
                bucketCount == 6 && primaryCount == 6 || bucketCount == 0 && primaryCount == 0)
                    .as("Wrong number of buckets on non rebalanced region buckets=" + bucketCount
                        + " primaries=" + primaryCount)
                    .isTrue();
          }
        });
      }
    }
  }

  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}(simulate={0})")
  public void testMoveBucketsWithRedundancy(boolean simulate) {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    // Create the region in two VMs
    vm0.invoke(() -> createPartitionedRegion("region1", 1));
    vm1.invoke(() -> createPartitionedRegion("region1", 1));

    // Create some buckets
    vm0.invoke(() -> {
      Region<Number, String> region = getCache().getRegion("region1");
      for (int i = 0; i < 12; i++) {
        region.put(i, "A");
      }
    });

    // Create the region in one more VM.
    vm2.invoke(() -> createPartitionedRegion("region1", 1));

    // Now simulate a rebalance
    long totalSize = vm0.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();
      RebalanceResults results = doRebalance(simulate, manager);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(0);

      // We don't know how many primaries will move, it depends on
      // if the move bucket code moves the primary or a redundant bucket
      // assertIndexDetailsEquals(0, results.getTotalPrimaryTransfersCompleted());
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(8);
      assertThat(results.getTotalBucketTransferBytes()).isGreaterThan(0);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(1);

      PartitionRebalanceInfo details = detailSet.iterator().next();
      assertThat(details.getBucketCreatesCompleted()).isEqualTo(0);
      assertThat(details.getBucketTransferBytes()).isGreaterThan(0);
      assertThat(details.getBucketTransfersCompleted()).isEqualTo(8);

      long value = 0;
      Set<PartitionMemberInfo> beforeDetails = details.getPartitionMemberDetailsAfter();
      for (PartitionMemberInfo memberDetails : beforeDetails) {
        value += memberDetails.getSize();
      }

      long afterSize = 0;
      Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
      assertThat(afterDetails).hasSize(3);

      for (PartitionMemberInfo memberDetails : afterDetails) {
        assertThat(memberDetails.getBucketCount()).isEqualTo(8);
        assertThat(memberDetails.getPrimaryCount()).isEqualTo(4);
        afterSize += memberDetails.getSize();
      }

      assertThat(afterSize).isEqualTo(value);

      if (!simulate) {
        validateStatistics(manager, results);
      }

      return value;
    });

    if (!simulate) {
      for (VM vm : toArray(vm0, vm1, vm2)) {
        vm.invoke(() -> {
          Region region = getCache().getRegion("region1");

          PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
          assertThat(details.getCreatedBucketCount()).isEqualTo(12);
          assertThat(details.getActualRedundantCopies()).isEqualTo(1);
          assertThat(details.getLowRedundancyBucketCount()).isEqualTo(0);

          long afterSize = 0;
          for (PartitionMemberInfo memberDetails : details.getPartitionMemberInfo()) {
            assertThat(memberDetails.getBucketCount()).isEqualTo(8);
            assertThat(memberDetails.getPrimaryCount()).isEqualTo(4);
            afterSize += memberDetails.getSize();
          }

          assertThat(afterSize).isEqualTo(totalSize);
        });
      }
    }
  }

  /**
   * Verifies when overflowing entries to disk that the stats are correct and we still rebalance
   * correctly
   */
  @Test
  public void testMoveBucketsOverflowToDisk() {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);
    VM vm3 = getVM(3);

    // Create the region in two VMs
    vm0.invoke(
        () -> createPartitionedRegion("region1", createLRUEntryAttributes(1, OVERFLOW_TO_DISK)));
    vm1.invoke(
        () -> createPartitionedRegion("region1", createLRUEntryAttributes(1, OVERFLOW_TO_DISK)));

    // Create some buckets
    vm0.invoke(() -> {
      Region<Number, String> region = ((Cache) getCache()).getRegion("region1");
      for (int bucket = 0; bucket < 12; bucket++) {
        Map<Number, String> map = new HashMap<>();
        for (int key = 0; key < 200; key++) {
          map.put(bucket + 113 * key, "A");
        }
        region.putAll(map);
      }
    });

    // Do some puts and gets, to trigger eviction

    // Do some operations
    vm0.invoke(() -> {
      Region<Number, String> region = getCache().getRegion("region1");

      Random random = new Random();

      for (int count = 0; count < 5000; count++) {
        int bucket = count % 12;
        int key = random.nextInt(20);
        region.put(bucket + 113 * key, "B");
      }

      for (int count = 0; count < 500; count++) {
        int bucket = count % 12;
        int key = random.nextInt(20);
        region.get(bucket + 113 * key);
      }
    });

    // Create the region in one more VM.
    vm2.invoke(
        () -> createPartitionedRegion("region1", createLRUEntryAttributes(1, OVERFLOW_TO_DISK)));

    // Now do a rebalance
    vm0.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();
      RebalanceResults results = doRebalance(false, manager);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(0);

      // We don't know how many primaries will move, it depends on
      // if the move bucket code moves the primary or a redundant bucket
      // assertIndexDetailsEquals(0, results.getTotalPrimaryTransfersCompleted());
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(8);
      assertThat(results.getTotalBucketTransferBytes()).isGreaterThan(0);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(1);

      PartitionRebalanceInfo details = detailSet.iterator().next();
      assertThat(details.getBucketCreatesCompleted()).isEqualTo(0);
      assertThat(details.getBucketTransferBytes()).isGreaterThan(0);
      assertThat(details.getBucketTransfersCompleted()).isEqualTo(8);

      long value = 0;
      Set<PartitionMemberInfo> beforeDetails = details.getPartitionMemberDetailsAfter();
      for (PartitionMemberInfo memberDetails : beforeDetails) {
        value += memberDetails.getSize();
      }

      Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
      assertThat(afterDetails).hasSize(3);

      long afterSize = 0;
      for (PartitionMemberInfo memberDetails : afterDetails) {
        assertThat(memberDetails.getBucketCount()).isEqualTo(8);
        assertThat(memberDetails.getPrimaryCount()).isEqualTo(4);
        afterSize += memberDetails.getSize();
      }
      assertThat(afterSize).isEqualTo(value);

      validateStatistics(manager, results);
    });

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        Region region = getCache().getRegion("region1");

        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertThat(details.getCreatedBucketCount()).isEqualTo(12);
        assertThat(details.getActualRedundantCopies()).isEqualTo(1);
        assertThat(details.getLowRedundancyBucketCount()).isEqualTo(0);

        for (PartitionMemberInfo memberDetails : details.getPartitionMemberInfo()) {
          assertThat(memberDetails.getBucketCount()).isEqualTo(8);
          assertThat(memberDetails.getPrimaryCount()).isEqualTo(4);
        }
      });
    }

    // Create the region in one more VM.
    vm3.invoke(
        () -> createPartitionedRegion("region1", createLRUEntryAttributes(1, OVERFLOW_TO_DISK)));

    // Do another rebalance
    vm0.invoke(() -> {
      ResourceManager manager = getCache().getResourceManager();
      RebalanceResults results = doRebalance(false, manager);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(0);

      // We don't know how many primaries will move, it depends on
      // if the move bucket code moves the primary or a redundant bucket
      // assertIndexDetailsEquals(0, results.getTotalPrimaryTransfersCompleted());
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(6);
      assertThat(results.getTotalBucketTransferBytes()).isGreaterThan(0);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(1);

      PartitionRebalanceInfo details = detailSet.iterator().next();
      assertThat(details.getBucketCreatesCompleted()).isEqualTo(0);
      assertThat(details.getBucketTransferBytes()).isGreaterThan(0);
      assertThat(details.getBucketTransfersCompleted()).isEqualTo(6);

      long totalSize = 0;
      Set<PartitionMemberInfo> beforeDetails = details.getPartitionMemberDetailsAfter();
      for (PartitionMemberInfo memberDetails : beforeDetails) {
        totalSize += memberDetails.getSize();
      }

      Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
      assertThat(afterDetails).hasSize(4);

      long afterSize = 0;
      for (PartitionMemberInfo memberDetails : afterDetails) {
        assertThat(memberDetails.getBucketCount()).isEqualTo(6);

        afterSize += memberDetails.getSize();
      }
      assertThat(afterSize).isEqualTo(totalSize);
    });

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        Region region = getCache().getRegion("region1");

        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertThat(details.getCreatedBucketCount()).isEqualTo(12);
        assertThat(details.getActualRedundantCopies()).isEqualTo(1);
        assertThat(details.getLowRedundancyBucketCount()).isEqualTo(0);

        for (PartitionMemberInfo memberDetails : details.getPartitionMemberInfo()) {
          assertThat(memberDetails.getBucketCount()).isEqualTo(6);
        }
      });
    }
  }

  /**
   * Test to make sure we balance buckets between three hosts with redundancy
   */
  @Test
  public void testMoveBucketsNestedPR() {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    // Create the region in two VMs
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        Region parent = getCache().createRegionFactory(REPLICATE).create("parent");
        createPartitionedRegionAsSubregion(parent, "region1");
      });
    }

    // Create some buckets
    vm0.invoke(() -> {
      Region<Number, String> region = getCache().getRegion("parent/region1");
      for (int i = 0; i < 12; i++) {
        region.put(i, "A");
      }
    });

    // Create the region in one more VM.
    vm2.invoke(() -> {
      Region parent = getCache().createRegionFactory(REPLICATE).create("parent");
      createPartitionedRegionAsSubregion(parent, "region1");
    });

    // Now simulate a rebalance
    long totalSize = vm0.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();
      RebalanceResults results = doRebalance(false, manager);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(0);

      // We don't know how many primaries will move, it depends on
      // if the move bucket code moves the primary or a redundant bucket
      // assertIndexDetailsEquals(0, results.getTotalPrimaryTransfersCompleted());
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(8);
      assertThat(results.getTotalBucketTransferBytes()).isGreaterThan(0);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(1);

      PartitionRebalanceInfo details = detailSet.iterator().next();
      assertThat(details.getBucketCreatesCompleted()).isEqualTo(0);
      assertThat(0 < details.getBucketTransferBytes()).isTrue();
      assertThat(details.getBucketTransfersCompleted()).isEqualTo(8);

      long value = 0;
      Set<PartitionMemberInfo> beforeDetails = details.getPartitionMemberDetailsAfter();
      for (PartitionMemberInfo memberDetails : beforeDetails) {
        value += memberDetails.getSize();
      }

      Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
      assertThat(afterDetails).hasSize(3);

      long afterSize = 0;
      for (PartitionMemberInfo memberDetails : afterDetails) {
        assertThat(memberDetails.getBucketCount()).isEqualTo(8);
        assertThat(memberDetails.getPrimaryCount()).isEqualTo(4);
        afterSize += memberDetails.getSize();
      }
      assertThat(afterSize).isEqualTo(value);
      validateStatistics(manager, results);

      return value;
    });

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        Region region = getCache().getRegion("parent/region1");

        PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertThat(details.getCreatedBucketCount()).isEqualTo(12);
        assertThat(details.getActualRedundantCopies()).isEqualTo(1);
        assertThat(details.getLowRedundancyBucketCount()).isEqualTo(0);

        long afterSize = 0;
        for (PartitionMemberInfo memberDetails : details.getPartitionMemberInfo()) {
          assertThat(memberDetails.getBucketCount()).isEqualTo(8);
          assertThat(memberDetails.getPrimaryCount()).isEqualTo(4);
          afterSize += memberDetails.getSize();
        }
        assertThat(afterSize).isEqualTo(totalSize);
      });
    }
  }

  /**
   * Test to make sure that we move buckets to balance between three hosts with a redundancy of 1
   * and two colocated regions. Makes sure that the buckets stay colocated when we move them.
   */
  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}(simulate={0})")
  public void testMoveBucketsColocatedRegions(boolean simulate) {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    vm0.invoke(() -> createPartitionedRegion("region1", 1, 200));
    vm0.invoke(() -> createPartitionedRegion("region2", 200, "region1"));
    vm1.invoke(() -> createPartitionedRegion("region1", 1, 200));
    vm1.invoke(() -> createPartitionedRegion("region2", 200, "region1"));

    // Create some buckets.
    vm0.invoke(() -> {
      Region<Number, String> region1 = getCache().getRegion("region1");
      Region<Number, String> region2 = getCache().getRegion("region2");
      for (int key = 0; key < 12; key++) {
        region1.put(key, "A");
        region2.put(key, "A");
      }
    });

    // create the leader region, but not the colocated region in one of the VMs.
    vm2.invoke(() -> createPartitionedRegion("region1", 1, 200));

    // Simulate a rebalance, and make sure we don't
    // move any buckets yet, because we don't have
    // the colocated region in the new VMs.
    vm0.invoke(() -> {
      ResourceManager manager = getCache().getResourceManager();
      RebalanceResults results = doRebalance(simulate, manager);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(0);
      assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(0);
      assertThat(results.getTotalBucketTransferBytes()).isEqualTo(0);
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(0);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(2);

      for (PartitionRebalanceInfo details : detailSet) {
        assertThat(details.getBucketCreatesCompleted()).isEqualTo(0);
        assertThat(details.getPrimaryTransfersCompleted()).isEqualTo(0);
        assertThat(details.getBucketTransferBytes()).isEqualTo(0);
        assertThat(details.getBucketTransfersCompleted()).isEqualTo(0);
      }
    });

    // now create the colocated region in the last VM.
    vm2.invoke(() -> createPartitionedRegion("region2", 200, "region1"));

    vm0.invoke(() -> {
      ResourceManager manager = getCache().getResourceManager();
      RebalanceResults results = doRebalance(simulate, manager);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(0);
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(16);
      assertThat(results.getTotalBucketTransferBytes()).isGreaterThan(0);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(2);

      for (PartitionRebalanceInfo details : detailSet) {
        assertThat(details.getBucketCreatesCompleted()).isEqualTo(0);
        assertThat(details.getBucketTransferBytes()).isGreaterThan(0);
        assertThat(details.getBucketTransfersCompleted()).isEqualTo(8);

        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        assertThat(afterDetails).hasSize(3);

        for (PartitionMemberInfo memberDetails : afterDetails) {
          assertThat(memberDetails.getBucketCount()).isEqualTo(8);
          assertThat(memberDetails.getPrimaryCount()).isEqualTo(4);
        }
      }
    });

    if (!simulate) {
      for (VM vm : toArray(vm0, vm1, vm2)) {
        vm.invoke(() -> {
          PartitionedRegion region1 = (PartitionedRegion) getCache().getRegion("region1");
          PartitionedRegion region2 = (PartitionedRegion) getCache().getRegion("region2");

          PartitionRegionInfo details =
              PartitionRegionHelper.getPartitionRegionInfo(getCache().getRegion("region1"));
          assertThat(details).isNotNull();
          assertThat(details.getCreatedBucketCount()).isEqualTo(12);
          assertThat(details.getActualRedundantCopies()).isEqualTo(1);
          assertThat(details.getLowRedundancyBucketCount()).isEqualTo(0);

          details = PartitionRegionHelper.getPartitionRegionInfo(getCache().getRegion("region2"));
          assertThat(details).isNotNull();
          assertThat(details.getCreatedBucketCount()).isEqualTo(12);
          assertThat(details.getActualRedundantCopies()).isEqualTo(1);
          assertThat(details.getLowRedundancyBucketCount()).isEqualTo(0);

          assertThat(region2.getLocalPrimaryBucketsListTestOnly())
              .isEqualTo(region1.getLocalPrimaryBucketsListTestOnly());

          assertThat(region2.getLocalBucketsListTestOnly())
              .isEqualTo(region1.getLocalBucketsListTestOnly());
        });
      }
    }
  }

  /**
   * Test to ensure that we wait for in progress write operations before moving a primary.
   */
  @Test
  @Parameters({"PUT", "INVALIDATE", "DESTROY", "CACHE_LOADER"})
  @TestCaseName("{method}(operation={0})")
  public void runTestWaitForOperation(OperationEnum operation) throws Exception {
    VM vm1 = getVM(1);

    // Create a region in this VM with a cache writer
    // and cache loader
    createPartitionedRegion("region1", 1, 100, (CacheLoader<Number, String>) helper1 -> "anobject");
    Region<Number, String> region = getCache().getRegion("region1");

    // create some buckets
    region.put(1, "A");
    region.put(2, "A");

    BlockingCacheListener<Number, String> cacheWriter = new BlockingCacheListener<>(2);
    region.getAttributesMutator().addCacheListener(cacheWriter);

    // start two threads doing operations, one on each bucket
    // the threads will block on the cache writer. The rebalance operation
    // will try to move one of these buckets, but it shouldn't
    // be able to because of the in progress operation.
    executorServiceRule.submit(() -> operation.execute(region, 1));
    executorServiceRule.submit(() -> operation.execute(region, 2));

    cacheWriter.waitForOperationsStarted();

    // make sure we can tell that the buckets have low redundancy
    validateRedundancy("region1", 2, 0, 2);

    // Create the region in the other VM (should have no effect)
    vm1.invoke(() -> createPartitionedRegion("region1", 1, 100,
        (CacheLoader<Number, String>) helper1 -> "anobject"));

    // Make sure we still have low redundancy
    validateRedundancy("region1", 2, 0, 2);

    ResourceManager manager = getCache().getResourceManager();
    RebalanceOperation rebalance = manager.createRebalanceFactory().start();

    Throwable thrown = catchThrowable(() -> rebalance.getResults(5, SECONDS));
    assertThat(thrown).isInstanceOf(TimeoutException.class);

    cacheWriter.release();

    RebalanceResults results = rebalance.getResults(TIMEOUT_SECONDS, SECONDS);
    assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(2);
    assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(1);
    assertThat(results.getTotalBucketTransferBytes()).isEqualTo(0);
    assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(0);

    Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
    assertThat(detailSet).hasSize(1);

    PartitionRebalanceInfo details = detailSet.iterator().next();
    assertThat(details.getBucketCreatesCompleted()).isEqualTo(2);
    assertThat(details.getPrimaryTransfersCompleted()).isEqualTo(1);
    assertThat(details.getBucketTransferBytes()).isEqualTo(0);
    assertThat(details.getBucketTransfersCompleted()).isEqualTo(0);

    Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
    assertThat(afterDetails).hasSize(2);

    for (PartitionMemberInfo memberDetails : afterDetails) {
      assertThat(memberDetails.getBucketCount()).isEqualTo(2);
      assertThat(memberDetails.getPrimaryCount()).isEqualTo(1);

      validateRedundancy("region1", 2, 1, 0);
      vm1.invoke(() -> validateRedundancy("region1", 2, 1, 0));
    }
  }

  @Test
  @Parameters({"true,false", "false,false", "true,true", "false,true"})
  @TestCaseName("{method}(simulate={0}, userAccessor={1})")
  public void testRecoverRedundancyWithOfflinePersistence(boolean simulate, boolean useAccessor)
      throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);
    VM vm3 = getVM(3);

    int redundantCopies = 1;

    // Create the region in only 2 VMs
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(
          () -> createPersistentPartitionedRegion("region1", getUniqueName(), getDiskDirs(),
              redundantCopies));
    }

    VM rebalanceVM = useAccessor ? vm3 : vm0;
    if (useAccessor) {
      // Create an accessor
      rebalanceVM.invoke(() -> createAccessor("region1", "ds1"));
    }

    // Create some buckets
    vm0.invoke(() -> {
      Region<Number, String> region = getCache().getRegion("region1");
      region.put(1, "A");
      region.put(2, "A");
      region.put(3, "A");
      region.put(4, "A");
      region.put(5, "A");
      region.put(6, "A");
    });

    // Close the cache in vm1
    Set<Integer> bucketsOnVM1 = vm1.invoke(() -> getBucketList("region1"));
    vm1.invoke(() -> getCache().getRegion("region1").close());

    // make sure we can tell that the buckets have low redundancy
    vm0.invoke(() -> validateRedundancy("region1", 6, 0, 6));

    // Now create the cache in another member
    vm2.invoke(
        () -> createPersistentPartitionedRegion("region1", getUniqueName(), getDiskDirs(),
            redundantCopies));

    // Make sure we still have low redundancy
    vm0.invoke(() -> validateRedundancy("region1", 6, 0, 6));

    /*
     * Simulates a rebalance if simulation flag is set. Otherwise, performs a rebalance.
     *
     * A rebalance will replace offline buckets, so this should restore redundancy
     */
    rebalanceVM.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();
      RebalanceResults results = doRebalance(simulate, manager);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(6);
      assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(3);
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(0);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(1);

      PartitionRebalanceInfo details = detailSet.iterator().next();
      assertThat(details.getBucketCreatesCompleted()).isEqualTo(6);
      assertThat(details.getPrimaryTransfersCompleted()).isEqualTo(3);
      assertThat(details.getBucketTransfersCompleted()).isEqualTo(0);

      Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
      assertThat(afterDetails).hasSize(2);

      for (PartitionMemberInfo memberDetails : afterDetails) {
        assertThat(memberDetails.getBucketCount()).isEqualTo(6);
        assertThat(memberDetails.getPrimaryCount()).isEqualTo(3);
      }

      if (!simulate) {
        validateStatistics(manager, results);
      }
    });

    Set<Integer> bucketsOnVM0 = vm0.invoke(() -> getBucketList("region1"));
    Set<Integer> bucketsOnVM2 = vm2.invoke(() -> getBucketList("region1"));

    if (simulate) {
      // Otherwise, we should still have broken redundancy at this point
      vm0.invoke(() -> validateRedundancy("region1", 6, 0, 6));
    } else {
      // Make sure redundancy is repaired if not simulated
      vm0.invoke(() -> validateRedundancy("region1", 6, 1, 0));
    }

    vm2.invoke(() -> getCache().getRegion("region1").close());
    vm0.invoke(() -> getCache().getRegion("region1").close());

    if (useAccessor) {
      vm3.invoke(() -> getCache().getRegion("region1").close());
    }

    // We need to restart both VMs at the same time, because
    // they will wait for each other before allowing operations.
    AsyncInvocation createRegionOnVM0 = vm0.invokeAsync(
        () -> createPersistentPartitionedRegion("region1", getUniqueName(), getDiskDirs(),
            redundantCopies));
    AsyncInvocation createRegionOnVM2 = vm2.invokeAsync(
        () -> createPersistentPartitionedRegion("region1", getUniqueName(), getDiskDirs(),
            redundantCopies));

    createRegionOnVM0.await();
    createRegionOnVM2.await();

    if (useAccessor) {
      vm3.invoke(() -> createAccessor("region1", "ds1"));
    }

    // pause for async bucket recovery threads to finish their work. Otherwise
    // the rebalance op may think that the other member doesn't have buckets, then
    // ask it to create them and get a negative reply because it actually does
    // have the buckets, causing the test to fail

    // Try to rebalance again. This shouldn't do anything, because
    // we already recovered redundancy earlier.
    // Note that we don't check the primary balance. This rebalance
    // has a race with the redundancy recovery thread, which is also trying
    // to rebalance primaries. So this thread might end up rebalancing primaries,
    // or it might not.
    if (!simulate) {
      rebalanceVM.invoke(() -> {
        ResourceManager manager = getCache().getResourceManager();

        RebalanceResults results = doRebalance(simulate, manager);

        assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(0);
        assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(0);

        Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
        assertThat(detailSet).hasSize(1);

        PartitionRebalanceInfo details = detailSet.iterator().next();
        assertThat(details.getBucketCreatesCompleted()).isEqualTo(0);
        assertThat(details.getBucketTransfersCompleted()).isEqualTo(0);

        Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
        assertThat(afterDetails).hasSize(2);

        for (PartitionMemberInfo memberDetails : afterDetails) {
          assertThat(memberDetails.getBucketCount()).isEqualTo(6);
          assertThat(memberDetails.getPrimaryCount()).isEqualTo(3);
        }
      });

      // Redundancy should be repaired.
      vm0.invoke(() -> validateRedundancy("region1", 6, 1, 0));
    }

    vm1.invoke(
        () -> createPersistentPartitionedRegion("region1", getUniqueName(), getDiskDirs(),
            redundantCopies));

    // Look at vm0 buckets.
    assertThat(vm0.invoke(() -> getBucketList("region1"))).isEqualTo(bucketsOnVM0);

    /*
     * Look at vm1 buckets.
     */
    if (simulate) {
      /*
       * No rebalancing above because the simulation flag is on. Therefore, vm1 will have recovered
       * its buckets. We need to wait for the buckets because they might still be in the middle of
       * creation in the background
       */
      vm1.invoke(() -> waitForBucketList("region1", bucketsOnVM1));
    } else {
      /*
       * vm1 should have no buckets because offline buckets were recovered when vm0 and vm2 were
       * rebalanced above.
       */
      assertThat(vm1.invoke(() -> getBucketList("region1"))).isEmpty();
    }

    // look at vm2 buckets
    assertThat(vm2.invoke(() -> getBucketList("region1"))).isEqualTo(bucketsOnVM2);
  }

  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}(simulate={0})")
  public void testMoveBucketsWithUnrecoveredValues(boolean simulate) {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    // Create the region in only 1 VM
    vm0.invoke(() -> createPersistentPartitionedRegion("region1", "store", getDiskDirs(),
        new NullReturningLoader<>()));

    // Create some buckets
    vm0.invoke(() -> {
      Region<Number, String> region = getCache().getRegion("region1");
      region.put(1, "A");
      region.put(2, "A");
      region.put(3, "A");
      region.put(4, "A");
      region.put(5, "A");
      region.put(6, "A");
    });

    vm0.invoke(() -> {
      PartitionedRegion region = (PartitionedRegion) getCache().getRegion("region1");
      PartitionedRegionDataStore dataStore = region.getDataStore();

      for (int i = 1; i <= 6; i++) {
        BucketRegion bucket = dataStore.getLocalBucketById(i);

        assertThat(bucket.getNumOverflowBytesOnDisk()).isEqualTo(0);
        assertThat(bucket.getNumOverflowOnDisk()).isEqualTo(0);
        assertThat(bucket.getNumEntriesInVM()).isEqualTo(1);
      }
      getCache().close();
    });

    // now recover the region
    vm0.invoke(() -> createPersistentPartitionedRegion("region1", "store", getDiskDirs(),
        new NullReturningLoader<>()));

    vm0.invoke(() -> {
      PartitionedRegion region = (PartitionedRegion) getCache().getRegion("region1");
      PartitionedRegionDataStore dataStore = region.getDataStore();

      for (int i = 1; i <= 6; i++) {
        BucketRegion bucket = dataStore.getLocalBucketById(i);

        assertThat(bucket.getNumOverflowOnDisk()).isEqualTo(1);
        assertThat(bucket.getNumEntriesInVM()).isEqualTo(0);

        // the size recorded on disk is not the same is the in memory size, apparently
        assertThat(bucket.getNumOverflowBytesOnDisk())
            .as("Bucket size was " + bucket.getNumOverflowBytesOnDisk())
            .isGreaterThan(1);
        assertThat(bucket.getTotalBytes()).isEqualTo(bucket.getNumOverflowBytesOnDisk());
      }
    });

    // Create the region in the other VM (should have no effect)
    vm1.invoke(() -> createPersistentPartitionedRegion("region1", "store", getDiskDirs(),
        new NullReturningLoader<>()));

    // Now simulate a rebalance
    vm0.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();
      RebalanceResults results = doRebalance(simulate, manager);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(0);
      assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(0);
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(3);
      assertThat(results.getTotalBucketTransferBytes())
          .as("Transferred Bytes = " + results.getTotalBucketTransferBytes())
          .isGreaterThan(0);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(1);

      PartitionRebalanceInfo details = detailSet.iterator().next();
      assertThat(details.getBucketCreatesCompleted()).isEqualTo(0);
      assertThat(details.getPrimaryTransfersCompleted()).isEqualTo(0);
      assertThat(details.getBucketTransferBytes()).isGreaterThan(0);
      assertThat(details.getBucketTransfersCompleted()).isEqualTo(3);

      Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
      assertThat(afterDetails).hasSize(2);

      for (PartitionMemberInfo memberDetails : afterDetails) {
        assertThat(memberDetails.getBucketCount()).isEqualTo(3);
        assertThat(memberDetails.getPrimaryCount()).isEqualTo(3);
      }

      if (!simulate) {
        validateStatistics(manager, results);
      }
    });

    if (!simulate) {
      for (VM vm : toArray(vm0, vm1)) {
        vm.invoke(() -> {
          Region region = getCache().getRegion("region1");

          PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
          assertThat(details.getCreatedBucketCount()).isEqualTo(6);
          assertThat(details.getActualRedundantCopies()).isEqualTo(0);
          assertThat(details.getLowRedundancyBucketCount()).isEqualTo(0);
          assertThat(details.getPartitionMemberInfo()).hasSize(2);

          for (PartitionMemberInfo memberDetails : details.getPartitionMemberInfo()) {
            assertThat(memberDetails.getBucketCount()).isEqualTo(3);
            assertThat(memberDetails.getPrimaryCount()).isEqualTo(3);
          }

          // check to make sure that moving buckets didn't close the cache loader
          NullReturningLoader loader =
              (NullReturningLoader) getCache().getRegion("region1").getAttributes()
                  .getCacheLoader();
          assertThat(loader.isClosed()).isFalse();
        });
      }

      for (VM vm : toArray(vm0, vm1)) {
        vm.invoke(() -> {
          NullReturningLoader loader =
              (NullReturningLoader) getCache().getRegion("region1").getAttributes()
                  .getCacheLoader();
          assertThat(loader.isClosed()).isFalse();

          // check to make sure that closing the PR closes the cache loader
          getCache().getRegion("region1").close();
          assertThat(loader.isClosed()).isTrue();
        });
      }
    }
  }

  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}(simulate={0})")
  public void testBalanceBucketsByCount(boolean simulate) {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    // Cache is closed so loadProbeToRestore does not need to be restored
    vm0.invoke(
        () -> {
          LoadProbe ignored =
              getCache().getInternalResourceManager().setLoadProbe(new BucketCountLoadProbe());
        });

    // Create the region in only 1 VM
    vm0.invoke(() -> createPartitionedRegion("region1", 0, new NullReturningLoader()));

    // Create some buckets with very uneven sizes
    vm0.invoke(() -> {
      Region<Number, Object> region = getCache().getRegion("region1");
      region.put(1, new byte[1024 * 1024]);
      region.put(2, "A");
      region.put(3, "A");
      region.put(4, "A");
      region.put(5, "A");
      region.put(6, "A");
    });

    // Create the region in the other VM (should have no effect)
    vm1.invoke(() -> createPartitionedRegion("region1", 0, new NullReturningLoader()));

    // Now simulate a rebalance
    vm0.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();
      RebalanceResults results = doRebalance(simulate, manager);

      assertThat(results.getTotalBucketCreatesCompleted()).isEqualTo(0);
      assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(0);
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(3);
      assertThat(results.getTotalBucketTransferBytes()).isGreaterThan(0);

      Set<PartitionRebalanceInfo> detailSet = results.getPartitionRebalanceDetails();
      assertThat(detailSet).hasSize(1);

      PartitionRebalanceInfo details = detailSet.iterator().next();
      assertThat(details.getBucketCreatesCompleted()).isEqualTo(0);
      assertThat(details.getPrimaryTransfersCompleted()).isEqualTo(0);
      assertThat(details.getBucketTransferBytes()).isGreaterThan(0);
      assertThat(details.getBucketTransfersCompleted()).isEqualTo(3);

      Set<PartitionMemberInfo> afterDetails = details.getPartitionMemberDetailsAfter();
      assertThat(afterDetails).hasSize(2);

      for (PartitionMemberInfo memberDetails : afterDetails) {
        assertThat(memberDetails.getBucketCount()).isEqualTo(3);
        assertThat(memberDetails.getPrimaryCount()).isEqualTo(3);
      }

      if (!simulate) {
        validateStatistics(manager, results);
      }
    });

    if (!simulate) {
      for (VM vm : toArray(vm0, vm1)) {
        vm.invoke(() -> {
          Region region = getCache().getRegion("region1");

          PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);
          assertThat(details.getCreatedBucketCount()).isEqualTo(6);
          assertThat(details.getActualRedundantCopies()).isEqualTo(0);
          assertThat(details.getLowRedundancyBucketCount()).isEqualTo(0);
          assertThat(details.getPartitionMemberInfo()).hasSize(2);

          for (PartitionMemberInfo memberDetails : details.getPartitionMemberInfo()) {
            assertThat(memberDetails.getBucketCount()).isEqualTo(3);
            assertThat(memberDetails.getPrimaryCount()).isEqualTo(3);
          }

          // check to make sure that moving buckets didn't close the cache loader
          NullReturningLoader loader =
              (NullReturningLoader) getCache().getRegion("region1").getAttributes()
                  .getCacheLoader();
          assertThat(loader.isClosed()).isFalse();
        });
      }
    }
  }

  /**
   * If the bucket image provider's cache is recycled while a bucket is moving during a rebalance,
   * we expect a subsequent rebalance will succeed after the cache is reconstructed.
   */
  @Test
  public void testBucketImageProviderCacheClosesDuringBucketMove() {
    VM server1 = getVM(0);
    VM server2 = getVM(1);

    int redundantCopies = 0;
    String regionName = "region1";

    server1.invoke(() -> createPersistentPartitionedRegion(regionName, getUniqueName(),
        getDiskDirs(), redundantCopies));

    // Do puts before starting the second server so that we guarantee all buckets reside on the
    // first server. This way we ensure a rebalance should have some work to do.
    server1.invoke(() -> {
      doPuts(regionName);
    });

    // Recycling server 2 sets the "recreated" state on the partitioned buckets, which can
    // change the behavior during failed GII handling when receiving bucket images. See
    // AbstractDiskRegion.isRecreated.
    server2.invoke(() -> {
      createPersistentPartitionedRegion(regionName, getUniqueName(), getDiskDirs(),
          redundantCopies);
      getCache().close();
      createPersistentPartitionedRegion(regionName, getUniqueName(), getDiskDirs(),
          redundantCopies);
    });

    server1.invoke(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();

      // This will cause a CacheClosedException to occur during rebalance, specifically
      // when a RequestImageMessage during any bucket GII is received.
      DistributionMessageObserver
          .setInstance(
              new CacheClosingDistributionMessageObserver("_B__" + regionName + "_", getCache()));

      try {
        doRebalance(false, manager);
      } catch (CacheClosedException ex) {
        // The CacheClosingDistributionMessageObserver will cause an expected CacheClosedException
      } finally {
        DistributionMessageObserver.setInstance(null);
      }

      // Rebuild the cache and retry the rebalance. We expect it to succeed because the
      // CacheClosingDistributionMessageObserver has been uninstalled.
      createPersistentPartitionedRegion(regionName, getUniqueName(), getDiskDirs(),
          redundantCopies);
      manager = getCache().getInternalResourceManager();
      RebalanceResults results = doRebalance(false, manager);

      // The rebalance should have done some work since the buckets were imbalanced
      assertThat(results.getTotalBucketTransfersCompleted() > 0).isTrue();
    });
  }

  /**
   * If the bucket image provider is bounced while a bucket is moving during a rebalance,
   * we expect a subsequent rebalance will succeed after member restarts and cache is reconstructed.
   */
  @Test
  public void testBucketImageProviderBouncesDuringBucketMove()
      throws InterruptedException, TimeoutException {
    getBlackboard().initBlackboard();

    // Setting up 3 servers to create the partitioned region on to avoid issues with
    // losing membership quorum. Alternatively we could disable network partition detection
    // when constructing the cache, but this solves the same problem.
    VM server1 = getVM(0);
    VM server2 = getVM(1);
    VM server3 = getVM(2);

    int redundantCopies = 0;
    String regionName = "region1";

    server1.invoke(() -> createPersistentPartitionedRegion(regionName, getUniqueName(),
        getDiskDirs(), redundantCopies));

    // Do puts before starting the second server so that we guarantee all buckets reside on the
    // first server. This way we ensure a rebalance should have some work to do.
    server1.invoke(() -> {
      doPuts(regionName);
    });

    // Recycling server 2 and 3 sets the "recreated" state on the partitioned buckets, which can
    // change the behavior during failed GII handling when receiving bucket images. See
    // AbstractDiskRegion.isRecreated.
    server2.invoke(() -> createPersistentPartitionedRegion(regionName, getUniqueName(),
        getDiskDirs(), redundantCopies));

    server3.invoke(() -> createPersistentPartitionedRegion(regionName, getUniqueName(),
        getDiskDirs(), redundantCopies));

    server2.invoke(() -> {
      getCache().close();
      createPersistentPartitionedRegion(regionName, getUniqueName(), getDiskDirs(),
          redundantCopies);
    });

    server3.invoke(() -> {
      getCache().close();
      createPersistentPartitionedRegion(regionName, getUniqueName(), getDiskDirs(),
          redundantCopies);
    });

    server1.invokeAsync(() -> {
      InternalResourceManager manager = getCache().getInternalResourceManager();

      // This will signal a bounce of the VM upon receving the request for any bucket GII for the
      // test partitioned region
      DistributionMessageObserver
          .setInstance(new SignalBounceOnRequestImageMessageObserver("_B__" + regionName + "_",
              getCache(), getBlackboard()));

      doRebalance(false, manager);
    });

    SignalBounceOnRequestImageMessageObserver.waitThenBounce(getBlackboard(), server1);

    server1.invoke(() -> {
      // Rebuild the cache and retry the rebalance. We expect it to succeed because the
      // SignalBounceOnRequestImageMessageObserver has been uninstalled.
      createPersistentPartitionedRegion(regionName, getUniqueName(), getDiskDirs(),
          redundantCopies);
      InternalResourceManager manager = getCache().getInternalResourceManager();
      RebalanceResults results = doRebalance(false, getCache().getInternalResourceManager());

      // The rebalance should have done some work since the buckets were imbalanced
      assertThat(results.getTotalBucketTransfersCompleted() > 0).isTrue();
    });
  }

  private void createPartitionedRegion(String regionName, EvictionAttributes evictionAttributes) {
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(1);
    partitionAttributesFactory.setRecoveryDelay(-1);
    partitionAttributesFactory.setStartupRecoveryDelay(-1);

    RegionFactory regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.setEvictionAttributes(evictionAttributes);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(regionName);
  }

  private <K, V> void createPartitionedRegion(String regionName, CacheLoader<K, V> cacheLoader) {
    PartitionAttributesFactory<K, V> partitionAttributesFactory =
        new PartitionAttributesFactory<>();
    partitionAttributesFactory.setRedundantCopies(0);
    partitionAttributesFactory.setRecoveryDelay(-1);
    partitionAttributesFactory.setStartupRecoveryDelay(-1);

    RegionFactory<K, V> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
    regionFactory.setCacheLoader(cacheLoader);

    regionFactory.create(regionName);
  }

  private void createPartitionedRegion(String regionName, int redundantCopies) {
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setRecoveryDelay(-1);
    partitionAttributesFactory.setStartupRecoveryDelay(-1);

    RegionFactory regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(regionName);
  }

  /**
   * NOTE: do not replace createPartitionedRegionRunnable with lambda
   * because ResourceObserverAdapter is NOT serializable
   */
  private SerializableRunnableIF createPartitionedRegionRunnable(String regionName,
      int redundantCopies) {
    return new SerializableRunnable() {
      @Override
      public void run() {
        createPartitionedRegion(regionName, redundantCopies);
      }
    };
  }

  private void createPartitionedRegion(String region, int redundantCopies, int localMaxMemory) {
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setLocalMaxMemory(localMaxMemory);
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setRecoveryDelay(-1);
    partitionAttributesFactory.setStartupRecoveryDelay(-1);

    RegionFactory regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(region);
  }

  private <K, V> void createPartitionedRegion(String regionName, int redundantCopies,
      CacheLoader<K, V> cacheLoader) {
    PartitionAttributesFactory<K, V> partitionAttributesFactory =
        new PartitionAttributesFactory<>();
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setRecoveryDelay(-1);
    partitionAttributesFactory.setStartupRecoveryDelay(-1);

    RegionFactory<K, V> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.setCacheLoader(cacheLoader);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(regionName);
  }

  private <K, V> void createPartitionedRegion(String regionName, int redundantCopies,
      int localMaxMemory, CacheLoader<K, V> cacheLoader) {
    PartitionAttributesFactory<K, V> partitionAttributesFactory =
        new PartitionAttributesFactory<>();
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setRecoveryDelay(-1);
    partitionAttributesFactory.setStartupRecoveryDelay(-1);
    partitionAttributesFactory.setLocalMaxMemory(localMaxMemory);

    RegionFactory<K, V> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.setCacheLoader(cacheLoader);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(regionName);
  }

  private void createPartitionedRegion(String region, int localMaxMemory, String colocatedWith) {
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setLocalMaxMemory(localMaxMemory);
    partitionAttributesFactory.setRedundantCopies(1);
    partitionAttributesFactory.setRecoveryDelay(-1);
    partitionAttributesFactory.setStartupRecoveryDelay(-1);
    partitionAttributesFactory.setColocatedWith(colocatedWith);

    RegionFactory regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(region);
  }

  private void createPersistentPartitionedRegion(String regionName, String diskStoreName,
      File[] diskDirs, int redundantCopies) {
    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(diskDirs).create(diskStoreName);

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setRecoveryDelay(-1);
    partitionAttributesFactory.setStartupRecoveryDelay(-1);

    RegionFactory regionFactory = getCache().createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setDiskSynchronous(true);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(regionName);
  }

  private <K, V> void createPersistentPartitionedRegion(String regionName, String diskStoreName,
      File[] diskDirs, CacheLoader<K, V> cacheLoader) {
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
    try {
      DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
      diskStoreFactory.setDiskDirs(diskDirs).setMaxOplogSize(1).create(diskStoreName);

      PartitionAttributesFactory<K, V> partitionAttributesFactory =
          new PartitionAttributesFactory<>();
      partitionAttributesFactory.setRedundantCopies(0);
      partitionAttributesFactory.setRecoveryDelay(-1);
      partitionAttributesFactory.setStartupRecoveryDelay(-1);

      RegionFactory<K, V> regionFactory = getCache().createRegionFactory(PARTITION_PERSISTENT);
      regionFactory.setCacheLoader(cacheLoader);
      regionFactory.setDiskStoreName(diskStoreName);
      regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

      regionFactory.create(regionName);
    } finally {
      System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");
    }
  }

  private void createAccessor(String regionName, String diskStoreName) {
    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(getDiskDirs()).create(diskStoreName);

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(1);
    partitionAttributesFactory.setRecoveryDelay(-1);
    partitionAttributesFactory.setStartupRecoveryDelay(-1);
    partitionAttributesFactory.setLocalMaxMemory(0);

    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(partitionAttributesFactory.create());

    getCache().createRegion(regionName, attr.create());
  }

  private void createPartitionedRegionAsSubregion(Region parent, String regionName) {
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(1);
    partitionAttributesFactory.setRecoveryDelay(-1);
    partitionAttributesFactory.setStartupRecoveryDelay(-1);

    RegionFactory regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
    regionFactory.createSubregion(parent, regionName);
  }

  private void createTwoRegionsWithParallelRecoveryObserver() {
    ParallelRecoveryObserver ob =
        (ParallelRecoveryObserver) InternalResourceManager.getResourceObserver();

    ob.observeRegion("region1");
    ob.observeRegion("region2");

    createPartitionedRegion("region1", 1);
    createPartitionedRegion("region2", 1);
  }

  private void createPRRegionWithAsyncQueue(int localMaxMemory) {
    // Create an async event listener that doesn't dispatch anything

    // NOTE: "new AsyncEventListener" cannot be converted to a lambda -- only one class is
    // allowed per cluster and the lambda may create a different class name in different VMs.
    getCache().createAsyncEventQueueFactory()
        .setMaximumQueueMemory(1)
        .setParallel(true)
        .create("parallelQueue", new AsyncEventListener() {
          @Override
          public boolean processEvents(List<AsyncEvent> events) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              return false;
            }
            return false;
          }
        });

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(1);
    partitionAttributesFactory.setRecoveryDelay(-1);
    partitionAttributesFactory.setStartupRecoveryDelay(-1);
    partitionAttributesFactory.setLocalMaxMemory(localMaxMemory);

    RegionFactory regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.addAsyncEventQueueId("parallelQueue");
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create("region1");
  }

  private void doPuts(String regionName) {
    Region<Number, String> region = getCache().getRegion(regionName);
    region.put(1, "A");
    region.put(2, "A");
    region.put(3, "A");
    region.put(4, "A");
    region.put(5, "A");
    region.put(6, "A");
  }

  private void setRedundancyZone(String zone) {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "resource.manager.threads", "2");

    Properties props = new Properties();
    props.setProperty(REDUNDANCY_ZONE, zone);

    getCache(props);
  }

  private RebalanceResults doRebalance(boolean simulate, ResourceManager manager)
      throws TimeoutException, InterruptedException {
    return doRebalance(simulate, manager, null, null);
  }

  private RebalanceResults doRebalance(boolean simulate, ResourceManager manager,
      Set<String> includes, Set<String> excludes) throws TimeoutException, InterruptedException {
    RebalanceResults results;
    if (simulate) {
      results = manager.createRebalanceFactory().includeRegions(includes).excludeRegions(excludes)
          .simulate().getResults(TIMEOUT_SECONDS, SECONDS);
    } else {
      results = manager.createRebalanceFactory().includeRegions(includes).excludeRegions(excludes)
          .start().getResults(TIMEOUT_SECONDS, SECONDS);
    }
    assertThat(manager.getRebalanceOperations()).isEmpty();
    return results;
  }

  private Set<Integer> getBucketList(String regionName) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(regionName);
    return new TreeSet<>(region.getDataStore().getAllLocalBucketIds());
  }

  private void waitForBucketList(String regionName, Collection<Integer> expected) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(regionName);

    await().untilAsserted(new WaitCriterion() {

      @Override
      public boolean done() {
        return getBuckets().equals(expected);
      }

      private Set<Integer> getBuckets() {
        return new TreeSet<>(region.getDataStore().getAllLocalBucketIds());
      }

      @Override
      public String description() {
        return "Timeout waiting for buckets to match. Expected " + expected + " but got "
            + getBuckets();
      }
    });
  }

  private void validateRedundancy(String regionName, int expectedCreatedBucketCount,
      int expectedRedundantCopies, int expectedLowRedundancyBucketCount) {
    Region region = getCache().getRegion(regionName);
    PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region);

    assertThat(details).isNotNull();
    assertThat(details.getCreatedBucketCount()).isEqualTo(expectedCreatedBucketCount);
    assertThat(details.getActualRedundantCopies()).isEqualTo(expectedRedundantCopies);
    assertThat(details.getLowRedundancyBucketCount()).isEqualTo(expectedLowRedundancyBucketCount);
  }

  private void validateRedundancyOfTwoRegions(String regionName1, String regionName2,
      int expectedCreatedBucketCount, int expectedRedundantCopies,
      int expectedLowRedundancyBucketCount) {
    Region region1 = getCache().getRegion(regionName1);
    Region region2 = getCache().getRegion(regionName2);

    validateRedundancyOfTwoRegions(region1, region2, expectedCreatedBucketCount,
        expectedRedundantCopies, expectedLowRedundancyBucketCount);
  }

  private void validateRedundancyOfTwoRegions(Region region1, Region region2,
      int expectedCreatedBucketCount, int expectedRedundantCopies,
      int expectedLowRedundancyBucketCount) {
    PartitionRegionInfo details = PartitionRegionHelper.getPartitionRegionInfo(region1);
    assertThat(details).isNotNull();
    assertThat(details.getCreatedBucketCount()).isEqualTo(expectedCreatedBucketCount);
    assertThat(details.getActualRedundantCopies()).isEqualTo(expectedRedundantCopies);
    assertThat(details.getLowRedundancyBucketCount()).isEqualTo(expectedLowRedundancyBucketCount);

    details = PartitionRegionHelper.getPartitionRegionInfo(region2);
    assertThat(details).isNotNull();
    assertThat(details.getCreatedBucketCount()).isEqualTo(expectedCreatedBucketCount);
    assertThat(details.getActualRedundantCopies()).isEqualTo(expectedRedundantCopies);
    assertThat(details.getLowRedundancyBucketCount()).isEqualTo(expectedLowRedundancyBucketCount);
  }

  private void validateBucketCount(String regionName, int numLocalBuckets) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(regionName);

    assertThat(region.getLocalBucketsListTestOnly()).hasSize(numLocalBuckets);
  }

  private void validateStatistics(InternalResourceManager manager, RebalanceResults results) {
    ResourceManagerStats stats = manager.getStats();

    assertThat(stats.getRebalancesInProgress())
        .isEqualTo(0);
    assertThat(stats.getRebalancesCompleted())
        .isEqualTo(1);
    assertThat(stats.getRebalanceBucketCreatesInProgress())
        .isEqualTo(0);
    assertThat(stats.getRebalanceBucketCreatesCompleted())
        .isEqualTo(results.getTotalBucketCreatesCompleted());
    assertThat(stats.getRebalanceBucketCreatesFailed())
        .isEqualTo(0);

    // The time stats may not be exactly the same, because they are not
    // incremented at exactly the same time.
    assertThat(TimeUnit.NANOSECONDS.toMillis(stats.getRebalanceBucketCreateTime()))
        .isCloseTo(results.getTotalBucketCreateTime(), within(2000L));
    assertThat(stats.getRebalanceBucketCreateBytes())
        .isEqualTo(results.getTotalBucketCreateBytes());
    assertThat(stats.getRebalanceBucketTransfersInProgress())
        .isEqualTo(0);
    assertThat(stats.getRebalanceBucketTransfersCompleted())
        .isEqualTo(results.getTotalBucketTransfersCompleted());
    assertThat(stats.getRebalanceBucketTransfersFailed())
        .isEqualTo(0);
    assertThat(TimeUnit.NANOSECONDS.toMillis(stats.getRebalanceBucketTransfersTime()))
        .isCloseTo(results.getTotalBucketTransferTime(), within(2000L));
    assertThat(stats.getRebalanceBucketTransfersBytes())
        .isEqualTo(results.getTotalBucketTransferBytes());
    assertThat(stats.getRebalancePrimaryTransfersInProgress())
        .isEqualTo(0);
    assertThat(stats.getRebalancePrimaryTransfersCompleted())
        .isEqualTo(results.getTotalPrimaryTransfersCompleted());
    assertThat(stats.getRebalancePrimaryTransfersFailed())
        .isEqualTo(0);
    assertThat(TimeUnit.NANOSECONDS.toMillis(stats.getRebalancePrimaryTransferTime()))
        .isCloseTo(results.getTotalPrimaryTransferTime(), within(2000L));
  }

  @FunctionalInterface
  private interface Operation {

    void execute(Region region, Integer key);
  }

  private enum OperationEnum implements Operation {
    PUT((region, key) -> region.put(key, "B")),
    INVALIDATE((region, key) -> region.invalidate(key)),
    DESTROY((region, key) -> region.destroy(key)),
    CACHE_LOADER((region, key) -> {
      PartitionedRegion partitionedRegion = (PartitionedRegion) region;
      // get a key that doesn't exist, but is in the same bucket as the given key
      region.get(key + partitionedRegion.getPartitionAttributes().getTotalNumBuckets());
    });

    private final Operation delegate;

    OperationEnum(Operation delegate) {
      this.delegate = delegate;
    }

    @Override
    public void execute(Region region, Integer key) {
      delegate.execute(region, key);
    }
  }

  private static class ParallelRecoveryObserver extends ResourceObserverAdapter {

    private final Set<String> regions = new HashSet<>();
    private final CyclicBarrier barrier;
    private volatile boolean observerCalled;

    private ParallelRecoveryObserver(int numRegions) {
      barrier = new CyclicBarrier(numRegions);
    }

    private void observeRegion(String region) {
      regions.add(region);
    }

    private void checkAllRegionRecoveryOrRebalanceStarted(String rn) {
      if (regions.contains(rn)) {
        try {
          barrier.await(TIMEOUT_SECONDS, SECONDS);
        } catch (BrokenBarrierException | InterruptedException | TimeoutException e) {
          throw new RuntimeException("failed waiting for barrier", e);
        }
        observerCalled = true;
      } else {
        throw new RuntimeException("region not registered " + rn);
      }
    }

    private boolean isObserverCalled() {
      return observerCalled;
    }

    @Override
    public void rebalancingStarted(Region region) {
      super.rebalancingStarted(region);
      checkAllRegionRecoveryOrRebalanceStarted(region.getName());
    }

    @Override
    public void recoveryStarted(Region region) {
      super.recoveryStarted(region);
      checkAllRegionRecoveryOrRebalanceStarted(region.getName());
    }
  }

  private static class BlockingCacheListener<K, V> extends CacheListenerAdapter<K, V> {

    private final CountDownLatch operationStartedLatch;
    private final CountDownLatch resumeOperationLatch = new CountDownLatch(1);

    private BlockingCacheListener(int threads) {
      operationStartedLatch = new CountDownLatch(threads);
    }

    private void waitForOperationsStarted() throws InterruptedException {
      operationStartedLatch.await(TIMEOUT_SECONDS, SECONDS);

    }

    private void doWait() {
      operationStartedLatch.countDown();
      try {
        resumeOperationLatch.await(TIMEOUT_SECONDS, SECONDS);
      } catch (InterruptedException e) {
        throw new CacheWriterException(e);
      }
    }

    @Override
    public void afterUpdate(EntryEvent event) throws CacheWriterException {
      doWait();
    }

    @Override
    public void afterCreate(EntryEvent event) {
      doWait();
    }

    @Override
    public void afterDestroy(EntryEvent event) {
      doWait();
    }

    @Override
    public void afterInvalidate(EntryEvent event) {
      doWait();
    }

    public void release() {
      resumeOperationLatch.countDown();
    }
  }

  private static class NullReturningLoader<K, V> implements CacheLoader<K, V> {

    private volatile boolean closed;

    @Override
    public V load(LoaderHelper helper) throws CacheLoaderException {
      return null;
    }

    public boolean isClosed() {
      return closed;
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
