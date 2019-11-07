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
package org.apache.geode.internal.cache.partitioned;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.cache.partition.PartitionRegionHelper.getPartitionRegionInfo;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.DistributedTestUtils.getLocators;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getCurrentVMNum;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.partition.PartitionRegionInfo;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.ColocationLogger;
import org.apache.geode.internal.cache.InitialImageOperation.RequestImageMessage;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserver;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category(RegionsTest.class)
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class PersistentColocatedPartitionedRegionDistributedTest implements Serializable {

  private static final long TIMEOUT_MILLIS = GeodeAwaitility.getTimeout().getValueInMS();

  private static final long DEFAULT_RECOVERY_DELAY = -1;
  private static final int DEFAULT_REDUNDANT_COPIES = 0;
  private static final long DEFAULT_STARTUP_RECOVERY_DELAY = 0;

  private static final String PATTERN_FOR_MISSING_CHILD_LOG =
      "(?s)Persistent data recovery for region .*is prevented by offline colocated region.*";

  private static final int NUM_BUCKETS = 15;

  private static volatile InternalCache cache;
  private static volatile CountDownLatch latch;

  private final transient List<AsyncInvocation<Void>> asyncInvocations = new ArrayList<>();

  private String locators;
  private String regionName;
  private String childRegionName1;
  private String childRegionName2;
  private String diskStoreName1;
  private String diskStoreName2;

  private VM vm0;
  private VM vm1;
  private VM vm2;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() {
    locators = getLocators();
    regionName = getClass().getSimpleName() + "_region";
    childRegionName1 = "region2";
    childRegionName2 = "region3";
    diskStoreName1 = "disk1";
    diskStoreName2 = "disk2";

    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
  }

  @After
  public void tearDown() {
    for (VM vm : toArray(vm0, vm1, vm2, getController())) {
      vm.invoke(() -> {
        ColocationLogger.testhookResetLogInterval();
        DistributionMessageObserver.setInstance(null);
        tearDownPartitionedRegionObserver();

        while (latch != null && latch.getCount() > 0) {
          latch.countDown();
        }

        closeCache();
      });
    }
    disconnectAllFromDS();
  }

  /**
   * Testing that we can colocate persistent PRs
   */
  @Test
  public void testColocatedPRs() throws Exception {
    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY,
            DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
            DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR(regionName, childRegionName2, DEFAULT_RECOVERY_DELAY,
            DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "b");
      createData(childRegionName2, "c");
    });

    Map<Integer, Set<Integer>> bucketIdsInVM = new HashMap<>();
    for (VM vm : toArray(vm0, vm1, vm2)) {
      bucketIdsInVM.put(vm.getId(), vm.invoke(() -> {
        Set<Integer> bucketIds = getBucketIds(regionName);
        assertThat(getBucketIds(childRegionName1)).isEqualTo(bucketIds);
        assertThat(getBucketIds(childRegionName2)).isEqualTo(bucketIds);
        return bucketIds;
      }));
    }

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> closeCache());
    }

    for (VM vm : toArray(vm0, vm1, vm2)) {
      addAsync(vm.invokeAsync(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY,
            DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
            DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR(regionName, childRegionName2, DEFAULT_RECOVERY_DELAY,
            DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
      }));
    }
    awaitAllAsync();

    // The secondary buckets can be recovered asynchronously, so wait for them to come back.
    for (VM vm : toArray(vm0, vm1)) {
      Set<Integer> bucketIds = bucketIdsInVM.get(vm.getId());
      vm.invoke(() -> {
        waitForBuckets(regionName, bucketIds);
        waitForBuckets(childRegionName1, bucketIds);
      });
    }

    vm0.invoke(() -> {
      validateData(regionName, "a");
      validateData(childRegionName1, "b");

      // region 3 didn't have persistent data, so it nothing should be recovered
      validateData(childRegionName2, null);
      // Make sure can do a put in all of the buckets in region 3
      createData(childRegionName2, "c");
      // Now all of those buckets should exist.
      validateData(childRegionName2, "c");
    });

    // The region 3 buckets should be restored in the appropriate places.
    for (VM vm : toArray(vm0, vm1, vm2)) {
      Set<Integer> bucketIds = bucketIdsInVM.get(vm.getId());
      assertThat(vm.invoke(() -> getBucketIds(childRegionName2))).isEqualTo(bucketIds);
    }
  }

  /**
   * Testing that missing colocated persistent PRs are logged as warning
   */
  @Test
  public void testMissingColocatedParentPR() {
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY,
            DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
            DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "b");
    });

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        Set<Integer> bucketIds = getBucketIds(regionName);
        assertThat(bucketIds).isNotEmpty();
        assertThat(getBucketIds(childRegionName1)).isEqualTo(bucketIds);
      });
    }

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> closeCache());
    }

    // The following should fail immediately with ISE on vm0,
    // it's not necessary to also try the operation on vm1.
    vm0.invoke("createPRsMissingParentRegion", () -> {
      createCache();

      Throwable thrown =
          catchThrowable(
              () -> createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
                  DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES,
                  DEFAULT_STARTUP_RECOVERY_DELAY));

      assertThat(thrown)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Region specified in 'colocated-with'");
    });
  }

  /**
   * Testing that parent colocated persistent PRs only missing on local member throws exception
   */
  @Test
  public void testMissingColocatedParentPRWherePRConfigExists() throws Exception {
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY,
            DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
            DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "b");
    });

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        Set<Integer> bucketIds = getBucketIds(regionName);
        assertThat(bucketIds).isNotEmpty();
        assertThat(getBucketIds(childRegionName1)).isEqualTo(bucketIds);
      });
    }

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> closeCache());
    }

    vm1.invoke(() -> {
      latch = new CountDownLatch(1);
    });

    AsyncInvocation createPRsInVM0 = vm0.invokeAsync(() -> {
      createCache();
      createDiskStore(diskStoreName1);
      createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY,
          DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);

      vm1.invoke(() -> latch.countDown());

      createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
          DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
    });

    vm1.invoke(() -> {
      // this thread delays the attempted creation on the local member of colocated child region
      // when parent doesn't exist. The delay is so that both parent and child regions will be
      // created on another member and the PR root config will have an entry for the parent region.
      createCache();
      createDiskStore(diskStoreName1);

      latch.await(TIMEOUT_MILLIS, MILLISECONDS);

      Throwable thrown =
          catchThrowable(
              () -> createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
                  DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES,
                  DEFAULT_STARTUP_RECOVERY_DELAY));

      assertThat(thrown)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageMatching("Region specified in 'colocated-with'.*");

      createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY,
          DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
      createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
          DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
    });

    createPRsInVM0.await();
  }

  /**
   * Testing that missing colocated child persistent PRs are logged as warning
   */
  @Test
  public void testMissingColocatedChildPRDueToDelayedStart() throws Exception {
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY,
            DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
            DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "b");
    });

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        Set<Integer> bucketIds = getBucketIds(regionName);
        assertThat(bucketIds).isNotEmpty();
        assertThat(getBucketIds(childRegionName1)).isEqualTo(bucketIds);
      });
    }

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> closeCache());
    }

    int colocationLoggerIntervalMillis = 1000;
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        ColocationLogger.testhookSetLogInterval(colocationLoggerIntervalMillis);
      });
    }

    AsyncInvocation<String> createMissingChildPRInVM0 =
        vm0.invokeAsync(() -> createMissingChildPR(1));
    AsyncInvocation<String> createMissingChildPRInVM1 =
        vm1.invokeAsync(() -> createMissingChildPR(1));

    createMissingChildPRInVM0.await();

    String colocationLogMessage = createMissingChildPRInVM1.get();
    assertThat(colocationLogMessage).matches(PATTERN_FOR_MISSING_CHILD_LOG);
  }

  /**
   * Testing that missing colocated child persistent PRs are logged as warning
   */
  @Test
  public void testMissingColocatedChildPR() throws Exception {
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY,
            DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
            DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "b");
    });

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        Set<Integer> bucketIds = getBucketIds(regionName);
        assertThat(bucketIds).isNotEmpty();
        assertThat(getBucketIds(childRegionName1)).isEqualTo(bucketIds);
      });
    }

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> closeCache());
    }

    int colocationLoggerIntervalMillis = 1000;
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        ColocationLogger.testhookSetLogInterval(colocationLoggerIntervalMillis);
      });
    }

    AsyncInvocation<String> createPRWithMissingChildInVM0 =
        vm0.invokeAsync(() -> createPRWithMissingChild(2));
    AsyncInvocation<String> createPRWithMissingChildInVM1 =
        vm1.invokeAsync(() -> createPRWithMissingChild(2));

    createPRWithMissingChildInVM0.await();

    String colocationLogMessage = createPRWithMissingChildInVM1.get();
    assertThat(colocationLogMessage).matches(PATTERN_FOR_MISSING_CHILD_LOG);
  }

  /**
   * Test that when there is more than one missing colocated child persistent PRs for a region all
   * missing regions are logged in the warning.
   */
  @Test
  public void testMultipleColocatedChildPRsMissing() throws Exception {
    int childPRCount = 2;

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY,
            DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);

        for (int i = 2; i < childPRCount + 2; ++i) {
          createChildPR_withPersistence(regionName, "region" + i, diskStoreName1,
              DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES,
              DEFAULT_STARTUP_RECOVERY_DELAY);
        }
      });
    }

    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "b");
      createData(childRegionName1, "c");
    });

    Map<Integer, Set<Integer>> bucketIdsInVM = new HashMap<>();
    for (VM vm : toArray(vm0, vm1)) {
      bucketIdsInVM.put(vm.getId(), vm.invoke(() -> {
        Set<Integer> bucketIds = getBucketIds(regionName);
        assertThat(bucketIds).isNotEmpty();
        return bucketIds;
      }));
    }

    for (int i = 2; i < childPRCount + 2; ++i) {
      String childRegionName = "region" + i;
      for (VM vm : toArray(vm0, vm1)) {
        Set<Integer> bucketIds = bucketIdsInVM.get(vm.getId());
        assertThat(vm.invoke(() -> getBucketIds(childRegionName))).isEqualTo(bucketIds);
      }
    }

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> closeCache());
    }

    int colocationLoggerIntervalMillis = 1000;
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        ColocationLogger.testhookSetLogInterval(colocationLoggerIntervalMillis);
      });
    }

    AsyncInvocation<String> createPRWithMissingChildInVM0 =
        vm0.invokeAsync(() -> createPRWithMissingChild(2));
    AsyncInvocation<String> createPRWithMissingChildInVM1 =
        vm1.invokeAsync(() -> createPRWithMissingChild(2));

    createPRWithMissingChildInVM0.await();

    String colocationLogMessage = createPRWithMissingChildInVM1.get();
    assertThat(colocationLogMessage).matches(PATTERN_FOR_MISSING_CHILD_LOG);
  }

  /**
   * Test that when there is more than one missing colocated child persistent PRs for a region all
   * missing regions are logged in the warning. Verifies that as regions are created they no longer
   * appear in the warning.
   */
  @Test
  public void testMultipleColocatedChildPRsMissingWithSequencedStart() throws Exception {
    int childPRCount = 2;

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY,
            DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);

        for (int i = 2; i < childPRCount + 2; ++i) {
          createChildPR_withPersistence(regionName, "region" + i, diskStoreName1,
              DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES,
              DEFAULT_STARTUP_RECOVERY_DELAY);
        }
      });
    }

    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "b");
      createData(childRegionName1, "c");
    });

    Map<Integer, Set<Integer>> bucketIdsInVM = new HashMap<>();
    for (VM vm : toArray(vm0, vm1)) {
      bucketIdsInVM.put(vm.getId(), vm.invoke(() -> {
        Set<Integer> bucketIds = getBucketIds(regionName);
        assertThat(bucketIds).isNotEmpty();
        return bucketIds;
      }));
    }

    for (int i = 2; i < childPRCount + 2; ++i) {
      String childRegionName = "region" + i;
      for (VM vm : toArray(vm0, vm1)) {
        Set<Integer> bucketIds = bucketIdsInVM.get(vm.getId());
        assertThat(vm.invoke(() -> getBucketIds(childRegionName))).isEqualTo(bucketIds);
      }
    }

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> closeCache());
    }

    int colocationLoggerIntervalMillis = 1000;
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        ColocationLogger.testhookSetLogInterval(colocationLoggerIntervalMillis);
      });
    }

    int expectedLogMessagesCount = 2;

    AsyncInvocation<String> createMultipleChildPRGenerationsInVM0 =
        vm0.invokeAsync(
            () -> createMultipleChildPRGenerations(childPRCount, expectedLogMessagesCount));
    AsyncInvocation<String> createMultipleChildPRGenerationsInVM1 =
        vm1.invokeAsync(
            () -> createMultipleChildPRGenerations(childPRCount, expectedLogMessagesCount));

    createMultipleChildPRGenerationsInVM0.await();

    String colocationLogMessage = createMultipleChildPRGenerationsInVM1.get();
    assertThat(colocationLogMessage).matches(PATTERN_FOR_MISSING_CHILD_LOG);
  }

  /**
   * Testing that all missing persistent PRs in a colocation hierarchy are logged as warnings
   */
  @Test
  public void testHierarchyOfColocatedChildPRsMissing() throws Exception {
    int childPRGenerations = 2;

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY,
            DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
            DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);

        for (int i = 3; i < childPRGenerations + 2; ++i) {
          createChildPR_withPersistence("region" + (i - 1), "region" + i, diskStoreName1,
              DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        }
      });
    }

    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "b");
      createData(childRegionName2, "c");
    });

    Map<Integer, Set<Integer>> bucketIdsInVM = new HashMap<>();
    for (VM vm : toArray(vm0, vm1)) {
      bucketIdsInVM.put(vm.getId(), vm.invoke(() -> {
        Set<Integer> bucketIds = getBucketIds(regionName);
        assertThat(bucketIds).isNotEmpty();
        return bucketIds;
      }));
    }

    for (int i = 2; i < childPRGenerations + 2; ++i) {
      String childRegionName = "region" + i;
      for (VM vm : toArray(vm0, vm1)) {
        Set<Integer> bucketIds = bucketIdsInVM.get(vm.getId());
        assertThat(vm.invoke(() -> getBucketIds(childRegionName))).isEqualTo(bucketIds);
      }
    }

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> closeCache());
    }

    int colocationLoggerIntervalMillis = 1000;
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        ColocationLogger.testhookSetLogInterval(colocationLoggerIntervalMillis);
      });
    }

    // Expected warning logs only on the child region, because without the child there's nothing
    // known about the remaining hierarchy

    AsyncInvocation<String> createPRWithMissingChildInVM0 =
        vm0.invokeAsync(() -> createPRWithMissingChild(childPRGenerations));
    AsyncInvocation<String> createPRWithMissingChildInVM1 =
        vm1.invokeAsync(() -> createPRWithMissingChild(childPRGenerations));

    createPRWithMissingChildInVM0.await();

    String colocationLogMessage = createPRWithMissingChildInVM1.get();
    assertThat(colocationLogMessage).matches(PATTERN_FOR_MISSING_CHILD_LOG);
  }

  /**
   * Testing that all missing persistent PRs in a colocation hierarchy are logged as warnings
   */
  @Test
  public void testHierarchyOfColocatedChildPRsMissingGrandchild() throws Exception {
    int childPRGenerationsCount = 3;

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY,
            DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
            DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);

        for (int i = 3; i < childPRGenerationsCount + 2; ++i) {
          createChildPR_withPersistence("region" + (i - 1), "region" + i, diskStoreName1,
              DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        }
      });
    }

    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "b");
      createData(childRegionName2, "c");
    });

    Map<Integer, Set<Integer>> bucketIdsInVM = new HashMap<>();
    for (VM vm : toArray(vm0, vm1)) {
      bucketIdsInVM.put(vm.getId(), vm.invoke(() -> {
        Set<Integer> bucketIds = getBucketIds(regionName);
        assertThat(bucketIds).isNotEmpty();
        return bucketIds;
      }));
    }

    for (int i = 2; i < childPRGenerationsCount + 2; ++i) {
      String childRegionName = "region" + i;
      for (VM vm : toArray(vm0, vm1)) {
        Set<Integer> bucketIds = bucketIdsInVM.get(vm.getId());
        assertThat(vm.invoke(() -> getBucketIds(childRegionName))).isEqualTo(bucketIds);
      }
    }

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> closeCache());
    }

    int colocationLoggerIntervalMillis = 1000;
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        ColocationLogger.testhookSetLogInterval(colocationLoggerIntervalMillis);
      });
    }

    // Expected warning logs only on the child region, because without the child
    // there's nothing known about the remaining hierarchy
    int expectedLogMessagesCount = childPRGenerationsCount * (childPRGenerationsCount + 1) / 2;

    AsyncInvocation<String> createChildPRGenerationsInVM0 = vm0.invokeAsync(
        () -> createChildPRGenerations(childPRGenerationsCount, expectedLogMessagesCount));
    AsyncInvocation<String> createChildPRGenerationsInVM1 = vm1.invokeAsync(
        () -> createChildPRGenerations(childPRGenerationsCount, expectedLogMessagesCount));

    createChildPRGenerationsInVM0.await();

    String colocationLogMessage = createChildPRGenerationsInVM1.get();
    assertThat(colocationLogMessage).matches(PATTERN_FOR_MISSING_CHILD_LOG);
  }

  /**
   * Testing that all missing persistent PRs in a colocation tree hierarchy are logged as warnings.
   * This test is a combines the "multiple children" and "hierarchy of children" tests. This is the
   * colocation tree for this test
   *
   * <pre>
   *                  Parent
   *                /         \
   *             /               \
   *         Gen1_C1            Gen1_C2
   *         /    \              /    \
   *  Gen2_C1_1  Gen2_C1_2  Gen2_C2_1  Gen2_C2_2
   * </pre>
   */
  @Test
  public void testFullTreeOfColocatedChildPRsWithMissingRegions() throws Exception {
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence("Parent", diskStoreName1, DEFAULT_RECOVERY_DELAY,
            DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence("Parent", "Gen1_C1", diskStoreName1, DEFAULT_RECOVERY_DELAY,
            DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence("Parent", "Gen1_C2", diskStoreName1, DEFAULT_RECOVERY_DELAY,
            DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence("Gen1_C1", "Gen2_C1_1", diskStoreName1,
            DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence("Gen1_C1", "Gen2_C1_2", diskStoreName1,
            DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence("Gen1_C2", "Gen2_C2_1", diskStoreName1,
            DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence("Gen1_C2", "Gen2_C2_2", diskStoreName1,
            DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    vm0.invoke(() -> {
      createData("Parent", "a");
      createData("Gen1_C1", "b");
      createData("Gen1_C2", "c");
      createData("Gen2_C1_1", "c");
      createData("Gen2_C1_2", "c");
      createData("Gen2_C2_1", "c");
      createData("Gen2_C2_2", "c");
    });

    Map<Integer, Set<Integer>> bucketIdsInVM = new HashMap<>();
    for (VM vm : toArray(vm0, vm1)) {
      bucketIdsInVM.put(vm.getId(), vm.invoke(() -> {
        Set<Integer> bucketIds = getBucketIds("Parent");
        assertThat(bucketIds).isNotEmpty();
        return bucketIds;
      }));
    }

    for (String region : asList("Gen1_C1", "Gen1_C2", "Gen2_C1_1", "Gen2_C1_2", "Gen2_C2_1",
        "Gen2_C2_2")) {
      for (VM vm : toArray(vm0, vm1)) {
        Set<Integer> bucketIds = bucketIdsInVM.get(vm.getId());
        assertThat(vm.invoke(() -> getBucketIds(region))).isEqualTo(bucketIds);
      }
    }

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> closeCache());
    }

    int colocationLoggerIntervalMillis = 1000;
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        ColocationLogger.testhookSetLogInterval(colocationLoggerIntervalMillis);
      });
    }

    AsyncInvocation<String> createChildPRTreeInVM0 = vm0.invokeAsync(() -> createChildPRTree());
    AsyncInvocation<String> createChildPRTreeInVM1 = vm1.invokeAsync(() -> createChildPRTree());

    createChildPRTreeInVM0.await();

    String colocationLogMessage = createChildPRTreeInVM1.get();
    assertThat(colocationLogMessage).matches(PATTERN_FOR_MISSING_CHILD_LOG);
  }

  /**
   * Testing what happens we we recreate colocated persistent PRs by creating one PR everywhere and
   * then the other PR everywhere.
   */
  @Test
  public void testColocatedPRsRecoveryOnePRAtATime() throws Exception {
    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY, 1,
            DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> createChildPR_withRecovery(regionName, childRegionName1,
          DEFAULT_RECOVERY_DELAY, 1, DEFAULT_STARTUP_RECOVERY_DELAY));
    }

    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "b");
    });

    Map<Integer, Set<Integer>> bucketIdsInVM = new HashMap<>();
    for (VM vm : toArray(vm0, vm1, vm2)) {
      bucketIdsInVM.put(vm.getId(), vm.invoke(() -> {
        Set<Integer> bucketIds = getBucketIds(regionName);
        assertThat(bucketIds).isNotEmpty();
        return bucketIds;
      }));
    }

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        Set<Integer> primaryBucketIds = getPrimaryBucketIds(regionName);
        assertThat(getPrimaryBucketIds(childRegionName1)).isEqualTo(primaryBucketIds);
      });
    }

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> closeCache());
    }

    for (VM vm : toArray(vm0, vm1, vm2)) {
      addAsync(
          vm.invokeAsync(() -> {
            createCache();
            createDiskStore(diskStoreName1);
            createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY, 1,
                DEFAULT_STARTUP_RECOVERY_DELAY);
          }));
    }
    awaitAllAsync();

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> createChildPR_withRecovery(regionName, childRegionName1,
          DEFAULT_RECOVERY_DELAY, 1, DEFAULT_STARTUP_RECOVERY_DELAY));
    }

    for (VM vm : toArray(vm0, vm1, vm2)) {
      Set<Integer> bucketIds = bucketIdsInVM.get(vm.getId());
      vm.invoke(() -> {
        assertThat(getBucketIds(regionName)).isEqualTo(bucketIds);
        assertThat(getBucketIds(childRegionName1)).isEqualTo(bucketIds);
      });
    }

    // primary can differ
    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        Set<Integer> primaryBucketIds = getPrimaryBucketIds(regionName);
        assertThat(getPrimaryBucketIds(childRegionName1)).isEqualTo(primaryBucketIds);
      });
    }

    vm0.invoke(() -> {
      validateData(regionName, "a");
      // region 2 didn't have persistent data, so it nothing should be recovered
      validateData(childRegionName1, null);
      // Make sure can do a put in all of the buckets in vm2
      createData(childRegionName1, "c");
      // Now all of those buckets should exist
      validateData(childRegionName1, "c");
    });

    // Now all the buckets should be restored in the appropriate places.
    for (VM vm : toArray(vm0, vm1, vm2)) {
      Set<Integer> bucketIds = bucketIdsInVM.get(vm.getId());
      vm.invoke(() -> {
        assertThat(getBucketIds(childRegionName1)).isEqualTo(bucketIds);
      });
    }
  }

  @Test
  public void testColocatedPRsRecoveryOneMemberLater() throws Exception {
    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY, 1,
            DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> createChildPR_withRecovery(regionName, childRegionName1,
          DEFAULT_RECOVERY_DELAY, 1, DEFAULT_STARTUP_RECOVERY_DELAY));
    }

    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "b");
    });

    Map<Integer, Set<Integer>> bucketIdsInVM = new HashMap<>();
    for (VM vm : toArray(vm0, vm1, vm2)) {
      bucketIdsInVM.put(vm.getId(), vm.invoke(() -> {
        Set<Integer> bucketIds = getBucketIds(regionName);
        assertThat(getBucketIds(childRegionName1)).isEqualTo(bucketIds);
        return bucketIds;
      }));
    }

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        Set<Integer> primaryBucketIds = getPrimaryBucketIds(regionName);
        assertThat(getPrimaryBucketIds(childRegionName1)).isEqualTo(primaryBucketIds);
      });
    }

    vm0.invoke(() -> {
      assertThat(getCache().getDistributionManager().getDistributionManagerIds()).hasSize(4);
    });

    vm2.invoke(() -> closeCache());

    // Make sure the other members notice that vm2 has gone
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        await().untilAsserted(() -> {
          assertThat(getCache().getDistributionManager().getDistributionManagerIds()).hasSize(3);
        });
      });
    }

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> closeCache());
    }

    // Create the members, but don't initialize VM2 yet
    for (VM vm : toArray(vm0, vm1)) {
      addAsync(vm.invokeAsync(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY, 1,
            DEFAULT_STARTUP_RECOVERY_DELAY);
      }));
    }
    awaitAllAsync();

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> createChildPR_withRecovery(regionName, childRegionName1,
          DEFAULT_RECOVERY_DELAY, 1, DEFAULT_STARTUP_RECOVERY_DELAY));
    }

    for (VM vm : toArray(vm0, vm1)) {
      Set<Integer> bucketIds = bucketIdsInVM.get(vm.getId());
      vm.invoke(() -> waitForBucketRecovery(regionName, bucketIds));
    }

    vm0.invoke(() -> {
      validateData(regionName, "a");
      // region 2 didn't have persistent data, so it nothing should be recovered
      validateData(childRegionName1, null);
      // Make sure can do a put in all of the buckets in vm2
      createData(childRegionName1, "c");
      // Now all of those buckets should exist
      validateData(childRegionName1, "c");
    });

    // Now we initialize vm2.
    Set<Integer> bucketIds = bucketIdsInVM.get(vm2.getId());
    vm2.invoke(() -> {
      createCache();
      createDiskStore(diskStoreName1);
      createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY, 1,
          DEFAULT_STARTUP_RECOVERY_DELAY);

      // Make sure vm2 hasn't created any buckets in the parent PR yet
      // We don't want any buckets until the child PR is created
      assertThat(getBucketIds(regionName)).isEmpty();

      createChildPR_withRecovery(regionName, childRegionName1, DEFAULT_RECOVERY_DELAY, 1,
          DEFAULT_STARTUP_RECOVERY_DELAY);

      // Now vm2 should have created all of the appropriate buckets.
      assertThat(getBucketIds(regionName)).isEqualTo(bucketIds);
      assertThat(getBucketIds(childRegionName1)).isEqualTo(bucketIds);
    });

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        Set<Integer> buckets = getPrimaryBucketIds(regionName);
        assertThat(getPrimaryBucketIds(childRegionName1)).isEqualTo(buckets);
      });
    }
  }

  @Test
  public void testReplaceOfflineMemberAndRestart() throws Exception {
    // Create the PR on three members
    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        CountDownLatch recoveryDone = prepareRecovery(2);

        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, 0, 1,
            DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1, 0, 1,
            DEFAULT_STARTUP_RECOVERY_DELAY);

        assertThat(recoveryDone.await(TIMEOUT_MILLIS, MILLISECONDS)).isTrue();
      });
    }

    // Create some buckets.
    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "a");
    });

    // Close one of the members to trigger redundancy recovery.
    vm2.invoke(() -> closeCache());

    vm0.invoke(() -> {
      // Wait until redundancy is recovered.
      waitForRedundancyRecovery(regionName, 1);
      waitForRedundancyRecovery(childRegionName1, 1);

      createData(regionName, "b");
      createData(childRegionName1, "b");
    });

    addIgnoredException(PartitionOfflineException.class);

    // Close the remaining members.
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> closeCache());
    }

    vm2.invoke(() -> {
      latch = new CountDownLatch(2);
    });

    // Recreate the members. Try to make sure that the member with the latest copy of the buckets
    // is the one that decides to throw away it's copy by starting it last.
    for (VM vm : toArray(vm0, vm1)) {
      addAsync(vm.invokeAsync(() -> {
        CountDownLatch recoveryDone = prepareRecovery(2);

        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, 0, 1,
            DEFAULT_STARTUP_RECOVERY_DELAY);

        vm2.invoke(() -> latch.countDown());

        createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1, 0, 1,
            DEFAULT_STARTUP_RECOVERY_DELAY);

        assertThat(recoveryDone.await(TIMEOUT_MILLIS, MILLISECONDS)).isTrue();
      }));
    }
    addAsync(vm2.invokeAsync(() -> {
      latch.await();

      CountDownLatch recoveryDone = prepareRecovery(2);

      createCache();
      createDiskStore(diskStoreName1);
      createPR_withPersistence(regionName, diskStoreName1, 0, 1,
          DEFAULT_STARTUP_RECOVERY_DELAY);
      createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1, 0, 1,
          DEFAULT_STARTUP_RECOVERY_DELAY);

      assertThat(recoveryDone.await(TIMEOUT_MILLIS, MILLISECONDS)).isTrue();
    }));
    awaitAllAsync();

    vm0.invoke(() -> {
      validateData(regionName, "b");
      validateData(childRegionName1, "b");
    });

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        waitForRedundancyRecovery(regionName, 1);
        waitForRedundancyRecovery(childRegionName1, 1);
      });
    }

    // Make sure we don't have any extra buckets after the restart
    int parentRegionBucketCount = vm0.invoke(() -> getBucketIds(regionName).size());
    parentRegionBucketCount += vm1.invoke(() -> getBucketIds(regionName).size());
    parentRegionBucketCount += vm2.invoke(() -> getBucketIds(regionName).size());

    assertThat(parentRegionBucketCount).isEqualTo(2 * NUM_BUCKETS);

    int childRegionBucketCount = vm0.invoke(() -> getBucketIds(childRegionName1).size());
    childRegionBucketCount += vm1.invoke(() -> getBucketIds(childRegionName1).size());
    childRegionBucketCount += vm2.invoke(() -> getBucketIds(childRegionName1).size());

    assertThat(childRegionBucketCount).isEqualTo(2 * NUM_BUCKETS);
  }

  @Test
  public void testReplaceOfflineMemberAndRestart_WithMultipleDiskStores() throws Exception {
    // Create the PR on three members
    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        CountDownLatch recoveryDone = prepareRecovery(2);

        createCache();
        createDiskStore(diskStoreName1);
        createDiskStore(diskStoreName2);
        createPR_withPersistence(regionName, diskStoreName1, 0, 1,
            DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence(regionName, childRegionName1, diskStoreName2, 0, 1,
            DEFAULT_STARTUP_RECOVERY_DELAY);

        assertThat(recoveryDone.await(TIMEOUT_MILLIS, MILLISECONDS)).isTrue();
      });
    }

    // Create some buckets.
    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "a");
    });

    // Close one of the members to trigger redundancy recovery.
    vm2.invoke(() -> closeCache());

    vm0.invoke(() -> {
      // Wait until redundancy is recovered.
      waitForRedundancyRecovery(regionName, 1);
      waitForRedundancyRecovery(childRegionName1, 1);

      createData(regionName, "b");
      createData(childRegionName1, "b");
    });

    addIgnoredException(PartitionOfflineException.class);

    // Close the remaining members.
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> closeCache());
    }

    vm2.invoke(() -> {
      latch = new CountDownLatch(2);
    });

    // Recreate the members. Try to make sure that the member with the latest copy of the buckets
    // is the one that decides to throw away it's copy by starting it last.
    for (VM vm : toArray(vm0, vm1)) {
      addAsync(vm.invokeAsync(() -> {
        CountDownLatch recoveryDone = prepareRecovery(2);

        createCache();
        createDiskStore(diskStoreName1);
        createDiskStore(diskStoreName2);
        createPR_withPersistence(regionName, diskStoreName1, 0, 1,
            DEFAULT_STARTUP_RECOVERY_DELAY);

        vm2.invoke(() -> latch.countDown());

        createChildPR_withPersistence(regionName, childRegionName1, diskStoreName2, 0, 1,
            DEFAULT_STARTUP_RECOVERY_DELAY);

        assertThat(recoveryDone.await(TIMEOUT_MILLIS, MILLISECONDS)).isTrue();
      }));
    }
    addAsync(vm2.invokeAsync(() -> {
      latch.await();

      CountDownLatch recoveryDone = prepareRecovery(2);

      createCache();
      createDiskStore(diskStoreName1);
      createDiskStore(diskStoreName2);
      createPR_withPersistence(regionName, diskStoreName1, 0, 1,
          DEFAULT_STARTUP_RECOVERY_DELAY);
      createChildPR_withPersistence(regionName, childRegionName1, diskStoreName2, 0, 1,
          DEFAULT_STARTUP_RECOVERY_DELAY);

      assertThat(recoveryDone.await(TIMEOUT_MILLIS, MILLISECONDS)).isTrue();
    }));
    awaitAllAsync();

    vm0.invoke(() -> {
      validateData(regionName, "b");
      validateData(childRegionName1, "b");
    });

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        waitForRedundancyRecovery(regionName, 1);
        waitForRedundancyRecovery(childRegionName1, 1);
      });
    }

    // Make sure we don't have any extra buckets after the restart
    int parentRegionBucketCount = vm0.invoke(() -> getBucketIds(regionName).size());
    parentRegionBucketCount += vm1.invoke(() -> getBucketIds(regionName).size());
    parentRegionBucketCount += vm2.invoke(() -> getBucketIds(regionName).size());

    assertThat(parentRegionBucketCount).isEqualTo(2 * NUM_BUCKETS);

    int childRegionBucketCount = vm0.invoke(() -> getBucketIds(childRegionName1).size());
    childRegionBucketCount += vm1.invoke(() -> getBucketIds(childRegionName1).size());
    childRegionBucketCount += vm2.invoke(() -> getBucketIds(childRegionName1).size());

    assertThat(childRegionBucketCount).isEqualTo(2 * NUM_BUCKETS);
  }

  @Test
  public void testReplaceOfflineMemberAndRestartCreateColocatedPRLate() throws Exception {
    addIgnoredException(PartitionOfflineException.class);
    addIgnoredException(RegionDestroyedException.class);

    // Create the PRs on three members
    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, 0, 1, DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> createChildPR_withPersistence_andRecovery(regionName, childRegionName1,
          diskStoreName1, 0, 1, DEFAULT_STARTUP_RECOVERY_DELAY));
    }

    // Create some buckets.
    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "a");
    });

    // Close one of the members to trigger redundancy recovery.
    vm2.invoke(() -> closeCache());

    // Wait until redundancy is recovered.
    vm0.invoke(() -> {
      waitForRedundancyRecovery(regionName, 1);
      waitForRedundancyRecovery(childRegionName1, 1);

      createData(regionName, "b");
      createData(childRegionName1, "b");
    });

    // Close the remaining members.
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> closeCache());
    }

    // Recreate the parent region. Try to make sure that the member with the latest copy of the
    // buckets is the one that decides to throw away it's copy by starting it last.
    for (VM vm : toArray(vm2, vm1, vm0)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, 0, 1,
            DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    // Recreate the child region.
    for (VM vm : toArray(vm2, vm1, vm0)) {
      addAsync(vm.invokeAsync(() -> createChildPR_withPersistence_andRecovery(regionName,
          childRegionName1, diskStoreName1, 0, 1, DEFAULT_STARTUP_RECOVERY_DELAY)));
    }
    awaitAllAsync();

    vm0.invoke(() -> {
      // Validate the data
      validateData(regionName, "b");
      validateData(childRegionName1, "b");

      // Make sure we can actually use the buckets in the child region.
      createData(childRegionName1, "c");

      waitForRedundancyRecovery(regionName, 1);
      waitForRedundancyRecovery(childRegionName1, 1);
    });

    // Make sure we don't have any extra buckets after the restart
    int parentRegionBucketCount = vm0.invoke(() -> getBucketIds(regionName).size());
    parentRegionBucketCount += vm1.invoke(() -> getBucketIds(regionName).size());
    parentRegionBucketCount += vm2.invoke(() -> getBucketIds(regionName).size());

    assertThat(parentRegionBucketCount).isEqualTo(2 * NUM_BUCKETS);

    int childRegionBucketCount = vm0.invoke(() -> getBucketIds(childRegionName1).size());
    childRegionBucketCount += vm1.invoke(() -> getBucketIds(childRegionName1).size());
    childRegionBucketCount += vm2.invoke(() -> getBucketIds(childRegionName1).size());

    assertThat(childRegionBucketCount).isEqualTo(2 * NUM_BUCKETS);
  }

  @Test
  public void testReplaceOfflineMemberAndRestartCreateColocatedPRLate_withMultipleDiskStore()
      throws Exception {
    addIgnoredException(PartitionOfflineException.class);
    addIgnoredException(RegionDestroyedException.class);

    // Create the PRs on three members
    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, 0, 1, DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> createChildPR_withPersistence_andRecovery(regionName, childRegionName1,
          diskStoreName2, 0, 1, DEFAULT_STARTUP_RECOVERY_DELAY));
    }

    // Create some buckets.
    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "a");
    });

    // Close one of the members to trigger redundancy recovery.
    vm2.invoke(() -> closeCache());

    // Wait until redundancy is recovered.
    vm0.invoke(() -> {
      waitForRedundancyRecovery(regionName, 1);
      waitForRedundancyRecovery(childRegionName1, 1);

      createData(regionName, "b");
      createData(childRegionName1, "b");
    });

    // Close the remaining members.
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> closeCache());
    }

    // Recreate the parent region. Try to make sure that the member with the latest copy of the
    // buckets is the one that decides to throw away it's copy by starting it last.
    for (VM vm : toArray(vm2, vm1, vm0)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, 0, 1,
            DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    // Recreate the child region.
    for (VM vm : toArray(vm2, vm1, vm0)) {
      addAsync(vm.invokeAsync(() -> createChildPR_withPersistence_andRecovery(regionName,
          childRegionName1, diskStoreName2, 0, 1, DEFAULT_STARTUP_RECOVERY_DELAY)));
    }
    awaitAllAsync();

    // Validate the data
    vm0.invoke(() -> {
      validateData(regionName, "b");
      validateData(childRegionName1, "b");

      // Make sure we can actually use the buckets in the child region.
      createData(childRegionName1, "c");

      waitForRedundancyRecovery(regionName, 1);
      waitForRedundancyRecovery(childRegionName1, 1);
    });

    // Make sure we don't have any extra buckets after the restart
    int parentRegionBucketCount = vm0.invoke(() -> getBucketIds(regionName).size());
    parentRegionBucketCount += vm1.invoke(() -> getBucketIds(regionName).size());
    parentRegionBucketCount += vm2.invoke(() -> getBucketIds(regionName).size());

    assertThat(parentRegionBucketCount).isEqualTo(2 * NUM_BUCKETS);

    int childRegionBucketCount = vm0.invoke(() -> getBucketIds(childRegionName1).size());
    childRegionBucketCount += vm1.invoke(() -> getBucketIds(childRegionName1).size());
    childRegionBucketCount += vm2.invoke(() -> getBucketIds(childRegionName1).size());

    assertThat(childRegionBucketCount).isEqualTo(2 * NUM_BUCKETS);
  }

  /**
   * Test what happens when we crash in the middle of satisfying redundancy for a colocated bucket.
   */
  @Test
  public void testCrashDuringRedundancySatisfaction() throws Exception {
    vm0.invoke(() -> {
      createCache();
      createDiskStore(diskStoreName1);
      createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY, 1, -1);
      createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
          DEFAULT_RECOVERY_DELAY, 1, -1);

      createData(regionName, "a");
      createData(childRegionName1, "a");
    });

    vm1.invoke(() -> {
      createCache();
      createDiskStore(diskStoreName1);
      createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY, 1, -1);
      createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
          DEFAULT_RECOVERY_DELAY, 1, -1);
    });

    // We shouldn't have created any buckets in vm1 yet.
    vm1.invoke(() -> {
      assertThat(getBucketIds(regionName)).isEmpty();
    });

    // Add an observer that will disconnect before allowing the peer to GII a colocated bucket.
    // This should leave the peer with only the parent bucket
    vm0.invoke(() -> {
      latch = new CountDownLatch(1);

      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
        @Override
        public void beforeProcessMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          if (message instanceof RequestImageMessage) {
            RequestImageMessage requestImageMessage = (RequestImageMessage) message;
            if (requestImageMessage.getRegionPath().contains(regionName) ||
                requestImageMessage.getRegionPath().contains(childRegionName1)) {
              DistributionMessageObserver.setInstance(null);

              latch.countDown();
            }
          }
        }
      });
    });

    AsyncInvocation<Void> disconnectDuringGiiInVm0 = vm0.invokeAsync(() -> {
      latch.await(TIMEOUT_MILLIS, MILLISECONDS);

      closeCache();
    });

    vm1.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(PartitionOfflineException.class)) {
        // Do a rebalance to create buckets in vm1. THis will cause vm0 to disconnect
        // as we satisfy redundancy with vm1.
        Throwable thrown = catchThrowable(() -> {
          getCache().getResourceManager().createRebalanceFactory().start().getResults();
        });
        if (thrown != null) {
          assertThat(thrown).isInstanceOf(PartitionOfflineException.class);
        }
      }
    });

    disconnectDuringGiiInVm0.await();

    // close the cache in vm1
    vm1.invoke(() -> closeCache());

    // Create the cache and PRs on both members
    for (VM vm : toArray(vm0, vm1)) {
      addAsync(vm.invokeAsync(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY, 1, -1);
        createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
            DEFAULT_RECOVERY_DELAY, 1, -1);
      }));
    }
    awaitAllAsync();

    // Make sure the data was recovered correctly
    vm0.invoke(() -> {
      validateData(regionName, "a");
      validateData(childRegionName1, "a");
    });
  }

  @Test
  @Parameters({"disk1", "disk2"})
  @TestCaseName("{method}(childRegionDiskStore={0})")
  public void testRebalanceWithOfflineChildRegion(String childRegionDiskStore) throws Exception {
    // Create the PRs on two members
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, 0, DEFAULT_REDUNDANT_COPIES,
            DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> createChildPR_withPersistence(regionName, childRegionName1,
          childRegionDiskStore, 0, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY));
    }

    // Create some buckets.
    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "a");
    });

    // Close the members
    for (VM vm : toArray(vm1, vm0)) {
      vm.invoke(() -> closeCache());
    }

    // Recreate the parent region. Try to make sure that the member with the latest copy of the
    // buckets is the one that decides to throw away it's copy by starting it last.

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, 0, DEFAULT_REDUNDANT_COPIES,
            DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    // Now create the parent region on vm-2. vm-2 did not previous host the child region.
    vm2.invoke(() -> {
      createCache();
      createDiskStore(diskStoreName1);
      createPR_withPersistence(regionName, diskStoreName1, 0, DEFAULT_REDUNDANT_COPIES,
          DEFAULT_STARTUP_RECOVERY_DELAY);
    });

    // Rebalance the parent region.
    // This should not move any buckets, because we haven't recovered the child region
    vm2.invoke(() -> {
      RebalanceResults rebalanceResults =
          getCache().getResourceManager().createRebalanceFactory().start().getResults();
      assertThat(rebalanceResults.getTotalBucketTransfersCompleted()).isZero();
    });

    // Recreate the child region.
    for (VM vm : toArray(vm0, vm1, vm2)) {
      addAsync(vm.invokeAsync(() -> createChildPR_withPersistence(regionName, childRegionName1,
          childRegionDiskStore, 0, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY)));
    }
    awaitAllAsync();

    vm0.invoke(() -> {
      // Validate the data
      validateData(regionName, "a");
      validateData(childRegionName1, "a");

      // Make sure we can actually use the buckets in the child region.
      createData(childRegionName1, "c");
    });
  }

  /**
   * Test that a rebalance will regions are in the middle of recovery doesn't cause issues.
   *
   * This is slightly different than {@link #testRebalanceWithOfflineChildRegion(boolean)} because
   * in this case all of the regions have been created, but they are in the middle of actually
   * recovering buckets from disk.
   */
  @Test
  public void testRebalanceDuringRecovery() throws Exception {
    // Create the PRs on two members
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY, 1,
            DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
            DEFAULT_RECOVERY_DELAY, 1, DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    // Create some buckets.
    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "a");
    });

    // Close the members
    for (VM vm : toArray(vm1, vm0)) {
      vm.invoke(() -> closeCache());
    }

    vm1.invoke(() -> {
      PartitionedRegionObserverHolder.setInstance(new PRObserver(childRegionName1));
    });
    try {
      for (VM vm : toArray(vm0, vm1)) {
        addAsync(vm.invokeAsync(() -> {
          createCache();
          createDiskStore(diskStoreName1);
          createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY, 1,
              DEFAULT_STARTUP_RECOVERY_DELAY);
          createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
              DEFAULT_RECOVERY_DELAY, 1, DEFAULT_STARTUP_RECOVERY_DELAY);
        }));
      }

      vm1.invoke(() -> {
        PRObserver observer = (PRObserver) PartitionedRegionObserverHolder.getInstance();
        observer.waitForCreate();
      });

      // Now create the parent region on vm-2. vm-2 did not
      // previous host the child region.
      vm2.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY, 1,
            DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
            DEFAULT_RECOVERY_DELAY, 1, DEFAULT_STARTUP_RECOVERY_DELAY);
      });

      // Try to forcibly move some buckets to vm2 (this should not succeed).
      moveBucket(0, vm1, vm2);
      moveBucket(1, vm1, vm2);

    } finally {
      vm1.invoke(() -> {
        PRObserver observer = (PRObserver) PartitionedRegionObserverHolder.getInstance();
        observer.tearDown();
        PartitionedRegionObserverHolder.setInstance(new PartitionedRegionObserverAdapter());
      });
    }

    awaitAllAsync();

    vm0.invoke(() -> {
      // Validate the data
      validateData(regionName, "a");
      validateData(childRegionName1, "a");

      // Make sure we can actually use the buckets in the child region.
      createData(childRegionName1, "c");
    });

    // Make sure the system is recoverable by restarting it

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> closeCache());
    }

    for (VM vm : toArray(vm0, vm1, vm2)) {
      addAsync(vm.invokeAsync(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY, 1,
            DEFAULT_STARTUP_RECOVERY_DELAY);
        createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
            DEFAULT_RECOVERY_DELAY, 1, DEFAULT_STARTUP_RECOVERY_DELAY);
      }));
    }
    awaitAllAsync();
  }

  @Test
  public void testParentRegionGetWithOfflineChildRegion() {
    // Expect a get() on the un-recovered (due to offline child) parent region to fail
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, 0, DEFAULT_REDUNDANT_COPIES,
            DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(
          () -> createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1, 0,
              DEFAULT_REDUNDANT_COPIES,
              DEFAULT_STARTUP_RECOVERY_DELAY));
    }

    // Create some buckets.
    vm0.invoke(() -> {
      createData(regionName, "a");
      createData(childRegionName1, "a");
    });

    // Close the members
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> closeCache());
    }

    // Recreate the parent region. Try to make sure that the member with the latest copy of the
    // buckets is the one that decides to throw away it's copy by starting it last.
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        createCache();
        createDiskStore(diskStoreName1);
        createPR_withPersistence(regionName, diskStoreName1, 0, DEFAULT_REDUNDANT_COPIES,
            DEFAULT_STARTUP_RECOVERY_DELAY);
      });
    }

    // Now create the parent region on vm-2. vm-2 did not previously host the child region.
    vm2.invoke(() -> {
      createCache();
      createDiskStore(diskStoreName1);
      createPR_withPersistence(regionName, diskStoreName1, 0, DEFAULT_REDUNDANT_COPIES,
          DEFAULT_STARTUP_RECOVERY_DELAY);
    });

    vm0.invoke(() -> {
      Region<Integer, String> region = getCache().getRegion(regionName);
      Throwable thrown = catchThrowable(() -> region.get(0));
      assertThat(thrown).isInstanceOf(PartitionOfflineException.class);
    });
  }

  private void createPR_withPersistence(String regionName, String diskStoreName, long recoveryDelay,
      int redundantCopies, long startupRecoveryDelay) {
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRecoveryDelay(recoveryDelay);
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setStartupRecoveryDelay(startupRecoveryDelay);

    RegionFactory regionFactory = getCache().createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(regionName);
  }

  private void createChildPR(String parentRegionName, String childRegionName, long recoveryDelay,
      int redundantCopies, long startupRecoveryDelay) {
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setColocatedWith(parentRegionName);
    partitionAttributesFactory.setRecoveryDelay(recoveryDelay);
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setStartupRecoveryDelay(startupRecoveryDelay);

    RegionFactory regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(childRegionName);
  }

  private void createChildPR_withPersistence(String parentRegionName, String childRegionName,
      String diskStoreName, long recoveryDelay, int redundantCopies, long startupRecoveryDelay) {
    createDiskStore(diskStoreName);

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setColocatedWith(parentRegionName);
    partitionAttributesFactory.setRecoveryDelay(recoveryDelay);
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setStartupRecoveryDelay(startupRecoveryDelay);

    RegionFactory regionFactory = getCache().createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(childRegionName);
  }

  private void createChildPR_withPersistence_andRecovery(String parentRegionName,
      String childRegionName, String diskStoreName, long recoveryDelay, int redundantCopies,
      long startupRecoveryDelay) throws InterruptedException {
    CountDownLatch recoveryDone = prepareRecovery(1, childRegionName1);

    createChildPR_withPersistence(parentRegionName, childRegionName, diskStoreName, recoveryDelay,
        redundantCopies, startupRecoveryDelay);

    assertThat(recoveryDone.await(TIMEOUT_MILLIS, MILLISECONDS)).isTrue();
  }

  private void createChildPR_withRecovery(String parentRegionName, String childRegionName,
      long recoveryDelay, int redundantCopies, long startupRecoveryDelay)
      throws InterruptedException {
    CountDownLatch recoveryDone = prepareRecovery(1, childRegionName1);

    createChildPR(parentRegionName, childRegionName, recoveryDelay, redundantCopies,
        startupRecoveryDelay);

    assertThat(recoveryDone.await(TIMEOUT_MILLIS, MILLISECONDS)).isTrue();
  }

  private CountDownLatch prepareRecovery(int count) {
    return prepareRecovery(count, null);
  }

  private CountDownLatch prepareRecovery(int count, String regionName) {
    CountDownLatch recoveryDone = new CountDownLatch(count);
    ResourceObserver observer = new InternalResourceManager.ResourceObserverAdapter() {
      @Override
      public void recoveryFinished(Region region) {
        if (regionName == null || region.getName().contains(regionName)) {
          recoveryDone.countDown();
        }
      }
    };
    InternalResourceManager.setResourceObserver(observer);
    return recoveryDone;
  }

  private String createPRWithMissingChild(int expectedLogMessagesCount) {
    try (MockAppender mockAppender = new MockAppender(ColocationLogger.class)) {
      createCache();
      createDiskStore(diskStoreName1);
      createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY,
          DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);

      // Let this thread continue running long enough for the missing region to be logged a
      // couple times. Child regions do not get created by this thread.
      await().untilAsserted(() -> {
        assertThat(mockAppender.getLogs()).hasSize(expectedLogMessagesCount);
      });

      return mockAppender.getLogs().get(0).getMessage().getFormattedMessage();
    }
  }

  private String createMissingChildPR(int expectedLogMessagesCount) throws InterruptedException {
    try (MockAppender mockAppender = new MockAppender(ColocationLogger.class)) {
      createCache();
      createDiskStore(diskStoreName1);
      createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY,
          DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);

      // Delay creation of second (i.e child) region to see missing colocated region log
      // message (logInterval/2 < delay < logInterval)
      await().untilAsserted(() -> {
        assertThat(mockAppender.getLogs()).hasSize(expectedLogMessagesCount);
      });

      createChildPR_withPersistence(regionName, childRegionName1, diskStoreName1,
          DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);

      return mockAppender.getLogs().get(0).getMessage().getFormattedMessage();
    }
  }

  private String createChildPRGenerations(int childPRGenerationsCount, int expectedLogMessagesCount)
      throws InterruptedException {
    try (MockAppender mockAppender = new MockAppender(ColocationLogger.class)) {
      createCache();
      createDiskStore(diskStoreName1);
      createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY,
          DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);

      // Delay creation of child generation regions to see missing colocated region log message
      // parent region is generation 1, child region is generation 2, grandchild is 3, etc.
      for (int generation = 2; generation < childPRGenerationsCount + 2; ++generation) {
        String childPRName = "region" + generation;
        String colocatedWithRegionName =
            generation == 2 ? regionName : "region" + (generation - 1);

        // delay between starting generations of child regions until the expected missing
        // colocation messages are logged
        int expectedCount = (generation - 1) * generation / 2;
        await().untilAsserted(() -> {
          assertThat(mockAppender.getLogs()).hasSize(expectedCount);
        });

        // Start the child region
        createChildPR_withPersistence(colocatedWithRegionName, childPRName, diskStoreName1,
            DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
      }

      assertThat(mockAppender.getLogs()).hasSize(expectedLogMessagesCount);

      verify(mockAppender.getAppender(), atLeastOnce()).getName();
      verify(mockAppender.getAppender(), atLeastOnce()).isStarted();

      // Another delay before exiting the thread to make sure that missing region logging
      // doesn't continue after all regions are created (delay > logInterval)
      Thread.sleep(ColocationLogger.getLogInterval() * 2);

      verifyNoMoreInteractions(mockAppender.getAppender());

      return mockAppender.getLogs().get(0).getMessage().getFormattedMessage();
    }
  }

  private String createMultipleChildPRGenerations(int childCount, int expectedLogMessagesCount)
      throws InterruptedException {
    try (MockAppender mockAppender = new MockAppender(ColocationLogger.class)) {
      createCache();
      createDiskStore(diskStoreName1);
      createPR_withPersistence(regionName, diskStoreName1, DEFAULT_RECOVERY_DELAY,
          DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);

      // Delay creation of child generation regions to see missing colocated region log message
      for (int regionCount = 2; regionCount < childCount + 2; ++regionCount) {
        String childPRName = "region" + regionCount;

        // delay between starting generations of child regions until the expected missing
        // colocation messages are logged
        int expectedCount = regionCount - 1;
        await().untilAsserted(() -> {
          assertThat(mockAppender.getLogs()).hasSize(expectedCount);
        });

        assertThat(mockAppender.getLogs().size()).isEqualTo(regionCount - 1);

        // Start the child region
        createChildPR_withPersistence(regionName, childPRName, diskStoreName1,
            DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
      }

      assertThat(mockAppender.getLogs()).hasSize(expectedLogMessagesCount);

      String logMessage = mockAppender.getLogs().get(0).getMessage().getFormattedMessage();

      verify(mockAppender.getAppender(), atLeastOnce()).getName();
      verify(mockAppender.getAppender(), atLeastOnce()).isStarted();

      // Another delay before exiting the thread to make sure that missing region logging
      // doesn't continue after all regions are created (delay > logInterval)
      Thread.sleep(ColocationLogger.getLogInterval() * 2);

      verifyNoMoreInteractions(mockAppender.getAppender());

      return logMessage;
    }
  }

  /**
   * This thread starts up multiple colocated child regions in the sequence defined by
   * {@link #childRegionTreeRestartOrder}. The complete startup sequence, which includes timed
   * periods waiting for log messages, takes at least 28 secs. Tests waiting for this
   * {@link SerializableCallable} to complete must have sufficient overhead in the wait for runtime
   * variations that exceed the minimum time to complete.
   */
  private String createChildPRTree() throws InterruptedException {
    try (MockAppender mockAppender = new MockAppender(ColocationLogger.class)) {

      // Logger interval may have been hooked by the test, so adjust test delays here
      int logInterval = ColocationLogger.getLogInterval();

      createCache();
      createDiskStore(diskStoreName1);
      createPR_withPersistence("Parent", diskStoreName1, DEFAULT_RECOVERY_DELAY,
          DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);

      // Delay creation of descendant regions in the hierarchy to see missing colocated region
      // log messages (logInterval/2 < delay < logInterval)

      int expectedLogMessagesCount = 1;
      ArgumentCaptor<LogEvent> logEventCaptor = ArgumentCaptor.forClass(LogEvent.class);
      Appender appender = mockAppender.getAppender();

      for (Object[] regionInfo : childRegionTreeRestartOrder()) {
        logEventCaptor = ArgumentCaptor.forClass(LogEvent.class);
        String childRegionName = (String) regionInfo[0];
        String parentRegionName = (String) regionInfo[1];

        // delay between starting generations of child regions and verify expected logging
        ArgumentCaptor<LogEvent> loggingEventCaptorRef = logEventCaptor;
        await().untilAsserted(() -> {
          verify(appender, times(expectedLogMessagesCount))
              .append(loggingEventCaptorRef.capture());
        });

        // Finally start the next child region
        createChildPR_withPersistence(parentRegionName, childRegionName, diskStoreName1,
            DEFAULT_RECOVERY_DELAY, DEFAULT_REDUNDANT_COPIES, DEFAULT_STARTUP_RECOVERY_DELAY);
      }

      List<LogEvent> logEvents = logEventCaptor.getAllValues();
      assertThat(logEvents).hasSize(expectedLogMessagesCount);

      String logMessage = logEvents.get(0).getMessage().getFormattedMessage();

      // acknowledge interactions with the mock that have occurred
      verify(appender, atLeastOnce()).getName();
      verify(appender, atLeastOnce()).isStarted();
      verify(appender, atLeastOnce()).append(any(LogEvent.class));

      // Another delay before exiting the thread to make sure that missing region logging
      // doesn't continue after all regions are created (delay > logInterval)
      Thread.sleep(logInterval * 2);

      verifyNoMoreInteractions(appender);

      return logMessage;
    }
  }

  /**
   * The colocation tree has the regions started in a specific order so that the logging is
   * predictable. For each entry in the list, the array values are:
   *
   * <pre>
   *   [0] - the region name
   *   [1] - the name of that region's parent
   *   [2] - the number of warnings that will be logged after the region is created (1 warning for
   *         each region in the tree that exists that still has 1 or more missing children.)
   * </pre>
   */
  private List<Object[]> childRegionTreeRestartOrder() {
    List<Object[]> list = new ArrayList<>();
    list.add(new Object[] {"Gen1_C1", "Parent", 2});
    list.add(new Object[] {"Gen2_C1_1", "Gen1_C1", 2});
    list.add(new Object[] {"Gen1_C2", "Parent", 3});
    list.add(new Object[] {"Gen2_C1_2", "Gen1_C1", 2});
    list.add(new Object[] {"Gen2_C2_1", "Gen1_C2", 2});
    list.add(new Object[] {"Gen2_C2_2", "Gen1_C2", 0});
    return list;
  }

  private void createData(String regionName, String value) {
    Region<Integer, String> region = getCache().getRegion(regionName);
    int startKey = 0;
    int endKey = NUM_BUCKETS;
    for (int i = startKey; i < endKey; i++) {
      region.put(i, value);
    }
  }

  private Set<Integer> getBucketIds(String regionName) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(regionName);
    return new TreeSet<>(region.getDataStore().getAllLocalBucketIds());
  }

  private Set<Integer> getPrimaryBucketIds(String regionName) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(regionName);
    return new TreeSet<>(region.getDataStore().getAllLocalPrimaryBucketIds());
  }

  private void moveBucket(int bucketId, VM sourceVM, VM targetVM) {
    InternalDistributedMember sourceId =
        sourceVM.invoke(() -> getCache().getInternalDistributedSystem().getDistributedMember());

    targetVM.invoke(() -> {
      PartitionedRegion region = (PartitionedRegion) getCache().getRegion(regionName);
      region.getDataStore().moveBucket(bucketId, sourceId, false);
    });
  }

  private void validateData(String regionName, String value) {
    Region region = getCache().getRegion(regionName);
    int startKey = 0;
    int endKey = NUM_BUCKETS;
    for (int i = startKey; i < endKey; i++) {
      assertThat(region.get(i)).isEqualTo(value);
    }
  }

  private void waitForBuckets(String regionName, Set<Integer> expectedBucketIds) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(regionName);

    await().untilAsserted(() -> {
      Set<Integer> allLocalBucketIds = new TreeSet<>(region.getDataStore().getAllLocalBucketIds());
      assertThat(allLocalBucketIds).isEqualTo(expectedBucketIds);
    });
  }

  private void waitForBucketRecovery(String regionName, Set<Integer> lostBucketIds) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(regionName);
    PartitionedRegionDataStore dataStore = region.getDataStore();

    await().untilAsserted(() -> {
      Set<Integer> allLocalBucketIds = dataStore.getAllLocalBucketIds();
      assertThat(lostBucketIds).isEqualTo(allLocalBucketIds);
    });
  }

  private void waitForRedundancyRecovery(String regionName, int expectedRedundancy) {
    Region region = getCache().getRegion(regionName);

    await().untilAsserted(() -> {
      PartitionRegionInfo info = getPartitionRegionInfo(region);
      assertThat(info.getActualRedundantCopies()).isEqualTo(expectedRedundancy);
    });
  }

  private void addAsync(AsyncInvocation<Void> asyncInvocation) {
    asyncInvocations.add(asyncInvocation);
  }

  private void awaitAllAsync() throws ExecutionException, InterruptedException {
    for (AsyncInvocation<Void> asyncInvocation : asyncInvocations) {
      asyncInvocation.await();
    }
    asyncInvocations.clear();
  }

  private void createDiskStore(String diskStoreName) {
    DiskStore diskStore = getCache().findDiskStore(diskStoreName);
    if (diskStore == null) {
      getCache().createDiskStoreFactory().setDiskDirs(getDiskDirs()).create(diskStoreName);
    }
  }

  private void createCache() {
    assertThat(cache).isNull();
    // cache = (InternalCache) new CacheFactory(getDistributedSystemProperties()).create();
    cache = (InternalCache) new CacheFactory().set(LOCATORS, locators).create();
  }

  private InternalCache getCache() {
    return cache;
  }

  private void closeCache() {
    if (cache != null) {
      cache.close();
      cache = null;
    }
  }

  private File getDiskDir() {
    try {
      File file = new File(temporaryFolder.getRoot(), diskStoreName1 + getCurrentVMNum());
      if (!file.exists()) {
        temporaryFolder.newFolder(diskStoreName1 + getCurrentVMNum());
      }
      return file.getAbsoluteFile();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private File[] getDiskDirs() {
    return new File[] {getDiskDir()};
  }

  private void tearDownPartitionedRegionObserver() {
    PartitionedRegionObserver prObserver = PartitionedRegionObserverHolder.getInstance();
    if (prObserver != null) {
      if (prObserver instanceof PRObserver) {
        ((PRObserver) prObserver).tearDown();
      }
      PartitionedRegionObserverHolder.setInstance(new PartitionedRegionObserverAdapter());
    }
  }

  private static class PRObserver extends PartitionedRegionObserverAdapter {

    private final CountDownLatch rebalanceDone = new CountDownLatch(1);
    private final CountDownLatch bucketCreateStarted = new CountDownLatch(3);

    private final String childRegionName;

    PRObserver(String childRegionName) {
      this.childRegionName = childRegionName;
    }

    public void tearDown() {
      bucketCreateStarted.countDown();
      rebalanceDone.countDown();
    }

    @Override
    public void beforeBucketCreation(PartitionedRegion region, int bucketId) {
      if (region.getName().contains(childRegionName)) {
        bucketCreateStarted.countDown();
        waitForRebalance();
      }
    }

    void waitForCreate() throws InterruptedException {
      assertThat(bucketCreateStarted.await(TIMEOUT_MILLIS, TimeUnit.SECONDS))
          .withFailMessage("Failed waiting for bucket creation to start")
          .isTrue();
    }

    private void waitForRebalance() {
      try {
        assertThat(rebalanceDone.await(TIMEOUT_MILLIS, TimeUnit.SECONDS))
            .withFailMessage("Failed waiting for the rebalance to start")
            .isTrue();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
