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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.internal.cache.PartitionedRegionHelper.PR_ROOT_REGION_NAME;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

/**
 * This test is to test and validate the partitioned region creation in multiple vm scenario. This
 * will verify the functionality under distributed scenario.
 */
@SuppressWarnings("serial")
public class PartitionedRegionCreationDUnitTest extends CacheTestCase {

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;
  private String signalRegionName;
  private String partitionedRegionName;
  private int numberOfRegions;
  private String prNamePrefix;

  private transient List<AsyncInvocation> asyncInvocations;

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Before
  public void setUp() throws Exception {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);
    vm3 = getHost(0).getVM(3);

    partitionedRegionName = getUniqueName() + "_PartitionedRegion";
    prNamePrefix = partitionedRegionName + "-";
    signalRegionName = getUniqueName() + "_Signal";
    numberOfRegions = 1;
    asyncInvocations = new ArrayList<>();
  }

  /**
   * This tests creates partition regions with scope = DISTRIBUTED_ACK and then validating thoes
   * partition regions
   */
  @Test
  public void testSequentialCreation() throws Exception {
    for (int i = 0; i < numberOfRegions; i++) {
      final int whichRegion = i;
      vm0.invoke(() -> createPartitionedRegion(prNamePrefix + whichRegion));
      vm1.invoke(() -> createPartitionedRegion(prNamePrefix + whichRegion));
      vm2.invoke(() -> createPartitionedRegion(prNamePrefix + whichRegion));
      vm3.invoke(() -> createPartitionedRegion(prNamePrefix + whichRegion));
    }

    // validating that regions are successfully created
    vm0.invoke(() -> validatePR(prNamePrefix, numberOfRegions));
    vm1.invoke(() -> validatePR(prNamePrefix, numberOfRegions));
    vm2.invoke(() -> validatePR(prNamePrefix, numberOfRegions));
    vm3.invoke(() -> validatePR(prNamePrefix, numberOfRegions));
  }

  /**
   * This test create regions with scope = DISTRIBUTED_NO_ACK and then validating these partition
   * regions
   */
  @Test
  public void testConcurrentCreation() throws Exception {
    invokeAsync(vm0, () -> createPartitionedRegions(prNamePrefix, numberOfRegions));
    invokeAsync(vm1, () -> createPartitionedRegions(prNamePrefix, numberOfRegions));
    invokeAsync(vm2, () -> createPartitionedRegions(prNamePrefix, numberOfRegions));
    invokeAsync(vm3, () -> createPartitionedRegions(prNamePrefix, numberOfRegions));

    awaitAllAsyncInvocations();

    // validating that regions are successfully created
    vm0.invoke(() -> validatePR(prNamePrefix, numberOfRegions));
    vm1.invoke(() -> validatePR(prNamePrefix, numberOfRegions));
    vm2.invoke(() -> validatePR(prNamePrefix, numberOfRegions));
    vm3.invoke(() -> validatePR(prNamePrefix, numberOfRegions));
  }

  /**
   * This test create regions with scope = DISTRIBUTED_NO_ACK and then validating these partition
   * regions. Test specially added for SQL fabric testing since that always creates regions in
   * parallel.
   */
  @Test
  public void testBetterConcurrentCreation() throws Exception {
    numberOfRegions = 3;

    createReplicateRegion(signalRegionName);
    vm0.invoke(() -> createReplicateRegion(signalRegionName));
    vm1.invoke(() -> createReplicateRegion(signalRegionName));
    vm2.invoke(() -> createReplicateRegion(signalRegionName));
    vm3.invoke(() -> createReplicateRegion(signalRegionName));

    CompletableFuture<Void> createdAccessor = executorServiceRule.runAsync(
        () -> createPRAccessorConcurrently(signalRegionName, prNamePrefix, numberOfRegions));

    invokeAsync(vm0, () -> createPRConcurrently(signalRegionName, prNamePrefix, numberOfRegions));
    invokeAsync(vm1, () -> createPRConcurrently(signalRegionName, prNamePrefix, numberOfRegions));
    invokeAsync(vm2, () -> createPRConcurrently(signalRegionName, prNamePrefix, numberOfRegions));
    invokeAsync(vm3, () -> createPRConcurrently(signalRegionName, prNamePrefix, numberOfRegions));

    // do the put to signal start
    Region<String, String> signalRegion = getCache().getRegion(signalRegionName);
    signalRegion.put("start", "true");

    awaitAllAsyncInvocations();
    createdAccessor.get(60, SECONDS);

    // validating that regions are successfully created
    vm0.invoke(() -> validatePR(prNamePrefix, numberOfRegions));
    vm1.invoke(() -> validatePR(prNamePrefix, numberOfRegions));
    vm2.invoke(() -> validatePR(prNamePrefix, numberOfRegions));
    vm3.invoke(() -> validatePR(prNamePrefix, numberOfRegions));
    validatePR(prNamePrefix, numberOfRegions);
  }

  /**
   * Test whether partition region creation is prevented when an instance is created that has the
   * incorrect redundancy
   */
  @Test
  public void testPartitionedRegionRedundancyConflict() throws Exception {
    vm0.invoke(() -> {
      Cache cache = getCache();

      PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
      partitionAttributesFactory.setRedundantCopies(0);

      RegionFactory regionFactory = cache.createRegionFactory(PARTITION);
      regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

      Region region = regionFactory.create(partitionedRegionName);

      assertThat(region).isNotNull();
      assertThat(region.isDestroyed()).isFalse();
      assertThat(cache.getRegion(partitionedRegionName)).isNotNull();
    });

    for (int i = 1; i < 4; i++) {
      final int redundancy = i;

      vm1.invoke(() -> {
        Cache cache = getCache();

        PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
        partitionAttributesFactory.setRedundantCopies(redundancy);

        RegionFactory regionFactory = cache.createRegionFactory(PARTITION);
        regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

        try (IgnoredException ie = addIgnoredException(IllegalStateException.class)) {
          assertThatThrownBy(() -> regionFactory.create(partitionedRegionName))
              .isInstanceOf(IllegalStateException.class);
        }

        assertThat(cache.getRegion(partitionedRegionName)).isNull();
      });
    }
  }

  /**
   * This test creates partition region with scope = DISTRIBUTED_ACK and tests whether all the
   * attributes of partitioned region are properly initialized
   */
  @Test
  public void testPartitionRegionInitialization() throws Exception {
    invokeAsync(vm0, () -> createPartitionedRegions(prNamePrefix, numberOfRegions));
    invokeAsync(vm1, () -> createPartitionedRegions(prNamePrefix, numberOfRegions));
    invokeAsync(vm2, () -> createPartitionedRegions(prNamePrefix, numberOfRegions));
    invokeAsync(vm3, () -> createPartitionedRegions(prNamePrefix, numberOfRegions));

    awaitAllAsyncInvocations();

    invokeAsync(vm0, () -> validatePRInitialization());
    invokeAsync(vm1, () -> validatePRInitialization());
    invokeAsync(vm2, () -> validatePRInitialization());
    invokeAsync(vm3, () -> validatePRInitialization());

    awaitAllAsyncInvocations();
  }

  @Test
  public void testPartitionRegionRegistration() throws Exception {
    invokeAsync(vm0, () -> createPartitionedRegions(prNamePrefix, numberOfRegions));
    invokeAsync(vm1, () -> createPartitionedRegions(prNamePrefix, numberOfRegions));
    invokeAsync(vm2, () -> createPartitionedRegions(prNamePrefix, numberOfRegions));
    invokeAsync(vm3, () -> createPartitionedRegions(prNamePrefix, numberOfRegions));

    awaitAllAsyncInvocations();

    invokeAsync(vm0, () -> validatePRRegistration(prNamePrefix, numberOfRegions));
    invokeAsync(vm1, () -> validatePRRegistration(prNamePrefix, numberOfRegions));
    invokeAsync(vm2, () -> validatePRRegistration(prNamePrefix, numberOfRegions));
    invokeAsync(vm3, () -> validatePRRegistration(prNamePrefix, numberOfRegions));

    awaitAllAsyncInvocations();
  }

  /**
   * This tests persistence conflicts btw members of partition region
   */
  @Test
  public void testPartitionRegionPersistenceConflicts() throws Exception {
    VM dataStore0 = vm0;
    VM dataStore1 = vm1;
    VM accessor0 = vm2;
    VM accessor1 = vm3;

    addIgnoredException("IllegalStateException");

    accessor0.invoke(() -> createPersistentPR(partitionedRegionName, 0, false));
    accessor1
        .invoke(() -> createPersistentPRThrowsIfLocalMaxMemoryIsZero(partitionedRegionName, true));
    dataStore0.invoke(() -> createPersistentPR(partitionedRegionName, 100, true));
    dataStore1.invoke(
        () -> createPersistentPRThrowsIfDataPolicyConflict(partitionedRegionName, 100, false));
  }

  private void validatePRInitialization() {
    Cache cache = getCache();
    Region root = cache.getRegion(PR_ROOT_REGION_NAME);
    assertThat(root).isNotNull();

    RegionAttributes regionAttributes = root.getAttributes();
    assertThat(regionAttributes.getScope().isDistributedAck()).isTrue();
    assertThat(regionAttributes.getDataPolicy()).isEqualTo(DataPolicy.REPLICATE);
  }

  private void validatePRRegistration(final String prPrefixName, final int count) {
    InternalCache cache = getCache();
    Region root = PartitionedRegionHelper.getPRRoot(cache);
    assertThat(root).isNotNull();

    for (int i = 0; i < count; i++) {
      PartitionedRegion region = (PartitionedRegion) cache.getRegion(prPrefixName + i);
      assertThat(region).isNotNull();
      String name = region.getRegionIdentifier();
      PartitionRegionConfig partitionRegionConfig = (PartitionRegionConfig) root.get(name);
      assertThat(partitionRegionConfig).isNotNull();
    }
  }

  public void createPartitionedRegion(final String regionName) {
    Cache cache = getCache();
    RegionFactory regionFactory = cache.createRegionFactory(PARTITION);
    Region region = regionFactory.create(regionName);

    assertThat(region).isNotNull();
    assertThat(region.isDestroyed()).isFalse();
    assertThat(cache.getRegion(regionName)).isNotNull();
  }

  private void createPartitionedRegions(final String prPrefixName, final int count) {
    Cache cache = getCache();
    RegionFactory regionFactory = cache.createRegionFactory(PARTITION);

    for (int i = 0; i < count; i++) {
      Region region = regionFactory.create(prPrefixName + i);
      assertThat(region).isNotNull();
      assertThat(region.isDestroyed()).isFalse();
      assertThat(cache.getRegion(prPrefixName + i)).isNotNull();
    }
  }

  private void createPersistentPR(final String regionName, final int localMaxMemory,
      final boolean isPersistent) {
    Cache cache = getCache();

    RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);

    if (isPersistent) {
      regionFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    } else {
      regionFactory.setDataPolicy(DataPolicy.PARTITION);
    }

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setLocalMaxMemory(localMaxMemory);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    Region region = regionFactory.create(regionName);

    assertThat(region).isNotNull();
    assertThat(region.isDestroyed()).isFalse();
    assertThat(cache.getRegion(regionName)).isNotNull();
  }

  private void createPersistentPRThrowsIfLocalMaxMemoryIsZero(final String regionName,
      final boolean isPersistent) {
    String message = "Persistence is not allowed when local-max-memory is zero.";
    createPersistentPRThrows(regionName, 0, isPersistent, message);
  }

  private void createPersistentPRThrowsIfDataPolicyConflict(final String regionName,
      final int localMaxMemory, final boolean isPersistent) {
    assertThat(localMaxMemory).isGreaterThan(0);
    String message = "DataPolicy for Datastore members should all be persistent or not.";
    createPersistentPRThrows(regionName, localMaxMemory, isPersistent, message);
  }

  private void createPersistentPRThrows(final String regionName, final int localMaxMemory,
      final boolean isPersistent, final String message) {
    Cache cache = getCache();

    RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);

    if (isPersistent) {
      regionFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    } else {
      regionFactory.setDataPolicy(DataPolicy.PARTITION);
    }

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setLocalMaxMemory(localMaxMemory);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    assertThatThrownBy(() -> regionFactory.create(regionName))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining(message);
  }

  private void validatePR(final String regionName, final int count) {
    Cache cache = getCache();
    for (int i = 0; i < count; i++) {
      assertThat(cache.getRegion(regionName + i)).isNotNull();
    }
  }

  private void createReplicateRegion(final String regionName) {
    Cache cache = getCache();
    RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE);
    regionFactory.create(regionName);
  }

  private void createPRConcurrently(final String regionName, final String prPrefixName,
      final int count) {
    createPRConcurrently(regionName, prPrefixName, count, false);
  }

  private void createPRAccessorConcurrently(final String regionName, final String prPrefixName,
      final int count) {
    createPRConcurrently(regionName, prPrefixName, count, true);
  }

  private void createPRConcurrently(final String regionName, final String prPrefixName,
      final int count, final boolean accessor) {
    Cache cache = getCache();

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(2);
    if (accessor) {
      partitionAttributesFactory.setLocalMaxMemory(0);
    }

    RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    // wait for put
    Region region = cache.getRegion(regionName);

    await().until(() -> region.getEntry("start") != null);

    for (int i = 0; i < count; ++i) {
      Region partitionedRegion = regionFactory.create(prPrefixName + i);

      assertThat(partitionedRegion).isNotNull();
      assertThat(partitionedRegion.isDestroyed()).isFalse();
      assertThat(cache.getRegion(regionName)).isNotNull();
    }
  }

  private void invokeAsync(final VM vm, final SerializableRunnableIF runnable) {
    asyncInvocations.add(vm.invokeAsync(runnable));
  }

  private void awaitAllAsyncInvocations() throws ExecutionException, InterruptedException {
    for (AsyncInvocation async : asyncInvocations) {
      async.await();
    }
    asyncInvocations.clear();
  }
}
