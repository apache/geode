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

import static org.apache.geode.internal.cache.PartitionedRegionHelper.PR_ROOT_REGION_NAME;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;

/**
 * This class tests bucket Creation and distribution for the multiple Partition regions.
 */

public class PartitionedRegionBucketCreationDistributionDUnitTest extends CacheTestCase {

  private String regionOne;
  private String regionTwo;

  private int numberOfBuckets;
  private int localMaxMemory;

  private int firstKey;
  private int lastKey;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  private transient List<AsyncInvocation> asyncInvocations;

  @Before
  public void setUp() {
    regionOne = getUniqueName() + "0";
    regionTwo = getUniqueName() + "1";

    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);
    vm3 = getHost(0).getVM(3);

    localMaxMemory = 200;
    numberOfBuckets = 11;

    firstKey = 0;
    lastKey = 50;

    asyncInvocations = new ArrayList<>();
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
  }

  /**
   * This test performs following operations:
   *
   * <p>
   * 1. Validate bucket2Node region of the partition regions.<br>
   * (a) bucket2Node Region should not be null.<br>
   * (b) Scope of the bucket2Node region should be DISTRIBUTED_ACK.<br>
   * (c) Size of bucket2Node region should be 0 before any put() operation.<br>
   * (d) Parent region of the bucket2Node region should be root i.e. region with name "PRRoot".
   *
   * <p>
   * 2. Do put() operation from the different VMs so that buckets gets generated.
   *
   * <p>
   * 3. Validate bucket regions of multiple partition Regions<br>
   * (a) Size of bucket2Node region should be > 0.<br>
   * (b) In case of the partition regions with redundancy > 0 scope of the bucket regions should be
   * scope of the partition regions.<br>
   * (c) In case of the partition regions with redundancy > 0 no two bucket regions with same
   * bucketId should not be present on the same node.
   */
  @Test
  public void testBucketCreationInMultiplePartitionRegion() throws Exception {
    // creating regionOne
    vm0.invoke(() -> createPartitionRegion(regionOne, 200, 0, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionOne, 200, 0, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionOne, 200, 0, numberOfBuckets));
    vm3.invoke(() -> createPartitionRegion(regionOne, 0, 0, numberOfBuckets));

    // creating regionTwo
    vm0.invoke(() -> createPartitionRegion(regionTwo, 200, 1, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionTwo, 200, 1, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionTwo, 200, 1, numberOfBuckets));
    vm3.invoke(() -> createPartitionRegion(regionTwo, 0, 1, numberOfBuckets));

    // validating bucket2Node of multiple partition regions before doing any put().
    vm0.invoke(() -> validateBucket2NodeBeforePut(regionOne));
    vm1.invoke(() -> validateBucket2NodeBeforePut(regionOne));
    vm2.invoke(() -> validateBucket2NodeBeforePut(regionOne));
    vm3.invoke(() -> validateBucket2NodeBeforePut(regionOne));

    vm0.invoke(() -> validateBucket2NodeBeforePut(regionTwo));
    vm1.invoke(() -> validateBucket2NodeBeforePut(regionTwo));
    vm2.invoke(() -> validateBucket2NodeBeforePut(regionTwo));
    vm3.invoke(() -> validateBucket2NodeBeforePut(regionTwo));

    // doing put() operation on multiple partition region
    asyncInvocations.clear();

    invokeAsync(vm0, () -> putInMultiplePartitionRegion(regionOne, firstKey, lastKey));
    invokeAsync(vm1, () -> putInMultiplePartitionRegion(regionOne, firstKey, lastKey));
    invokeAsync(vm2, () -> putInMultiplePartitionRegion(regionOne, firstKey, lastKey));
    invokeAsync(vm3, () -> putInMultiplePartitionRegion(regionOne, firstKey, lastKey));

    invokeAsync(vm0, () -> putInMultiplePartitionRegion(regionTwo, firstKey, lastKey));
    invokeAsync(vm1, () -> putInMultiplePartitionRegion(regionTwo, firstKey, lastKey));
    invokeAsync(vm2, () -> putInMultiplePartitionRegion(regionTwo, firstKey, lastKey));
    invokeAsync(vm3, () -> putInMultiplePartitionRegion(regionTwo, firstKey, lastKey));

    awaitAllAsyncInvocations();

    // validating bucket regions of multiple partition regions.
    vm0.invoke(() -> validateBucketCreationAfterPut(regionOne));
    vm1.invoke(() -> validateBucketCreationAfterPut(regionOne));
    vm2.invoke(() -> validateBucketCreationAfterPut(regionOne));
    vm3.invoke(() -> validateBucketCreationAfterPutForNode3(regionOne));

    vm0.invoke(() -> validateBucketCreationAfterPut(regionTwo));
    vm1.invoke(() -> validateBucketCreationAfterPut(regionTwo));
    vm2.invoke(() -> validateBucketCreationAfterPut(regionTwo));
    vm3.invoke(() -> validateBucketCreationAfterPutForNode3(regionTwo));

    // validateBucketScopesAfterPutInMultiplePartitionRegion
    vm0.invoke(() -> validateBucketScopeAfterPut(regionOne));
    vm1.invoke(() -> validateBucketScopeAfterPut(regionOne));
    vm2.invoke(() -> validateBucketScopeAfterPut(regionOne));

    vm0.invoke(() -> validateBucketScopeAfterPut(regionTwo));
    vm1.invoke(() -> validateBucketScopeAfterPut(regionTwo));
    vm2.invoke(() -> validateBucketScopeAfterPut(regionTwo));
  }

  /**
   * This test performs following operations
   *
   * <p>
   * 1.Creates multiple partition regions in 4 vms
   *
   * <p>
   * 2. Performs Put() operation from vm0 for the keys 0 to 111.
   *
   * <p>
   * 3. Validates bucket distribution over all the nodes for multiple partition regions.
   */
  @Test
  public void testBucketCreationInPRPutFromOneNode() throws Exception {
    lastKey = numberOfBuckets;

    vm0.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, 0, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, 0, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, 0, numberOfBuckets));
    vm3.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, 0, numberOfBuckets));

    vm0.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, 0, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, 0, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, 0, numberOfBuckets));
    vm3.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, 0, numberOfBuckets));

    // doing put() operation from vm0 only
    asyncInvocations.clear();

    invokeAsync(vm0, () -> putFromOneVm(regionOne, firstKey, lastKey));
    invokeAsync(vm0,
        () -> putFromOneVm(regionOne, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm0, () -> putFromOneVm(regionOne, firstKey + 2 * numberOfBuckets,
        lastKey + 2 * numberOfBuckets));

    invokeAsync(vm0, () -> putFromOneVm(regionTwo, firstKey, lastKey));
    invokeAsync(vm0,
        () -> putFromOneVm(regionTwo, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm0, () -> putFromOneVm(regionTwo, firstKey + 2 * numberOfBuckets,
        lastKey + 2 * numberOfBuckets));

    awaitAllAsyncInvocations();

    // validating bucket distribution over all the nodes
    int numberOfBucketsExpectedOnEachNode = getNumberOfBucketsExpectedOnEachNode();

    vm0.invoke(() -> validateBucketsDistribution(regionOne, numberOfBucketsExpectedOnEachNode));
    vm1.invoke(() -> validateBucketsDistribution(regionOne, numberOfBucketsExpectedOnEachNode));
    vm2.invoke(() -> validateBucketsDistribution(regionOne, numberOfBucketsExpectedOnEachNode));
    vm3.invoke(() -> validateBucketsDistribution(regionOne, numberOfBucketsExpectedOnEachNode));

    vm0.invoke(() -> validateBucketsDistribution(regionTwo, numberOfBucketsExpectedOnEachNode));
    vm1.invoke(() -> validateBucketsDistribution(regionTwo, numberOfBucketsExpectedOnEachNode));
    vm2.invoke(() -> validateBucketsDistribution(regionTwo, numberOfBucketsExpectedOnEachNode));
    vm3.invoke(() -> validateBucketsDistribution(regionTwo, numberOfBucketsExpectedOnEachNode));
  }

  /**
   * This test performs following operations
   *
   * <p>
   * 1. Creates multiple partition regions in 4 vms with scope DISTRIBUTED_ACK and
   * DISTRIBUTED_NO_ACK.
   *
   * <p>
   * 2. Performs Put() operation from all the vms for the keys 0 to 111.
   *
   * <p>
   * 3. Validates bucket distribution over all the nodes for multiple partition regions.
   */
  @Test
  public void testBucketCreationInMultiplePartitionRegionFromAllNodes() throws Exception {
    lastKey = numberOfBuckets;

    vm0.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, 0, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, 0, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, 0, numberOfBuckets));
    vm3.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, 0, numberOfBuckets));

    vm0.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, 0, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, 0, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, 0, numberOfBuckets));
    vm3.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, 0, numberOfBuckets));

    // doing put() operation from all vms
    asyncInvocations.clear();

    invokeAsync(vm0, () -> putFromOneVm(regionOne, firstKey, lastKey));
    invokeAsync(vm0,
        () -> putFromOneVm(regionOne, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm0, () -> putFromOneVm(regionOne, firstKey + 2 * numberOfBuckets,
        lastKey + 2 * numberOfBuckets));

    invokeAsync(vm1, () -> putFromOneVm(regionOne, firstKey, lastKey));
    invokeAsync(vm1,
        () -> putFromOneVm(regionOne, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm1, () -> putFromOneVm(regionOne, firstKey + 2 * numberOfBuckets,
        lastKey + 2 * numberOfBuckets));

    invokeAsync(vm2, () -> putFromOneVm(regionOne, firstKey, lastKey));
    invokeAsync(vm2,
        () -> putFromOneVm(regionOne, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm2, () -> putFromOneVm(regionOne, firstKey + 2 * numberOfBuckets,
        lastKey + 2 * numberOfBuckets));

    invokeAsync(vm3, () -> putFromOneVm(regionOne, firstKey, lastKey));
    invokeAsync(vm3,
        () -> putFromOneVm(regionOne, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm3, () -> putFromOneVm(regionOne, firstKey + 2 * numberOfBuckets,
        lastKey + 2 * numberOfBuckets));

    invokeAsync(vm0, () -> putFromOneVm(regionTwo, firstKey, lastKey));
    invokeAsync(vm0,
        () -> putFromOneVm(regionTwo, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm0, () -> putFromOneVm(regionTwo, firstKey + 2 * numberOfBuckets,
        lastKey + 2 * numberOfBuckets));

    invokeAsync(vm1, () -> putFromOneVm(regionTwo, firstKey, lastKey));
    invokeAsync(vm1,
        () -> putFromOneVm(regionTwo, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm1, () -> putFromOneVm(regionTwo, firstKey + 2 * numberOfBuckets,
        lastKey + 2 * numberOfBuckets));

    invokeAsync(vm2, () -> putFromOneVm(regionTwo, firstKey, lastKey));
    invokeAsync(vm2,
        () -> putFromOneVm(regionTwo, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm2, () -> putFromOneVm(regionTwo, firstKey + 2 * numberOfBuckets,
        lastKey + 2 * numberOfBuckets));

    invokeAsync(vm3, () -> putFromOneVm(regionTwo, firstKey, lastKey));
    invokeAsync(vm3,
        () -> putFromOneVm(regionTwo, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm3, () -> putFromOneVm(regionTwo, firstKey + 2 * numberOfBuckets,
        lastKey + 2 * numberOfBuckets));

    awaitAllAsyncInvocations();

    // validating bucket distribution over all the nodes
    int numberOfBucketsExpectedOnEachNode = getNumberOfBucketsExpectedOnEachNode() - 4;

    vm0.invoke(() -> validateBucketsDistribution(regionOne, numberOfBucketsExpectedOnEachNode));
    vm1.invoke(() -> validateBucketsDistribution(regionOne, numberOfBucketsExpectedOnEachNode));
    vm2.invoke(() -> validateBucketsDistribution(regionOne, numberOfBucketsExpectedOnEachNode));
    vm2.invoke(() -> validateBucketsDistribution(regionOne, numberOfBucketsExpectedOnEachNode));

    vm0.invoke(() -> validateBucketsDistribution(regionTwo, numberOfBucketsExpectedOnEachNode));
    vm1.invoke(() -> validateBucketsDistribution(regionTwo, numberOfBucketsExpectedOnEachNode));
    vm2.invoke(() -> validateBucketsDistribution(regionTwo, numberOfBucketsExpectedOnEachNode));
    vm2.invoke(() -> validateBucketsDistribution(regionTwo, numberOfBucketsExpectedOnEachNode));
  }

  /**
   * This test performs following operations
   *
   * <p>
   * 1. Creates multiple partition regions in 3 vms with scope DISTRIBUTED_ACK and
   * DISTRIBUTED_NO_ACK
   *
   * <p>
   * 2. Performs Put() operation from 3 the vms for the keys startIndexForRgion to enIndexForRegion
   *
   * <p>
   * 3. Creates partition region on new node
   *
   * <p>
   * 4. Performs Put() operation from 3 the vms for the keys firstRegion to enIndexForRegion.
   *
   * <p>
   * 5. Validate bucket creation on new node.
   */
  @Test
  public void testBucketDistributionAfterNodeAdditionInPR() throws Exception {
    lastKey = 5;

    vm0.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, 0, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, 0, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, 0, numberOfBuckets));

    vm0.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, 0, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, 0, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, 0, numberOfBuckets));

    // doing put() in multiple partition regions from 3 nodes.
    int delta = (lastKey - firstKey) / 3;

    asyncInvocations.clear();

    invokeAsync(vm0, () -> putInMultiplePartitionRegion(regionOne, firstKey, firstKey + 1 * delta));
    invokeAsync(vm1,
        () -> putInMultiplePartitionRegion(regionOne, firstKey + 1 * delta, firstKey + 2 * delta));
    invokeAsync(vm2, () -> putInMultiplePartitionRegion(regionOne, firstKey + 2 * delta, lastKey));

    invokeAsync(vm0, () -> putInMultiplePartitionRegion(regionTwo, firstKey, firstKey + 1 * delta));
    invokeAsync(vm1,
        () -> putInMultiplePartitionRegion(regionTwo, firstKey + 1 * delta, firstKey + 2 * delta));
    invokeAsync(vm2, () -> putInMultiplePartitionRegion(regionTwo, firstKey + 2 * delta, lastKey));

    awaitAllAsyncInvocations();

    vm3.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, 0, numberOfBuckets));
    vm3.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, 0, numberOfBuckets));

    firstKey = 5;
    lastKey = numberOfBuckets;

    // doing put() in multiple partition regions from 3 nodes.
    asyncInvocations.clear();

    invokeAsync(vm0, () -> putInMultiplePartitionRegion(regionOne, firstKey, firstKey + 1 * delta));
    invokeAsync(vm1,
        () -> putInMultiplePartitionRegion(regionOne, firstKey + 1 * delta, firstKey + 2 * delta));
    invokeAsync(vm2, () -> putInMultiplePartitionRegion(regionOne, firstKey + 2 * delta, lastKey));

    invokeAsync(vm0, () -> putInMultiplePartitionRegion(regionTwo, firstKey, firstKey + 1 * delta));
    invokeAsync(vm1,
        () -> putInMultiplePartitionRegion(regionTwo, firstKey + 1 * delta, firstKey + 2 * delta));
    invokeAsync(vm2, () -> putInMultiplePartitionRegion(regionTwo, firstKey + 2 * delta, lastKey));

    awaitAllAsyncInvocations();

    // validating bucket creation in the 4th node
    vm0.invoke(() -> validateBuckets(regionOne));
    vm1.invoke(() -> validateBuckets(regionOne));
    vm2.invoke(() -> validateBuckets(regionOne));
    vm3.invoke(() -> validateBuckets(regionOne));

    vm0.invoke(() -> validateBuckets(regionTwo));
    vm1.invoke(() -> validateBuckets(regionTwo));
    vm2.invoke(() -> validateBuckets(regionTwo));
    vm3.invoke(() -> validateBuckets(regionTwo));
  }

  /**
   * this is to test global property TOTAL_BUCKETS_NUM_PROPERTY.
   *
   * <p>
   * 1.create partition region with scope = DISTRIBUTED_ACK redundancy = 3 on 4 vms
   *
   * <p>
   * 2.set global property TOTAL_BUCKETS_NUM_PROPERTY = 11
   *
   * <p>
   * 3.perform put() operation for the keys in the range 0 to 100
   *
   * <p>
   * 4.test number of buckets created. It should be = 11
   */
  @Test
  public void testTotalNumBucketProperty() throws Exception {
    vm0.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, 1, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, 1, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, 1, numberOfBuckets));
    vm3.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, 1, numberOfBuckets));

    vm0.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, 1, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, 1, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, 1, numberOfBuckets));
    vm3.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, 1, numberOfBuckets));

    firstKey = 0;
    lastKey = 20;

    asyncInvocations.clear();

    invokeAsync(vm0, () -> putFromOneVm(regionOne, firstKey, lastKey));
    invokeAsync(vm0,
        () -> putFromOneVm(regionOne, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm0, () -> putFromOneVm(regionOne, firstKey + 2 * numberOfBuckets,
        lastKey + 2 * numberOfBuckets));

    invokeAsync(vm0, () -> putFromOneVm(regionTwo, firstKey, lastKey));
    invokeAsync(vm0,
        () -> putFromOneVm(regionTwo, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm0, () -> putFromOneVm(regionTwo, firstKey + 2 * numberOfBuckets,
        lastKey + 2 * numberOfBuckets));

    awaitAllAsyncInvocations();

    int expectedNumberOfBuckets = 11;

    vm0.invoke(() -> validateTotalNumberOfBuckets(regionOne, expectedNumberOfBuckets));
    vm1.invoke(() -> validateTotalNumberOfBuckets(regionOne, expectedNumberOfBuckets));
    vm2.invoke(() -> validateTotalNumberOfBuckets(regionOne, expectedNumberOfBuckets));
    vm3.invoke(() -> validateTotalNumberOfBuckets(regionOne, expectedNumberOfBuckets));

    vm0.invoke(() -> validateTotalNumberOfBuckets(regionTwo, expectedNumberOfBuckets));
    vm1.invoke(() -> validateTotalNumberOfBuckets(regionTwo, expectedNumberOfBuckets));
    vm2.invoke(() -> validateTotalNumberOfBuckets(regionTwo, expectedNumberOfBuckets));
    vm3.invoke(() -> validateTotalNumberOfBuckets(regionTwo, expectedNumberOfBuckets));

    firstKey = 200;
    lastKey = 400;

    asyncInvocations.clear();

    int delta = (lastKey - firstKey) / 4;

    invokeAsync(vm0, () -> putFromOneVm(regionOne, firstKey, firstKey + 1 * delta));
    invokeAsync(vm1, () -> putFromOneVm(regionOne, firstKey + 1 * delta, firstKey + 2 * delta));
    invokeAsync(vm2, () -> putFromOneVm(regionOne, firstKey + 2 * delta, firstKey + 3 * delta));
    invokeAsync(vm3, () -> putFromOneVm(regionOne, firstKey + 3 * delta, lastKey));

    invokeAsync(vm0, () -> putFromOneVm(regionTwo, firstKey, firstKey + 1 * delta));
    invokeAsync(vm1, () -> putFromOneVm(regionTwo, firstKey + 1 * delta, firstKey + 2 * delta));
    invokeAsync(vm2, () -> putFromOneVm(regionTwo, firstKey + 2 * delta, firstKey + 3 * delta));
    invokeAsync(vm3, () -> putFromOneVm(regionTwo, firstKey + 3 * delta, lastKey));

    firstKey += numberOfBuckets;
    lastKey += numberOfBuckets;
    int delta2 = (lastKey - firstKey) / 4;

    invokeAsync(vm0, () -> putFromOneVm(regionOne, firstKey, firstKey + 1 * delta2));
    invokeAsync(vm1, () -> putFromOneVm(regionOne, firstKey + 1 * delta2, firstKey + 2 * delta2));
    invokeAsync(vm2, () -> putFromOneVm(regionOne, firstKey + 2 * delta2, firstKey + 3 * delta2));
    invokeAsync(vm3, () -> putFromOneVm(regionOne, firstKey + 3 * delta2, lastKey));

    invokeAsync(vm0, () -> putFromOneVm(regionTwo, firstKey, firstKey + 1 * delta2));
    invokeAsync(vm1, () -> putFromOneVm(regionTwo, firstKey + 1 * delta2, firstKey + 2 * delta2));
    invokeAsync(vm2, () -> putFromOneVm(regionTwo, firstKey + 2 * delta2, firstKey + 3 * delta2));
    invokeAsync(vm3, () -> putFromOneVm(regionTwo, firstKey + 3 * delta2, lastKey));

    awaitAllAsyncInvocations();

    vm0.invoke(() -> validateTotalNumberOfBuckets(regionOne, expectedNumberOfBuckets));
    vm1.invoke(() -> validateTotalNumberOfBuckets(regionOne, expectedNumberOfBuckets));
    vm2.invoke(() -> validateTotalNumberOfBuckets(regionOne, expectedNumberOfBuckets));
    vm3.invoke(() -> validateTotalNumberOfBuckets(regionOne, expectedNumberOfBuckets));

    vm0.invoke(() -> validateTotalNumberOfBuckets(regionTwo, expectedNumberOfBuckets));
    vm1.invoke(() -> validateTotalNumberOfBuckets(regionTwo, expectedNumberOfBuckets));
    vm2.invoke(() -> validateTotalNumberOfBuckets(regionTwo, expectedNumberOfBuckets));
    vm3.invoke(() -> validateTotalNumberOfBuckets(regionTwo, expectedNumberOfBuckets));
  }

  /**
   * Ensure that all buckets can be allocated for a PR with different levels of redundancy. This is
   * important given that bucket creation may fail due to VMs that refuse bucket allocation. VMs may
   * refuse for different reasons, in this case VMs may refuse because they are above their maximum.
   */
  @Test
  public void testCompleteBucketAllocation() throws Exception {
    invokeInEveryVM(() -> {
      PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
      partitionAttributesFactory.setLocalMaxMemory(10);
      partitionAttributesFactory.setRedundantCopies(0);
      partitionAttributesFactory.setTotalNumBuckets(23);

      RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
      regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

      regionFactory.create(regionOne);
    });

    vm0.invoke(() -> {
      Cache cache = getCache();
      Region<Integer, String> region = cache.getRegion(regionOne);
      // create all the buckets
      for (int i = 0; i < numberOfBuckets; i++) {
        region.put(i, "value-" + i);
      }
    });

    // TODO: add some validation
  }

  private void putFromOneVm(final String regionName, final int firstKey, final int lastKey) {
    Cache cache = getCache();
    Region<Integer, String> region = cache.getRegion(regionName);
    for (int i = firstKey; i < lastKey; i++) {
      region.put(i, regionName + i);
    }
  }

  private void validateBuckets(final String regionName) {
    Cache cache = getCache();
    Region region = cache.getRegion(regionName);
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;
    PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();
    assertThat(dataStore.getLocalBucket2RegionMap().size()).isGreaterThan(0);
  }

  private void validateBucketCreationAfterPut(final String regionName) {
    Cache cache = getCache();
    Region prRoot = cache.getRegion(PR_ROOT_REGION_NAME);
    Region region = cache.getRegion(regionName);
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;

    RegionAdvisor regionAdvisor = partitionedRegion.getRegionAdvisor();
    assertThat(regionAdvisor.getBucketSet().size()).isGreaterThan(0);

    PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();
    ConcurrentMap<Integer, BucketRegion> localBucket2RegionMap =
        dataStore.getLocalBucket2RegionMap();
    assertThat(localBucket2RegionMap.size()).isGreaterThan(0);

    // taking the buckets which are local to the node and not all the available buckets.
    for (Integer bucketId : localBucket2RegionMap.keySet()) {
      BucketRegion bucketRegion = localBucket2RegionMap.get(bucketId);
      Region subregion = prRoot.getSubregion(partitionedRegion.getBucketName(bucketId));
      assertThat(bucketRegion.getFullPath()).isEqualTo(subregion.getFullPath());
      assertThat(subregion.getParentRegion()).isEqualTo(prRoot);
    }
  }

  private void validateBucketCreationAfterPutForNode3(final String regionName) {
    Cache cache = getCache();
    Region region = cache.getRegion(regionName);
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;
    assertThat(partitionedRegion.getDataStore()).isNull();
  }

  private void validateBucketScopeAfterPut(final String regionName) {
    Cache cache = getCache();
    Region prRoot = cache.getRegion(PR_ROOT_REGION_NAME);
    Region region = cache.getRegion(regionName);
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;

    RegionAdvisor regionAdvisor = partitionedRegion.getRegionAdvisor();
    assertThat(regionAdvisor.getBucketSet().size()).isGreaterThan(0);

    // taking the buckets which are local to the node and not all the available buckets.
    PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();
    for (Integer bucketId : dataStore.getLocalBucket2RegionMap().keySet()) {
      Region bucketRegion = prRoot.getSubregion(partitionedRegion.getBucketName(bucketId));
      assertThat(bucketRegion.getAttributes().getScope()).isEqualTo(Scope.DISTRIBUTED_ACK);
    }
  }

  private void validateBucket2NodeBeforePut(final String regionName) {
    Cache cache = getCache();
    Region region = cache.getRegion(regionName);
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;

    RegionAdvisor regionAdvisor = partitionedRegion.getRegionAdvisor();

    assertThat(regionAdvisor.getNumProfiles()).isGreaterThan(0);
    assertThat(regionAdvisor.getNumDataStores()).isGreaterThan(0);

    int bucketSetSize = regionAdvisor.getCreatedBucketsCount();
    assertThat(bucketSetSize).isEqualTo(0);
  }

  private void validateBucketsDistribution(final String regionName,
      final int expectedNumberOfBuckets) {
    InternalCache cache = getCache();
    Region prRoot = cache.getRegion(PR_ROOT_REGION_NAME);

    Region region = cache.getRegion(regionName);
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;

    int numberLocalBuckets = partitionedRegion.getDataStore().getBucketsManaged();
    assertThat(numberLocalBuckets).isGreaterThanOrEqualTo(expectedNumberOfBuckets);

    partitionedRegion.getDataStore().visitBuckets((bucketId, regionArg) -> {
      Region bucketRegion = prRoot.getSubregion(partitionedRegion.getBucketName(bucketId));
      assertThat(bucketRegion.getFullPath()).isEqualTo(regionArg.getFullPath());
    });
  }

  private void createPartitionRegion(final String regionName, final int localMaxMemory,
      final int redundancy, final int numberOfBuckets) {
    Cache cache = getCache();

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setLocalMaxMemory(localMaxMemory);
    partitionAttributesFactory.setRedundantCopies(redundancy);
    partitionAttributesFactory.setTotalNumBuckets(numberOfBuckets);

    RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(regionName);
  }

  private void validateTotalNumberOfBuckets(final String regionName,
      final int expectedNumberOfBuckets) {
    Cache cache = getCache();
    Region region = cache.getRegion(regionName);
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;

    Set<Integer> bucketSet = partitionedRegion.getRegionAdvisor().getBucketSet();
    assertThat(bucketSet).hasSize(expectedNumberOfBuckets);
  }

  private int getNumberOfBucketsExpectedOnEachNode() {
    return numberOfBuckets / 4 - 1;
  }

  private void putInMultiplePartitionRegion(final String regionName, final int firstKey,
      final int lastKey) {
    Region<String, String> region = getCache().getRegion(regionName);
    for (int i = firstKey; i < lastKey; i++) {
      region.put(i + regionName + i, regionName + i);
    }
  }

  private void invokeAsync(final VM vm, final SerializableRunnableIF runnable) {
    asyncInvocations.add(vm.invokeAsync(runnable));
  }

  private void awaitAllAsyncInvocations() throws ExecutionException, InterruptedException {
    for (AsyncInvocation async : asyncInvocations) {
      async.await();
    }
  }

}
