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

import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.cache.PartitionedRegionDataStore.BucketVisitor;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * This class tests bucket Creation and distribution for the multiple Partition regions.
 */
@Category(DistributedTest.class)
public class PartitionedRegionBucketCreationDistributionDUnitTest extends CacheTestCase {

  private static final int MAX_NUMBER_OF_REGIONS = 2;

  private String regionName;
  private String regionOne;
  private String regionTwo;

  private int redundancy;
  private int numberOfBuckets;
  private int localMaxMemory;

  private int firstRegion;
  private int lastRegion;

  private int firstKey;
  private int lastKey;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  private transient List<AsyncInvocation> asyncInvocations;

  @Before
  public void setUp() {
    regionName = getUniqueName();
    regionOne = getUniqueName() + "0";
    regionTwo = getUniqueName() + "1";

    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);
    vm3 = getHost(0).getVM(3);

    localMaxMemory = 200;
    numberOfBuckets = 11;
    redundancy = 0;

    firstRegion = 0;
    lastRegion = MAX_NUMBER_OF_REGIONS;

    firstKey = 0;
    lastKey = 50;

    asyncInvocations = new ArrayList<>();
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
  }

  /**
   * This test performs following operations
   *
   * <p>
   * 1. Validate bucket2Node region of the partition regions.
   *
   * <p>
   * (a) bucket2Node Region should not be null.
   *
   * <p>
   * (b) Scope of the bucket2Node region should be DISTRIBUTED_ACK.
   *
   * <p>
   * (c) Size of bucket2Node region should be 0 before any put() operation.
   *
   * <p>
   * (d) Parent region of the bucket2Node region should be root i.e. region with name "PRRoot".
   *
   * <p>
   * 2. Do put() operation from the different VMs so that buckets gets generated.
   *
   * <p>
   * 3. Validate bucket regions of multiple partition Regions
   *
   * <p>
   * (a) Size of bucket2Node region should be > 0.
   *
   * <p>
   * (b) In case of the partition regions with redundancy > 0 scope of the bucket regions should be
   * scope of the partition regions.
   *
   * <p>
   * (c) In case of the partition regions with redundancy > 0 no two bucket regions with same
   * bucketId should not be present on the same node.
   */
  @Test
  public void testBucketCreationInMultiplePartitionRegion() throws Exception {
    firstKey = 0;
    lastKey = 50;

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
    vm0.invoke(validateBucket2NodeBeforePut(regionOne));
    vm1.invoke(validateBucket2NodeBeforePut(regionOne));
    vm2.invoke(validateBucket2NodeBeforePut(regionOne));
    vm3.invoke(validateBucket2NodeBeforePut(regionOne));

    vm0.invoke(validateBucket2NodeBeforePut(regionTwo));
    vm1.invoke(validateBucket2NodeBeforePut(regionTwo));
    vm2.invoke(validateBucket2NodeBeforePut(regionTwo));
    vm3.invoke(validateBucket2NodeBeforePut(regionTwo));

    // doing put() operation on multiple partition region
    asyncInvocations.clear();

    invokeAsync(vm0, putInMultiplePartitionRegion(regionOne, firstKey, lastKey));
    invokeAsync(vm1, putInMultiplePartitionRegion(regionOne, firstKey, lastKey));
    invokeAsync(vm2, putInMultiplePartitionRegion(regionOne, firstKey, lastKey));
    invokeAsync(vm3, putInMultiplePartitionRegion(regionOne, firstKey, lastKey));

    invokeAsync(vm0, putInMultiplePartitionRegion(regionTwo, firstKey, lastKey));
    invokeAsync(vm1, putInMultiplePartitionRegion(regionTwo, firstKey, lastKey));
    invokeAsync(vm2, putInMultiplePartitionRegion(regionTwo, firstKey, lastKey));
    invokeAsync(vm3, putInMultiplePartitionRegion(regionTwo, firstKey, lastKey));

    awaitAllAsyncInvocations();

    // validating bucket regions of multiple partition regions.
    vm0.invoke(validateBucketCreationAfterPut(regionOne));
    vm1.invoke(validateBucketCreationAfterPut(regionOne));
    vm2.invoke(validateBucketCreationAfterPut(regionOne));
    vm3.invoke(validateBucketCreationAfterPutForNode3(regionOne));

    vm0.invoke(validateBucketCreationAfterPut(regionTwo));
    vm1.invoke(validateBucketCreationAfterPut(regionTwo));
    vm2.invoke(validateBucketCreationAfterPut(regionTwo));
    vm3.invoke(validateBucketCreationAfterPutForNode3(regionTwo));

    // validateBucketScopesAfterPutInMultiplePartitionRegion
    vm0.invoke(validateBucketScopeAfterPut(regionOne));
    vm1.invoke(validateBucketScopeAfterPut(regionOne));
    vm2.invoke(validateBucketScopeAfterPut(regionOne));
    vm3.invoke(validateBucketCreationAfterPutForNode3(regionOne));

    vm0.invoke(validateBucketScopeAfterPut(regionTwo));
    vm1.invoke(validateBucketScopeAfterPut(regionTwo));
    vm2.invoke(validateBucketScopeAfterPut(regionTwo));
    vm3.invoke(validateBucketCreationAfterPutForNode3(regionTwo));
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
    firstKey = 0;
    lastKey = numberOfBuckets;

    vm0.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, redundancy, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, redundancy, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, redundancy, numberOfBuckets));
    vm3.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, redundancy, numberOfBuckets));

    vm0.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, redundancy, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, redundancy, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, redundancy, numberOfBuckets));
    vm3.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, redundancy, numberOfBuckets));

    // doing put() operation from vm0 only
    asyncInvocations.clear();

    invokeAsync(vm0, putFromOneVm(regionOne, firstKey, lastKey));
    invokeAsync(vm0, putFromOneVm(regionOne, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm0, putFromOneVm(regionOne, firstKey + 2 * numberOfBuckets, lastKey + 2 * numberOfBuckets));

    invokeAsync(vm0, putFromOneVm(regionTwo, firstKey, lastKey));
    invokeAsync(vm0, putFromOneVm(regionTwo, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm0, putFromOneVm(regionTwo, firstKey + 2 * numberOfBuckets, lastKey + 2 * numberOfBuckets));

    awaitAllAsyncInvocations();

    // validating bucket distribution over all the nodes
    int numberOfBucketsExpectedOnEachNode = getNumberOfBucketsExpectedOnEachNode();

    vm0.invoke(validateBucketsDistribution(regionOne, numberOfBucketsExpectedOnEachNode));
    vm1.invoke(validateBucketsDistribution(regionOne, numberOfBucketsExpectedOnEachNode));
    vm2.invoke(validateBucketsDistribution(regionOne, numberOfBucketsExpectedOnEachNode));
    vm3.invoke(validateBucketsDistribution(regionOne, numberOfBucketsExpectedOnEachNode));

    vm0.invoke(validateBucketsDistribution(regionTwo, numberOfBucketsExpectedOnEachNode));
    vm1.invoke(validateBucketsDistribution(regionTwo, numberOfBucketsExpectedOnEachNode));
    vm2.invoke(validateBucketsDistribution(regionTwo, numberOfBucketsExpectedOnEachNode));
    vm3.invoke(validateBucketsDistribution(regionTwo, numberOfBucketsExpectedOnEachNode));
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
    // start/end indexes for keys
    firstKey = 0;
    lastKey = numberOfBuckets;

    vm0.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, redundancy, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, redundancy, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, redundancy, numberOfBuckets));
    vm3.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, redundancy, numberOfBuckets));

    vm0.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, redundancy, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, redundancy, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, redundancy, numberOfBuckets));
    vm3.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, redundancy, numberOfBuckets));

    // doing put() operation from all vms
    asyncInvocations.clear();

    invokeAsync(vm0, putFromOneVm(regionOne, firstKey, lastKey));
    invokeAsync(vm0, putFromOneVm(regionOne, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm0, putFromOneVm(regionOne, firstKey + 2 * numberOfBuckets, lastKey + 2 * numberOfBuckets));

    invokeAsync(vm1, putFromOneVm(regionOne, firstKey, lastKey));
    invokeAsync(vm1, putFromOneVm(regionOne, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm1, putFromOneVm(regionOne, firstKey + 2 * numberOfBuckets, lastKey + 2 * numberOfBuckets));

    invokeAsync(vm2, putFromOneVm(regionOne, firstKey, lastKey));
    invokeAsync(vm2, putFromOneVm(regionOne, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm2, putFromOneVm(regionOne, firstKey + 2 * numberOfBuckets, lastKey + 2 * numberOfBuckets));

    invokeAsync(vm3, putFromOneVm(regionOne, firstKey, lastKey));
    invokeAsync(vm3, putFromOneVm(regionOne, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm3, putFromOneVm(regionOne, firstKey + 2 * numberOfBuckets, lastKey + 2 * numberOfBuckets));

    invokeAsync(vm0, putFromOneVm(regionTwo, firstKey, lastKey));
    invokeAsync(vm0, putFromOneVm(regionTwo, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm0, putFromOneVm(regionTwo, firstKey + 2 * numberOfBuckets, lastKey + 2 * numberOfBuckets));

    invokeAsync(vm1, putFromOneVm(regionTwo, firstKey, lastKey));
    invokeAsync(vm1, putFromOneVm(regionTwo, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm1, putFromOneVm(regionTwo, firstKey + 2 * numberOfBuckets, lastKey + 2 * numberOfBuckets));

    invokeAsync(vm2, putFromOneVm(regionTwo, firstKey, lastKey));
    invokeAsync(vm2, putFromOneVm(regionTwo, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm2, putFromOneVm(regionTwo, firstKey + 2 * numberOfBuckets, lastKey + 2 * numberOfBuckets));

    invokeAsync(vm3, putFromOneVm(regionTwo, firstKey, lastKey));
    invokeAsync(vm3, putFromOneVm(regionTwo, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm3, putFromOneVm(regionTwo, firstKey + 2 * numberOfBuckets, lastKey + 2 * numberOfBuckets));

    awaitAllAsyncInvocations();

    // validating bucket distribution over all the nodes
    int numberOfBucketsExpectedOnEachNode = getNumberOfBucketsExpectedOnEachNode() - 4;

    vm0.invoke(validateBucketsDistribution(regionOne, numberOfBucketsExpectedOnEachNode));
    vm1.invoke(validateBucketsDistribution(regionOne, numberOfBucketsExpectedOnEachNode));
    vm2.invoke(validateBucketsDistribution(regionOne, numberOfBucketsExpectedOnEachNode));
    vm2.invoke(validateBucketsDistribution(regionOne, numberOfBucketsExpectedOnEachNode));

    vm0.invoke(validateBucketsDistribution(regionTwo, numberOfBucketsExpectedOnEachNode));
    vm1.invoke(validateBucketsDistribution(regionTwo, numberOfBucketsExpectedOnEachNode));
    vm2.invoke(validateBucketsDistribution(regionTwo, numberOfBucketsExpectedOnEachNode));
    vm2.invoke(validateBucketsDistribution(regionTwo, numberOfBucketsExpectedOnEachNode));
  }

  /**
   * This test performs following operations
   *
   * <p>
   * 1. Creates multiple partition regions in 3 vms with scope DISTRIBUTED_ACK and
   * DISTRIBUTED_NO_ACK
   *
   * <p>
   * 2. Performs Put() operation from 3 the vms for the keys startIndexForRgion to
   * enIndexForRegion
   *
   * <p>
   * 3. Creates partition region on new node
   *
   * <p>
   * 4. Performs Put() operation from 3 the vms for the keys firstRegion to
   * enIndexForRegion.
   *
   * <p>
   * 5. Validate bucket creation on new node.
   */
  @Test
  public void testBucketDistributionAfterNodeAdditionInPR() throws Exception {
    // start/end indexes for keys
    firstKey = 0;
    lastKey = 5;

    vm0.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, redundancy, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, redundancy, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, redundancy, numberOfBuckets));

    vm0.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, redundancy, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, redundancy, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, redundancy, numberOfBuckets));

    // doing put() in multiple partition regions from 3 nodes.
    putInMultiplePartitionedRegionFrom3Nodes(firstRegion, lastRegion,
        firstKey, lastKey);

    vm3.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, redundancy, numberOfBuckets));
    vm3.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, redundancy, numberOfBuckets));

    firstKey = 5;
    lastKey = numberOfBuckets;

    // doing put() in multiple partition regions from 3 nodes.
    putInMultiplePartitionedRegionFrom3Nodes(firstRegion, lastRegion,
        firstKey, lastKey);

    int delta = (lastKey - firstKey) / 3;

    asyncInvocations.clear();

    invokeAsync(vm0, putInMultiplePartitionRegion(regionOne, firstKey, firstKey + 1 * delta));
    invokeAsync(vm1, putInMultiplePartitionRegion(regionOne, firstKey + 1 * delta, firstKey + 2 * delta));
    invokeAsync(vm2, putInMultiplePartitionRegion(regionOne, firstKey + 2 * delta, lastKey));

    invokeAsync(vm0, putInMultiplePartitionRegion(regionTwo, firstKey, firstKey + 1 * delta));
    invokeAsync(vm1, putInMultiplePartitionRegion(regionTwo, firstKey + 1 * delta, firstKey + 2 * delta));
    invokeAsync(vm2, putInMultiplePartitionRegion(regionTwo, firstKey + 2 * delta, lastKey));

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
    lastRegion = 1;

    redundancy = 1;

    vm0.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, redundancy, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, redundancy, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, redundancy, numberOfBuckets));
    vm3.invoke(() -> createPartitionRegion(regionOne, localMaxMemory, redundancy, numberOfBuckets));

    vm0.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, redundancy, numberOfBuckets));
    vm1.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, redundancy, numberOfBuckets));
    vm2.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, redundancy, numberOfBuckets));
    vm3.invoke(() -> createPartitionRegion(regionTwo, localMaxMemory, redundancy, numberOfBuckets));

    firstKey = 0;
    lastKey = 20;
    putInMultiplePartitionRegionFromOneVm(vm0, firstRegion, lastRegion, firstKey, lastKey);

    asyncInvocations.clear();

    invokeAsync(vm0, putFromOneVm(regionOne, firstKey, lastKey));
    invokeAsync(vm0, putFromOneVm(regionOne, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm0, putFromOneVm(regionOne, firstKey + 2 * numberOfBuckets, lastKey + 2 * numberOfBuckets));

    invokeAsync(vm0, putFromOneVm(regionTwo, firstKey, lastKey));
    invokeAsync(vm0, putFromOneVm(regionTwo, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm0, putFromOneVm(regionTwo, firstKey + 2 * numberOfBuckets, lastKey + 2 * numberOfBuckets));

    awaitAllAsyncInvocations();

    int expectedNumberOfBuckets = 11;

    vm0.invoke(validateTotalNumberOfBuckets(regionOne, expectedNumberOfBuckets));
    vm1.invoke(validateTotalNumberOfBuckets(regionOne, expectedNumberOfBuckets));
    vm2.invoke(validateTotalNumberOfBuckets(regionOne, expectedNumberOfBuckets));
    vm3.invoke(validateTotalNumberOfBuckets(regionOne, expectedNumberOfBuckets));

    vm0.invoke(validateTotalNumberOfBuckets(regionTwo, expectedNumberOfBuckets));
    vm1.invoke(validateTotalNumberOfBuckets(regionTwo, expectedNumberOfBuckets));
    vm2.invoke(validateTotalNumberOfBuckets(regionTwo, expectedNumberOfBuckets));
    vm3.invoke(validateTotalNumberOfBuckets(regionTwo, expectedNumberOfBuckets));

    firstKey = 200;
    lastKey = 400;

    asyncInvocations.clear();

    long delta = (lastKey - firstKey) / 4;

    invokeAsync(vm0, putFromOneVm(firstKey, firstKey + 1 * delta, firstRegion, lastRegion));
    invokeAsync(vm1, putFromOneVm(firstKey + 1 * delta, firstKey + 2 * delta, firstRegion, lastRegion));
    invokeAsync(vm2, putFromOneVm(firstKey + 2 * delta, firstKey + 3 * delta, firstRegion, lastRegion));
    invokeAsync(vm3, putFromOneVm(firstKey + 3 * delta, lastKey, firstRegion, lastRegion));

    firstKey += numberOfBuckets;
    lastKey += numberOfBuckets;
    delta = (lastKey - firstKey) / 4;

    invokeAsync(vm0, putFromOneVm(firstKey, firstKey + 1 * delta, firstRegion, lastRegion));
    invokeAsync(vm1, putFromOneVm(firstKey + 1 * delta, firstKey + 2 * delta, firstRegion, lastRegion));
    invokeAsync(vm2, putFromOneVm(firstKey + 2 * delta, firstKey + 3 * delta, firstRegion, lastRegion));
    invokeAsync(vm3, putFromOneVm(firstKey + 3 * delta, lastKey, firstRegion, lastRegion));

    awaitAllAsyncInvocations();
    
    vm0.invoke(validateTotalNumberOfBuckets(regionOne, expectedNumberOfBuckets));
    vm1.invoke(validateTotalNumberOfBuckets(regionOne, expectedNumberOfBuckets));
    vm2.invoke(validateTotalNumberOfBuckets(regionOne, expectedNumberOfBuckets));
    vm3.invoke(validateTotalNumberOfBuckets(regionOne, expectedNumberOfBuckets));

    vm0.invoke(validateTotalNumberOfBuckets(regionTwo, expectedNumberOfBuckets));
    vm1.invoke(validateTotalNumberOfBuckets(regionTwo, expectedNumberOfBuckets));
    vm2.invoke(validateTotalNumberOfBuckets(regionTwo, expectedNumberOfBuckets));
    vm3.invoke(validateTotalNumberOfBuckets(regionTwo, expectedNumberOfBuckets));
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
      partitionAttributesFactory.setRedundantCopies(0).setLocalMaxMemory(10)
          .setTotalNumBuckets(23);

      RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
      regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

      regionFactory.create(regionName);
    });

    vm0.invoke(() -> {
      Cache cache = getCache();
      Region region = cache.getRegion(regionName);
      // create all the buckets
      for (int i = 0; i < numberOfBuckets; i++) {
        region.put(i, "value-" + i);
      }
    });

    // TODO: there is no validation!!
  }

  /**
   * This function performs put() operations in multiple Partition Regions. Range of the keys which
   * are put is firstKey to lastKey. Each Vm puts different set of keys.
   */
  private void putInMultiplePartitionedRegionFrom3Nodes(int firstRegion,
      int lastRegion, int firstKey, int lastKey)
      throws ExecutionException, InterruptedException {

    int delta = (lastKey - firstKey) / 3;

    asyncInvocations.clear();

    invokeAsync(vm0, putInMultiplePartitionRegion(regionName, firstKey,
        firstKey + 1 * delta, firstRegion, lastRegion));
    invokeAsync(vm1, putInMultiplePartitionRegion(regionName, firstKey + 1 * delta,
            firstKey + 2 * delta, firstRegion, lastRegion));
    invokeAsync(vm2, putInMultiplePartitionRegion(regionName,
        firstKey + 2 * delta, lastKey, firstRegion, lastRegion));

    awaitAllAsyncInvocations();
  }

  /**
   * This function performs put() operation from the single vm in multiple partition regions.
   */
  private void putInMultiplePartitionRegionFromOneVm(final VM vm, final int firstRegion,
      final int lastRegion, final long firstKey, final long lastKey)
      throws ExecutionException, InterruptedException {

    asyncInvocations.clear();

    invokeAsync(vm, putFromOneVm(firstKey, lastKey, firstRegion, lastRegion));
    invokeAsync(vm, putFromOneVm(firstKey + numberOfBuckets,
        lastKey + numberOfBuckets, firstRegion, lastRegion));
    invokeAsync(vm, putFromOneVm(firstKey + 2 * numberOfBuckets,
        lastKey + 2 * numberOfBuckets, firstRegion, lastRegion));

    awaitAllAsyncInvocations();
  }

  private void putInMultiplePartitionRegionFromOneVm(final VM vm, final String regionName, final int firstKey, final int lastKey)
      throws ExecutionException, InterruptedException {

    asyncInvocations.clear();

    invokeAsync(vm, putFromOneVm(regionName, firstKey, lastKey));
    invokeAsync(vm, putFromOneVm(regionName, firstKey + numberOfBuckets, lastKey + numberOfBuckets));
    invokeAsync(vm, putFromOneVm(regionName, firstKey + 2 * numberOfBuckets, lastKey + 2 * numberOfBuckets));

    awaitAllAsyncInvocations();
  }

  private CacheSerializableRunnable putFromOneVm(final String regionName, final int firstKey,
                                                 final int lastKey) {

    CacheSerializableRunnable putFromVm = new CacheSerializableRunnable("putFromOneVm") {

      @Override
      public void run2() {
        Cache cache = getCache();
          Region region = cache.getRegion(Region.SEPARATOR + regionName);
          for (int i = firstKey; i < lastKey; i++) {
            region.put(i, regionName + i);
          }
      }
    };
    return putFromVm;
  }

  /**
   * This function performs put() in multiple partition regions for the given node.
   */
  private CacheSerializableRunnable putFromOneVm(final long firstKey,
      final long lastKey, final int firstRegion, final int lastRegion) {

    CacheSerializableRunnable putFromVm = new CacheSerializableRunnable("putFromOneVm") {

      @Override
      public void run2() {
        Cache cache = getCache();
        for (int i = firstRegion; i < lastRegion; i++) {
          PartitionedRegion pr =
              (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName + (i));
          assertNotNull(pr);
          for (long k = firstKey; k < lastKey; k++) {
            Long key = new Long(k);
            pr.put(key, regionName + k);
          }
        }
      }
    };
    return putFromVm;
  }

  private CacheSerializableRunnable putFromOneVm(final String regionName, final long firstKey,
                                                 final long lastKey) {

    CacheSerializableRunnable putFromVm = new CacheSerializableRunnable("putFromOneVm") {

      @Override
      public void run2() {
        Cache cache = getCache();
          PartitionedRegion pr =
              (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName);
          assertNotNull(pr);
          for (long k = firstKey; k < lastKey; k++) {
            Long key = new Long(k);
            pr.put(key, regionName + k);
          }
      }
    };
    return putFromVm;
  }

  /**
   * This function performs put() operations in multiple Partition Regions. Range of the keys which
   * are put is firstKey to lastKey. Each Vm puts different set of keys.
   */
  private void putInMultiplePartitionedRegion(int firstRegion, int lastRegion,
      int firstKey, int lastKey) throws ExecutionException, InterruptedException {

    int delta = (lastKey - firstKey) / 4;

    asyncInvocations.clear();

    invokeAsync(vm0, putInMultiplePartitionRegion(regionName, firstKey,
        firstKey + 1 * delta, firstRegion, lastRegion));
    invokeAsync(vm1, putInMultiplePartitionRegion(regionName, firstKey + 1 * delta,
            firstKey + 2 * delta, firstRegion, lastRegion));
    invokeAsync(vm2, putInMultiplePartitionRegion(regionName, firstKey + 2 * delta,
            firstKey + 3 * delta, firstRegion, lastRegion));
    invokeAsync(vm3, putInMultiplePartitionRegion(regionName,
        firstKey + 3 * delta, lastKey, firstRegion, lastRegion));

    awaitAllAsyncInvocations();
  }

  private void putInMultiplePartitionedRegion(final String regionName, final int firstKey, final int lastKey) throws ExecutionException, InterruptedException {

    int delta = (lastKey - firstKey) / 4;

    asyncInvocations.clear();

    invokeAsync(vm0, putInMultiplePartitionRegion(regionName, firstKey, firstKey + 1 * delta));
    invokeAsync(vm1, putInMultiplePartitionRegion(regionName, firstKey + 1 * delta, firstKey + 2 * delta));
    invokeAsync(vm2, putInMultiplePartitionRegion(regionName, firstKey + 2 * delta, firstKey + 3 * delta));
    invokeAsync(vm3, putInMultiplePartitionRegion(regionName, firstKey + 3 * delta, lastKey));

    awaitAllAsyncInvocations();
  }

  /**
   * This function performs put() operations in multiple Partition Regions. Range of the keys which
   * are put is firstKey to edIndexForKey. Each Vm puts different set of keys
   */
  private void putInMultiplePartitionedRegionFromAllVms(int firstRegion,
      int lastRegion, long firstKey, long lastKey)
      throws ExecutionException, InterruptedException {

    asyncInvocations.clear();

    long delta = (lastKey - firstKey) / 4;

    invokeAsync(vm0, putFromOneVm(firstKey, firstKey + 1 * delta,
        firstRegion, lastRegion));
    invokeAsync(vm1, putFromOneVm(firstKey + 1 * delta,
        firstKey + 2 * delta, firstRegion, lastRegion));
    invokeAsync(vm2, putFromOneVm(firstKey + 2 * delta,
        firstKey + 3 * delta, firstRegion, lastRegion));
    invokeAsync(vm3, putFromOneVm(firstKey + 3 * delta, lastKey,
        firstRegion, lastRegion));

    firstKey += numberOfBuckets;
    lastKey += numberOfBuckets;
    delta = (lastKey - firstKey) / 4;

    invokeAsync(vm0, putFromOneVm(firstKey, firstKey + 1 * delta,
        firstRegion, lastRegion));
    invokeAsync(vm1, putFromOneVm(firstKey + 1 * delta,
        firstKey + 2 * delta, firstRegion, lastRegion));
    invokeAsync(vm2, putFromOneVm(firstKey + 2 * delta,
        firstKey + 3 * delta, firstRegion, lastRegion));
    invokeAsync(vm3, putFromOneVm(firstKey + 3 * delta, lastKey,
        firstRegion, lastRegion));

    awaitAllAsyncInvocations();
  }

  /**
   * This function performs validation of bucket regions of multiple partition regions on 4 VMs.
   */
  private void validateBucketsAfterPutInMultiplePartitionRegion(final int firstRegion,
      final int lastRegion) throws ExecutionException, InterruptedException {

    // validation of bucket regions creation
    asyncInvocations.clear();

    invokeAsync(vm0, validateBucketCreationAfterPut(firstRegion, lastRegion));
    invokeAsync(vm1, validateBucketCreationAfterPut(firstRegion, lastRegion));
    invokeAsync(vm2, validateBucketCreationAfterPut(firstRegion, lastRegion));
    invokeAsync(vm3, validateBucketCreationAfterPutForNode3(firstRegion, lastRegion));

    awaitAllAsyncInvocations();

  }

  private void validateBucketScopesAfterPutInMultiplePartitionRegion(final int firstRegion,
                                                                final int lastRegion) throws ExecutionException, InterruptedException {
    // validating scope of buckets
    asyncInvocations.clear();

    invokeAsync(vm0, validateBucketScopeAfterPut(firstRegion, lastRegion));
    invokeAsync(vm1, validateBucketScopeAfterPut(firstRegion, lastRegion));
    invokeAsync(vm2, validateBucketScopeAfterPut(firstRegion, lastRegion));
    invokeAsync(vm3, validateBucketCreationAfterPutForNode3(firstRegion, lastRegion));

    awaitAllAsyncInvocations();
  }

  /**
   * This function performs validation of bucket regions of multiple partition regions on 4 VMs.
   */
  private void validateBucketsDistributionInMultiplePartitionRegion(final int firstRegion,
      final int lastRegion, int noBucketsExpectedOnEachNode)
      throws ExecutionException, InterruptedException {

    asyncInvocations.clear();

    invokeAsync(vm0, validateBucketsDistribution(firstRegion, lastRegion,
        noBucketsExpectedOnEachNode));
    invokeAsync(vm1, validateBucketsDistribution(firstRegion, lastRegion,
        noBucketsExpectedOnEachNode));
    invokeAsync(vm2, validateBucketsDistribution(firstRegion, lastRegion,
        noBucketsExpectedOnEachNode));
    invokeAsync(vm3, validateBucketsDistribution(firstRegion, lastRegion,
        noBucketsExpectedOnEachNode));

    awaitAllAsyncInvocations();
  }

  /**
   * This function is used for the validation of bucket on all the region.
   */
  private void validateBucketsOnAllNodes(final int firstRegion,
      final int lastRegion) {
    CacheSerializableRunnable validateAllNodes =
        new CacheSerializableRunnable("validateBucketsOnAllNodes") {

          @Override
          public void run2() {
            Cache cache = getCache();
            int threshold = 0;
            for (int i = firstRegion; i < lastRegion; i++) {
              PartitionedRegion pr =
                  (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName + i);
              assertNotNull(pr);
              assertNotNull(pr.getDataStore());
              assertTrue(pr.getDataStore().localBucket2RegionMap.size() > threshold);
            }
          }
        };

    vm0.invoke(validateAllNodes);
    vm1.invoke(validateAllNodes);
    vm2.invoke(validateAllNodes);
    vm3.invoke(validateAllNodes);
  }

  private void validateBuckets(final VM vm, final int firstRegion,
                                         final int lastRegion) {
    vm.invoke(new CacheSerializableRunnable("validateBucketsOnAllNodes") {

      @Override
      public void run2() {
        Cache cache = getCache();
        int threshold = 0;
        for (int i = firstRegion; i < lastRegion; i++) {
          PartitionedRegion pr =
              (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName + i);
          assertNotNull(pr);
          assertNotNull(pr.getDataStore());
          assertTrue(pr.getDataStore().localBucket2RegionMap.size() > threshold);
        }
      }
    });
  }

  private void validateBuckets(final VM vm, final String regionName) {
    vm.invoke(new CacheSerializableRunnable("validateBucketsOnAllNodes") {

      @Override
      public void run2() {
        Cache cache = getCache();
        int threshold = 0;
        PartitionedRegion region =
            (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName);
        assertNotNull(region.getDataStore());
        assertTrue(region.getDataStore().localBucket2RegionMap.size() > threshold);
      }
    });
  }

  private void validateBuckets(final String regionName) {
    Cache cache = getCache();
    int threshold = 0;
    PartitionedRegion region =
        (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(region.getDataStore());
    assertTrue(region.getDataStore().localBucket2RegionMap.size() > threshold);
  }

  /**
   * This functions performs following validations on the partitions regions
   *
   * <p>
   * (1) Size of bucket2Node region should be > 0.
   *
   * <p>
   * (3) In case of the partition regions with redundancy > 0 scope of the bucket regions should be
   * scope of the partition regions.
   *
   * <p>
   * (4) In case of the partition regions with redundancy > 0 no two bucket regions with same
   * bucketId should be generated on the same node.
   */
  private CacheSerializableRunnable validateBucketCreationAfterPut(final int firstRegion,
      final int lastRegion) {

    CacheSerializableRunnable validateAfterPut = new CacheSerializableRunnable("validateAfterPut") {

      @Override
      public void run2() {
        Cache cache = getCache();
        Region root = cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME);
        assertNotNull("Root regions is null", root);
        for (int i = firstRegion; i < lastRegion; i++) {
          PartitionedRegion pr =
              (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName + i);

          assertTrue(pr.getRegionAdvisor().getBucketSet().size() > 0);
          assertTrue("Size of local region map should be > 0 for region: " + pr.getFullPath(),
              pr.getDataStore().localBucket2RegionMap.size() > 0);
          // taking the buckets which are local to the node and not all the
          // available buckets.
          Set bucketIds = pr.getDataStore().localBucket2RegionMap.keySet();
          Iterator buckteIdItr = bucketIds.iterator();
          while (buckteIdItr.hasNext()) {
            Integer key = (Integer) buckteIdItr.next();
            BucketRegion val = (BucketRegion) pr.getDataStore().localBucket2RegionMap.get(key);

            Region bucketRegion = root.getSubregion(pr.getBucketName(key.intValue()));
            assertTrue(bucketRegion.getFullPath().equals(val.getFullPath()));
            // Bucket region should not be null
            assertNotNull("Bucket region cannot be null", bucketRegion);
            // Parent region of the bucket region should be root
            assertEquals("Parent region is not root", root, bucketRegion.getParentRegion());
          }
        }
      }
    };
    return validateAfterPut;
  }

  private CacheSerializableRunnable validateBucketCreationAfterPut(final String regionName) {

    CacheSerializableRunnable validateAfterPut = new CacheSerializableRunnable("validateAfterPut") {

      @Override
      public void run2() {
        Cache cache = getCache();
        Region root = cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME);
        assertNotNull("Root regions is null", root);
        PartitionedRegion pr =
            (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName);

        assertTrue(pr.getRegionAdvisor().getBucketSet().size() > 0);
        assertTrue("Size of local region map should be > 0 for region: " + pr.getFullPath(),
            pr.getDataStore().localBucket2RegionMap.size() > 0);
        // taking the buckets which are local to the node and not all the
        // available buckets.
        Set bucketIds = pr.getDataStore().localBucket2RegionMap.keySet();
        Iterator buckteIdItr = bucketIds.iterator();
        while (buckteIdItr.hasNext()) {
          Integer key = (Integer) buckteIdItr.next();
          BucketRegion val = (BucketRegion) pr.getDataStore().localBucket2RegionMap.get(key);

          Region bucketRegion = root.getSubregion(pr.getBucketName(key.intValue()));
          assertTrue(bucketRegion.getFullPath().equals(val.getFullPath()));
          // Bucket region should not be null
          assertNotNull("Bucket region cannot be null", bucketRegion);
          // Parent region of the bucket region should be root
          assertEquals("Parent region is not root", root, bucketRegion.getParentRegion());
        }
      }
    };
    return validateAfterPut;
  }

  private CacheSerializableRunnable validateBucketCreationAfterPutForNode3(
      final int firstRegion, final int lastRegion) {

    CacheSerializableRunnable validateAfterPut =
        new CacheSerializableRunnable("validateBucketCreationAfterPutForNode3") {

          @Override
          public void run2() {
            Cache cache = getCache();
            for (int i = firstRegion; i < lastRegion; i++) {
              PartitionedRegion pr =
                  (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName + i);
              assertNotNull("This Partition Region is null " + pr.getName(), pr);
              assertNull("DataStore should be null", pr.getDataStore());
            }
          }
        };
    return validateAfterPut;
  }

  private CacheSerializableRunnable validateBucketCreationAfterPutForNode3(final String regionName) {

    CacheSerializableRunnable validateAfterPut =
        new CacheSerializableRunnable("validateBucketCreationAfterPutForNode3") {

          @Override
          public void run2() {
            Cache cache = getCache();
            PartitionedRegion pr =
                (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName);
            assertNotNull("This Partition Region is null " + pr.getName(), pr);
            assertNull("DataStore should be null", pr.getDataStore());
          }
        };
    return validateAfterPut;
  }

  private CacheSerializableRunnable validateBucketScopeAfterPut(final int firstRegion,
      final int lastRegion) {

    CacheSerializableRunnable validateAfterPut =
        new CacheSerializableRunnable("validateBucketScopeAfterPut") {

          @Override
          public void run2() {
            Cache cache = getCache();
            Region root = cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME);

            for (int i = firstRegion; i < lastRegion; i++) {
              PartitionedRegion region =
                  (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName + i);

              assertTrue(region.getRegionAdvisor().getBucketSet().size() > 0);
              // taking the buckets which are local to the node and not all the available buckets.
              Set <Integer> bucketIds = region.getDataStore().localBucket2RegionMap.keySet();
              for (Integer key : bucketIds) {
                Region bucketRegion = root.getSubregion(region.getBucketName(key));
                assertNotNull("Bucket region cannot be null", bucketRegion);
                assertEquals(Scope.DISTRIBUTED_ACK, bucketRegion.getAttributes().getScope());
              } // while
            }
          }
        };
    return validateAfterPut;
  }

  private CacheSerializableRunnable validateBucketScopeAfterPut(final String regionName) {

    CacheSerializableRunnable validateAfterPut =
        new CacheSerializableRunnable("validateBucketScopeAfterPut") {

          @Override
          public void run2() {
            Cache cache = getCache();
            Region root = cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME);

            PartitionedRegion region =
                (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName);

            assertTrue(region.getRegionAdvisor().getBucketSet().size() > 0);
            // taking the buckets which are local to the node and not all the available buckets.
            Set <Integer> bucketIds = region.getDataStore().localBucket2RegionMap.keySet();
            for (Integer key : bucketIds) {
              Region bucketRegion = root.getSubregion(region.getBucketName(key));
              assertNotNull("Bucket region cannot be null", bucketRegion);
              assertEquals(Scope.DISTRIBUTED_ACK, bucketRegion.getAttributes().getScope());
            } // while
          }
        };
    return validateAfterPut;
  }

  /**
   * This function validates bucket2Node region of the Partition regiion before any put()
   * operations.
   *
   * <p>
   * It performs following validations:
   *
   * <p>
   * (1) bucket2Node Region should not be null.
   *
   * <p>
   * (2) Scope of the bucket2Node region should be DISTRIBUTED_ACK.
   *
   * <p>
   * (3) Size of bucket2Node region should be 0.
   *
   * <p>
   * (4) Parent region of the bucket2Node region shoud be root i.e. region with name "PRRoot".
   */
  private CacheSerializableRunnable validateBucket2NodeBeforePut(final int firstRegion,
      final int lastRegion) {

    CacheSerializableRunnable validateBucketBeforePut =
        new CacheSerializableRunnable("Bucket2NodeValidation") {

          @Override
          public void run2() {
            Cache cache = getCache();
            Region root = cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME);
            assertNotNull("Root regions is null", root);

            for (int i = firstRegion; i < lastRegion; i++) {
              PartitionedRegion region =
                  (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName + i);
              assertNotNull(region);

              assertTrue(region.getRegionAdvisor().getNumProfiles() > 0);
              assertTrue(region.getRegionAdvisor().getNumDataStores() > 0);
              int bucketSetSize = region.getRegionAdvisor().getCreatedBucketsCount();

              if (bucketSetSize != 0) {
                Set buckets = region.getRegionAdvisor().getBucketSet();
                Iterator it = buckets.iterator();
                int numBucketsWithStorage = 0;
                try {
                  while (true) {
                    Integer bucketId = (Integer) it.next();
                    region.getRegionAdvisor().getBucket(bucketId.intValue()).getBucketAdvisor()
                        .dumpProfiles("Bucket owners for bucket "
                            + region.bucketStringForLogs(bucketId.intValue()));
                    numBucketsWithStorage++;
                  }
                } catch (NoSuchElementException end) {
                  LogWriterUtils.getLogWriter()
                      .info("BucketSet iterations " + numBucketsWithStorage);
                }
                fail("There should be no buckets assigned");
              }
            }
          }
        };
    return validateBucketBeforePut;
  }

  private CacheSerializableRunnable validateBucket2NodeBeforePut(final String regionName) {

    CacheSerializableRunnable validateBucketBeforePut =
        new CacheSerializableRunnable("Bucket2NodeValidation") {

          @Override
          public void run2() {
            Cache cache = getCache();
            Region root = cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME);
            assertNotNull("Root regions is null", root);

            PartitionedRegion region =
                (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName);
            assertNotNull(region);

            assertTrue(region.getRegionAdvisor().getNumProfiles() > 0);
            assertTrue(region.getRegionAdvisor().getNumDataStores() > 0);
            int bucketSetSize = region.getRegionAdvisor().getCreatedBucketsCount();

            if (bucketSetSize != 0) {
              Set buckets = region.getRegionAdvisor().getBucketSet();
              Iterator it = buckets.iterator();
              int numBucketsWithStorage = 0;
              try {
                while (true) {
                  Integer bucketId = (Integer) it.next();
                  region.getRegionAdvisor().getBucket(bucketId.intValue()).getBucketAdvisor()
                      .dumpProfiles("Bucket owners for bucket "
                          + region.bucketStringForLogs(bucketId.intValue()));
                  numBucketsWithStorage++;
                }
              } catch (NoSuchElementException end) {
                LogWriterUtils.getLogWriter()
                    .info("BucketSet iterations " + numBucketsWithStorage);
              }
              fail("There should be no buckets assigned");
            }
          }
        };
    return validateBucketBeforePut;
  }

  private CacheSerializableRunnable validateBucketsDistribution(final int firstRegion,
      final int lastRegion, final int numberOfBucketsExpectedOnEachNode) {

    CacheSerializableRunnable validateBucketDist =
        new CacheSerializableRunnable("validateBucketsDistribution") {

          @Override
          public void run2() {
            Cache cache = getCache();
            Region root = cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME);

            for (int i = firstRegion; i < lastRegion; i++) {
              PartitionedRegion region =
                  (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName + i);

              assertNotNull(region.getDataStore());
              int localBSize = region.getDataStore().getBucketsManaged();

              assertTrue("Bucket Distribution for region = " + region.getFullPath()
                  + " is not correct for member " + region.getDistributionManager().getId()
                  + " existing size " + localBSize + " smaller than expected "
                  + numberOfBucketsExpectedOnEachNode, localBSize >= numberOfBucketsExpectedOnEachNode);

              region.getDataStore().visitBuckets(new BucketVisitor() {
                @Override
                public void visit(Integer bucketId, Region r) {
                  Region bucketRegion = root.getSubregion(region.getBucketName(bucketId.intValue()));
                  assertEquals(bucketRegion.getFullPath(), r.getFullPath());
                }
              });
            }
          }
        };
    return validateBucketDist;
  }

  private CacheSerializableRunnable validateBucketsDistribution(final String regionName, final int numberOfBucketsExpectedOnEachNode) {

    CacheSerializableRunnable validateBucketDist =
        new CacheSerializableRunnable("validateBucketsDistribution") {

          @Override
          public void run2() {
            Cache cache = getCache();
            Region root = cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME);

            PartitionedRegion region =
                (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName);

            assertNotNull(region.getDataStore());
            int localBSize = region.getDataStore().getBucketsManaged();

            assertTrue("Bucket Distribution for region = " + region.getFullPath()
                + " is not correct for member " + region.getDistributionManager().getId()
                + " existing size " + localBSize + " smaller than expected "
                + numberOfBucketsExpectedOnEachNode, localBSize >= numberOfBucketsExpectedOnEachNode);

            region.getDataStore().visitBuckets(new BucketVisitor() {
              @Override
              public void visit(Integer bucketId, Region r) {
                Region bucketRegion = root.getSubregion(region.getBucketName(bucketId.intValue()));
                assertEquals(bucketRegion.getFullPath(), r.getFullPath());
              }
            });
          }
        };
    return validateBucketDist;
  }

  private void createPartitionRegion(final VM vm, final int firstRegion, final int lastRegion,
                                     final int localMaxMemory, final int redundancy) {
    vm.invoke(createMultiplePRWithTotalNumBucketPropSet(regionName, firstRegion,
        lastRegion, redundancy, localMaxMemory, 11));
  }

  private void createPartitionRegion(final String regionName, final int localMaxMemory, final int redundancy, final int numberOfBuckets) {
    Cache cache = getCache();
    cache.createRegion(regionName, createRegionAttrs(redundancy, localMaxMemory, numberOfBuckets));
  }

  private void createPRWithTotalNumPropSetList(final VM vm, final int firstRegion,
                                               final int lastRegion, final int localMaxMemory, final int redundancy) {
    vm.invoke(createMultiplePRWithTotalNumBucketPropSet(regionName, firstRegion,
        lastRegion, redundancy, localMaxMemory, 11));
  }

  private CacheSerializableRunnable createMultiplePRWithTotalNumBucketPropSet(final String regionName,
                                                                              final int firstRegion,
                                                                              final int lastRegion,
                                                                              final int redundancy,
                                                                              final int localMaxMemory,
                                                                              final int numberOfBuckets) {
    CacheSerializableRunnable createPRWithTotalNumBucketPropSet =
        new CacheSerializableRunnable("createPRWithTotalNumBucketPropSet") {

          @Override
          public void run2() throws CacheException {
            Cache cache = getCache();
            for (int i = firstRegion; i < lastRegion; i++) {
              cache.createRegion(regionName + i,
                  createRegionAttrs(redundancy, localMaxMemory, numberOfBuckets));
            }
          }
        };
    return createPRWithTotalNumBucketPropSet;
  }

  private void validateTotalNumBuckets(final VM vm, final String regionName, final int firstRegion,
                                       final int lastRegion, final int expectedNumberOfBuckets) {
    vm.invoke(validateTotalNumberOfBuckets(regionName, expectedNumberOfBuckets, firstRegion, lastRegion));
  }

  /**
   * This function validates total number of buckets from bucket2NodeRegion of partition region.
   */
  private CacheSerializableRunnable validateTotalNumberOfBuckets(final String regionName,
                                                                 final int expectedNumBuckets,
                                                                 final int firstRegion,
                                                                 final int lastRegion) {
    CacheSerializableRunnable validateTotNumBuckets =
        new CacheSerializableRunnable("validateTotNumBuckets") {

          @Override
          public void run2() throws CacheException {
            Cache cache = getCache();
            for (int i = firstRegion; i < lastRegion; i++) {
              PartitionedRegion pr =
                  (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionName + i);
              assertNotNull("This region is null " + pr.getName(), pr);

              Set bucketsWithStorage = pr.getRegionAdvisor().getBucketSet();
              assertEquals(expectedNumBuckets, bucketsWithStorage.size());
            }
            LogWriterUtils.getLogWriter()
                .info("Total Number of buckets validated in partition region");
          }
        };
    return validateTotNumBuckets;
  }

  private CacheSerializableRunnable validateTotalNumberOfBuckets(final String regionName,
                                                                 final int expectedNumBuckets) {
    CacheSerializableRunnable validateTotNumBuckets =
        new CacheSerializableRunnable("validateTotNumBuckets") {

          @Override
          public void run2() throws CacheException {
            Cache cache = getCache();
            PartitionedRegion region =
                (PartitionedRegion) cache.getRegion(regionName);

            Set bucketsWithStorage = region.getRegionAdvisor().getBucketSet();
            assertEquals(expectedNumBuckets, bucketsWithStorage.size());
          }
        };
    return validateTotNumBuckets;
  }

  private int getNumberOfBucketsExpectedOnEachNode() {
    return (numberOfBuckets / 4) - 1;
  }

  private RegionAttributes createRegionAttrs(final int redundancy, final int loclMaxMemory, final int numberOfBuckets) {
    AttributesFactory attr = new AttributesFactory();
    attr.setMirrorType(MirrorType.NONE);
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes prAttr = paf.setRedundantCopies(redundancy).setLocalMaxMemory(loclMaxMemory)
        .setTotalNumBuckets(numberOfBuckets).create();
    attr.setPartitionAttributes(prAttr);
    return attr.create();
  }

  /**
   * This function performs put() operations in multiple Partition Regions
   */
  private CacheSerializableRunnable putInMultiplePartitionRegion(final String regionName,
                                                                 final int firstKey,
                                                                 final int lastKey,
                                                                 final int startIndexNumOfRegions,
                                                                 final int endIndexNumOfRegions) {
    CacheSerializableRunnable putInPRs = new CacheSerializableRunnable("doPutOperations") {

      @Override
      public void run2() throws CacheException {
        for (int i = startIndexNumOfRegions; i < endIndexNumOfRegions; i++) {
          Region<String, String> region = getCache().getRegion(Region.SEPARATOR + regionName + i);
          for (int j = firstKey; j < lastKey; j++) {
            region.put(i + regionName + j, regionName + j);
          }
        }
      }
    };
    return putInPRs;
  }

  private CacheSerializableRunnable putInMultiplePartitionRegion(final String regionName,
                                                                 final int firstKey,
                                                                 final int lastKey) {
    CacheSerializableRunnable putInPRs = new CacheSerializableRunnable("doPutOperations") {

      @Override
      public void run2() throws CacheException {
        Region<String, String> region = getCache().getRegion(regionName);
        for (int i = firstKey; i < lastKey; i++) {
          region.put(i + regionName + i, regionName + i);
        }
      }
    };
    return putInPRs;
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
