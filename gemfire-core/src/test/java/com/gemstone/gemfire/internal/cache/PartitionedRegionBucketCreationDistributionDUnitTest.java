/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionedRegionStorageException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore.BucketVisitor;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * 
 * This class tests bucket Creation and distribution for the multiple Partition
 * regions.
 * @author gthombar, modified by Tushar (for bug#35713)
 */
public class PartitionedRegionBucketCreationDistributionDUnitTest extends
    PartitionedRegionDUnitTestCase
{

  /** Prefix is used in name of Partition Region */
  protected static String prPrefix = null;

  /** Maximum number of regions * */
  static final int MAX_REGIONS = 2;

  /** redundancy used for the creation of the partition region */
  final int redundancy = 0;

  /** local maxmemory used for the creation of the partition region */
  int localMaxMemory = 200;

  /** max number of buckets */
  int totalBucketNumProperty = 11;

  /** to store references of 4 vms */
  VM vm[] = new VM[4];

  /** constructor */
  public PartitionedRegionBucketCreationDistributionDUnitTest(String name) {
    super(name);
  }

  /**
   * This test performs following operations <br>
   * 1. Validate bucket2Node region of the partition regions.</br><br>
   * (a) bucket2Node Region should not be null.</br><br>
   * (b) Scope of the bucket2Node region should be DISTRIBUTED_ACK.</br><br>
   * (c) Size of bucket2Node region should be 0 before any put() operation.
   * </br><br>
   * (d) Parent region of the bucket2Node region should be root i.e. region with
   * name "PRRoot".</br><br>
   * 2. Do put() operation from the different VMs so that buckets gets
   * generated.</br><br>
   * 3. Validate bucket regions of multiple partition Regions</br><br>
   * (a) Size of bucket2Node region should be > 0.</br><br>
   * (b) In case of the partition regions with redundancy > 0 scope of the
   * bucket regions should be scope of the partition regions.</br><br>
   * (c) In case of the partition regions with redundancy > 0 no two bucket
   * regions with same bucketId should not be present on the same node.</br>
   */
  public void testBucketCreationInMultiplePartitionRegion() throws Throwable
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);

    /** Prefix will be used for naming the partititon Region */
    prPrefix = getUniqueName();

    /** these indices represents range of partition regions present in each VM */
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    /** Start index for key */
    int startIndexForKey = 0;
    /** End index for key */
    int endIndexForKey = 50;

    //creating partition regions
    createMultiplePR(startIndexForRegion, endIndexForRegion);
    // validating bucket2Node of multiple partition regions before doing any
    // put().
    validateBucket2NodeBeforePutInMultiplePartitionedRegion(
        startIndexForRegion, endIndexForRegion);
    getLogWriter()
        .info(
            "testBucketCerationInMultiPlePartitionRegion() - Bucket2Node region of partition regions before any put() successfully validated ");
    // doing put() operation on multiple partition region
    putInMultiplePartitionedRegion(startIndexForRegion, endIndexForRegion,
        startIndexForKey, endIndexForKey);
    getLogWriter()
        .info(
            "testBucketCerationInMultiPlePartitionRegion() - Put() operation successfully in partition regions");
    // validating bucket regions of multiple partition regions.
    validateBucketsAfterPutInMultiplePartitionRegion(startIndexForRegion,
        endIndexForRegion);
    getLogWriter()
        .info(
            "testBucketCerationInMultiPlePartitionRegion() - Bucket regions of partition regions successfully validated");
    getLogWriter().info(
        "testBucketCerationInMultiPlePartitionRegion() Successfully completed");
  }

  /**
   * This test performs following operations <br>
   * 1.Creates multiple partition regions in 4 vms</br><br>
   * 2. Performs Put() operation from vm0 for the keys 0 to 111.</br><br>
   * 3. Validates bucket distribution over all the nodes for multiple partition
   * regions.</br>
   */
  public void testBucketCreationInPRPutFromOneNode() throws Throwable
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);

    /** Prefix will be used for naming the partititon Region */
    prPrefix = "testBucketCreationInPRFromOneNode";
    /** these indices represents range of partition regions present in each VM */
    final int startIndexForRegion = 0;
    final int endIndexForRegion = MAX_REGIONS;
    /** Start index for key */
    final long startIndexForKey = 0;
    /** End index for key */
    final long endIndexForKey = totalBucketNumProperty;

    final int startIndexForNode = 0;
    final int endIndexForNode = 4;
    List vmList;
    // creating multiple partition regions on 3 nodes localMaxMemory=200 redundancy = 0
//    int midIndexForRegion = (endIndexForRegion - startIndexForRegion) / 2;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 200;
    // redundancy = 0;
    createPartitionRegion(vmList, startIndexForRegion, endIndexForRegion,
        localMaxMemory, redundancy);
        
    getLogWriter()
        .info(
            "testBucketCerationInMultiPlePartitionRegion() - Partition Regions successfully created ");
    // doing put() operation from vm0 only
    putInMultiplePartitionRegionFromOneVm(vm[0], startIndexForRegion,
        endIndexForRegion, startIndexForKey, endIndexForKey);
    getLogWriter()
        .info(
            "testBucketCerationInMultiPlePartitionRegion() - Put() Opereration done only from one VM ");
    // validating bucket distribution ovar all the nodes
    int noBucketsExpectedOnEachNode = getNoBucketsExpectedOnEachNode();
    validateBucketsDistributionInMultiplePartitionRegion(startIndexForRegion,
        endIndexForRegion, noBucketsExpectedOnEachNode);
    getLogWriter()
        .info(
            "testBucketCerationInMultiPlePartitionRegion() - Bucket regions are equally distributed");
    getLogWriter().info(
        "testBucketCerationInMultiPlePartitionRegion() successfully completed");
  }

  /**
   * This test performs following operations <br>
   * 1. Creates multiple partition regions in 4 vms with scope DISTRIBUTED_ACK
   * and DISTRIBUTED_NO_ACK.</br><br>
   * 2. Performs Put() operation from all the vms for the keys 0 to 111.</br>
   * <br>
   * 3. Validates bucket distribution over all the nodes for multiple partition
   * regions.</br>
   */
  public void testBucketCreationInMultiplePartitionRegionFromAllNodes()
      throws Throwable
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);

    /** Prefix will be used for naming the partititon Region */
    prPrefix = "testBucketCreationInMultiplePartitionRegionFromAllNodes";

    /** these indices represents range of partition regions present in each VM */
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    /** Start index for key */
    long startIndexForKey = 0;
    /** End index for key */
    long endIndexForKey = totalBucketNumProperty;

    int startIndexForNode = 0;
    int endIndexForNode = 4;
    List vmList;
    // creating multiple partition regions on 3 nodes with scope =
    // DISTRIBUTED_ACK localMaxMemory=200 redundancy = 0
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 200;
    // redundancy = 0;
    // creating multiple partition regions on 3 nodes with localMaxMemory=200 redundancy = 0
    createPartitionRegion(vmList, startIndexForRegion, endIndexForRegion,
        localMaxMemory, redundancy);
    getLogWriter()
        .info(
            "testBucketCerationInMultiPlePartitionRegion() - Partition Regions successfully created ");
    // doing put() operation from all vms
    putInMultiplePartitionedRegionFromAllVms(startIndexForRegion,
        endIndexForRegion, startIndexForKey, endIndexForKey);
    getLogWriter()
        .info(
            "testBucketCerationInMultiPlePartitionRegion() - Put() Opereration done only from one VM ");
    // validating bucket distribution ovar all the nodes
    int noBucketsExpectedOnEachNode = getNoBucketsExpectedOnEachNode() - 4;
    validateBucketsDistributionInMultiplePartitionRegion(startIndexForRegion,
        endIndexForRegion, noBucketsExpectedOnEachNode);
    getLogWriter()
        .info(
            "testBucketCerationInMultiPlePartitionRegion() - Bucket regions are equally distributed");
    getLogWriter().info(
        "testBucketCerationInMultiPlePartitionRegion() successfully created");
  }

  /**
   * This test performs following operations <br>
   * 1. Creates multiple partition regions in 3 vms with scope DISTRIBUTED_ACK
   * and DISTRIBUTED_NO_ACK.</br><br>
   * 2. Performs Put() operation from 3 the vms for the keys startIndexForRgion
   * to enIndexForRegion.</br><br>
   * 3. Creates partition region on new node</br><br>
   * 4. Performs Put() operation from 3 the vms for the keys startIndexForRgion
   * to enIndexForRegion.</br><br>
   * 5. Validate bucket creation on new node.</br>
   */
  public void testBucketDistributionAfterNodeAdditionInPR() throws Throwable
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);
    /** Prefix will be used for naming the partititon Region */
    prPrefix = "testBucketDistributionAfterNodeAdditionInPR";

    /** these indices represents range of partition regions present in each VM */
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    /** Start index for key */
    int startIndexForKey = 0;
    /** End index for key */
    int endIndexForKey = 5;

    int startIndexForNode = 0;
    int endIndexForNode = 4;
    // creating partition regions on 4 nodes out of which partition regions on
    // one node are only accessors.
    // partition regions on vm3 are only accessors.
    List vmList = new ArrayList();
    // creating multiple partition regions on 3 nodes with scope =
    // DISTRIBUTED_ACK localMaxMemory=200 redundancy = 0
    startIndexForNode = 0;
    endIndexForNode = 3;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 200;
    // redundancy = 0;
    createPartitionRegion(vmList, startIndexForRegion, endIndexForRegion,
        localMaxMemory, redundancy);

    startIndexForNode = 0;
    endIndexForNode = 3;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    // doing put() in multiple partition regions from 3 nodes.
    putInMultiplePartitionedRegionFrom3Nodes(startIndexForRegion,
        endIndexForRegion, startIndexForKey, endIndexForKey);
    getLogWriter()
        .info(
            "testBucketDistributionAfterNodeAdditionInPR() - Put() operation successfully in partition regions on 3 Nodes");

    // creating multiple partition regions on 3 nodes with localMaxMemory=200 redundancy = 0
    startIndexForNode = 3;
    endIndexForNode = 4;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 200;
    // redundancy = 0;
    createPartitionRegion(vmList, startIndexForRegion, endIndexForKey,
        localMaxMemory, redundancy);
        
    startIndexForKey = 5;
    endIndexForKey = totalBucketNumProperty;
    // doing put() in multiple partition regions from 3 nodes.
    putInMultiplePartitionedRegionFrom3Nodes(startIndexForRegion,
        endIndexForRegion, startIndexForKey, endIndexForKey);
    getLogWriter()
        .info(
            "testBucketDistributionAfterNodeAdditionInPR() - Put() operation successfully in partition regions on 4th node");
    // validating bucket creation in the 4th node
    validateBucketsOnAllNodes(startIndexForRegion, endIndexForRegion);
    getLogWriter()
        .info(
            "testBucketDistributionAfterNodeAdditionInPR() - buckets on all the nodes are validated");
    getLogWriter().info(
        "testBucketDistributionAfterNodeAdditionInPR() successfully created");
  }

  /**
   * this is to test global property TOTAL_BUCKETS_NUM_PROPERTY. 1.create
   * partition region with scope = DISTRIBUTED_ACK redundancy = 3 on 4 vms 2.set
   * global property TOTAL_BUCKETS_NUM_PROPERTY = 11 3.perform put() operation
   * for the keys in the range 0 to 100 4.test number of buckets created. It
   * should be = 11
   */
  public void testTotalNumBucketProperty() throws Throwable
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);

    /** Prefix will be used for naming the partititon Region */
    prPrefix = "testTotalNumBucketProperty";

    /** these indices represents range of partition regions present in each VM */
    int startIndexForRegion = 0;
    int endIndexForRegion = 1;
    /** Start index for key */
    int startIndexForKey = 0;
    /** End index for key */
    int endIndexForKey = 20;

    int startIndexForNode = 0;
    int endIndexForNode = 4;
    List vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 200;
    final int localRedundancy = 1;
    createPRWithTotalNumPropSetList(vmList, startIndexForRegion,
        endIndexForRegion, localMaxMemory, localRedundancy);
    putInMultiplePartitionRegionFromOneVm(vm[0], startIndexForRegion,
        endIndexForRegion, startIndexForKey, endIndexForKey);
    int expectedNumBuckets = 11;
    validateTotalNumBuckets(prPrefix, vmList, startIndexForRegion,
        endIndexForRegion, expectedNumBuckets);
    startIndexForKey = 200;
    endIndexForKey = 400;
    putInMultiplePartitionedRegionFromAllVms(startIndexForRegion,
        endIndexForRegion, startIndexForKey, endIndexForKey);
    validateTotalNumBuckets(prPrefix, vmList, startIndexForRegion,
        endIndexForRegion, expectedNumBuckets);
    getLogWriter().info("testTotalNumBucketProperty() completed successfully");

  }

  /**
   * This is to test LOCAL_MAX_MEMORY property 1. create partitione region with
   * scope = DISTRIBUTED_ACK localMaxMemory = 1MB and redundancy = 0 on all vms
   * 2. do put() operations so that size of the objects that were put >
   * localMaxMemory of partition region
   */
  public void _testLocalMaxMemoryInPartitionedRegion() throws Throwable
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);

    /** Prefix will be used for naming the partititon Region */
    prPrefix = getUniqueName();

    /** these indices represents range of partition regions present in each VM */
    int startIndexForRegion = 0;
    int endIndexForRegion = 1;
    /** Start index for key */
//    int startIndexForKey = 0;
    /** End index for key */
//    int endIndexForKey = 4000000;

    int startIndexForNode = 0;
    int endIndexForNode = 4;
    List vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 1;
    // redundancy = 0;
    createPartitionRegion(vmList, startIndexForRegion, endIndexForRegion,
        localMaxMemory, redundancy);
    // doing put() operation on multiple partition region
    putForLocalMaxMemoryInMultiplePR(prPrefix + 0);

  }
  
  
  /**
   * Ensure that all buckets can be allocated for a PR with different
   * levels of redundancy.  This is important given that bucket creation
   * may fail due to VMs that refuse bucket allocation.  VMs may refuse for 
   * different reasons, in this case VMs may refuse because they are above
   * their maximum.
   */
  public void testCompleteBucketAllocation() throws Exception {
    final String regionName = getUniqueName();
    final int maxBuckets = 23;
    
    
    Host host = Host.getHost(0);
    createVMs(host);
    invokeInEveryVM(new SerializableRunnable("Create PR") {
      public void run() {
        getCache().createRegion(regionName, createRegionAttrs(0, 10, maxBuckets));
        
      }
    });
    
    this.vm[0].invoke(new SerializableRunnable("Create keys") {
      public void run() {
        Cache c = getCache();
        PartitionedRegion r = (PartitionedRegion) c.getRegion(regionName);
        int i = 0;
        // Create all the buckets
        for (;;) {
          r.put(new Integer(i), "v-" + Integer.toString(i));
          i++;
          if (((i % 10) == 0) &&
              r.getRegionAdvisor().getBucketSet().size() >= maxBuckets) {
            break;
          }
        }
      }
    });
    
//    final int bucketPerHost = (int) Math.ceil(((double) maxBuckets / Host.getHostCount()));

//    invokeInEveryVM(new SerializableRunnable("") {
//      
//    }
    
  }
  
  /**
   * Added for defect #47181.
   * Use below combination to reproduce the issue:
   * mcast-port = 0
   * Locators should be empty
   * enforce-unique-host = true
   * redundancy-zone = "zone"
   */
  public void testEnforceUniqueHostForLonerDistributedSystem() {
	  Cache cache = createLonerCacheWithEnforceUniqueHost();
      
	  AttributesFactory attr = new AttributesFactory();
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      PartitionAttributes prAttr = paf.create();
      attr.setPartitionAttributes(prAttr);
      RegionAttributes regionAttribs = attr.create();
      Region region = cache.createRegion("PR1", regionAttribs);
      
      for (int i = 0; i < 113; i++) {
    	region.put("Key_" + i, new Integer(i));
      }
      //verify region size
      assertEquals(113, region.size());
  }

  /**
   * This function performs destroy(key) operations in multiple Partiton
   * Regions. The range of keys to be destroyed is from 100 to 200. Each Vm
   * destroys different set of the keys.
   * @param startIndexForRegion
   * @param endIndexForRegion
   * @param startIndexForDestroy
   * @param endIndexForDestroy
   * @throws Throwable
   */
  private void destroyInMultiplePartitionedRegion(int startIndexForRegion,
      int endIndexForRegion, int startIndexForDestroy, int endIndexForDestroy)
      throws Throwable
  {
    prPrefix = "testMemoryOfPartitionRegion";
    int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    int delta = (endIndexForDestroy - startIndexForDestroy) / 4;
    async[0] = vm[0].invokeAsync(destroyInMultiplePartitionRegion(prPrefix, startIndexForDestroy,
            startIndexForDestroy + 1 * delta, startIndexForRegion,
            endIndexForRegion));
    async[1] = vm[1].invokeAsync(destroyInMultiplePartitionRegion(prPrefix, startIndexForDestroy + 1
            * delta, startIndexForDestroy + 2 * delta, startIndexForRegion,
            endIndexForRegion));
    async[2] = vm[2].invokeAsync(destroyInMultiplePartitionRegion(prPrefix, startIndexForDestroy + 2
            * delta, startIndexForDestroy + 3 * delta, startIndexForRegion,
            endIndexForRegion));
    async[3] = vm[3]
        .invokeAsync(destroyInMultiplePartitionRegion(prPrefix, startIndexForDestroy
                + 3 * delta, endIndexForDestroy, startIndexForRegion,
                endIndexForRegion));

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }
    
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        fail("Exception during " + count, async[count].getException());
      }
    }
  }

  /**
   * This function performs destroy(key) operations in multiple Partiton
   * Regions. The range of keys to be destroyed is from 100 to 200. Each Vm
   * destroys different set of the keys.
   * @param startIndexForRegion
   * @param endIndexForRegion
   * @param startIndexForInvalidate
   * @param endIndexForInvalidate
   */
  private void invalidateInMultiplePartitionedRegion(int startIndexForRegion,
      int endIndexForRegion, int startIndexForInvalidate,
      int endIndexForInvalidate) throws Throwable 
  {
    prPrefix = "testMemoryOfPartitionRegion";
    int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    int delta = (endIndexForInvalidate - startIndexForInvalidate) / 4;
    async[0] = vm[0].invokeAsync(invalidatesInMultiplePartitionRegion(prPrefix,
            startIndexForInvalidate, startIndexForInvalidate + 1 * delta,
            startIndexForRegion, endIndexForRegion));
    async[1] = vm[1].invokeAsync(invalidatesInMultiplePartitionRegion(prPrefix, startIndexForInvalidate
            + 1 * delta, startIndexForInvalidate + 2 * delta,
            startIndexForRegion, endIndexForRegion));
    async[2] = vm[2].invokeAsync(invalidatesInMultiplePartitionRegion(prPrefix, startIndexForInvalidate
            + 2 * delta, startIndexForInvalidate + 3 * delta,
            startIndexForRegion, endIndexForRegion));
    async[3] = vm[3].invokeAsync(invalidatesInMultiplePartitionRegion(prPrefix, startIndexForInvalidate
            + 3 * delta, endIndexForInvalidate, startIndexForRegion,
            endIndexForRegion));

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
      }
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        fail("Exception during " + count, async[count].getException());
      }
    }
  }

  /**
   * This function performs put() operations in multiple Partition Regions.
   * Range of the keys which are put is startIndexForKey to endIndexForKey. Each
   * Vm puts different set of keys.
   */
  private void putInMultiplePartitionedRegionFrom3Nodes(
      int startIndexForRegion, int endIndexForRegion, int startIndexForKey,
      int endIndexForKey) throws Throwable
  {
    int AsyncInvocationArrSize = 3;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    int delta = (endIndexForKey - startIndexForKey) / 3;

    async[0] = vm[0].invokeAsync(putInMultiplePartitionRegion(prPrefix, startIndexForKey,
            startIndexForKey + 1 * delta, startIndexForRegion,
            endIndexForRegion));
    async[1] = vm[1].invokeAsync(putInMultiplePartitionRegion(prPrefix, startIndexForKey + 1 * delta,
            startIndexForKey + 2 * delta, startIndexForRegion,
            endIndexForRegion));
    async[2] = vm[2].invokeAsync(putInMultiplePartitionRegion(prPrefix, startIndexForKey + 2 * delta,
            endIndexForKey, startIndexForRegion, endIndexForRegion));

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }
 
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        fail("exception during" + count, async[count].getException());
      }
    }
  }

  /**
   * This function performs put() operation from the single vm in multiple
   * partition regions.
   * 
   * @param vm0
   * @param startIndexForRegion
   * @param endIndexForRegion
   * @param startIndexForKey
   * @param endIndexForKey
   */

  private void putInMultiplePartitionRegionFromOneVm(VM vm0,
      final int startIndexForRegion, final int endIndexForRegion,
      final long startIndexForKey, final long endIndexForKey) throws Throwable
  {
    int AsyncInvocationArrSize = 3;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = vm0.invokeAsync(putFromOneVm(startIndexForKey, endIndexForKey,
        startIndexForRegion, endIndexForRegion));
    async[1] = vm0.invokeAsync(putFromOneVm(startIndexForKey
        + totalBucketNumProperty, endIndexForKey + totalBucketNumProperty,
        startIndexForRegion, endIndexForRegion));
    async[2] = vm0.invokeAsync(putFromOneVm(startIndexForKey + 2
        * totalBucketNumProperty, endIndexForKey + 2 * totalBucketNumProperty,
        startIndexForRegion, endIndexForRegion));

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }
    
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        fail("exception during " + count, async[count].getException());
     }
    }
  }

  /**
   * This function performs put() in multiple partition regions for the given
   * node.
   * 
   * @param startIndexForKey
   * @param endIndexForKey
   * @param startIndexForRegion
   * @param endIndexForRegion
   * @return
   */
  private CacheSerializableRunnable putFromOneVm(final long startIndexForKey,
      final long endIndexForKey, final int startIndexForRegion,
      final int endIndexForRegion)
  {
    CacheSerializableRunnable putFromVm = new CacheSerializableRunnable(
        "putFromOneVm") {

      String innerPrPrefix = prPrefix;

      public void run2()
      {
        Cache cache = getCache();
        for (int i = startIndexForRegion; i < endIndexForRegion; i++) {
          PartitionedRegion pr = (PartitionedRegion)cache
              .getRegion(Region.SEPARATOR + innerPrPrefix + (i));
          assertNotNull(pr);
          for (long k = startIndexForKey; k < endIndexForKey; k++) {
            Long key = new Long(k);
            pr.put(key, innerPrPrefix + k);
          }
        }
      }
    };
    return putFromVm;
  }

  /**
   * This function performs put() operations in multiple Partition Regions.
   * Range of the keys which are put is startIndexForKey to endIndexForKey. Each
   * Vm puts different set of keys.
   */
  private void putInMultiplePartitionedRegion(int startIndexForRegion,
      int endIndexForRegion, int startIndexForKey, int endIndexForKey)
      throws Throwable
  {
    int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    int delta = (endIndexForKey - startIndexForKey) / 4;
    async[0] = vm[0].invokeAsync(putInMultiplePartitionRegion(prPrefix, startIndexForKey,
            startIndexForKey + 1 * delta, startIndexForRegion,
            endIndexForRegion));
    async[1] = vm[1].invokeAsync(putInMultiplePartitionRegion(prPrefix, startIndexForKey + 1 * delta,
            startIndexForKey + 2 * delta, startIndexForRegion,
            endIndexForRegion));
    async[2] = vm[2].invokeAsync(putInMultiplePartitionRegion(prPrefix, startIndexForKey + 2 * delta,
            startIndexForKey + 3 * delta, startIndexForRegion,
            endIndexForRegion));
    async[3] = vm[3].invokeAsync(putInMultiplePartitionRegion(prPrefix, startIndexForKey + 3 * delta,
            endIndexForKey, startIndexForRegion, endIndexForRegion));

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }
    
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        fail("exception during " + count, async[count].getException());
      }
    }
  }

  /**
   * This function performs put() operations in multiple Partition Regions.
   * Range of the keys which are put is startIndexForKey to edIndexForKey. Each
   * Vm puts different set of keys
   */
  private void putInMultiplePartitionedRegionFromAllVms(
      int startIndexForRegion, int endIndexForRegion, long startIndexForKey,
      long endIndexForKey) throws Throwable
  {
    int AsyncInvocationArrSize = 8;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    long delta = (endIndexForKey - startIndexForKey) / 4;
    async[0] = vm[0].invokeAsync(putFromOneVm(startIndexForKey,
        startIndexForKey + 1 * delta, startIndexForRegion, endIndexForRegion));
    async[1] = vm[1].invokeAsync(putFromOneVm(startIndexForKey + 1 * delta,
        startIndexForKey + 2 * delta, startIndexForRegion, endIndexForRegion));
    async[2] = vm[2].invokeAsync(putFromOneVm(startIndexForKey + 2 * delta,
        startIndexForKey + 3 * delta, startIndexForRegion, endIndexForRegion));
    async[3] = vm[3].invokeAsync(putFromOneVm(startIndexForKey + 3 * delta,
        endIndexForKey, startIndexForRegion, endIndexForRegion));

    startIndexForKey += totalBucketNumProperty;
    endIndexForKey += totalBucketNumProperty;
    delta = (endIndexForKey - startIndexForKey) / 4;
    async[4] = vm[0].invokeAsync(putFromOneVm(startIndexForKey,
        startIndexForKey + 1 * delta, startIndexForRegion, endIndexForRegion));
    async[5] = vm[1].invokeAsync(putFromOneVm(startIndexForKey + 1 * delta,
        startIndexForKey + 2 * delta, startIndexForRegion, endIndexForRegion));
    async[6] = vm[2].invokeAsync(putFromOneVm(startIndexForKey + 2 * delta,
        startIndexForKey + 3 * delta, startIndexForRegion, endIndexForRegion));
    async[7] = vm[3].invokeAsync(putFromOneVm(startIndexForKey + 3 * delta,
        endIndexForKey, startIndexForRegion, endIndexForRegion));

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }
    
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        fail("exception during " + count, async[count].getException());
      }
    }
  }

  /**
   * This function performs validation of bucket2Node region of multiple
   * partition regions on 4 VMs.
   * 
   * @param vm0
   * @param vm1
   * @param vm2
   * @param vm3
   * @param startIndexForRegion
   * @param endIndexForRegion
   */
  private void validateBucket2NodeBeforePutInMultiplePartitionedRegion(
      int startIndexForRegion, int endIndexForRegion) throws Throwable
  {
    int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = vm[0].invokeAsync(validateBucket2NodeBeforePut(
        startIndexForRegion, endIndexForRegion));
    async[1] = vm[1].invokeAsync(validateBucket2NodeBeforePut(
        startIndexForRegion, endIndexForRegion));
    async[2] = vm[2].invokeAsync(validateBucket2NodeBeforePut(
        startIndexForRegion, endIndexForRegion));
    async[3] = vm[3].invokeAsync(validateBucket2NodeBeforePut(
        startIndexForRegion, endIndexForRegion));
    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }
    
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        getLogWriter().warning("Failure in async invocation on vm " 
            + vm[count]
            + " with exception " + async[count].getException());
        throw async[count].getException();
//        fail("exception during " + count, async[count].getException());
      }
    }
  }

  /**
   * This function performs validation of bucket regions of multiple partition
   * regions on 4 VMs.
   * 
   * @param vm0
   * @param vm1
   * @param vm2
   * @param vm3
   * @param startIndexForRegion
   * @param endIndexForRegion
   */
  private void validateBucketsAfterPutInMultiplePartitionRegion(
      final int startIndexForRegion, final int endIndexForRegion)
      throws Throwable
  {
    int AsyncInvocationArrSize = 8;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    // validation of bucket regions creation
    async[0] = vm[0].invokeAsync(validateBucketCreationAfterPut(
        startIndexForRegion, endIndexForRegion));
    async[1] = vm[1].invokeAsync(validateBucketCreationAfterPut(
        startIndexForRegion, endIndexForRegion));
    async[2] = vm[2].invokeAsync(validateBucketCreationAfterPut(
        startIndexForRegion, endIndexForRegion));
    async[3] = vm[3].invokeAsync(validateBucketCreationAfterPutForNode3(
        startIndexForRegion, endIndexForRegion));

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < 4; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }
    
    for (int count = 0; count < 4; count++) {
      if (async[count].exceptionOccurred()) {
        fail("got exception on " + count, async[count].getException());
      }
    }

    // validating scope of buckets
    async[4] = vm[0].invokeAsync(validateBucketScopeAfterPut(
        startIndexForRegion, endIndexForRegion));
    async[5] = vm[1].invokeAsync(validateBucketScopeAfterPut(
        startIndexForRegion, endIndexForRegion));
    async[6] = vm[2].invokeAsync(validateBucketScopeAfterPut(
        startIndexForRegion, endIndexForRegion));
    async[7] = vm[3].invokeAsync(validateBucketCreationAfterPutForNode3(
        startIndexForRegion, endIndexForRegion));

    /** main thread is waiting for the other threads to complete */
    for (int count = 4; count < AsyncInvocationArrSize; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }
    
    for (int count = 4; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        getLogWriter().warning("Failure of async invocation on VM " + 
            this.vm[count] + " exception thrown " + async[count].getException());
        throw async[count].getException();
      }
    }
    
  }

  /**
   * This function performs validation of bucket regions of multiple partition
   * regions on 4 VMs.
   * 
   * @param vm0
   * @param vm1
   * @param vm2
   * @param vm3
   * @param startIndexForRegion
   * @param endIndexForRegion
   */
  private void validateBucketsDistributionInMultiplePartitionRegion(
      final int startIndexForRegion, final int endIndexForRegion,
      int noBucketsExpectedOnEachNode) throws Throwable
  {
    int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = vm[0].invokeAsync(validateBucketsDistribution(
        startIndexForRegion, endIndexForRegion, noBucketsExpectedOnEachNode));
    async[1] = vm[1].invokeAsync(validateBucketsDistribution(
        startIndexForRegion, endIndexForRegion, noBucketsExpectedOnEachNode));
    async[2] = vm[2].invokeAsync(validateBucketsDistribution(
        startIndexForRegion, endIndexForRegion, noBucketsExpectedOnEachNode));
    async[3] = vm[3].invokeAsync(validateBucketsDistribution(
        startIndexForRegion, endIndexForRegion, noBucketsExpectedOnEachNode));

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }
    
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        fail("Validation of bucket distribution failed on " + count,
            async[count].getException());
      }
    }
  }

  /**
   * This function is used for the validation of bucket on all the region.
   * 
   * @param vm0
   * @param vm1
   * @param vm2
   * @param vm3
   * @param startIndexForRegion
   * @param endIndexForRegion
   */
  private void validateBucketsOnAllNodes(final int startIndexForRegion,
      final int endIndexForRegion)
  {
    CacheSerializableRunnable validateAllNodes = new CacheSerializableRunnable(
        "validateBucketsOnAllNodes") {
      int innerStartIndexForRegion = startIndexForRegion;

      int innerEndIndexForRegion = endIndexForRegion;

      String innerPrPrefix = prPrefix;

      public void run2()
      {
        Cache cache = getCache();
        int threshold = 0;
        for (int i = innerStartIndexForRegion; i < innerEndIndexForRegion; i++) {
          PartitionedRegion pr = (PartitionedRegion)cache
              .getRegion(Region.SEPARATOR + innerPrPrefix + i);
          assertNotNull(pr);
          assertNotNull(pr.getDataStore());
          assertTrue(pr.getDataStore().localBucket2RegionMap.size() > threshold);
        }
      }
    };

    vm[0].invoke(validateAllNodes);
    vm[1].invoke(validateAllNodes);
    vm[2].invoke(validateAllNodes);
    vm[3].invoke(validateAllNodes);
  }

  private void validateRedundancy(VM vm0, final int startIndexForRegion,
      final int endIndexForRegion, final int redundancyManageFlag)
  {
    vm0.invoke(new CacheSerializableRunnable("validateRedundancy") {
      int innerStartIndexForRegion = startIndexForRegion;

      int innerEndIndexForRegion = endIndexForRegion;

      String innerPrPrefix = prPrefix;

      public void run2()
      {
        Cache cache = getCache();
        for (int i = innerStartIndexForRegion; i < endIndexForRegion; i++) {
          PartitionedRegion pr = (PartitionedRegion)cache
              .getRegion(Region.SEPARATOR + innerPrPrefix + i);
          assertNotNull(pr);
          Set bucketIds = pr.getDataStore().localBucket2RegionMap.keySet();
          Iterator buckteIdItr = bucketIds.iterator();
          while (buckteIdItr.hasNext()) {
            Integer bucketId = (Integer) buckteIdItr.next();
            

            
            if (redundancyManageFlag == 0) {
              // checking redundancy
              if (pr.getRegionAdvisor().getBucketRedundancy(bucketId.intValue()) >= redundancy) {
                fail("Redundancy satisfied for the partition region "
                    + pr.getName());
              }
            }
            else {
              if (pr.getRegionAdvisor().getBucketRedundancy(bucketId.intValue()) < redundancy) {
                fail("Redundancy not satisfied for the partition region "
                    + pr.getName());
              }
            }
            

          }
          if (redundancyManageFlag == 0) {
            getLogWriter().info(
                "validateRedundancy() - Redundancy not satisfied for the partition region  : "
                    + pr.getName());
          }
          else {
            getLogWriter().info(
                "validateRedundancy() - Redundancy satisfied for the partition region  : "
                    + pr.getName());
          }
        }
      }
    });
  }

  /**
   * This functions performs following validations on the partitions regiions
   * <br>
   * (1) Size of bucket2Node region should be > 0.</br><br>
   * (3) In case of the partition regions with redundancy > 0 scope of the
   * bucket regions should be scope of the partition regions.</br><br>
   * (4) In case of the partition regions with redundancy > 0 no two bucket
   * regions with same bucketId should be generated on the same node.</br>
   * 
   * @param startIndexForRegion
   * @param endIndexForRegion
   * @return
   */
  private CacheSerializableRunnable validateBucketCreationAfterPut(
      final int startIndexForRegion, final int endIndexForRegion)
  {
    CacheSerializableRunnable validateAfterPut = new CacheSerializableRunnable(
        "validateAfterPut") {
      int innerStartIndexForRegion = startIndexForRegion;

      int innerEndIndexForRegion = endIndexForRegion;

      String innerPrPrefix = prPrefix;

      int innerMidIndexForRegion = innerStartIndexForRegion
          + (endIndexForRegion - startIndexForRegion) / 2;

      int innerQuarterIndex = innerStartIndexForRegion
          + (endIndexForRegion - startIndexForRegion) / 4;

      public void run2()
      {
        Cache cache = getCache();
        Region root = cache
            .getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME);
        assertNotNull("Root regions is null", root);
        for (int i = innerStartIndexForRegion; i < innerEndIndexForRegion; i++) {
          PartitionedRegion pr = (PartitionedRegion)cache
              .getRegion(Region.SEPARATOR + innerPrPrefix + i);

          assertTrue(pr.getRegionAdvisor().getBucketSet().size() > 0);
          assertTrue("Size of local region map should be > 0 for region: " +  pr.getFullPath(), pr
              .getDataStore().localBucket2RegionMap.size() > 0);
          // taking the buckets which are local to the node and not all the
          // available buckets.
          Set bucketIds = pr.getDataStore().localBucket2RegionMap.keySet();
          Iterator buckteIdItr = bucketIds.iterator();
          while (buckteIdItr.hasNext()) {
            Integer key = (Integer) buckteIdItr.next();
            BucketRegion val = (BucketRegion)pr.getDataStore().localBucket2RegionMap
                .get(key);

            Region bucketRegion = root.getSubregion(pr.getBucketName(key.intValue()));
            assertTrue(bucketRegion.getFullPath().equals(val.getFullPath()));
            // Bucket region should not be null
            assertNotNull("Bucket region cannot be null", bucketRegion);
            // Parent region of the bucket region should be root
            assertEquals("Parent region is not root", root, bucketRegion
                .getParentRegion());
          }
        }
      }
    };
    return validateAfterPut;
  }

  private CacheSerializableRunnable validateBucketCreationAfterPutForNode3(
      final int startIndexForRegion, final int endIndexForRegion)
  {
    CacheSerializableRunnable validateAfterPut = new CacheSerializableRunnable(
        "validateBucketCreationAfterPutForNode3") {
      int innerStartIndexForRegion = startIndexForRegion;

      int innerEndIndexForRegion = endIndexForRegion;

      String innerPrPrefix = prPrefix;

      public void run2()
      {
        Cache cache = getCache();
        for (int i = innerStartIndexForRegion; i < innerEndIndexForRegion; i++) {
          PartitionedRegion pr = (PartitionedRegion)cache
              .getRegion(Region.SEPARATOR + innerPrPrefix + i);
          assertNotNull("This Partition Region is null " + pr.getName(), pr);
          assertNull("DataStore should be null", pr.getDataStore());
        }
      }
    };
    return validateAfterPut;
  }

  private CacheSerializableRunnable validateBucketScopeAfterPut(
      final int startIndexForRegion, final int endIndexForRegion)
  {
    CacheSerializableRunnable validateAfterPut = new CacheSerializableRunnable(
        "validateBucketScopeAfterPut") {
      int innerStartIndexForRegion = startIndexForRegion;

      int innerEndIndexForRegion = endIndexForRegion;

      String innerPrPrefix = prPrefix;

      int innerMidIndexForRegion = innerStartIndexForRegion
          + (endIndexForRegion - startIndexForRegion) / 2;

      int innerQuarterIndex = innerStartIndexForRegion
          + (endIndexForRegion - startIndexForRegion) / 4;

      public void run2()
      {
        Cache cache = getCache();
        Region root = cache
            .getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME);
        assertNotNull("Root regions is null", root);
        for (int i = innerStartIndexForRegion; i < innerEndIndexForRegion; i++) {
          PartitionedRegion pr = (PartitionedRegion)cache
              .getRegion(Region.SEPARATOR + innerPrPrefix + i);

          assertTrue(pr.getRegionAdvisor().getBucketSet().size() > 0);
          // taking the buckets which are local to the node and not all the
          // available buckets.
          Set bucketIds = pr.getDataStore().localBucket2RegionMap.keySet();
          Iterator buckteIdItr = bucketIds.iterator();
          while (buckteIdItr.hasNext()) {
            Integer key = (Integer) buckteIdItr.next();
            Region bucketRegion = root.getSubregion(pr.getBucketName(key.intValue()));
            assertNotNull("Bucket region cannot be null", bucketRegion);
            assertEquals(Scope.DISTRIBUTED_ACK, bucketRegion.getAttributes().getScope());
          }  // while 
        }
      }
    };
    return validateAfterPut;
  }

  /**
   * <br>
   * This function validates bucket2Node region of the Partition regiion before
   * any put() operations.</br><br>
   * It performs following validatioons <br>
   * (1) bucket2Node Region should not be null.</br><br>
   * (2) Scope of the bucket2Node region should be DISTRIBUTED_ACK.</br><br>
   * (3) Size of bucket2Node region should be 0.</br><br>
   * (4) Parent region of the bucket2Node region shoud be root i.e. region with
   * name "PRRoot".</br>
   * 
   * @param startIndexForRegion
   * @param endIndexForRegion
   * @return
   */

  private CacheSerializableRunnable validateBucket2NodeBeforePut(
      final int startIndexForRegion, final int endIndexForRegion)
  {

    CacheSerializableRunnable validateBucketBeforePut = new CacheSerializableRunnable(
        "Bucket2NodeValidation") {
      int innerStartIndexForRegion = startIndexForRegion;

      int innerEndIndexForRegion = endIndexForRegion;

      String innerPrPrefix = prPrefix;

      public void run2()
      {
        Cache cache = getCache();
        Region root = cache
            .getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME);
        assertNotNull("Root regions is null", root);
        for (int i = innerStartIndexForRegion; i < innerEndIndexForRegion; i++) {
          PartitionedRegion pr = (PartitionedRegion)cache
              .getRegion(Region.SEPARATOR + innerPrPrefix + i);
          assertNotNull(pr);

          assertTrue(pr.getRegionAdvisor().getNumProfiles() > 0);
          assertTrue(pr.getRegionAdvisor().getNumDataStores() > 0);
          final int bucketSetSize = pr.getRegionAdvisor().getCreatedBucketsCount();
          getLogWriter().info("BucketSet size " + bucketSetSize);
          if (bucketSetSize != 0) {
            Set buckets = pr.getRegionAdvisor().getBucketSet();
            Iterator it  = buckets.iterator();
            int numBucketsWithStorage = 0;
            try {
              while(true) {
                Integer bucketId = (Integer) it.next();
                pr.getRegionAdvisor().getBucket(bucketId.intValue()).getBucketAdvisor()
                  .dumpProfiles("Bucket owners for bucket " 
                      + pr.bucketStringForLogs(bucketId.intValue()));
                numBucketsWithStorage++;
              }
            } catch (NoSuchElementException end) {
              getLogWriter().info("BucketSet iterations " + numBucketsWithStorage);
            }
            fail("There should be no buckets assigned");
          }
        }
      }
    };
    return validateBucketBeforePut;
  }

  private CacheSerializableRunnable validateBucketsDistribution(
      final int startIndexForRegion, final int endIndexForRegion,
      final int noBucketsExpectedOnEachNode)
  {
    CacheSerializableRunnable validateBucketDist = new CacheSerializableRunnable(
        "validateBucketsDistribution") {

      String innerPrPrefix = prPrefix;

      public void run2()
      {
        Cache cache = getCache();
        final Region root = cache
            .getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME);
        assertNotNull("Root regions is null", root);
        for (int i = startIndexForRegion; i < endIndexForRegion; i++) {
          final PartitionedRegion pr = (PartitionedRegion)cache
              .getRegion(Region.SEPARATOR + innerPrPrefix + i);
          assertNotNull("This region can not be null" + pr.getName(), pr);
          
          assertNotNull(pr.getDataStore());
          final int localBSize = pr.getDataStore().getBucketsManaged();
          getLogWriter().info(
              "validateBucketsDistribution() - Number of bukctes for "
                  + pr.getName() + " : "  + localBSize);

          assertTrue("Bucket Distribution for region = " + pr.getFullPath() +" is not correct for member "
              + pr.getDistributionManager().getId() + " existing size "
              + localBSize + " smaller than expected "
              + noBucketsExpectedOnEachNode,
              localBSize >= noBucketsExpectedOnEachNode);

          pr.getDataStore().visitBuckets(new BucketVisitor() {
            public void visit(Integer bucketId, Region r)
            {
              Region bucketRegion = root.getSubregion(pr.getBucketName(bucketId.intValue()));
              assertEquals(bucketRegion.getFullPath(), r.getFullPath());              
            }}
          );
        }
      }
    };
    return validateBucketDist;
  }

  /** this function creates vms in given host */
  private void createVMs(Host host)
  {
    for (int i = 0; i < 4; i++) {
      vm[i] = host.getVM(i);
    }
  }

  /**
   * This function createas multiple partition regions on nodes specified in the
   * vmList
   */
  private void createPartitionRegion(List vmList, int startIndexForRegion,
      int endIndexForRegion, int localMaxMemory, int redundancy)
  {
    Iterator nodeIterator = vmList.iterator();
    while (nodeIterator.hasNext()) {
      VM vm = (VM)nodeIterator.next();
      vm.invoke(createMultiplePRWithTotalNumBucketPropSet(prPrefix,
          startIndexForRegion, endIndexForRegion, redundancy, localMaxMemory, 11));
    }
  }

  /**
   * This function creates a partition region with TOTAL_BUCKETS_NUM_PROPERTY
   * set to 11.
   */
  private void createPRWithTotalNumPropSetList(List vmList,
      int startIndexForRegion, int endIndexForRegion, int localMaxMemory,
      int redundancy)
  {
    Iterator nodeIterator = vmList.iterator();
    while (nodeIterator.hasNext()) {
      VM vm = (VM)nodeIterator.next();
      vm.invoke(createMultiplePRWithTotalNumBucketPropSet(prPrefix,
          startIndexForRegion, endIndexForRegion, redundancy, localMaxMemory, 11));
    }
  }

  CacheSerializableRunnable createMultiplePRWithTotalNumBucketPropSet(
      final String prPrefix, final int startIndexForRegion,
      final int endIndexForRegion, final int redundancy, final int localMaxMem, 
      final int numBuckets) {
    CacheSerializableRunnable createPRWithTotalNumBucketPropSet = new CacheSerializableRunnable(
        "createPRWithTotalNumBucketPropSet") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        for (int i = startIndexForRegion; i < endIndexForRegion; i++) {
          cache.createRegion(prPrefix + i,
              createRegionAttrs(redundancy, localMaxMem, numBuckets));
        }
        getLogWriter()
            .info(
                "createMultiplePartitionRegion() - Partition Regions Successfully Completed ");
      }
    };
    return createPRWithTotalNumBucketPropSet;
  }

  private void validateTotalNumBuckets(String prPrefix, List vmList,
      int startIndexForRegion, int endIndexForRegion, int expectedNumBuckets)
  {
    Iterator nodeIterator = vmList.iterator();
    while (nodeIterator.hasNext()) {
      VM vm = (VM)nodeIterator.next();
      vm.invoke(validateTotalNumberOfBuckets(prPrefix, expectedNumBuckets,
          startIndexForRegion, endIndexForRegion));
    }
  }

  /**
   * This function validates total number of buckets from bucket2NodeRegion of
   * partition region.
   */
  CacheSerializableRunnable validateTotalNumberOfBuckets(final String prPrefix,
      final int expectedNumBuckets, final int startIndexForRegion,
      final int endIndexForRegion)
  {
    CacheSerializableRunnable validateTotNumBuckets = new CacheSerializableRunnable(
        "validateTotNumBuckets") {
      String innerPrPrefix = prPrefix;

      int innerStartIndexForRegion = startIndexForRegion;

      int innerEndIndexForRegion = endIndexForRegion;

      public void run2() throws CacheException
      {
        Cache cache = getCache();
        for (int i = innerStartIndexForRegion; i < innerEndIndexForRegion; i++) {
          PartitionedRegion pr = (PartitionedRegion)cache
              .getRegion(Region.SEPARATOR + innerPrPrefix + i);
          assertNotNull("This region is null " + pr.getName(), pr);

          Set bucketsWithStorage = pr.getRegionAdvisor().getBucketSet();
          assertEquals(expectedNumBuckets, bucketsWithStorage.size());
        }
        getLogWriter().info(
            "Total Number of buckets validated in partition region");
      }
    };
    return validateTotNumBuckets;
  }

  /** This function adds nodes to node list */
  private List addNodeToList(int startIndexForNode, int endIndexForNode)
  {
    List localvmList = new ArrayList();
    for (int i = startIndexForNode; i < endIndexForNode; i++) {
      localvmList.add(vm[i]);
    }
    return localvmList;
  }

  /**
   * This function performs following 1. creates multiplePartition regions on
   * different nodes with vm3 node as accessor. 2. range of partition regions is
   * specified by startIndexForRegion and endIndexForRegion. 3. first quarter of
   * total partition regions are of scope = DISTRIBUTED_ACK and redundancy=0. 4.
   * second quarter of total partition regions are of scope = DISTRIBUTED_ACK
   * and redundancy=2. 5. third quarter of total partition regions are of scope =
   * DISTRIBUTED_NO_ACK and redundancy=0. 4. fourth quarter of total partition
   * regions are of scope = DISTRIBUTED_NO_ACK and redundancy=2.
   * 
   * @param startIndexForRegion
   * @param endIndexForRegion
   */
  void createMultiplePR(int startIndexForRegion, int endIndexForRegion)
  {
    int startIndexForNode = 0;
    int endIndexForNode = 4;
    // creating partition regions on 4 nodes out of which partition regions on
    // one node are only accessors.
    // partition regions on vm3 are only accessors.
    int midIndexForRegion = (endIndexForRegion - startIndexForRegion) / 2;
    // creating multiple partition regions on 3 nodes with scope =
    // DISTRIBUTED_ACK localMaxMemory=200 redundancy = 0
    startIndexForNode = 0;
    endIndexForNode = 3;
    List vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 200;
    // redundancy = 0;
    createPartitionRegion(vmList, startIndexForRegion, midIndexForRegion,
        localMaxMemory, redundancy);

    // creating multiple partition regions on VM3 as only accessor
    startIndexForNode = 3;
    endIndexForNode = 4;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 0;
    createPartitionRegion(vmList, startIndexForRegion, midIndexForRegion,
        localMaxMemory, redundancy);

    // creating multiple partition regions on 3 nodes localMaxMemory=200 redundancy = 1
    startIndexForNode = 0;
    endIndexForNode = 3;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 200;
    final int redundancyTwo = 1;
    createPartitionRegion(vmList, midIndexForRegion, endIndexForNode,
        localMaxMemory, redundancyTwo);

    // creating multiple partition regions on VM3 as only accessor
    startIndexForNode = 3;
    endIndexForNode = 4;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 0;
    createPartitionRegion(vmList, midIndexForRegion, endIndexForNode,
        localMaxMemory, redundancyTwo);

    getLogWriter()
        .info(
            "testBucketCerationInMultiPlePartitionRegion() - Partition Regions successfully created ");
  }

  /**
   * This function is used to calculate memory of partiton region at each node
   */
  private void calculateTotalMemoryOfPartitionRegion()
  {
    for (int i = 0; i < 4; i++) {
      if (vm[i] == null)
        getLogWriter().fine("VM is null" + vm[i]);
      vm[i].invoke(calculateMemoryOfPartitionRegion(i, i + 1));
    }
  }

  /**
   * This function verifies memory of partition region calculated at each node.
   */
  private void checkTotalMemoryOfPartitionRegion()
  {

    CacheSerializableRunnable testTotalMemory = new CacheSerializableRunnable(
        "testTotalMemory") {
      int innerStartIndexForKey = 0;

      int innerEndIndexForKey = 4;

      String innerPrPrefix = prPrefix;

      List sizeList = new ArrayList();

      public void run2()
      {
        Cache cache = getCache();
        innerPrPrefix = "createPRForStrorage";
        PartitionedRegion prForStorage = (PartitionedRegion)cache
            .getRegion(Region.SEPARATOR + innerPrPrefix + 0);
        for (int i = innerStartIndexForKey; i < innerEndIndexForKey; i++) {
          Object obj = prForStorage.get(new Long(i));
          sizeList.add(obj);
        }
        Iterator sizeItr = sizeList.iterator();
        Object objSize = sizeItr.next();
        while (sizeItr.hasNext()) {
          assertEquals(sizeItr.next(), objSize);
        }
        getLogWriter().info("Size of partition region on each node is equal");
      }
    };
    vm[0].invoke(testTotalMemory);
  }

  /**
   * This function calculates memory of partition region at each node and puts
   * it in storage region.
   * 
   * @param startIndexForKey
   * @param endIndexForKey
   * @return
   */
  private CacheSerializableRunnable calculateMemoryOfPartitionRegion(
      final int startIndexForKey, final int endIndexForKey)
  {
    CacheSerializableRunnable calulateTotalMemory = new CacheSerializableRunnable(
        "calulateTotalMemory") {
      int innerStartIndexForKey = startIndexForKey;

      int innerEndIndexForKey = endIndexForKey;

      String innerPrPrefix = prPrefix;

      public void run2()
      {
        Cache cache = getCache();
        PartitionedRegion pr = (PartitionedRegion)cache
            .getRegion(Region.SEPARATOR + "testMemoryOfPartitionRegion" + 0);
        assertNotNull("pr can not be null", pr);
        assertNotNull("DataStore cannot be null", pr.getDataStore());
        long prMemory = pr.getDataStore().currentAllocatedMemory();
        innerPrPrefix = "createPRForStrorage";
        PartitionedRegion prForStorage = (PartitionedRegion)cache
            .getRegion(Region.SEPARATOR + innerPrPrefix + 0);
        for (int i = innerStartIndexForKey; i < innerEndIndexForKey; i++) {
          prForStorage.put(new Long(i), new Long(prMemory));

        }

      }
    };

    return calulateTotalMemory;
  }

  private int getNoBucketsExpectedOnEachNode()
  {
    int noBucketsExpectedOnEachNode = (totalBucketNumProperty / 4) - 1;
    return noBucketsExpectedOnEachNode;
  }

  protected RegionAttributes createRegionAttrs(int red, int localMaxMem, int numBuckets)
  {
    AttributesFactory attr = new AttributesFactory();
    attr.setMirrorType(MirrorType.NONE);
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes prAttr = paf.setRedundantCopies(red)
        .setLocalMaxMemory(localMaxMem)
        .setTotalNumBuckets(numBuckets)
        .create();
    attr.setPartitionAttributes(prAttr);
    return attr.create();
  }

  private void putForLocalMaxMemoryInMultiplePR(String regionName)
      throws Throwable
  {
    final int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = vm[0].invokeAsync(doPutForLocalMaxMemory(regionName, "vm0"));
    async[1] = vm[1].invokeAsync(doPutForLocalMaxMemory(regionName, "vm1"));
    async[2] = vm[2].invokeAsync(doPutForLocalMaxMemory(regionName, "vm2"));
    async[3] = vm[3].invokeAsync(doPutForLocalMaxMemory(regionName, "vm3"));

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }
    /** testing whether exception occurred */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      assertTrue(async[count].exceptionOccurred());
      assertTrue(async[count].getException() instanceof PartitionedRegionStorageException);
    }
  }

  private CacheSerializableRunnable doPutForLocalMaxMemory(
      final String regionName, final String key) throws Exception
  {

    CacheSerializableRunnable putForLocalMaxMemory = new CacheSerializableRunnable(
        "putForLocalMaxMemory") {
      public void run2()
      {
        final int MAX_SIZE = 1024;
        Cache cache = getCache();
        byte Obj[] = new byte[MAX_SIZE];
        Arrays.fill(Obj, (byte)'A');
        PartitionedRegion pr = (PartitionedRegion)cache
            .getRegion(Region.SEPARATOR + regionName);
        for (int i = 0; i < MAX_SIZE * 2; i++) {
          pr.put(key + i, Obj);
          getLogWriter().info("MAXSIZE : " + i);
        }
        getLogWriter().info("Put successfully done for vm" + key);
      }
    };
    return putForLocalMaxMemory;
  }
}
