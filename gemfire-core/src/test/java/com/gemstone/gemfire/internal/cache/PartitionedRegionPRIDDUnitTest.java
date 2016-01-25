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
import java.util.Iterator;
import java.util.List;
import java.util.*;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.*;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * 
 * @author gthombar This class tests PRID generation in multiple partiton
 *         regions on 4 VMs
 */
public class PartitionedRegionPRIDDUnitTest extends
    PartitionedRegionDUnitTestCase
{

  /** Maximum number of regions * */
  public final static int MAX_REGIONS = 1;

  /** redundancy used for the creation of the partition region */
  // int redundancy = 0;

  /** local maxmemory used for the creation of the partition region */
  int localMaxMemory = 200;

  /** to store references of 4 vms */
  VM vm[] = new VM[4];

  public PartitionedRegionPRIDDUnitTest(String name) {
    super(name);
  }
  
  /**
   * This test performs following operations 1. creates 25 partition regions on
   * 3 nodes. 2. creates more 25 partition regions on 4 nodes 3. tests PRID
   * generation
   * 
   */
  public void testPRIDGenerationInMultiplePartitionRegion() throws Exception
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);

    /** Prefix will be used for naming the partititon Region */
    String prPrefix = "testBucketCreationInMultiPlePartitionRegion";

    /** these indices represents range of partition regions present in each VM */
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;

    int startIndexForNode;
    int endIndexForNode;
    List vmList = new ArrayList();
    startIndexForNode = 0;
    endIndexForNode = 3;
    addNodeToList(vmList, startIndexForNode, endIndexForNode);
    localMaxMemory = 200;
    final int redundancy = 0;
    // Create 1/2 * MAX_REGIONS regions in VM 0,1,2 with scope D_ACK.
    createPartitionRegion(vmList, startIndexForRegion, endIndexForRegion,
        localMaxMemory, redundancy, prPrefix);
    getLogWriter()
        .info(
            "testPRIDGenerationInMultiplePartitionRegion() - Partition regions on 3 nodes successfully created");

    startIndexForRegion = MAX_REGIONS;
    endIndexForRegion = 2 * MAX_REGIONS;
    // creating partition regions on 4 with scope DISTRIBUTED_ACK
    startIndexForNode = 0;
    endIndexForNode = 4;
    addNodeToList(vmList, startIndexForNode, endIndexForNode);
    localMaxMemory = 200;
    final int pr2_redundancy = 1;
    // Create MAX_REGION regions on VM 0,1,2,3. 
    // VM 3 contains regions from id MAX_REGIONS to 2*MAX_REGIONS only.
    createPartitionRegion(vmList, startIndexForRegion, endIndexForRegion,
        localMaxMemory, pr2_redundancy, prPrefix);
    getLogWriter()
        .info(
            "testPRIDGenerationInMultiplePartitionRegion() - Partition regions on 4 nodes successfully created");
    // validating PRID generation for multiple partition regions    
    int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    async[0] = vm[0].invokeAsync(validatePRIDCreation(0,
        endIndexForRegion, prPrefix));
    async[1] = vm[1].invokeAsync(validatePRIDCreation(0,
        endIndexForRegion, prPrefix));
    async[2] = vm[2].invokeAsync(validatePRIDCreation(0,
        endIndexForRegion, prPrefix));
    async[3] = vm[3].invokeAsync(validatePRIDCreation(MAX_REGIONS,
        endIndexForRegion, prPrefix));

    /** main thread is waiting for the other threads to complete */
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      DistributedTestCase.join(async[count], 30 * 1000, getLogWriter());
    }
    
    for (int count = 0; count < AsyncInvocationArrSize; count++) {
      if (async[count].exceptionOccurred()) {
        fail("VM " + count 
            + " encountered this exception during async invocation", 
            async[count].getException());
      }
    }
  }


  /**
   * This function perfoms following checks on PRID creation 1. PRID generated
   * should be between 0 to number of partition regions in distributed system 2.
   * PRID should be unique for the partition regions
   * 
   * @param startIndexForRegion
   * @param endIndexForRegion
   * @return
   */
  private CacheSerializableRunnable validatePRIDCreation(
      final int startIndexForRegion, final int endIndexForRegion, final String prPrefix)
  {
    CacheSerializableRunnable validatePRID = new CacheSerializableRunnable(
        "validatePRIDCreation") {
      String innerPrPrefix = prPrefix;

      public void run2()
      {
        int noPartitionRegions = endIndexForRegion - startIndexForRegion;
        Cache cache = getCache();
        // getting root region
        Region root = cache.getRegion(Region.SEPARATOR
            + PartitionedRegionHelper.PR_ROOT_REGION_NAME);
        // root region should niot be null
        assertNotNull("Root region can not be null", root);
        // getting allParititionedRegions
//        Region allPartitionedRegions = root
//            .getSubregion(PartitionedRegionHelper.PARTITIONED_REGION_CONFIG_NAME);
        // allPartitionedRegion should not be null
//        assertNotNull("allPartitionedRegion can not be null",
//            allPartitionedRegions);
        // scope of all partition region should be DISTRIBUTED_ACK
        List prIdList = new ArrayList();
        for (int i = startIndexForRegion; i < endIndexForRegion; i++) {
          final String rName = Region.SEPARATOR + innerPrPrefix + i;
          PartitionedRegion pr = (PartitionedRegion)cache
              .getRegion(rName);
          assertNotNull("This Partitioned Region " + rName + " cannot be null", pr);
          PartitionRegionConfig prConfig = (PartitionRegionConfig)root
              .get(pr.getRegionIdentifier());
          assertNotNull("PRConfig for Partitioned Region " + rName + " can not be null", prConfig);
          prIdList.add(Integer.toString(prConfig.getPRId()));

          // this partition region should present in prIdToPr
          /*if (PartitionedRegion.prIdToPR.containsKey(Integer.toString(prConfig
              .getPRId())) == false)
            fail("this partition region is not present in the prIdToPR map "
                + pr.getName());*/
        }

        // checking uniqueness of prId in allPartitionRegion
        SortedSet prIdSet = new TreeSet(prIdList);
        if (prIdSet.size() != prIdList.size())
          fail("Duplicate PRID are generated");

        // prId generated should be between 0 to number of partition regions-1
        Iterator prIdSetItr = prIdSet.iterator();
        while (prIdSetItr.hasNext()) {
          int val = Integer.parseInt((String)prIdSetItr.next());
          if (val > noPartitionRegions - 1 & val < 0) {
            fail("PRID limit is out of range");
          }
        }
        // no of PRID generated in allPartitionRegion should be equal to number of partition region
        if (prIdSet.size() != noPartitionRegions)
          fail("Different PRID generated equal to " + prIdSet.size());

        // no of PRID generated in prIdToPR should be equal to number of partition region
        if (PartitionedRegion.prIdToPR.size() != noPartitionRegions)
          fail("number of entries in the prIdToPR is "
              + PartitionedRegion.prIdToPR.size());

        // checking uniqueness of prId in prIdToPR
        SortedSet prIdPRSet = new TreeSet(PartitionedRegion.prIdToPR.keySet());
        if (prIdPRSet.size() != PartitionedRegion.prIdToPR.size())
          fail("Duplicate PRID are generated in prIdToPR");

        getLogWriter().info("Size of allPartition region : " + prIdSet.size());
        getLogWriter()
            .info("Size of prIdToPR region     : " + prIdPRSet.size());
        getLogWriter().info("PRID generated successfully");
      }
    };
    return validatePRID;
  }

  /**
   * This function createas multiple partition regions on nodes specified in the
   * vmList
   */
  private void createPartitionRegion(List vmList, int startIndexForRegion,
      int endIndexForRegion, int localMaxMemory, int redundancy, String prPrefix) throws Exception
  {
    int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    int numNodes = 0;
    Iterator nodeIterator = vmList.iterator();
    while (nodeIterator.hasNext()) {
      VM vm = (VM)nodeIterator.next();
      async[numNodes] = vm.invokeAsync(createMultiplePartitionRegion(prPrefix, startIndexForRegion,
              endIndexForRegion, redundancy, localMaxMemory));
      numNodes++;
    }
    for (int i = 0; i < numNodes; i++) {
      DistributedTestCase.join(async[i], 30 * 1000, getLogWriter());
    }
    
    for (int i = 0; i < numNodes; i++) {
      if (async[i].exceptionOccurred()) {
        fail("VM " + i 
            + " encountered this exception during async invocation", 
            async[i].getException());
      }
    }
  }

  /**
   * This function adds nodes to node list in the range specified by
   * startIndexForNode and endIndexForNode
   */
  private void addNodeToList(List vmList, int startIndexForNode,
      int endIndexForNode)
  {
    vmList.clear();
    for (int i = startIndexForNode; i < endIndexForNode; i++) {
      vmList.add(vm[i]);
    }
  }

  /** creates 4 VMS on the given host */
  private void createVMs(Host host)
  {
    for (int i = 0; i < 4; i++) {
      vm[i] = host.getVM(i);
    }
  }
}
