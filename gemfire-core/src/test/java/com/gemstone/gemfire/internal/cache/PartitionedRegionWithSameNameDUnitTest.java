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
/*
 * Created on Mar 24, 2006
 * 
 * TODO To change the template for this generated file go to Window -
 * Preferences - Java - Code Style - Code Templates
 */
package com.gemstone.gemfire.internal.cache;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author gthombar This test is to verify creation of partition region and
 *         distributed region with same name.
 */
public class PartitionedRegionWithSameNameDUnitTest extends
    PartitionedRegionDUnitTestCase
{
  /** Prefix is used in name of Partition Region */
  private static String prPrefix = null;

  /** Maximum number of regions * */
  static int MAX_REGIONS = 1;

  /** redundancy used for the creation of the partition region */
  int redundancy = 0;

  /** local maxmemory used for the creation of the partition region */
  int localMaxMemory = 200;

  /** to store references of 4 vms */
  VM vm[] = new VM[4];

  /**
   * Used to decide whether to create subregion as distributed region or
   * partitioned region
   */
  static protected final int DISTRIBUTED_REGION = 0;

  static protected final int PARTITIONED_REGION = 1;

  public PartitionedRegionWithSameNameDUnitTest(String name) {
    super(name);
  }

  /**
   * This test performs following operation <br>
   * 1. Creates partition region with scope = DISTRIBUTED_ACK and localMaxMemoty
   * =200 on vm0. </br> <br>
   * 2. then creates distributed region with the same name as partition region
   * on vm0 </br> In this test RegionExistException is expected while creating
   * region with the same as partition region.
   */
  public void testNameWithPartitionRegionFirstOnSameVM()
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);

    prPrefix = "testNameWithPartitionRegionFirstOnSameVM";
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    int startIndexForNode = 0;
    int endIndexForNode = 1;
    boolean firstCreationFlag = true;
    boolean multipleVMFlag = false;
    List vmList;
    // creating multiple partition regions on vm0 with scope =
    // DISTRIBUTED_ACK localMaxMemory=200 redundancy = 2
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 200;
    redundancy = 1;
    // to indicate that partition Region is created first
    firstCreationFlag = true;
    createPartitionRegion(vmList, startIndexForRegion, endIndexForRegion,
        localMaxMemory, redundancy, firstCreationFlag, multipleVMFlag);
    LogWriterUtils.getLogWriter()
        .info(
            "testNameWithPartitionRegionFirstOnSameVM() - Partition Regions successfully created ");
    // creating distributed region on same vm with same name as previouslu
    // created partition region
    startIndexForNode = 0;
    endIndexForNode = 1;
    firstCreationFlag = false;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    createDistributedRegion(vmList, startIndexForRegion, endIndexForRegion,
        Scope.DISTRIBUTED_ACK, firstCreationFlag, multipleVMFlag);
    LogWriterUtils.getLogWriter()
        .info(
            "testNameWithPartitionRegionFirstOnSameVM() - test completed successfully ");
  }

  /**
   * This test performs following operation <br>
   * 1. Creates distributed region on vm0. </br> <br>
   * 2. then creates Partition region with the same name as region on vm0 </br>
   * In this test RegionExistException is expected while creating Partition
   * region with the same as region.
   */
  public void testNameWithDistributedRegionFirstOnSameVM()
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);

    prPrefix = "testNameWithLocalRegionFirstOnSameVM";
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    int startIndexForNode = 0;
    int endIndexForNode = 1;
    boolean firstCreationFlag = true;
    boolean multipleVMFlag = false;

    List vmList;
    // creating distributed region on vm0 with scope = Scope.DISTRIBUTED_ACK
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    firstCreationFlag = true;
    createDistributedRegion(vmList, startIndexForRegion, endIndexForRegion,
        Scope.DISTRIBUTED_ACK, firstCreationFlag, multipleVMFlag);

    // creating multiple partition regions on vm0 with scope =
    // DISTRIBUTED_ACK localMaxMemory=200 redundancy = 2
    localMaxMemory = 200;
    redundancy = 1;
    firstCreationFlag = false;
    createPartitionRegion(vmList, startIndexForRegion, endIndexForRegion,
        localMaxMemory, redundancy, firstCreationFlag, multipleVMFlag);

    LogWriterUtils.getLogWriter()
        .info(
            "testNameWithPartitionRegionFirstOnSameVM() - test completed successfully ");
  }

  /**
   * This test performs following operations <br>
   * 1. Creates partition region with scope= DISTRIBUTED_ACK and localMaxMemory =
   * 200 on vm0.</br> <br>
   * 2. creates distributed region with the same name as partition region on
   * vm1,vm2,vm3</br> InternalGemFireException is expected while creating
   * region.
   */

  public void testNameWithPartitionRegionFirstOnDifferentVM()
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);

    prPrefix = "testNameWithPartitionRegionFirstOnDifferentVM";
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    int startIndexForNode = 0;
    int endIndexForNode = 1;
    boolean firstCreationFlag = true;
    boolean multipleVMFlag = true;
    List vmList;
    // creating multiple partition regions on 1 node with scope =
    // DISTRIBUTED_ACK localMaxMemory=200 redundancy = 2
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 200;
    redundancy = 1;
    // to indicate that partition Region is created first
    firstCreationFlag = true;
    createPartitionRegion(vmList, startIndexForRegion, endIndexForRegion,
        localMaxMemory, redundancy, firstCreationFlag, multipleVMFlag);
    LogWriterUtils.getLogWriter()
        .info(
            "testNameWithPartitionRegionFirstOnDifferentVM() - Partition Regions successfully created ");
    // creating distrubuted region with the scope = DISTRIBUTED_ACK on
    // vm1,vm2,vm3
    startIndexForRegion = 0;
    endIndexForRegion = MAX_REGIONS;
    startIndexForNode = 1;
    endIndexForNode = 4;
    firstCreationFlag = false;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    createDistributedRegion(vmList, startIndexForRegion, endIndexForRegion,
        Scope.DISTRIBUTED_ACK, firstCreationFlag, multipleVMFlag);
    LogWriterUtils.getLogWriter()
        .info(
            "testNameWithPartitionRegionFirstOnDifferentVM() - test completed successfully ");
  }

  /**
   * This test performs following operations <br>
   * 1. Creates region on vm0</br> <br>
   * 2. creates partition region with the same name as region on vm1,vm2,vm3</br>
   * InternalGemFireException is expected while creating region.
   */

  public void testNameWithDistributedRegionFirstOnDifferentVM()
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);

    prPrefix = "testNameWithLocalRegionFirstOnDifferentVM";
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    int startIndexForNode = 0;
    int endIndexForNode = 1;
    boolean firstCreationFlag = true;
    boolean multipleVMFlag = true;
    List vmList;
    // creating multiple partition regions on 1 node with scope =
    // DISTRIBUTED_ACK localMaxMemory=200 redundancy = 2
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    firstCreationFlag = true;
    createDistributedRegion(vmList, startIndexForRegion, endIndexForRegion,
        Scope.DISTRIBUTED_NO_ACK, firstCreationFlag, multipleVMFlag);

    startIndexForNode = 1;
    endIndexForNode = 4;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 200;
    redundancy = 1;
    firstCreationFlag = false;
    createPartitionRegion(vmList, startIndexForRegion, endIndexForRegion,
        localMaxMemory, redundancy, firstCreationFlag, multipleVMFlag);

    LogWriterUtils.getLogWriter()
        .info(
            "testNameWithLocalRegionFirstOnDifferentVM() - test completed successfully ");
  }

  /**
   * This test performs following operations <br>
   * 1. Creates Distributed region with scope = LOCAL on vm0.</br> <br>
   * 2. creates partition region with the same name as distributed region on
   * vm1,vm2,vm3</br> NoException is expected.
   */
  public void testLocalRegionFirst()
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);

    prPrefix = "testPartitionRegionVsLocalRegionFirst";
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    int startIndexForNode = 0;
    int endIndexForNode = 1;
    boolean firstCreationFlag = true;
    boolean multipleVMFlag = true;
    List vmList;
    // creating local region on vm0
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    firstCreationFlag = true;
    createDistributedRegion(vmList, startIndexForRegion, endIndexForRegion,
        Scope.LOCAL, firstCreationFlag, multipleVMFlag);

    // creating partition region with the same name as local region on
    // vm1,vm2,vm3
    startIndexForNode = 1;
    endIndexForNode = 4;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 200;
    redundancy = 1;
    firstCreationFlag = true;
    createPartitionRegion(vmList, startIndexForRegion, endIndexForRegion,
        localMaxMemory, redundancy, firstCreationFlag, multipleVMFlag);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionRegionVsLocalRegionFirst() - test completed successfully ");
  }

  /**
   * This test performs following operations <br>
   * 1. Creates partition region with scope = DISTRIBUTED_NO_ACK on vm0.</br>
   * <br>
   * 2. creates distributed region scope = LOCAL with the same name as
   * partitioned region on vm1,vm2,vm3</br> NoException is expected.
   */
  public void testLocalRegionSecond()
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);

    prPrefix = "testPartitionRegionVsLocalRegionSecond";
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    int startIndexForNode = 0;
    int endIndexForNode = 1;
    boolean firstCreationFlag = true;
    boolean multipleVMFlag = true;
    List vmList;
    // creating multiple partition regions on 1 node with scope =
    // DISTRIBUTED_ACK localMaxMemory=200 redundancy = 2
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 200;
    redundancy = 1;
    firstCreationFlag = true;
    createPartitionRegion(vmList, startIndexForRegion, endIndexForRegion,
        localMaxMemory, redundancy, firstCreationFlag, multipleVMFlag);
    // creating local region with the same name as partition region on
    // vm1,vm2,vm3
    startIndexForNode = 1;
    endIndexForNode = 4;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    firstCreationFlag = true;
    createDistributedRegion(vmList, startIndexForRegion, endIndexForRegion,
        Scope.LOCAL, firstCreationFlag, multipleVMFlag);
    LogWriterUtils.getLogWriter()
        .info(
            "testPartitionRegionVsLocalRegionSecond() - test completed successfully ");
  }

  /**
   * This test performs following operations <br>
   * 1. Creates partitoned region as parent region on all vms </br> <br>
   * 2. Creates distributed subregion of parent region </br>
   * OperationNotSupportedException is expected.
   */
  public void testWithPartitionedRegionAsParentRegionAndDistributedSubRegion()
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);

    prPrefix = "parent_partitioned_region";
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    int startIndexForNode = 0;
    int endIndexForNode = 4;
    boolean firstCreationFlag = true;
    boolean multipleVMFlag = true;
    List vmList;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    // creating parent region as partioned region on all vms.
    localMaxMemory = 200;
    redundancy = 1;
    firstCreationFlag = true;
    createPartitionRegion(vmList, startIndexForRegion, endIndexForRegion,
        localMaxMemory, redundancy, firstCreationFlag, multipleVMFlag);
    LogWriterUtils.getLogWriter()
        .info(
            "testWithPartitionedRegionAsParentRegionAndDistributedSubRegion() - Parent region as partitioned region is created ");
    // create subregion of partition region
    createSubRegionOfPartitionedRegion(vmList, DISTRIBUTED_REGION);
    LogWriterUtils.getLogWriter()
        .info(
            "testWithPartitionedRegionAsParentRegionAndDistributedSubRegion() completed Successfully ");
  }

  /**
   * This test performs following operations <br>
   * 1. Creates partitoned region as parent region on all vms </br> <br>
   * 2. Creates partitioned subregion of parent region </br>
   * OperationNotSupportedException is expected
   */

  public void testWithPartitionedRegionAsParentRegionAndPartitionedSubRegion()
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);

    prPrefix = "parent_partitioned_region";
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    int startIndexForNode = 0;
    int endIndexForNode = 4;
    boolean firstCreationFlag = true;
    boolean multipleVMFlag = true;
    List vmList;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    // creating parent region as partioned region on all vms.
    localMaxMemory = 200;
    redundancy = 1;
    firstCreationFlag = true;
    createPartitionRegion(vmList, startIndexForRegion, endIndexForRegion,
        localMaxMemory, redundancy, firstCreationFlag, multipleVMFlag);
    LogWriterUtils.getLogWriter()
        .info(
            "testWithPartitionedRegionAsParentRegionAndPartitionedSubRegion() - Parent region as partitioned region is created ");
    // create subregion of partition region
    createSubRegionOfPartitionedRegion(vmList, PARTITIONED_REGION);
    LogWriterUtils.getLogWriter()
        .info(
            "testWithPartitionedRegionAsParentRegionAndPartitionedSubRegion() completed Successfully ");
  }

  /**
   * This test performs the following operatiions <br>
   * 1.Creates a distributed region as parent on vm0,vm1,vm2 and vm3.</br> <br>
   * 2.Creates a partitioned region as subregion of parent on vm0. </br> <br>
   * 3.Creates a distributed region as subregion of parent on vm1,vm2,vm3 </br>
   * In this case InternalGemFireException is expected.
   */
  public void testWithSubRegionPartitionedRegionFirst()
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);

    prPrefix = "parent_region";
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    int startIndexForNode = 0;
    int endIndexForNode = 4;
    boolean firstCreationFlag = true;
    boolean multipleVMFlag = true;
    List vmList;

    // creating parent region as distributed region on all vms.
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    firstCreationFlag = true;
    createDistributedRegion(vmList, startIndexForRegion, endIndexForRegion,
        Scope.DISTRIBUTED_ACK, firstCreationFlag, multipleVMFlag);
    LogWriterUtils.getLogWriter().info(
        "testWithSubRegionPartitionedRegionFirst() - Parent region is created");
    // creating distributed region as subregion of parent on vm0
    prPrefix = "child_region";
    startIndexForNode = 0;
    endIndexForNode = 1;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    createPartitionedSubRegion(vmList, firstCreationFlag);
    LogWriterUtils.getLogWriter()
        .info(
            "testWithSubRegionPartitionedRegionFirst() - Partitioned sub region on vm0 ");
    // creating partiton region as subregion of parent region with the same name
    firstCreationFlag = false;
    startIndexForNode = 1;
    endIndexForNode = 4;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    createDistributedSubRegion(vmList, firstCreationFlag);
    LogWriterUtils.getLogWriter().info(
        "testWithSubRegionPartitionedRegionFirst() completed successfully ");

  }

  /**
   * This test performs the following operatiions <br>
   * 1.Creates a distributed region as parent on vm0,vm1,vm2 and vm3.</br> <br>
   * 2.Creates a distributed region as subregion of parent on vm0. </br> <br>
   * 3.Creates a partitioned region as subregion of parent on vm1,vm2,vm3 </br>
   * In this case IllegalStateException is expected.
   */
  public void testWithSubRegionDistributedRegionFirst()
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    createVMs(host);

    prPrefix = "parent_region";
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    int startIndexForNode = 0;
    int endIndexForNode = 4;
    boolean firstCreationFlag = true;
    boolean multipleVMFlag = true;
    List vmList;

    // creating parent region as distributed region on all vms.
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    firstCreationFlag = true;
    createDistributedRegion(vmList, startIndexForRegion, endIndexForRegion,
        Scope.DISTRIBUTED_ACK, firstCreationFlag, multipleVMFlag);
    LogWriterUtils.getLogWriter().info(
        "testWithSubRegionDistributedRegionFirst() - Parent region is created");
    // creating distributed region as subregion of parent on vm0
    prPrefix = "child_region";
    startIndexForNode = 0;
    endIndexForNode = 1;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    createDistributedSubRegion(vmList, firstCreationFlag);
    LogWriterUtils.getLogWriter()
        .info(
            "testWithSubRegionDistributedRegionFirst() - Distributed sub region on vm0 ");
    // creating partiton region as subregion of parent region with the same name
    firstCreationFlag = false;
    startIndexForNode = 1;
    endIndexForNode = 4;
    vmList = addNodeToList(startIndexForNode, endIndexForNode);
    createPartitionedSubRegion(vmList, firstCreationFlag);
    LogWriterUtils.getLogWriter().info(
        "testWithSubRegionDistributedRegionFirst() completed successfully ");

  }

  /** this function creates distributed subregion of parent region. */
  private void createDistributedSubRegion(List vmList, boolean firstCreationFlag)
  {
    Iterator nodeIterator = vmList.iterator();
    while (nodeIterator.hasNext()) {
      VM vm = (VM)nodeIterator.next();
      vm.invoke(createSubRegion(firstCreationFlag, DISTRIBUTED_REGION));
    }
  }

  /** this function creates partitioned subregion of parent region */
  private void createPartitionedSubRegion(List vmList, boolean firstCreationFlag)
  {
    Iterator nodeIterator = vmList.iterator();
    while (nodeIterator.hasNext()) {
      VM vm = (VM)nodeIterator.next();
      vm.invoke(createSubRegion(firstCreationFlag, PARTITIONED_REGION));
    }
  }

  /** This function creates subregion of partition region */
  private void createSubRegionOfPartitionedRegion(List vmList, int regionType)
  {
    Iterator nodeIterator = vmList.iterator();
    while (nodeIterator.hasNext()) {
      VM vm = (VM)nodeIterator.next();
      vm.invoke(SubRegionOfPartitonedRegion(regionType));
    }
  }

  private CacheSerializableRunnable SubRegionOfPartitonedRegion(
      final int regionType)
  {
    CacheSerializableRunnable subRegionOfPartiotionRegion = new CacheSerializableRunnable(
        "subRegionOfPartiotionRegion") {
      int innerRegionType = regionType;

      public void run2() throws CacheException
      {
        Cache cache = getCache();
        PartitionedRegion parentRegion = (PartitionedRegion)cache
            .getRegion(Region.SEPARATOR + "parent_partitioned_region0");
        assertNotNull("Parent region cannot be null ", parentRegion);
        switch (innerRegionType) {
        case DISTRIBUTED_REGION: {

          AttributesFactory af = new AttributesFactory();
          af.setScope(Scope.DISTRIBUTED_ACK);
          RegionAttributes ra = af.create();
          try {
            parentRegion.createSubregion(Region.SEPARATOR
                + "child_region", ra);
            fail("Distributed Subregion of partition region is created ");
          }
          catch (UnsupportedOperationException expected) {
            // getLogWriter()
            // .info(
            // "Expected exception OperationNotSupportedException for creating
            // distributed region as subregion ");
          }
        }
          break;
        case PARTITIONED_REGION: {
          try {
            parentRegion.createSubregion("child_region",
                createRegionAttrsForPR(0, 200));
            fail("Partitioneed Subregion of partition region is created ");
          }
          catch (UnsupportedOperationException expected) {
            // getLogWriter()
            // .info(
            // "Expected exception OperationNotSupportedException for creating
            // partitioned region as subregion ");
          }

        }
        }
      }
    };

    return subRegionOfPartiotionRegion;
  }

  private CacheSerializableRunnable createSubRegion(
      final boolean firstCreationFlag, final int regionType)
  {
    CacheSerializableRunnable createSubRegion = new CacheSerializableRunnable(
        "createSubRegion") {
      boolean innerFirstCreationFlag = firstCreationFlag;

      int innerRegionType = regionType;

      public void run2() throws CacheException
      {
        Cache cache = getCache();
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        RegionAttributes ra = af.create();
        Region parentRegion = cache.getRegion(Region.SEPARATOR
            + "parent_region0");
        assertNotNull("Parent region is not null", parentRegion);
        if (innerFirstCreationFlag) {
          switch (innerRegionType) {
          case DISTRIBUTED_REGION: {
            Region childRegion = parentRegion.createSubregion("child_region",
                ra);
            LogWriterUtils.getLogWriter().info(
                "Distributed Subregion is created as : "
                    + childRegion.getName());
          }
            break;
          case PARTITIONED_REGION: {
            Region childRegion = parentRegion.createSubregion("child_region",
                createRegionAttrsForPR(0, 200));
            LogWriterUtils.getLogWriter().info(
                "Partitioned Subregion is created as : "
                    + childRegion.getName());

          }

          }
        }
        else {
          switch (innerRegionType) {
          case DISTRIBUTED_REGION:

            final String expectedExceptions = IllegalStateException.class
                .getName();
            getCache().getLogger().info(
                "<ExpectedException action=add>" + expectedExceptions
                    + "</ExpectedException>");
            try {
              parentRegion.createSubregion("child_region",
                  ra);
              fail("distributed subregion of the same name as partitioned region is created");
            }
            catch (IllegalStateException expected) {
              // getLogWriter()
              // .info(
              // "Got a correct exception when creating distributed sub region a
              // same name of partitioned region");
            }
            getCache().getLogger().info(
                "<ExpectedException action=remove>" + expectedExceptions
                    + "</ExpectedException>");
            break;
          case PARTITIONED_REGION:

            final String expectedExceptions_pr = IllegalStateException.class
                .getName();
            getCache().getLogger().info(
                "<ExpectedException action=add>" + expectedExceptions_pr
                    + "</ExpectedException>");
            try {
              parentRegion.createSubregion("child_region",
                  createRegionAttrsForPR(0, 200));
              fail("partitioned subregion of the same name as distributed region is created");
            }
            catch (IllegalStateException expected) {
              // getLogWriter()
              // .info(
              // "Got a correct exception when creating distributed sub region a
              // same name of partitioned region");
            }
            getCache().getLogger().info(
                "<ExpectedException action=remove>" + expectedExceptions_pr
                    + "</ExpectedException>");
          }
        }
      }
    };
    return createSubRegion;
  }

  /**
   * This function createas multiple partition regions on nodes specified in the
   * vmList
   */
  private void createPartitionRegion(List vmList, int startIndexForRegion,
      int endIndexForRegion, int localMaxMemory, int redundancy, boolean firstCreationFlag,
      boolean multipleVMFlag)
  {
    Iterator nodeIterator = vmList.iterator();
    while (nodeIterator.hasNext()) {
      VM vm = (VM)nodeIterator.next();
      vm.invoke(createMultiplePartitionRegion(prPrefix, startIndexForRegion,
          endIndexForRegion, redundancy, localMaxMemory, firstCreationFlag,
          multipleVMFlag));
    }
  }

  /**
   * This function createas multiple partition regions on nodes specified in the
   * vmList
   */
  private void createDistributedRegion(List vmList, int startIndexForRegion,
      int endIndexForRegion, Scope scope, boolean firstCreationFlag,
      boolean multipleVMFlag)
  {
    Iterator nodeIterator = vmList.iterator();
    while (nodeIterator.hasNext()) {
      VM vm = (VM)nodeIterator.next();
      vm.invoke(createMultipleDistributedlRegion(prPrefix, startIndexForRegion,
          endIndexForRegion, scope, firstCreationFlag, multipleVMFlag));
    }
  }

  CacheSerializableRunnable createMultipleDistributedlRegion(
      final String prPrefix, final int startIndexForRegion,
      final int endIndexForRegion, final Scope scope,
      final boolean firstCreationFlag, final boolean multipleVMFlag)
  {
    CacheSerializableRunnable createLocalRegion = new CacheSerializableRunnable(
        "createDistributedRegion") {
      String innerPrPrefix = prPrefix;

      int innerStartIndexForRegion = startIndexForRegion;

      int innerEndIndexForRegion = endIndexForRegion;

      Scope innerScope = scope;

      boolean innerFirstCreationFlag = firstCreationFlag;

      public void run2() throws CacheException
      {
        Cache cache = getCache();

        AttributesFactory af = new AttributesFactory();
        af.setScope(innerScope);
        RegionAttributes ra = af.create();
        if (firstCreationFlag) {
          for (int i = innerStartIndexForRegion; i < innerEndIndexForRegion; i++) {
            try {
              cache.createRegion(innerPrPrefix + i, ra);
            }
            catch (RegionExistsException ex) {
              Assert.fail(
                  "Got incorrect exception because the partition region being created prior to local region",
                  ex);
            }
          }
        }
        else {
          for (int i = innerStartIndexForRegion; i < innerEndIndexForRegion; i++) {
            if (!multipleVMFlag) {
              try {
                cache.createRegion(innerPrPrefix + i, ra);
                fail("test failed : Distributed region with same name as Partitioned region gets created");
              }
              catch (RegionExistsException expected) {
                // getLogWriter()
                // .info(
                // "Expected exception RegionExistsException for creating
                // distributed region with the same name as Partition Region"
                // + ex);
              }
            }
            else {
              final String expectedExceptions = IllegalStateException.class
                  .getName();
              getCache().getLogger().info(
                  "<ExpectedException action=add>" + expectedExceptions
                      + "</ExpectedException>");
              try {
                cache.createRegion(innerPrPrefix + i, ra);
                fail("test failed : Distributed region with same name as Partitioned region gets created");
              }
              catch (IllegalStateException expected) {
                // getLogWriter()
                // .info(
                // "Expected exception IllegalStateException for creating
                // distributed region with the same name as Partition Region"
                // + ex);
              }

              getCache().getLogger().info(
                  "<ExpectedException action=remove>" + expectedExceptions
                      + "</ExpectedException>");
            }
          }
        }
      }
    };
    return createLocalRegion;
  }

  /**
   * This function creates multiple partition regions in a VM. The name of the
   * Partition Region will be PRPrefix+index (index starts from
   * startIndexForRegion and ends to endIndexForRegion)
   * 
   * @param PRPrefix :
   *          Used in the name of the Partition Region
   * 
   * These indices Represents range of the Partition Region
   * @param startIndexForRegion :
   * @param endIndexForRegion
   * @param redundancy
   * @param localmaxMemory
   * @return
   */
  public CacheSerializableRunnable createMultiplePartitionRegion(
      final String PRPrefix, final int startIndexForRegion,
      final int endIndexForRegion, final int redundancy, final int localmaxMemory,
      final boolean firstCreationFlag, final boolean multipleVMFlag)
  {
    SerializableRunnable createPRs = new CacheSerializableRunnable(
        "createPrRegions") {
      String innerPRPrefix = PRPrefix;

      int innerStartIndexForRegion = startIndexForRegion;

      int innerEndIndexForRegion = endIndexForRegion;

      int innerRedundancy = redundancy;

      int innerlocalmaxMemory = localmaxMemory;

      public void run2() throws CacheException
      {
        Cache cache = getCache();
        if (firstCreationFlag) {
          for (int i = startIndexForRegion; i < endIndexForRegion; i++) {
            cache.createRegion(innerPRPrefix + i,
                createRegionAttrsForPR(innerRedundancy, innerlocalmaxMemory));
          }
        }
        else {
          for (int i = startIndexForRegion; i < endIndexForRegion; i++) {
            if (!multipleVMFlag) {
              try {
                cache.createRegion(innerPRPrefix
                    + i, createRegionAttrsForPR(innerRedundancy, innerlocalmaxMemory));
                fail("test failed : partition region with same name as local region is created");
              }
              catch (RegionExistsException expected) {
                // getLogWriter()
                // .info(
                // "Expected exception RegionExistsException for creating
                // partition region with same name as of the distributed region
                // ", ex);
              }
            }
            else {
              final String expectedExceptions = IllegalStateException.class
                  .getName();
              getCache().getLogger().info(
                  "<ExpectedException action=add>" + expectedExceptions
                      + "</ExpectedException>");
              try {
                cache.createRegion(innerPRPrefix
                    + i, createRegionAttrsForPR(innerRedundancy, innerlocalmaxMemory));
                fail("test failed : partition region with same name as distributed region is created");
              }
              catch (IllegalStateException expected) {
                // getLogWriter()
                // .info(
                // "Expected exception IllegalStateException for creating
                // partition region with same name as of the distributed region"
                // + ex);
              }
              getCache().getLogger().info(
                  "<ExpectedException action=remove>" + expectedExceptions
                      + "</ExpectedException>");
            }
          }
        }
        LogWriterUtils.getLogWriter()
            .info(
                "createMultiplePartitionRegion() - Partition Regions Successfully Completed ");
      }
    };
    return (CacheSerializableRunnable)createPRs;
  }

  /**
   * This function creates Region attributes with provided scope,redundancy and
   * localmaxMemory
   */
  public static RegionAttributes createRegionAttrsForPR(int red, int localMaxMem)
  {
    AttributesFactory attr = new AttributesFactory();
    attr.setMirrorType(MirrorType.NONE);
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes prAttr = paf.setRedundantCopies(red)
        .setLocalMaxMemory(localMaxMem).create();
    attr.setPartitionAttributes(prAttr);
    return attr.create();
  }

  /** this function creates vms in given host */
  private void createVMs(Host host)
  {
    for (int i = 0; i < 4; i++) {
      vm[i] = host.getVM(i);
    }
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
}
