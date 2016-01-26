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
package com.gemstone.gemfire.internal.cache.partitioned.fixed;

import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.DuplicatePrimaryPartitionException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.partition.PartitionNotAvailableException;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.ExpectedException;

/**
 * This Dunit test class have multiple tests to tests different validations of
 * static partitioning
 * 
 * @author kbachhav
 * 
 */

public class FixedPartitioningDUnitTest extends FixedPartitioningTestBase {

  public FixedPartitioningDUnitTest(String name) {
    super(name);
  }

  private static final long serialVersionUID = 1L;

  public void setUp() throws Exception {
    super.setUp();
    final Host host = Host.getHost(0);
    member1 = host.getVM(0);
    member2 = host.getVM(1);
    member3 = host.getVM(2);
    member4 = host.getVM(3);

  }

  public void tearDown2() throws Exception {
    super.tearDown2();
  }

  /**
   * This test validates that null partition name cannot be added in
   * FixedPartitionAttributes
   */
  public void testNullPartitionName() {
    try {
      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      FixedPartitionAttributes fpa = FixedPartitionAttributes
          .createFixedPartition(null, true, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa);
      member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 0, 40, 3,
              new QuarterPartitionResolver(), null, false});
      fail("IllegalStateException Expected");
    }
    catch (Exception illegal) {
      if (!((illegal.getCause() instanceof IllegalStateException) && (illegal
          .getCause().getMessage()
          .contains("Fixed partition name cannot be null")))) {
        fail("Expected IllegalStateException ", illegal);
      }
    }
  }

  /**
   * This tests validate that same partition name cannot be added more than once
   * as primary as well as secondary on same member.
   * 
   */

  public void testSamePartitionNameTwice() {
    try {
      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition(Quarter1, true, 3);
      FixedPartitionAttributes fpa2 = FixedPartitionAttributes
          .createFixedPartition(Quarter1, false, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 0, 40, 3,
              new QuarterPartitionResolver(), null, false });
      fail("IllegalStateException Expected");
    }
    catch (Exception illegal) {
      if (!((illegal.getCause() instanceof IllegalStateException) && (illegal
          .getCause().getMessage()
          .contains("can be added only once in FixedPartitionAttributes")))) {
        fail("Expected IllegalStateException ", illegal);
      }
    }
  }

  /**
   * This test validates that FixedPartitionAttributes cannot be defined for
   * accessor nodes
   */
  public void testFixedPartitionAttributes_Accessor() {
    try {
      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition(Quarter1, true, 3);
      FixedPartitionAttributes fpa2 = FixedPartitionAttributes
          .createFixedPartition(Quarter2, false, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 0, 0, 3,
              new QuarterPartitionResolver(), null, false});
      fail("IllegalStateException Expected");
    }
    catch (Exception illegal) {
      if (!((illegal.getCause() instanceof IllegalStateException) && (illegal
          .getCause().getMessage().contains("can not be defined for accessor")))) {
        fail("Expected IllegalStateException ", illegal);
      }
    }
  }

  /**
   * Test validation : only one node should return primary for a particular
   * partition name for a specific FPR at any given point of time.
   * DuplicatePrimaryPartitionException is thrown during FPR creation if this
   * condition is not met.
   */

  public void testSamePartitionName_Primary_OnTwoMembers() {
    ExpectedException ex = addExpectedException("DuplicatePrimaryPartitionException");
    try {
      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition(Quarter1, true, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 0, 40, 9,
              new QuarterPartitionResolver(), null, false});

      member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 0, 40, 9,
              new QuarterPartitionResolver(), null, false});

      member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 0, 40, 9,
              new QuarterPartitionResolver(), null, false});
      fail("DuplicatePrimaryPartitionException Expected");
    }
    catch (Exception duplicate) {
      if (!((duplicate.getCause() instanceof DuplicatePrimaryPartitionException) && (duplicate
          .getCause().getMessage()
          .contains("can not be defined as primary on more than one node")))) {
        fail("Expected DuplicatePrimaryPartitionException ", duplicate);
      }
    } finally {
      ex.remove();
    }
  }

  /**
   * Test validation : if same partition is having different num-buckets across
   * the nodes then illegalStateException will be thrown
   */

  public void testSamePartitionName_DifferentNumBuckets() {
    ExpectedException ex = addExpectedException("IllegalStateException");
    try {
      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition(Quarter1, true, 3);
      FixedPartitionAttributes fpa2 = FixedPartitionAttributes
          .createFixedPartition(Quarter2, false, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 1, 40, 9,
              new QuarterPartitionResolver(), null, false});

      member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
      fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 8);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 1, 40, 9,
              new QuarterPartitionResolver(), null, false});
      fail("IllegalStateException Expected");
    }
    catch (Exception illegal) {
      if (!((illegal.getCause() instanceof IllegalStateException) && (illegal
          .getCause().getMessage().contains("num-buckets are not same")))) {
        fail("Expected IllegalStateException ", illegal);
      }
    } finally {
      ex.remove();
    }
  }

  /**
   * Number of primary partitions (which should be one for a partition) and
   * secondary partitions of a FPR for a partition should never exceed number of
   * redundant copies + 1. IllegalStateException is thrown during FPR creation
   * if this condition is not met.
   */

  public void testNumberOfPartitions() {
    ExpectedException expected = addExpectedException("IllegalStateException");
    try {
      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", null, 1, 0, 9,
              new QuarterPartitionResolver(), null, false});
      
      member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition("Q11", true, 3);
      FixedPartitionAttributes fpa2 = FixedPartitionAttributes
          .createFixedPartition("Q12", false, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 1, 40, 9,
              new QuarterPartitionResolver(), null, false});

      member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      fpa1 = FixedPartitionAttributes.createFixedPartition("Q12", true, 3);
      fpa2 = FixedPartitionAttributes.createFixedPartition("Q13", false, 3);
      FixedPartitionAttributes fpa3 = FixedPartitionAttributes
          .createFixedPartition("Q11", false, 3);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 1, 40, 9,
              new QuarterPartitionResolver(), null, false});

      member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      fpa1 = FixedPartitionAttributes.createFixedPartition("Q13", true, 3);
      fpa2 = FixedPartitionAttributes.createFixedPartition("Q11", false, 3);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 1, 40, 9,
              new QuarterPartitionResolver(), null, false});
      fail("IllegalStateException expected");
    }
    catch (Exception ex) {
      if (!((ex.getCause() instanceof IllegalStateException) && (ex.getCause()
          .getMessage()
          .contains("should never exceed number of redundant copies")))) {
        fail("Expected IllegalStateException ", ex);
      }
    } finally {
      expected.remove();
    }
  }

  /**
   * Sum of num-buckets for different primary partitions should not be
   * greater than totalNumBuckets.
   */

  public void testNumBuckets_totalNumBuckets() {
    ExpectedException expected = addExpectedException("IllegalStateException");
    try {
      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", null, 1, 0, 5,
              new QuarterPartitionResolver(), null, false});
      
      member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition(Quarter1, true, 3);
      FixedPartitionAttributes fpa2 = FixedPartitionAttributes
          .createFixedPartition(Quarter2, false, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 1, 40, 5,
              new QuarterPartitionResolver(), null, false});

      member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
      fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 1, 40, 5,
              new QuarterPartitionResolver(), null, false});

      fail("IllegalStateException expected");
    }
    catch (Exception ex) {
      if (!((ex.getCause() instanceof IllegalStateException) && (ex.getCause()
          .getMessage()
          .contains("for different primary partitions should not be greater than total-num-buckets ")))) {
        fail("Expected IllegalStateException ", ex);
      }
    } finally {
      expected.remove();
    }
    
  }

  /**
   * This test validates that if the required partition is not available at the
   * time of entry operation then PartitionNotAvailabelException is thrown
   */
  public void testPut_PartitionNotAvailableException() {
    try {
      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", null, 1, 0, 12,
              new QuarterPartitionResolver(), null, false});

      member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition(Quarter1, true, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 1, 40, 12,
              new QuarterPartitionResolver(), null, false});

      member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 1, 40, 12,
              new QuarterPartitionResolver(), null, false});

      member1.invoke(FixedPartitioningTestBase.class, "putThorughAccessor",
          new Object[] { "Quarter" });
      fail("PartitionNotAvailableException Expected");
    }
    catch (Exception ex) {
      if (!((ex.getCause() instanceof PartitionNotAvailableException))) {
        fail("Expected PartitionNotAvailableException ", ex);
      }
    }
  }
  
  /**
   * This test validates that if one datastore has the fixed partition
   * attributes defined then other datastore should also have the fixed
   * partition attributes defined
   */
  
  public void test_DataStoreWithoutPartition_DataStoreWithPartition() {
    ExpectedException expected = addExpectedException("IllegalStateException");
    try {
      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", null, 1, 40, 12,
              new QuarterPartitionResolver(), null, false});

      member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition(Quarter1, true, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 1, 40, 12,
              new QuarterPartitionResolver(), null, false});
    }
    catch (Exception ex) {
      if (!((ex.getCause() instanceof IllegalStateException))) {
        fail("Expected IllegalStateException ", ex);
      }
    } finally {
      expected.remove();
    }
  }
  
  /**
   * This test validates that if one datastore has the fixed partition
   * attributes defined then other datastore should also have the fixed
   * partition attributes defined
   */

  public void test_DataStoreWithPartition_DataStoreWithoutPartition() {
    ExpectedException expected = addExpectedException("IllegalStateException");
    try {
      member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition(Quarter1, true, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", fpaList, 1, 40, 12,
              new QuarterPartitionResolver(), null, false});

      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
          new Object[] { "Quarter", null, 1, 40, 12,
              new QuarterPartitionResolver(), null, false});
    }
    catch (Exception ex) {
      if (!((ex.getCause() instanceof IllegalStateException))) {
        fail("Expected IllegalStateException ", ex);
      }
    } finally {
      expected.remove();
    }
  }
  
  /**
   * This tests validate that accessor member does the put on datastores as per
   * primary FixedPartitionAttributes defined on datastores
   */
  public void testPut_ValidateDataOnMember_OnlyPrimary_Accessor() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", null, 0, 0, 12,
            new QuarterPartitionResolver(), null, false});

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
            new QuarterPartitionResolver(), null, false});

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
            new QuarterPartitionResolver(), null, false});

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
            new QuarterPartitionResolver(), null, false});

    member1.invoke(FixedPartitioningTestBase.class, "putThorughAccessor",
        new Object[] { "Quarter" });

    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryData",
        new Object[] { Quarter1 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryData",
        new Object[] { Quarter2 });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimaryData",
    new Object[] { Quarter3 });
    
  }

  
  public void testBug43283() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    
    member1.invoke(FixedPartitioningTestBase.class, "setPRObserverBeforeCalculateStartingBucketId");
    member2.invoke(FixedPartitioningTestBase.class, "setPRObserverBeforeCalculateStartingBucketId");
    member3.invoke(FixedPartitioningTestBase.class, "setPRObserverBeforeCalculateStartingBucketId");
    member4.invoke(FixedPartitioningTestBase.class, "setPRObserverBeforeCalculateStartingBucketId");
    try {

      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition(Quarter1, true, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      AsyncInvocation inv1 = member1
          .invokeAsync(FixedPartitioningTestBase.class,
              "createRegionWithPartitionAttributes", new Object[] { "Quarter",
                  fpaList, 0, 40, 12, new QuarterPartitionResolver(), null,
                  false });

      FixedPartitionAttributes fpa2 = FixedPartitionAttributes
          .createFixedPartition(Quarter2, true, 3);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa2);
      AsyncInvocation inv2 = member2
          .invokeAsync(FixedPartitioningTestBase.class,
              "createRegionWithPartitionAttributes", new Object[] { "Quarter",
                  fpaList, 0, 40, 12, new QuarterPartitionResolver(), null,
                  false });

      FixedPartitionAttributes fpa3 = FixedPartitionAttributes
          .createFixedPartition(Quarter3, true, 3);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa3);
      AsyncInvocation inv3 = member3
          .invokeAsync(FixedPartitioningTestBase.class,
              "createRegionWithPartitionAttributes", new Object[] { "Quarter",
                  fpaList, 0, 40, 12, new QuarterPartitionResolver(), null,
                  false });
      
      member4.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Quarter",
              null, 0, 0, 12, new QuarterPartitionResolver(), null, false });
      try {
        member4.invoke(FixedPartitioningTestBase.class,
            "putThorughAccessor_Immediate", new Object[] { "Quarter" });
      }
      catch (Exception e) {
        e.printStackTrace();
        if (!(e.getCause() instanceof PartitionNotAvailableException)) {
          fail("exception thrown is not PartitionNotAvailableException");
        }
      }
      try {
        inv1.join();
        inv2.join();
        inv3.join();
      }
      catch (InterruptedException e) {
        e.printStackTrace();
        fail("Unexpected Exception");
      }
    } finally {
      member1.invoke(FixedPartitioningTestBase.class, "resetPRObserverBeforeCalculateStartingBucketId");
      member2.invoke(FixedPartitioningTestBase.class, "resetPRObserverBeforeCalculateStartingBucketId");
      member3.invoke(FixedPartitioningTestBase.class, "resetPRObserverBeforeCalculateStartingBucketId");
      member4.invoke(FixedPartitioningTestBase.class, "resetPRObserverBeforeCalculateStartingBucketId");
    }
    
  }
  /**
   * This tests validate that datastore member does the put on itself as well as
   * other datastores as per primary FixedPartitionAttributes defined on
   * datastores.
   */
  
  public void testPut_ValidateDataOnMember_OnlyPrimary_Datastore() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
            new QuarterPartitionResolver(), null, false});

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
            new QuarterPartitionResolver(), null, false});

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
            new QuarterPartitionResolver(), null, false});

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
            new QuarterPartitionResolver(), null, false});

    member1.invoke(FixedPartitioningTestBase.class, "putThroughDataStore",
        new Object[] { "Quarter" });

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryData",
        new Object[] { Quarter1 });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryData",
        new Object[] { Quarter2 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryData",
        new Object[] { Quarter3 });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimaryData",
        new Object[] { Quarter4 });

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 3, 3 });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 3, 3 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 3, 3 });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 3, 3 });
  }
  
  /**
   * This test validate that a delete operation on empty region will throw
   * EntryNotFoundException
   */
  public void testDelete_WithoutPut() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      new QuarterPartitionResolver(), null, false });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      new QuarterPartitionResolver(), null, false });

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      new QuarterPartitionResolver(), null, false });

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      new QuarterPartitionResolver(), null, false });

    try {
      member1.invoke(FixedPartitioningTestBase.class, "deleteOperation",
          new Object[] { "Quarter" });
      fail("EntryNotFoundException expected");
    }
    catch (Exception ex) {
      if (!((ex.getCause() instanceof EntryNotFoundException))) {
        fail("Expected EntryNotFoundException ", ex);
      }
    }
    
    member1.invoke(FixedPartitioningTestBase.class, "putThroughDataStore",
        new Object[] { "Quarter" });
    
    member1.invoke(FixedPartitioningTestBase.class, "getThroughDataStore",
        new Object[] { "Quarter" });
    
  }
  
  /**
   * This tests validate that datastore member tries the put on itself as well as
   * other datastores as per primary FixedPartitionAttributes defined on
   * datastores. But No resolver is provided. So IllegalStateException in expected
   */
  
  public void testPut_NoResolver() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
            null, null , false});

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      null, null, false });

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      null, null, false });

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      null, null, false });

    try {
      member1.invoke(FixedPartitioningTestBase.class, "putThroughDataStore_NoResolver",
          new Object[] { "Quarter" });
      fail("IllegalStateException expected");
    }
    catch (Exception ex) {
      if (!((ex.getCause() instanceof IllegalStateException))) {
        fail("Expected IllegalStateException ", ex);
      }
    }
  }
  
  /**
   * This tests validate that datastore member tries the put with callback on itself as well as
   * other datastores as per primary FixedPartitionAttributes defined on
   * datastores. here CallBack implements FixedPartitionResolver.
   */
  
  public void testPut_CallBackWithResolver() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
            null, null, false });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      null, null, false });

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      null, null, false });

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      null, null, false });

    member1.invoke(FixedPartitioningTestBase.class, "putThroughDataStore_CallBackWithResolver",
        new Object[] { "Quarter" });
    
    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryData",
        new Object[] { Quarter1 });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryData",
        new Object[] { Quarter2 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryData",
        new Object[] { Quarter3 });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimaryData",
        new Object[] { Quarter4 });

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 3, 3 });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 3, 3 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 3, 3 });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 3, 3 });
  }
  
  /**
   * This test validates that a PR without Fixed Partition Attributes and with
   * FixedPartitionResolver will do custom partitioning as per resolver.
   */
  
  public void testPut_WithResolver_NoFPAs() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", null, 0, 40, 12,
      new QuarterPartitionResolver(), null, false});

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", null, 0, 40, 12,
      new QuarterPartitionResolver(), null, false});

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", null, 0, 40, 12,
      new QuarterPartitionResolver(), null, false});

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", null, 0, 40, 12,
      new QuarterPartitionResolver(), null, false});

    member1.invoke(FixedPartitioningTestBase.class, "putThroughDataStore",
        new Object[] { "Quarter" });
    
  }
  
  /**
   * This tests validate that datastore member tries the put on itself as well
   * as other datastores as per primary FixedPartitionAttributes defined on
   * datastores. Here No Resolver is provided through attributes. Some keys
   * implement FixedPartitionResolver and some does't implement any resolver.
   * IllegalStateException is expected.
   */
  
  
  public void testPut_FixedPartitionResolver_NoResolver() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
            null, null, false });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      null, null, false });

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      null, null, false });

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      null, null, false });

    try {
      member1.invoke(FixedPartitioningTestBase.class,
          "putThroughDataStore_FixedPartitionResolver_NoResolver",
          new Object[] { "Quarter" });
      fail("IllegalStateException expected");
    }
    catch (Exception ex) {
      if (!((ex.getCause() instanceof IllegalStateException))) {
        fail("Expected IllegalStateException ", ex);
      }
    }
  }
  
  /**
   * This tests validate that datastore member tries the put on itself as well
   * as other datastores as per primary FixedPartitionAttributes defined on
   * datastores. Here No Resolver is provided through attributes. Some keys
   * implement FixedPartitionResolver and some implements PartitionResolver.
   * IllegalStateException is expected.
   */
  
  public void testPut_FixedPartitionResolver_PartitionResolver() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
            null, null, false });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      null, null, false });

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      null, null, false });

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      null, null, false });

    try {
      member1.invoke(FixedPartitioningTestBase.class,
          "putThroughDataStore_FixedPartitionResolver_PartitionResolver",
          new Object[] { "Quarter" });
      fail("IllegalStateException expected");
    }
    catch (Exception ex) {
      if (!((ex.getCause() instanceof IllegalStateException))) {
        fail("Expected IllegalStateException ", ex);
      }
    }
  }
  
  /**
   * This tests validate that datastore member tries the put on itself as well
   * as other datastores as per primary FixedPartitionAttributes defined on
   * datastores with only one bucket per partition.  
   */

  public void testFPR_DefaultNumBuckets() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      new QuarterPartitionResolver(), null, false });

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      new QuarterPartitionResolver(), null, false });

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 0, 40, 12,
      new QuarterPartitionResolver(), null, false });
    
    member1.invoke(FixedPartitioningTestBase.class, "putThroughDataStore",
        new Object[] { "Quarter" });

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryData",
        new Object[] { Quarter1 });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryData",
        new Object[] { Quarter2 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryData",
        new Object[] { Quarter3 });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimaryData",
        new Object[] { Quarter4 });

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 1, 1 });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 1, 1 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 1, 1 });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 1, 1 });
  }
  /**
   * This tests validate that accessor member does the put on datastores as per
   * primary and secondary FixedPartitionAttributes defined on datastores.
   */
  
  public void testPut_ValidateDataOnMember_PrimarySecondary_Accessor() {
    createCacheOnMember();
    createRegionWithPartitionAttributes("Quarter", null, 3, 0, 12,
        new QuarterPartitionResolver(), null, false);

    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    FixedPartitionAttributes fpa2 = FixedPartitionAttributes
        .createFixedPartition(Quarter3, false, 3);
    FixedPartitionAttributes fpa3 = FixedPartitionAttributes
        .createFixedPartition(Quarter4, false, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    fpaList.add(fpa3);
    member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 3, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpa3 = FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    fpaList.add(fpa3);
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 3, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpa3 = FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    fpaList.add(fpa3);
    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 3, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpa3 = FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    fpaList.add(fpa3);
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 3, 40, 12,
            new QuarterPartitionResolver(), null, false });

    putThroughDataStore("Quarter");

    member1.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData_TwoSecondaries", new Object[] { Quarter1,
            false });
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData_TwoSecondaries", new Object[] { Quarter2,
            false });
    member3.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData_TwoSecondaries", new Object[] { Quarter3,
            false });
    member4.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData_TwoSecondaries", new Object[] { Quarter4,
            false });
  }
  
  /**
   * This tests validate that datastore member does the put on itself as well as
   * other datastores as per primary and secondary FixedPartitionAttributes defined on
   * datastores.
   */
  
  public void testPut_ValidateDataOnMember_PrimarySecondary_Datastore() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    FixedPartitionAttributes fpa2 = FixedPartitionAttributes
        .createFixedPartition(Quarter2, false, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 1, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 1, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 1, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 1, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member1.invoke(FixedPartitioningTestBase.class, "putThroughDataStore",
        new Object[] { "Quarter" });

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter1, false });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter2, false });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter3, false });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter4, false });

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
  }

  /**
   * This tests validate that if only the secondary partitions are available
   * then put should happen successfully for these secondary partitions. These
   * secondary partitions should acts as primary. When the primary partition
   * joins the system then this new member should create the primary buckets for
   * this partition on itself. And Secondary partitions who were holding primary
   * buckets status should now act as secondary buckets.
   * 
   */
  
  public void testPut_ValidateDataOnMember_OnlySecondary_Datastore() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, false, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 3, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 3, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 3, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 3, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member1.invoke(FixedPartitioningTestBase.class, "putThroughDataStore",
        new Object[] { "Quarter" });

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 3, 3 });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 3, 3 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 3, 3 });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 3, 3 });

    createCacheOnMember();
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    createRegionWithPartitionAttributes("Quarter", fpaList, 3, 40, 12,
        new QuarterPartitionResolver(), null, false);

    pause(1000);

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 3, 0 });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 3, 3 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 3, 3 });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 3, 3 });
    checkPrimaryBucketsForQuarter(3, 3);

  }
  
  /**
   * Accessor =1 Datastore = 4
   *  Datastores        Primary               Secondary       
   * Member1 =       Q1(0,1,2)           Q3(3,4,5), Q4(6,7,8)
   * Member2 =       Q2(9,10,11)         Q3(3,4,5), Q4(6,7,8)
   * Member3 =       Q3(3,4,5)           Q1(0,1,2), Q2(9,10,11)
   * Member4 =       Q4(6,7,8)           Q1(0,1,2), Q2(9,10,11)
   * Put happens for all buckets
   * Member 4 goes down, then either member1 or member2 holds primary for member4
   * 
   *                   Primary                          Secondary       
   * Member1 =       Q1(0,1,2)                  Q3(3,4,5), Q4(6,7,8)
   * Member2 =       Q2(9,10,11), Q4(6,7,8)     Q3(3,4,5) 
   * Member3 =       Q3(3,4,5)                  Q1(0,1,2), Q2(9,10,11)
   * 
   * Put happens considering Member2 is holding primary for Q4.
   * 
   * Member4 comes again, then Memeber4 should do the GII from member2 for buckets 6,7,8 and should acqiure primary status
   * Member1 =       Q1(0,1,2)           Q3(3,4,5), Q4(6,7,8)
   * Member2 =       Q2(9,10,11)         Q3(3,4,5), Q4(6,7,8)
   * Member3 =       Q3(3,4,5)           Q1(0,1,2), Q2(9,10,11)
   * Member4 =       Q4(6,7,8)           Q1(0,1,2), Q2(9,10,11)
   *        
   * 
   */
  public void testPut_ValidateDataOnMember_PrimarySecondary_Accessor_CacheClosed() {
    createCacheOnMember();
    createRegionWithPartitionAttributes("Quarter", null, 3, 0, 12,
        new QuarterPartitionResolver(), null, false);

    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    FixedPartitionAttributes fpa2 = FixedPartitionAttributes
        .createFixedPartition(Quarter3, false, 3);
    FixedPartitionAttributes fpa3 = FixedPartitionAttributes
        .createFixedPartition(Quarter4, false, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    fpaList.add(fpa3);
    member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 3, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpa3 = FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    fpaList.add(fpa3);
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 3, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpa3 = FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    fpaList.add(fpa3);
    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 3, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpa3 = FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    fpaList.add(fpa3);
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 3, 40, 12,
            new QuarterPartitionResolver(), null, false });

    putThroughDataStore("Quarter");

    member1.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData_TwoSecondaries", new Object[] { Quarter1,
            false });
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData_TwoSecondaries", new Object[] { Quarter2,
            false });
    member3.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData_TwoSecondaries", new Object[] { Quarter3,
            false });
    member4.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData_TwoSecondaries", new Object[] { Quarter4,
            false });

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 9, 3 });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 9, 3 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 9, 3 });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 9, 3 });

    member4.invoke(FixedPartitioningTestBase.class, "closeCache");
    pause(1000);

    member1.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData_TwoSecondaries", new Object[] { Quarter1,
            false });
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData_TwoSecondaries", new Object[] { Quarter2,
            false });
    member3.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData_TwoSecondaries", new Object[] { Quarter3,
            false });

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarterAfterCacheClosed",
        new Object[] { 9, 3 });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarterAfterCacheClosed",
        new Object[] { 9, 3 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarterAfterCacheClosed",
        new Object[] { 9, 3 });
  }
  
  /**
   * Datastore = 4
   *  Datastores        Primary               Secondary       
   * Member1 =       Q1(0,1,2)              Q2(3,4,5)
   * Member2 =       Q2(3,4,5)              Q3(6,7,8)
   * Member3 =       Q3(6,7,8)              Q4(9,10,11)
   * Member4 =       Q4(9,10,11)            Q1(0,1,2)
   * Put happens for all buckets
   * Member 4 goes down, then either member1 or member2 holds primary for member4
   * 
   *                   Primary                          Secondary       
   * Member1 =       Q1(0,1,2)                  Q2(3,4,5)
   * Member2 =       Q2(3,4,5)                  Q3(6,7,8)  
   * Member3 =       Q3(6,7,8), Q4(9,10,11)                  
   * 
   * Put happens considering Member3 is holding primary for Q4.
   * 
   * Member4 comes again, then Memeber4 should do the GII from member2 for buckets 6,7,8 and should acqiure primary status
   *  Datastores        Primary               Secondary       
   * Member1 =       Q1(0,1,2)              Q2(3,4,5)
   * Member2 =       Q2(3,4,5)              Q3(6,7,8)
   * Member3 =       Q3(6,7,8)              Q4(9,10,11)
   * Member4 =       Q4(9,10,11)            Q1(0,1,2)
   */
  
  public void testPut_ValidateDataOnMember_PrimarySecondary_Datastore_CacheClosed() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    FixedPartitionAttributes fpa2 = FixedPartitionAttributes
        .createFixedPartition(Quarter2, false, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 1, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 1, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 1, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 1, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member1.invoke(FixedPartitioningTestBase.class, "putThroughDataStore",
        new Object[] { "Quarter" });

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter1, false });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter2, false });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter3, false });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter4, false });

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });

    member4.invoke(FixedPartitioningTestBase.class, "closeCache");
    pause(1000);

    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarterAfterCacheClosed",
        new Object[] { 6, 6 });

    member1.invoke(FixedPartitioningTestBase.class, "putHAData", new Object[] { "Quarter" });

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
        
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 1, 40, 12,
            new QuarterPartitionResolver(), null, false });

    pause(1000);

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter1, true });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter2, true });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter3, true });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter4, true });

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
  }
  
  public void test_Bug46619_Put_ValidateDataOnMember_PrimarySecondary_Datastore_CacheClosed() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember_DisableMovePrimary");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    FixedPartitionAttributes fpa2 = FixedPartitionAttributes
        .createFixedPartition(Quarter2, false, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 1, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember_DisableMovePrimary");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 1, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember_DisableMovePrimary");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 1, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember_DisableMovePrimary");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 1, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member1.invoke(FixedPartitioningTestBase.class, "putThroughDataStore",
        new Object[] { "Quarter" });

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter1, false });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter2, false });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter3, false });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter4, false });

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });

    member4.invoke(FixedPartitioningTestBase.class, "closeCache");
    member2.invoke(FixedPartitioningTestBase.class, "closeCache");
    pause(1000);

    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarterAfterCacheClosed",
        new Object[] { 6, 6 });
    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarterAfterCacheClosed",
        new Object[] { 6, 6 });
    
    member1.invoke(FixedPartitioningTestBase.class, "putHAData", new Object[] { "Quarter" });

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember_DisableMovePrimary");
        
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member4.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 1, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember_DisableMovePrimary");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 1, 40, 12,
            new QuarterPartitionResolver(), null, false });
    pause(1000);

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter1, true });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter2, true });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter3, true });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimarySecondaryData",
        new Object[] { Quarter4, true });

    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 6 });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 0 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 6 });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 0 });
    
    member4.invoke(FixedPartitioningTestBase.class, "doRebalance");
    
    pause(2000);
    member1.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
    member2.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
    member3.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
    member4.invoke(FixedPartitioningTestBase.class, "checkPrimaryBucketsForQuarter",
        new Object[] { 6, 3 });
  }
  
  /**
   * Datastore = 4
   * Datastores        Primary               Secondary       
   * Member1 =       Q1(0,1,2)               Q2(3,4,5),Q3(6,7,8),Q4(9,10,11)
   * Member2 =       Q3(6,7,8)               Q1(0,1,2),  Q2(3,4,5),Q4(9,10,11)
   * Member3 =       Q2(3,4,5),Q4(9,10,11)   Q1(0,1,2),  Q3(6,7,8)
   *  
   * Put happens for all buckets
   * 
   * Member 3 goes down, then either member1 or member2 holds primary for member4
   * 
   *                   Primary                          Secondary       
   * Member1 =       Q1(0,1,2),Q2(3,4,5)             Q3(6,7,8), Q4(9,10,11)
   * Member2 =       Q3(6,7,8),Q4(9,10,11)               Q1(0,1,2), Q2(3,4,5)
   * 
   * Member 3 comes again then it should be same as it was before member 3 went down
   * 
   * Member1 =       Q1(0,1,2)               Q2(3,4,5),Q3(6,7,8),Q4(9,10,11)
   * Member2 =       Q3(6,7,8)               Q1(0,1,2),  Q2(3,4,5),Q4(9,10,11)
   * Member3 =       Q2(3,4,5),Q4(9,10,11)   Q1(0,1,2),  Q3(6,7,8)
   * 
   */
  
  public void testPut_ValidateDataOnMember_MultiplePrimaries_Datastore_CacheClosed() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3));

    member1.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 2, 40, 12,
            new QuarterPartitionResolver(), null, false });

     member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3));    
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3));
    
    member2.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 2, 40, 12,
            new QuarterPartitionResolver(), null, false });

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3));    
    

    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 2, 40, 12,
            new QuarterPartitionResolver(), null, false });


    member1.invoke(FixedPartitioningTestBase.class, "putThroughDataStore",
        new Object[] { "Quarter" });
    member1.invoke(FixedPartitioningTestBase.class, "checkStartingBucketIDs");
    member2.invoke(FixedPartitioningTestBase.class, "checkStartingBucketIDs");
    member3.invoke(FixedPartitioningTestBase.class, "checkStartingBucketIDs");

    member3.invoke(FixedPartitioningTestBase.class, "closeCache");
    
    pause(1000);  
    
    member1.invoke(FixedPartitioningTestBase.class, "checkStartingBucketIDs_Nodedown");
    member2.invoke(FixedPartitioningTestBase.class, "checkStartingBucketIDs_Nodedown");
    
    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");    

    member3.invoke(FixedPartitioningTestBase.class, "createRegionWithPartitionAttributes",
        new Object[] { "Quarter", fpaList, 2, 40, 12,
            new QuarterPartitionResolver(), null, false });
    
    pause(3000);
    
    member1.invoke(FixedPartitioningTestBase.class, "checkStartingBucketIDs_Nodeup");
    member2.invoke(FixedPartitioningTestBase.class, "checkStartingBucketIDs_Nodeup");
    member3.invoke(FixedPartitioningTestBase.class, "checkStartingBucketIDs_Nodeup");

    
  }
  
}
