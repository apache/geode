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
package org.apache.geode.internal.cache.partitioned.fixed;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DuplicatePrimaryPartitionException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.PartitionedRegionStorageException;
import org.apache.geode.cache.partition.PartitionNotAvailableException;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.PartitioningTest;

/**
 * This Dunit test class have multiple tests to tests different validations of static partitioning
 */
@Category({PartitioningTest.class})
public class FixedPartitioningDUnitTest extends FixedPartitioningTestBase {

  public static final String REGION_NAME = "Quarter";

  public FixedPartitioningDUnitTest() {
    super();
  }

  private static final long serialVersionUID = 1L;

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    member1 = host.getVM(0);
    member2 = host.getVM(1);
    member3 = host.getVM(2);
    member4 = host.getVM(3);
  }

  /**
   * This test validates that null partition name cannot be added in FixedPartitionAttributes
   */
  @Test
  public void testNullPartitionName() {
    try {
      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      FixedPartitionAttributes fpa = FixedPartitionAttributes.createFixedPartition(null, true, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa);
      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(
          REGION_NAME,
          fpaList, 0, 40, 3, new QuarterPartitionResolver(), null, false));
      fail("IllegalStateException Expected");
    } catch (Exception illegal) {
      if (!((illegal.getCause() instanceof IllegalStateException)
          && (illegal.getCause().getMessage().contains("Fixed partition name cannot be null")))) {
        Assert.fail("Expected IllegalStateException ", illegal);
      }
    }
  }

  /**
   * This tests validate that same partition name cannot be added more than once as primary as well
   * as secondary on same member.
   *
   */

  @Test
  public void testSamePartitionNameTwice() {
    try {
      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      FixedPartitionAttributes fpa1 =
          FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
      FixedPartitionAttributes fpa2 =
          FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member1
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              fpaList, 0, 40, 3, new QuarterPartitionResolver(), null, false));
      fail("IllegalStateException Expected");
    } catch (Exception illegal) {
      if (!((illegal.getCause() instanceof IllegalStateException) && (illegal.getCause()
          .getMessage().contains("can be added only once in FixedPartitionAttributes")))) {
        Assert.fail("Expected IllegalStateException ", illegal);
      }
    }
  }

  /**
   * This test validates that FixedPartitionAttributes cannot be defined for accessor nodes
   */
  @Test
  public void testFixedPartitionAttributes_Accessor() {
    try {
      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      FixedPartitionAttributes fpa1 =
          FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
      FixedPartitionAttributes fpa2 =
          FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member1
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              fpaList, 0, 0, 3, new QuarterPartitionResolver(), null, false));
      fail("IllegalStateException Expected");
    } catch (Exception illegal) {
      if (!((illegal.getCause() instanceof IllegalStateException)
          && (illegal.getCause().getMessage().contains("can not be defined for accessor")))) {
        Assert.fail("Expected IllegalStateException ", illegal);
      }
    }
  }

  /**
   * Test validation : only one node should return primary for a particular partition name for a
   * specific FPR at any given point of time. DuplicatePrimaryPartitionException is thrown during
   * FPR creation if this condition is not met.
   */

  @Test
  public void testSamePartitionName_Primary_OnTwoMembers() {
    IgnoredException ex =
        IgnoredException.addIgnoredException("DuplicatePrimaryPartitionException");
    try {
      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      FixedPartitionAttributes fpa1 =
          FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      member1
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              fpaList, 0, 40, 9, new QuarterPartitionResolver(), null, false));

      member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
      fpaList.clear();
      fpaList.add(fpa1);
      member2
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              fpaList, 0, 40, 9, new QuarterPartitionResolver(), null, false));

      member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
      fpaList.clear();
      fpaList.add(fpa1);
      member3
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              fpaList, 0, 40, 9, new QuarterPartitionResolver(), null, false));
      fail("DuplicatePrimaryPartitionException Expected");
    } catch (Exception duplicate) {
      if (!((duplicate.getCause() instanceof DuplicatePrimaryPartitionException)
          && (duplicate.getCause().getMessage()
              .contains("can not be defined as primary on more than one node")))) {
        Assert.fail("Expected DuplicatePrimaryPartitionException ", duplicate);
      }
    } finally {
      ex.remove();
    }
  }

  /**
   * Test validation : if same partition is having different num-buckets across the nodes then
   * illegalStateException will be thrown
   */

  @Test
  public void testSamePartitionName_DifferentNumBuckets() {
    IgnoredException ex = IgnoredException.addIgnoredException("IllegalStateException");
    try {
      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      FixedPartitionAttributes fpa1 =
          FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
      FixedPartitionAttributes fpa2 =
          FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member1
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              fpaList, 1, 40, 9, new QuarterPartitionResolver(), null, false));

      member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
      fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 8);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member2
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              fpaList, 1, 40, 9, new QuarterPartitionResolver(), null, false));
      fail("IllegalStateException Expected");
    } catch (Exception illegal) {
      if (!((illegal.getCause() instanceof IllegalStateException)
          && (illegal.getCause().getMessage().contains("num-buckets are not same")))) {
        Assert.fail("Expected IllegalStateException ", illegal);
      }
    } finally {
      ex.remove();
    }
  }

  /**
   * Number of primary partitions (which should be one for a partition) and secondary partitions of
   * a FPR for a partition should never exceed number of redundant copies + 1. IllegalStateException
   * is thrown during FPR creation if this condition is not met.
   */

  @Test
  public void testNumberOfPartitions() {
    IgnoredException expected = IgnoredException.addIgnoredException("IllegalStateException");
    try {
      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member1
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              null, 1, 0, 9, new QuarterPartitionResolver(), null, false));

      member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      FixedPartitionAttributes fpa1 = FixedPartitionAttributes.createFixedPartition("Q11", true, 3);
      FixedPartitionAttributes fpa2 =
          FixedPartitionAttributes.createFixedPartition("Q12", false, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member2
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              fpaList, 1, 40, 9, new QuarterPartitionResolver(), null, false));

      member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      fpa1 = FixedPartitionAttributes.createFixedPartition("Q12", true, 3);
      fpa2 = FixedPartitionAttributes.createFixedPartition("Q13", false, 3);
      FixedPartitionAttributes fpa3 =
          FixedPartitionAttributes.createFixedPartition("Q11", false, 3);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member3
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              fpaList, 1, 40, 9, new QuarterPartitionResolver(), null, false));

      member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      fpa1 = FixedPartitionAttributes.createFixedPartition("Q13", true, 3);
      fpa2 = FixedPartitionAttributes.createFixedPartition("Q11", false, 3);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member4
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              fpaList, 1, 40, 9, new QuarterPartitionResolver(), null, false));
      fail("IllegalStateException expected");
    } catch (Exception ex) {
      if (!((ex.getCause() instanceof IllegalStateException) && (ex.getCause().getMessage()
          .contains("should never exceed number of redundant copies")))) {
        Assert.fail("Expected IllegalStateException ", ex);
      }
    } finally {
      expected.remove();
    }
  }

  /**
   * Sum of num-buckets for different primary partitions should not be greater than totalNumBuckets.
   */

  @Test
  public void testNumBuckets_totalNumBuckets() {
    IgnoredException expected = IgnoredException.addIgnoredException("IllegalStateException");
    try {
      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member1
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              null, 1, 0, 5, new QuarterPartitionResolver(), null, false));

      member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      FixedPartitionAttributes fpa1 =
          FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
      FixedPartitionAttributes fpa2 =
          FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member2
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              fpaList, 1, 40, 5, new QuarterPartitionResolver(), null, false));

      member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
      fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member3
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              fpaList, 1, 40, 5, new QuarterPartitionResolver(), null, false));

      fail("IllegalStateException expected");
    } catch (Exception ex) {
      if (!((ex.getCause() instanceof IllegalStateException)
          && (ex.getCause().getMessage().contains(
              "for different primary partitions should not be greater than total-num-buckets ")))) {
        Assert.fail("Expected IllegalStateException ", ex);
      }
    } finally {
      expected.remove();
    }

  }

  /**
   * This test validates that if the required partition is not available at the time of entry
   * operation then PartitionNotAvailabelException is thrown
   */
  @Test
  public void testPut_PartitionNotAvailableException() {
    try {
      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member1
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              null, 1, 0, 12, new QuarterPartitionResolver(), null, false));

      member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      FixedPartitionAttributes fpa1 =
          FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      member2
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

      member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
      fpaList.clear();
      fpaList.add(fpa1);
      member3
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

      member1.invoke(() -> FixedPartitioningTestBase.putThroughAccessor(REGION_NAME));
      fail("PartitionNotAvailableException Expected");
    } catch (Exception ex) {
      if (!((ex.getCause() instanceof PartitionNotAvailableException))) {
        Assert.fail("Expected PartitionNotAvailableException ", ex);
      }
    }
  }

  /**
   * This test validates that if one datastore has the fixed partition attributes defined then other
   * datastore should also have the fixed partition attributes defined
   */

  @Test
  public void test_DataStoreWithoutPartition_DataStoreWithPartition() {
    IgnoredException expected = IgnoredException.addIgnoredException("IllegalStateException");
    try {
      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member1
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              null, 1, 40, 12, new QuarterPartitionResolver(), null, false));

      member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      FixedPartitionAttributes fpa1 =
          FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      member2
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));
    } catch (Exception ex) {
      if (!((ex.getCause() instanceof IllegalStateException))) {
        Assert.fail("Expected IllegalStateException ", ex);
      }
    } finally {
      expected.remove();
    }
  }

  /**
   * This test validates that if one datastore has the fixed partition attributes defined then other
   * datastore should also have the fixed partition attributes defined
   */

  @Test
  public void test_DataStoreWithPartition_DataStoreWithoutPartition() {
    IgnoredException expected = IgnoredException.addIgnoredException("IllegalStateException");
    try {
      member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      FixedPartitionAttributes fpa1 =
          FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      member2
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member1
          .invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
              null, 1, 40, 12, new QuarterPartitionResolver(), null, false));
    } catch (Exception ex) {
      if (!((ex.getCause() instanceof IllegalStateException))) {
        Assert.fail("Expected IllegalStateException ", ex);
      }
    } finally {
      expected.remove();
    }
  }

  /**
   * This tests validate that accessor member does the put on datastores as per primary
   * FixedPartitionAttributes defined on datastores
   */
  @Test
  public void testPut_ValidateDataOnMember_OnlyPrimary_Accessor() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
        null, 0, 0, 12, new QuarterPartitionResolver(), null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 =
        FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
        fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
        fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
        fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member1.invoke(() -> FixedPartitioningTestBase.putThroughAccessor(REGION_NAME));

    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryData(Quarter1));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryData(Quarter2));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryData(Quarter3));

  }

  @Test
  public void putWhileDataStoresAreBeingCreatedFails() throws Exception {

    VM accessor = VM.getVM(0);
    VM datastore1 = VM.getVM(1);
    VM datastore2 = VM.getVM(2);
    VM datastore3 = VM.getVM(3);
    accessor.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    datastore1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    datastore2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    datastore3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());

    getBlackboard().initBlackboard();

    // Create an accessor for the PR
    accessor.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes(REGION_NAME,
        null, 0, 0, 12, new QuarterPartitionResolver(), null, false));

    // Set an observer in datastore 1 that will wait in the middle of PR creation
    datastore1
        .invoke(() -> FixedPartitioningTestBase.setPRObserverBeforeCalculateStartingBucketId());
    try {
      // Start datstore1 asynchronously. This will get stuck waiting in the PR observer
      AsyncInvocation inv1 = datastore1.invokeAsync(() -> createRegionWithQuarter(Quarter1));

      // Make sure datastore 1 gets stuck waiting on the observer
      getBlackboard().waitForGate("waiting", 1, TimeUnit.MINUTES);

      // Start two more data stores asynchronously. These will get stuck waiting for a dlock behind
      // datatstore1
      AsyncInvocation inv2 = datastore2.invokeAsync(() -> createRegionWithQuarter(Quarter2));
      AsyncInvocation inv3 = datastore3.invokeAsync(() -> createRegionWithQuarter(Quarter3));


      // Make sure a put to datastore1, which is in the middle of pr initialization, fails correctly
      Assertions.assertThatThrownBy(() -> accessor
          .invoke(() -> FixedPartitioningTestBase.putForQuarter(REGION_NAME, Quarter1)))
          .hasCauseInstanceOf(PartitionedRegionStorageException.class);

      // Make sure a put to datastore2, which stuck behind datastore1 in initialization, fails
      // correctly
      Assertions.assertThatThrownBy(() -> accessor
          .invoke(() -> FixedPartitioningTestBase.putForQuarter(REGION_NAME, Quarter2)))
          .hasCauseInstanceOf(PartitionedRegionStorageException.class);

      getBlackboard().signalGate("done");

      inv1.get();
      inv2.get();
      inv3.get();
    } finally {
      datastore1
          .invoke(() -> FixedPartitioningTestBase.resetPRObserverBeforeCalculateStartingBucketId());
    }

  }

  private void createRegionWithQuarter(String quarter) {
    FixedPartitionAttributes partition =
        FixedPartitionAttributes.createFixedPartition(quarter, true, 3);
    FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        Arrays.asList(partition), 0,
        40, 12, new QuarterPartitionResolver(), null, false);
  }

  /**
   * This tests validate that datastore member does the put on itself as well as other datastores as
   * per primary FixedPartitionAttributes defined on datastores.
   */

  @Test
  public void testPut_ValidateDataOnMember_OnlyPrimary_Datastore() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 =
        FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member1.invoke(() -> FixedPartitioningTestBase.putThroughDataStore("Quarter"));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryData(Quarter1));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryData(Quarter2));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryData(Quarter3));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryData(Quarter4));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(3, 3));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(3, 3));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(3, 3));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(3, 3));
  }

  /**
   * This test validate that a delete operation on empty region will throw EntryNotFoundException
   */
  @Test
  public void testDelete_WithoutPut() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 =
        FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    try {
      member1.invoke(() -> FixedPartitioningTestBase.deleteOperation("Quarter"));
      fail("EntryNotFoundException expected");
    } catch (Exception ex) {
      if (!((ex.getCause() instanceof EntryNotFoundException))) {
        Assert.fail("Expected EntryNotFoundException ", ex);
      }
    }

    member1.invoke(() -> FixedPartitioningTestBase.putThroughDataStore("Quarter"));

    member1.invoke(() -> FixedPartitioningTestBase.getThroughDataStore("Quarter"));

  }

  /**
   * This tests validate that datastore member tries the put on itself as well as other datastores
   * as per primary FixedPartitionAttributes defined on datastores. But No resolver is provided. So
   * IllegalStateException in expected
   */

  @Test
  public void testPut_NoResolver() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 =
        FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, null, null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, null, null, false));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, null, null, false));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, null, null, false));

    try {
      member1.invoke(() -> FixedPartitioningTestBase.putThroughDataStore_NoResolver("Quarter"));
      fail("IllegalStateException expected");
    } catch (Exception ex) {
      if (!((ex.getCause() instanceof IllegalStateException))) {
        Assert.fail("Expected IllegalStateException ", ex);
      }
    }
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties result = super.getDistributedSystemProperties();
    result.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.internal.cache.partitioned.fixed.MyDate3");
    return result;

  }


  /**
   * This tests validate that datastore member tries the put with callback on itself as well as
   * other datastores as per primary FixedPartitionAttributes defined on datastores. here CallBack
   * implements FixedPartitionResolver.
   */

  @Test
  public void testPut_CallBackWithResolver() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 =
        FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, null, null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, null, null, false));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, null, null, false));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, null, null, false));

    member1.invoke(
        () -> FixedPartitioningTestBase.putThroughDataStore_CallBackWithResolver("Quarter"));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryData(Quarter1));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryData(Quarter2));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryData(Quarter3));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryData(Quarter4));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(3, 3));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(3, 3));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(3, 3));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(3, 3));
  }

  /**
   * This test validates that a PR without Fixed Partition Attributes and with
   * FixedPartitionResolver will do custom partitioning as per resolver.
   */

  @Test
  public void testPut_WithResolver_NoFPAs() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        null, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        null, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        null, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        null, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member1.invoke(() -> FixedPartitioningTestBase.putThroughDataStore("Quarter"));

  }

  /**
   * This tests validate that datastore member tries the put on itself as well as other datastores
   * as per primary FixedPartitionAttributes defined on datastores. Here No Resolver is provided
   * through attributes. Some keys implement FixedPartitionResolver and some does't implement any
   * resolver. IllegalStateException is expected.
   */


  @Test
  public void testPut_FixedPartitionResolver_NoResolver() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 =
        FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, null, null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, null, null, false));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, null, null, false));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, null, null, false));

    try {
      member1.invoke(() -> FixedPartitioningTestBase
          .putThroughDataStore_FixedPartitionResolver_NoResolver("Quarter"));
      fail("IllegalStateException expected");
    } catch (Exception ex) {
      if (!((ex.getCause() instanceof IllegalStateException))) {
        Assert.fail("Expected IllegalStateException ", ex);
      }
    }
  }

  /**
   * This tests validate that datastore member tries the put on itself as well as other datastores
   * as per primary FixedPartitionAttributes defined on datastores. Here No Resolver is provided
   * through attributes. Some keys implement FixedPartitionResolver and some implements
   * PartitionResolver. IllegalStateException is expected.
   */

  @Test
  public void testPut_FixedPartitionResolver_PartitionResolver() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 =
        FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, null, null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, null, null, false));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, null, null, false));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, null, null, false));

    try {
      member1.invoke(() -> FixedPartitioningTestBase
          .putThroughDataStore_FixedPartitionResolver_PartitionResolver("Quarter"));
      fail("IllegalStateException expected");
    } catch (Exception ex) {
      if (!((ex.getCause() instanceof IllegalStateException))) {
        Assert.fail("Expected IllegalStateException ", ex);
      }
    }
  }

  /**
   * This tests validate that datastore member tries the put on itself as well as other datastores
   * as per primary FixedPartitionAttributes defined on datastores with only one bucket per
   * partition.
   */

  @Test
  public void testFPR_DefaultNumBuckets() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter1, true);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true);
    fpaList.clear();
    fpaList.add(fpa1);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true);
    fpaList.clear();
    fpaList.add(fpa1);
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true);
    fpaList.clear();
    fpaList.add(fpa1);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, false));

    member1.invoke(() -> FixedPartitioningTestBase.putThroughDataStore("Quarter"));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryData(Quarter1));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryData(Quarter2));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryData(Quarter3));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryData(Quarter4));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(1, 1));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(1, 1));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(1, 1));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(1, 1));
  }

  /**
   * This tests validate that accessor member does the put on datastores as per primary and
   * secondary FixedPartitionAttributes defined on datastores.
   */

  @Test
  public void testPut_ValidateDataOnMember_PrimarySecondary_Accessor() {
    createCacheOnMember();
    createRegionWithPartitionAttributes("Quarter", null, 3, 0, 12, new QuarterPartitionResolver(),
        null, false);

    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 =
        FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    FixedPartitionAttributes fpa2 =
        FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    FixedPartitionAttributes fpa3 =
        FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    fpaList.add(fpa3);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 3, 40, 12, new QuarterPartitionResolver(), null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpa3 = FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    fpaList.add(fpa3);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 3, 40, 12, new QuarterPartitionResolver(), null, false));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpa3 = FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    fpaList.add(fpa3);
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 3, 40, 12, new QuarterPartitionResolver(), null, false));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpa3 = FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    fpaList.add(fpa3);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 3, 40, 12, new QuarterPartitionResolver(), null, false));

    putThroughDataStore("Quarter");

    member1.invoke(
        () -> FixedPartitioningTestBase.checkPrimarySecondaryData_TwoSecondaries(Quarter1, false));
    member2.invoke(
        () -> FixedPartitioningTestBase.checkPrimarySecondaryData_TwoSecondaries(Quarter2, false));
    member3.invoke(
        () -> FixedPartitioningTestBase.checkPrimarySecondaryData_TwoSecondaries(Quarter3, false));
    member4.invoke(
        () -> FixedPartitioningTestBase.checkPrimarySecondaryData_TwoSecondaries(Quarter4, false));
  }

  /**
   * This tests validate that datastore member does the put on itself as well as other datastores as
   * per primary and secondary FixedPartitionAttributes defined on datastores.
   */

  @Test
  public void testPut_ValidateDataOnMember_PrimarySecondary_Datastore() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 =
        FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    FixedPartitionAttributes fpa2 =
        FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

    member1.invoke(() -> FixedPartitioningTestBase.putThroughDataStore("Quarter"));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter1, false));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter2, false));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter3, false));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter4, false));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
  }

  /**
   * This tests validate that if only the secondary partitions are available then put should happen
   * successfully for these secondary partitions. These secondary partitions should acts as primary.
   * When the primary partition joins the system then this new member should create the primary
   * buckets for this partition on itself. And Secondary partitions who were holding primary buckets
   * status should now act as secondary buckets.
   *
   */

  @Test
  public void testPut_ValidateDataOnMember_OnlySecondary_Datastore() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 =
        FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 3, 40, 12, new QuarterPartitionResolver(), null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 3, 40, 12, new QuarterPartitionResolver(), null, false));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 3, 40, 12, new QuarterPartitionResolver(), null, false));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 3, 40, 12, new QuarterPartitionResolver(), null, false));

    member1.invoke(() -> FixedPartitioningTestBase.putThroughDataStore("Quarter"));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(3, 3));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(3, 3));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(3, 3));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(3, 3));

    createCacheOnMember();
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    createRegionWithPartitionAttributes("Quarter", fpaList, 3, 40, 12,
        new QuarterPartitionResolver(), null, false);

    Wait.pause(1000);

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(3, 0));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(3, 3));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(3, 3));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(3, 3));
    checkPrimaryBucketsForQuarter(3, 3);

  }

  /**
   * Accessor =1 Datastore = 4 Datastores Primary Secondary Member1 = Q1(0,1,2) Q3(3,4,5), Q4(6,7,8)
   * Member2 = Q2(9,10,11) Q3(3,4,5), Q4(6,7,8) Member3 = Q3(3,4,5) Q1(0,1,2), Q2(9,10,11) Member4 =
   * Q4(6,7,8) Q1(0,1,2), Q2(9,10,11) Put happens for all buckets Member 4 goes down, then either
   * member1 or member2 holds primary for member4
   *
   * Primary Secondary Member1 = Q1(0,1,2) Q3(3,4,5), Q4(6,7,8) Member2 = Q2(9,10,11), Q4(6,7,8)
   * Q3(3,4,5) Member3 = Q3(3,4,5) Q1(0,1,2), Q2(9,10,11)
   *
   * Put happens considering Member2 is holding primary for Q4.
   *
   * Member4 comes again, then Memeber4 should do the GII from member2 for buckets 6,7,8 and should
   * acqiure primary status Member1 = Q1(0,1,2) Q3(3,4,5), Q4(6,7,8) Member2 = Q2(9,10,11)
   * Q3(3,4,5), Q4(6,7,8) Member3 = Q3(3,4,5) Q1(0,1,2), Q2(9,10,11) Member4 = Q4(6,7,8) Q1(0,1,2),
   * Q2(9,10,11)
   *
   *
   */
  @Test
  public void testPut_ValidateDataOnMember_PrimarySecondary_Accessor_CacheClosed() {
    createCacheOnMember();
    createRegionWithPartitionAttributes("Quarter", null, 3, 0, 12, new QuarterPartitionResolver(),
        null, false);

    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 =
        FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    FixedPartitionAttributes fpa2 =
        FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    FixedPartitionAttributes fpa3 =
        FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    fpaList.add(fpa3);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 3, 40, 12, new QuarterPartitionResolver(), null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpa3 = FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    fpaList.add(fpa3);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 3, 40, 12, new QuarterPartitionResolver(), null, false));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpa3 = FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    fpaList.add(fpa3);
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 3, 40, 12, new QuarterPartitionResolver(), null, false));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpa3 = FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    fpaList.add(fpa3);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 3, 40, 12, new QuarterPartitionResolver(), null, false));

    putThroughDataStore("Quarter");

    member1.invoke(
        () -> FixedPartitioningTestBase.checkPrimarySecondaryData_TwoSecondaries(Quarter1, false));
    member2.invoke(
        () -> FixedPartitioningTestBase.checkPrimarySecondaryData_TwoSecondaries(Quarter2, false));
    member3.invoke(
        () -> FixedPartitioningTestBase.checkPrimarySecondaryData_TwoSecondaries(Quarter3, false));
    member4.invoke(
        () -> FixedPartitioningTestBase.checkPrimarySecondaryData_TwoSecondaries(Quarter4, false));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(9, 3));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(9, 3));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(9, 3));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(9, 3));

    member4.invoke(() -> FixedPartitioningTestBase.closeCache());
    Wait.pause(1000);

    member1.invoke(
        () -> FixedPartitioningTestBase.checkPrimarySecondaryData_TwoSecondaries(Quarter1, false));
    member2.invoke(
        () -> FixedPartitioningTestBase.checkPrimarySecondaryData_TwoSecondaries(Quarter2, false));
    member3.invoke(
        () -> FixedPartitioningTestBase.checkPrimarySecondaryData_TwoSecondaries(Quarter3, false));

    member1.invoke(
        () -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarterAfterCacheClosed(9, 3));
    member2.invoke(
        () -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarterAfterCacheClosed(9, 3));
    member3.invoke(
        () -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarterAfterCacheClosed(9, 3));
  }

  /**
   * Datastore = 4 Datastores Primary Secondary Member1 = Q1(0,1,2) Q2(3,4,5) Member2 = Q2(3,4,5)
   * Q3(6,7,8) Member3 = Q3(6,7,8) Q4(9,10,11) Member4 = Q4(9,10,11) Q1(0,1,2) Put happens for all
   * buckets Member 4 goes down, then either member1 or member2 holds primary for member4
   *
   * Primary Secondary Member1 = Q1(0,1,2) Q2(3,4,5) Member2 = Q2(3,4,5) Q3(6,7,8) Member3 =
   * Q3(6,7,8), Q4(9,10,11)
   *
   * Put happens considering Member3 is holding primary for Q4.
   *
   * Member4 comes again, then Memeber4 should do the GII from member2 for buckets 6,7,8 and should
   * acqiure primary status Datastores Primary Secondary Member1 = Q1(0,1,2) Q2(3,4,5) Member2 =
   * Q2(3,4,5) Q3(6,7,8) Member3 = Q3(6,7,8) Q4(9,10,11) Member4 = Q4(9,10,11) Q1(0,1,2)
   */

  @Test
  public void testPut_ValidateDataOnMember_PrimarySecondary_Datastore_CacheClosed() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 =
        FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    FixedPartitionAttributes fpa2 =
        FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

    member1.invoke(() -> FixedPartitioningTestBase.putThroughDataStore("Quarter"));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter1, false));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter2, false));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter3, false));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter4, false));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));

    member4.invoke(() -> FixedPartitioningTestBase.closeCache());
    Wait.pause(1000);

    member3.invoke(
        () -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarterAfterCacheClosed(6, 6));

    member1.invoke(() -> FixedPartitioningTestBase.putHAData("Quarter"));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());

    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

    Wait.pause(1000);

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter1, true));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter2, true));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter3, true));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter4, true));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
  }

  @Test
  public void test_Bug46619_Put_ValidateDataOnMember_PrimarySecondary_Datastore_CacheClosed() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember_DisableMovePrimary());
    FixedPartitionAttributes fpa1 =
        FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    FixedPartitionAttributes fpa2 =
        FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember_DisableMovePrimary());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember_DisableMovePrimary());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember_DisableMovePrimary());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

    member1.invoke(() -> FixedPartitioningTestBase.putThroughDataStore("Quarter"));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter1, false));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter2, false));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter3, false));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter4, false));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));

    member4.invoke(() -> FixedPartitioningTestBase.closeCache());
    member2.invoke(() -> FixedPartitioningTestBase.closeCache());
    Wait.pause(1000);

    member3.invoke(
        () -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarterAfterCacheClosed(6, 6));
    member1.invoke(
        () -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarterAfterCacheClosed(6, 6));

    member1.invoke(() -> FixedPartitioningTestBase.putHAData("Quarter"));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember_DisableMovePrimary());

    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember_DisableMovePrimary());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, false));
    Wait.pause(1000);

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter1, true));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter2, true));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter3, true));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData(Quarter4, true));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 6));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 0));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 6));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 0));

    member4.invoke(() -> FixedPartitioningTestBase.doRebalance());

    Wait.pause(2000);
    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter(6, 3));
  }

  /**
   * Datastore = 4 Datastores Primary Secondary Member1 = Q1(0,1,2) Q2(3,4,5),Q3(6,7,8),Q4(9,10,11)
   * Member2 = Q3(6,7,8) Q1(0,1,2), Q2(3,4,5),Q4(9,10,11) Member3 = Q2(3,4,5),Q4(9,10,11) Q1(0,1,2),
   * Q3(6,7,8)
   *
   * Put happens for all buckets
   *
   * Member 3 goes down, then either member1 or member2 holds primary for member4
   *
   * Primary Secondary Member1 = Q1(0,1,2),Q2(3,4,5) Q3(6,7,8), Q4(9,10,11) Member2 =
   * Q3(6,7,8),Q4(9,10,11) Q1(0,1,2), Q2(3,4,5)
   *
   * Member 3 comes again then it should be same as it was before member 3 went down
   *
   * Member1 = Q1(0,1,2) Q2(3,4,5),Q3(6,7,8),Q4(9,10,11) Member2 = Q3(6,7,8) Q1(0,1,2),
   * Q2(3,4,5),Q4(9,10,11) Member3 = Q2(3,4,5),Q4(9,10,11) Q1(0,1,2), Q3(6,7,8)
   *
   */

  @Test
  public void testPut_ValidateDataOnMember_MultiplePrimaries_Datastore_CacheClosed() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());

    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();

    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3));

    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 2, 40, 12, new QuarterPartitionResolver(), null, false));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpaList.clear();
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3));

    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 2, 40, 12, new QuarterPartitionResolver(), null, false));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpaList.clear();
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3));
    fpaList.add(FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3));


    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 2, 40, 12, new QuarterPartitionResolver(), null, false));


    member1.invoke(() -> FixedPartitioningTestBase.putThroughDataStore("Quarter"));
    member1.invoke(() -> FixedPartitioningTestBase.checkStartingBucketIDs());
    member2.invoke(() -> FixedPartitioningTestBase.checkStartingBucketIDs());
    member3.invoke(() -> FixedPartitioningTestBase.checkStartingBucketIDs());

    member3.invoke(() -> FixedPartitioningTestBase.closeCache());

    Wait.pause(1000);

    member1.invoke(() -> FixedPartitioningTestBase.checkStartingBucketIDs_Nodedown());
    member2.invoke(() -> FixedPartitioningTestBase.checkStartingBucketIDs_Nodedown());

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());

    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes("Quarter",
        fpaList, 2, 40, 12, new QuarterPartitionResolver(), null, false));

    Wait.pause(3000);

    member1.invoke(() -> FixedPartitioningTestBase.checkStartingBucketIDs_Nodeup());
    member2.invoke(() -> FixedPartitioningTestBase.checkStartingBucketIDs_Nodeup());
    member3.invoke(() -> FixedPartitioningTestBase.checkStartingBucketIDs_Nodeup());


  }

}
