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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Wait;

@Category(DistributedTest.class)
public class FixedPartitioningWithColocationAndPersistenceDUnitTest extends
    FixedPartitioningTestBase {

  public FixedPartitioningWithColocationAndPersistenceDUnitTest() {
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
   * This tests validates that in colocation of FPRs child region cannot specify
   * FixedPartitionAttributes
   * 
   */

  @Test
  public void testColocation_WithFPROnChildRegion() {
    try {
      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition("Customer100", true, 2);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 0, 40, 8, null, null, false ));

      fpa1 = FixedPartitionAttributes.createFixedPartition("Order100", true, 2);
      fpaList.clear();
      fpaList.add(fpa1);
      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order",
              fpaList, 0, 40, 8, null, "Customer", false ));
      fail("IllegalStateException expected");
    }
    catch (Exception illegal) {
      if (!((illegal.getCause() instanceof IllegalStateException) && (illegal
          .getCause().getMessage()
          .contains("not be specified in PartitionAttributesFactory if colocated-with is specified")))) {
        Assert.fail("Expected IllegalStateException ", illegal);
      }
    }
  }

  /**
   * This tests validates that in Customer-Order-shipment colocation, Order and
   * shipment have the FixedPartitionAttributes of the parent region Customer.
   * 
   * Put happens for all 3 regions. Colocation of the data is achieved by using
   * a partition-resolver
   * {@link CustomerFixedPartitionResolver#getRoutingObject(com.gemstone.gemfire.cache.EntryOperation)}
   * Also the Fixed Partitioning is achieved using same partition-resolver
   * {@link CustomerFixedPartitionResolver#getPartitionName(com.gemstone.gemfire.cache.EntryOperation, java.util.Set)}
   * 
   * Validation are done for the same number of the buckets Validation are done
   * for the same buckets on particular member for all 3 regions.
   * 
   */

  @Test
  public void testColocation_FPRs_ChildUsingAttributesOfParent() {
    try {
      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());

      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition("10", true, 5);
      FixedPartitionAttributes fpa2 = FixedPartitionAttributes
          .createFixedPartition("30", false, 5);
      FixedPartitionAttributes fpa3 = FixedPartitionAttributes
          .createFixedPartition("40", false, 5);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false ));

      fpa1 = FixedPartitionAttributes.createFixedPartition("20", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("30", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("40", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false ));

      fpa1 = FixedPartitionAttributes.createFixedPartition("30", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("20", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false ));

      fpa1 = FixedPartitionAttributes.createFixedPartition("40", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("20", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false ));

      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));

      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));
      member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));

      member1.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member1.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));
      member3.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member3.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));
      member4.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member4.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.putOrderPartitionedRegion( "Order" ));

      member1.invoke(() -> FixedPartitioningTestBase.putCustomerPartitionedRegion( "Customer" ));

      member1.invoke(() -> FixedPartitioningTestBase.putShipmentPartitionedRegion( "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.validateAfterPutPartitionedRegion( "Customer",
              "Order", "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));
      member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));
      member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));

    }
    catch (Exception e) {
      Assert.fail("Unexpected Exception ", e);
    }
  }

  @Test
  public void testColocation_FPR_Persistence_ChildUsingAttributesOfParent() {
    try {
      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());

      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition("10", true, 5);
      FixedPartitionAttributes fpa2 = FixedPartitionAttributes
          .createFixedPartition("30", false, 5);
      FixedPartitionAttributes fpa3 = FixedPartitionAttributes
          .createFixedPartition("40", false, 5);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              true ));

      fpa1 = FixedPartitionAttributes.createFixedPartition("20", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("30", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("40", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              true ));

      fpa1 = FixedPartitionAttributes.createFixedPartition("30", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("20", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              true ));

      fpa1 = FixedPartitionAttributes.createFixedPartition("40", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("20", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              true ));

      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));

      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));
      member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));

      member1.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member1.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));
      member3.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member3.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));
      member4.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member4.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.putOrderPartitionedRegion( "Order" ));

      member1.invoke(() -> FixedPartitioningTestBase.putCustomerPartitionedRegion( "Customer" ));

      member1.invoke(() -> FixedPartitioningTestBase.putShipmentPartitionedRegion( "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.validateAfterPutPartitionedRegion( "Customer",
              "Order", "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));
      member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));
      member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));

    }
    catch (Exception e) {
      Assert.fail("Unexpected Exception ", e);
    }
  }

  /**
   * This tests validates that Customer-Order-shipment colocation with failover
   * scenario,
   */

  @Test
  public void testColocation_FPRs_ChildUsingAttributesOfParent_HA() {
    try {
      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());

      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition("10", true, 5);
      FixedPartitionAttributes fpa2 = FixedPartitionAttributes
          .createFixedPartition("30", false, 5);
      FixedPartitionAttributes fpa3 = FixedPartitionAttributes
          .createFixedPartition("40", false, 5);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false ));

      fpa1 = FixedPartitionAttributes.createFixedPartition("20", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("30", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("40", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false ));

      fpa1 = FixedPartitionAttributes.createFixedPartition("30", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("40", true, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      FixedPartitionAttributes fpa4 = FixedPartitionAttributes
          .createFixedPartition("20", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      fpaList.add(fpa4);
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false ));

      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));

      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));

      member1.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member1.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));
      member3.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member3.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.putCustomerPartitionedRegion( "Customer" ));
      member1.invoke(() -> FixedPartitioningTestBase.putOrderPartitionedRegion( "Order" ));
      member1.invoke(() -> FixedPartitioningTestBase.putShipmentPartitionedRegion( "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.validateAfterPutPartitionedRegion( "Customer",
              "Order", "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));
      member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 20, 10,
              "Customer", "Order", "Shipment" ));

      member3.invoke(() -> FixedPartitioningTestBase.closeCache());
      Wait.pause(4000);

      member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocationAfterCacheClosed(
              15, 5, "Customer", "Order", "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocationAfterCacheClosed(
              15, 5, "Customer", "Order", "Shipment" ));

      member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      fpa1 = FixedPartitionAttributes.createFixedPartition("30", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("20", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false ));
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));

      Wait.pause(4000);

      member1.invoke(() -> FixedPartitioningTestBase.validateAfterPutPartitionedRegion( "Customer",
              "Order", "Shipment" ));

      member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));

      member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      fpa1 = FixedPartitionAttributes.createFixedPartition("40", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("20", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false ));
      member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));

      Wait.pause(4000);

      member1.invoke(() -> FixedPartitioningTestBase.validateAfterPutPartitionedRegion( "Customer",
              "Order", "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));
      member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));
      member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));

    }
    catch (Exception e) {
      Assert.fail("Unexpected Exception ", e);
    }
  }

  @Test
  public void testColocation_FPR_Persistence_ChildUsingAttributesOfParent_HA() {
    try {
      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());

      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition("10", true, 5);
      FixedPartitionAttributes fpa2 = FixedPartitionAttributes
          .createFixedPartition("30", false, 5);
      FixedPartitionAttributes fpa3 = FixedPartitionAttributes
          .createFixedPartition("40", false, 5);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              true ));

      fpa1 = FixedPartitionAttributes.createFixedPartition("20", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("30", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("40", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              true ));

      fpa1 = FixedPartitionAttributes.createFixedPartition("30", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("40", true, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      FixedPartitionAttributes fpa4 = FixedPartitionAttributes
          .createFixedPartition("20", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      fpaList.add(fpa4);
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              true ));

      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));

      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));

      member1.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member1.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));
      member3.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member3.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.putCustomerPartitionedRegion( "Customer" ));
      member1.invoke(() -> FixedPartitioningTestBase.putOrderPartitionedRegion( "Order" ));
      member1.invoke(() -> FixedPartitioningTestBase.putShipmentPartitionedRegion( "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.validateAfterPutPartitionedRegion( "Customer",
              "Order", "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));
      member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 20, 10,
              "Customer", "Order", "Shipment" ));

      member3.invoke(() -> FixedPartitioningTestBase.closeCache());
      Wait.pause(4000);

      member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocationAfterCacheClosed(
              15, 5, "Customer", "Order", "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocationAfterCacheClosed(
              15, 5, "Customer", "Order", "Shipment" ));

      member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      fpa1 = FixedPartitionAttributes.createFixedPartition("30", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("20", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false ));
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));

      Wait.pause(4000);

      member1.invoke(() -> FixedPartitioningTestBase.validateAfterPutPartitionedRegion( "Customer",
              "Order", "Shipment" ));

      member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));

      member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      fpa1 = FixedPartitionAttributes.createFixedPartition("40", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("20", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false ));
      member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));

      Wait.pause(4000);

      member1.invoke(() -> FixedPartitioningTestBase.validateAfterPutPartitionedRegion( "Customer",
              "Order", "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));
      member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));
      member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 15, 5, "Customer",
              "Order", "Shipment" ));

    }
    catch (Exception e) {
      Assert.fail("Unexpected Exception ", e);
    }
  }

  /**
   * Tests validate the behavior of FPR with persistence when one member is kept
   * alive and other members goes down and come up
   */
  @Test
  public void testFPR_Persistence_OneMemberAlive() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true ));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true ));

    member1.invoke(() -> FixedPartitioningTestBase.putForQuarter( "Quarter", "Q1" ));
    member1.invoke(() -> FixedPartitioningTestBase.putForQuarter( "Quarter", "Q2" ));

    member2.invoke(() -> FixedPartitioningTestBase.closeCache());

    Wait.pause(1000);

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true ));

    member2.invoke(() -> FixedPartitioningTestBase.getForQuarter( "Quarter", "Q2" ));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true ));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true ));

    member1.invoke(() -> FixedPartitioningTestBase.putThroughDataStore( "Quarter" ));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryData( Quarter1 ));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryData( Quarter2 ));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryData( Quarter3 ));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryData( Quarter4 ));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 3, 3 ));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 3, 3 ));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 3, 3 ));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 3, 3 ));

  }

  /**
   * Tests validate the behavior of FPR with persistence when all members goes
   * down and comes up.
   * 
   */
  @Test
  public void testFPR_Persistence() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    FixedPartitionAttributes fpa2 = FixedPartitionAttributes
        .createFixedPartition(Quarter2, false, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true ));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true ));

    member1.invoke(() -> FixedPartitioningTestBase.putForQuarter( "Quarter", "Q1" ));
    member1.invoke(() -> FixedPartitioningTestBase.putForQuarter( "Quarter", "Q2" ));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryDataPersistence( Quarter1 ));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryDataPersistence( Quarter2 ));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 3 ));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 3 ));

    member1.invoke(() -> FixedPartitioningTestBase.closeCache());
    member2.invoke(() -> FixedPartitioningTestBase.closeCache());

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true ));

    Wait.pause(4000);
    member2.invoke(() -> FixedPartitioningTestBase.getForQuarter( "Quarter", Quarter1 ));
    member2.invoke(() -> FixedPartitioningTestBase.getForQuarter( "Quarter", Quarter2 ));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryDataPersistence( Quarter2 ));
    Wait.pause(2000);
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 6 ));

    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true ));

    Wait.pause(4000);

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryDataPersistence( Quarter1 ));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryDataPersistence( Quarter2 ));
    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 3 ));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 3 ));

    member1.invoke(() -> FixedPartitioningTestBase.getForQuarter( "Quarter", Quarter1 ));

    member2.invoke(() -> FixedPartitioningTestBase.getForQuarter( "Quarter", Quarter2 ));
  }

  /**
   * Tests validate the behavior of FPR with persistence and with colocation
   * when one member is kept alive and other members goes down and come up
   */
  @Test
  public void testColocation_FPR_Persistence_Colocation_OneMemberAlive() {
    try {
      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());

      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition("10", true, 5);
      FixedPartitionAttributes fpa2 = FixedPartitionAttributes
          .createFixedPartition("20", false, 5);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 1, 50, 20, new CustomerFixedPartitionResolver(), null,
              true ));

      fpa1 = FixedPartitionAttributes.createFixedPartition("20", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 1, 50, 20, new CustomerFixedPartitionResolver(), null,
              true ));

      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              1, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              1, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));

      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 1, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 1, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));

      member1.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member1.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.putCustomerPartitionedRegion_Persistence1( "Customer" ));
      member1.invoke(() -> FixedPartitioningTestBase.putOrderPartitionedRegion_Persistence1( "Order" ));
      member1.invoke(() -> FixedPartitioningTestBase.putShipmentPartitionedRegion_Persistence1( "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.validateAfterPutPartitionedRegion( "Customer",
              "Order", "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 10, 5, "Customer",
              "Order", "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 10, 5, "Customer",
              "Order", "Shipment" ));

      member2.invoke(() -> FixedPartitioningTestBase.closeCache());
      member1.invoke(() -> FixedPartitioningTestBase.putCustomerPartitionedRegion_Persistence2( "Customer" ));
      member1.invoke(() -> FixedPartitioningTestBase.putOrderPartitionedRegion_Persistence2( "Order" ));
      member1.invoke(() -> FixedPartitioningTestBase.putShipmentPartitionedRegion_Persistence2( "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.validateAfterPutPartitionedRegion( "Customer",
              "Order", "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 10, 10,
              "Customer", "Order", "Shipment" ));

      member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      fpa1 = FixedPartitionAttributes.createFixedPartition("20", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      AsyncInvocation[] async = new AsyncInvocation[2];
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 1, 50, 20, new CustomerFixedPartitionResolver(), null,
              true ));
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              1, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 1, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));

      Wait.pause(4000);
      member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 10, 5, "Customer",
              "Order", "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 10, 5, "Customer",
              "Order", "Shipment" ));
      member1.invoke(() -> FixedPartitioningTestBase.validateAfterPutPartitionedRegion( "Customer",
              "Order", "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.validateAfterPutPartitionedRegion( "Customer",
              "Order", "Shipment" ));

    }
    catch (Exception e) {
      Assert.fail("Unexpected Exception ", e);
    }
  }

  /**
   * Tests validate the behavior of FPR with persistence and with colocation
   * when all members goes down and comes up.
   * 
   */

  @Test
  public void testColocation_FPR_Persistence_Colocation() {
    try {
      member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());

      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition("10", true, 5);
      FixedPartitionAttributes fpa2 = FixedPartitionAttributes
          .createFixedPartition("20", false, 5);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 1, 50, 20, new CustomerFixedPartitionResolver(), null,
              true ));

      fpa1 = FixedPartitionAttributes.createFixedPartition("20", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 1, 50, 20, new CustomerFixedPartitionResolver(), null,
              true ));

      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              1, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Order", null,
              1, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false ));

      member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 1, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Shipment",
              null, 1, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false ));

      member1.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member1.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkFPR( "Order" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkFPR( "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.putCustomerPartitionedRegion_Persistence( "Customer" ));
      member1.invoke(() -> FixedPartitioningTestBase.putOrderPartitionedRegion_Persistence( "Order" ));
      member1.invoke(() -> FixedPartitioningTestBase.putShipmentPartitionedRegion_Persistence( "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.validateAfterPutPartitionedRegion( "Customer",
              "Order", "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 10, 5, "Customer",
              "Order", "Shipment" ));
      member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForColocation( 10, 5, "Customer",
              "Order", "Shipment" ));

      member1.invoke(() -> FixedPartitioningTestBase.closeCache());
      member2.invoke(() -> FixedPartitioningTestBase.closeCache());

      member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
      fpa1 = FixedPartitionAttributes.createFixedPartition("20", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Customer",
              fpaList, 1, 50, 20, new CustomerFixedPartitionResolver(), null,
              true ));

      Wait.pause(4000);
      member2.invoke(() -> FixedPartitioningTestBase.getForColocation( "Customer", "Order", "Shipment" ));

    }
    catch (Exception e) {
      Assert.fail("Unexpected Exception ", e);
    }
  }

  @Test
  public void testFPR_Persistence2() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    FixedPartitionAttributes fpa2 = FixedPartitionAttributes
        .createFixedPartition(Quarter2, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true ));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true ));

    member1.invoke(() -> FixedPartitioningTestBase.putThroughDataStore( "Quarter" ));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData( Quarter1, false ));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData( Quarter3, false ));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 6 ));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 6 ));

    member1.invoke(() -> FixedPartitioningTestBase.closeCache());
    member2.invoke(() -> FixedPartitioningTestBase.closeCache());

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invokeAsync(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true ));

    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invokeAsync(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true ));

    Wait.pause(4000);
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData( Quarter3, false ));

    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 6 ));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData( Quarter1, false ));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData( Quarter3, false ));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 6 ));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 6 ));
  }

  @Test
  public void testFPR_Persistence3() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    FixedPartitionAttributes fpa2 = FixedPartitionAttributes
        .createFixedPartition(Quarter2, false, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true ));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true ));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member3.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true ));

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member4.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true ));

    member1.invoke(() -> FixedPartitioningTestBase.putThroughDataStore( "Quarter" ));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData( Quarter1, false ));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData( Quarter2, false ));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData( Quarter3, false ));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData( Quarter4, false ));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 3 ));
    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 3 ));
    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 3 ));
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 3 ));

    member1.invoke(() -> FixedPartitioningTestBase.closeCache());
    member2.invoke(() -> FixedPartitioningTestBase.closeCache());
    member3.invoke(() -> FixedPartitioningTestBase.closeCache());
    member4.invoke(() -> FixedPartitioningTestBase.closeCache());

    member4.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member4.invokeAsync(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true ));

    member3.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member3.invokeAsync(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true ));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invokeAsync(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true ));

    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    fpaList.clear();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invokeAsync(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true ));

    Wait.pause(4000);
    member4.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData( Quarter4, false ));

    member4.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 3 ));

    member3.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData( Quarter3, false ));

    member3.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 3 ));

    member2.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData( Quarter2, false ));

    member2.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 3 ));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimarySecondaryData( Quarter1, false ));

    member1.invoke(() -> FixedPartitioningTestBase.checkPrimaryBucketsForQuarter( 6, 3 ));

  }

  /**
   * Test validate a normal PR's persistence behavior. normal PR region is
   * created on member1 and member2. Put is done on this PR Member1 and Meber2's
   * cache is closed respectively. Member2 is brought back and persisted data is
   * verified.
   */
  @Test
  public void testPR_Persistence() {
    member1.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    member1.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter", null,
            1, 40, 12, new QuarterPartitionResolver(), null, true ));

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter", null,
            1, 40, 12, new QuarterPartitionResolver(), null, true ));

    member1.invoke(() -> FixedPartitioningTestBase.putThroughDataStore( "Quarter" ));

    member1.invoke(() -> FixedPartitioningTestBase.closeCache());
    member2.invoke(() -> FixedPartitioningTestBase.closeCache());

    Wait.pause(1000);

    member2.invoke(() -> FixedPartitioningTestBase.createCacheOnMember());
    member2.invoke(() -> FixedPartitioningTestBase.createRegionWithPartitionAttributes( "Quarter", null,
            1, 40, 12, new QuarterPartitionResolver(), null, true ));

    member2.invoke(() -> FixedPartitioningTestBase.getThroughDataStore( "Quarter" ));
  }

}
