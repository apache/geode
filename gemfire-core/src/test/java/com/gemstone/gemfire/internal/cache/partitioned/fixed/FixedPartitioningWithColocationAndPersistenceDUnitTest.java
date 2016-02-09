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

import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Wait;

public class FixedPartitioningWithColocationAndPersistenceDUnitTest extends
    FixedPartitioningTestBase {

  public FixedPartitioningWithColocationAndPersistenceDUnitTest(String name) {
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

  /**
   * This tests validates that in colocation of FPRs child region cannot specify
   * FixedPartitionAttributes
   * 
   */

  public void testColocation_WithFPROnChildRegion() {
    try {
      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition("Customer100", true, 2);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 0, 40, 8, null, null, false });

      fpa1 = FixedPartitionAttributes.createFixedPartition("Order100", true, 2);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order",
              fpaList, 0, 40, 8, null, "Customer", false });
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

  public void testColocation_FPRs_ChildUsingAttributesOfParent() {
    try {
      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");

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
      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false });

      fpa1 = FixedPartitionAttributes.createFixedPartition("20", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("30", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("40", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false });

      fpa1 = FixedPartitionAttributes.createFixedPartition("30", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("20", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false });

      fpa1 = FixedPartitionAttributes.createFixedPartition("40", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("20", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member4.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false });

      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member4.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });

      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });
      member4.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });

      member1.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member1.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member2.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });
      member3.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member3.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });
      member4.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member4.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "putOrderPartitionedRegion", new Object[] { "Order" });

      member1.invoke(FixedPartitioningTestBase.class,
          "putCustomerPartitionedRegion", new Object[] { "Customer" });

      member1.invoke(FixedPartitioningTestBase.class,
          "putShipmentPartitionedRegion", new Object[] { "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "validateAfterPutPartitionedRegion", new Object[] { "Customer",
              "Order", "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });
      member3.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });
      member4.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });

    }
    catch (Exception e) {
      Assert.fail("Unexpected Exception ", e);
    }
  }

  public void testColocation_FPR_Persistence_ChildUsingAttributesOfParent() {
    try {
      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");

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
      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              true });

      fpa1 = FixedPartitionAttributes.createFixedPartition("20", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("30", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("40", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              true });

      fpa1 = FixedPartitionAttributes.createFixedPartition("30", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("20", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              true });

      fpa1 = FixedPartitionAttributes.createFixedPartition("40", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("20", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member4.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              true });

      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member4.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });

      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });
      member4.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });

      member1.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member1.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member2.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });
      member3.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member3.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });
      member4.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member4.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "putOrderPartitionedRegion", new Object[] { "Order" });

      member1.invoke(FixedPartitioningTestBase.class,
          "putCustomerPartitionedRegion", new Object[] { "Customer" });

      member1.invoke(FixedPartitioningTestBase.class,
          "putShipmentPartitionedRegion", new Object[] { "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "validateAfterPutPartitionedRegion", new Object[] { "Customer",
              "Order", "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });
      member3.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });
      member4.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });

    }
    catch (Exception e) {
      Assert.fail("Unexpected Exception ", e);
    }
  }

  /**
   * This tests validates that Customer-Order-shipment colocation with failover
   * scenario,
   */

  public void testColocation_FPRs_ChildUsingAttributesOfParent_HA() {
    try {
      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");

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
      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false });

      fpa1 = FixedPartitionAttributes.createFixedPartition("20", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("30", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("40", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false });

      fpa1 = FixedPartitionAttributes.createFixedPartition("30", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("40", true, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      FixedPartitionAttributes fpa4 = FixedPartitionAttributes
          .createFixedPartition("20", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      fpaList.add(fpa4);
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false });

      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });

      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });

      member1.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member1.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member2.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });
      member3.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member3.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "putCustomerPartitionedRegion", new Object[] { "Customer" });
      member1.invoke(FixedPartitioningTestBase.class,
          "putOrderPartitionedRegion", new Object[] { "Order" });
      member1.invoke(FixedPartitioningTestBase.class,
          "putShipmentPartitionedRegion", new Object[] { "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "validateAfterPutPartitionedRegion", new Object[] { "Customer",
              "Order", "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });
      member3.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 20, 10,
              "Customer", "Order", "Shipment" });

      member3.invoke(FixedPartitioningTestBase.class, "closeCache");
      Wait.pause(4000);

      member1.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocationAfterCacheClosed", new Object[] {
              15, 5, "Customer", "Order", "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocationAfterCacheClosed", new Object[] {
              15, 5, "Customer", "Order", "Shipment" });

      member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      fpa1 = FixedPartitionAttributes.createFixedPartition("30", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("20", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false });
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });

      Wait.pause(4000);

      member1.invoke(FixedPartitioningTestBase.class,
          "validateAfterPutPartitionedRegion", new Object[] { "Customer",
              "Order", "Shipment" });

      member3.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });

      member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      fpa1 = FixedPartitionAttributes.createFixedPartition("40", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("20", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member4.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false });
      member4.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member4.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });

      Wait.pause(4000);

      member1.invoke(FixedPartitioningTestBase.class,
          "validateAfterPutPartitionedRegion", new Object[] { "Customer",
              "Order", "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });
      member3.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });
      member4.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });

    }
    catch (Exception e) {
      Assert.fail("Unexpected Exception ", e);
    }
  }

  public void testColocation_FPR_Persistence_ChildUsingAttributesOfParent_HA() {
    try {
      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");

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
      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              true });

      fpa1 = FixedPartitionAttributes.createFixedPartition("20", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("30", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("40", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              true });

      fpa1 = FixedPartitionAttributes.createFixedPartition("30", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("40", true, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      FixedPartitionAttributes fpa4 = FixedPartitionAttributes
          .createFixedPartition("20", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      fpaList.add(fpa4);
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              true });

      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });

      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });

      member1.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member1.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member2.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });
      member3.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member3.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "putCustomerPartitionedRegion", new Object[] { "Customer" });
      member1.invoke(FixedPartitioningTestBase.class,
          "putOrderPartitionedRegion", new Object[] { "Order" });
      member1.invoke(FixedPartitioningTestBase.class,
          "putShipmentPartitionedRegion", new Object[] { "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "validateAfterPutPartitionedRegion", new Object[] { "Customer",
              "Order", "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });
      member3.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 20, 10,
              "Customer", "Order", "Shipment" });

      member3.invoke(FixedPartitioningTestBase.class, "closeCache");
      Wait.pause(4000);

      member1.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocationAfterCacheClosed", new Object[] {
              15, 5, "Customer", "Order", "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocationAfterCacheClosed", new Object[] {
              15, 5, "Customer", "Order", "Shipment" });

      member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      fpa1 = FixedPartitionAttributes.createFixedPartition("30", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("20", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false });
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member3.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });

      Wait.pause(4000);

      member1.invoke(FixedPartitioningTestBase.class,
          "validateAfterPutPartitionedRegion", new Object[] { "Customer",
              "Order", "Shipment" });

      member3.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });

      member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      fpa1 = FixedPartitionAttributes.createFixedPartition("40", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpa3 = FixedPartitionAttributes.createFixedPartition("20", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      fpaList.add(fpa3);
      member4.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 2, 50, 20, new CustomerFixedPartitionResolver(), null,
              false });
      member4.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              2, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member4.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 2, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });

      Wait.pause(4000);

      member1.invoke(FixedPartitioningTestBase.class,
          "validateAfterPutPartitionedRegion", new Object[] { "Customer",
              "Order", "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });
      member3.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });
      member4.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 15, 5, "Customer",
              "Order", "Shipment" });

    }
    catch (Exception e) {
      Assert.fail("Unexpected Exception ", e);
    }
  }

  /**
   * Tests validate the behavior of FPR with persistence when one member is kept
   * alive and other members goes down and come up
   */
  public void testFPR_Persistence_OneMemberAlive() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member1.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member2.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true });

    member1.invoke(FixedPartitioningTestBase.class, "putForQuarter",
        new Object[] { "Quarter", "Q1" });
    member1.invoke(FixedPartitioningTestBase.class, "putForQuarter",
        new Object[] { "Quarter", "Q2" });

    member2.invoke(FixedPartitioningTestBase.class, "closeCache");

    Wait.pause(1000);

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member2.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true });

    member2.invoke(FixedPartitioningTestBase.class, "getForQuarter",
        new Object[] { "Quarter", "Q2" });

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member3.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true });

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    member4.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true });

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

    member1.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 3, 3 });
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 3, 3 });
    member3.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 3, 3 });
    member4.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 3, 3 });

  }

  /**
   * Tests validate the behavior of FPR with persistence when all members goes
   * down and comes up.
   * 
   */
  public void testFPR_Persistence() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    FixedPartitionAttributes fpa2 = FixedPartitionAttributes
        .createFixedPartition(Quarter2, false, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true });

    member1.invoke(FixedPartitioningTestBase.class, "putForQuarter",
        new Object[] { "Quarter", "Q1" });
    member1.invoke(FixedPartitioningTestBase.class, "putForQuarter",
        new Object[] { "Quarter", "Q2" });

    member1.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryDataPersistence", new Object[] { Quarter1 });
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryDataPersistence", new Object[] { Quarter2 });

    member1.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 3 });
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 3 });

    member1.invoke(FixedPartitioningTestBase.class, "closeCache");
    member2.invoke(FixedPartitioningTestBase.class, "closeCache");

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true });

    Wait.pause(4000);
    member2.invoke(FixedPartitioningTestBase.class, "getForQuarter",
        new Object[] { "Quarter", Quarter1 });
    member2.invoke(FixedPartitioningTestBase.class, "getForQuarter",
        new Object[] { "Quarter", Quarter2 });
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryDataPersistence", new Object[] { Quarter2 });
    Wait.pause(2000);
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 6 });

    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true });

    Wait.pause(4000);

    member1.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryDataPersistence", new Object[] { Quarter1 });
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryDataPersistence", new Object[] { Quarter2 });
    member1.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 3 });
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 3 });

    member1.invoke(FixedPartitioningTestBase.class, "getForQuarter",
        new Object[] { "Quarter", Quarter1 });

    member2.invoke(FixedPartitioningTestBase.class, "getForQuarter",
        new Object[] { "Quarter", Quarter2 });
  }

  /**
   * Tests validate the behavior of FPR with persistence and with colocation
   * when one member is kept alive and other members goes down and come up
   */
  public void testColocation_FPR_Persistence_Colocation_OneMemberAlive() {
    try {
      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");

      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition("10", true, 5);
      FixedPartitionAttributes fpa2 = FixedPartitionAttributes
          .createFixedPartition("20", false, 5);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 1, 50, 20, new CustomerFixedPartitionResolver(), null,
              true });

      fpa1 = FixedPartitionAttributes.createFixedPartition("20", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 1, 50, 20, new CustomerFixedPartitionResolver(), null,
              true });

      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              1, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              1, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });

      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 1, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 1, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });

      member1.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member1.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member2.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "putCustomerPartitionedRegion_Persistence1",
          new Object[] { "Customer" });
      member1.invoke(FixedPartitioningTestBase.class,
          "putOrderPartitionedRegion_Persistence1", new Object[] { "Order" });
      member1.invoke(FixedPartitioningTestBase.class,
          "putShipmentPartitionedRegion_Persistence1",
          new Object[] { "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "validateAfterPutPartitionedRegion", new Object[] { "Customer",
              "Order", "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 10, 5, "Customer",
              "Order", "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 10, 5, "Customer",
              "Order", "Shipment" });

      member2.invoke(FixedPartitioningTestBase.class, "closeCache");
      member1.invoke(FixedPartitioningTestBase.class,
          "putCustomerPartitionedRegion_Persistence2",
          new Object[] { "Customer" });
      member1.invoke(FixedPartitioningTestBase.class,
          "putOrderPartitionedRegion_Persistence2", new Object[] { "Order" });
      member1.invoke(FixedPartitioningTestBase.class,
          "putShipmentPartitionedRegion_Persistence2",
          new Object[] { "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "validateAfterPutPartitionedRegion", new Object[] { "Customer",
              "Order", "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 10, 10,
              "Customer", "Order", "Shipment" });

      member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      fpa1 = FixedPartitionAttributes.createFixedPartition("20", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      AsyncInvocation[] async = new AsyncInvocation[2];
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 1, 50, 20, new CustomerFixedPartitionResolver(), null,
              true });
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              1, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 1, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });

      Wait.pause(4000);
      member1.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 10, 5, "Customer",
              "Order", "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 10, 5, "Customer",
              "Order", "Shipment" });
      member1.invoke(FixedPartitioningTestBase.class,
          "validateAfterPutPartitionedRegion", new Object[] { "Customer",
              "Order", "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class,
          "validateAfterPutPartitionedRegion", new Object[] { "Customer",
              "Order", "Shipment" });

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

  public void testColocation_FPR_Persistence_Colocation() {
    try {
      member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");

      FixedPartitionAttributes fpa1 = FixedPartitionAttributes
          .createFixedPartition("10", true, 5);
      FixedPartitionAttributes fpa2 = FixedPartitionAttributes
          .createFixedPartition("20", false, 5);
      List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 1, 50, 20, new CustomerFixedPartitionResolver(), null,
              true });

      fpa1 = FixedPartitionAttributes.createFixedPartition("20", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 1, 50, 20, new CustomerFixedPartitionResolver(), null,
              true });

      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              1, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Order", null,
              1, 50, 20, new CustomerFixedPartitionResolver(), "Customer",
              false });

      member1.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 1, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Shipment",
              null, 1, 50, 20, new CustomerFixedPartitionResolver(), "Order",
              false });

      member1.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member1.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Order" });
      member2.invoke(FixedPartitioningTestBase.class, "checkFPR",
          new Object[] { "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "putCustomerPartitionedRegion_Persistence",
          new Object[] { "Customer" });
      member1.invoke(FixedPartitioningTestBase.class,
          "putOrderPartitionedRegion_Persistence", new Object[] { "Order" });
      member1.invoke(FixedPartitioningTestBase.class,
          "putShipmentPartitionedRegion_Persistence",
          new Object[] { "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "validateAfterPutPartitionedRegion", new Object[] { "Customer",
              "Order", "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 10, 5, "Customer",
              "Order", "Shipment" });
      member2.invoke(FixedPartitioningTestBase.class,
          "checkPrimaryBucketsForColocation", new Object[] { 10, 5, "Customer",
              "Order", "Shipment" });

      member1.invoke(FixedPartitioningTestBase.class, "closeCache");
      member2.invoke(FixedPartitioningTestBase.class, "closeCache");

      member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
      fpa1 = FixedPartitionAttributes.createFixedPartition("20", true, 5);
      fpa2 = FixedPartitionAttributes.createFixedPartition("10", false, 5);
      fpaList = new ArrayList<FixedPartitionAttributes>();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
      member2.invoke(FixedPartitioningTestBase.class,
          "createRegionWithPartitionAttributes", new Object[] { "Customer",
              fpaList, 1, 50, 20, new CustomerFixedPartitionResolver(), null,
              true });

      Wait.pause(4000);
      member2.invoke(FixedPartitioningTestBase.class, "getForColocation",
          new Object[] { "Customer", "Order", "Shipment" });

    }
    catch (Exception e) {
      Assert.fail("Unexpected Exception ", e);
    }
  }

  public void testFPR_Persistence2() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    FixedPartitionAttributes fpa2 = FixedPartitionAttributes
        .createFixedPartition(Quarter2, true, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true });

    member1.invoke(FixedPartitioningTestBase.class, "putThroughDataStore",
        new Object[] { "Quarter" });

    member1.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData", new Object[] { Quarter1, false });
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData", new Object[] { Quarter3, false });

    member1.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 6 });
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 6 });

    member1.invoke(FixedPartitioningTestBase.class, "closeCache");
    member2.invoke(FixedPartitioningTestBase.class, "closeCache");

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invokeAsync(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true });

    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invokeAsync(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 0, 40, 12, new QuarterPartitionResolver(), null, true });

    Wait.pause(4000);
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData", new Object[] { Quarter3, false });

    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 6 });

    member1.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData", new Object[] { Quarter1, false });
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData", new Object[] { Quarter3, false });

    member1.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 6 });
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 6 });
  }

  public void testFPR_Persistence3() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition(Quarter1, true, 3);
    FixedPartitionAttributes fpa2 = FixedPartitionAttributes
        .createFixedPartition(Quarter2, false, 3);
    List<FixedPartitionAttributes> fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true });

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member3.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true });

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member4.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true });

    member1.invoke(FixedPartitioningTestBase.class, "putThroughDataStore",
        new Object[] { "Quarter" });

    member1.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData", new Object[] { Quarter1, false });
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData", new Object[] { Quarter2, false });
    member3.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData", new Object[] { Quarter3, false });
    member4.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData", new Object[] { Quarter4, false });

    member1.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 3 });
    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 3 });
    member3.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 3 });
    member4.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 3 });

    member1.invoke(FixedPartitioningTestBase.class, "closeCache");
    member2.invoke(FixedPartitioningTestBase.class, "closeCache");
    member3.invoke(FixedPartitioningTestBase.class, "closeCache");
    member4.invoke(FixedPartitioningTestBase.class, "closeCache");

    member4.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter4, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter1, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member4.invokeAsync(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true });

    member3.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter3, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter4, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member3.invokeAsync(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter2, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter3, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member2.invokeAsync(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true });

    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter1, true, 3);
    fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter2, false, 3);
    fpaList = new ArrayList<FixedPartitionAttributes>();
    fpaList.add(fpa1);
    fpaList.add(fpa2);
    member1.invokeAsync(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter",
            fpaList, 1, 40, 12, new QuarterPartitionResolver(), null, true });

    Wait.pause(4000);
    member4.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData", new Object[] { Quarter4, false });

    member4.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 3 });

    member3.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData", new Object[] { Quarter3, false });

    member3.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 3 });

    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData", new Object[] { Quarter2, false });

    member2.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 3 });

    member1.invoke(FixedPartitioningTestBase.class,
        "checkPrimarySecondaryData", new Object[] { Quarter1, false });

    member1.invoke(FixedPartitioningTestBase.class,
        "checkPrimaryBucketsForQuarter", new Object[] { 6, 3 });

  }

  /**
   * Test validate a normal PR's persistence behavior. normal PR region is
   * created on member1 and member2. Put is done on this PR Member1 and Meber2's
   * cache is closed respectively. Member2 is brought back and persisted data is
   * verified.
   */
  public void testPR_Persistence() {
    member1.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    member1.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter", null,
            1, 40, 12, new QuarterPartitionResolver(), null, true });

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    member2.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter", null,
            1, 40, 12, new QuarterPartitionResolver(), null, true });

    member1.invoke(FixedPartitioningTestBase.class, "putThroughDataStore",
        new Object[] { "Quarter" });

    member1.invoke(FixedPartitioningTestBase.class, "closeCache");
    member2.invoke(FixedPartitioningTestBase.class, "closeCache");

    Wait.pause(1000);

    member2.invoke(FixedPartitioningTestBase.class, "createCacheOnMember");
    member2.invoke(FixedPartitioningTestBase.class,
        "createRegionWithPartitionAttributes", new Object[] { "Quarter", null,
            1, 40, 12, new QuarterPartitionResolver(), null, true });

    member2.invoke(FixedPartitioningTestBase.class, "getThroughDataStore",
        new Object[] { "Quarter" });
  }

}
