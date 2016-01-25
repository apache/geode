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

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.cache.ClientServerObserver;
import com.gemstone.gemfire.internal.cache.ClientServerObserverAdapter;
import com.gemstone.gemfire.internal.cache.ClientServerObserverHolder;
import com.gemstone.gemfire.internal.cache.FixedPartitionAttributesImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.Node;
import com.gemstone.gemfire.internal.cache.PRHARedundancyProvider;
import com.gemstone.gemfire.internal.cache.PartitionRegionConfig;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.cache.execute.data.CustId;
import com.gemstone.gemfire.internal.cache.execute.data.Customer;
import com.gemstone.gemfire.internal.cache.execute.data.Order;
import com.gemstone.gemfire.internal.cache.execute.data.OrderId;
import com.gemstone.gemfire.internal.cache.execute.data.Shipment;
import com.gemstone.gemfire.internal.cache.execute.data.ShipmentId;
import com.gemstone.gemfire.internal.cache.partitioned.PartitionedRegionObserver;
import com.gemstone.gemfire.internal.cache.partitioned.PartitionedRegionObserverAdapter;
import com.gemstone.gemfire.internal.cache.partitioned.PartitionedRegionObserverHolder;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;

/**
 * This is the base class to do operations
 */

public class FixedPartitioningTestBase extends DistributedTestCase {

  private static final long serialVersionUID = 1L;
  
  protected static String Quarter1 = "Q1";  
  protected static String Quarter2 = "Q2";
  protected static String Quarter3 = "Q3";
  protected static String Quarter4 = "Q4";
  
  protected static VM member1 = null;

  protected static VM member2 = null;

  protected static VM member3 = null;

  protected static VM member4 = null;

  static Cache cache = null;

  protected static PartitionedRegion region_FPR = null;

  protected static int redundantCopies;
  
  private static PartitionedRegionObserver origObserver;

  public enum Months_Accessor {
    JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP
  };

  public enum Months_DataStore {
    JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
  };
  
  public enum Q1_Months {
    JAN, FEB, MAR
  };

  public enum Q2_Months {
    APR, MAY, JUN
  };

  public enum Q3_Months {
    JUL, AUG, SEP
  };

  public enum Q4_Months {
    OCT, NOV, DEC
  };

  public FixedPartitioningTestBase(String name) {
    super(name);
  }

  public static void createCacheOnMember() {
    new FixedPartitioningTestBase("Temp").createCache();
  }

  public static void createCacheOnMember_DisableMovePrimary() {
    System.setProperty("gemfire.DISABLE_MOVE_PRIMARIES_ON_STARTUP", "true");
    new FixedPartitioningTestBase("Temp").createCache();
  }
  
  private void createCache() {
    try {
      Properties props = new Properties();
      cache = null;
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(props);
      cache = CacheFactory.create(ds);
      assertNotNull(cache);
    }
    catch (Exception e) {
      fail("Failed while creating the cache", e);
    }
  }

  public static void createRegionWithPartitionAttributes(String regionName,
      List<FixedPartitionAttributes> fpaList, Integer redCopies,
      Integer localMaxMemory, Integer totalNumBuckets,
      PartitionResolver resolver, String colocatedWith, boolean isPersistence) {
    PartitionAttributesFactory paf_FPR = new PartitionAttributesFactory();
    paf_FPR.setRedundantCopies(redCopies);
    paf_FPR.setLocalMaxMemory(localMaxMemory);
    paf_FPR.setTotalNumBuckets(totalNumBuckets);
    paf_FPR.setPartitionResolver(resolver);
    paf_FPR.setColocatedWith(colocatedWith);
    if (fpaList != null) {
      for (FixedPartitionAttributes fpa : fpaList) {
        paf_FPR.addFixedPartitionAttributes(fpa);
      }
    }
    AttributesFactory af_FPR = new AttributesFactory();
    af_FPR.setPartitionAttributes(paf_FPR.create());
    if (isPersistence) {
      DiskStore ds = cache.findDiskStore("disk");
      if(ds == null) {
        ds = cache.createDiskStoreFactory()
        .setDiskDirs(getDiskDirs()).create("disk");
      }
      af_FPR.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
      af_FPR.setDiskStoreName("disk");
    }
    region_FPR = (PartitionedRegion)cache.createRegion(regionName, af_FPR
        .create());
    assertNotNull(region_FPR);
  }

  public static void doRebalance(){
    ResourceManager manager =  cache.getResourceManager();
    RebalanceOperation operation = manager.createRebalanceFactory().start();
    try {
      RebalanceResults result = operation.getResults();
    } catch (InterruptedException e) {
      fail("Not expecting exception", e);
    }
    
  }
  
  public static File[] getDiskDirs() {
    return new File[] {getDiskDir()};
  }
  
  private static File getDiskDir() {
    int vmNum = VM.getCurrentVMNum();
    File dir = new File("diskDir", String.valueOf(vmNum)).getAbsoluteFile();
    dir.mkdirs();
    return dir;
  }
  
  public static void putThorughAccessor(String regionName) {
    region_FPR = (PartitionedRegion)cache.getRegion(regionName);
    assertNotNull(region_FPR);
    for (Months_Accessor month : Months_Accessor.values()) {
      for (int i = 1; i < 10; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        String value = month.toString() + i;
        region_FPR.put(date, value);
      }
    }
  }
  
  public static void putThorughAccessor_Immediate(String regionName) {
    region_FPR = (PartitionedRegion)cache.getRegion(regionName);
    assertNotNull(region_FPR);
      for (int i = 1; i < 10; i++) {
        for (Months_Accessor month : Months_Accessor.values()) {
          Date date = generateDate(i, month.toString(), "Date");
          String value = month.toString() + i;
          region_FPR.put(date, value);
        }
      }
  }

  public static void putThroughDataStore(String regionName) {
    region_FPR = (PartitionedRegion)cache.getRegion(regionName);
    assertNotNull(region_FPR);
    for (Months_DataStore month : Months_DataStore.values()) {
      for (int i = 1; i < 10; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        String value = month.toString() + i;
        region_FPR.put(date, value);
      }
    }
  }

  public static void putForQuarter(String regionName, String quarters) {
    region_FPR = (PartitionedRegion)cache.getRegion(regionName);
    assertNotNull(region_FPR);
    if(quarters.equals("Q1")){
      for (Q1_Months month : Q1_Months.values()) {
        for (int i = 1; i < 10; i++) {
          Date date = generateDate(i, month.toString(), "Date");
          String value = month.toString() + i;
          region_FPR.put(date, value);
        }
      } 
    }else if(quarters.equals("Q2")){
      for (Q2_Months month : Q2_Months.values()) {
        for (int i = 1; i < 10; i++) {
          Date date = generateDate(i, month.toString(), "Date");
          String value = month.toString() + i;
          region_FPR.put(date, value);
        }
      } 
    }else 
    if(quarters.equals("Q3")){
      for (Q3_Months month : Q3_Months.values()) {
        for (int i = 1; i < 10; i++) {
          Date date = generateDate(i, month.toString(), "Date");
          String value = month.toString() + i;
          region_FPR.put(date, value);
        }
      } 
    }else if(quarters.equals("Q4")){
      for (Q4_Months month : Q4_Months.values()) {
        for (int i = 1; i < 10; i++) {
          Date date = generateDate(i, month.toString(), "Date");
          String value = month.toString() + i;
          region_FPR.put(date, value);
        }
      } 
    }else{
      fail("Wrong Quarter");
    }
  }
  
  public static void getForQuarter(String regionName, String quarters) {
    region_FPR = (PartitionedRegion)cache.getRegion(regionName);
    assertNotNull(region_FPR);
    if (quarters.equals(Quarter1)) {
      for (Q1_Months month : Q1_Months.values()) {
        for (int i = 1; i < 10; i++) {
          Date date = generateDate(i, month.toString(), "Date");
          String value = month.toString() + i;
          assertEquals(value, region_FPR.get(date));
        }
      }
    }
    else if (quarters.equals(Quarter2)) {
      for (Q2_Months month : Q2_Months.values()) {
        for (int i = 1; i < 10; i++) {
          Date date = generateDate(i, month.toString(), "Date");
          String value = month.toString() + i;
          assertEquals(value, region_FPR.get(date));
        }
      }
    }
    else if (quarters.equals(Quarter3)) {
      for (Q3_Months month : Q3_Months.values()) {
        for (int i = 1; i < 10; i++) {
          Date date = generateDate(i, month.toString(), "Date");
          String value = month.toString() + i;
          assertEquals(value, region_FPR.get(date));
        }
      }
    }
    else if (quarters.equals(Quarter4)) {
      for (Q4_Months month : Q4_Months.values()) {
        for (int i = 1; i < 10; i++) {
          Date date = generateDate(i, month.toString(), "Date");
          String value = month.toString() + i;
          assertEquals(value, region_FPR.get(date));
        }
      }
    }
    else {
      fail("Wrong Quarter");
    }
  }
  
  public static void getForColocation(String customerRegion, String order, String shipment) {
    Region region_Cust = (PartitionedRegion)cache.getRegion(customerRegion);
    assertNotNull(region_Cust);
    for (int i = 1; i <= 20; i++) {
        CustId custid = new CustId(i);
        Customer customer = new Customer("name" + i, "Address" + i);
        try {
          assertEquals(customer, region_Cust.get(custid));
        }
        catch (Exception e) {
          fail(
              "getForColocation : failed while doing get operation in CustomerPartitionedRegion ",
              e);
        }
    }
    Region region_Ord = (PartitionedRegion)cache.getRegion(order);
    assertNull(region_Ord);
    Region region_Ship = (PartitionedRegion)cache.getRegion(shipment);
    assertNull(region_Ship);
  }
  
  public static void getThroughDataStore(String regionName){
    region_FPR = (PartitionedRegion)cache.getRegion(regionName);
    assertNotNull(region_FPR);
    for (Months_DataStore month : Months_DataStore.values()) {
      for (int i = 1; i < 10; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        String value = month.toString() + i;
        assertEquals(value,region_FPR.get(date));
      }
    }
  }
  
  public static void putThroughDataStore_NoResolver(String regionName){
    region_FPR = (PartitionedRegion)cache.getRegion(regionName);
    assertNotNull(region_FPR);
    for (Months_DataStore month : Months_DataStore.values()) {
      for (int i = 1; i < 10; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        String value = month.toString() + i;
        region_FPR.put(date, value);
      }
    }
  }
  
  public static void putThroughDataStore_CallBackWithResolver(String regionName){
    region_FPR = (PartitionedRegion)cache.getRegion(regionName);
    assertNotNull(region_FPR);
    for (Months_DataStore month : Months_DataStore.values()) {
      for (int i = 1; i < 10; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        Date callbackDate = generateDate(i, month.toString(), "MyDate3");
        String value = month.toString() + i;
        region_FPR.put(date, value, callbackDate);
      }
    }
  }
  
  public static void putThroughDataStore_FixedPartitionResolver_NoResolver(String regionName){
    region_FPR = (PartitionedRegion)cache.getRegion(regionName);
    assertNotNull(region_FPR);
    for (Months_DataStore month : Months_DataStore.values()) {
      for (int i = 1; i < 10; i++) {
        if (i % 2 == 1) {
          MyDate1 date = (MyDate1)generateDate(i, month.toString(), "MyDate1");
          String value = month.toString() + i;
          region_FPR.put(date, value);
        }
        else {
          Date date = generateDate(i, month.toString(), "Date");
          String value = month.toString() + i;
          region_FPR.put(date, value);
        }
        
      }
    }
  }
  
  public static void putThroughDataStore_FixedPartitionResolver_PartitionResolver(String regionName){
    region_FPR = (PartitionedRegion)cache.getRegion(regionName);
    assertNotNull(region_FPR);
    for (int i = 1; i < 10; i++) {
      for (Months_DataStore month : Months_DataStore.values()) {
        if (month.ordinal() % 2 == 1) {
          MyDate1 date = (MyDate1)generateDate(i, month.toString(), "MyDate1");
          String value = month.toString() + i;
          region_FPR.put(date, value);
        }
        else {
          MyDate2 date = (MyDate2)generateDate(i, month.toString(), "MyDate2");
          String value = month.toString() + i;
          region_FPR.put(date, value);
        }
      }
    }
  }
  
  
  public static void deleteOperation(String regionName) {
    region_FPR = (PartitionedRegion)cache.getRegion(regionName);
    assertNotNull(region_FPR);
    Date date = generateDate(1, "JAN", "Date");
    region_FPR.destroy(date);
  }
  
  public static void putOperation(String regionName){
    region_FPR = (PartitionedRegion)cache.getRegion(regionName);
    assertNotNull(region_FPR);
    Date date = generateDate(1, "JAN", "Date");
    region_FPR.put(date, "Jan1");
  }
  
  public static void putCustomerPartitionedRegion(String partitionedRegionName) {
    assertNotNull(cache);
    Region partitionedregion = cache.getRegion(Region.SEPARATOR
        + partitionedRegionName);
    assertNotNull(partitionedregion);

    for (int i = 1; i <= 40; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      try {
        partitionedregion.put(custid, customer);
      }
      catch (Exception e) {
        fail(
            "putCustomerPartitionedRegion : failed while doing put operation in CustomerPartitionedRegion ",
            e);
      }
      getLogWriter().info("Customer :- { " + custid + " : " + customer + " }");
    }
  }
  
  public static void putOrderPartitionedRegion(String partitionedRegionName) {
    assertNotNull(cache);
    Region partitionedregion = cache.getRegion(Region.SEPARATOR
        + partitionedRegionName);
    assertNotNull(partitionedregion);
    
    for (int i = 1; i <= 40; i++) {
      CustId custid = new CustId(i);
      for (int j = 1; j <= 10; j++) {
        int oid = (i * 10) + j;
        OrderId orderId = new OrderId(oid, custid);
        Order order = new Order("OREDR" + oid);
        try {
          partitionedregion.put(orderId, order);
        }
        catch (Exception e) {
          fail(
              "putOrderPartitionedRegion : failed while doing put operation in OrderPartitionedRegion ",
              e);
        }
        getLogWriter().info("Order :- { " + orderId + " : " + order + " }");
      }
    }
  }

  public static void putShipmentPartitionedRegion(String partitionedRegionName) {
    assertNotNull(cache);
    Region partitionedregion = cache.getRegion(Region.SEPARATOR
        + partitionedRegionName);
    assertNotNull(partitionedregion);
    for (int i = 1; i <= 40; i++) {
      CustId custid = new CustId(i);
      for (int j = 1; j <= 10; j++) {
        int oid = (i * 10) + j;
        OrderId orderId = new OrderId(oid, custid);
        for (int k = 1; k <= 10; k++) {
          int sid = (oid * 10) + k;
          ShipmentId shipmentId = new ShipmentId(sid, orderId);
          Shipment shipment = new Shipment("Shipment" + sid);
          try {
            partitionedregion.put(shipmentId, shipment);
          }
          catch (Exception e) {
            fail(
                "putShipmentPartitionedRegion : failed while doing put operation in ShipmentPartitionedRegion ",
                e);
          }
          getLogWriter().info(
              "Shipment :- { " + shipmentId + " : " + shipment + " }");
        }
      }
    }
  }
  
  public static void putCustomerPartitionedRegion_Persistence(String partitionedRegionName) {
    assertNotNull(cache);
    Region partitionedregion = cache.getRegion(Region.SEPARATOR
        + partitionedRegionName);
    assertNotNull(partitionedregion);

    for (int i = 1; i <= 20; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      try {
        partitionedregion.put(custid, customer);
      }
      catch (Exception e) {
        fail(
            "putCustomerPartitionedRegion : failed while doing put operation in CustomerPartitionedRegion ",
            e);
      }
      getLogWriter().info("Customer :- { " + custid + " : " + customer + " }");
    }
  }
  
  public static void putOrderPartitionedRegion_Persistence(String partitionedRegionName) {
    assertNotNull(cache);
    Region partitionedregion = cache.getRegion(Region.SEPARATOR
        + partitionedRegionName);
    assertNotNull(partitionedregion);
    
    for (int i = 1; i <= 20; i++) {
      CustId custid = new CustId(i);
      for (int j = 1; j <= 10; j++) {
        int oid = (i * 10) + j;
        OrderId orderId = new OrderId(oid, custid);
        Order order = new Order("OREDR" + oid);
        try {
          partitionedregion.put(orderId, order);
        }
        catch (Exception e) {
          fail(
              "putOrderPartitionedRegion : failed while doing put operation in OrderPartitionedRegion ",
              e);
        }
        getLogWriter().info("Order :- { " + orderId + " : " + order + " }");
      }
    }
  }
  
  public static void putShipmentPartitionedRegion_Persistence(String partitionedRegionName) {
    assertNotNull(cache);
    Region partitionedregion = cache.getRegion(Region.SEPARATOR
        + partitionedRegionName);
    assertNotNull(partitionedregion);
    for (int i = 1; i <= 20; i++) {
      CustId custid = new CustId(i);
      for (int j = 1; j <= 10; j++) {
        int oid = (i * 10) + j;
        OrderId orderId = new OrderId(oid, custid);
        for (int k = 1; k <= 10; k++) {
          int sid = (oid * 10) + k;
          ShipmentId shipmentId = new ShipmentId(sid, orderId);
          Shipment shipment = new Shipment("Shipment" + sid);
          try {
            partitionedregion.put(shipmentId, shipment);
          }
          catch (Exception e) {
            fail(
                "putShipmentPartitionedRegion : failed while doing put operation in ShipmentPartitionedRegion ",
                e);
          }
          getLogWriter().info(
              "Shipment :- { " + shipmentId + " : " + shipment + " }");
        }
      }
    }
  }
  
  public static void putCustomerPartitionedRegion_Persistence1(String partitionedRegionName) {
    assertNotNull(cache);
    Region partitionedregion = cache.getRegion(Region.SEPARATOR
        + partitionedRegionName);
    assertNotNull(partitionedregion);

    for (int i = 1; i <= 20; i++) {
      if (i % 2 == 0) {
        CustId custid = new CustId(i);
        Customer customer = new Customer("name" + i, "Address" + i);
        try {
          partitionedregion.put(custid, customer);
        }
        catch (Exception e) {
          fail(
              "putCustomerPartitionedRegion : failed while doing put operation in CustomerPartitionedRegion ",
              e);
        }
        getLogWriter()
            .info("Customer :- { " + custid + " : " + customer + " }");
      }
    }
  }
  
  public static void putOrderPartitionedRegion_Persistence1(String partitionedRegionName) {
    assertNotNull(cache);
    Region partitionedregion = cache.getRegion(Region.SEPARATOR
        + partitionedRegionName);
    assertNotNull(partitionedregion);
    
    for (int i = 1; i <= 20; i++) {
      if (i % 2 == 0) {
        CustId custid = new CustId(i);
        for (int j = 1; j <= 10; j++) {
          int oid = (i * 10) + j;
          OrderId orderId = new OrderId(oid, custid);
          Order order = new Order("OREDR" + oid);
          try {
            partitionedregion.put(orderId, order);
          }
          catch (Exception e) {
            fail(
                "putOrderPartitionedRegion : failed while doing put operation in OrderPartitionedRegion ",
                e);
          }
          getLogWriter().info("Order :- { " + orderId + " : " + order + " }");
        }
      }
    }
  }
  
  public static void putShipmentPartitionedRegion_Persistence1(String partitionedRegionName) {
    assertNotNull(cache);
    Region partitionedregion = cache.getRegion(Region.SEPARATOR
        + partitionedRegionName);
    assertNotNull(partitionedregion);
    for (int i = 1; i <= 20; i++) {
      if (i % 2 == 0) {
        CustId custid = new CustId(i);
        for (int j = 1; j <= 10; j++) {
          int oid = (i * 10) + j;
          OrderId orderId = new OrderId(oid, custid);
          for (int k = 1; k <= 10; k++) {
            int sid = (oid * 10) + k;
            ShipmentId shipmentId = new ShipmentId(sid, orderId);
            Shipment shipment = new Shipment("Shipment" + sid);
            try {
              partitionedregion.put(shipmentId, shipment);
            }
            catch (Exception e) {
              fail(
                  "putShipmentPartitionedRegion : failed while doing put operation in ShipmentPartitionedRegion ",
                  e);
            }
            getLogWriter().info(
                "Shipment :- { " + shipmentId + " : " + shipment + " }");
          }
        }
      }
    }
  }
  
  public static void putCustomerPartitionedRegion_Persistence2(String partitionedRegionName) {
    assertNotNull(cache);
    Region partitionedregion = cache.getRegion(Region.SEPARATOR
        + partitionedRegionName);
    assertNotNull(partitionedregion);

    for (int i = 1; i <= 20; i++) {
      if (i % 2 == 1) {
        CustId custid = new CustId(i);
        Customer customer = new Customer("name" + i, "Address" + i);
        try {
          partitionedregion.put(custid, customer);
        }
        catch (Exception e) {
          fail(
              "putCustomerPartitionedRegion : failed while doing put operation in CustomerPartitionedRegion ",
              e);
        }
        getLogWriter()
            .info("Customer :- { " + custid + " : " + customer + " }");
      }
    }
  }
  
  public static void putOrderPartitionedRegion_Persistence2(String partitionedRegionName) {
    assertNotNull(cache);
    Region partitionedregion = cache.getRegion(Region.SEPARATOR
        + partitionedRegionName);
    assertNotNull(partitionedregion);
    
    for (int i = 1; i <= 20; i++) {
      if (i % 2 == 1) {
        CustId custid = new CustId(i);
        for (int j = 1; j <= 10; j++) {
          int oid = (i * 10) + j;
          OrderId orderId = new OrderId(oid, custid);
          Order order = new Order("OREDR" + oid);
          try {
            partitionedregion.put(orderId, order);
          }
          catch (Exception e) {
            fail(
                "putOrderPartitionedRegion : failed while doing put operation in OrderPartitionedRegion ",
                e);
          }
          getLogWriter().info("Order :- { " + orderId + " : " + order + " }");
        }
      }
    }
  }
  
  public static void putShipmentPartitionedRegion_Persistence2(String partitionedRegionName) {
    assertNotNull(cache);
    Region partitionedregion = cache.getRegion(Region.SEPARATOR
        + partitionedRegionName);
    assertNotNull(partitionedregion);
    for (int i = 1; i <= 20; i++) {
      if (i % 2 == 1) {
        CustId custid = new CustId(i);
        for (int j = 1; j <= 10; j++) {
          int oid = (i * 10) + j;
          OrderId orderId = new OrderId(oid, custid);
          for (int k = 1; k <= 10; k++) {
            int sid = (oid * 10) + k;
            ShipmentId shipmentId = new ShipmentId(sid, orderId);
            Shipment shipment = new Shipment("Shipment" + sid);
            try {
              partitionedregion.put(shipmentId, shipment);
            }
            catch (Exception e) {
              fail(
                  "putShipmentPartitionedRegion : failed while doing put operation in ShipmentPartitionedRegion ",
                  e);
            }
            getLogWriter().info(
                "Shipment :- { " + shipmentId + " : " + shipment + " }");
          }
        }
      }
    }
  }
  
  public static void putHAData(String regionName) {
    region_FPR = (PartitionedRegion)cache.getRegion(regionName);
    assertNotNull(region_FPR);
    for (Months_DataStore month : Months_DataStore.values()) {
      for (int i = 10; i < 20; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        String value = month.toString() + i;
        region_FPR.put(date, value);
      }
    }
  }

  private static Date generateDate(int i, String month, String dateType) {
    String year = "2010";
    String day = null;
    if (i > 0 && i < 10) {
      day = "0" + i;
    }
    else {
      day = new Integer(i).toString();
    }
    String dateString = day + "-" + month + "-" + year;
    String DATE_FORMAT = "dd-MMM-yyyy";
    SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT, Locale.US);
    try {
      if (dateType == "Date") {
        return sdf.parse(dateString);
      }
      else if (dateType == "MyDate1") {
        return new MyDate1(sdf.parse(dateString).getTime());
      }
      else if (dateType == "MyDate2") {
        return new MyDate2(sdf.parse(dateString).getTime());
      }
      else if (dateType == "MyDate3") {
        return new MyDate3(sdf.parse(dateString).getTime());
      }
      else {
        return null;
      }
    }
    catch (ParseException e) {
      FixedPartitioningTestBase.fail("Exception Occured while parseing date", e);
    }
    return null;
  }
  
  public static void validateAfterPutPartitionedRegion(
      String customerPartitionedRegionName, String orderPartitionedRegionName,
      String shipmentPartitionedRegionName) throws ClassNotFoundException {

    assertNotNull(cache);
    PartitionedRegion customerPartitionedregion = null;
    PartitionedRegion orderPartitionedregion = null;
    PartitionedRegion shipmentPartitionedregion = null;
    try {
      customerPartitionedregion = (PartitionedRegion)cache
          .getRegion(Region.SEPARATOR + customerPartitionedRegionName);
      orderPartitionedregion = (PartitionedRegion)cache
          .getRegion(Region.SEPARATOR + orderPartitionedRegionName);
      shipmentPartitionedregion = (PartitionedRegion)cache
          .getRegion(Region.SEPARATOR + shipmentPartitionedRegionName);
    }
    catch (Exception e) {
      fail(
          "validateAfterPutPartitionedRegion : failed while getting the region",
          e);
    }
    assertNotNull(customerPartitionedregion);

    for (int i = 0; i < 10; i++) {
      InternalDistributedMember idmForCustomer = customerPartitionedregion
          .getBucketPrimary(i);
      InternalDistributedMember idmForOrder = orderPartitionedregion
          .getBucketPrimary(i);
      InternalDistributedMember idmForShipment = shipmentPartitionedregion
          .getBucketPrimary(i);

      // take all the keys from the shipment for each bucket
      Set customerKey = customerPartitionedregion.getBucketKeys(i);
      assertNotNull(customerKey);
      Iterator customerIterator = customerKey.iterator();
      while (customerIterator.hasNext()) {
        CustId custId = (CustId)customerIterator.next();
        assertNotNull(customerPartitionedregion.get(custId));
        Set orderKey = orderPartitionedregion.getBucketKeys(i);
        assertNotNull(orderKey);
        Iterator orderIterator = orderKey.iterator();
        while (orderIterator.hasNext()) {
          OrderId orderId = (OrderId)orderIterator.next();
          // assertNotNull(orderPartitionedregion.get(orderId));

          if (custId.equals(orderId.getCustId())) {
            getLogWriter().info(
                orderId + "belongs to node " + idmForCustomer + " "
                    + idmForOrder);
            assertEquals(idmForCustomer, idmForOrder);
          }
          Set shipmentKey = shipmentPartitionedregion.getBucketKeys(i);
          assertNotNull(shipmentKey);
          Iterator shipmentIterator = shipmentKey.iterator();
          while (shipmentIterator.hasNext()) {
            ShipmentId shipmentId = (ShipmentId)shipmentIterator.next();
            // assertNotNull(shipmentPartitionedregion.get(shipmentId));
            if (orderId.equals(shipmentId.getOrderId())) {
              getLogWriter().info(
                  shipmentId + "belongs to node " + idmForOrder + " "
                      + idmForShipment);
            }
          }
        }
      }
    }
  }

  public static void checkPrimaryData(String partitionName) {
    Region localRegion = PartitionRegionHelper.getLocalData(region_FPR);
    if (partitionName.equals(Quarter1)) {
      assertEquals(27, localRegion.size());
      assertTRUE_Q1(false);
      assertFALSE_Q2(false);
      assertFALSE_Q3(false);
      assertFALSE_Q4(false);
    }
    else if (partitionName.equals(Quarter2)) {
      assertEquals(27, localRegion.size());
      assertFALSE_Q1(false);
      assertTRUE_Q2(false);
      assertFALSE_Q3(false);
      assertFALSE_Q4(false);
    }
    else if (partitionName.equals(Quarter3)) {
      assertEquals(27, localRegion.size());
      assertFALSE_Q1(false);
      assertFALSE_Q2(false);
      assertTRUE_Q3(false);
      assertFALSE_Q4(false);
    }
    else if (partitionName.equals(Quarter4)) {
      assertEquals(27, localRegion.size());
      assertFALSE_Q1(false);
      assertFALSE_Q2(false);
      assertFALSE_Q3(false);
      assertTRUE_Q4(false);
    }
  }

  public static void checkPrimaryDataPersistence(String partitionName) {
    Region localRegion = PartitionRegionHelper.getLocalData(region_FPR);
    if (partitionName.equals(Quarter1)) {
      assertEquals(27*2, localRegion.size());
      assertTRUE_Q1(false);
      assertTRUE_Q2(false);
      assertFALSE_Q3(false);
      assertFALSE_Q4(false);
    }
    else if (partitionName.equals(Quarter2)) {
      assertEquals(27*2, localRegion.size());
      assertTRUE_Q1(false);
      assertTRUE_Q2(false);
      assertFALSE_Q3(false);
      assertFALSE_Q4(false);
    }
    else if (partitionName.equals(Quarter3)) {
      assertEquals(27, localRegion.size());
      assertFALSE_Q1(false);
      assertFALSE_Q2(false);
      assertTRUE_Q3(false);
      assertFALSE_Q4(false);
    }
    else if (partitionName.equals(Quarter4)) {
      assertEquals(27, localRegion.size());
      assertFALSE_Q1(false);
      assertFALSE_Q2(false);
      assertFALSE_Q3(false);
      assertTRUE_Q4(false);
    }
  }
  
  public static void checkPrimarySecondaryData(String partitionName,
      Boolean isHA) {
    Region localRegion = PartitionRegionHelper.getLocalData(region_FPR);
    if (partitionName.equals(Quarter1)) {
      assertTRUE_Q1(isHA.booleanValue());
      assertTRUE_Q2(isHA.booleanValue());
      assertFALSE_Q3(isHA.booleanValue());
      assertFALSE_Q4(isHA.booleanValue());
    }
    else if (partitionName.equals(Quarter2)) {
      assertFALSE_Q1(isHA.booleanValue());
      assertTRUE_Q2(isHA.booleanValue());
      assertTRUE_Q3(isHA.booleanValue());
      assertFALSE_Q4(isHA.booleanValue());
    }
    else if (partitionName.equals(Quarter3)) {
      assertFALSE_Q1(isHA.booleanValue());
      assertFALSE_Q2(isHA.booleanValue());
      assertTRUE_Q3(isHA.booleanValue());
      assertTRUE_Q4(isHA.booleanValue());
    }
    else if (partitionName.equals(Quarter4)) {
      assertTRUE_Q1(isHA.booleanValue());
      assertFALSE_Q2(isHA.booleanValue());
      assertFALSE_Q3(isHA.booleanValue());
      assertTRUE_Q4(isHA.booleanValue());
    }
  }

  public static void checkPrimarySecondaryData_TwoSecondaries(
      String partitionName, Boolean isHA) {
    Region localRegion = PartitionRegionHelper.getLocalData(region_FPR);
    if (partitionName.equals(Quarter1)) {
      assertTRUE_Q1(isHA.booleanValue());
      assertFALSE_Q2(isHA.booleanValue());
      assertTRUE_Q3(isHA.booleanValue());
      assertTRUE_Q4(isHA.booleanValue());
    }
    else if (partitionName.equals(Quarter2)) {
      assertFALSE_Q1(isHA.booleanValue());
      assertTRUE_Q2(isHA.booleanValue());
      assertTRUE_Q3(isHA.booleanValue());
      assertTRUE_Q4(isHA.booleanValue());
    }
    else if (partitionName.equals(Quarter3)) {
      assertTRUE_Q1(isHA.booleanValue());
      assertTRUE_Q2(isHA.booleanValue());
      assertTRUE_Q3(isHA.booleanValue());
      assertFALSE_Q4(isHA.booleanValue());
    }
    else if (partitionName.equals(Quarter4)) {
      assertTRUE_Q1(isHA.booleanValue());
      assertTRUE_Q2(isHA.booleanValue());
      assertFALSE_Q3(isHA.booleanValue());
      assertTRUE_Q4(isHA.booleanValue());
    }
  }

  public static void assertTRUE_Q1(boolean isHA) {
    int day = isHA ? 20 : 10;
    Region localRegion = PartitionRegionHelper.getLocalData(region_FPR);
    for (Q1_Months month : Q1_Months.values()) {
      for (int i = 1; i < day; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        assertTrue(localRegion.keySet().contains(date));
      }
    }
  }

  public static void assertTRUE_Q2(boolean isHA) {
    int day = isHA ? 20 : 10;
    Region localRegion = PartitionRegionHelper.getLocalData(region_FPR);
    for (Q2_Months month : Q2_Months.values()) {
      for (int i = 1; i < day; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        assertTrue(localRegion.keySet().contains(date));
      }
    }
  }

  public static void assertTRUE_Q3(boolean isHA) {
    int day = isHA ? 20 : 10;
    Region localRegion = PartitionRegionHelper.getLocalData(region_FPR);
    for (Q3_Months month : Q3_Months.values()) {
      for (int i = 1; i < day; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        assertTrue(localRegion.keySet().contains(date));
      }
    }
  }

  public static void assertTRUE_Q4(boolean isHA) {
    int day = isHA ? 20 : 10;
    Region localRegion = PartitionRegionHelper.getLocalData(region_FPR);
    for (Q4_Months month : Q4_Months.values()) {
      for (int i = 1; i < day; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        assertTrue(localRegion.keySet().contains(date));
      }
    }
  }

  public static void assertFALSE_Q1(boolean isHA) {
    int day = isHA ? 20 : 10;
    Region localRegion = PartitionRegionHelper.getLocalData(region_FPR);
    for (Q1_Months month : Q1_Months.values()) {
      for (int i = 1; i < day; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        assertFalse(localRegion.keySet().contains(date));
      }
    }
  }

  public static void assertFALSE_Q2(boolean isHA) {
    int day = isHA ? 20 : 10;
    Region localRegion = PartitionRegionHelper.getLocalData(region_FPR);
    for (Q2_Months month : Q2_Months.values()) {
      for (int i = 1; i < day; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        assertFalse(localRegion.keySet().contains(date));
      }
    }
  }

  public static void assertFALSE_Q3(boolean isHA) {
    int day = isHA ? 20 : 10;
    Region localRegion = PartitionRegionHelper.getLocalData(region_FPR);
    for (Q3_Months month : Q3_Months.values()) {
      for (int i = 1; i < day; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        assertFalse(localRegion.keySet().contains(date));
      }
    }
  }

  public static void assertFALSE_Q4(boolean isHA) {
    int day = isHA ? 20 : 10;
    Region localRegion = PartitionRegionHelper.getLocalData(region_FPR);
    for (Q4_Months month : Q4_Months.values()) {
      for (int i = 1; i < day; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        assertFalse(localRegion.keySet().contains(date));
      }
    }
  }

  public static void checkPrimaryBucketsForQuarter(Integer numBuckets,
      Integer primaryBuckets) {
    HashMap localBucket2RegionMap = (HashMap)region_FPR.getDataStore()
        .getSizeLocally();
    getLogWriter().info(
        "Size of the " + region_FPR + " in this VM :- "
            + localBucket2RegionMap.size() + "List of buckets : "
            + localBucket2RegionMap.keySet());
    assertEquals(numBuckets.intValue(), localBucket2RegionMap.size());
    getLogWriter().info(
        "Size of primary buckets the " + region_FPR + " in this VM :- "
            + region_FPR.getDataStore().getNumberOfPrimaryBucketsManaged());
    getLogWriter().info(
        "Lit of Primaries in this VM :- "
            + region_FPR.getDataStore().getAllLocalPrimaryBucketIds());
    
    assertEquals(primaryBuckets.intValue(), region_FPR.getDataStore()
        .getNumberOfPrimaryBucketsManaged());
  }
  
  public static void checkPrimaryBucketsForQuarterAfterCacheClosed(
      Integer numBuckets, Integer primaryBuckets) {
    HashMap localBucket2RegionMap = (HashMap)region_FPR.getDataStore()
        .getSizeLocally();
    getLogWriter().info(
        "Size of the " + region_FPR + " in this VM :- "
            + localBucket2RegionMap.size() + "List of buckets : "
            + localBucket2RegionMap.keySet());
    assertEquals(numBuckets.intValue(), localBucket2RegionMap.size());
    getLogWriter().info(
        "Size of primary buckets the " + region_FPR + " in this VM :- "
            + region_FPR.getDataStore().getNumberOfPrimaryBucketsManaged());
    getLogWriter().info(
        "Lit of Primaries in this VM :- "
            + region_FPR.getDataStore().getAllLocalPrimaryBucketIds());

    assertEquals(region_FPR.getDataStore().getNumberOfPrimaryBucketsManaged()
        % primaryBuckets.intValue(), 0);
  }
  
  public static void checkPrimaryBucketsForCustomer(Integer numBuckets,
      Integer primaryBuckets, String customerPartitionedRegionName) {
    PartitionedRegion customerPartitionedregion = null;
    try {
      customerPartitionedregion = (PartitionedRegion)cache
          .getRegion(Region.SEPARATOR + customerPartitionedRegionName);
    }
    catch (Exception e) {
      fail(
          "validateAfterPutPartitionedRegion : failed while getting the region",
          e);
    }
    HashMap localBucket2RegionMap_Customer = (HashMap)customerPartitionedregion
        .getDataStore().getSizeLocally();
    assertEquals(numBuckets.intValue(), localBucket2RegionMap_Customer.size());
    List primaryBuckets_Customer = customerPartitionedregion.getDataStore()
        .getLocalPrimaryBucketsListTestOnly();
    assertEquals(primaryBuckets.intValue(), primaryBuckets_Customer.size());
  }
  
  public static void checkPrimaryBucketsForColocation(Integer numBuckets,
      Integer primaryBuckets, String customerPartitionedRegionName,
      String orderPartitionedRegionName, String shipmentPartitionedRegionName) {
    assertNotNull(cache);
    PartitionedRegion customerPartitionedregion = null;
    PartitionedRegion orderPartitionedregion = null;
    PartitionedRegion shipmentPartitionedregion = null;
    try {
      customerPartitionedregion = (PartitionedRegion)cache
          .getRegion(Region.SEPARATOR + customerPartitionedRegionName);
      orderPartitionedregion = (PartitionedRegion)cache
          .getRegion(Region.SEPARATOR + orderPartitionedRegionName);
      shipmentPartitionedregion = (PartitionedRegion)cache
          .getRegion(Region.SEPARATOR + shipmentPartitionedRegionName);
    }
    catch (Exception e) {
      fail(
          "validateAfterPutPartitionedRegion : failed while getting the region",
          e);
    }
    HashMap localBucket2RegionMap_Customer = (HashMap)customerPartitionedregion
        .getDataStore().getSizeLocally();
    HashMap localBucket2RegionMap_Order = (HashMap)orderPartitionedregion
        .getDataStore().getSizeLocally();
    HashMap localBucket2RegionMap_Shipment = (HashMap)shipmentPartitionedregion
        .getDataStore().getSizeLocally();
    assertEquals(numBuckets.intValue(), localBucket2RegionMap_Customer.size());
    assertEquals(numBuckets.intValue(), localBucket2RegionMap_Order.size());
    assertEquals(numBuckets.intValue(), localBucket2RegionMap_Shipment.size());
    
    assertEquals(localBucket2RegionMap_Customer.keySet(), localBucket2RegionMap_Order.keySet());
    assertEquals(localBucket2RegionMap_Customer.keySet(), localBucket2RegionMap_Shipment.keySet());
    assertEquals(localBucket2RegionMap_Order.keySet(), localBucket2RegionMap_Shipment.keySet());
    
    List primaryBuckets_Customer = customerPartitionedregion
    .getDataStore().getLocalPrimaryBucketsListTestOnly();
    List primaryBuckets_Order = orderPartitionedregion
    .getDataStore().getLocalPrimaryBucketsListTestOnly();
    List primaryBuckets_Shipment = shipmentPartitionedregion
    .getDataStore().getLocalPrimaryBucketsListTestOnly();

    assertEquals(primaryBuckets.intValue(), primaryBuckets_Customer.size());
    assertEquals(primaryBuckets.intValue(), primaryBuckets_Order.size());
    assertEquals(primaryBuckets.intValue(), primaryBuckets_Shipment.size());
    
    assertEquals(primaryBuckets_Customer, primaryBuckets_Order);
    assertEquals(primaryBuckets_Customer, primaryBuckets_Shipment);
    assertEquals(primaryBuckets_Order, primaryBuckets_Shipment);
  }

  public static void checkPrimaryBucketsForColocationAfterCacheClosed(
      Integer numBuckets, Integer primaryBuckets,
      String customerPartitionedRegionName, String orderPartitionedRegionName,
      String shipmentPartitionedRegionName) {
    assertNotNull(cache);
    PartitionedRegion customerPartitionedregion = null;
    PartitionedRegion orderPartitionedregion = null;
    PartitionedRegion shipmentPartitionedregion = null;
    try {
      customerPartitionedregion = (PartitionedRegion)cache
          .getRegion(Region.SEPARATOR + customerPartitionedRegionName);
      orderPartitionedregion = (PartitionedRegion)cache
          .getRegion(Region.SEPARATOR + orderPartitionedRegionName);
      shipmentPartitionedregion = (PartitionedRegion)cache
          .getRegion(Region.SEPARATOR + shipmentPartitionedRegionName);
    }
    catch (Exception e) {
      fail(
          "validateAfterPutPartitionedRegion : failed while getting the region",
          e);
    }
    HashMap localBucket2RegionMap_Customer = (HashMap)customerPartitionedregion
        .getDataStore().getSizeLocally();
    HashMap localBucket2RegionMap_Order = (HashMap)orderPartitionedregion
        .getDataStore().getSizeLocally();
    HashMap localBucket2RegionMap_Shipment = (HashMap)shipmentPartitionedregion
        .getDataStore().getSizeLocally();
    assertEquals(numBuckets.intValue(), localBucket2RegionMap_Customer.size());
    assertEquals(numBuckets.intValue(), localBucket2RegionMap_Order.size());
    assertEquals(numBuckets.intValue(), localBucket2RegionMap_Shipment.size());

    assertEquals(localBucket2RegionMap_Customer.keySet(),
        localBucket2RegionMap_Order.keySet());
    assertEquals(localBucket2RegionMap_Customer.keySet(),
        localBucket2RegionMap_Shipment.keySet());
    assertEquals(localBucket2RegionMap_Order.keySet(),
        localBucket2RegionMap_Shipment.keySet());

    List primaryBuckets_Customer = customerPartitionedregion.getDataStore()
        .getLocalPrimaryBucketsListTestOnly();
    List primaryBuckets_Order = orderPartitionedregion.getDataStore()
        .getLocalPrimaryBucketsListTestOnly();
    List primaryBuckets_Shipment = shipmentPartitionedregion.getDataStore()
        .getLocalPrimaryBucketsListTestOnly();

    assertEquals(primaryBuckets_Customer.size() % primaryBuckets.intValue(), 0);
    assertEquals(primaryBuckets_Order.size() % primaryBuckets.intValue(), 0);
    assertEquals(primaryBuckets_Shipment.size() % primaryBuckets.intValue(), 0);

    assertEquals(primaryBuckets_Customer, primaryBuckets_Order);
    assertEquals(primaryBuckets_Customer, primaryBuckets_Shipment);
    assertEquals(primaryBuckets_Order, primaryBuckets_Shipment);
  }

  public static void checkFPR(String regionName) {
    region_FPR = (PartitionedRegion)cache.getRegion(regionName);
    PartitionedRegion colocatedRegion = (PartitionedRegion)cache
        .getRegion(region_FPR.getColocatedWith());
    List<FixedPartitionAttributesImpl> childFPAs = region_FPR
        .getFixedPartitionAttributesImpl();
    List<FixedPartitionAttributesImpl> parentFPAs = colocatedRegion
        .getFixedPartitionAttributesImpl();
    assertEquals(parentFPAs, childFPAs);
  }
  
  public static void checkStartingBucketIDs() {
    assertEquals(
        region_FPR.getDataStore().getAllLocalPrimaryBucketIds().size() % 3, 0);

  }

  public static void checkStartingBucketIDs_Nodedown() {
    assertEquals(
        region_FPR.getDataStore().getAllLocalPrimaryBucketIds().size() % 3, 0);
  }

  public static void checkStartingBucketIDs_Nodeup() {
    assertEquals(
        region_FPR.getDataStore().getAllLocalPrimaryBucketIds().size() % 3, 0);
  }

  
  public static void setPRObserverBeforeCalculateStartingBucketId() {
    PartitionedRegion.BEFORE_CALCULATE_STARTING_BUCKET_FLAG = true;
    origObserver = PartitionedRegionObserverHolder
        .setInstance(new PartitionedRegionObserverAdapter() {
          public void beforeCalculatingStartingBucketId() {
            WaitCriterion wc = new WaitCriterion() {
              String excuse;

              public boolean done() {
                Region prRoot = PartitionedRegionHelper.getPRRoot(cache);
                PartitionRegionConfig regionConfig = (PartitionRegionConfig)prRoot
                    .get("#Quarter");
                if (regionConfig == null) {
                  return false;
                }
                else {
                  if (!regionConfig.isFirstDataStoreCreated()) {
                    return true;
                  }
                  else {
                    return false;
                  }
                }
              }

              public String description() {
                return excuse;
              }
            };
            DistributedTestCase.waitForCriterion(wc, 20000, 500, false);
            getLogWriter().info("end of beforeCalculatingStartingBucketId");
          }
        });
  }
  
  public static void resetPRObserverBeforeCalculateStartingBucketId()
      throws Exception {
    PartitionedRegion.BEFORE_CALCULATE_STARTING_BUCKET_FLAG = false;
  }
  
  public void tearDown2() throws Exception {
    try {
      closeCache();
      member1.invoke(FixedPartitioningTestBase.class, "closeCache");
      member2.invoke(FixedPartitioningTestBase.class, "closeCache");
      member3.invoke(FixedPartitioningTestBase.class, "closeCache");
      member4.invoke(FixedPartitioningTestBase.class, "closeCache");
    }
    finally {
   // locally destroy all root regions and close the cache
      remoteTearDown();
      // Now invoke it in every VM
      for (int h = 0; h < Host.getHostCount(); h++) {
        Host host = Host.getHost(h);
        for (int v = 0; v < host.getVMCount(); v++) {
          VM vm = host.getVM(v);
          vm.invoke(FixedPartitioningTestBase.class, "remoteTearDown");
        }
      }
      super.tearDown2();
    }
  }

  /**
   * Local destroy all root regions and close the cache.  
   */
  protected synchronized static void remoteTearDown() {
    try {
      if (cache != null && !cache.isClosed()) {
        //try to destroy the root regions first so that
        //we clean up any persistent files.
        for (Iterator itr = cache.rootRegions().iterator(); itr.hasNext();) {
          Region root = (Region)itr.next();
//          String name = root.getName();
          //for colocated regions you can't locally destroy a partitioned
          //region.
          if(root.isDestroyed() || root instanceof HARegion || root instanceof PartitionedRegion) {
            continue;
          }
          try {
            root.localDestroyRegion("teardown");
          }
          catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          }
          catch (Throwable t) {
            getLogWriter().error(t);
          }
        }
      }
    }
    finally {
      try {
        closeCache();
      }
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Throwable t) {
        getLogWriter().error("Error in closing the cache ", t);
        
      }
    }

    try {
      cleanDiskDirs();
    } catch(IOException e) {
      getLogWriter().error("Error cleaning disk dirs", e);
    }
  }
  
  public static void cleanDiskDirs() throws IOException {
    FileUtil.delete(getDiskDir());
  }
  
  public static void closeCache() {
    System.clearProperty("gemfire.DISABLE_MOVE_PRIMARIES_ON_STARTUP");
    //System.setProperty("gemfire.DISABLE_MOVE_PRIMARIES_ON_STARTUP", "false");
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }


}
