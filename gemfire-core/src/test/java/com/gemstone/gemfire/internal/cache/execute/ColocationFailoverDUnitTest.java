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
package com.gemstone.gemfire.internal.cache.execute;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.partitioned.RegionAdvisor;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class ColocationFailoverDUnitTest extends DistributedTestCase {

  private static final long serialVersionUID = 1L;

  protected static Cache cache = null;

  protected static VM dataStore1 = null;

  protected static VM dataStore2 = null;

  protected static VM dataStore3 = null;

  protected static VM dataStore4 = null;

  protected static Region customerPR = null;

  protected static Region orderPR = null;

  protected static Region shipmentPR = null;
  
  public static String customerPR_Name = "ColocationFailoverDUnitTest_CustomerPR" ;
  public static String orderPR_Name = "ColocationFailoverDUnitTest_OrderPR" ;
  public static String shipmentPR_Name = "ColocationFailoverDUnitTest_ShipmentPR" ;

  public ColocationFailoverDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
    dataStore2 = host.getVM(1);
    dataStore3 = host.getVM(2);
    dataStore4 = host.getVM(3);
  }

  public void testPrimaryColocationFailover() throws Throwable {    
    createCacheInAllVms();
    createCustomerPR();
    createOrderPR();
    createShipmentPR();
    putInPRs();
    verifyColocationInAllVms();
    dataStore1.invoke(ColocationFailoverDUnitTest.class, "closeCache");
    verifyPrimaryColocationAfterFailover();
  }
  
  public void testColocationFailover() throws Throwable {
    createCacheInAllVms();
    createCustomerPR();
    createOrderPR();
    createShipmentPR();
    putInPRs();
    verifyColocationInAllVms();
    dataStore1.invoke(ColocationFailoverDUnitTest.class, "closeCache");
    pause(5000); //wait for volunteering primary
    verifyColocationAfterFailover();
  }
  
  private void verifyColocationInAllVms() {
    verifyColocation();
    dataStore1.invoke(ColocationFailoverDUnitTest.class, "verifyColocation");
    dataStore2.invoke(ColocationFailoverDUnitTest.class, "verifyColocation");
    dataStore3.invoke(ColocationFailoverDUnitTest.class, "verifyColocation");
    dataStore4.invoke(ColocationFailoverDUnitTest.class, "verifyColocation");
  }

  private void verifyPrimaryColocationAfterFailover() {
    verifyPrimaryColocation();
    dataStore2.invoke(ColocationFailoverDUnitTest.class,
        "verifyPrimaryColocation");
    dataStore3.invoke(ColocationFailoverDUnitTest.class,
        "verifyPrimaryColocation");
    dataStore4.invoke(ColocationFailoverDUnitTest.class,
        "verifyPrimaryColocation");
  }

  private void verifyColocationAfterFailover() {
    verifyColocation();
    dataStore2.invoke(ColocationFailoverDUnitTest.class, "verifyColocation");
    dataStore3.invoke(ColocationFailoverDUnitTest.class, "verifyColocation");
    dataStore4.invoke(ColocationFailoverDUnitTest.class, "verifyColocation");
  }

  public static void closeCache() {
    if (cache != null) {
      cache.close();
    }
  }

  protected static boolean tryVerifyPrimaryColocation() {
   HashMap customerPrimaryMap = new HashMap();
    RegionAdvisor customeAdvisor = ((PartitionedRegion)customerPR)
        .getRegionAdvisor();
    Iterator customerIterator = customeAdvisor.getBucketSet().iterator();
    while (customerIterator.hasNext()) {
      Integer bucketId = (Integer)customerIterator.next();
      if (customeAdvisor.isPrimaryForBucket(bucketId.intValue())) {        
        customerPrimaryMap.put(bucketId, customeAdvisor
            .getPrimaryMemberForBucket(bucketId.intValue()).getId());
      }
    }
    HashMap orderPrimaryMap = new HashMap();
    RegionAdvisor orderAdvisor = ((PartitionedRegion)orderPR)
        .getRegionAdvisor();
    Iterator orderIterator = orderAdvisor.getBucketSet().iterator();
    while (orderIterator.hasNext()) {
      Integer bucketId = (Integer)orderIterator.next();
      if (orderAdvisor.isPrimaryForBucket(bucketId.intValue())) {
        orderPrimaryMap.put(bucketId, orderAdvisor.getPrimaryMemberForBucket(
            bucketId.intValue()).getId());
      }
    }
    HashMap shipmentPrimaryMap = new HashMap();
    RegionAdvisor shipmentAdvisor = ((PartitionedRegion)shipmentPR)
        .getRegionAdvisor();
    Iterator shipmentIterator = shipmentAdvisor.getBucketSet().iterator();
    while (shipmentIterator.hasNext()) {
      Integer bucketId = (Integer)shipmentIterator.next();
      if (shipmentAdvisor.isPrimaryForBucket(bucketId.intValue())) {
        shipmentPrimaryMap.put(bucketId, shipmentAdvisor
            .getPrimaryMemberForBucket(bucketId.intValue()).getId());
      }
    }
    // verification for primary
    int s1, s2;
    s1 = customerPrimaryMap.size();
    s2 = orderPrimaryMap.size();
    if (s1 != s2) {
      excuse = "customerPrimaryMap size (" + s1 + ") != orderPrimaryMap size (" + s2 + ")";
      return false;
    }
    if (!customerPrimaryMap.entrySet().equals(orderPrimaryMap.entrySet())) {
      excuse = "customerPrimaryMap entrySet != orderPrimaryMap entrySet";
      return false;
    }
    if (!customerPrimaryMap.entrySet().equals(shipmentPrimaryMap.entrySet())) {
      excuse = "customerPrimaryMap entrySet != shipmentPrimaryMap entrySet";
      return false;
    }
    if (!customerPrimaryMap.equals(orderPrimaryMap)) {
      excuse = "customerPrimaryMap != orderPrimaryMap";
      return false;
    }
    if (!customerPrimaryMap.equals(shipmentPrimaryMap)) {
      excuse = "customerPrimaryMap != shipmentPrimaryMap";
      return false;
    }
    return true;
  }

  private static void verifyPrimaryColocation() {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return tryVerifyPrimaryColocation();
      }
      public String description() {
        dump();
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
  }

  
  protected static void dump() {
    final InternalLogWriter logger = getLogWriter();
    ((PartitionedRegion)customerPR).dumpAllBuckets(false);
    ((PartitionedRegion)orderPR).dumpAllBuckets(false);
    ((PartitionedRegion)shipmentPR).dumpAllBuckets(false);
    for (int i = 0; i < 6; i++) {
      ((PartitionedRegion)customerPR).dumpB2NForBucket(i);
    }
    for (int i = 0; i < 6; i++) {
      ((PartitionedRegion)orderPR).dumpB2NForBucket(i);
    }
    for (int i = 0; i < 6; i++) {
      ((PartitionedRegion)shipmentPR).dumpB2NForBucket(i);
    }
  }

  protected static String excuse;
  
  /**
   * @return true if verified
   */
  protected static boolean tryVerifyColocation() {
    HashMap customerMap = new HashMap();
    HashMap customerPrimaryMap = new HashMap();
    RegionAdvisor customeAdvisor = ((PartitionedRegion)customerPR)
        .getRegionAdvisor();
    Iterator customerIterator = customeAdvisor.getBucketSet().iterator();
    while (customerIterator.hasNext()) {
      Integer bucketId = (Integer)customerIterator.next();
      Set someOwners = customeAdvisor.getBucketOwners(bucketId.intValue());
      customerMap.put(bucketId, someOwners);
      if (customeAdvisor.isPrimaryForBucket(bucketId.intValue())) {
        customerPrimaryMap.put(bucketId, customeAdvisor
            .getPrimaryMemberForBucket(bucketId.intValue()).getId());
      }
    }
    HashMap orderMap = new HashMap();
    HashMap orderPrimaryMap = new HashMap();
    RegionAdvisor orderAdvisor = ((PartitionedRegion)orderPR)
        .getRegionAdvisor();
    Iterator orderIterator = orderAdvisor.getBucketSet().iterator();
    while (orderIterator.hasNext()) {
      Integer bucketId = (Integer)orderIterator.next();
      Set someOwners = orderAdvisor.getBucketOwners(bucketId.intValue());
      orderMap.put(bucketId, someOwners);
      if (orderAdvisor.isPrimaryForBucket(bucketId.intValue())) {
        orderPrimaryMap.put(bucketId, orderAdvisor.getPrimaryMemberForBucket(
            bucketId.intValue()).getId());
      }
    }
    HashMap shipmentMap = new HashMap();
    HashMap shipmentPrimaryMap = new HashMap();
    RegionAdvisor shipmentAdvisor = ((PartitionedRegion)shipmentPR)
        .getRegionAdvisor();
    Iterator shipmentIterator = shipmentAdvisor.getBucketSet().iterator();
    while (shipmentIterator.hasNext()) {
      Integer bucketId = (Integer)shipmentIterator.next();
      Set someOwners = shipmentAdvisor.getBucketOwners(bucketId.intValue());
      shipmentMap.put(bucketId, someOwners);
      if (!customerMap.get(bucketId).equals(someOwners)) {
        excuse = "customerMap at " + bucketId + " has wrong owners";
        return false;
      }
      if (!orderMap.get(bucketId).equals(someOwners)) {
        excuse = "orderMap at " + bucketId + " has wrong owners";
        return false;
      }
      if (shipmentAdvisor.isPrimaryForBucket(bucketId.intValue())) {
        shipmentPrimaryMap.put(bucketId, shipmentAdvisor
            .getPrimaryMemberForBucket(bucketId.intValue()).getId());
      }
    }
    
    // verification for primary
    if (customerPrimaryMap.size() != orderPrimaryMap.size()) {
      excuse = "customerPrimaryMap and orderPrimaryMap have different sizes";
      return false;
    }
    if (customerPrimaryMap.size() != shipmentPrimaryMap.size()) {
      excuse = "customerPrimaryMap and shipmentPrimaryMap have different sizes";
      return false;
    }
    if (!customerPrimaryMap.entrySet().equals(orderPrimaryMap.entrySet())) {
      excuse = "customerPrimaryMap and orderPrimaryMap have different entrySets";
      return false;
    }
    if (!customerPrimaryMap.entrySet().equals(shipmentPrimaryMap.entrySet())) {
      excuse = "customerPrimaryMap and shipmentPrimaryMap have different entrySets";
      return false;
    }
    if (!customerPrimaryMap.equals(orderPrimaryMap)) {
      excuse = "customerPrimaryMap and orderPrimaryMap not equal";
      return false;
    }
    if (!customerPrimaryMap.equals(shipmentPrimaryMap)) {
      excuse = "customerPrimaryMap and shipmentPrimaryMap not equal";
      return false;
    }

    // verification for all
    if (customerMap.size() != orderMap.size()) {
      excuse = "customerMap and orderMap have different sizes";
      return false;
    }
    if (customerMap.size() != shipmentMap.size()) {
      excuse = "customerMap and shipmentMap have different sizes";
      return false;
    }
    if (!customerMap.entrySet().equals(orderMap.entrySet())) {
      excuse = "customerMap and orderMap have different entrySets";
      return false;
    }
    if (!customerMap.entrySet().equals(shipmentMap.entrySet())) {
      excuse = "customerMap and shipmentMap have different entrySets";
      return false;
    }
    if (!customerMap.equals(orderMap)) {
      excuse = "customerMap and orderMap not equal";
      return false;
    }
    if (!customerMap.equals(shipmentMap)) {
      excuse = "customerMap and shipmentMap not equal";
      return false;
    }

    return true;
  }
   
  private static void verifyColocation() {
    // TODO does having this WaitCriterion help?
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return tryVerifyColocation();
      }
      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 2 * 60 * 1000, 1000, true);
  }

  public static void createCacheInAllVms() {
    createCacheInVm();
    dataStore1.invoke(ColocationFailoverDUnitTest.class, "createCacheInVm");
    dataStore2.invoke(ColocationFailoverDUnitTest.class, "createCacheInVm");
    dataStore3.invoke(ColocationFailoverDUnitTest.class, "createCacheInVm");
    dataStore4.invoke(ColocationFailoverDUnitTest.class, "createCacheInVm");
  }

  public static void createCacheInVm() {
    new ColocationFailoverDUnitTest("temp").createCache();
  }

  public void createCache() {
    try {
      Properties props = new Properties();
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

  private static void createCustomerPR() {
    Object args[] = new Object[] { customerPR_Name, new Integer(1),
        new Integer(50), new Integer(6), null };
    createPR(customerPR_Name, new Integer(1), new Integer(50), new Integer(6),
        null);
    dataStore1.invoke(ColocationFailoverDUnitTest.class, "createPR", args);
    dataStore2.invoke(ColocationFailoverDUnitTest.class, "createPR", args);
    dataStore3.invoke(ColocationFailoverDUnitTest.class, "createPR", args);
    dataStore4.invoke(ColocationFailoverDUnitTest.class, "createPR", args);
  }

  private static void createOrderPR() {
    Object args[] = new Object[] { orderPR_Name, new Integer(1), new Integer(50),
        new Integer(6), customerPR_Name};
    createPR(orderPR_Name, new Integer(1), new Integer(50), new Integer(6),
        customerPR_Name);
    dataStore1.invoke(ColocationFailoverDUnitTest.class, "createPR", args);
    dataStore2.invoke(ColocationFailoverDUnitTest.class, "createPR", args);
    dataStore3.invoke(ColocationFailoverDUnitTest.class, "createPR", args);
    dataStore4.invoke(ColocationFailoverDUnitTest.class, "createPR", args);
  }

  private static void createShipmentPR() {
    Object args[] = new Object[] { shipmentPR_Name, new Integer(1),
        new Integer(50), new Integer(6), orderPR_Name };
    createPR(shipmentPR_Name, new Integer(1), new Integer(50), new Integer(6),
        orderPR_Name);
    dataStore1.invoke(ColocationFailoverDUnitTest.class, "createPR", args);
    dataStore2.invoke(ColocationFailoverDUnitTest.class, "createPR", args);
    dataStore3.invoke(ColocationFailoverDUnitTest.class, "createPR", args);
    dataStore4.invoke(ColocationFailoverDUnitTest.class, "createPR", args);
  }

  public static void createPR(String partitionedRegionName, Integer redundancy,
      Integer localMaxMemory, Integer totalNumBuckets, String colocatedWith) {

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes prAttr = paf.setRedundantCopies(redundancy.intValue())
        .setLocalMaxMemory(localMaxMemory.intValue()).setTotalNumBuckets(
            totalNumBuckets.intValue()).setColocatedWith(colocatedWith)
        .setPartitionResolver(new KeyPartitionResolver()).create();
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(prAttr);
    assertNotNull(cache);

    if (partitionedRegionName.equals(customerPR_Name)) {
      customerPR = cache.createRegion(partitionedRegionName, attr.create());
      assertNotNull(customerPR);
      getLogWriter().info(
          "Partitioned Region " + partitionedRegionName
              + " created Successfully :" + customerPR);

    }
    if (partitionedRegionName.equals(orderPR_Name)) {
      orderPR = cache.createRegion(partitionedRegionName, attr.create());
      assertNotNull(orderPR);
      getLogWriter().info(
          "Partitioned Region " + partitionedRegionName
              + " created Successfully :" + orderPR);

    }

    if (partitionedRegionName.equals(shipmentPR_Name)) {
      shipmentPR = cache.createRegion(partitionedRegionName, attr.create());
      assertNotNull(shipmentPR);
      getLogWriter().info(
          "Partitioned Region " + partitionedRegionName
              + " created Successfully :" + shipmentPR);

    }
  }

  private static void putInPRs() {
    put();
    dataStore1.invoke(ColocationFailoverDUnitTest.class, "put");
    dataStore2.invoke(ColocationFailoverDUnitTest.class, "put");
    dataStore3.invoke(ColocationFailoverDUnitTest.class, "put");
    dataStore4.invoke(ColocationFailoverDUnitTest.class, "put");
  }

  public static void put() {
    for (int i = 0; i < 20; i++) {
      customerPR.put("CPing--" + i, "CPong--" + i);
      orderPR.put("OPing--" + i, "OPong--" + i);
      shipmentPR.put("SPing--" + i, "SPong--" + i);
    }
  }

  public void tearDown2() throws Exception {
    closeCache();
    invokeInEveryVM(new SerializableRunnable() {
      public void run() {
        closeCache();
      }
    });
    super.tearDown2();
  }
}

class KeyPartitionResolver implements PartitionResolver {

  public KeyPartitionResolver() {
  }

  public String getName() {
    return this.getClass().getName();
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {
//    Serializable routingbject = null;
    String key = (String)opDetails.getKey();
    return new RoutingObject("" + key.charAt(key.length() - 1));
  }

  public void close() {
  }

  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof KeyPartitionResolver))
      return false;
    KeyPartitionResolver otherKeyPartitionResolver = (KeyPartitionResolver)o;
    return otherKeyPartitionResolver.getName().equals(getName());
  }
}

class RoutingObject implements DataSerializable {
  public RoutingObject(String value) {
    this.value = value;
  }

  private String value;

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.value = DataSerializer.readString(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.value, out);
  }

  public int hashCode() {
    return Integer.parseInt(this.value);
  }
}
