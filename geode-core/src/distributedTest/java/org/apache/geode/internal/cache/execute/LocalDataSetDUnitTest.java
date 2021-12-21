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
package org.apache.geode.internal.cache.execute;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.functions.LocalDataSetFunction;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.FunctionServiceTest;

@Category({FunctionServiceTest.class})
public class LocalDataSetDUnitTest extends JUnit4CacheTestCase {

  private static final long serialVersionUID = 1L;

  protected static VM dataStore1 = null;

  protected static VM dataStore2 = null;

  protected static VM dataStore3 = null;

  protected static VM accessor = null;

  protected static Region customerPR = null;

  protected static Region orderPR = null;

  protected static Region shipmentPR = null;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
    dataStore2 = host.getVM(1);
    dataStore3 = host.getVM(2);
    accessor = host.getVM(3);
  }

  @Test
  public void testLocalDataSet() {
    createCacheInAllVms();
    createCustomerPR();
    createOrderPR();
    createShipmentPR();
    putInPRs();
    registerFunctions();
    executeFunctions();
  }

  @Test
  public void testLocalDataSetIteration() {
    createCacheInAllVms();
    createCustomerPR();
    createOrderPR();
    createShipmentPR();
    putInPRs();
    registerIteratorFunctionOnAll();

    SerializableCallable installHook = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion) basicGetCache().getRegion("CustomerPR");
        Runnable r = new ReadHook();
        pr.getDataStore().setBucketReadHook(r);
        return null;
      }
    };
    invokeInAllDataStores(installHook);
    accessor.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region region = basicGetCache().getRegion("CustomerPR");
        Set filter = new HashSet();
        filter.add("1");
        FunctionService.onRegion(region).withFilter(filter).execute(IterateFunction.id).getResult();
        return null;
      }
    });

    SerializableCallable bucketRead = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return getHookInvoked();
      }
    };
    Integer ds1 = (Integer) dataStore1.invoke(bucketRead);
    Integer ds2 = (Integer) dataStore2.invoke(bucketRead);
    Integer ds3 = (Integer) dataStore3.invoke(bucketRead);
    assertEquals(1, ds1 + ds2 + ds3);
  }

  private void invokeInAllDataStores(SerializableCallable installHook) {
    dataStore1.invoke(installHook);
    dataStore2.invoke(installHook);
    dataStore3.invoke(installHook);
  }

  protected static class IterateFunction implements Function {
    public static final String id = "IteratorFunction";

    @Override
    public void execute(FunctionContext context) {
      Region localRegion =
          PartitionRegionHelper.getLocalDataForContext((RegionFunctionContext) context);
      Iterator it = localRegion.keySet().iterator();
      while (it.hasNext()) {
        LogWriterUtils.getLogWriter().info("LocalKeys:" + it.next());
      }
      context.getResultSender().lastResult(Boolean.TRUE);
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean optimizeForWrite() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

  static volatile boolean invoked = false;

  public static void setHookInvoked() {
    invoked = true;
  }

  public Integer getHookInvoked() {
    if (invoked) {
      return Integer.valueOf(1);
    }
    return Integer.valueOf(0);
  }

  protected static class ReadHook implements Runnable {

    @Override
    public void run() {
      System.out.println("SWAP:invokedHook");
      setHookInvoked();
    }
  }

  private void executeFunctions() {
    dataStore1.invoke(() -> LocalDataSetDUnitTest.executeFunction());

  }

  public static void executeFunction() {
    try {
      FunctionService.onRegion(customerPR).execute("LocalDataSetFunction" + true).getResult();
      FunctionService.onRegion(customerPR).execute("LocalDataSetFunction" + false).getResult();
      Set<String> filter = new HashSet<>();
      filter.add("YOYO-CUST-KEY-" + 0);
      FunctionService.onRegion(customerPR).withFilter(filter).execute("LocalDataSetFunction" + true)
          .getResult();
      FunctionService.onRegion(customerPR).withFilter(filter)
          .execute("LocalDataSetFunction" + false).getResult();
      filter.clear();
      for (int i = 0; i < 6; i++) {
        filter.add("YOYO-CUST-KEY-" + i);
      }
      FunctionService.onRegion(customerPR).withFilter(filter).execute("LocalDataSetFunction" + true)
          .getResult();
      FunctionService.onRegion(customerPR).withFilter(filter)
          .execute("LocalDataSetFunction" + false).getResult();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Test failed due to ", e);
    }
  }

  private void registerFunctions() {
    dataStore1.invoke(() -> LocalDataSetDUnitTest.registerFunction());
    dataStore2.invoke(() -> LocalDataSetDUnitTest.registerFunction());
    dataStore3.invoke(() -> LocalDataSetDUnitTest.registerFunction());
  }

  public static void registerFunction() {
    Function function1 = new LocalDataSetFunction(false);
    Function function2 = new LocalDataSetFunction(true);
    FunctionService.registerFunction(function1);
    FunctionService.registerFunction(function2);
  }

  private void registerIteratorFunctionOnAll() {
    accessor.invoke(() -> LocalDataSetDUnitTest.registerIteratorFunction());
    dataStore1.invoke(() -> LocalDataSetDUnitTest.registerIteratorFunction());
    dataStore2.invoke(() -> LocalDataSetDUnitTest.registerIteratorFunction());
    dataStore3.invoke(() -> LocalDataSetDUnitTest.registerIteratorFunction());
  }

  public static void registerIteratorFunction() {
    Function function = new IterateFunction();
    FunctionService.registerFunction(function);
  }

  public static void createCacheInAllVms() {
    dataStore1.invoke(() -> LocalDataSetDUnitTest.createCacheInVm());
    dataStore2.invoke(() -> LocalDataSetDUnitTest.createCacheInVm());
    dataStore3.invoke(() -> LocalDataSetDUnitTest.createCacheInVm());
    accessor.invoke(() -> LocalDataSetDUnitTest.createCacheInVm());
  }

  public static void createCacheInVm() {
    new LocalDataSetDUnitTest().createCache();
  }

  public void createCache() {
    try {
      getCache();
      assertNotNull(basicGetCache());
    } catch (Exception e) {
      Assert.fail("Failed while creating the cache", e);
    }
  }

  private static void createCustomerPR() {
    Object[] args =
        new Object[] {"CustomerPR", new Integer(1), new Integer(0), new Integer(10), null};
    accessor.invoke(LocalDataSetDUnitTest.class, "createPR", args);
    args = new Object[] {"CustomerPR", new Integer(1), new Integer(50), new Integer(10), null};
    dataStore1.invoke(LocalDataSetDUnitTest.class, "createPR", args);
    dataStore2.invoke(LocalDataSetDUnitTest.class, "createPR", args);
    dataStore3.invoke(LocalDataSetDUnitTest.class, "createPR", args);

  }

  private static void createOrderPR() {
    Object[] args =
        new Object[] {"OrderPR", new Integer(1), new Integer(0), new Integer(10), "CustomerPR"};
    accessor.invoke(LocalDataSetDUnitTest.class, "createPR", args);
    args = new Object[] {"OrderPR", new Integer(1), new Integer(50), new Integer(10), "CustomerPR"};
    dataStore1.invoke(LocalDataSetDUnitTest.class, "createPR", args);
    dataStore2.invoke(LocalDataSetDUnitTest.class, "createPR", args);
    dataStore3.invoke(LocalDataSetDUnitTest.class, "createPR", args);
  }

  private static void createShipmentPR() {
    Object[] args =
        new Object[] {"ShipmentPR", new Integer(1), new Integer(0), new Integer(10), "OrderPR"};
    accessor.invoke(LocalDataSetDUnitTest.class, "createPR", args);
    args = new Object[] {"ShipmentPR", new Integer(1), new Integer(50), new Integer(10), "OrderPR"};
    dataStore1.invoke(LocalDataSetDUnitTest.class, "createPR", args);
    dataStore2.invoke(LocalDataSetDUnitTest.class, "createPR", args);
    dataStore3.invoke(LocalDataSetDUnitTest.class, "createPR", args);
  }

  public static void createPR(String partitionedRegionName, Integer redundancy,
      Integer localMaxMemory, Integer totalNumBuckets, String colocatedWith) {

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes prAttr = paf.setRedundantCopies(redundancy.intValue())
        .setLocalMaxMemory(localMaxMemory.intValue()).setTotalNumBuckets(totalNumBuckets.intValue())
        .setColocatedWith(colocatedWith).setPartitionResolver(new LDSPartitionResolver()).create();
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(prAttr);
    assertNotNull(basicGetCache());

    if (partitionedRegionName.equals("CustomerPR")) {
      customerPR = basicGetCache().createRegion(partitionedRegionName, attr.create());
      assertNotNull(customerPR);
      LogWriterUtils.getLogWriter().info(
          "Partitioned Region " + partitionedRegionName + " created Successfully :" + customerPR);

    }
    if (partitionedRegionName.equals("OrderPR")) {
      orderPR = basicGetCache().createRegion(partitionedRegionName, attr.create());
      assertNotNull(orderPR);
      LogWriterUtils.getLogWriter().info(
          "Partitioned Region " + partitionedRegionName + " created Successfully :" + orderPR);

    }

    if (partitionedRegionName.equals("ShipmentPR")) {
      shipmentPR = basicGetCache().createRegion(partitionedRegionName, attr.create());
      assertNotNull(shipmentPR);
      LogWriterUtils.getLogWriter().info(
          "Partitioned Region " + partitionedRegionName + " created Successfully :" + shipmentPR);

    }
  }

  private static void putInPRs() {
    accessor.invoke(() -> LocalDataSetDUnitTest.put());
  }

  public static void put() {
    for (int i = 0; i < 120; i++) {
      customerPR.put("YOYO-CUST-KEY-" + i, "YOYO-CUST-VAL-" + i);
      orderPR.put("YOYO-ORD-KEY-" + i, "YOYO-ORD-VAL-" + i);
      shipmentPR.put("YOYO-SHIP-KEY-" + i, "YOYO-SHIP-VAL-" + i);
    }
  }

}


class LDSPartitionResolver implements PartitionResolver {

  public LDSPartitionResolver() {}

  @Override
  public String getName() {
    return getClass().getName();
  }

  @Override
  public Serializable getRoutingObject(EntryOperation opDetails) {
    String key = (String) opDetails.getKey();
    return new LDSRoutingObject("" + key.charAt(key.length() - 1));
  }

  @Override
  public void close() {}

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LDSPartitionResolver)) {
      return false;
    }
    LDSPartitionResolver otherKeyPartitionResolver = (LDSPartitionResolver) o;
    return otherKeyPartitionResolver.getName().equals(getName());
  }
}


class LDSRoutingObject implements DataSerializable {
  public LDSRoutingObject(String value) {
    this.value = value;
  }

  private String value;

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    value = DataSerializer.readString(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(value, out);
  }

  public int hashCode() {
    return Integer.parseInt(value);
  }
}
