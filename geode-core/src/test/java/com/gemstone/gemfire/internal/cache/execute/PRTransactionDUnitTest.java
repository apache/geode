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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import util.TestException;

import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.TransactionDataNotColocatedException;
import com.gemstone.gemfire.cache.TransactionDataRebalancedException;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.execute.data.CustId;
import com.gemstone.gemfire.internal.cache.execute.data.Customer;
import com.gemstone.gemfire.internal.cache.execute.data.Order;
import com.gemstone.gemfire.internal.cache.execute.data.OrderId;
import com.gemstone.gemfire.internal.cache.execute.data.Shipment;
import com.gemstone.gemfire.internal.cache.execute.data.ShipmentId;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;

/**
 * Test for co-located PR transactions.
 * the test creates two data hosts and uses function execution to 
 * execute transactions.  
 * @author sbawaska
 *
 */
public class PRTransactionDUnitTest extends PRColocationDUnitTest {

  public static final int VERIFY_TX = 0;

  public static final int VERIFY_ROLLBACK = 1;

  public static final int VERIFY_NON_COLOCATION = 2;

  public static final int VERIFY_DESTROY = 4;

  public static final int VERIFY_LISTENER_CALLBACK = 5;
  
  public static final int VERIFY_INVALIDATE = 6;
  
  public static final int VERIFY_TXSTATE_CONFLICT = 7;
  
  public static final int VERIFY_REP_READ = 8;

  final int totalIterations = 50;

  final int warmupIterations = 10;

  final int perfOrderShipmentPairs = 1;

  public PRTransactionDUnitTest(String name) {
    super(name);
  }

  public void testBasicPRTransactionRedundancy0() {
    basicPRTXInFunction(0, true);
  }

  public void testBasicPRTransactionRedundancy1() {
    basicPRTXInFunction(1, true);
  }

  public void testBasicPRTransactionRedundancy2() {
    basicPRTXInFunction(2, true);
  }

  public void testBasicPRTransactionNoDataRedundancy0() {
    basicPRTXInFunction(0, false);
  }

  public void testBasicPRTransactionNoDataRedundancy1() {
    basicPRTXInFunction(1, false);
  }

  public void testBasicPRTransactionNoDataRedundancy2() {
    basicPRTXInFunction(2, false);
  }

  /**
   * Test all the basic functionality of colocated transactions.
   * This method invokes {@link MyTransactionFunction} and tells it what to
   * test, using different arguments.
   * @param redundantBuckets redundant buckets for colocated PRs
   * @param populateData if false tests are carried out on empty colocated PRs
   */
  protected void basicPRTXInFunction(int redundantBuckets, boolean populateData) {

    if (populateData) {
      createPopulateAndVerifyCoLocatedPRs(redundantBuckets);
    }
    else {
      createColocatedPRs(redundantBuckets);
    }

    SerializableCallable registerFunction = new SerializableCallable(
        "register Fn") {
      public Object call() throws Exception {
        Function txFunction = new MyTransactionFunction();
        FunctionService.registerFunction(txFunction);
        return Boolean.TRUE;
      }
    };

    dataStore1.invoke(registerFunction);
    dataStore2.invoke(registerFunction);
    dataStore3.invoke(registerFunction);

    accessor.invoke(new SerializableCallable("run function") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)basicGetCache()
            .getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
        PartitionedRegion orderpr = (PartitionedRegion)basicGetCache()
            .getRegion(Region.SEPARATOR + OrderPartitionedRegionName);
        CustId custId = new CustId(2);
        Customer newCus = new Customer("foo", "bar");
        Order order = new Order("fooOrder");
        OrderId orderId = new OrderId(22, custId);
        ArrayList args = new ArrayList();
        Function txFunction = new MyTransactionFunction();
        FunctionService.registerFunction(txFunction);
        Execution e = FunctionService.onRegion(pr);
        Set filter = new HashSet();
        // test transaction non-coLocated operations
        filter.clear();
        args.clear();
        args.add(new Integer(VERIFY_NON_COLOCATION));
        LogWriterUtils.getLogWriter().info("VERIFY_NON_COLOCATION");
        args.add(custId);
        args.add(newCus);
        args.add(orderId);
        args.add(order);
        filter.add(custId);
        try {
          e.withFilter(filter).withArgs(args).execute(txFunction.getId())
              .getResult();
          fail("Expected exception was not thrown");
        }
        catch (FunctionException fe) {
          LogWriterUtils.getLogWriter().info("Caught Expected exception");
          if(fe.getCause() instanceof TransactionDataNotColocatedException) {
          }
          else {
            throw new TestException(
                "Expected to catch FunctionException with cause TransactionDataNotColocatedException"
                    + " but cause is  "+fe.getCause(),fe.getCause());
          }
        }
        // verify that the transaction modifications are applied        
        args.set(0, new Integer(VERIFY_TX));
        LogWriterUtils.getLogWriter().info("VERIFY_TX");        
        orderpr.put(orderId, order);
        assertNotNull(orderpr.get(orderId));
        e.withFilter(filter).withArgs(args).execute(txFunction.getId())
            .getResult();
        assertTrue("Unexpected customer value after commit", newCus.equals(pr
            .get(custId)));
        Order commitedOrder = (Order)orderpr.get(orderId);
        assertTrue("Unexpected order value after commit. Expected:" + order
            + " Found:" + commitedOrder, order.equals(commitedOrder));
        //verify conflict detection
        args.set(0, new Integer(VERIFY_TXSTATE_CONFLICT));
        e.withFilter(filter).withArgs(args).execute(txFunction.getId())
            .getResult();
        // verify that the transaction is rolled back        
        args.set(0, new Integer(VERIFY_ROLLBACK));
        LogWriterUtils.getLogWriter().info("VERIFY_ROLLBACK");        
        e.withFilter(filter).withArgs(args).execute(txFunction.getId())
            .getResult();
        // verify destroy
        args.set(0, new Integer(VERIFY_DESTROY));
        LogWriterUtils.getLogWriter().info("VERIFY_DESTROY");
        e.withFilter(filter).withArgs(args).execute(txFunction.getId())
            .getResult();
        // verify invalidate
        args.set(0, new Integer(VERIFY_INVALIDATE));
        LogWriterUtils.getLogWriter().info("VERIFY_INVALIDATE");
        e.withFilter(filter).withArgs(args).execute(txFunction.getId())
            .getResult();
        return Boolean.TRUE;
      }
    });
  }

  protected void createColocatedPRs(int redundantBuckets) {

    createCacheInAllVms();

    redundancy = new Integer(redundantBuckets);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);

    // Create Customer PartitionedRegion in All VMs
    createPRWithCoLocation(CustomerPartitionedRegionName, null);

    // Create Order PartitionedRegion in All VMs
    createPRWithCoLocation(OrderPartitionedRegionName,
        CustomerPartitionedRegionName);

    // Create Shipment PartitionedRegion in All VMs
    createPRWithCoLocation(ShipmentPartitionedRegionName,
        OrderPartitionedRegionName);

    // Initial Validation for the number of data stores and number of profiles
    accessor.invoke(PRColocationDUnitTest.class,
        "validateBeforePutCustomerPartitionedRegion",
        new Object[] { CustomerPartitionedRegionName });
  }

  @Override
  protected void createCacheInAllVms() {    
    dataStore1.invoke(runGetCache);
    dataStore2.invoke(runGetCache);
    dataStore3.invoke(runGetCache);
    accessor.invoke(runGetCache);
  }
  
  private SerializableCallable runGetCache = new SerializableCallable("runGetCache") {
    public Object call() throws Exception {
      getCache();
      return null;
    }
  };

  protected void populateAndVerifyColocatedPRs(int redundantBuckets) {
    // Put the customer 1-10 in CustomerPartitionedRegion
    accessor.invoke(PRColocationDUnitTest.class,
        "putCustomerPartitionedRegion",
        new Object[] { CustomerPartitionedRegionName });

    // Put the order 1-10 for each Customer in OrderPartitionedRegion
    accessor.invoke(PRColocationDUnitTest.class, "putOrderPartitionedRegion",
        new Object[] { OrderPartitionedRegionName });

    // Put the shipment 1-10 for each order in ShipmentPartitionedRegion
    accessor.invoke(PRColocationDUnitTest.class,
        "putShipmentPartitionedRegion",
        new Object[] { ShipmentPartitionedRegionName });

    // for VM0 DataStore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore1 = (Integer)dataStore1.invoke(
        PRColocationDUnitTest.class, "validateDataStore", new Object[] {
            CustomerPartitionedRegionName, OrderPartitionedRegionName,
            ShipmentPartitionedRegionName });

    // for VM1 DataStore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore2 = (Integer)dataStore2.invoke(
        PRColocationDUnitTest.class, "validateDataStore", new Object[] {
            CustomerPartitionedRegionName, OrderPartitionedRegionName,
            ShipmentPartitionedRegionName });

    // for VM3 Datastore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore3 = (Integer)dataStore3.invoke(
        PRColocationDUnitTest.class, "validateDataStore", new Object[] {
            CustomerPartitionedRegionName, OrderPartitionedRegionName,
            ShipmentPartitionedRegionName });

    // Check the total number of buckets created in all three Vms are equalto 30
    totalNumBucketsInTest = totalBucketsInDataStore1.intValue()
        + totalBucketsInDataStore2.intValue()
        + totalBucketsInDataStore3.intValue();
    assertEquals(30 + (redundantBuckets * 30), totalNumBucketsInTest);

    // This is the importatnt check. Checks that the colocated Customer,Order
    // and Shipment are in the same VM

    accessor.invoke(PRColocationDUnitTest.class,
        "validateAfterPutPartitionedRegion", new Object[] {
            CustomerPartitionedRegionName, OrderPartitionedRegionName,
            ShipmentPartitionedRegionName });

  }

  protected void createPopulateAndVerifyCoLocatedPRs(int redundantBuckets) {

    createColocatedPRs(redundantBuckets);
    populateAndVerifyColocatedPRs(redundantBuckets);
  }

  protected void createPRWithCoLocation(String prName, String coLocatedWith) {
    this.regionName = prName;
    this.colocatedWith = coLocatedWith;
    this.isPartitionResolver = new Boolean(true);
    this.attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver, getEnableConcurrency() };
    createPartitionedRegion(attributeObjects);
  }

  protected boolean getEnableConcurrency() {
    return false;
  }
  
  public void testPRTXInCacheListenerRedundancy0() {
    basicPRTXInCacheListener(0);
  }

  public void testPRTXInCacheListenerRedundancy1() {
    basicPRTXInCacheListener(1);
  }

  public void testPRTXInCacheListenerRedundancy2() {
    basicPRTXInCacheListener(2);
  }

  /**
   * This method executes a transaction inside a cache listener
   * @param bucketRedundancy redundancy for the colocated PRs
   */
  protected void basicPRTXInCacheListener(int bucketRedundancy) {
    createCacheInAllVms();
    redundancy = new Integer(bucketRedundancy);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);
    // Create Customer PartitionedRegion in All VMs
    createPRWithCoLocation(CustomerPartitionedRegionName, null);
    createPRWithCoLocation(OrderPartitionedRegionName,
        CustomerPartitionedRegionName);
    // register Cache Listeners
    SerializableCallable registerListeners = new SerializableCallable() {
      public Object call() throws Exception {
        Region custRegion = basicGetCache().getRegion(Region.SEPARATOR
            + CustomerPartitionedRegionName);
        custRegion.getAttributesMutator().addCacheListener(
            new TransactionListener());
        return null;
      }
    };

    dataStore1.invoke(registerListeners);
    dataStore2.invoke(registerListeners);
    dataStore3.invoke(registerListeners);

    // Put the customer 1-10 in CustomerPartitionedRegion
    accessor.invoke(PRColocationDUnitTest.class,
        "putCustomerPartitionedRegion",
        new Object[] { CustomerPartitionedRegionName });

    dataStore1.invoke(PRTransactionDUnitTest.class,
        "validatePRTXInCacheListener");
    dataStore2.invoke(PRTransactionDUnitTest.class,
        "validatePRTXInCacheListener");
    dataStore3.invoke(PRTransactionDUnitTest.class,
        "validatePRTXInCacheListener");

  }

  /**
   * verify that 10 orders are created for each customer
   * @throws ClassNotFoundException
   */
  public static void validatePRTXInCacheListener()
      throws ClassNotFoundException {
    PartitionedRegion customerPartitionedregion = null;
    PartitionedRegion orderPartitionedregion = null;
    try {
      customerPartitionedregion = (PartitionedRegion)basicGetCache()
          .getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
      orderPartitionedregion = (PartitionedRegion)basicGetCache()
          .getRegion(Region.SEPARATOR + OrderPartitionedRegionName);
    }
    catch (Exception e) {
      Assert.fail(
          "validateAfterPutPartitionedRegion : failed while getting the region",
          e);
    }
    assertNotNull(customerPartitionedregion);
    assertNotNull(orderPartitionedregion);

    customerPartitionedregion.getDataStore().dumpEntries(false);
    orderPartitionedregion.getDataStore().dumpEntries(false);
    Iterator custIterator = customerPartitionedregion.getDataStore()
        .getEntries().iterator();
    LogWriterUtils.getLogWriter().info(
        "Found " + customerPartitionedregion.getDataStore().getEntries().size()
            + " Customer entries in the partition");
    Region.Entry custEntry = null;
    while (custIterator.hasNext()) {
      custEntry = (Entry)custIterator.next();
      CustId custid = (CustId)custEntry.getKey();
      Customer cust = (Customer)custEntry.getValue();
      Iterator orderIterator = orderPartitionedregion.getDataStore()
          .getEntries().iterator();
      LogWriterUtils.getLogWriter().info(
          "Found " + orderPartitionedregion.getDataStore().getEntries().size()
              + " Order entries in the partition");
      int orderPerCustomer = 0;
      Region.Entry orderEntry = null;
      while (orderIterator.hasNext()) {
        orderEntry = (Entry)orderIterator.next();
        OrderId orderId = (OrderId)orderEntry.getKey();
        Order order = (Order)orderEntry.getValue();
        if (custid.equals(orderId.getCustId())) {
          orderPerCustomer++;
        }
      }
      assertEquals(10, orderPerCustomer);
    }
  }

  public void BUG46661DISABLEtestCacheListenerCallbacks() {
    createPopulateAndVerifyCoLocatedPRs(1);

    SerializableCallable registerListeners = new SerializableCallable() {
      public Object call() throws Exception {
        Region custRegion = basicGetCache().getRegion(Region.SEPARATOR
            + CustomerPartitionedRegionName);
        custRegion.getAttributesMutator().addCacheListener(
            new TransactionListener2());
        return null;
      }
    };

    accessor.invoke(registerListeners);
    dataStore1.invoke(registerListeners);
    dataStore2.invoke(registerListeners);
    dataStore3.invoke(registerListeners);

    accessor.invoke(new SerializableCallable("run function") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)basicGetCache()
            .getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
        PartitionedRegion orderpr = (PartitionedRegion)basicGetCache()
            .getRegion(Region.SEPARATOR + OrderPartitionedRegionName);
        CustId custId = new CustId(2);
        Customer newCus = new Customer("foo", "bar");
        Order order = new Order("fooOrder");
        OrderId orderId = new OrderId(22, custId);
        ArrayList args = new ArrayList();
        Function txFunction = new MyTransactionFunction();
        FunctionService.registerFunction(txFunction);
        Execution e = FunctionService.onRegion(pr);
        Set filter = new HashSet();
        boolean caughtException = false;
        // test transaction non-coLocated operations
        filter.clear();
        args.clear();
        args.add(new Integer(VERIFY_LISTENER_CALLBACK));
        LogWriterUtils.getLogWriter().info("VERIFY_LISTENER_CALLBACK");
        args.add(custId);
        args.add(newCus);
        args.add(orderId);
        args.add(order);
        filter.add(custId);
        caughtException = false;
        e.withFilter(filter).withArgs(args).execute(txFunction.getId())
            .getResult();
        return null;
      }
    });

  }
  
  public void testRepeatableRead() throws Exception {
    createColocatedPRs(1);
    SerializableCallable registerFunction = new SerializableCallable(
        "register Fn") {
      public Object call() throws Exception {
        Function txFunction = new MyTransactionFunction();
        FunctionService.registerFunction(txFunction);
        return Boolean.TRUE;
      }
    };

    dataStore1.invoke(registerFunction);
    dataStore2.invoke(registerFunction);
    dataStore3.invoke(registerFunction);

    accessor.invoke(new SerializableCallable("run function") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)basicGetCache()
            .getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
        PartitionedRegion orderpr = (PartitionedRegion)basicGetCache()
            .getRegion(Region.SEPARATOR + OrderPartitionedRegionName);
        CustId custId = new CustId(2);
        Customer newCus = new Customer("foo", "bar");
        Order order = new Order("fooOrder");
        OrderId orderId = new OrderId(22, custId);
        ArrayList args = new ArrayList();
        Function txFunction = new MyTransactionFunction();
        FunctionService.registerFunction(txFunction);
        Execution e = FunctionService.onRegion(pr);
        Set filter = new HashSet();
        filter.clear();
        args.clear();
        args.add(new Integer(VERIFY_REP_READ));
        LogWriterUtils.getLogWriter().info("VERIFY_REP_READ");
        args.add(custId);
        args.add(newCus);
        args.add(orderId);
        args.add(order);
        filter.add(custId);
        e.withFilter(filter).withArgs(args).execute(txFunction.getId())
            .getResult();
      
        return null;
      }
    });

  }

  public void testPRTXPerformance() throws Exception {
    defaultStringSize = 1024;

    createPopulateAndVerifyCoLocatedPRs(1);

    // register functions
    SerializableCallable registerPerfFunctions = new SerializableCallable(
        "register Fn") {
      public Object call() throws Exception {
        Function perfFunction = new PerfFunction();
        FunctionService.registerFunction(perfFunction);
        Function perfTxFunction = new PerfTxFunction();
        FunctionService.registerFunction(perfTxFunction);
        return Boolean.TRUE;
      }
    };

    dataStore1.invoke(registerPerfFunctions);
    dataStore2.invoke(registerPerfFunctions);
    dataStore3.invoke(registerPerfFunctions);
    accessor.invoke(registerPerfFunctions);

    SerializableCallable runPerfFunction = new SerializableCallable(
        "runPerfFunction") {
      public Object call() throws Exception {
        long perfTime = 0;
        Region customerPR = basicGetCache().getRegion(CustomerPartitionedRegionName);
        Execution e = FunctionService.onRegion(customerPR);
        // for each customer, update order and shipment
        for (int iterations = 1; iterations <= totalIterations; iterations++) {
          LogWriterUtils.getLogWriter().info("running perfFunction");
          long startTime = 0;
          ArrayList args = new ArrayList();
          CustId custId = new CustId(iterations % 10);
          for (int i = 1; i <= perfOrderShipmentPairs; i++) {
            OrderId orderId = new OrderId(custId.getCustId().intValue() * 10 + i,
                custId);
            Order order = new Order("NewOrder" + i + iterations);
            ShipmentId shipmentId = new ShipmentId(orderId.getOrderId().intValue()
                * 10 + i, orderId);
            Shipment shipment = new Shipment("newShipment" + i + iterations);
            args.add(orderId);
            args.add(order);
            args.add(shipmentId);
            args.add(shipment);
          }
          Set filter = new HashSet();
          filter.add(custId);
          if (iterations > warmupIterations) {
            startTime = NanoTimer.getTime();
          }
          e.withFilter(filter).withArgs(args).execute("perfFunction")
              .getResult();
          if (startTime > 0) {
            perfTime += NanoTimer.getTime() - startTime;
          }
        }
        return new Long(perfTime);
      }
    };

    Long perfTime = (Long)accessor.invoke(runPerfFunction);

    SerializableCallable runPerfTxFunction = new SerializableCallable(
        "runPerfTxFunction") {
      public Object call() throws Exception {
        long perfTime = 0;
        Region customerPR = basicGetCache().getRegion(CustomerPartitionedRegionName);
        Execution e = FunctionService.onRegion(customerPR);
        // for each customer, update order and shipment
        for (int iterations = 1; iterations <= totalIterations; iterations++) {
          LogWriterUtils.getLogWriter().info("Running perfFunction");
          long startTime = 0;
          ArrayList args = new ArrayList();
          CustId custId = new CustId(iterations % 10);
          for (int i = 1; i <= perfOrderShipmentPairs; i++) {
            OrderId orderId = new OrderId(custId.getCustId().intValue() * 10 + i,
                custId);
            Order order = new Order("NewOrder" + i + iterations);
            ShipmentId shipmentId = new ShipmentId(orderId.getOrderId().intValue()
                * 10 + i, orderId);
            Shipment shipment = new Shipment("newShipment" + i + iterations);
            args.add(orderId);
            args.add(order);
            args.add(shipmentId);
            args.add(shipment);
          }
          Set filter = new HashSet();
          filter.add(custId);
          if (iterations > warmupIterations) {
            startTime = NanoTimer.getTime();
          }
          e.withFilter(filter).withArgs(args).execute("perfTxFunction")
              .getResult();
          if (startTime > 0) {
            perfTime += NanoTimer.getTime() - startTime;
          }
        }
        return new Long(perfTime);
      }
    };

    Long perfTxTime = (Long)accessor.invoke(runPerfTxFunction);

    double diff = (perfTime.longValue() - perfTxTime.longValue()) * 1.0;
    double percentDiff = (diff / perfTime.longValue()) * 100;

    LogWriterUtils.getLogWriter().info(
        (totalIterations - warmupIterations) + " iterations of function took:"
            + +perfTime.longValue() + " Nanos, and transaction function took:"
            + perfTxTime.longValue() + " Nanos, difference :" + diff
            + " percentDifference:" + percentDiff);

  }

  // Don't want to run the test twice
  public void testColocatedPartitionedRegion() throws Throwable {
  }

  public void testColocationPartitionedRegion() throws Throwable {
  }

  public void testColocationPartitionedRegionWithRedundancy() throws Throwable {
  }

  public void testPartitionResolverPartitionedRegion() throws Throwable {
  }

  public void testColocationPartitionedRegionWithNullColocationSpecifiedOnOneNode() {
  }
  @Override
  public void testColocatedPRRedundancyRecovery() throws Throwable {
  }
  @Override
  public void testColocatedPRWithAccessorOnDifferentNode1() throws Throwable {
  }
  @Override
  public void testColocatedPRWithAccessorOnDifferentNode2() throws Throwable {
  }
  @Override
  public void testColocatedPRWithDestroy() throws Throwable {
  }
  @Override
  public void testColocatedPRWithLocalDestroy() throws Throwable {
  }
  @Override
  public void testColocatedPRWithPROnDifferentNode1() throws Throwable {
  }
  
  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(verifyNoTxState);
  }

  SerializableCallable verifyNoTxState = new SerializableCallable() {
    @Override
    public Object call() throws Exception {
      TXManagerImpl mgr = getGemfireCache().getTxManager();
      assertEquals(0, mgr.hostedTransactionsInProgressForTest());
      return null;
    }
  };

  class TransactionListener extends CacheListenerAdapter {

    public void afterCreate(EntryEvent event) {
      // for each customer, put 10 orders in a tx.
      Region custPR = event.getRegion();
      Region orderPR = custPR.getCache().getRegion(
          Region.SEPARATOR + OrderPartitionedRegionName);
      CacheTransactionManager mgr = custPR.getCache()
          .getCacheTransactionManager();
      mgr.begin();
      CustId custId = (CustId)event.getKey();
      for (int j = 1; j <= 10; j++) {
        int oid = (custId.getCustId().intValue() * 10) + j;
        OrderId orderId = new OrderId(oid, custId);
        Order order = new Order("OREDR" + oid);
        try {
          assertNotNull(orderPR);
          orderPR.put(orderId, order);
        }
        catch (Exception e) {
          // mgr.rollback();
          Assert.fail(" failed while doing put operation in CacheListener ", e);
        }
      }
      mgr.commit();
      LogWriterUtils.getLogWriter().info("COMMIT completed");
    }
  }

  static class TransactionListener2 extends CacheListenerAdapter {
    private int numberOfPutCallbacks = 0;
    private int numberOfDestroyCallbacks = 0;
    private int numberOfInvalidateCallbacks = 0;

    public void afterCreate(EntryEvent event) {
      synchronized (this) {
        numberOfPutCallbacks++;
      }
    }

    public void afterUpdate(EntryEvent event) {
      afterCreate(event);
    }
    
    public void afterDestroy(EntryEvent event) {
      synchronized(this){
        numberOfDestroyCallbacks++;
      }      
    }
    
    public void afterInvalidate(EntryEvent event) {
      synchronized(this){
        numberOfInvalidateCallbacks++;
      }
    }

    public synchronized int getNumberOfPutCallbacks() {
      return numberOfPutCallbacks;
    }
    
    public synchronized int getNumberOfDestroyCallbacks(){
      return numberOfDestroyCallbacks;
    }
    
    public synchronized int getNumberOfInvalidateCallbacks(){
      return numberOfInvalidateCallbacks;
    }
  }  
}
