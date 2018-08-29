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

import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Test;
import util.TestException;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Customer;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.execute.data.Shipment;
import org.apache.geode.internal.cache.execute.data.ShipmentId;
import org.apache.geode.internal.cache.partitioned.PRLocallyDestroyedException;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionRebalanceOp;
import org.apache.geode.internal.cache.partitioned.rebalance.BucketOperatorImpl;
import org.apache.geode.internal.cache.partitioned.rebalance.ExplicitMoveDirector;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;

/**
 * Test for co-located PR transactions. the test creates two data hosts and uses function execution
 * to execute transactions.
 */
public class PRTransactionDUnitTest extends PRColocationDUnitTest {
  private static final Logger logger = LogService.getLogger();

  static final int VERIFY_TX = 0;

  static final int VERIFY_ROLLBACK = 1;

  static final int VERIFY_NON_COLOCATION = 2;

  static final int VERIFY_DESTROY = 4;

  static final int VERIFY_LISTENER_CALLBACK = 5;

  static final int VERIFY_INVALIDATE = 6;

  static final int VERIFY_TXSTATE_CONFLICT = 7;

  static final int VERIFY_REP_READ = 8;

  static final int UPDATE_NON_COLOCATION = 9;

  private final int totalIterations = 50;

  private final int warmupIterations = 10;

  private final int perfOrderShipmentPairs = 1;

  public PRTransactionDUnitTest() {
    super();
  }

  @Test
  public void testBasicPRTransactionRedundancy0() {
    basicPRTXInFunction(0, true);
  }

  @Test
  public void testBasicPRTransactionRedundancy1() {
    basicPRTXInFunction(1, true);
  }

  @Test
  public void testBasicPRTransactionRedundancy2() {
    basicPRTXInFunction(2, true);
  }

  @Test
  public void testBasicPRTransactionNoDataRedundancy0() {
    basicPRTXInFunction(0, false);
  }

  @Test
  public void testBasicPRTransactionNoDataRedundancy1() {
    basicPRTXInFunction(1, false);
  }

  @Test
  public void testBasicPRTransactionNoDataRedundancy2() {
    basicPRTXInFunction(2, false);
  }

  @Test
  public void testBasicPRTransactionNonColocatedFunction0() {
    basicPRTXInNonColocatedFunction(0);
  }

  /**
   * Test two non colocated functions in a transaction. This method invokes
   * {@link MyTransactionFunction} and tells it what to test, using different arguments.
   *
   * @param redundantBuckets redundant buckets for colocated PRs
   */
  private void basicPRTXInNonColocatedFunction(int redundantBuckets) {
    setupColocatedRegions(redundantBuckets);

    dataStore1.invoke(() -> registerFunction());
    dataStore2.invoke(() -> registerFunction());

    dataStore1.invoke(() -> runTXFunctions());
  }

  private void registerFunction() {
    logger.info("register Fn");
    Function txFunction = new MyTransactionFunction();
    FunctionService.registerFunction(txFunction);
  }

  private void runFunction(final Region pr, int cust, boolean isFirstFunc) {
    CustId custId = new CustId(cust);
    Customer newCus = new Customer("foo", "bar");
    ArrayList args = new ArrayList();
    Execution execution = FunctionService.onRegion(pr);
    Set filter = new HashSet();

    args.add(new Integer(UPDATE_NON_COLOCATION));
    logger.info("UPDATE_NON_COLOCATION");
    args.add(custId);
    args.add(newCus);
    filter.add(custId);
    try {
      execution.withFilter(filter).setArguments(args).execute(new MyTransactionFunction().getId())
          .getResult();
      assertTrue("Expected exception was not thrown", isFirstFunc);
    } catch (Exception exp) {
      if (!isFirstFunc) {
        if (exp instanceof TransactionException && exp.getMessage()
            .startsWith("Function execution is not colocated with transaction.")) {
        } else {
          logger.info("Expected to catch TransactionException but caught exception " + exp, exp);
          Assert.fail("Expected to catch TransactionException but caught exception ", exp);
        }
      } else {
        logger.info("Caught unexpected exception", exp);
        Assert.fail("Unexpected exception was thrown", exp);
      }
    }
  }

  private void runTXFunctions() {
    PartitionedRegion pr = (PartitionedRegion) basicGetCache()
        .getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
    CacheTransactionManager mgr = pr.getCache().getCacheTransactionManager();
    mgr.begin();
    runFunction(pr, 1, true);
    runFunction(pr, 2, false);
    mgr.commit();
  }

  /**
   * Test all the basic functionality of colocated transactions. This method invokes
   * {@link MyTransactionFunction} and tells it what to test, using different arguments.
   *
   * @param redundantBuckets redundant buckets for colocated PRs
   * @param populateData if false tests are carried out on empty colocated PRs
   */
  private void basicPRTXInFunction(int redundantBuckets, boolean populateData) {

    if (populateData) {
      createPopulateAndVerifyCoLocatedPRs(redundantBuckets);
    } else {
      createColocatedPRs(redundantBuckets);
    }

    dataStore1.invoke(() -> registerFunction());
    dataStore2.invoke(() -> registerFunction());
    dataStore3.invoke(() -> registerFunction());

    accessor.invoke(new SerializableCallable("run function") {
      @Override
      public Object call() {
        PartitionedRegion pr = (PartitionedRegion) basicGetCache()
            .getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
        PartitionedRegion orderpr = (PartitionedRegion) basicGetCache()
            .getRegion(Region.SEPARATOR + OrderPartitionedRegionName);
        CustId custId = new CustId(2);
        Customer newCus = new Customer("foo", "bar");
        Order order = new Order("fooOrder");
        OrderId orderId = new OrderId(22, custId);
        ArrayList args = new ArrayList();
        Function txFunction = new MyTransactionFunction();
        FunctionService.registerFunction(txFunction);
        Execution execution = FunctionService.onRegion(pr);
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
          execution.withFilter(filter).setArguments(args).execute(txFunction.getId()).getResult();
          fail("Expected exception was not thrown");
        } catch (FunctionException fe) {
          LogWriterUtils.getLogWriter().info("Caught Expected exception");
          if (fe.getCause() instanceof TransactionDataNotColocatedException) {
          } else {
            throw new TestException(
                "Expected to catch FunctionException with cause TransactionDataNotColocatedException"
                    + " but cause is  " + fe.getCause(),
                fe.getCause());
          }
        }
        // verify that the transaction modifications are applied
        args.set(0, new Integer(VERIFY_TX));
        LogWriterUtils.getLogWriter().info("VERIFY_TX");
        orderpr.put(orderId, order);
        assertNotNull(orderpr.get(orderId));
        execution.withFilter(filter).setArguments(args).execute(txFunction.getId()).getResult();
        assertTrue("Unexpected customer value after commit", newCus.equals(pr.get(custId)));
        Order commitedOrder = (Order) orderpr.get(orderId);
        assertTrue(
            "Unexpected order value after commit. Expected:" + order + " Found:" + commitedOrder,
            order.equals(commitedOrder));
        // verify conflict detection
        args.set(0, new Integer(VERIFY_TXSTATE_CONFLICT));
        execution.withFilter(filter).setArguments(args).execute(txFunction.getId()).getResult();
        // verify that the transaction is rolled back
        args.set(0, new Integer(VERIFY_ROLLBACK));
        LogWriterUtils.getLogWriter().info("VERIFY_ROLLBACK");
        execution.withFilter(filter).setArguments(args).execute(txFunction.getId()).getResult();
        // verify destroy
        args.set(0, new Integer(VERIFY_DESTROY));
        LogWriterUtils.getLogWriter().info("VERIFY_DESTROY");
        execution.withFilter(filter).setArguments(args).execute(txFunction.getId()).getResult();
        // verify invalidate
        args.set(0, new Integer(VERIFY_INVALIDATE));
        LogWriterUtils.getLogWriter().info("VERIFY_INVALIDATE");
        execution.withFilter(filter).setArguments(args).execute(txFunction.getId()).getResult();
        return Boolean.TRUE;
      }
    });
  }

  private void createColocatedPRs(int redundantBuckets) {
    createCacheInAllVms();

    redundancy = new Integer(redundantBuckets);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);

    // Create Customer PartitionedRegion in All VMs
    createPRWithCoLocation(CustomerPartitionedRegionName, null);

    // Create Order PartitionedRegion in All VMs
    createPRWithCoLocation(OrderPartitionedRegionName, CustomerPartitionedRegionName);

    // Create Shipment PartitionedRegion in All VMs
    createPRWithCoLocation(ShipmentPartitionedRegionName, OrderPartitionedRegionName);

    // Initial Validation for the number of data stores and number of profiles
    accessor.invoke(() -> PRColocationDUnitTest
        .validateBeforePutCustomerPartitionedRegion(CustomerPartitionedRegionName));
  }

  @Override
  protected void createCacheInAllVms() {
    dataStore1.invoke(runGetCache);
    dataStore2.invoke(runGetCache);
    dataStore3.invoke(runGetCache);
    accessor.invoke(runGetCache);
  }

  private SerializableCallable runGetCache = new SerializableCallable("runGetCache") {
    @Override
    public Object call() {
      getCache();
      return null;
    }
  };

  private void populateAndVerifyColocatedPRs(int redundantBuckets) {
    // Put the customer 1-10 in CustomerPartitionedRegion
    accessor.invoke(
        () -> PRColocationDUnitTest.putCustomerPartitionedRegion(CustomerPartitionedRegionName));

    // Put the order 1-10 for each Customer in OrderPartitionedRegion
    accessor
        .invoke(() -> PRColocationDUnitTest.putOrderPartitionedRegion(OrderPartitionedRegionName));

    // Put the shipment 1-10 for each order in ShipmentPartitionedRegion
    accessor.invoke(
        () -> PRColocationDUnitTest.putShipmentPartitionedRegion(ShipmentPartitionedRegionName));

    // for VM0 DataStore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore1 = (Integer) dataStore1
        .invoke(() -> PRColocationDUnitTest.validateDataStore(CustomerPartitionedRegionName,
            OrderPartitionedRegionName, ShipmentPartitionedRegionName));

    // for VM1 DataStore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore2 = (Integer) dataStore2
        .invoke(() -> PRColocationDUnitTest.validateDataStore(CustomerPartitionedRegionName,
            OrderPartitionedRegionName, ShipmentPartitionedRegionName));

    // for VM3 Datastore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore3 = (Integer) dataStore3
        .invoke(() -> PRColocationDUnitTest.validateDataStore(CustomerPartitionedRegionName,
            OrderPartitionedRegionName, ShipmentPartitionedRegionName));

    // Check the total number of buckets created in all three Vms are equalto 30
    totalNumBucketsInTest = totalBucketsInDataStore1.intValue()
        + totalBucketsInDataStore2.intValue() + totalBucketsInDataStore3.intValue();
    assertEquals(30 + (redundantBuckets * 30), totalNumBucketsInTest);

    // This is the importatnt check. Checks that the colocated Customer,Order
    // and Shipment are in the same VM

    accessor.invoke(() -> PRColocationDUnitTest.validateAfterPutPartitionedRegion(
        CustomerPartitionedRegionName, OrderPartitionedRegionName, ShipmentPartitionedRegionName));

  }

  private void createPopulateAndVerifyCoLocatedPRs(int redundantBuckets) {

    createColocatedPRs(redundantBuckets);
    populateAndVerifyColocatedPRs(redundantBuckets);
  }

  private void createPRWithCoLocation(String prName, String coLocatedWith) {
    setAttributes(prName, coLocatedWith);
    createPartitionedRegion(attributeObjects);
  }

  private void setAttributes(String prName, String coLocatedWith) {
    this.regionName = prName;
    this.colocatedWith = coLocatedWith;
    this.isPartitionResolver = new Boolean(true);
    this.attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver, getEnableConcurrency()};
  }

  protected boolean getEnableConcurrency() {
    return false;
  }

  /**
   * This method executes a transaction with get on non colocated entries and expects the
   * transaction to fail with TransactionDataNotColocatedException.
   *
   * @param bucketRedundancy redundancy for the colocated PRs
   */
  private void basicPRTXWithNonColocatedGet(int bucketRedundancy) {
    setupColocatedRegions(bucketRedundancy);

    dataStore1.invoke(verifyNonColocated);
    dataStore2.invoke(verifyNonColocated);

    dataStore1.invoke(getTx);
  }

  @SuppressWarnings("unchecked")
  private void setupColocatedRegions(int bucketRedundancy) {
    dataStore1.invoke(runGetCache);
    dataStore2.invoke(runGetCache);
    redundancy = new Integer(bucketRedundancy);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(2);

    createColocatedRegionsInTwoNodes();

    // Put the customer 1-2 in CustomerPartitionedRegion
    dataStore1.invoke(
        () -> PRColocationDUnitTest.putCustomerPartitionedRegion(CustomerPartitionedRegionName, 2));

    // Put the associated order in colocated OrderPartitionedRegion
    dataStore1.invoke(
        () -> PRColocationDUnitTest.putOrderPartitionedRegion(OrderPartitionedRegionName, 2));
  }

  private void createColocatedRegionsInTwoNodes() {
    setAttributes(CustomerPartitionedRegionName, null);
    createPRInTwoNodes();
    setAttributes(OrderPartitionedRegionName, CustomerPartitionedRegionName);
    createPRInTwoNodes();
  }


  @SuppressWarnings("serial")
  private SerializableRunnable verifyNonColocated = new SerializableRunnable("verifyNonColocated") {
    @Override
    public void run() throws PRLocallyDestroyedException, ForceReattemptException {
      containsKeyLocally();
    }
  };

  @SuppressWarnings("serial")
  private SerializableRunnable getTx = new SerializableRunnable("getTx") {
    @Override
    public void run() {
      performGetTx();
    }
  };


  @SuppressWarnings({"unchecked", "rawtypes"})
  private void containsKeyLocally() throws PRLocallyDestroyedException, ForceReattemptException {
    PartitionedRegion pr = (PartitionedRegion) basicGetCache()
        .getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);

    CustId cust1 = new CustId(1);
    CustId cust2 = new CustId(2);
    int bucketId1 = pr.getKeyInfo(cust1).getBucketId();
    int bucketId2 = pr.getKeyInfo(cust2).getBucketId();

    List<Integer> localPrimaryBucketList = pr.getLocalPrimaryBucketsListTestOnly();
    Set localBucket1Keys;
    Set localBucket2Keys;
    assertTrue(localPrimaryBucketList.size() == 1);
    for (int bucketId : localPrimaryBucketList) {
      if (bucketId == bucketId1) {
        // primary bucket has cust1
        localBucket1Keys = pr.getDataStore().getKeysLocally(bucketId1, false);
        for (Object key : localBucket1Keys) {
          LogService.getLogger().info("local key set contains " + key);
        }
        assertTrue(localBucket1Keys.size() == 1);
      } else {
        localBucket2Keys = pr.getDataStore().getKeysLocally(bucketId2, false);
        for (Object key : localBucket2Keys) {
          LogService.getLogger().info("local key set contains " + key);
        }
        assertTrue(localBucket2Keys.size() == 1);
      }
    }
  }


  private void performGetTx() {
    PartitionedRegion pr = (PartitionedRegion) basicGetCache()
        .getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
    CacheTransactionManager mgr = pr.getCache().getCacheTransactionManager();
    CustId cust1 = new CustId(1);
    CustId cust2 = new CustId(2);
    boolean isCust1Local = isCust1Local(pr, cust1);

    // touch first get on remote node -- using TXStateStub
    Assertions.assertThatThrownBy(() -> getTx(!isCust1Local, mgr, pr, cust1, cust2))
        .isInstanceOf(TransactionDataNotColocatedException.class);

    // touch first get on local node-- using TXState
    Assertions.assertThatThrownBy(() -> getTx(isCust1Local, mgr, pr, cust1, cust2))
        .isInstanceOf(TransactionDataNotColocatedException.class);
  }

  @SuppressWarnings("unchecked")
  private boolean isCust1Local(PartitionedRegion pr, CustId cust1) {
    int bucketId1 = pr.getKeyInfo(cust1).getBucketId();
    List<Integer> localPrimaryBucketList = pr.getLocalPrimaryBucketsListTestOnly();
    assertTrue(localPrimaryBucketList.size() == 1);
    return (Integer) localPrimaryBucketList.get(0) == bucketId1;
  }

  private void getTx(boolean doCust1First, CacheTransactionManager mgr, PartitionedRegion pr,
      CustId cust1, CustId cust2) {
    CustId first = doCust1First ? cust1 : cust2;
    CustId second = !doCust1First ? cust1 : cust2;

    mgr.begin();
    boolean doRollback = true;
    try {
      pr.get(first);
      pr.get(second);
      doRollback = false;
    } finally {
      if (doRollback) {
        mgr.rollback();
      } else {
        mgr.commit();
      }
    }
  }

  /**
   * This method executes a transaction with operation on a key in a moved bucket, and expects
   * transaction to fail with TransactionDataRebalancedException.
   *
   * @param op which entry op to be executed
   * @param bucketRedundancy redundancy for the colocated PRs
   */
  @SuppressWarnings("unchecked")
  private void basicPRTXWithOpOnMovedBucket(Op op, int bucketRedundancy, DistributedMember dm1,
      DistributedMember dm2) {
    // First transaction.
    TransactionId txId = dataStore1.invoke(() -> beginTx(false));
    dataStore1.invoke(resumeTx(op, txId, dm1, dm2));

    // Second one. Will go through different path (using TXState or TXStateStub)
    txId = dataStore1.invoke(() -> beginTx(false));
    dataStore1.invoke(resumeTx(op, txId, dm1, dm2));
  }

  @SuppressWarnings({"rawtypes", "serial"})
  private SerializableCallable getDM() {
    return new SerializableCallable("getDM") {
      @Override
      public Object call() {
        return ((GemFireCacheImpl) basicGetCache()).getMyId();
      }
    };
  }

  private TransactionId beginTx(boolean doPut) {
    PartitionedRegion pr = (PartitionedRegion) basicGetCache()
        .getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
    CacheTransactionManager mgr = basicGetCache().getCacheTransactionManager();
    CustId cust1 = new CustId(1);
    mgr.begin();
    Object value = pr.get(cust1);
    assertNotNull(value);
    if (doPut) {
      pr.put(cust1, "bar");
    }
    return mgr.suspend();
  }

  @SuppressWarnings("serial")
  private SerializableRunnable resumeTx(Op op, TransactionId txId, DistributedMember dm1,
      DistributedMember dm2) {
    return new SerializableRunnable("resume tx") {
      @Override
      public void run() {
        PartitionedRegion pr = (PartitionedRegion) basicGetCache()
            .getRegion(Region.SEPARATOR + OrderPartitionedRegionName);
        CacheTransactionManager mgr = basicGetCache().getCacheTransactionManager();

        moveBucket(op, dm1, dm2);

        Assertions.assertThatThrownBy(() -> _resumeTx(op, txId, pr, mgr))
            .isInstanceOf(TransactionDataRebalancedException.class);
      }
    };
  }

  private void _resumeTx(Op op, TransactionId txId, PartitionedRegion pr,
      CacheTransactionManager mgr) {
    CustId cust1 = new CustId(1);
    OrderId order1 = new OrderId(11, cust1);
    OrderId neworder = new OrderId(111, cust1);
    mgr.resume(txId);
    try {
      switch (op) {
        case GET:
          pr.get(order1);
          break;
        case CONTAINSVALUEFORKEY:
          pr.containsValueForKey(order1);
          break;
        case CONTAINSKEY:
          pr.containsKey(order1);
          break;
        case CREATE:
          pr.create(neworder, new Order("test"));
          break;
        case PUT:
          pr.put(order1, new Order("test"));
          break;
        case INVALIDATE:
          pr.invalidate(order1);
          break;
        case DESTROY:
          pr.destroy(order1);
          break;
        case GETENTRY:
          pr.getEntry(order1);
          break;
        default:
          throw new AssertionError("Unknown operations " + op);
      }

    } finally {
      mgr.rollback();
    }
  }

  @SuppressWarnings("unchecked")
  private void moveBucket(Op op, DistributedMember dm1, DistributedMember dm2) {
    PartitionedRegion pr = (PartitionedRegion) basicGetCache()
        .getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
    CustId cust1 = new CustId(1);
    OrderId order1 = new OrderId(11, cust1);
    boolean isCust1Local = isCust1LocalSingleBucket(pr, cust1);
    DistributedMember source = isCust1Local ? dm1 : dm2;
    DistributedMember destination = isCust1Local ? dm2 : dm1;
    PartitionedRegion prOrder = (PartitionedRegion) basicGetCache()
        .getRegion(Region.SEPARATOR + OrderPartitionedRegionName);

    LogService.getLogger().info("source ={}, destination ={}", source, destination);

    switch (op) {
      case GET:
        moveBucketForGet(order1, isCust1Local, source, destination, prOrder);
        break;
      case CONTAINSVALUEFORKEY:
      case CONTAINSKEY:
      case CREATE:
      case PUT:
      case INVALIDATE:
      case DESTROY:
      case GETENTRY:
        PartitionRegionHelper.moveBucketByKey(prOrder, source, destination, order1);
        break;
      default:
        throw new AssertionError("Unknown operations " + op);
    }
  }

  @SuppressWarnings("unchecked")
  private void moveBucketForGet(OrderId order1, boolean isCust1Local, DistributedMember source,
      DistributedMember destination, PartitionedRegion prOrder) {
    if (isCust1Local && useBucketReadHook) {
      // Use TXState
      setBucketReadHook(order1, source, destination, prOrder);
    } else {
      // Use TXState and TXStateStub -- transaction data on remote node
      PartitionRegionHelper.moveBucketByKey(prOrder, source, destination, order1);
    }
  }

  private void setBucketReadHook(OrderId order1, DistributedMember source,
      DistributedMember destination, PartitionedRegion prOrder) {
    prOrder.getDataStore().setBucketReadHook(new Runnable() {
      @Override
      @SuppressWarnings("unchecked")
      public void run() {
        LogService.getLogger().info("In bucketReadHook");
        PartitionRegionHelper.moveBucketByKey(prOrder, source, destination, order1);
      }
    });
  }

  private void createPRInTwoNodes() {
    dataStore1.invoke(PRColocationDUnitTest.class, "createPR", this.attributeObjects);
    dataStore2.invoke(PRColocationDUnitTest.class, "createPR", this.attributeObjects);
  }

  @SuppressWarnings("unchecked")
  private boolean isCust1LocalSingleBucket(PartitionedRegion pr, CustId cust1) {
    List<Integer> localPrimaryBucketList = pr.getLocalPrimaryBucketsListTestOnly();
    return (Integer) localPrimaryBucketList.size() == 1;
  }

  @SuppressWarnings("unchecked")
  private void setupMoveBucket(int bucketRedundancy) {
    dataStore1.invoke(runGetCache);
    dataStore2.invoke(runGetCache);
    redundancy = new Integer(bucketRedundancy);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(1);

    createColocatedRegionsInTwoNodes();

    // Put the customer 1 in CustomerPartitionedRegion
    dataStore1.invoke(
        () -> PRColocationDUnitTest.putCustomerPartitionedRegion(CustomerPartitionedRegionName, 1));

    // Put the associated order in colocated OrderPartitionedRegion
    dataStore1.invoke(
        () -> PRColocationDUnitTest.putOrderPartitionedRegion(OrderPartitionedRegionName, 1));
  }

  @Test
  public void testTxWithNonColocatedGet() {
    basicPRTXWithNonColocatedGet(0);
  }

  /**
   * This method executes a transaction with entry op on non colocated entries and expects the
   * transaction to fail with TransactionDataNotColocatedException.
   *
   * @param bucketRedundancy redundancy for the colocated PRs
   */
  private void basicPRTXWithNonColocatedOp(Op op, int bucketRedundancy) {
    dataStore1.invoke(verifyNonColocated);
    dataStore2.invoke(verifyNonColocated);

    // TXState and TXStateStub will be exectued on different nodes.
    dataStore1.invoke(doOp(op));
    dataStore2.invoke(doOp(op));
  }

  @SuppressWarnings("serial")
  private SerializableRunnable doOp(Op op) {
    return new SerializableRunnable("doOp") {
      @Override
      public void run() {
        basicDoOp(op);
      }
    };
  }

  private void basicDoOp(Op op) {
    CacheTransactionManager mgr = basicGetCache().getCacheTransactionManager();

    Assertions.assertThatThrownBy(() -> doOp(op, mgr))
        .isInstanceOf(TransactionDataNotColocatedException.class);
  }

  private void doOp(Op op, CacheTransactionManager mgr) {
    PartitionedRegion cust = (PartitionedRegion) basicGetCache()
        .getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
    PartitionedRegion order = (PartitionedRegion) basicGetCache()
        .getRegion(Region.SEPARATOR + OrderPartitionedRegionName);

    CustId cust1 = new CustId(1);
    CustId cust2 = new CustId(2);
    OrderId order2 = new OrderId(21, cust2);
    OrderId neworder2 = new OrderId(221, cust2);

    mgr.begin();
    cust.get(cust1); // touch 1 bucket

    try {
      switch (op) {
        case GET:
          order.get(order2);
          break;
        case CONTAINSVALUEFORKEY:
          order.containsValueForKey(order2);
          break;
        case CONTAINSKEY:
          order.containsKey(order2);
          break;
        case CREATE:
          order.create(neworder2, new Order("test"));
          break;
        case PUT:
          order.put(order2, new Order("test"));
          break;
        case INVALIDATE:
          order.invalidate(order2);
          break;
        case DESTROY:
          order.destroy(order2);
          break;
        case GETENTRY:
          order.getEntry(order2);
          break;
        default:
          throw new AssertionError("Unknown operations " + op);
      }
    } finally {
      mgr.rollback();
    }
  }

  @Test
  public void testTxWithNonColocatedOps() {
    setupColocatedRegions(0);

    for (Op op : Op.values()) {
      LogService.getLogger().info("Testing Operation: " + op.name());
      basicPRTXWithNonColocatedOp(op, 0);
    }
  }

  private boolean useBucketReadHook;

  @SuppressWarnings("unchecked")
  @Test
  public void testTxWithOpsOnMovedBucket() {
    setupMoveBucket(0);

    DistributedMember dm1 = (DistributedMember) dataStore1.invoke(getDM());
    DistributedMember dm2 = (DistributedMember) dataStore2.invoke(getDM());

    for (Op op : Op.values()) {
      LogService.getLogger().info("Testing Operation: " + op.name());
      basicPRTXWithOpOnMovedBucket(op, 0, dm1, dm2);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTxWithGetOnMovedBucketUsingBucketReadHook() {
    setupMoveBucket(0);

    DistributedMember dm1 = (DistributedMember) dataStore1.invoke(getDM());
    DistributedMember dm2 = (DistributedMember) dataStore2.invoke(getDM());

    Op op = Op.GET;
    useBucketReadHook = true;
    basicPRTXWithOpOnMovedBucket(op, 0, dm1, dm2);
    useBucketReadHook = false;
  }

  @Test
  public void testPRTXInCacheListenerRedundancy0() {
    basicPRTXInCacheListener(0);
  }

  @Test
  public void testPRTXInCacheListenerRedundancy1() {
    basicPRTXInCacheListener(1);
  }

  @Test
  public void testPRTXInCacheListenerRedundancy2() {
    basicPRTXInCacheListener(2);
  }

  /**
   * This method executes a transaction inside a cache listener
   *
   * @param bucketRedundancy redundancy for the colocated PRs
   */
  private void basicPRTXInCacheListener(int bucketRedundancy) {
    createCacheInAllVms();
    redundancy = new Integer(bucketRedundancy);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);
    // Create Customer PartitionedRegion in All VMs
    createPRWithCoLocation(CustomerPartitionedRegionName, null);
    createPRWithCoLocation(OrderPartitionedRegionName, CustomerPartitionedRegionName);
    // register Cache Listeners
    SerializableCallable registerListeners = new SerializableCallable() {
      @Override
      public Object call() {
        Region custRegion =
            basicGetCache().getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
        custRegion.getAttributesMutator().addCacheListener(new TransactionListener());
        return null;
      }
    };

    dataStore1.invoke(registerListeners);
    dataStore2.invoke(registerListeners);
    dataStore3.invoke(registerListeners);

    // Put the customer 1-10 in CustomerPartitionedRegion
    accessor.invoke(
        () -> PRColocationDUnitTest.putCustomerPartitionedRegion(CustomerPartitionedRegionName));

    dataStore1.invoke(() -> PRTransactionDUnitTest.validatePRTXInCacheListener());
    dataStore2.invoke(() -> PRTransactionDUnitTest.validatePRTXInCacheListener());
    dataStore3.invoke(() -> PRTransactionDUnitTest.validatePRTXInCacheListener());

  }

  /**
   * verify that 10 orders are created for each customer
   *
   */
  private static void validatePRTXInCacheListener() {
    PartitionedRegion customerPartitionedregion = null;
    PartitionedRegion orderPartitionedregion = null;
    try {
      customerPartitionedregion = (PartitionedRegion) basicGetCache()
          .getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
      orderPartitionedregion = (PartitionedRegion) basicGetCache()
          .getRegion(Region.SEPARATOR + OrderPartitionedRegionName);
    } catch (Exception e) {
      Assert.fail("validateAfterPutPartitionedRegion : failed while getting the region", e);
    }
    assertNotNull(customerPartitionedregion);
    assertNotNull(orderPartitionedregion);

    customerPartitionedregion.getDataStore().dumpEntries(false);
    orderPartitionedregion.getDataStore().dumpEntries(false);
    Iterator custIterator = customerPartitionedregion.getDataStore().getEntries().iterator();
    LogWriterUtils.getLogWriter()
        .info("Found " + customerPartitionedregion.getDataStore().getEntries().size()
            + " Customer entries in the partition");
    Region.Entry custEntry = null;
    while (custIterator.hasNext()) {
      custEntry = (Entry) custIterator.next();
      CustId custid = (CustId) custEntry.getKey();
      Customer cust = (Customer) custEntry.getValue();
      Iterator orderIterator = orderPartitionedregion.getDataStore().getEntries().iterator();
      LogWriterUtils.getLogWriter()
          .info("Found " + orderPartitionedregion.getDataStore().getEntries().size()
              + " Order entries in the partition");
      int orderPerCustomer = 0;
      Region.Entry orderEntry = null;
      while (orderIterator.hasNext()) {
        orderEntry = (Entry) orderIterator.next();
        OrderId orderId = (OrderId) orderEntry.getKey();
        Order order = (Order) orderEntry.getValue();
        if (custid.equals(orderId.getCustId())) {
          orderPerCustomer++;
        }
      }
      assertEquals(10, orderPerCustomer);
    }
  }

  @Ignore("BUG46661")
  @Test
  public void testCacheListenerCallbacks() {
    createPopulateAndVerifyCoLocatedPRs(1);

    SerializableCallable registerListeners = new SerializableCallable() {
      @Override
      public Object call() {
        Region custRegion =
            basicGetCache().getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
        custRegion.getAttributesMutator().addCacheListener(new TransactionListener2());
        return null;
      }
    };

    accessor.invoke(registerListeners);
    dataStore1.invoke(registerListeners);
    dataStore2.invoke(registerListeners);
    dataStore3.invoke(registerListeners);

    accessor.invoke(new SerializableCallable("run function") {
      @Override
      public Object call() {
        PartitionedRegion pr = (PartitionedRegion) basicGetCache()
            .getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
        PartitionedRegion orderpr = (PartitionedRegion) basicGetCache()
            .getRegion(Region.SEPARATOR + OrderPartitionedRegionName);
        CustId custId = new CustId(2);
        Customer newCus = new Customer("foo", "bar");
        Order order = new Order("fooOrder");
        OrderId orderId = new OrderId(22, custId);
        ArrayList args = new ArrayList();
        Function txFunction = new MyTransactionFunction();
        FunctionService.registerFunction(txFunction);
        Execution execution = FunctionService.onRegion(pr);
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
        execution.withFilter(filter).setArguments(args).execute(txFunction.getId()).getResult();
        return null;
      }
    });

  }

  @Test
  public void testRepeatableRead() {
    createColocatedPRs(1);

    dataStore1.invoke(() -> registerFunction());
    dataStore2.invoke(() -> registerFunction());
    dataStore3.invoke(() -> registerFunction());

    accessor.invoke(new SerializableCallable("run function") {
      @Override
      public Object call() {
        PartitionedRegion pr = (PartitionedRegion) basicGetCache()
            .getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
        PartitionedRegion orderpr = (PartitionedRegion) basicGetCache()
            .getRegion(Region.SEPARATOR + OrderPartitionedRegionName);
        CustId custId = new CustId(2);
        Customer newCus = new Customer("foo", "bar");
        Order order = new Order("fooOrder");
        OrderId orderId = new OrderId(22, custId);
        ArrayList args = new ArrayList();
        Function txFunction = new MyTransactionFunction();
        FunctionService.registerFunction(txFunction);
        Execution execution = FunctionService.onRegion(pr);
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
        execution.withFilter(filter).setArguments(args).execute(txFunction.getId()).getResult();

        return null;
      }
    });

  }

  @Test
  public void testPRTXPerformance() {
    PRColocationDUnitTestHelper.defaultStringSize = 1024;

    createPopulateAndVerifyCoLocatedPRs(1);

    // register functions
    SerializableCallable registerPerfFunctions = new SerializableCallable("register Fn") {
      @Override
      public Object call() {
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

    SerializableCallable runPerfFunction = new SerializableCallable("runPerfFunction") {
      @Override
      public Object call() {
        long perfTime = 0;
        Region customerPR = basicGetCache().getRegion(CustomerPartitionedRegionName);
        Execution execution = FunctionService.onRegion(customerPR);
        // for each customer, update order and shipment
        for (int iterations = 1; iterations <= totalIterations; iterations++) {
          LogWriterUtils.getLogWriter().info("running perfFunction");
          long startTime = 0;
          ArrayList args = new ArrayList();
          CustId custId = new CustId(iterations % 10);
          for (int i = 1; i <= perfOrderShipmentPairs; i++) {
            OrderId orderId = new OrderId(custId.getCustId().intValue() * 10 + i, custId);
            Order order = new Order("NewOrder" + i + iterations);
            ShipmentId shipmentId =
                new ShipmentId(orderId.getOrderId().intValue() * 10 + i, orderId);
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
          execution.withFilter(filter).setArguments(args).execute("perfFunction").getResult();
          if (startTime > 0) {
            perfTime += NanoTimer.getTime() - startTime;
          }
        }
        return new Long(perfTime);
      }
    };

    Long perfTime = (Long) accessor.invoke(runPerfFunction);

    SerializableCallable runPerfTxFunction = new SerializableCallable("runPerfTxFunction") {
      @Override
      public Object call() {
        long perfTime = 0;
        Region customerPR = basicGetCache().getRegion(CustomerPartitionedRegionName);
        Execution execution = FunctionService.onRegion(customerPR);
        // for each customer, update order and shipment
        for (int iterations = 1; iterations <= totalIterations; iterations++) {
          LogWriterUtils.getLogWriter().info("Running perfFunction");
          long startTime = 0;
          ArrayList args = new ArrayList();
          CustId custId = new CustId(iterations % 10);
          for (int i = 1; i <= perfOrderShipmentPairs; i++) {
            OrderId orderId = new OrderId(custId.getCustId().intValue() * 10 + i, custId);
            Order order = new Order("NewOrder" + i + iterations);
            ShipmentId shipmentId =
                new ShipmentId(orderId.getOrderId().intValue() * 10 + i, orderId);
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
          execution.withFilter(filter).setArguments(args).execute("perfTxFunction").getResult();
          if (startTime > 0) {
            perfTime += NanoTimer.getTime() - startTime;
          }
        }
        return new Long(perfTime);
      }
    };

    Long perfTxTime = (Long) accessor.invoke(runPerfTxFunction);

    double diff = (perfTime.longValue() - perfTxTime.longValue()) * 1.0;
    double percentDiff = (diff / perfTime.longValue()) * 100;

    LogWriterUtils.getLogWriter()
        .info((totalIterations - warmupIterations) + " iterations of function took:"
            + +perfTime.longValue() + " Nanos, and transaction function took:"
            + perfTxTime.longValue() + " Nanos, difference :" + diff + " percentDifference:"
            + percentDiff);

  }

  @Test
  public void testCommitToFailAfterPrimaryBucketMoved() {
    basicPRTXCommitToFailAfterPrimaryBucketMoved(1);
  }

  /**
   * Test commit fail after the transactional node no longer hosts the primary bucket of the
   * operations executed in the transaction.
   *
   * @param redundantBuckets redundant buckets for colocated PRs
   */
  private void basicPRTXCommitToFailAfterPrimaryBucketMoved(int redundantBuckets) {
    setupColocatedRegions(redundantBuckets);

    InternalDistributedMember dm1 = (InternalDistributedMember) dataStore1.invoke(getDM());
    InternalDistributedMember dm2 = (InternalDistributedMember) dataStore2.invoke(getDM());

    TransactionId txId = dataStore1.invoke(() -> beginTx(true));
    dataStore1.invoke(() -> movePrimaryBucket(dm1, dm2));
    dataStore1.invoke(() -> resumeTxAfterPrimaryMoved(txId));

  }

  private void movePrimaryBucket(InternalDistributedMember dm1, InternalDistributedMember dm2) {
    PartitionedRegion pr = (PartitionedRegion) basicGetCache()
        .getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
    CustId cust1 = new CustId(1);
    int bucketId = pr.getKeyInfo(cust1).getBucketId();
    boolean isCust1LocalPrimary = pr.getBucketRegion(cust1).getBucketAdvisor().isPrimary();
    InternalDistributedMember destination = isCust1LocalPrimary ? dm2 : dm1;
    InternalDistributedMember source = isCust1LocalPrimary ? dm1 : dm2;

    ExplicitMoveDirector director = new ExplicitMoveDirector(cust1, bucketId, source, destination,
        pr.getCache().getDistributedSystem());
    PartitionedRegionRebalanceOp rebalanceOp =
        new PartitionedRegionRebalanceOp(pr, false, director, true, true);
    BucketOperatorImpl operator = new BucketOperatorImpl(rebalanceOp);
    boolean moved = operator.movePrimary(source, destination, bucketId);
    if (!moved) {
      fail("Not able to move primary bucket by invoking BucketOperatorImpl.movePrimary");
    }
  }

  private void resumeTxAfterPrimaryMoved(TransactionId txId) {
    PartitionedRegion pr = (PartitionedRegion) basicGetCache()
        .getRegion(Region.SEPARATOR + CustomerPartitionedRegionName);
    CacheTransactionManager mgr = basicGetCache().getCacheTransactionManager();

    mgr.resume(txId);

    Throwable thrown = catchThrowable(() -> mgr.commit());
    assertTrue(thrown instanceof TransactionDataRebalancedException);
  }

  // Don't want to run the test twice
  @Override
  @Test
  public void testColocatedPartitionedRegion() {}

  @Override
  @Test
  public void testColocationPartitionedRegion() {}

  @Override
  @Test
  public void testColocationPartitionedRegionWithRedundancy() {}

  @Override
  @Test
  public void testPartitionResolverPartitionedRegion() {}

  @Override
  @Test
  public void testColocationPartitionedRegionWithNullColocationSpecifiedOnOneNode() {}

  @Override
  @Test
  public void testColocatedPRRedundancyRecovery() {}

  @Override
  @Test
  public void testColocatedPRWithAccessorOnDifferentNode1() {}

  @Override
  @Test
  public void testColocatedPRWithAccessorOnDifferentNode2() {}

  @Override
  @Test
  public void testColocatedPRWithDestroy() {}

  @Override
  @Test
  public void testColocatedPRWithLocalDestroy() {}

  @Override
  @Test
  public void testColocatedPRWithPROnDifferentNode1() {}

  @Override
  public final void preTearDownCacheTestCase() {
    Invoke.invokeInEveryVM(verifyNoTxState);
  }

  private SerializableCallable verifyNoTxState = new SerializableCallable() {
    @Override
    public Object call() {
      TXManagerImpl mgr = getGemfireCache().getTxManager();
      assertEquals(0, mgr.hostedTransactionsInProgressForTest());
      return null;
    }
  };

  enum Op {
    GET, CONTAINSVALUEFORKEY, CONTAINSKEY, CREATE, PUT, INVALIDATE, DESTROY, GETENTRY;
  }

  static class TransactionListener extends CacheListenerAdapter {

    @Override
    public void afterCreate(EntryEvent event) {
      // for each customer, put 10 orders in a tx.
      Region custPR = event.getRegion();
      Region orderPR = custPR.getCache().getRegion(Region.SEPARATOR + OrderPartitionedRegionName);
      CacheTransactionManager mgr = custPR.getCache().getCacheTransactionManager();
      mgr.begin();
      CustId custId = (CustId) event.getKey();
      for (int j = 1; j <= 10; j++) {
        int oid = (custId.getCustId().intValue() * 10) + j;
        OrderId orderId = new OrderId(oid, custId);
        Order order = new Order("OREDR" + oid);
        try {
          assertNotNull(orderPR);
          orderPR.put(orderId, order);
        } catch (Exception e) {
          // mgr.rollback();
          Assert.fail(" failed while doing put operation in CacheListener ", e);
        }
      }
      mgr.commit();
      LogWriterUtils.getLogWriter().info("COMMIT completed");
    }
  }

  static class TransactionListener2 extends CacheListenerAdapter {

    private int numberOfPutCallbacks;
    private int numberOfDestroyCallbacks;
    private int numberOfInvalidateCallbacks;

    @Override
    public void afterCreate(EntryEvent event) {
      synchronized (this) {
        numberOfPutCallbacks++;
      }
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      afterCreate(event);
    }

    @Override
    public void afterDestroy(EntryEvent event) {
      synchronized (this) {
        numberOfDestroyCallbacks++;
      }
    }

    @Override
    public void afterInvalidate(EntryEvent event) {
      synchronized (this) {
        numberOfInvalidateCallbacks++;
      }
    }

    synchronized int getNumberOfPutCallbacks() {
      return numberOfPutCallbacks;
    }

    synchronized int getNumberOfDestroyCallbacks() {
      return numberOfDestroyCallbacks;
    }

    synchronized int getNumberOfInvalidateCallbacks() {
      return numberOfInvalidateCallbacks;
    }
  }
}
