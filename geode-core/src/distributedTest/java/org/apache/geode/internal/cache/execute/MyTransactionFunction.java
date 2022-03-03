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

import java.util.ArrayList;
import java.util.Iterator;

import util.TestException;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.TXEntryState;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXRegionState;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.execute.PRTransactionDUnitTest.TransactionListener2;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Customer;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;

public class MyTransactionFunction implements Function {

  @Override
  public void execute(FunctionContext context) {
    RegionFunctionContext ctx = (RegionFunctionContext) context;
    verifyExecutionOnPrimary(ctx);
    ArrayList args = (ArrayList) ctx.getArguments();
    Integer testOperation = (Integer) args.get(0);
    int op = testOperation;
    switch (op) {
      case PRTransactionDUnitTest.VERIFY_TX:
        verifyTransactionExecution(ctx);
        ctx.getDataSet().getCache().getLogger().info("verifyTransactionExecution Passed");
        break;
      case PRTransactionDUnitTest.VERIFY_ROLLBACK:
        verifyTransactionRollback(ctx);
        ctx.getDataSet().getCache().getLogger().info("verifyTransactionRollback Passed");
        break;
      case PRTransactionDUnitTest.VERIFY_DESTROY:
        verifyDestroyOperation(ctx);
        ctx.getDataSet().getCache().getLogger().info("verifyDestroy Passed");
        break;
      case PRTransactionDUnitTest.VERIFY_INVALIDATE:
        verifyInvalidateOperation(ctx);
        ctx.getDataSet().getCache().getLogger().info("verifyInvalidate Passed");
        break;
      case PRTransactionDUnitTest.VERIFY_NON_COLOCATION:
        verifyNonCoLocatedOpsRejection(ctx);
        ctx.getDataSet().getCache().getLogger().info("verifyNonCoLocatedOpsRejection Passed");
        break;
      case PRTransactionDUnitTest.VERIFY_LISTENER_CALLBACK:
        verifyListenerCallback(ctx);
        break;
      case PRTransactionDUnitTest.VERIFY_TXSTATE_CONFLICT:
        verifyTxStateAndConflicts(ctx);
        break;
      case PRTransactionDUnitTest.VERIFY_REP_READ:
        verifyRepeatableRead(ctx);
        break;
      case PRTransactionDUnitTest.UPDATE_NON_COLOCATION:
        updateNonColocation(ctx);
        break;
    }
    context.getResultSender().lastResult(null);
  }

  @Override
  public String getId() {
    return "txFuntion";
  }

  private void verifyTransactionExecution(RegionFunctionContext ctx) {
    Region custPR = ctx.getDataSet();
    Region orderPR = custPR.getCache().getRegion(PRTransactionDUnitTest.OrderPartitionedRegionName);
    CacheTransactionManager mgr = custPR.getCache().getCacheTransactionManager();
    ArrayList args = (ArrayList) ctx.getArguments();
    CustId custId = (CustId) args.get(1);
    Customer newCus = (Customer) args.get(2);
    OrderId orderId = (OrderId) args.get(3);
    Order order = (Order) args.get(4);
    mgr.begin();
    custPR.put(custId, newCus);
    Assert.assertTrue(custPR.containsKey(custId));
    Assert.assertTrue(custPR.containsValueForKey(custId));
    orderPR.put(orderId, order);
    Assert.assertTrue(orderPR.containsKey(orderId));
    Assert.assertTrue(orderPR.containsValueForKey(orderId));
    mgr.commit();
    Customer commitedCust = (Customer) custPR.get(custId);
    Assert.assertTrue(newCus.equals(commitedCust),
        "Expected Customer to be:" + newCus + " but was:" + commitedCust);
    Order commitedOrder = (Order) orderPR.get(orderId);
    Assert.assertTrue(order.equals(commitedOrder),
        "Expected Order to be:" + order + " but was:" + commitedOrder);
    // put a never before put key
    OrderId newOrderId = new OrderId(4000, custId);
    Order newOrder = new Order("NewOrder");
    mgr.begin();
    custPR.put(custId, newCus);
    orderPR.put(newOrderId, newOrder);
    mgr.commit();
    commitedOrder = (Order) orderPR.get(newOrderId);
    Assert.assertTrue(newOrder.equals(commitedOrder),
        "Expected Order to be:" + order + " but was:" + commitedOrder);
  }

  private void verifyDestroyOperation(RegionFunctionContext ctx) {
    Region custPR = ctx.getDataSet();
    Region orderPR = custPR.getCache().getRegion(PRColocationDUnitTest.OrderPartitionedRegionName);
    CacheTransactionManager mgr = custPR.getCache().getCacheTransactionManager();
    ArrayList args = (ArrayList) ctx.getArguments();
    CustId custId = (CustId) args.get(1);
    Customer newCus = (Customer) args.get(2);
    OrderId orderId = (OrderId) args.get(3);
    Order order = (Order) args.get(4);
    Customer oldCustomer = (Customer) custPR.get(custId);
    Customer commitedCust = null;
    // test destroy rollback
    mgr.begin();
    custPR.put(custId, newCus);
    custPR.destroy(custId);
    orderPR.put(orderId, order);
    mgr.rollback();
    commitedCust = (Customer) custPR.get(custId);
    Assert.assertTrue(oldCustomer.equals(commitedCust),
        "Expected customer to rollback to:" + oldCustomer + " but was:" + commitedCust);
    // test destroy rollback on unmodified entry
    mgr.begin();
    custPR.destroy(custId);
    orderPR.put(orderId, order);
    mgr.rollback();
    commitedCust = (Customer) custPR.get(custId);
    Assert.assertTrue(oldCustomer.equals(commitedCust),
        "Expected customer to rollback to:" + oldCustomer + " but was:" + commitedCust);
    // test remote destroy
    boolean caughtEx = false;
    try {
      mgr.begin();
      Customer cust = new Customer("foo", "bar");
      custPR.put(custId, cust);
      custPR.destroy(custId);
      custPR.putIfAbsent(custId, cust);
      custPR.remove(custId, cust);
      custPR.destroy(new CustId(1));
      custPR.destroy(new CustId(3));
      custPR.destroy(new CustId(7));
      mgr.commit();
    } catch (Exception e) {
      mgr.rollback();
      if (e instanceof TransactionDataNotColocatedException) {
        caughtEx = true;
      } else if (e instanceof TransactionDataRebalancedException) {
        caughtEx = true;
      } else if (e instanceof EntryNotFoundException
          && e.getMessage().matches("Entry not found for key.*1")) {
        caughtEx = true;
      } else {
        throw new TestException(
            "Expected to catch PR remote destroy exception, but caught:" + e.getMessage(), e);
      }
    }
    if (!caughtEx) {
      throw new TestException("An Expected exception was not thrown");
    }
    // test destroy on unmodified entry
    mgr.begin();
    custPR.destroy(custId);
    orderPR.put(orderId, order);
    mgr.commit();
    commitedCust = (Customer) custPR.get(custId);
    Assert.assertTrue(commitedCust == null, "Expected Customer to be null but was:" + commitedCust);
    Order commitedOrder = (Order) orderPR.get(orderId);
    Assert.assertTrue(order.equals(commitedOrder),
        "Expected Order to be:" + order + " but was:" + commitedOrder);
    // put the customer again for invalidate verification
    mgr.begin();
    custPR.putIfAbsent(custId, newCus);
    mgr.commit();
    // test destroy on new entry
    // TODO: This throws EntryNotFound
    OrderId newOrderId = new OrderId(5000, custId);
    mgr.begin();
    Order newOrder = new Order("New Order to be destroyed");
    orderPR.put(newOrderId, newOrder);
    orderPR.destroy(newOrderId);
    mgr.commit();
    Assert.assertTrue(orderPR.get(newOrderId) == null, "Did not expect orderId to be present");

    // test ConcurrentMap operations
    mgr.begin();
    Order order1 = new Order("New Order to be replaced");
    Order order2 = new Order("New Order to be destroyed");
    orderPR.putIfAbsent(newOrderId, order1);
    Assert.assertTrue(order1.equals(orderPR.replace(newOrderId, order2)));
    mgr.commit(); // value is order2
    Assert.assertTrue(order2.equals(orderPR.get(newOrderId)));
    mgr.begin();
    Assert.assertTrue(orderPR.replace(newOrderId, order2, order1));
    mgr.commit(); // value is order1
    Assert.assertTrue(orderPR.get(newOrderId).equals(order1));
    mgr.begin();
    // this should return false since the value is order1
    Assert.assertTrue(!orderPR.remove(newOrderId, new Object()));
    mgr.commit();
    Assert.assertTrue(orderPR.get(newOrderId).equals(order1));
    mgr.begin();
    Assert.assertTrue(orderPR.remove(newOrderId, order1));
    mgr.commit(); // gone now
    Assert.assertTrue(orderPR.get(newOrderId) == null);
  }

  private void verifyInvalidateOperation(RegionFunctionContext ctx) {
    Region custPR = ctx.getDataSet();
    Region orderPR = custPR.getCache().getRegion(PRTransactionDUnitTest.OrderPartitionedRegionName);
    CacheTransactionManager mgr = custPR.getCache().getCacheTransactionManager();
    ArrayList args = (ArrayList) ctx.getArguments();
    CustId custId = (CustId) args.get(1);
    Customer newCus = (Customer) args.get(2);
    OrderId orderId = (OrderId) args.get(3);
    Order order = (Order) args.get(4);
    Customer oldCustomer = (Customer) custPR.get(custId);
    Customer commitedCust = null;
    // test destroy rollback
    mgr.begin();
    custPR.put(custId, newCus);
    custPR.invalidate(custId);
    orderPR.put(orderId, order);
    mgr.rollback();
    commitedCust = (Customer) custPR.get(custId);
    Assert.assertTrue(oldCustomer.equals(commitedCust),
        "Expected customer to rollback to:" + oldCustomer + " but was:" + commitedCust);
    // test destroy rollback on unmodified entry
    mgr.begin();
    custPR.invalidate(custId);
    orderPR.put(orderId, order);
    mgr.rollback();
    commitedCust = (Customer) custPR.get(custId);
    Assert.assertTrue(oldCustomer.equals(commitedCust),
        "Expected customer to rollback to:" + oldCustomer + " but was:" + commitedCust);
    // test remote destroy
    boolean caughtEx = false;
    try {
      mgr.begin();
      custPR.put(custId, new Customer("foo", "bar"));
      custPR.invalidate(custId);
      custPR.invalidate(new CustId(1));
      custPR.invalidate(new CustId(3));
      custPR.invalidate(new CustId(7));
      mgr.commit();
    } catch (Exception e) {
      mgr.rollback();
      if ((e instanceof TransactionDataNotColocatedException)
          || (e instanceof TransactionDataRebalancedException)) {
        caughtEx = true;
      } else if (e instanceof EntryNotFoundException
          && e.getMessage().matches("Entry not found for key.*1")) {
        caughtEx = true;
      } else {
        throw new TestException(
            "Expected to catch PR remote destroy exception, but caught:" + e.getMessage(), e);
      }
    }
    if (!caughtEx) {
      throw new TestException("An Expected exception was not thrown");
    }
    // test destroy on unmodified entry
    mgr.begin();
    custPR.invalidate(custId);
    orderPR.put(orderId, order);
    mgr.commit();
    commitedCust = (Customer) custPR.get(custId);
    Assert.assertTrue(commitedCust == null, "Expected Customer to be null but was:" + commitedCust);
    Order commitedOrder = (Order) orderPR.get(orderId);
    Assert.assertTrue(order.equals(commitedOrder),
        "Expected Order to be:" + order + " but was:" + commitedOrder);
    // test destroy on new entry
    // TODO: This throws EntryNotFound
    /*
     * OrderId newOrderId = new OrderId(5000,custId); mgr.begin(); orderPR.put(newOrderId, new
     * Order("New Order to be destroyed")); orderPR.invalidate(newOrderId); mgr.commit();
     * Assert.assertTrue(orderPR.get(newOrderId)==null,"Did not expect orderId to be present");
     */
  }

  private void verifyTransactionRollback(RegionFunctionContext ctx) {
    Region custPR = ctx.getDataSet();
    Region orderPR = custPR.getCache().getRegion(PRTransactionDUnitTest.OrderPartitionedRegionName);
    CacheTransactionManager mgr = custPR.getCache().getCacheTransactionManager();
    ArrayList args = (ArrayList) ctx.getArguments();
    CustId custId = (CustId) args.get(1);
    Customer newCus = (Customer) args.get(2);
    OrderId orderId = (OrderId) args.get(3);
    Order order = (Order) args.get(4);
    Customer oldCustomer = (Customer) custPR.get(custId);
    Order oldOrder = (Order) orderPR.get(orderId);
    mgr.begin();
    custPR.put(custId, newCus);
    Customer txCust = (Customer) custPR.get(custId);
    orderPR.put(orderId, order);
    Order txOrder = (Order) orderPR.get(orderId);
    Assert.assertTrue(newCus.equals(txCust),
        "Expected Customer to be:" + newCus + " but was:" + txCust);
    Assert.assertTrue(txOrder.equals(order),
        "Expected Order to be:" + order + " but was:" + txOrder);
    mgr.rollback();
    Customer commitedCust = (Customer) custPR.get(custId);
    Assert.assertTrue(oldCustomer.equals(commitedCust),
        "Expected Customer to be:" + oldCustomer + " but was:" + commitedCust);
    Order commitedOrder = (Order) orderPR.get(orderId);
    Assert.assertTrue(oldOrder.equals(commitedOrder),
        "Expected Order to be:" + oldOrder + " but was:" + commitedOrder);

    mgr.begin();
    Assert.assertTrue(custPR.remove(custId, oldCustomer));
    orderPR.replace(orderId, order);
    mgr.rollback();

    Assert.assertTrue(oldCustomer.equals(custPR.get(custId)));
    Assert.assertTrue(oldOrder.equals(orderPR.get(orderId)));

    mgr.begin();
    Assert.assertTrue(custPR.replace(custId, oldCustomer, newCus));
    orderPR.remove(orderId, oldOrder);
    Assert.assertTrue(null == orderPR.putIfAbsent(orderId, order));
    mgr.rollback();
    Assert.assertTrue(oldCustomer.equals(custPR.get(custId)));
    Assert.assertTrue(oldOrder.equals(orderPR.get(orderId)));

  }

  private void verifyNonCoLocatedOpsRejection(RegionFunctionContext ctx) {
    Region custPR = ctx.getDataSet();
    Region orderPR = custPR.getCache().getRegion(PRTransactionDUnitTest.OrderPartitionedRegionName);
    CacheTransactionManager mgr = custPR.getCache().getCacheTransactionManager();
    ArrayList args = (ArrayList) ctx.getArguments();
    CustId custId = (CustId) args.get(1);
    Customer newCus = (Customer) args.get(2);
    OrderId orderId = (OrderId) args.get(3);
    Order order = (Order) args.get(4);
    mgr.begin();
    try {
      custPR.put(custId, newCus);
      custPR.put(new CustId(4), "foo4");
      custPR.put(new CustId(5), "foo5");
      custPR.put(new CustId(6), "foo6");
      orderPR.put(orderId, order);
      Assert.assertTrue(false);
    } finally {
      mgr.rollback();
    }
  }

  private void verifyListenerCallback(RegionFunctionContext ctx) {
    verifyTransactionExecution(ctx);
    TransactionListener2 listener =
        (TransactionListener2) ctx.getDataSet().getAttributes().getCacheListeners()[0];
    Assert.assertTrue(listener.getNumberOfPutCallbacks() == 2,
        "Expected 2 put callback, but " + "got " + listener.getNumberOfPutCallbacks());
    verifyDestroyOperation(ctx);
    Assert.assertTrue(listener.getNumberOfDestroyCallbacks() == 1,
        "Expected 1 destroy callbacks, but " + "got " + listener.getNumberOfDestroyCallbacks());
    verifyInvalidateOperation(ctx);
    Assert.assertTrue(listener.getNumberOfInvalidateCallbacks() == 1,
        "Expected 1 invalidate callbacks, but " + "got "
            + listener.getNumberOfInvalidateCallbacks());
  }

  private void verifyExecutionOnPrimary(RegionFunctionContext ctx) {
    PartitionedRegion pr = (PartitionedRegion) ctx.getDataSet();
    ArrayList args = (ArrayList) ctx.getArguments();
    CustId custId = (CustId) args.get(1);
    int bucketId = PartitionedRegionHelper.getHashKey(pr, null, custId, null, null);
    DistributedMember primary = pr.getRegionAdvisor().getPrimaryMemberForBucket(bucketId);
    DistributedMember me = pr.getCache().getDistributedSystem().getDistributedMember();
    Assert.assertTrue(me.equals(primary), "Function should have been executed on primary:" + primary
        + " but was executed on member:" + me);
  }

  private void verifyTxStateAndConflicts(RegionFunctionContext ctx) {
    Region custPR = ctx.getDataSet();
    Region orderPR = custPR.getCache().getRegion(PRTransactionDUnitTest.OrderPartitionedRegionName);
    ArrayList args = (ArrayList) ctx.getArguments();
    CustId custId = (CustId) args.get(1);
    CacheTransactionManager mgr = custPR.getCache().getCacheTransactionManager();
    OrderId vOrderId = new OrderId(3000, custId);
    Order vOrder = new Order("vOrder");
    TXManagerImpl mImp = (TXManagerImpl) mgr;
    mImp.begin();
    orderPR.put(vOrderId, vOrder);
    TXStateProxy txState = mImp.pauseTransaction();
    Iterator it = txState.getRegions().iterator();
    Assert.assertTrue(txState.getRegions().size() == 1,
        "Expected 1 region; " + "found:" + txState.getRegions().size());
    LocalRegion lr = (LocalRegion) it.next();
    Assert.assertTrue(lr instanceof BucketRegion);
    TXRegionState txRegion = txState.readRegion(lr);
    TXEntryState txEntry = txRegion.readEntry(txRegion.getEntryKeys().iterator().next());
    mImp.unpauseTransaction(txState);
    orderPR.put(vOrderId, new Order("foo"));
    TransactionId txId = null;
    txId = mImp.suspend();
    // since both puts were on same key, verify that
    // TxRegionState and TXEntryState are same
    LocalRegion lr1 = (LocalRegion) txState.getRegions().iterator().next();
    Assert.assertTrue(lr == lr1);
    TXRegionState txRegion1 = txState.readRegion(lr);
    TXEntryState txEntry1 = txRegion1.readEntry(txRegion.getEntryKeys().iterator().next());
    Assert.assertTrue(txEntry == txEntry1);
    // to check for conflicts, start a new transaction, operate on same key,
    // commit the second and expect the first to fail
    mImp.begin();
    orderPR.put(vOrderId, new Order("foobar"));
    mImp.commit();
    // now begin the first
    mImp.resume(txId);
    boolean caughtException = false;
    try {
      mImp.commit();
    } catch (CommitConflictException e) {
      caughtException = true;
    }
    if (!caughtException) {
      throw new TestException("An expected exception was not thrown");
    }
  }

  private void verifyRepeatableRead(RegionFunctionContext ctx) {
    Region custPR = ctx.getDataSet();
    Region orderPR = custPR.getCache().getRegion(PRColocationDUnitTest.OrderPartitionedRegionName);
    ArrayList args = (ArrayList) ctx.getArguments();
    CustId custId = (CustId) args.get(1);
    Customer cust = (Customer) args.get(2);
    Assert.assertTrue(custPR.get(custId) == null);
    CacheTransactionManager mgr = custPR.getCache().getCacheTransactionManager();
    TXManagerImpl mImp = (TXManagerImpl) mgr;
    mImp.begin();
    custPR.put(custId, cust);
    Assert.assertTrue(cust.equals(custPR.get(custId)));
    TXStateProxy txState = mImp.pauseTransaction();
    Assert.assertTrue(custPR.get(custId) == null);
    mImp.unpauseTransaction(txState);
    mImp.commit();
    // change value
    mImp.begin();
    Customer oldCust = (Customer) custPR.get(custId);
    Assert.assertTrue(oldCust.equals(cust));
    txState = mImp.pauseTransaction();
    Customer newCust = new Customer("fooNew", "barNew");
    custPR.put(custId, newCust);
    mImp.unpauseTransaction(txState);
    Assert.assertTrue(oldCust.equals(custPR.get(custId)));
    mImp.commit();
  }

  private void updateNonColocation(RegionFunctionContext ctx) {
    Region custPR = ctx.getDataSet();

    ArrayList args = (ArrayList) ctx.getArguments();
    CustId custId = (CustId) args.get(1);
    Customer newCus = (Customer) args.get(2);

    custPR.put(custId, newCus);
    Assert.assertTrue(custPR.containsKey(custId));
    Assert.assertTrue(custPR.containsValueForKey(custId));

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
