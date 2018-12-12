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
package org.apache.geode.internal.cache.partitioned;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion.IteratorType;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.CustomerIDPartitionResolver;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Customer;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.RegionsTest;

/**
 * Verifies that the {@link org.apache.geode.cache.PartitionResolver} is called only once on a node,
 * and not called while local iteration.
 *
 */
@Category({RegionsTest.class})
public class PartitionResolverDUnitTest extends JUnit4CacheTestCase {

  private static final String CUSTOMER = "custRegion";
  private static final String ORDER = "orderRegion";
  Host host;
  VM accessor;
  VM datastore1;
  VM datastore2;

  public PartitionResolverDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    host = Host.getHost(0);
    accessor = host.getVM(0);
    datastore1 = host.getVM(1);
    datastore2 = host.getVM(2);
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    CountingResolver.resetResolverCount();
  }

  void createRegion(boolean isAccessor, int redundantCopies) {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.DISTRIBUTED_ACK);
    af = new AttributesFactory();
    af.setPartitionAttributes(new PartitionAttributesFactory<CustId, Customer>()
        .setTotalNumBuckets(4).setLocalMaxMemory(isAccessor ? 0 : 1)
        .setPartitionResolver(new CountingResolver("CountingResolverCust"))
        .setRedundantCopies(redundantCopies).create());
    getCache().createRegion(CUSTOMER, af.create());
    af.setPartitionAttributes(new PartitionAttributesFactory<OrderId, Order>().setTotalNumBuckets(4)
        .setLocalMaxMemory(isAccessor ? 0 : 1)
        .setPartitionResolver(new CountingResolver("CountingResolverOrder"))
        .setRedundantCopies(redundantCopies).setColocatedWith(CUSTOMER).create());
    getCache().createRegion(ORDER, af.create());
  }

  void populateData() {
    Region custRegion = getCache().getRegion(CUSTOMER);
    Region orderRegion = getCache().getRegion(ORDER);
    for (int i = 0; i < 5; i++) {
      CustId custId = new CustId(i);
      Customer customer = new Customer("customer" + i, "address" + i);
      OrderId orderId = new OrderId(i, custId);
      Order order = new Order("order" + i);
      custRegion.put(custId, customer);
      orderRegion.put(orderId, order);
    }
  }

  static class CountingResolver extends CustomerIDPartitionResolver {
    static AtomicInteger count = new AtomicInteger();

    public CountingResolver(String resolverID) {
      super(resolverID);
    }

    @Override
    public Serializable getRoutingObject(EntryOperation opDetails) {
      count.incrementAndGet();
      opDetails.getRegion().getCache().getLogger().fine(
          "Resolver called key:" + opDetails.getKey() + " Region " + opDetails.getRegion().getName()
              + " id:" + ((GemFireCacheImpl) opDetails.getRegion().getCache()).getMyId(),
          new Throwable());
      return super.getRoutingObject(opDetails);
    }

    public static void resetResolverCount() {
      count.set(0);
    }
  }

  private void initAccessorAndDataStore(final int redundantCopies) {
    accessor.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        createRegion(true/* accessor */, redundantCopies);
        return null;
      }
    });
    datastore1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        createRegion(false/* accessor */, redundantCopies);
        return null;
      }
    });
    datastore2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        createRegion(false, redundantCopies);
        populateData();
        return null;
      }
    });
    verifyResolverCountInVM(accessor, 0);
    verifyResolverCountInVM(datastore1, getNumberOfKeysOwnedByVM(datastore1));
    verifyResolverCountInVM(datastore2, 10);
    getCache().getLogger().fine("Reset resolver count");
  }

  private int getNumberOfKeysOwnedByVM(VM datastore12) {
    final Integer numKeys = (Integer) datastore12.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        PartitionedRegion custRegion = (PartitionedRegion) getGemfireCache().getRegion(CUSTOMER);
        Set<BucketRegion> bucketSet = custRegion.getDataStore().getAllLocalPrimaryBucketRegions();
        int count = 0;
        for (BucketRegion br : bucketSet) {
          count += br.size();
        }
        return count;
      }
    });
    return numKeys * 2 /* for Customer region */;
  }

  private void verifyResolverCountInVM(final VM vm, final int i) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        assertEquals("My id: " + getGemfireCache().getMyId(), i, CountingResolver.count.get());
        CountingResolver.resetResolverCount();
        return null;
      }
    });
  }

  @Test
  public void testKeysInIterationOnAccessor() {
    resolverInIteration(IteratorType.KEYS, accessor);
    verifyResolverCountInVM(accessor, 0);
    verifyResolverCountInVM(datastore1, 0);
    verifyResolverCountInVM(datastore2, 0);
  }

  @Test
  public void testValuesInIterationOnAccessor() {
    resolverInIteration(IteratorType.VALUES, accessor);
    verifyResolverCountInVM(accessor, 0);
    verifyResolverCountInVM(datastore1, getNumberOfKeysOwnedByVM(datastore1));
    verifyResolverCountInVM(datastore2, getNumberOfKeysOwnedByVM(datastore2));
  }

  @Test
  public void testEntriesInIterationOnAccessor() {
    resolverInIteration(IteratorType.ENTRIES, accessor);
    verifyResolverCountInVM(accessor, 0);
    verifyResolverCountInVM(datastore1, getNumberOfKeysOwnedByVM(datastore1));
    verifyResolverCountInVM(datastore2, getNumberOfKeysOwnedByVM(datastore2));
  }

  @Test
  public void testKeysInIterationOnDataStore() {
    resolverInIteration(IteratorType.KEYS, datastore1);
    verifyResolverCountInVM(accessor, 0);
    verifyResolverCountInVM(datastore1, 0);
    verifyResolverCountInVM(datastore2, 0);
  }

  @Test
  public void testValuesInIterationOnDataStore() {
    resolverInIteration(IteratorType.VALUES, datastore1);
    verifyResolverCountInVM(accessor, 0);
    verifyResolverCountInVM(datastore1, 0);
    verifyResolverCountInVM(datastore2, getNumberOfKeysOwnedByVM(datastore2));
  }

  @Test
  public void testEntriesInIterationOnDataStore() {
    resolverInIteration(IteratorType.ENTRIES, datastore1);
    verifyResolverCountInVM(accessor, 0);
    verifyResolverCountInVM(datastore1, 0);
    verifyResolverCountInVM(datastore2, getNumberOfKeysOwnedByVM(datastore2));
  }

  private void resolverInIteration(final IteratorType type, final VM vm) {
    initAccessorAndDataStore(0);
    SerializableCallable doIteration = new SerializableCallable() {
      public Object call() throws Exception {
        Region custRegion = getGemfireCache().getRegion(CUSTOMER);
        Region orderRegion = getGemfireCache().getRegion(ORDER);
        Iterator custIterator = null;
        Iterator orderIterator = null;
        switch (type) {
          case ENTRIES:
            custIterator = custRegion.entrySet().iterator();
            orderIterator = orderRegion.entrySet().iterator();
            break;
          case KEYS:
            custIterator = custRegion.keySet().iterator();
            orderIterator = orderRegion.keySet().iterator();
            break;
          case VALUES:
            custIterator = custRegion.values().iterator();
            orderIterator = orderRegion.values().iterator();
            break;
          default:
            break;
        }
        while (custIterator.hasNext()) {
          custIterator.next();
        }
        while (orderIterator.hasNext()) {
          orderIterator.next();
        }
        return null;
      }
    };
    vm.invoke(doIteration);
  }

  @Test
  public void testKeysIterationInFunctionExection() {
    doIterationInFunction(IteratorType.KEYS);
    verifyResolverCountInVM(accessor, 0);
    verifyResolverCountInVM(datastore1, 0);
    verifyResolverCountInVM(datastore2, 0);
  }

  @Test
  public void testValuesIterationInFunctionExection() {
    doIterationInFunction(IteratorType.VALUES);
    verifyResolverCountInVM(accessor, 0);
    verifyResolverCountInVM(datastore1, 0);
    verifyResolverCountInVM(datastore2, 0);
  }

  @Test
  public void testEntriesIterationInFunctionExection() {
    doIterationInFunction(IteratorType.ENTRIES);
    verifyResolverCountInVM(accessor, 0);
    verifyResolverCountInVM(datastore1, 0);
    verifyResolverCountInVM(datastore2, 0);
  }

  private void doIterationInFunction(final IteratorType type) {
    initAccessorAndDataStore(0);
    SerializableCallable registerFunction = new SerializableCallable() {
      public Object call() throws Exception {
        FunctionService.registerFunction(new IteratorFunction());
        return null;
      }
    };
    accessor.invoke(registerFunction);
    datastore1.invoke(registerFunction);
    datastore2.invoke(registerFunction);

    accessor.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        FunctionService.onRegion(getGemfireCache().getRegion(CUSTOMER)).setArguments(type)
            .execute(IteratorFunction.id).getResult();
        return null;
      }
    });
  }

  static class IteratorFunction implements Function {
    private static final String id = "IteratorFunction";

    public void execute(FunctionContext context) {
      Region custRegion =
          PartitionRegionHelper.getLocalDataForContext((RegionFunctionContext) context);
      Region orderRegion =
          PartitionRegionHelper.getLocalData(custRegion.getCache().getRegion(ORDER));
      IteratorType type = (IteratorType) context.getArguments();
      Iterator custIterator = null;
      Iterator orderIterator = null;
      switch (type) {
        case ENTRIES:
          custIterator = custRegion.entrySet().iterator();
          orderIterator = orderRegion.entrySet().iterator();
          break;
        case KEYS:
          custIterator = custRegion.keySet().iterator();
          orderIterator = orderRegion.keySet().iterator();
          break;
        case VALUES:
          custIterator = custRegion.values().iterator();
          orderIterator = orderRegion.values().iterator();
          break;
        default:
          break;
      }
      while (custIterator.hasNext()) {
        custIterator.next();
      }
      while (orderIterator.hasNext()) {
        orderIterator.next();
      }
      context.getResultSender().lastResult(Boolean.TRUE);
    }

    public String getId() {
      return id;
    }

    public boolean hasResult() {
      return true;
    }

    public boolean optimizeForWrite() {
      return false;
    }

    public boolean isHA() {
      return false;
    }
  }

  @Test
  public void testOps() {
    initAccessorAndDataStore(0);
    doOps(false);
    verifyResolverCountInVM(accessor, 7);
    verifyResolverCountInVM(datastore1, getResolverCountForVM(datastore1));
    verifyResolverCountInVM(datastore2, getResolverCountForVM(datastore2));
  }

  @Ignore("TODO")
  @Test
  public void testTxOps() {
    initAccessorAndDataStore(0);
    doOps(true);
    verifyResolverCountInVM(accessor, 7);
    verifyResolverCountInVM(datastore1, getResolverCountForVM(datastore1));
    verifyResolverCountInVM(datastore2, getResolverCountForVM(datastore2));
  }

  private void doOps(final boolean isTx) {
    accessor.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region custRegion = getGemfireCache().getRegion(CUSTOMER);
        CacheTransactionManager mgr = getGemfireCache().getCacheTransactionManager();
        CustId custId = new CustId(6);
        Customer customer = new Customer("customer6", "address6");
        getGemfireCache().getLogger().fine("SWAP:Begin");
        if (isTx)
          mgr.begin();
        custRegion.put(custId, customer);
        if (isTx)
          mgr.commit();

        if (isTx)
          mgr.begin();
        custRegion.invalidate(custId);
        if (isTx)
          mgr.commit();

        if (isTx)
          mgr.begin();
        custRegion.put(custId, customer);
        if (isTx)
          mgr.commit();

        if (isTx)
          mgr.begin();
        custRegion.destroy(custId);
        if (isTx)
          mgr.commit();

        if (isTx)
          mgr.begin();
        custRegion.put(custId, customer);
        if (isTx)
          mgr.commit();

        if (isTx)
          mgr.begin();
        custRegion.containsKey(custId);
        if (isTx)
          mgr.commit();

        if (isTx)
          mgr.begin();
        custRegion.containsValueForKey(custId);
        if (isTx)
          mgr.commit();
        return null;
      }
    });
  }

  private int getResolverCountForVM(final VM datastore12) {
    Integer containsTestKey = (Integer) datastore12.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = PartitionRegionHelper.getLocalData(getGemfireCache().getRegion(CUSTOMER));
        Iterator it = r.keySet().iterator();
        while (it.hasNext()) {
          if (it.next().equals(new CustId(6))) {
            return 1;
          }
        }
        return 0;
      }
    });
    return containsTestKey * 7;
  }
}
