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
package org.apache.geode.internal.cache;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.internal.cache.ExpiryTask.permitExpiration;
import static org.apache.geode.internal.cache.ExpiryTask.suspendExpiration;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.naming.Context;
import javax.naming.NamingException;

import jakarta.transaction.RollbackException;
import jakarta.transaction.Status;
import jakarta.transaction.UserTransaction;
import org.awaitility.core.ThrowingRunnable;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.ExpirationDetector;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.cache.TransactionEvent;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.cache.TransactionWriter;
import org.apache.geode.cache.TransactionWriterException;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.execute.CustomerIDPartitionResolver;
import org.apache.geode.internal.cache.execute.InternalFunctionService;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Customer;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class RemoteTransactionDUnitTest extends JUnit4CacheTestCase {

  protected final String CUSTOMER = "custRegion";
  protected final String ORDER = "orderRegion";
  protected final String D_REFERENCE = "distrReference";

  private final SerializableCallable<Object> getNumberOfTXInProgress =
      new SerializableCallable<Object>() {
        @Override
        public Object call() {
          TXManagerImpl mgr = getCache().getTxManager();
          return mgr.hostedTransactionsInProgressForTest();
        }
      };
  private final SerializableCallable<Object> verifyNoTxState = new SerializableCallable<Object>() {
    @Override
    public Object call() {
      try {
        CacheFactory.getAnyInstance();
      } catch (CacheClosedException e) {
        return null;
      }
      // TXManagerImpl mgr = getCache().getTxManager();
      // assertIndexDetailsEquals(0, mgr.hostedTransactionsInProgressForTest());
      final TXManagerImpl mgr = getCache().getTxManager();
      GeodeAwaitility.await().atMost(Duration.ofMillis(30000)).untilAsserted(
          () -> assertThat(mgr.hostedTransactionsInProgressForTest()).isZero());
      return null;
    }
  };

  protected enum OP {
    PUT, GET, DESTROY, INVALIDATE, KEYS, VALUES, ENTRIES, PUTALL, GETALL, REMOVEALL
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties result = super.getDistributedSystemProperties();
    result.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.internal.cache.RemoteTransactionDUnitTest*"
            + ";org.apache.geode.test.dunit.**" + ";org.apache.geode.test.junit.**"
            + ";org.apache.geode.internal.cache.execute.data.CustId"
            + ";org.apache.geode.internal.cache.execute.data.Customer");
    return result;
  }

  @Override
  public final void preTearDownCacheTestCase() {
    try {
      Invoke.invokeInEveryVM(verifyNoTxState);
    } finally {
      closeAllCache();
    }
  }

  void createRegion(boolean accessor, int redundantCopies, InterestPolicy interestPolicy) {
    AttributesFactory<Object, Object> af = new AttributesFactory<Object, Object>();
    af.setScope(Scope.DISTRIBUTED_ACK);
    af.setDataPolicy(DataPolicy.REPLICATE);
    af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
    getCache().createRegion(D_REFERENCE, af.create());
    af = new AttributesFactory<Object, Object>();
    af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
    if (interestPolicy != null) {
      af.setSubscriptionAttributes(new SubscriptionAttributes(interestPolicy));
    }
    af.setPartitionAttributes(new PartitionAttributesFactory<>()
        .setTotalNumBuckets(4).setLocalMaxMemory(accessor ? 0 : 1)
        .setPartitionResolver(new CustomerIDPartitionResolver("resolver1"))
        .setRedundantCopies(redundantCopies).create());
    getCache().createRegion(CUSTOMER, af.create());
    af.setPartitionAttributes(new PartitionAttributesFactory<>().setTotalNumBuckets(4)
        .setLocalMaxMemory(accessor ? 0 : 1)
        .setPartitionResolver(new CustomerIDPartitionResolver("resolver2"))
        .setRedundantCopies(redundantCopies).setColocatedWith(CUSTOMER).create());
    getCache().createRegion(ORDER, af.create());
  }

  protected boolean getConcurrencyChecksEnabled() {
    return false;
  }

  void populateData() {
    Region<Object, Object> custRegion = getCache().getRegion(CUSTOMER);
    Region<Object, Object> orderRegion = getCache().getRegion(ORDER);
    Region<Object, Object> refRegion = getCache().getRegion(D_REFERENCE);
    for (int i = 0; i < 5; i++) {
      CustId custId = new CustId(i);
      Customer customer = new Customer("customer" + i, "address" + i);
      OrderId orderId = new OrderId(i, custId);
      Order order = new Order("order" + i);
      custRegion.put(custId, customer);
      orderRegion.put(orderId, order);
      refRegion.put(custId, customer);
    }
  }

  protected void initAccessorAndDataStore(VM accessor, VM datastore, final int redundantCopies) {
    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        createRegion(true/* accessor */, redundantCopies, null);
        return null;
      }
    });

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        createRegion(false/* accessor */, redundantCopies, null);
        populateData();
        return null;
      }
    });
  }

  protected void initAccessorAndDataStore(VM accessor, VM datastore1, VM datastore2,
      final int redundantCopies) {
    datastore2.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        createRegion(false/* accessor */, redundantCopies, null);
        return null;
      }
    });

    initAccessorAndDataStore(accessor, datastore1, redundantCopies);
  }

  private void initAccessorAndDataStoreWithInterestPolicy(VM accessor, VM datastore1, VM datastore2,
      final int redundantCopies) {
    datastore2.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        createRegion(false/* accessor */, redundantCopies, InterestPolicy.ALL);
        return null;
      }
    });

    initAccessorAndDataStore(accessor, datastore1, redundantCopies);
  }

  protected class DoOpsInTX extends SerializableCallable<Object> {
    private final OP op;
    Customer expectedCust;
    Customer expectedRefCust = null;
    Order expectedOrder;
    Order expectedOrder2;
    Order expectedOrder3;

    DoOpsInTX(OP op) {
      this.op = op;
    }

    @Override
    public Object call() {
      CacheTransactionManager mgr = getCache().getTxManager();
      LogWriterUtils.getLogWriter().fine("testTXPut starting tx");
      mgr.begin();
      Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
      Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
      Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
      CustId custId = new CustId(1);
      OrderId orderId = new OrderId(1, custId);
      OrderId orderId2 = new OrderId(2, custId);
      OrderId orderId3 = new OrderId(3, custId);
      switch (op) {
        case PUT:
          expectedCust = new Customer("foo", "bar");
          expectedOrder = new Order("fooOrder");
          expectedOrder2 = new Order("fooOrder2");
          expectedOrder3 = new Order("fooOrder3");
          custRegion.put(custId, expectedCust);
          orderRegion.put(orderId, expectedOrder);
          Map<OrderId, Order> orders = new HashMap<OrderId, Order>();
          orders.put(orderId2, expectedOrder2);
          orders.put(orderId3, expectedOrder3);
          getCache().getLogger().fine("SWAP:doingPutAll");
          refRegion.put(custId, expectedCust);
          Set<OrderId> ordersSet = new HashSet<>();
          ordersSet.add(orderId);
          ordersSet.add(orderId2);
          ordersSet.add(orderId3);
          break;
        case GET:
          expectedCust = custRegion.get(custId);
          expectedOrder = orderRegion.get(orderId);
          expectedRefCust = refRegion.get(custId);
          assertThat(expectedCust).isNotNull();
          assertThat(expectedOrder).isNotNull();
          assertThat(expectedRefCust).isNotNull();
          validateContains(custId, Collections.singleton(orderId), true, true);
          break;
        case DESTROY:
          validateContains(custId, Collections.singleton(orderId), true);
          custRegion.destroy(custId);
          orderRegion.destroy(orderId);
          refRegion.destroy(custId);
          validateContains(custId, Collections.singleton(orderId), false);
          break;
        case REMOVEALL:
          validateContains(custId, Collections.singleton(orderId), true);
          custRegion.removeAll(Collections.singleton(custId));
          orderRegion.removeAll(Collections.singleton(orderId));
          refRegion.removeAll(Collections.singleton(custId));
          validateContains(custId, Collections.singleton(orderId), false);
          break;
        case INVALIDATE:
          validateContains(custId, Collections.singleton(orderId), true);
          custRegion.invalidate(custId);
          orderRegion.invalidate(orderId);
          refRegion.invalidate(custId);
          validateContains(custId, Collections.singleton(orderId), true, false);
          break;
        default:
          throw new IllegalStateException();
      }
      return mgr.suspend();
    }
  }

  void validateContains(CustId custId, Set<OrderId> orderId, boolean doesIt) {
    validateContains(custId, orderId, doesIt, doesIt);
  }

  void validateContains(CustId custId, Set<OrderId> ordersSet, boolean containsKey,
      boolean containsValue) {
    Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
    Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
    Region<CustId, Order> refRegion = getCache().getRegion(D_REFERENCE);
    boolean rContainsKC = custRegion.containsKey(custId);
    boolean rContainsKO = containsKey;
    for (OrderId o : ordersSet) {
      getCache().getLogger()
          .fine("SWAP:rContainsKO:" + rContainsKO + " containsKey:" + orderRegion.containsKey(o));
      rContainsKO = rContainsKO && orderRegion.containsKey(o);
    }
    boolean rContainsKR = refRegion.containsKey(custId);

    boolean rContainsVC = custRegion.containsValueForKey(custId);
    boolean rContainsVO = containsValue;
    for (OrderId o : ordersSet) {
      rContainsVO = rContainsVO && orderRegion.containsValueForKey(o);
    }
    boolean rContainsVR = refRegion.containsValueForKey(custId);

    assertThat(containsKey).isEqualTo(rContainsKC);
    assertThat(containsKey).isEqualTo(rContainsKO);
    assertThat(containsKey).isEqualTo(rContainsKR);
    assertThat(containsValue).isEqualTo(rContainsVR);
    assertThat(containsValue).isEqualTo(rContainsVC);
    assertThat(containsValue).isEqualTo(rContainsVO);

    if (containsKey) {
      Region.Entry eC = custRegion.getEntry(custId);
      for (OrderId o : ordersSet) {
        assertThat(orderRegion.getEntry(o)).isNotNull();
      }
      Region.Entry eR = refRegion.getEntry(custId);
      assertThat(eC).isNotNull();
      assertThat(eR).isNotNull();
      // assertIndexDetailsEquals(1,custRegion.size());
      // assertIndexDetailsEquals(1,orderRegion.size());
      // assertIndexDetailsEquals(1,refRegion.size());

    } else {
      // assertIndexDetailsEquals(0,custRegion.size());
      // assertIndexDetailsEquals(0,orderRegion.size());
      // assertIndexDetailsEquals(0,refRegion.size());
      try {
        Region.Entry eC = custRegion.getEntry(custId);
        assertThat(eC).as("should have had an EntryNotFoundException:" + eC).isNull();

      } catch (EntryNotFoundException enfe) {
        // this is what we expect
      }
      try {
        for (OrderId o : ordersSet) {
          assertThat(orderRegion.getEntry(o))
              .as("should have had an EntryNotFoundException:" + orderRegion.getEntry(o))
              .isNull();
        }
      } catch (EntryNotFoundException enfe) {
        // this is what we expect
      }
      try {
        Region.Entry eR = refRegion.getEntry(custId);
        assertThat(eR).as("should have had an EntryNotFoundException:" + eR).isNull();
      } catch (EntryNotFoundException enfe) {
        // this is what we expect
      }

    }

  }


  void verifyAfterCommit(OP op) {
    Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
    Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
    Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
    CustId custId = new CustId(1);
    OrderId orderId = new OrderId(1, custId);
    OrderId orderId2 = new OrderId(2, custId);
    OrderId orderId3 = new OrderId(3, custId);
    Customer expectedCust;
    Order expectedOrder;
    Order expectedOrder2;
    Order expectedOrder3;
    Customer expectedRef;
    switch (op) {
      case PUT:
        expectedCust = new Customer("foo", "bar");
        expectedOrder = new Order("fooOrder");
        expectedOrder2 = new Order("fooOrder2");
        expectedOrder3 = new Order("fooOrder3");
        expectedRef = expectedCust;
        assertThat(custRegion.getEntry(custId)).isNotNull();
        assertThat(expectedCust).isEqualTo(custRegion.getEntry(custId).getValue());
        /*
         * assertThat(orderRegion.getEntry(orderId)).isNotNull();
         * assertIndexDetailsEquals(expectedOrder,
         * orderRegion.getEntry(orderId).getValue());
         *
         * assertThat(orderRegion.getEntry(orderId2)).isNotNull();
         * assertIndexDetailsEquals(expectedOrder2,
         * orderRegion.getEntry(orderId2).getValue());
         *
         * assertThat(orderRegion.getEntry(orderId3)).isNotNull();
         * assertIndexDetailsEquals(expectedOrder3,
         * orderRegion.getEntry(orderId3).getValue());
         */
        assertThat(refRegion.getEntry(custId)).isNotNull();
        assertThat(expectedRef).isEqualTo(refRegion.getEntry(custId).getValue());

        // Set<OrderId> ordersSet = new HashSet<>();
        // ordersSet.add(orderId);ordersSet.add(orderId2);ordersSet.add(orderId3);
        // validateContains(custId, ordersSet, true);
        break;
      case GET:
        expectedCust = custRegion.get(custId);
        expectedOrder = orderRegion.get(orderId);
        expectedRef = refRegion.get(custId);
        validateContains(custId, Collections.singleton(orderId), true);
        break;
      case DESTROY:
        assertThat(!custRegion.containsKey(custId)).isTrue();
        assertThat(!orderRegion.containsKey(orderId)).isTrue();
        assertThat(!refRegion.containsKey(custId)).isTrue();
        validateContains(custId, Collections.singleton(orderId), false);
        break;
      case INVALIDATE:
        boolean validateContainsKey = true;
        if (!((GemFireCacheImpl) custRegion.getCache()).isClient()) {
          assertThat(custRegion.containsKey(custId)).isTrue();
          assertThat(orderRegion.containsKey(orderId)).isTrue();
          assertThat(refRegion.containsKey(custId)).isTrue();
        }
        assertThat(custRegion.get(custId)).isNull();
        assertThat(orderRegion.get(orderId)).isNull();
        assertThat(refRegion.get(custId)).isNull();
        validateContains(custId, Collections.singleton(orderId), validateContainsKey, false);
        break;
      default:
        throw new IllegalStateException();
    }
  }


  void verifyAfterRollback(OP op) {
    Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
    Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
    Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
    assertThat(custRegion).isNotNull();
    assertThat(orderRegion).isNotNull();
    assertThat(refRegion).isNotNull();

    CustId custId = new CustId(1);
    OrderId orderId = new OrderId(1, custId);
    OrderId orderId2 = new OrderId(2, custId);
    OrderId orderId3 = new OrderId(3, custId);
    Customer expectedCust;
    Order expectedOrder;
    Customer expectedRef;
    switch (op) {
      case PUT:
        expectedCust = new Customer("customer1", "address1");
        expectedOrder = new Order("order1");
        expectedRef = new Customer("customer1", "address1");
        assertThat(expectedCust).isEqualTo(custRegion.getEntry(custId).getValue());
        assertThat(expectedOrder).isEqualTo(orderRegion.getEntry(orderId).getValue());
        getCache().getLogger().info("SWAP:verifyRollback:" + orderRegion);
        getCache().getLogger().info("SWAP:verifyRollback:" + orderRegion.getEntry(orderId2));
        assertThat(getCache().getTXMgr().getTXState()).isNull();
        assertThat(orderRegion.getEntry(orderId2))
            .as("" + orderRegion.getEntry(orderId2)).isNull();
        assertThat(orderRegion.getEntry(orderId3)).isNull();
        assertThat(orderRegion.get(orderId2)).isNull();
        assertThat(orderRegion.get(orderId3)).isNull();
        assertThat(expectedRef).isEqualTo(refRegion.getEntry(custId).getValue());
        validateContains(custId, Collections.singleton(orderId), true);
        break;
      case GET:
        expectedCust = custRegion.getEntry(custId).getValue();
        expectedOrder = orderRegion.getEntry(orderId).getValue();
        expectedRef = refRegion.getEntry(custId).getValue();
        validateContains(custId, Collections.singleton(orderId), true);
        break;
      case DESTROY:
        assertThat(custRegion.containsKey(custId)).isFalse();
        assertThat(orderRegion.containsKey(orderId)).isFalse();
        assertThat(refRegion.containsKey(custId)).isFalse();
        validateContains(custId, Collections.singleton(orderId), true);
        break;
      case INVALIDATE:
        assertThat(custRegion.containsKey(custId)).isTrue();
        assertThat(orderRegion.containsKey(orderId)).isTrue();
        assertThat(refRegion.containsKey(custId)).isTrue();
        assertThat(custRegion.get(custId)).isNull();
        assertThat(orderRegion.get(orderId)).isNull();
        assertThat(refRegion.get(custId)).isNull();
        validateContains(custId, Collections.singleton(orderId), true, true);
        break;
      default:
        throw new IllegalStateException();
    }
  }


  @Test
  public void testTXCreationAndCleanupAtCommit() {
    doBasicChecks(true);
  }

  @Test
  public void testTXCreationAndCleanupAtRollback() {
    doBasicChecks(false);
  }

  private void doBasicChecks(final boolean commit) {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(accessor, datastore, 0);

    final TXId txId = (TXId) accessor.invoke(new DoOpsInTX(OP.PUT));

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        assertThat(mgr).isNotNull();
        assertThat(mgr.isHostedTxInProgress(txId)).isTrue();
        return null;
      }
    });

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.resume(txId);
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(tx).isNotNull();
        mgr.unpauseTransaction(tx);
        if (commit) {
          mgr.commit();
        } else {
          mgr.rollback();
        }
        return null;
      }
    });

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        assertThat(mgr.isHostedTxInProgress(txId)).isFalse();
        return null;
      }
    });
    if (commit) {
      accessor.invoke(new SerializableCallable<Object>() {
        @Override
        public Object call() {
          verifyAfterCommit(OP.PUT);
          return null;
        }
      });
    } else {
      accessor.invoke(new SerializableCallable<Object>() {
        @Override
        public Object call() {
          verifyAfterRollback(OP.PUT);
          return null;
        }
      });
    }
  }

  @Test
  public void testPRTXGet() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(accessor, datastore, 0);

    final TXId txId = (TXId) accessor.invoke(new DoOpsInTX(OP.GET));

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        assertThat(mgr.isHostedTxInProgress(txId)).isTrue();
        TXStateProxy tx = mgr.getHostedTXState(txId);
        System.out.println("TXRS:" + tx.getRegions());
        assertThat(tx.getRegions().size()).isEqualTo(3);// 2 buckets for the two puts we
        // did in the accessor
        // plus the dist. region
        for (InternalRegion r : tx.getRegions()) {
          assertThat(r instanceof DistributedRegion).isTrue();
          TXRegionState rs = tx.readRegion(r);
          for (Object key : rs.getEntryKeys()) {
            TXEntryState es = rs.readEntry(key);
            assertThat(es.getValue(key, r, false)).isNotNull();
            assertThat(es.isDirty()).isFalse();
          }
        }
        return null;
      }
    });

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        CacheTransactionManager mgr = getCache().getTxManager();
        mgr.resume(txId);
        mgr.commit();
        verifyAfterCommit(OP.GET);
        return null;
      }
    });
  }

  @Test
  public void testPRTXGetOnRemoteWithLoader() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(accessor, datastore, 0);

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        AttributesMutator<Object, Object> am =
            getCache().getRegion(CUSTOMER).getAttributesMutator();
        am.setCacheLoader(new CacheLoader() {
          @Override
          public Object load(LoaderHelper helper) throws CacheLoaderException {
            return new Customer("sup dawg", "add");
          }

          @Override
          public void close() {}
        });
        return null;
      }
    });

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.begin();
        Region<Object, Object> cust = getCache().getRegion(CUSTOMER);
        Customer s = (Customer) cust.get(new CustId(8));
        assertThat(new Customer("sup dawg", "add")).isEqualTo(s);
        assertThat(cust.containsKey(new CustId(8))).isTrue();
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(cust.containsKey(new CustId(8))).isFalse();
        mgr.unpauseTransaction(tx);
        mgr.commit();
        Customer s2 = (Customer) cust.get(new CustId(8));
        Customer ex = new Customer("sup dawg", "add");
        assertThat(ex).isEqualTo(s);
        assertThat(ex).isEqualTo(s2);
        return null;
      }
    });
  }

  /**
   * Make sure that getEntry returns null properly and values when it should
   */
  @Test
  public void testPRTXGetEntryOnRemoteSide() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(accessor, datastore, 0);

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        return null;
      }
    });

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region<Object, Object> cust = getCache().getRegion(CUSTOMER);
        CustId sup = new CustId(7);
        Region.Entry e = cust.getEntry(sup);
        assertThat(e).isNull();
        CustId custId = new CustId(5);
        cust.put(custId, new Customer("customer5", "address5"));

        Region.Entry ee = cust.getEntry(custId);
        assertThat(ee).isNotNull();

        CacheTransactionManager mgr = getCache().getTxManager();
        mgr.begin();
        Region.Entry e2 = cust.getEntry(sup);
        assertThat(e2).isNull();
        mgr.commit();
        Region.Entry e3 = cust.getEntry(sup);
        assertThat(e3).isNull();

        mgr.begin();
        Customer dawg = new Customer("dawg", "dawgaddr");
        cust.put(sup, dawg);
        Region.Entry e4 = cust.getEntry(sup);
        assertThat(e4).isNotNull();
        assertThat(dawg).isEqualTo(e4.getValue());
        mgr.commit();

        Region.Entry e5 = cust.getEntry(sup);
        assertThat(e5).isNotNull();
        assertThat(dawg).isEqualTo(e5.getValue());
        return null;
      }
    });
  }


  @Test
  public void testPRTXGetOnLocalWithLoader() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(accessor, datastore, 0);

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        AttributesMutator<Object, Object> am =
            getCache().getRegion(CUSTOMER).getAttributesMutator();
        am.setCacheLoader(new CacheLoader<Object, Object>() {
          @Override
          public Object load(LoaderHelper<Object, Object> helper) throws CacheLoaderException {
            return new Customer("sup dawg", "addr");
          }

          @Override
          public void close() {}
        });
        CacheTransactionManager mgr = getCache().getTxManager();
        mgr.begin();
        Region cust = getCache().getRegion(CUSTOMER);
        CustId custId = new CustId(6);
        Customer s = (Customer) cust.get(custId);
        mgr.commit();
        Customer s2 = (Customer) cust.get(custId);
        Customer expectedCust = new Customer("sup dawg", "addr");
        assertThat(s).isEqualTo(expectedCust);
        assertThat(s2).isEqualTo(expectedCust);
        return null;
      }
    });

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        return null;
      }
    });
  }


  @Test
  public void testTXPut() {
    Host host = Host.getHost(0);
    VM acc = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(acc, datastore, 0);
    VM accessor = getVMForTransactions(acc, datastore);

    final TXId txId = (TXId) accessor.invoke(new DoOpsInTX(OP.PUT));

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        assertThat(mgr.isHostedTxInProgress(txId)).isTrue();
        TXStateProxy tx = mgr.getHostedTXState(txId);
        assertThat(tx.getRegions().size()).isEqualTo(3);// 2 buckets for the two puts we
        // did in the accessor
        // +1 for the dist_region
        for (InternalRegion r : tx.getRegions()) {
          assertThat(r instanceof DistributedRegion).isTrue();
          TXRegionState rs = tx.readRegion(r);
          for (Object key : rs.getEntryKeys()) {
            TXEntryState es = rs.readEntry(key);
            assertThat(es.getValue(key, r, false)).isNotNull();
            assertThat(es.isDirty()).isTrue();
          }
        }
        return null;
      }
    });
    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.resume(txId);
        assertThat(mgr.getTXState()).isNotNull();
        getCache().getLogger().fine("SWAP:accessorTXState:" + mgr.getTXState());
        mgr.commit();
        verifyAfterCommit(OP.PUT);
        assertThat(mgr.getTXState()).isNull();
        return null;
      }
    });
  }

  @Test
  public void testTXInvalidate() {
    Host host = Host.getHost(0);
    VM acc = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(acc, datastore, 0);
    VM accessor = getVMForTransactions(acc, datastore);

    final TXId txId = (TXId) accessor.invoke(new DoOpsInTX(OP.INVALIDATE));

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        assertThat(mgr.isHostedTxInProgress(txId)).isTrue();
        TXStateProxy tx = mgr.getHostedTXState(txId);
        assertThat(tx.getRegions().size()).isEqualTo(3);// 2 buckets for the two puts we
        // did in the accessor
        // plus the dist. region
        for (InternalRegion r : tx.getRegions()) {
          assertThat(r instanceof DistributedRegion).isTrue();
          TXRegionState rs = tx.readRegion(r);
          for (Object key : rs.getEntryKeys()) {
            TXEntryState es = rs.readEntry(key);
            assertThat(es.getValue(key, r, false)).isNotNull();
            assertThat(es.isDirty()).isTrue();
          }
        }
        return null;
      }
    });
    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        CacheTransactionManager mgr = getCache().getTxManager();
        mgr.resume(txId);
        mgr.commit();
        verifyAfterCommit(OP.INVALIDATE);
        return null;
      }
    });
  }


  @Test
  public void testTXDestroy() {
    Host host = Host.getHost(0);
    VM acc = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(acc, datastore, 0);
    VM accessor = getVMForTransactions(acc, datastore);

    final TXId txId = (TXId) accessor.invoke(new DoOpsInTX(OP.DESTROY));

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        assertThat(mgr.isHostedTxInProgress(txId)).isTrue();
        TXStateProxy tx = mgr.getHostedTXState(txId);
        assertThat(tx.getRegions().size()).isEqualTo(3);// 2 buckets for the two puts we
        // did in the accessor
        // plus the dist. region
        for (InternalRegion r : tx.getRegions()) {
          assertThat(r instanceof DistributedRegion);
          TXRegionState rs = tx.readRegion(r);
          for (Object key : rs.getEntryKeys()) {
            TXEntryState es = rs.readEntry(key);
            assertThat(es.getValue(key, r, false)).isNull();
            assertThat(es.isDirty()).isTrue();
          }
        }
        return null;
      }
    });
    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        CacheTransactionManager mgr = getCache().getTxManager();
        mgr.resume(txId);
        mgr.commit();
        verifyAfterCommit(OP.DESTROY);
        return null;
      }
    });
  }

  @Test
  public void testTxPutIfAbsent() {
    Host host = Host.getHost(0);
    VM acc = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(acc, datastore, 0);
    VM accessor = getVMForTransactions(acc, datastore);

    final CustId newCustId = new CustId(10);
    final Customer updateCust = new Customer("customer10", "address10");
    final TXId txId = (TXId) accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.begin();
        Region<CustId, Customer> cust = getCache().getRegion(CUSTOMER);
        Region<CustId, Customer> rr = getCache().getRegion(D_REFERENCE);
        Customer expectedCust = new Customer("customer" + 1, "address" + 1);
        getCache().getLogger().fine("SWAP:doingPutIfAbsent");
        CustId oldCustId = new CustId(1);
        Customer old = cust.putIfAbsent(oldCustId, updateCust);
        assertThat(expectedCust).as("expected:" + expectedCust + " but was " + old)
            .isEqualTo(old);
        // transaction should be bootstrapped
        old = rr.putIfAbsent(oldCustId, updateCust);
        assertThat(expectedCust).as("expected:" + expectedCust + " but was " + old).isEqualTo(old);
        // now a key that does not exist
        old = cust.putIfAbsent(newCustId, updateCust);
        assertThat(old).isNull();
        old = rr.putIfAbsent(newCustId, updateCust);
        assertThat(old).isNull();
        Region<OrderId, Order> order = getCache().getRegion(ORDER);
        Order oldOrder = order.putIfAbsent(new OrderId(10, newCustId), new Order("order10"));
        assertThat(old).isNull();
        assertThat(oldOrder).isNull();
        assertThat(cust.get(newCustId)).isNotNull();
        assertThat(rr.get(newCustId)).isNotNull();
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(cust.get(newCustId)).isNull();
        assertThat(rr.get(newCustId)).isNull();
        mgr.unpauseTransaction(tx);
        cust.put(oldCustId, new Customer("foo", "bar"));
        rr.put(oldCustId, new Customer("foo", "bar"));
        return mgr.suspend();
      }
    });

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region cust = getCache().getRegion(CUSTOMER);
        int hash1 = PartitionedRegionHelper.getHashKey((PartitionedRegion) cust, new CustId(1));
        int hash10 = PartitionedRegionHelper.getHashKey((PartitionedRegion) cust, new CustId(10));
        TXManagerImpl mgr = getCache().getTxManager();
        assertThat(mgr.isHostedTxInProgress(txId)).isTrue();
        TXStateProxy tx = mgr.getHostedTXState(txId);
        assertThat(tx.getRegions().size()).isEqualTo(3 + (hash1 == hash10 ? 0 : 1));// 2 buckets for
        // the two puts we did in the accessor one dist. region, and one more bucket if Cust1 and
        // Cust10 resolve to different buckets
        for (InternalRegion r : tx.getRegions()) {
          assertThat(r instanceof DistributedRegion).isTrue();
          TXRegionState rs = tx.readRegion(r);
          for (Object key : rs.getEntryKeys()) {
            TXEntryState es = rs.readEntry(key);
            assertThat(es.getValue(key, r, false)).isNotNull();
            assertThat(es.isDirty()).as("key:" + key + " r:" + r.getFullPath()).isTrue();
          }
        }
        return null;
      }
    });
    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.resume(txId);
        mgr.commit();
        Region<CustId, Customer> cust = getCache().getRegion(CUSTOMER);
        Region<CustId, Customer> rr = getCache().getRegion(D_REFERENCE);
        assertThat(updateCust).isEqualTo(cust.get(newCustId));
        assertThat(updateCust).isEqualTo(rr.get(newCustId));
        // test conflict
        mgr.begin();
        CustId conflictCust = new CustId(11);
        cust.putIfAbsent(conflictCust, new Customer("name11", "address11"));
        TXStateProxy tx = mgr.pauseTransaction();
        cust.put(conflictCust, new Customer("foo", "bar"));
        mgr.unpauseTransaction(tx);
        assertThatThrownBy(mgr::commit).isInstanceOf(CommitConflictException.class);
        return null;
      }
    });

  }

  public VM getVMForTransactions(VM accessor, VM datastore) {
    return accessor;
  }

  @Test
  public void testTxRemove() {
    Host host = Host.getHost(0);
    VM acc = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(acc, datastore, 0);
    VM accessor = getVMForTransactions(acc, datastore);

    final CustId custId = new CustId(1);
    final TXId txId = (TXId) accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region<CustId, Customer> cust = getCache().getRegion(CUSTOMER);
        Region<CustId, Customer> ref = getCache().getRegion(D_REFERENCE);
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.begin();
        Customer customer = new Customer("customer1", "address1");
        Customer fakeCust = new Customer("foo", "bar");
        assertThat(cust.remove(custId, fakeCust)).isFalse();
        assertThat(cust.remove(custId, customer)).isTrue();
        assertThat(ref.remove(custId, fakeCust)).isFalse();
        assertThat(ref.remove(custId, customer)).isTrue();
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(cust.get(custId)).isNotNull();
        assertThat(ref.get(custId)).isNotNull();
        mgr.unpauseTransaction(tx);
        return mgr.suspend();
      }
    });
    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        assertThat(mgr.isHostedTxInProgress(txId)).isTrue();
        TXStateProxy tx = mgr.getHostedTXState(txId);
        assertThat(tx.getRegions().size()).isEqualTo(2);// 2 buckets for the two puts we
        // did in the accessor one dist. region, and one more bucket if Cust1 and Cust10 resolve to
        // different buckets
        for (InternalRegion r : tx.getRegions()) {
          assertThat(r instanceof DistributedRegion).isTrue();
          TXRegionState rs = tx.readRegion(r);
          for (Object key : rs.getEntryKeys()) {
            TXEntryState es = rs.readEntry(key);
            assertThat(es.getValue(key, r, false)).isNull();
            assertThat(es.isDirty()).as("key:" + key + " r:" + r.getFullPath()).isTrue();
          }
        }
        return null;
      }
    });
    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.resume(txId);
        mgr.commit();
        Region<CustId, Customer> cust = getCache().getRegion(CUSTOMER);
        Region<CustId, Customer> rr = getCache().getRegion(D_REFERENCE);
        assertThat(cust.get(custId)).isNull();
        assertThat(rr.get(custId)).isNull();
        // check conflict
        mgr.begin();
        CustId conflictCust = new CustId(2);
        Customer customer = new Customer("customer2", "address2");
        getCache().getLogger().fine("SWAP:removeConflict");
        assertThat(cust.remove(conflictCust, customer)).isTrue();
        TXStateProxy tx = mgr.pauseTransaction();
        cust.put(conflictCust, new Customer("foo", "bar"));
        mgr.unpauseTransaction(tx);
        assertThatThrownBy(mgr::commit).isInstanceOf(CommitConflictException.class);
        return null;
      }
    });
  }

  @Test
  public void testTxRemoveAll() {
    Host host = Host.getHost(0);
    VM acc = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(acc, datastore, 0);
    VM accessor = getVMForTransactions(acc, datastore);

    final CustId custId1 = new CustId(1);
    final CustId custId2 = new CustId(2);
    final CustId custId20 = new CustId(20);
    final TXId txId = (TXId) accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region<CustId, Customer> cust = getCache().getRegion(CUSTOMER);
        Region<CustId, Customer> ref = getCache().getRegion(D_REFERENCE);
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.begin();
        Customer customer = new Customer("customer1", "address1");
        Customer customer2 = new Customer("customer2", "address2");
        Customer fakeCust = new Customer("foo2", "bar2");
        cust.removeAll(Arrays.asList(custId1, custId2, custId20));
        ref.removeAll(Arrays.asList(custId1, custId2, custId20));
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(cust.get(custId1)).isNotNull();
        assertThat(ref.get(custId2)).isNotNull();
        mgr.unpauseTransaction(tx);
        return mgr.suspend();
      }
    });
    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        assertThat(mgr.isHostedTxInProgress(txId)).isTrue();
        TXStateProxy tx = mgr.getHostedTXState(txId);
        assertThat(tx.getRegions().size()).isEqualTo(4);// 2 buckets for the two puts we
        // did in the accessor one dist. region, and one more bucket if Cust1 and Cust10 resolve to
        // different buckets
        for (InternalRegion r : tx.getRegions()) {
          assertThat(r instanceof DistributedRegion).isTrue();
          TXRegionState rs = tx.readRegion(r);
          for (Object key : rs.getEntryKeys()) {
            TXEntryState es = rs.readEntry(key);
            assertThat(es.getValue(key, r, false)).isNull();
            // custId20 won't be dirty because it doesn't exist.
            assertThat(key.equals(custId20) || es.isDirty())
                .as("key:" + key + " r:" + r.getFullPath()).isTrue();
          }
        }
        return null;
      }
    });
    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.resume(txId);
        mgr.commit();
        Region<CustId, Customer> cust = getCache().getRegion(CUSTOMER);
        Region<CustId, Customer> rr = getCache().getRegion(D_REFERENCE);
        assertThat(cust.get(custId1)).isNull();
        assertThat(rr.get(custId2)).isNull();
        // check conflict
        mgr.begin();
        CustId custId3 = new CustId(3);
        CustId custId4 = new CustId(4);
        getCache().getLogger().fine("SWAP:removeConflict");
        cust.removeAll(Arrays.asList(custId3, custId20, custId4));
        TXStateProxy tx = mgr.pauseTransaction();
        // cust.put(custId3, new Customer("foo", "bar"));
        cust.put(custId20, new Customer("foo", "bar"));
        assertThat(cust.get(custId20)).isNotNull();
        cust.put(custId4, new Customer("foo", "bar"));
        mgr.unpauseTransaction(tx);
        assertThatThrownBy(mgr::commit).isInstanceOf(CommitConflictException.class);
        assertThat(cust.get(custId3)).isNotNull();
        assertThat(cust.get(custId4)).isNotNull();
        assertThat(cust.get(custId20)).isNotNull();

        // Test a removeall an already missing key.
        // custId2 has already been removed
        mgr.begin();
        getCache().getLogger().fine("SWAP:removeConflict");
        cust.removeAll(Arrays.asList(custId2, custId3));
        tx = mgr.pauseTransaction();
        cust.put(custId2, new Customer("foo", "bar"));
        mgr.unpauseTransaction(tx);
        mgr.commit();
        assertThat(cust.get(custId2)).isNotNull();
        assertThat(cust.get(custId3)).isNull();
        return null;
      }
    });
  }

  @Test
  public void testTxRemoveAllNotColocated() {
    Host host = Host.getHost(0);
    VM acc = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(3);
    datastore2.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        createRegion(false/* accessor */, 0, null);
        return null;
      }
    });
    initAccessorAndDataStore(acc, datastore1, 0);
    VM accessor = getVMForTransactions(acc, datastore1);

    final CustId custId0 = new CustId(0);
    final CustId custId1 = new CustId(1);
    final CustId custId2 = new CustId(2);
    final CustId custId3 = new CustId(3);
    final CustId custId4 = new CustId(4);
    final CustId custId20 = new CustId(20);
    final TXId txId = (TXId) accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region<CustId, Customer> cust = getCache().getRegion(CUSTOMER);
        Region<CustId, Customer> ref = getCache().getRegion(D_REFERENCE);
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.begin();
        Customer customer = new Customer("customer1", "address1");
        Customer customer2 = new Customer("customer2", "address2");
        Customer fakeCust = new Customer("foo2", "bar2");
        assertThatThrownBy(() -> cust
            .removeAll(Arrays.asList(custId0, custId4, custId1, custId2, custId3, custId20)))
                .isInstanceOf(TransactionDataNotColocatedException.class);
        mgr.rollback();
        assertThat(cust.get(custId0)).isNotNull();
        assertThat(cust.get(custId1)).isNotNull();
        assertThat(cust.get(custId2)).isNotNull();
        assertThat(cust.get(custId3)).isNotNull();
        assertThat(cust.get(custId4)).isNotNull();
        return mgr.getTransactionId();
      }
    });
  }

  @Test
  public void testTxRemoveAllWithRedundancy() {
    Host host = Host.getHost(0);
    VM acc = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(3);

    // Create a second data store.
    datastore2.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        createRegion(false/* accessor */, 1, null);
        return null;
      }
    });

    initAccessorAndDataStore(acc, datastore1, 1);
    VM accessor = getVMForTransactions(acc, datastore1);

    // There are 4 buckets, so 0, 4, and 20 are all colocated
    final CustId custId0 = new CustId(0);
    final CustId custId4 = new CustId(4);
    final CustId custId20 = new CustId(20);
    final TXId txId = (TXId) accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region<CustId, Customer> cust = getCache().getRegion(CUSTOMER);
        Region<CustId, Customer> ref = getCache().getRegion(D_REFERENCE);
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.begin();
        cust.removeAll(Arrays.asList(custId0, custId4));
        mgr.commit();
        assertThat(cust.get(custId0)).isNull();
        assertThat(cust.get(custId4)).isNull();
        return mgr.getTransactionId();
      }
    });

    SerializableCallable<Object> checkArtifacts = new SerializableCallable<Object>() {
      @Override
      public Object call() {
        PartitionedRegion cust = (PartitionedRegion) getCache().getRegion(CUSTOMER);
        assertThat(cust.get(custId0)).isNull();
        assertThat(cust.get(custId4)).isNull();
        return null;
      }
    };
    datastore1.invoke(checkArtifacts);
    datastore2.invoke(checkArtifacts);
  }

  @Test
  public void testTxReplace() {
    Host host = Host.getHost(0);
    VM acc = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(acc, datastore, 0);
    VM accessor = getVMForTransactions(acc, datastore);

    final CustId custId = new CustId(1);
    final Customer updatedCust = new Customer("updated", "updated");
    final TXId txId = (TXId) accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region<CustId, Customer> cust = getCache().getRegion(CUSTOMER);
        Region<CustId, Customer> ref = getCache().getRegion(D_REFERENCE);
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.begin();
        Customer customer = new Customer("customer1", "address1");
        Customer fakeCust = new Customer("foo", "bar");
        assertThat(cust.replace(custId, fakeCust, updatedCust)).isFalse();
        assertThat(cust.replace(custId, customer, updatedCust)).isTrue();
        assertThat(ref.replace(custId, fakeCust, updatedCust)).isFalse();
        assertThat(ref.replace(custId, customer, updatedCust)).isTrue();
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(cust.get(custId)).isEqualTo(customer);
        assertThat(ref.get(custId)).isEqualTo(customer);
        mgr.unpauseTransaction(tx);
        return mgr.suspend();
      }
    });
    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        assertThat(mgr.isHostedTxInProgress(txId)).isTrue();
        TXStateProxy tx = mgr.getHostedTXState(txId);
        assertThat(tx.getRegions().size()).isEqualTo(2);// 2 buckets for the two puts we
        // did in the accessor one dist. region, and one more bucket if Cust1 and Cust10 resolve to
        // different buckets
        for (InternalRegion r : tx.getRegions()) {
          assertThat(r instanceof DistributedRegion).isTrue();
          TXRegionState rs = tx.readRegion(r);
          for (Object key : rs.getEntryKeys()) {
            TXEntryState es = rs.readEntry(key);
            assertThat(es.getValue(key, r, false)).isNotNull();
            assertThat(es.isDirty()).as("key:" + key + " r:" + r.getFullPath()).isTrue();
          }
        }
        return null;
      }
    });
    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.resume(txId);
        mgr.commit();
        Region<CustId, Customer> cust = getCache().getRegion(CUSTOMER);
        Region<CustId, Customer> rr = getCache().getRegion(D_REFERENCE);
        assertThat(updatedCust).isEqualTo(cust.get(custId));
        assertThat(updatedCust).isEqualTo(rr.get(custId));
        // check conflict
        mgr.begin();
        CustId conflictCust = new CustId(2);
        Customer customer = new Customer("customer2", "address2");
        getCache().getLogger().fine("SWAP:removeConflict");
        assertThat(cust.replace(conflictCust, customer, new Customer("conflict", "conflict")))
            .isTrue();
        TXStateProxy tx = mgr.pauseTransaction();
        cust.put(conflictCust, new Customer("foo", "bar"));
        mgr.unpauseTransaction(tx);
        assertThatThrownBy(mgr::commit).isInstanceOf(CommitConflictException.class);
        return null;
      }
    });
  }


  /**
   * When we have narrowed down on a target node for a transaction, test that we throw an exception
   * if that node does not host primary for subsequent entries
   */
  @Test
  public void testNonColocatedTX() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);

    datastore2.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        createRegion(false, 1, null);
        return null;
      }
    });

    initAccessorAndDataStore(accessor, datastore1, 1);

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        CacheTransactionManager mgr = getCache().getTxManager();
        mgr.begin();
        assertThatThrownBy(() -> put10Entries(custRegion, orderRegion))
            .isInstanceOf(TransactionDataNotColocatedException.class);
        mgr.rollback();
        put10Entries(custRegion, orderRegion);

        mgr.begin();

        assertThatThrownBy(() -> put10Entries(custRegion, orderRegion))
            .isInstanceOf(TransactionDataNotColocatedException.class);
        mgr.rollback();
        return null;
      }

      private void put10Entries(Region custRegion, Region orderRegion) {
        for (int i = 0; i < 10; i++) {
          CustId custId = new CustId(i);
          Customer customer = new Customer("customer" + i, "address" + i);
          OrderId orderId = new OrderId(i, custId);
          Order order = new Order("order" + i);
          custRegion.put(custId, customer);
          orderRegion.put(orderId, order);
        }
      }
    });
  }

  @Test
  public void testListenersForPut() {
    doTestListeners(OP.PUT);
  }

  @Test
  public void testListenersForDestroy() {
    doTestListeners(OP.DESTROY);
  }

  @Test
  public void testListenersForInvalidate() {
    doTestListeners(OP.INVALIDATE);
  }

  @Test
  public void testListenersForRemoveAll() {
    doTestListeners(OP.REMOVEALL);
  }

  private void doTestListeners(final OP op) {
    Host host = Host.getHost(0);
    VM acc = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(acc, datastore, 0);
    VM accessor = getVMForTransactions(acc, datastore);

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region ref = getCache().getRegion(D_REFERENCE);
        ref.getAttributesMutator().addCacheListener(new TestCacheListener(true));
        ref.getAttributesMutator().setCacheWriter(new TestCacheWriter(true));
        Region cust = getCache().getRegion(CUSTOMER);
        cust.getAttributesMutator().addCacheListener(new TestCacheListener(true));
        cust.getAttributesMutator().setCacheWriter(new TestCacheWriter(true));
        Region order = getCache().getRegion(ORDER);
        order.getAttributesMutator().addCacheListener(new TestCacheListener(true));
        order.getAttributesMutator().setCacheWriter(new TestCacheWriter(true));
        getCache().getTxManager().addListener(new TestTxListener(true));
        if (!getCache().isClient()) {
          getCache().getTxManager().setWriter(new TestTxWriter(true));
        }
        return null;
      }
    });

    SerializableCallable<Object> addListenersToDataStore = new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region ref = getCache().getRegion(D_REFERENCE);
        ref.getAttributesMutator().addCacheListener(new TestCacheListener(false));
        ref.getAttributesMutator().setCacheWriter(new TestCacheWriter(false));
        Region cust = getCache().getRegion(CUSTOMER);
        cust.getAttributesMutator().addCacheListener(new TestCacheListener(false));
        cust.getAttributesMutator().setCacheWriter(new TestCacheWriter(false));
        Region order = getCache().getRegion(ORDER);
        order.getAttributesMutator().addCacheListener(new TestCacheListener(false));
        order.getAttributesMutator().setCacheWriter(new TestCacheWriter(false));
        getCache().getTxManager().addListener(new TestTxListener(false));
        if (!getCache().isClient()) {
          getCache().getTxManager().setWriter(new TestTxWriter(false));
        }
        return null;
      }
    };
    datastore.invoke(addListenersToDataStore);

    TXId txId = (TXId) accessor.invoke(new DoOpsInTX(op));

    // Invalidate operations don't fire cache writers, so don't assert they were fired.
    if (op != OP.INVALIDATE) {
      // Ensure the cache writer was not fired in accessor
      accessor.invoke(new SerializableCallable<Object>() {
        @Override
        public Object call() {
          Region cust = getCache().getRegion(CUSTOMER);
          assertThat(((TestCacheWriter) cust.getAttributes().getCacheWriter()).wasFired).isFalse();
          Region order = getCache().getRegion(ORDER);
          assertThat(((TestCacheWriter) order.getAttributes().getCacheWriter()).wasFired).isFalse();
          Region ref = getCache().getRegion(D_REFERENCE);
          assertThat(((TestCacheWriter) ref.getAttributes().getCacheWriter()).wasFired).isFalse();
          return null;
        }
      });

      // Ensure the cache writer was fired in the primary
      datastore.invoke(new SerializableCallable<Object>() {
        @Override
        public Object call() {
          Region cust = getCache().getRegion(CUSTOMER);
          assertThat(((TestCacheWriter) cust.getAttributes().getCacheWriter()).wasFired).isTrue();
          Region order = getCache().getRegion(ORDER);
          assertThat(((TestCacheWriter) order.getAttributes().getCacheWriter()).wasFired).isTrue();
          Region ref = getCache().getRegion(D_REFERENCE);
          assertThat(((TestCacheWriter) ref.getAttributes().getCacheWriter()).wasFired).isTrue();
          return null;
        }
      });
    }

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        CacheTransactionManager mgr = getCache().getTxManager();
        mgr.resume(txId);
        mgr.commit();
        return null;
      }
    });

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TestTxListener l = (TestTxListener) getCache().getTxManager().getListener();
        assertThat(l.isListenerInvoked()).isTrue();
        return null;
      }
    });
    SerializableCallable<Object> verifyListeners = new SerializableCallable<Object>() {
      @Override
      public Object call() throws Exception {
        Region cust = getCache().getRegion(CUSTOMER);
        Region order = getCache().getRegion(ORDER);
        throwListenerException(cust);
        throwListenerException(order);
        throwWriterException(cust);
        throwWriterException(order);
        if (!getCache().isClient()) {
          throwTransactionCallbackException();
        }
        return null;
      }

      private void throwTransactionCallbackException() throws Exception {
        TestTxListener l = (TestTxListener) getCache().getTxManager().getListener();
        if (l.ex != null) {
          throw l.ex;
        }
        TestTxWriter w = (TestTxWriter) getCache().getTxManager().getWriter();
        if (w.ex != null) {
          throw w.ex;
        }
      }

      private void throwListenerException(Region r) throws Exception {
        Exception e = null;
        CacheListener listener = r.getAttributes().getCacheListeners()[0];
        if (listener instanceof TestCacheListener) {
          e = ((TestCacheListener) listener).ex;
        } else {
          // e = ((ClientListener)listener).???
        }
        if (e != null) {
          throw e;
        }
      }

      private void throwWriterException(Region r) throws Exception {
        Exception e = null;
        CacheListener listener = r.getAttributes().getCacheListeners()[0];
        if (listener instanceof TestCacheListener) {
          e = ((TestCacheListener) listener).ex;
        } else {
          // e = ((ClientListener)listener).???
        }
        if (e != null) {
          throw e;
        }
      }
    };
    accessor.invoke(verifyListeners);
    datastore.invoke(verifyListeners);
  }

  abstract static class CacheCallback {
    protected boolean isAccessor;
    protected Exception ex = null;

    protected void verifyOrigin(EntryEvent event) {
      try {
        assertThat(!isAccessor).isEqualTo(event.isOriginRemote());
      } catch (Exception e) {
        ex = e;
      }
    }

    protected void verifyPutAll(EntryEvent event) {
      CustId knownCustId = new CustId(1);
      OrderId knownOrderId = new OrderId(2, knownCustId);
      if (event.getKey().equals(knownOrderId)) {
        try {
          assertThat(event.getOperation().isPutAll()).isTrue();
          assertThat(event.getTransactionId()).isNotNull();
        } catch (Exception e) {
          ex = e;
        }
      }
    }

  }

  class TestCacheListener extends CacheCallback implements CacheListener {
    TestCacheListener(boolean isAccessor) {
      this.isAccessor = isAccessor;
    }

    @Override
    public void afterCreate(EntryEvent event) {
      verifyOrigin(event);
      verifyPutAll(event);
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      verifyOrigin(event);
      verifyPutAll(event);
    }

    @Override
    public void afterDestroy(EntryEvent event) {
      verifyOrigin(event);
    }

    @Override
    public void afterInvalidate(EntryEvent event) {
      verifyOrigin(event);
    }

    @Override
    public void afterRegionClear(RegionEvent event) {}

    @Override
    public void afterRegionCreate(RegionEvent event) {}

    @Override
    public void afterRegionDestroy(RegionEvent event) {}

    @Override
    public void afterRegionInvalidate(RegionEvent event) {}

    @Override
    public void afterRegionLive(RegionEvent event) {}

    @Override
    public void close() {}
  }

  class TestCacheWriter extends CacheCallback implements CacheWriter {

    private volatile boolean wasFired = false;

    TestCacheWriter(boolean isAccessor) {
      this.isAccessor = isAccessor;
    }

    @Override
    public void beforeCreate(EntryEvent event) throws CacheWriterException {
      getCache().getLogger()
          .info("SWAP:beforeCreate:" + event + " op:" + event.getOperation());
      verifyOrigin(event);
      verifyPutAll(event);
      setFired();
    }

    public void setFired() {
      wasFired = true;
    }

    @Override
    public void beforeUpdate(EntryEvent event) throws CacheWriterException {
      getCache().getLogger()
          .info("SWAP:beforeCreate:" + event + " op:" + event.getOperation());
      verifyOrigin(event);
      verifyPutAll(event);
      setFired();
    }

    @Override
    public void beforeDestroy(EntryEvent event) throws CacheWriterException {
      verifyOrigin(event);
      setFired();
    }

    @Override
    public void beforeRegionClear(RegionEvent event) throws CacheWriterException {
      setFired();
    }

    @Override
    public void beforeRegionDestroy(RegionEvent event) throws CacheWriterException {
      setFired();
    }

    @Override
    public void close() {}
  }

  abstract class txCallback {
    protected boolean isAccessor;
    protected Exception ex = null;

    protected void verify(TransactionEvent txEvent) {
      for (CacheEvent<?, ?> e : txEvent.getEvents()) {
        verifyOrigin(e);
        verifyPutAll(e);
      }
    }

    private void verifyOrigin(CacheEvent event) {
      try {
        assertThat(event.isOriginRemote()).isTrue(); // change to !isAccessor after fixing #41498
      } catch (Exception e) {
        ex = e;
      }
    }

    private void verifyPutAll(CacheEvent<?, ?> p_event) {
      if (!(p_event instanceof EntryEvent)) {
        return;
      }
      EntryEvent<?, ?> event = (EntryEvent<?, ?>) p_event;
      CustId knownCustId = new CustId(1);
      OrderId knownOrderId = new OrderId(2, knownCustId);
      if (event.getKey().equals(knownOrderId)) {
        try {
          assertThat(event.getOperation().isPutAll()).isTrue();
          assertThat(event.getTransactionId()).isNotNull();
        } catch (Exception e) {
          ex = e;
        }
      }
    }
  }

  class TestTxListener extends txCallback implements TransactionListener {
    private boolean listenerInvoked;

    TestTxListener(boolean isAccessor) {
      this.isAccessor = isAccessor;
    }

    @Override
    public void afterCommit(TransactionEvent event) {
      listenerInvoked = true;
      verify(event);
    }

    @Override
    public void afterFailedCommit(TransactionEvent event) {
      verify(event);
    }

    @Override
    public void afterRollback(TransactionEvent event) {
      listenerInvoked = true;
      verify(event);
    }

    public boolean isListenerInvoked() {
      return listenerInvoked;
    }

    @Override
    public void close() {}
  }

  class TestTxWriter extends txCallback implements TransactionWriter {
    public TestTxWriter(boolean isAccessor) {
      this.isAccessor = isAccessor;
    }

    @Override
    public void beforeCommit(TransactionEvent event) {
      verify(event);
    }

    @Override
    public void close() {}
  }

  @Test
  public void testRemoteExceptionThrown() {
    Host host = Host.getHost(0);
    VM acc = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(acc, datastore, 0);
    VM accessor = getVMForTransactions(acc, datastore);

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        getCache().getTxManager().setWriter(new TransactionWriter() {
          @Override
          public void close() {}

          @Override
          public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
            throw new TransactionWriterException("AssertionError");
          }
        });
        return null;
      }
    });

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        getCache().getTxManager().begin();
        Region r = getCache().getRegion(CUSTOMER);
        r.put(new CustId(8), new Customer("name8", "address8"));
        assertThatThrownBy(() -> getCache().getTxManager().commit())
            .isInstanceOf(CommitConflictException.class);
        return null;
      }
    });
  }

  @Test
  public void testSizeForTXHostedOnRemoteNode() {
    doSizeTest(false);
  }

  @Test
  public void testSizeOnAccessor() {
    doSizeTest(true);
  }

  private void doSizeTest(final boolean isAccessor) {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);
    initAccessorAndDataStore(accessor, datastore1, datastore2, 0);

    VM taskVM = isAccessor ? accessor : datastore1;

    final TXId txId = (TXId) taskVM.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region custRegion = getCache().getRegion(CUSTOMER);
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.begin();
        assertThat(custRegion.size()).isEqualTo(5);
        assertThat(mgr.getTXState()).isNotNull();
        return mgr.suspend();
      }
    });
    datastore1.invoke(verifyNoTxState);
    datastore2.invoke(verifyNoTxState);

    taskVM.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region custRegion = getCache().getRegion(CUSTOMER);
        Region orderRegion = getCache().getRegion(ORDER);
        TXManagerImpl mgr = getCache().getTxManager();
        PartitionedRegion custPR = (PartitionedRegion) custRegion;
        int remoteKey = -1;
        for (int i = 100; i < 200; i++) {
          DistributedMember myId = custPR.getMyId();
          if (!myId.equals(custPR.getOwnerForKey(custPR.getKeyInfo(new CustId(i))))) {
            remoteKey = i;
            break;
          }
        }
        if (remoteKey == -1) {
          throw new IllegalStateException("expected non-negative key");
        }
        mgr.resume(txId);
        assertThat(mgr.getTXState()).isNotNull();
        CustId custId = new CustId(remoteKey);
        OrderId orderId = new OrderId(remoteKey, custId);
        custRegion.put(custId, new Customer("customer" + remoteKey, "address" + remoteKey));
        getCache().getLogger()
            .info("Putting " + custId + ", keyInfo:" + custPR.getKeyInfo(new CustId(remoteKey)));
        orderRegion.put(orderId, new Order("order" + remoteKey));
        assertThat(custRegion.size()).isEqualTo(6);
        mgr.suspend();
        return null;
      }
    });
    final Integer txOnDatastore1 = (Integer) datastore1.invoke(getNumberOfTXInProgress);
    final Integer txOnDatastore2 = (Integer) datastore2.invoke(getNumberOfTXInProgress);
    assertThat(txOnDatastore1 + txOnDatastore2).isOne();

    taskVM.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        CacheTransactionManager mgr = getCache().getTxManager();
        mgr.resume(txId);
        mgr.commit();
        return null;
      }
    });

    datastore1.invoke(verifyNoTxState);
    datastore2.invoke(verifyNoTxState);

    final Integer txOnDatastore1_1 = (Integer) datastore1.invoke(getNumberOfTXInProgress);
    final Integer txOnDatastore2_2 = (Integer) datastore2.invoke(getNumberOfTXInProgress);
    assertThat(txOnDatastore1_1.intValue()).isZero();
    assertThat(txOnDatastore2_2.intValue()).isZero();
  }

  @Test
  public void testKeysIterator() {
    doTestIterator(OP.KEYS, 0, OP.PUT);
  }

  @Test
  public void testValuesIterator() {
    doTestIterator(OP.VALUES, 0, OP.PUT);
  }

  @Test
  public void testEntriesIterator() {
    doTestIterator(OP.ENTRIES, 0, OP.PUT);
  }

  @Test
  public void testKeysIterator1() {
    doTestIterator(OP.KEYS, 1, OP.PUT);
  }

  @Test
  public void testValuesIterator1() {
    doTestIterator(OP.VALUES, 1, OP.PUT);
  }

  @Test
  public void testEntriesIterator1() {
    doTestIterator(OP.ENTRIES, 1, OP.PUT);
  }

  @Test
  public void testKeysIteratorOnDestroy() {
    doTestIterator(OP.KEYS, 0, OP.DESTROY);
  }

  @Test
  public void testValuesIteratorOnDestroy() {
    doTestIterator(OP.VALUES, 0, OP.DESTROY);
  }

  @Test
  public void testEntriesIteratorOnDestroy() {
    doTestIterator(OP.ENTRIES, 0, OP.DESTROY);
  }

  @Test
  public void testKeysIterator1OnDestroy() {
    doTestIterator(OP.KEYS, 1, OP.DESTROY);
  }

  @Test
  public void testValuesIterator1OnDestroy() {
    doTestIterator(OP.VALUES, 1, OP.DESTROY);
  }

  @Test
  public void testEntriesIterator1OnDestroy() {
    doTestIterator(OP.ENTRIES, 1, OP.DESTROY);
  }

  private void doTestIterator(final OP iteratorType, final int redundancy, final OP op) {
    VM accessor = VM.getVM(0);
    VM datastore1 = VM.getVM(1);
    VM datastore2 = VM.getVM(2);
    initAccessorAndDataStore(accessor, datastore1, datastore2, redundancy);

    final TXId txId = (TXId) accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region custRegion = getCache().getRegion(CUSTOMER);
        Set originalSet;
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.begin();
        switch (iteratorType) {
          case KEYS:
            originalSet = getCustIdSet(5);
            assertThat(originalSet.containsAll(custRegion.keySet())).isTrue();
            assertThat(custRegion.keySet().size()).isEqualTo(5);
            break;
          case VALUES:
            originalSet = getCustomerSet(5);
            assertThat(originalSet.containsAll(custRegion.values())).isTrue();
            assertThat(custRegion.values().size()).isEqualTo(5);
            break;
          case ENTRIES:
            Set<CustId> originalKeySet = getCustIdSet(5);
            Set<Customer> originalValueSet = getCustomerSet(5);
            Set<Object> entrySet = new HashSet<Object>();
            Region.Entry entry;
            for (final Object value : custRegion.entrySet()) {
              entrySet.add(value);
            }
            for (final Object o : entrySet) {
              entry = (Entry) o;
              assertThat(originalKeySet.contains(entry.getKey())).isTrue();
              assertThat(originalValueSet.contains(entry.getValue())).isTrue();
            }
            assertThat(custRegion.entrySet().size()).isEqualTo(5);
            break;
          default:
            throw new IllegalArgumentException();
        }
        assertThat(mgr.getTXState()).isNotNull();
        return mgr.suspend();
      }
    });
    datastore1.invoke(verifyNoTxState);
    datastore2.invoke(verifyNoTxState);

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region custRegion = getCache().getRegion(CUSTOMER);
        Region orderRegion = getCache().getRegion(ORDER);

        TXManagerImpl mgr = getCache().getTxManager();
        mgr.resume(txId);
        assertThat(mgr.getTXState()).isNotNull();
        int expectedSetSize = 0;
        switch (op) {
          case PUT:
            CustId custId = new CustId(5);
            OrderId orderId = new OrderId(5, custId);
            custRegion.put(custId, new Customer("customer5", "address5"));
            orderRegion.put(orderId, new Order("order5"));
            expectedSetSize = 6;
            break;
          case DESTROY:
            CustId custId1 = new CustId(4);
            OrderId orderId1 = new OrderId(4, custId1);
            custRegion.destroy(custId1);
            orderRegion.destroy(orderId1);
            expectedSetSize = 4;
            break;
          default:
            throw new IllegalStateException();
        }

        switch (iteratorType) {
          case KEYS:
            assertThat(getCustIdSet(expectedSetSize).containsAll(custRegion.keySet())).isTrue();
            assertThat(expectedSetSize).isEqualTo(custRegion.keySet().size());
            break;
          case VALUES:
            assertThat(getCustomerSet(expectedSetSize).containsAll(custRegion.values())).isTrue();
            assertThat(expectedSetSize).isEqualTo(custRegion.values().size());
            break;
          case ENTRIES:
            Set originalKeySet = getCustIdSet(expectedSetSize);
            Set originalValueSet = getCustomerSet(expectedSetSize);
            Set entrySet = new HashSet();
            Region.Entry entry;
            for (final Object value : custRegion.entrySet()) {
              entrySet.add(value);
            }
            for (final Object o : entrySet) {
              entry = (Entry) o;
              assertThat(originalKeySet.contains(entry.getKey())).isTrue();
              assertThat(originalValueSet.contains(entry.getValue())).isTrue();
            }
            assertThat(expectedSetSize).isEqualTo(custRegion.entrySet().size());
            break;
          default:
            throw new IllegalArgumentException();
        }

        return mgr.suspend();
      }
    });
    final Integer txOnDatastore1 = (Integer) datastore1.invoke(getNumberOfTXInProgress);
    final Integer txOnDatastore2 = (Integer) datastore2.invoke(getNumberOfTXInProgress);
    assertThat(txOnDatastore1 + txOnDatastore2).isOne();

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        CacheTransactionManager mgr = getCache().getTxManager();
        mgr.resume(txId);
        mgr.commit();
        return null;
      }
    });
    final Integer txOnDatastore1_1 = (Integer) datastore1.invoke(getNumberOfTXInProgress);
    final Integer txOnDatastore2_2 = (Integer) datastore2.invoke(getNumberOfTXInProgress);
    assertThat(txOnDatastore1_1 + txOnDatastore2_2).isZero();

    datastore1.invoke(new SerializableCallable<Object>() {
      CustId custId;
      Customer customer;
      PartitionedRegion custRegion;
      int originalSetSize;
      int expectedSetSize;

      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTXMgr();
        custRegion = (PartitionedRegion) getCache().getRegion(CUSTOMER);
        mgr.begin();
        doLocalOp();
        Set expectedSet;
        switch (iteratorType) {
          case KEYS:
            expectedSet = getExpectedCustIdSet();
            assertThat(expectedSet).isEqualTo(custRegion.keySet());
            assertThat(expectedSetSize).isEqualTo(custRegion.keySet().size());
            break;
          case VALUES:
            expectedSet = getExpectedCustomerSet();
            assertThat(expectedSet).isEqualTo(custRegion.values());
            assertThat(expectedSetSize).isEqualTo(custRegion.values().size());
            break;
          case ENTRIES:
            Set originalKeySet = getExpectedCustIdSet();
            Set originalValueSet = getExpectedCustomerSet();
            Set entrySet = new HashSet();
            Region.Entry entry;
            for (final Object value : custRegion.entrySet()) {
              entrySet.add(value);
            }
            for (final Object o : entrySet) {
              entry = (Entry) o;
              assertThat(originalKeySet.contains(entry.getKey())).isTrue();
              assertThat(originalValueSet.contains(entry.getValue())).isTrue();
            }
            assertThat(expectedSetSize).isEqualTo(custRegion.entrySet().size());
            break;
          default:
            throw new IllegalArgumentException();
        }
        mgr.commit();
        return null;
      }

      private void doLocalOp() {
        switch (op) {
          case PUT:
            for (int i = 6;; i++) {
              custId = new CustId(i);
              customer = new Customer("customer" + i, "address" + i);
              int bucketId = PartitionedRegionHelper.getHashKey(custRegion, custId);
              InternalDistributedMember primary = custRegion.getBucketPrimary(bucketId);
              if (primary.equals(getCache().getMyId())) {
                custRegion.put(custId, customer);
                break;
              }
            }
            originalSetSize = 6;
            expectedSetSize = 7;
            break;
          case DESTROY:
            for (int i = 3;; i--) {
              custId = new CustId(i);
              customer = new Customer("customer" + i, "address" + i);
              int bucketId = PartitionedRegionHelper.getHashKey(custRegion, custId);
              InternalDistributedMember primary = custRegion.getBucketPrimary(bucketId);
              if (primary.equals(getCache().getMyId())) {
                custRegion.destroy(custId);
                break;
              }
            }
            originalSetSize = 4;
            expectedSetSize = 3;
            break;
          default:
            throw new IllegalStateException();
        }
      }

      private Set getExpectedCustIdSet() {
        Set retVal = getCustIdSet(originalSetSize);
        switch (op) {
          case PUT:
            retVal.add(custId);
            break;
          case DESTROY:
            retVal.remove(custId);
            break;
          default:
            throw new IllegalStateException();
        }
        return retVal;
      }

      private Set getExpectedCustomerSet() {
        Set retVal = getCustomerSet(originalSetSize);
        switch (op) {
          case PUT:
            retVal.add(customer);
            break;
          case DESTROY:
            retVal.remove(customer);
            break;
          default:
            throw new IllegalStateException();
        }
        return retVal;
      }

    });
  }

  @Test
  public void testKeyIterationOnRR() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);
    initAccessorAndDataStore(accessor, datastore1, datastore2, 0);

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region custRegion = getCache().getRegion(CUSTOMER);
        Region rr = getCache().getRegion(D_REFERENCE);
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.begin();
        CustId custId = new CustId(5);
        Customer customer = new Customer("customer5", "address5");
        custRegion.put(custId, customer);
        Set set = rr.keySet();
        Iterator it = set.iterator();
        int i = 0;
        while (it.hasNext()) {
          i++;
          it.next();
        }
        assertThat(i).isEqualTo(5);
        assertThat(getCustIdSet(5)).isEqualTo(set);
        assertThat(rr.keySet().size()).isEqualTo(5);
        rr.put(custId, customer);
        set = rr.keySet();
        assertThat(getCustIdSet(6)).isEqualTo(set);
        it = set.iterator();
        i = 0;
        while (it.hasNext()) {
          i++;
          it.next();
        }
        assertThat(i).isEqualTo(6);
        assertThat(rr.keySet().size()).isEqualTo(6);
        assertThat(rr.get(custId)).isNotNull();
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(getCustIdSet(5)).isEqualTo(rr.keySet());
        assertThat(rr.keySet().size()).isEqualTo(5);
        assertThat(rr.get(custId)).isNull();
        mgr.unpauseTransaction(tx);
        mgr.commit();
        return null;
      }
    });
  }

  @Test
  public void testValuesIterationOnRR() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);
    initAccessorAndDataStore(accessor, datastore1, datastore2, 0);

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region custRegion = getCache().getRegion(CUSTOMER);
        Region rr = getCache().getRegion(D_REFERENCE);
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.begin();
        CustId custId = new CustId(5);
        Customer customer = new Customer("customer5", "address5");
        custRegion.put(custId, customer);
        Set set = (Set) rr.values();
        Iterator it = set.iterator();
        int i = 0;
        while (it.hasNext()) {
          i++;
          it.next();
        }
        assertThat(i).isEqualTo(5);
        assertThat(getCustomerSet(5)).isEqualTo(set);
        assertThat(rr.values().size()).isEqualTo(5);
        rr.put(custId, customer);
        set = (Set) rr.values();
        assertThat(getCustomerSet(6)).isEqualTo(set);
        it = set.iterator();
        i = 0;
        while (it.hasNext()) {
          i++;
          it.next();
        }
        assertThat(i).isEqualTo(6);
        assertThat(rr.values().size()).isEqualTo(6);
        assertThat(rr.get(custId)).isNotNull();
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(getCustomerSet(5)).isEqualTo(rr.values());
        assertThat(rr.values().size()).isEqualTo(5);
        assertThat(rr.get(custId)).isNull();
        mgr.unpauseTransaction(tx);
        mgr.commit();
        return null;
      }
    });
  }

  @Test
  public void testEntriesIterationOnRR() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);
    initAccessorAndDataStore(accessor, datastore1, datastore2, 0);

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region custRegion = getCache().getRegion(CUSTOMER);
        Region rr = getCache().getRegion(D_REFERENCE);
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.begin();
        CustId custId = new CustId(5);
        Customer customer = new Customer("customer5", "address5");
        custRegion.put(custId, customer);
        Set set = rr.entrySet();
        Iterator it = set.iterator();
        int i = 0;
        while (it.hasNext()) {
          i++;
          it.next();
        }
        assertThat(i).isEqualTo(5);
        // assertThat(getCustIdSet(5).equals(set)).isTrue();
        assertThat(rr.entrySet().size()).isEqualTo(5);
        rr.put(custId, customer);
        set = rr.entrySet();
        // assertThat(getCustIdSet(6).equals(set)).isTrue();
        it = set.iterator();
        i = 0;
        while (it.hasNext()) {
          i++;
          it.next();
        }
        assertThat(i).isEqualTo(6);
        assertThat(rr.entrySet().size()).isEqualTo(6);
        assertThat(rr.get(custId)).isNotNull();
        TXStateProxy tx = mgr.pauseTransaction();
        // assertIndexDetailsEquals(getCustIdSet(5), rr.entrySet());
        assertThat(rr.entrySet().size()).isEqualTo(5);
        assertThat(rr.get(custId)).isNull();
        mgr.unpauseTransaction(tx);
        mgr.commit();
        return null;
      }
    });
  }

  @Test
  public void testIllegalIteration() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);
    initAccessorAndDataStore(accessor, datastore1, datastore2, 0);

    SerializableCallable<Object> doIllegalIteration = new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region r = getCache().getRegion(CUSTOMER);
        Set keySet = r.keySet();
        Set entrySet = r.entrySet();
        Set valueSet = (Set) r.values();
        CacheTransactionManager mgr = getCache().getTXMgr();
        mgr.begin();
        // now we allow for using non-TX iterators in TX context
        assertThatThrownBy(keySet::size).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(entrySet::size).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(valueSet::size).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(keySet::iterator).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(entrySet::iterator).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(valueSet::iterator).isInstanceOf(IllegalStateException.class);

        // TX iterators
        keySet = r.keySet();
        entrySet = r.entrySet();
        valueSet = (Set) r.values();
        mgr.commit();
        // don't allow for TX iterator after TX has committed
        assertThatThrownBy(keySet::size).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(entrySet::size).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(valueSet::size).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(keySet::iterator).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(entrySet::iterator).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(valueSet::iterator).isInstanceOf(IllegalStateException.class);

        return null;
      }
    };

    accessor.invoke(doIllegalIteration);
    datastore1.invoke(doIllegalIteration);
  }

  final CustId expectedCustId = new CustId(6);
  final Customer expectedCustomer = new Customer("customer6", "address6");

  class TXFunction implements Function {
    static final String id = "TXFunction";

    @Override
    public void execute(FunctionContext context) {
      Region r = getCache().getRegion(CUSTOMER);
      getCache().getLogger().fine("SWAP:callingPut");
      r.put(expectedCustId, expectedCustomer);
      GemFireCacheImpl.getInstance().getLogger().warning(" XXX DOIN A PUT ", new Exception());
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

  enum Executions {
    OnRegion, OnMember
  }

  @Test
  public void testTxFunctionOnRegion() {
    doTestTxFunction(Executions.OnRegion);
  }

  @Test
  public void testTxFunctionOnMember() {
    doTestTxFunction(Executions.OnMember);
  }

  private void doTestTxFunction(final Executions e) {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);
    initAccessorAndDataStore(accessor, datastore1, datastore2, 0);

    SerializableCallable<Object> registerFunction = new SerializableCallable<Object>() {
      @Override
      public Object call() {
        FunctionService.registerFunction(new TXFunction());
        return null;
      }
    };

    accessor.invoke(registerFunction);
    datastore1.invoke(registerFunction);
    datastore2.invoke(registerFunction);

    TXId txId = (TXId) accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        PartitionedRegion custRegion = (PartitionedRegion) getCache().getRegion(CUSTOMER);
        TXManagerImpl mgr = getCache().getTXMgr();
        Set regions = new HashSet();
        regions.add(custRegion);
        regions.add(getCache().getRegion(ORDER));
        mgr.begin();
        assertThatThrownBy(() -> {
          switch (e) {
            case OnRegion:
              FunctionService.onRegion(custRegion).execute(TXFunction.id).getResult();
              break;
            case OnMember:
              FunctionService.onMembers().execute(TXFunction.id).getResult();
              break;
          }
        }).isInstanceOf(TransactionException.class);

        assertThatThrownBy(
            () -> InternalFunctionService.onRegions(regions).execute(TXFunction.id).getResult())
                .isInstanceOf(TransactionException.class);
        Set filter = new HashSet();
        filter.add(expectedCustId);
        switch (e) {
          case OnRegion:
            FunctionService.onRegion(custRegion).withFilter(filter).execute(TXFunction.id)
                .getResult();
            break;
          case OnMember:
            DistributedMember owner =
                custRegion.getOwnerForKey(custRegion.getKeyInfo(expectedCustId));
            FunctionService.onMember(owner).execute(TXFunction.id).getResult();
            break;
        }
        TXStateProxy tx = mgr.pauseTransaction();
        GemFireCacheImpl.getInstance().getLogger().warning("TX SUSPENDO:" + tx);
        assertThat(custRegion.get(expectedCustId)).isNull();
        mgr.unpauseTransaction(tx);
        return mgr.suspend();
      }
    });

    final Integer txOnDatastore1 = (Integer) datastore1.invoke(getNumberOfTXInProgress);
    final Integer txOnDatastore2 = (Integer) datastore2.invoke(getNumberOfTXInProgress);
    assertThat(txOnDatastore1 + txOnDatastore2).isOne();

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        CacheTransactionManager mgr = getCache().getTXMgr();
        mgr.resume(txId);
        mgr.commit();
        return null;
      }
    });

    datastore1.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region custRegion = getCache().getRegion(CUSTOMER);
        assertThat(expectedCustomer).isEqualTo(custRegion.get(expectedCustId));
        return null;
      }
    });

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTXMgr();
        mgr.begin();
        PartitionedRegion custRegion = (PartitionedRegion) getCache().getRegion(CUSTOMER);
        Set filter = new HashSet();
        filter.add(expectedCustId);
        switch (e) {
          case OnRegion:
            FunctionService.onRegion(custRegion).withFilter(filter).execute(TXFunction.id)
                .getResult();
            break;
          case OnMember:
            DistributedMember owner =
                custRegion.getOwnerForKey(custRegion.getKeyInfo(expectedCustId));
            FunctionService.onMember(owner).execute(TXFunction.id).getResult();
            break;
        }
        TXStateProxy tx = mgr.pauseTransaction();
        custRegion.put(expectedCustId, new Customer("Cust6", "updated6"));
        mgr.unpauseTransaction(tx);
        assertThatThrownBy(mgr::commit).isInstanceOf(CommitConflictException.class);
        return null;
      }
    });
  }

  @Test
  public void testNestedTxFunction() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);
    initAccessorAndDataStore(accessor, datastore1, datastore2, 0);

    class NestedTxFunction2 extends FunctionAdapter {
      static final String id = "NestedTXFunction2";

      @Override
      public void execute(FunctionContext context) {
        TXManagerImpl mgr = getCache().getTxManager();
        assertThat(mgr.getTXState()).isNotNull();
        assertThatThrownBy(mgr::commit)
            .isInstanceOf(UnsupportedOperationInTransactionException.class);
        context.getResultSender().lastResult(Boolean.TRUE);
      }

      @Override
      public String getId() {
        return id;
      }
    }
    class NestedTxFunction extends FunctionAdapter {
      static final String id = "NestedTXFunction";

      @Override
      public void execute(FunctionContext context) {
        Region r = null;
        if (context instanceof RegionFunctionContext) {
          r = PartitionRegionHelper.getLocalDataForContext((RegionFunctionContext) context);
        } else {
          r = getCache().getRegion(CUSTOMER);
        }
        assertThat(getCache().getTxManager().getTXState()).isNotNull();
        PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(CUSTOMER);
        Set filter = new HashSet();
        filter.add(expectedCustId);
        LogWriterUtils.getLogWriter().info("SWAP:inside NestedTxFunc calling func2:");
        r.put(expectedCustId, expectedCustomer);
        FunctionService.onRegion(pr).withFilter(filter).execute(new NestedTxFunction2())
            .getResult();
        assertThat(getCache().getTxManager().getTXState()).isNotNull();
        context.getResultSender().lastResult(Boolean.TRUE);
      }

      @Override
      public boolean optimizeForWrite() {
        return true;
      }

      @Override
      public String getId() {
        return id;
      }
    }

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(CUSTOMER);
        mgr.begin();
        Set filter = new HashSet();
        filter.add(expectedCustId);
        FunctionService.onRegion(pr).withFilter(filter).execute(new NestedTxFunction()).getResult();
        assertThat(getCache().getTxManager().getTXState()).isNotNull();
        mgr.commit();
        assertThat(expectedCustomer).isEqualTo(pr.get(expectedCustId));
        return null;
      }
    });
  }

  @Test
  public void testDRFunctionExecution() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);

    class CreateDR extends SerializableCallable {
      private final boolean isAccessor;

      public CreateDR(boolean isAccessor) {
        this.isAccessor = isAccessor;
      }

      @Override
      public Object call() {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(isAccessor ? DataPolicy.EMPTY : DataPolicy.REPLICATE);
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
        getCache().createRegion(CUSTOMER, af.create());
        if (isAccessor) {
          Region custRegion = getCache().getRegion(CUSTOMER);
          for (int i = 0; i < 5; i++) {
            CustId custId = new CustId(i);
            Customer customer = new Customer("customer" + i, "address" + i);
            custRegion.put(custId, customer);
          }
        }
        return null;
      }
    }

    datastore1.invoke(new CreateDR(false));
    datastore2.invoke(new CreateDR(false));
    accessor.invoke(new CreateDR(true));

    SerializableCallable registerFunction = new SerializableCallable<Object>() {
      @Override
      public Object call() {
        FunctionService.registerFunction(new TXFunction());
        return null;
      }
    };

    accessor.invoke(registerFunction);
    datastore1.invoke(registerFunction);
    datastore2.invoke(registerFunction);

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region custRegion = getCache().getRegion(CUSTOMER);
        TXManagerImpl mgr = getCache().getTXMgr();
        mgr.begin();
        FunctionService.onRegion(custRegion).execute(TXFunction.id).getResult();
        assertThat(mgr.getTXState()).isNotNull();
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(mgr.getTXState()).isNull();
        getCache().getLogger().fine("SWAP:callingget");
        assertThat(custRegion.get(expectedCustId))
            .as("expected null but was:" + custRegion.get(expectedCustId)).isNull();
        mgr.unpauseTransaction(tx);
        mgr.commit();
        assertThat(expectedCustomer).isEqualTo(custRegion.get(expectedCustId));
        return null;
      }
    });

    datastore1.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        final Region custRegion = getCache().getRegion(CUSTOMER);
        TXManagerImpl mgr = getCache().getTXMgr();
        mgr.begin();
        FunctionService.onRegion(custRegion).execute(new FunctionAdapter() {
          @Override
          public String getId() {
            return "LocalDS";
          }

          @Override
          public void execute(FunctionContext context) {
            assertThat(getCache().getTxManager().getTXState()).isNotNull();
            custRegion.destroy(expectedCustId);
            context.getResultSender().lastResult(Boolean.TRUE);
          }
        }).getResult();
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(custRegion.get(expectedCustId)).isEqualTo(expectedCustomer);
        mgr.unpauseTransaction(tx);
        mgr.commit();
        assertThat(custRegion.get(expectedCustId)).isNull();
        return null;
      }
    });
  }

  @Test
  public void testTxFunctionWithOtherOps() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);
    initAccessorAndDataStore(accessor, datastore1, datastore2, 0);

    SerializableCallable<Object> registerFunction = new SerializableCallable<Object>() {
      @Override
      public Object call() {
        FunctionService.registerFunction(new TXFunction());
        return null;
      }
    };

    accessor.invoke(registerFunction);
    datastore1.invoke(registerFunction);
    datastore2.invoke(registerFunction);

    TXId txId = (TXId) accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region custRegion = getCache().getRegion(CUSTOMER);
        TXManagerImpl mgr = getCache().getTXMgr();
        mgr.begin();
        assertThatThrownBy(
            () -> FunctionService.onRegion(custRegion).execute(TXFunction.id).getResult())
                .isInstanceOf(TransactionException.class);
        Set filter = new HashSet();
        filter.add(expectedCustId);
        FunctionService.onRegion(custRegion).withFilter(filter).execute(TXFunction.id).getResult();
        assertThat(expectedCustomer).isEqualTo(custRegion.get(expectedCustId));
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(custRegion.get(expectedCustId)).isNull();
        mgr.unpauseTransaction(tx);
        return mgr.suspend();
      }
    });

    final Integer txOnDatastore1 = (Integer) datastore1.invoke(getNumberOfTXInProgress);
    final Integer txOnDatastore2 = (Integer) datastore2.invoke(getNumberOfTXInProgress);
    assertThat(txOnDatastore1 + txOnDatastore2).isOne();

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region custRegion = getCache().getRegion(CUSTOMER);
        CacheTransactionManager mgr = getCache().getTXMgr();
        mgr.resume(txId);
        mgr.commit();
        assertThat(expectedCustomer).isEqualTo(custRegion.get(expectedCustId));
        custRegion.destroy(expectedCustId);
        return null;
      }
    });
    // test onMembers
    SerializableCallable<Object> getMember = new SerializableCallable<Object>() {
      @Override
      public Object call() {
        return getCache().getMyId();
      }
    };
    final InternalDistributedMember ds1 = (InternalDistributedMember) datastore1.invoke(getMember);
    final InternalDistributedMember ds2 = (InternalDistributedMember) datastore2.invoke(getMember);
    TXId txId2 = (TXId) accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(CUSTOMER);
        // get owner for expectedKey
        DistributedMember owner = pr.getOwnerForKey(pr.getKeyInfo(expectedCustId));
        // get key on datastore1
        CustId keyOnOwner = null;
        keyOnOwner = getKeyOnMember(owner, pr);

        TXManagerImpl mgr = getCache().getTXMgr();
        mgr.begin();
        // bootstrap tx on owner
        pr.get(keyOnOwner);
        Set<DistributedMember> members = new HashSet<>();
        members.add(ds1);
        members.add(ds2);
        assertThatThrownBy(
            () -> FunctionService.onMembers(members).execute(TXFunction.id).getResult())
                .isInstanceOf(TransactionException.class);
        FunctionService.onMember(owner).execute(TXFunction.id).getResult();
        assertThat(expectedCustomer).isEqualTo(pr.get(expectedCustId));
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(pr.get(expectedCustId)).isNull();
        mgr.unpauseTransaction(tx);
        return mgr.suspend();
      }
    });
    final Integer txOnDatastore1_1 = (Integer) datastore1.invoke(getNumberOfTXInProgress);
    final Integer txOnDatastore2_1 = (Integer) datastore2.invoke(getNumberOfTXInProgress);
    assertThat(txOnDatastore1_1 + txOnDatastore2_1).isOne();

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region custRegion = getCache().getRegion(CUSTOMER);
        CacheTransactionManager mgr = getCache().getTXMgr();
        mgr.resume(txId2);
        mgr.commit();
        assertThat(expectedCustomer).isEqualTo(custRegion.get(expectedCustId));
        custRegion.destroy(expectedCustId);
        return null;
      }
    });
    // test function execution on data store
    final DistributedMember owner =
        (DistributedMember) accessor.invoke(new SerializableCallable<Object>() {
          @Override
          public Object call() {
            PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(CUSTOMER);
            return pr.getOwnerForKey(pr.getKeyInfo(expectedCustId));
          }
        });

    SerializableCallable<Object> testFnOnDs = new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTXMgr();
        PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(CUSTOMER);
        CustId keyOnDs = getKeyOnMember(pr.getMyId(), pr);
        mgr.begin();
        pr.get(keyOnDs);
        Set filter = new HashSet();
        filter.add(keyOnDs);
        FunctionService.onRegion(pr).withFilter(filter).execute(TXFunction.id).getResult();
        assertThat(expectedCustomer).isEqualTo(pr.get(expectedCustId));
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(pr.get(expectedCustId)).isNull();
        mgr.unpauseTransaction(tx);
        return mgr.suspend();
      }
    };

    if (owner.equals(ds1)) {
      TXId dsTxId = (TXId) datastore1.invoke(testFnOnDs);

      final Integer txOnDatastore1_2 = (Integer) datastore1.invoke(getNumberOfTXInProgress);
      final Integer txOnDatastore2_2 = (Integer) datastore2.invoke(getNumberOfTXInProgress);
      assertThat(txOnDatastore1_2 + txOnDatastore2_2).isZero();// ds1 has a local transaction,
                                                               // not remote
      CloseFnOnDsTx closeFnOnDsTx = new CloseFnOnDsTx(dsTxId);
      datastore1.invoke(closeFnOnDsTx);
    } else {
      TXId dsTxId = (TXId) datastore2.invoke(testFnOnDs);

      final Integer txOnDatastore1_2 = (Integer) datastore1.invoke(getNumberOfTXInProgress);
      final Integer txOnDatastore2_2 = (Integer) datastore2.invoke(getNumberOfTXInProgress);
      assertThat(txOnDatastore1_2 + txOnDatastore2_2).isZero();// ds1 has a local transaction,
                                                               // not remote
      CloseFnOnDsTx closeFnOnDsTx = new CloseFnOnDsTx(dsTxId);
      datastore2.invoke(closeFnOnDsTx);
    }

    // test that function is rejected if function target is not same as txState target
    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        CacheTransactionManager mgr = getCache().getTXMgr();
        PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(CUSTOMER);
        CustId keyOnDs1 = getKeyOnMember(ds1, pr);
        CustId keyOnDs2 = getKeyOnMember(ds2, pr);
        mgr.begin();
        pr.get(keyOnDs1);// bootstrap txState
        Set filter = new HashSet();
        filter.add(keyOnDs2);
        assertThatThrownBy(() -> FunctionService.onRegion(pr).withFilter(filter)
            .execute(TXFunction.id).getResult())
                .isInstanceOf(TransactionDataRebalancedException.class);
        assertThatThrownBy(() -> FunctionService.onMember(ds2).execute(TXFunction.id).getResult())
            .isInstanceOf(TransactionDataNotColocatedException.class);
        mgr.commit();
        return null;
      }
    });
  }

  protected class CloseFnOnDsTx extends SerializableCallable<Object> {

    private final TXId txId;

    CloseFnOnDsTx(TXId txId) {
      this.txId = txId;
    }

    @Override
    public Object call() {
      Region custRegion = getCache().getRegion(CUSTOMER);
      CacheTransactionManager mgr = getCache().getTXMgr();
      mgr.resume(txId);
      mgr.commit();
      assertThat(expectedCustomer).isEqualTo(custRegion.get(expectedCustId));
      custRegion.destroy(expectedCustId);
      return null;
    }
  }

  /**
   * @return first key found on the given member
   */
  CustId getKeyOnMember(final DistributedMember owner, PartitionedRegion pr) {
    CustId retVal = null;
    for (int i = 0; i < 5; i++) {
      CustId custId = new CustId(i);
      DistributedMember member = pr.getOwnerForKey(pr.getKeyInfo(custId));
      if (member.equals(owner)) {
        retVal = custId;
        break;
      }
    }
    return retVal;
  }

  protected Set<Customer> getCustomerSet(int size) {
    Set<Customer> expectedSet = new HashSet<>();
    for (int i = 0; i < size; i++) {
      expectedSet.add(new Customer("customer" + i, "address" + i));
    }
    return expectedSet;
  }

  Set<CustId> getCustIdSet(int size) {
    Set<CustId> expectedSet = new HashSet<>();
    for (int i = 0; i < size; i++) {
      expectedSet.add(new CustId(i));
    }
    return expectedSet;
  }

  @Test
  public void testRemoteJTACommit() {
    doRemoteJTA(true);
  }

  @Test
  public void testRemoteJTARollback() {
    doRemoteJTA(false);
  }

  private void doRemoteJTA(final boolean isCommit) {
    Host host = Host.getHost(0);
    VM acc = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(acc, datastore, 0);
    VM accessor = getVMForTransactions(acc, datastore);

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        getCache().getTxManager().addListener(new TestTxListener(false));
        return null;
      }
    });
    final CustId expectedCustId = new CustId(6);
    final Customer expectedCustomer = new Customer("customer6", "address6");
    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() throws Exception {
        getCache().getTxManager().addListener(new TestTxListener(true));
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Context ctx = getCache().getJNDIContext();
        UserTransaction tx = (UserTransaction) ctx.lookup("java:/UserTransaction");
        assertThat(Status.STATUS_NO_TRANSACTION).isEqualTo(tx.getStatus());
        tx.begin();
        assertThat(Status.STATUS_ACTIVE).isEqualTo(tx.getStatus());
        custRegion.put(expectedCustId, expectedCustomer);
        assertThat(expectedCustomer).isEqualTo(custRegion.get(expectedCustId));
        return null;
      }
    });
    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region custRegion = getCache().getRegion(CUSTOMER);
        assertThat(custRegion.get(expectedCustId)).isNull();
        return null;
      }
    });
    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() throws Exception {
        Region custRegion = getCache().getRegion(CUSTOMER);
        Context ctx = getCache().getJNDIContext();
        UserTransaction tx = (UserTransaction) ctx.lookup("java:/UserTransaction");
        if (isCommit) {
          tx.commit();
          assertThat(expectedCustomer).isEqualTo(custRegion.get(expectedCustId));
        } else {
          tx.rollback();
          assertThat(custRegion.get(expectedCustId)).isNull();
        }
        return null;
      }
    });
    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TestTxListener l = (TestTxListener) getCache().getTXMgr().getListener();
        assertThat(l.isListenerInvoked()).isTrue();
        return null;
      }
    });
  }


  @Test
  public void testOriginRemoteIsTrueForRemoteReplicatedRegions() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(accessor, datastore, 0);

    class OriginRemoteRRWriter extends CacheWriterAdapter {
      int fireC = 0;
      int fireD = 0;
      int fireU = 0;

      @Override
      public void beforeCreate(EntryEvent event) throws CacheWriterException {
        if (!event.isOriginRemote()) {
          throw new CacheWriterException("SUP?? This CREATE is supposed to be isOriginRemote");
        }
        fireC++;
      }

      @Override
      public void beforeDestroy(EntryEvent event) throws CacheWriterException {
        getCache().getLogger().fine("SWAP:writer:createEvent:" + event);
        if (!event.isOriginRemote()) {
          throw new CacheWriterException("SUP?? This DESTROY is supposed to be isOriginRemote");
        }
        fireD++;
      }

      @Override
      public void beforeUpdate(EntryEvent event) throws CacheWriterException {
        if (!event.isOriginRemote()) {
          throw new CacheWriterException("SUP?? This UPDATE is supposed to be isOriginRemote");
        }
        fireU++;
      }
    }

    datastore.invoke(new SerializableCallable<Object>() {

      @Override
      public Object call() {
        Region refRegion = getCache().getRegion(D_REFERENCE);
        refRegion.getAttributesMutator().setCacheWriter(new OriginRemoteRRWriter());
        return null;
      }

    });

    TXId putTxId = (TXId) accessor.invoke(new DoOpsInTX(OP.PUT));

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.resume(putTxId);
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(tx).isNotNull();
        mgr.unpauseTransaction(tx);
        mgr.commit();
        return null;
      }
    });

    TXId destroyTxId = (TXId) accessor.invoke(new DoOpsInTX(OP.DESTROY));

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.resume(destroyTxId);
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(tx).isNotNull();
        mgr.unpauseTransaction(tx);
        mgr.commit();
        return null;
      }
    });

    TXId putTxId2 = (TXId) accessor.invoke(new DoOpsInTX(OP.PUT));

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.resume(putTxId2);
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(tx).isNotNull();
        mgr.unpauseTransaction(tx);
        mgr.commit();
        return null;
      }
    });

    datastore.invoke(new SerializableCallable<Object>() {

      @Override
      public Object call() {
        Region refRegion = getCache().getRegion(D_REFERENCE);
        OriginRemoteRRWriter w = (OriginRemoteRRWriter) refRegion.getAttributes().getCacheWriter();
        assertThat(w.fireC).isOne();
        assertThat(w.fireD).isOne();
        assertThat(w.fireU).isOne();
        return null;
      }
    });
  }


  @Test
  public void testRemoteCreateInReplicatedRegion() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(accessor, datastore, 0);

    TXId txId = (TXId) accessor.invoke(new DoOpsInTX(OP.PUT));

    accessor.invoke(new SerializableCallable<Object>() {

      @Override
      public Object call() {
        Region refRegion = getCache().getRegion(D_REFERENCE);
        refRegion.create("sup", "dawg");
        return null;
      }
    });

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.resume(txId);
        TXStateProxy tx = mgr.pauseTransaction();
        assertThat(tx).isNotNull();
        mgr.unpauseTransaction(tx);
        mgr.commit();
        return null;
      }
    });

    accessor.invoke(new SerializableCallable<Object>() {

      @Override
      public Object call() {
        Region refRegion = getCache().getRegion(D_REFERENCE);
        assertThat(refRegion.get("sup")).isEqualTo("dawg");
        return null;
      }
    });
  }

  @Test
  public void testRemoteTxCleanupOnCrash() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(accessor, datastore, 0);

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region cust = getCache().getRegion(CUSTOMER);
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.begin();
        cust.put(new CustId(6), new Customer("customer6", "address6"));
        return null;
      }
    });
    final InternalDistributedMember member =
        (InternalDistributedMember) accessor.invoke(new SerializableCallable<Object>() {
          @Override
          public Object call() {
            return getCache().getMyId();
          }
        });
    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        TXManagerImpl mgr = getCache().getTxManager();
        assertThat(mgr.hostedTransactionsInProgressForTest()).isOne();
        mgr.memberDeparted(getCache().getDistributionManager(), member, true);
        assertThat(mgr.hostedTransactionsInProgressForTest()).isZero();
        return null;
      }
    });
  }

  @Test
  public void testNonColocatedPutAll() {
    doNonColocatedbulkOp(OP.PUTALL);
  }

  /**
   * disabled because rather than throwing an exception, getAll catches all exceptions and logs a
   * warning message
   */
  @Ignore("TODO: disabled because rather than throwing an exception, getAll catches all exceptions and logs a warning message")
  @Test
  public void testNonColocatedGetAll() {
    doNonColocatedbulkOp(OP.GETALL);
  }

  private void doNonColocatedbulkOp(final OP op) {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);
    initAccessorAndDataStore(accessor, datastore1, datastore2, 0);

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Map<CustId, Customer> custMap = new HashMap<CustId, Customer>();
        for (int i = 0; i < 10; i++) {
          CustId cId = new CustId(i);
          Customer c = new Customer("name" + i, "addr" + i);
          custMap.put(cId, c);
        }
        InternalCache cache = getCache();
        cache.getCacheTransactionManager().begin();
        Region<CustId, Customer> r = cache.getRegion(CUSTOMER);
        assertThatThrownBy(() -> {
          switch (op) {
            case PUTALL:
              r.putAll(custMap);
              break;
            case GETALL:
              r.put(new CustId(1), new Customer("cust1", "addr1"));
              r.getAll(custMap.keySet());
              break;
            default:
              break;
          }
        }).isInstanceOf(TransactionDataNotColocatedException.class);
        cache.getCacheTransactionManager().rollback();
        return null;
      }
    });
  }

  @Test
  public void testBasicPutAll() {
    doTestBasicBulkOP(OP.PUTALL);
  }

  @Test
  public void testBasicRemoveAll() {
    doTestBasicBulkOP(OP.REMOVEALL);
  }

  private void doTestBasicBulkOP(final OP op) {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore1, datastore2, 1);

    if (op.equals(OP.REMOVEALL)) {
      // for remove all populate more data
      accessor.invoke(new SerializableCallable<Object>() {
        @Override
        public Object call() {
          Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
          for (int i = 0; i < 50; i++) {
            custRegion.put(new CustId(i), new Customer("name" + i, "address" + i));
          }
          return null;
        }
      });
    }

    final List ds1Buckets = (List) datastore1.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        // do local operations with rollback and then commit
        Map<CustId, Customer> custMap = new HashMap<>();
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        PartitionedRegion pr = ((PartitionedRegion) custRegion);
        List localBuckets = pr.getLocalPrimaryBucketsListTestOnly();
        System.out.println("localBuckets:" + localBuckets);
        for (int i = 10; i < 20; i++) {
          int hash = PartitionedRegionHelper.getHashKey(i,
              pr.getPartitionAttributes().getTotalNumBuckets());
          if (localBuckets.contains(hash)) {
            custMap.put(new CustId(i), new Customer("name" + i, "address" + i));
          }
        }
        System.out.println("SWAP:custMap:" + custMap);
        int regionSize = custRegion.size();
        getCache().getCacheTransactionManager().begin();
        if (op.equals(OP.PUTALL)) {
          custRegion.putAll(custMap);
        } else {
          custRegion.removeAll(custMap.keySet());
        }
        getCache().getCacheTransactionManager().rollback();
        assertThat(regionSize).isEqualTo(custRegion.size());
        // now commit
        getCache().getCacheTransactionManager().begin();
        if (op.equals(OP.PUTALL)) {
          custRegion.putAll(custMap);
        } else {
          custRegion.removeAll(custMap.keySet());
        }
        getCache().getCacheTransactionManager().commit();
        assertThat(getExpectedSize(custMap, regionSize)).isEqualTo(custRegion.size());

        // bulk op on other member
        custMap.clear();
        for (int i = 10; i < 20; i++) {
          int hash = PartitionedRegionHelper.getHashKey(i,
              pr.getPartitionAttributes().getTotalNumBuckets());
          if (!localBuckets.contains(hash)) { // not on local member
            custMap.put(new CustId(i), new Customer("name" + i, "address" + i));
          }
        }
        System.out.println("SWAP:custMap:" + custMap);
        regionSize = custRegion.size();
        getCache().getCacheTransactionManager().begin();
        if (op.equals(OP.PUTALL)) {
          custRegion.putAll(custMap);
        } else {
          custRegion.removeAll(custMap.keySet());
        }
        getCache().getCacheTransactionManager().rollback();
        assertThat(regionSize).isEqualTo(custRegion.size());
        // now commit
        getCache().getCacheTransactionManager().begin();
        if (op.equals(OP.PUTALL)) {
          custRegion.putAll(custMap);
        } else {
          custRegion.removeAll(custMap.keySet());
        }
        getCache().getCacheTransactionManager().commit();
        assertThat(getExpectedSize(custMap, regionSize)).isEqualTo(custRegion.size());
        return localBuckets;
      }

      private int getExpectedSize(Map<CustId, Customer> custMap, int regionSize) {
        if (op.equals(OP.REMOVEALL)) {
          return regionSize - custMap.size();
        }
        return regionSize + custMap.size();
      }
    });

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        // do a transaction on one of the nodes
        Map<CustId, Customer> custMap = new HashMap<>();
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        for (int i = 20; i < 30; i++) {
          int hash = PartitionedRegionHelper.getHashKey(i,
              custRegion.getAttributes().getPartitionAttributes().getTotalNumBuckets());
          if (ds1Buckets.contains(hash)) {
            custMap.put(new CustId(i), new Customer("name" + i, "address" + i));
          }
        }
        System.out.println("SWAP:custMap:" + custMap);
        int regionSize = custRegion.size();
        getCache().getCacheTransactionManager().begin();
        if (op.equals(OP.PUTALL)) {
          custRegion.putAll(custMap);
        } else {
          custRegion.removeAll(custMap.keySet());
        }
        getCache().getCacheTransactionManager().rollback();
        assertThat(regionSize).isEqualTo(custRegion.size());
        // now commit
        getCache().getCacheTransactionManager().begin();
        if (op.equals(OP.PUTALL)) {
          custRegion.putAll(custMap);
        } else {
          custRegion.removeAll(custMap.keySet());
        }
        getCache().getCacheTransactionManager().commit();
        assertThat(getExpectedSize(custMap, regionSize)).isEqualTo(custRegion.size());
        return null;
      }

      private int getExpectedSize(Map<CustId, Customer> custMap, int regionSize) {
        if (op.equals(OP.REMOVEALL)) {
          return regionSize - custMap.size();
        }
        return regionSize + custMap.size();
      }
    });
  }

  @Test
  public void testDestroyCreateConflation() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    initAccessorAndDataStore(accessor, datastore, 0);

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region cust = getCache().getRegion(D_REFERENCE);
        cust.put("meow", "this is a meow, deal with it");
        cust.getAttributesMutator().addCacheListener(new OneUpdateCacheListener());
        cust.getAttributesMutator().setCacheWriter(new OneDestroyAndThenOneCreateCacheWriter());

        return null;
      }
    });

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region cust = getCache().getRegion(D_REFERENCE);
        cust.getAttributesMutator().addCacheListener(new OneUpdateCacheListener());
        return null;
      }
    });

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        CacheTransactionManager mgr = getCache().getTxManager();
        mgr.begin();
        Region cust = getCache().getRegion(D_REFERENCE);
        cust.destroy("meow");
        cust.create("meow", "this is the new meow, not the old meow");
        mgr.commit();
        return null;
      }
    });

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region cust = getCache().getRegion(D_REFERENCE);
        OneUpdateCacheListener rat =
            (OneUpdateCacheListener) cust.getAttributes().getCacheListener();
        assertThat(rat.getSuccess())
            .as("The OneUpdateCacheListener didnt get an update").isTrue();
        return null;
      }
    });

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region cust = getCache().getRegion(D_REFERENCE);
        OneDestroyAndThenOneCreateCacheWriter wri =
            (OneDestroyAndThenOneCreateCacheWriter) cust.getAttributes().getCacheWriter();
        wri.checkSuccess();
        return null;
      }
    });


  }

  class OneUpdateCacheListener extends CacheListenerAdapter {
    boolean success = false;

    public boolean getSuccess() {
      return success;
    }

    @Override
    public void afterCreate(EntryEvent event) {
      throw new UnsupportedOperationException("create not expected");
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      assertThat(success).as("Should have only had one update").isFalse();
      System.out.println("WE WIN!");
      success = true;
    }

    @Override
    public void afterDestroy(EntryEvent event) {
      throw new UnsupportedOperationException("destroy not expected");
    }

    @Override
    public void afterInvalidate(EntryEvent event) {
      throw new UnsupportedOperationException("invalidate not expected");
    }
  }

  class OneDestroyAndThenOneCreateCacheWriter extends CacheWriterAdapter {
    private boolean oneDestroy;
    private boolean oneCreate;

    public void checkSuccess() {
      assertThat(oneDestroy && oneCreate)
          .as("Didn't get both events. oneDestroy=" + oneDestroy + " oneCreate=" + oneCreate)
          .isTrue();
    }

    @Override
    public void beforeCreate(EntryEvent event) throws CacheWriterException {
      assertThat(oneDestroy).isTrue();
      assertThat(oneCreate).isFalse();
      oneCreate = true;
    }

    @Override
    public void beforeUpdate(EntryEvent event) throws CacheWriterException {
      throw new UnsupportedOperationException("update not expected");
    }

    @Override
    public void beforeDestroy(EntryEvent event) throws CacheWriterException {
      assertThat(oneDestroy).isFalse();
      assertThat(oneCreate).isFalse();
      oneDestroy = true;
    }

  }

  protected Integer startServer(VM vm) {
    return (Integer) vm.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() throws IOException {
        int port = getRandomAvailableTCPPort();
        CacheServer s = getCache().addCacheServer();
        s.setPort(port);
        s.start();
        return port;
      }
    });
  }

  protected void createClientRegion(VM vm, final int port, final boolean isEmpty,
      final boolean ri) {
    vm.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolServer("localhost"/* getServerHostName(Host.getHost(0)) */, port);
        ccf.setPoolSubscriptionEnabled(true);
        ccf.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
        ccf.set(SERIALIZABLE_OBJECT_FILTER,
            getDistributedSystemProperties().getProperty(SERIALIZABLE_OBJECT_FILTER));
        ClientCache cCache = getClientCache(ccf);
        ClientRegionFactory<Integer, String> crf = cCache.createClientRegionFactory(
            isEmpty ? ClientRegionShortcut.PROXY : ClientRegionShortcut.CACHING_PROXY);
        crf.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
        crf.addCacheListener(new ClientListener());
        Region r = crf.create(D_REFERENCE);
        Region cust = crf.create(CUSTOMER);
        Region order = crf.create(ORDER);
        if (ri) {
          r.registerInterestRegex(".*");
          cust.registerInterestRegex(".*");
          order.registerInterestRegex(".*");
        }
        return null;
      }
    });
  }

  protected class ClientCQListener implements CqListener {

    boolean invoked = false;

    @Override
    public void onError(CqEvent aCqEvent) {
      // TODO Auto-generated method stub

    }

    @Override
    public void onEvent(CqEvent aCqEvent) {
      // TODO Auto-generated method stub
      invoked = true;

    }

    @Override
    public void close() {
      // TODO Auto-generated method stub

    }

  }

  protected static class ClientListener extends CacheListenerAdapter {
    boolean invoked = false;
    int invokeCount = 0;
    int invalidateCount = 0;
    int putCount = 0;
    boolean putAllOp = false;
    boolean isOriginRemote = false;
    int creates;
    int updates;

    @Override
    public void afterCreate(EntryEvent event) {
      event.getRegion().getCache().getLogger().warning("ZZZ AFTER CREATE:" + event.getKey());
      invoked = true;
      invokeCount++;
      putCount++;
      creates++;
      event.getRegion().getCache().getLogger().warning("ZZZ AFTER CREATE:" + event.getKey()
          + " isPutAll:" + event.getOperation().isPutAll() + " op:" + event.getOperation());
      putAllOp = event.getOperation().isPutAll();
      isOriginRemote = event.isOriginRemote();
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      event.getRegion().getCache().getLogger().warning("ZZZ AFTER UPDATE:" + event.getKey()
          + " isPutAll:" + event.getOperation().isPutAll() + " op:" + event.getOperation());
      putAllOp = event.getOperation().isPutAll();
      invoked = true;
      invokeCount++;
      putCount++;
      updates++;
      isOriginRemote = event.isOriginRemote();
    }

    @Override
    public void afterInvalidate(EntryEvent event) {
      event.getRegion().getCache().getLogger().warning("ZZZ AFTER UPDATE:" + event.getKey());
      invoked = true;
      invokeCount++;
      invalidateCount++;
      isOriginRemote = event.isOriginRemote();
    }

    public void reset() {
      invoked = false;
      invokeCount = 0;
      invalidateCount = 0;
      putCount = 0;
      isOriginRemote = false;
      creates = 0;
      updates = 0;
    }
  }

  protected static class ServerListener extends CacheListenerAdapter {
    boolean invoked = false;
    int creates;
    int updates;

    @Override
    public void afterCreate(EntryEvent event) {
      invoked = true;
      creates++;
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      invoked = true;
      updates++;
    }

    @Override
    public void afterDestroy(EntryEvent event) {
      invoked = true;
    }

    @Override
    public void afterInvalidate(EntryEvent event) {
      invoked = true;
    }
  }

  protected static class ServerWriter extends CacheWriterAdapter {
    boolean invoked = false;

    @Override
    public void beforeCreate(EntryEvent event) throws CacheWriterException {
      invoked = true;
      event.getRegion().getCache().getLogger().info("SWAP:writer:" + event);
      assertThat(event.isOriginRemote()).isTrue();
    }

    @Override
    public void beforeUpdate(EntryEvent event) throws CacheWriterException {
      invoked = true;
      event.getRegion().getCache().getLogger().info("SWAP:writer:" + event);
      assertThat(event.isOriginRemote()).isTrue();
    }

    @Override
    public void beforeDestroy(EntryEvent event) throws CacheWriterException {
      invoked = true;
      event.getRegion().getCache().getLogger().info("SWAP:writer:" + event);
      assertThat(event.isOriginRemote()).isTrue();
    }
  }


  @Test
  public void testTXWithRI() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(datastore);

    createClientRegion(client, port, false, true);
    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        CustId custId = new CustId(1);
        OrderId orderId = new OrderId(1, custId);
        getCache().getCacheTransactionManager().begin();
        custRegion.put(custId, new Customer("foo", "bar"));
        orderRegion.put(orderId, new Order("fooOrder"));
        refRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

    client.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        final ClientListener cl =
            (ClientListener) custRegion.getAttributes().getCacheListeners()[0];
        GeodeAwaitility.await().untilAsserted(
            () -> assertThat(cl.invoked).isTrue().as("listener was never invoked"));

        return null;
      }
    });
  }

  private static final String EMPTY_REGION = "emptyRegionName";

  @Test
  public void testBug43176() {
    Host host = Host.getHost(0);
    VM datastore = host.getVM(0);
    VM client = host.getVM(1);

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        AttributesFactory<Integer, String> af = new AttributesFactory<>();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.EMPTY);
        af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
        getCache().createRegionFactory(af.create()).create(EMPTY_REGION);
        af.setDataPolicy(DataPolicy.REPLICATE);
        getCache().createRegionFactory(af.create()).create(D_REFERENCE);
        return null;
      }
    });

    final int port = startServer(datastore);
    client.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolServer("localhost"/* getServerHostName(Host.getHost(0)) */, port);
        ccf.setPoolSubscriptionEnabled(true);
        ccf.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
        ClientCache cCache = getClientCache(ccf);
        ClientRegionFactory<Integer, String> crf =
            cCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
        crf.addCacheListener(new ClientListener());
        crf.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
        Region r = crf.create(D_REFERENCE);
        Region empty = crf.create(EMPTY_REGION);
        r.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
        empty.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
        return null;
      }
    });

    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region ref = getCache().getRegion(D_REFERENCE);
        Region empty = getCache().getRegion(EMPTY_REGION);
        getCache().getCacheTransactionManager().begin();
        ref.put("one", "value1");
        empty.put("eone", "valueOne");
        getCache().getLogger().info("SWAP:callingCommit");
        getCache().getCacheTransactionManager().commit();
        assertThat(ref.containsKey("one")).isTrue();
        assertThat(ref.get("one")).isEqualTo("value1");
        assertThat(empty.containsKey("eone")).isFalse();
        assertThat(empty.get("eone")).isNull();
        return null;
      }
    });

    client.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region empty = getCache().getRegion(EMPTY_REGION);
        final ClientListener l = (ClientListener) empty.getAttributes().getCacheListeners()[0];
        GeodeAwaitility.await().untilAsserted(
            () -> assertThat(l.invoked).isTrue());
        return null;
      }
    });
  }

  @Test
  public void testTXWithRICommitInDatastore() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(datastore);

    createClientRegion(client, port, false, true);
    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        CustId custId = new CustId(1);
        OrderId orderId = new OrderId(1, custId);
        getCache().getCacheTransactionManager().begin();
        custRegion.put(custId, new Customer("foo", "bar"));
        orderRegion.put(orderId, new Order("fooOrder"));
        refRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

    client.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        final ClientListener cl =
            (ClientListener) custRegion.getAttributes().getCacheListeners()[0];

        GeodeAwaitility.await().untilAsserted(
            () -> assertThat(cl.invoked).isTrue().as("listener was never invoked"));

        return null;
      }
    });
  }


  @Test
  public void testListenersNotInvokedOnSecondary() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);

    initAccessorAndDataStoreWithInterestPolicy(accessor, datastore1, datastore2, 1);
    SerializableCallable<Object> registerListener = new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        custRegion.getAttributesMutator().addCacheListener(new ListenerInvocationCounter());
        return null;
      }
    };
    datastore1.invoke(registerListener);
    datastore2.invoke(registerListener);

    datastore1.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        getCache().getCacheTransactionManager().begin();
        CustId custId = new CustId(1);
        Customer customer = new Customer("customerNew", "addressNew");
        custRegion.put(custId, customer);
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

    SerializableCallable<Object> getListenerCount = new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        ListenerInvocationCounter l =
            (ListenerInvocationCounter) custRegion.getAttributes().getCacheListeners()[0];
        getCache().getLogger().info("SWAP:listenerCount:" + l.invocationCount);
        return l.invocationCount;
      }
    };

    int totalInvocation = (Integer) datastore1.invoke(getListenerCount)
        + (Integer) datastore2.invoke(getListenerCount);
    assertThat(totalInvocation).isOne();
  }

  private class ListenerInvocationCounter extends CacheListenerAdapter {
    private int invocationCount = 0;

    @Override
    public void afterUpdate(EntryEvent event) {
      invocationCount++;
    }
  }

  @Test
  public void testBug33073() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore1, datastore2, 0);
    final CustId custId = new CustId(19);

    final TXId txId = (TXId) datastore1.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        assertThat(refRegion.get(custId)).isNull();
        getCache().getCacheTransactionManager().begin();
        refRegion.put(custId, new Customer("name1", "address1"));
        return getCache().getCacheTransactionManager().suspend();
      }
    });
    datastore2.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        assertThat(refRegion.get(custId)).isNull();
        refRegion.put(custId, new Customer("nameNew", "addressNew"));
        return null;
      }
    });
    datastore1.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        assertThatThrownBy(() -> {
          getCache().getCacheTransactionManager().resume(txId);
          getCache().getCacheTransactionManager().commit();
        }).isInstanceOf(CommitConflictException.class);
        return null;
      }
    });
  }

  @Test
  public void testBug43081() throws Exception {
    createRegion(false, 0, null);
    Context ctx = getCache().getJNDIContext();
    UserTransaction tx = (UserTransaction) ctx.lookup("java:/UserTransaction");
    assertThat(Status.STATUS_NO_TRANSACTION).isEqualTo(tx.getStatus());
    Region pr = getCache().getRegion(CUSTOMER);
    Region rr = getCache().getRegion(D_REFERENCE);
    // test all ops
    for (int i = 0; i < 6; i++) {
      pr.put(new CustId(1), new Customer("name1", "address1"));
      rr.put("key1", "value1");
      tx.begin();
      switch (i) {
        case 0:
          pr.get(new CustId(1));
          rr.get("key1");
          break;
        case 1:
          pr.put(new CustId(1), new Customer("nameNew", "addressNew"));
          rr.put("key1", "valueNew");
          break;
        case 2:
          pr.invalidate(new CustId(1));
          rr.invalidate("key1");
          break;
        case 3:
          pr.destroy(new CustId(1));
          rr.destroy("key1");
          break;
        case 4:
          Map m = new HashMap();
          m.put(new CustId(1), new Customer("nameNew", "addressNew"));
          pr.putAll(m);
          m = new HashMap();
          m.put("key1", "valueNew");
          rr.putAll(m);
          break;
        case 5:
          Set s = new HashSet();
          s.add(new CustId(1));
          pr.getAll(s);
          s = new HashSet();
          s.add("key1");
          pr.getAll(s);
          break;
        case 6:
          pr.getEntry(new CustId(1));
          rr.getEntry("key1");
          break;
        default:
          break;
      }

      // Putting a string key causes this, the partition resolver
      // doesn't handle it.
      IgnoredException.addIgnoredException("IllegalStateException");
      assertThat(Status.STATUS_ACTIVE).isEqualTo(tx.getStatus());
      final CountDownLatch latch = new CountDownLatch(1);
      Thread t = new Thread(() -> {
        Context ctx1 = getCache().getJNDIContext();
        try {
          UserTransaction tx1 = (UserTransaction) ctx1.lookup("java:/UserTransaction");
        } catch (NamingException e) {
          e.printStackTrace();
        }
        Region pr1 = getCache().getRegion(CUSTOMER);
        Region rr1 = getCache().getRegion(D_REFERENCE);
        pr1.put(new CustId(1), new Customer("name11", "address11"));
        rr1.put("key1", "value1");
        latch.countDown();
      });
      t.start();
      latch.await();
      assertThatThrownBy(() -> {
        pr.put(new CustId(1), new Customer("name11", "address11"));
        tx.commit();
      }).isInstanceOf(RollbackException.class);
    }
  }

  @Test
  public void testBug45556() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    final String name = getName();

    class CountingListener extends CacheListenerAdapter {
      private int count;

      @Override
      public void afterCreate(EntryEvent event) {
        LogWriterUtils.getLogWriter().info("afterCreate invoked for " + event);
        count++;
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        LogWriterUtils.getLogWriter().info("afterUpdate invoked for " + event);
        count++;
      }
    }

    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region r = getCache().createRegionFactory(RegionShortcut.REPLICATE_PROXY).create(name);
        r.getAttributesMutator().addCacheListener(new CountingListener());
        return null;
      }
    });
    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region r = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(name);
        r.getAttributesMutator().addCacheListener(new CountingListener());
        r.put("key1", "value1");
        return null;
      }
    });
    final TransactionId txid = (TransactionId) accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region r = getCache().getRegion(name);
        CacheTransactionManager tm = getCache().getCacheTransactionManager();
        getCache().getLogger().fine("SWAP:BeginTX");
        tm.begin();
        r.put("txkey", "txvalue");
        return tm.suspend();
      }
    });
    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region rgn = getCache().getRegion(name);
        assertThat(rgn.get("txkey")).isNull();
        TXManagerImpl txMgr = getCache().getTxManager();
        TXStateProxy tx = txMgr.getHostedTXState((TXId) txid);
        assertThat(tx.getRegions().size()).isOne();
        for (InternalRegion r : tx.getRegions()) {
          assertThat(r instanceof DistributedRegion).isTrue();
          TXRegionState rs = tx.readRegion(r);
          for (Object key : rs.getEntryKeys()) {
            TXEntryState es = rs.readEntry(key);
            assertThat(key).isEqualTo("txkey");
            assertThat(es.getValue(key, r, false)).isNotNull();
            if (key.equals("txkey")) {
              assertThat(es.isDirty()).isTrue();
            }
          }
        }
        return null;
      }
    });
    accessor.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region rgn = getCache().getRegion(name);
        assertThat(rgn.get("txkey")).isNull();
        CacheTransactionManager mgr = getCache().getCacheTransactionManager();
        mgr.resume(txid);
        mgr.commit();
        CountingListener cl = (CountingListener) rgn.getAttributes().getCacheListeners()[0];
        assertThat(cl.count).isZero();
        assertThat(rgn.get("txkey")).isEqualTo("txvalue");
        return null;
      }
    });
    datastore.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region rgn = getCache().getRegion(name);
        CountingListener cl = (CountingListener) rgn.getAttributes().getCacheListeners()[0];
        assertThat(cl.count).isEqualTo(2);
        return null;
      }
    });
  }

  @Test
  public void testExpirySuspend_bug45984() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    final String regionName = getName();

    // create region with expiration
    vm1.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
        try {
          RegionFactory<String, String> rf = getCache().createRegionFactory();
          rf.setEntryTimeToLive(new ExpirationAttributes(1, ExpirationAction.LOCAL_DESTROY));
          rf.setScope(Scope.DISTRIBUTED_ACK);
          rf.create(regionName);
        } finally {
          System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
        }
        return null;
      }
    });

    // create replicate region
    vm2.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
        return null;
      }
    });

    vm1.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        final Region<String, String> r = getCache().getRegion(regionName);

        suspendExpiration();
        Region.Entry entry = null;
        long tilt;
        try {
          r.put("key", "value");
          LocalRegion lr = (LocalRegion) r;
          r.put("nonTXkey", "nonTXvalue");
          getCache().getCacheTransactionManager().begin();
          r.put("key", "newvalue");
          waitForEntryExpiration(lr, "key");
        } finally {
          permitExpiration();
        }
        TransactionId tx = getCache().getCacheTransactionManager().suspend();

        ThrowingRunnable runnable =
            () -> assertThat(!r.containsKey("key") && !r.containsKey("nonTXKey")).isTrue();

        // A remote tx will allow expiration to happen on the side that
        // is not hosting the tx. But it will not allow an expiration
        // initiated on the hosting jvm.
        // tx is hosted in vm2 so expiration can happen in vm1.
        GeodeAwaitility.await().untilAsserted(runnable);
        getCache().getCacheTransactionManager().resume(tx);
        assertThat(r.containsKey("key")).isTrue();
        getCache().getCacheTransactionManager().commit();
        GeodeAwaitility.await().untilAsserted(runnable);

        return null;
      }
    });

  }

  @Test
  public void testRemoteFetchVersionMessage() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String regionName = getName();

    final VersionTag tag = (VersionTag) vm0.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        LocalRegion r = (LocalRegion) getCache().createRegionFactory(RegionShortcut.REPLICATE)
            .create(regionName);
        r.put("key", "value");
        return r.getRegionEntry("key").getVersionStamp().asVersionTag();
      }
    });

    vm1.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.EMPTY);
        af.setScope(Scope.DISTRIBUTED_ACK);
        DistributedRegion r = (DistributedRegion) getCache().createRegion(regionName, af.create());
        r.cache.getLogger().info("SWAP:sending:remoteTagRequest");
        VersionTag remote = r.fetchRemoteVersionTag("key");
        r.cache.getLogger().info("SWAP:remoteTag:" + remote);
        assertThatThrownBy(() -> r.fetchRemoteVersionTag("nonExistentKey"))
            .isInstanceOf(EntryNotFoundException.class);
        assertThat(tag).isEqualTo(remote);
        return null;
      }
    });
  }

  @Test
  public void testTransactionWithRemoteVersionFetch() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String regionNameNormal = getName() + "_normal";
    final String regionName = getName();

    vm0.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region r = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
        Region n =
            getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionNameNormal);
        n.put("key", "value");
        n.put("key", "value1");
        n.put("key", "value2");
        return null;
      }
    });

    final VersionTag tag = (VersionTag) vm1.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region r = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.NORMAL);
        af.setScope(Scope.DISTRIBUTED_ACK);
        Region n = getCache().createRegion(regionNameNormal, af.create());
        TXManagerImpl mgr = getCache().getTxManager();
        mgr.begin();
        r.put("key", "value");
        assertThat(mgr.getTXState().isRealDealLocal()).isTrue();
        getCache().getLogger().fine("SWAP:doingPutInNormalRegion");
        n.put("key", "value");
        getCache().getLogger().fine("SWAP:commiting");
        mgr.commit();
        return ((LocalRegion) n).getRegionEntry("key").getVersionStamp().asVersionTag();
      }
    });

    vm0.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region n = getCache().getRegion(regionNameNormal);
        VersionTag localTag =
            ((LocalRegion) n).getRegionEntry("key").getVersionStamp().asVersionTag();
        assertThat(tag.getEntryVersion()).isEqualTo(localTag.getEntryVersion());
        assertThat(tag.getRegionVersion()).isEqualTo(localTag.getRegionVersion());
        return null;
      }
    });
  }

  @Test
  public void testBug49398() {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    final String lrName = getName() + "_lr";

    SerializableCallable<Object> createRegion = new SerializableCallable<Object>() {
      @Override
      public Object call() {
        createRegion(false, 1, null);
        getCache().createRegionFactory(RegionShortcut.LOCAL).create(lrName);
        return null;
      }
    };

    vm1.invoke(createRegion);
    vm2.invoke(createRegion);

    vm1.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        CacheTransactionManager txMgr = getCache().getCacheTransactionManager();
        Region ref = getCache().getRegion(D_REFERENCE);
        Region lr = getCache().getRegion(lrName);
        txMgr.begin();
        ref.put(new CustId(1), new Customer("name1", "address1"));
        lr.put("key", "value");
        txMgr.commit();
        return null;
      }
    });

    // make sure local region changes are not reflected in the other vm
    vm2.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region lr = getCache().getRegion(lrName);
        assertThat(lr.get("key")).isNull();
        return null;
      }
    });
  }

  public Object getEntryValue(final CustId custId0, PartitionedRegion cust) {
    RegionEntry entry = cust.getBucketRegion(custId0).getRegionEntry(custId0);
    Object value = entry.getValue();
    return value;
  }

  /**
   * Install Listeners and verify that they are invoked after all tx events have been applied to the
   * cache see GEODE-278
   */
  @Test
  public void testNonInlineRemoteEvents() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String key1 = "nonInline-1";
    final String key2 = "nonInline-2";

    class NonInlineListener extends CacheListenerAdapter {
      boolean assertException = false;

      @Override
      public void afterCreate(EntryEvent event) {
        if (event.getKey().equals(key1)) {
          if (getCache().getRegion(D_REFERENCE).get(key2) == null) {
            assertException = true;
          }
        }
      }
    }

    SerializableCallable<Object> createRegionWithListener = new SerializableCallable<Object>() {
      @Override
      public Object call() {
        createRegion(false, 0, null);
        getCache().getRegion(D_REFERENCE).getAttributesMutator()
            .addCacheListener(new NonInlineListener());
        return null;
      }
    };

    vm0.invoke(createRegionWithListener);
    vm1.invoke(createRegionWithListener);

    vm0.invoke(new SerializableCallable<Object>() {
      @Override
      public Object call() {
        Region region = getCache().getRegion(D_REFERENCE);
        CacheTransactionManager mgr = getCache().getCacheTransactionManager();
        mgr.begin();
        region.put(key1, "nonInlineValue-1");
        region.put(key2, "nonInlineValue-2");
        mgr.commit();
        return null;
      }
    });

    SerializableCallable<Object> verifyAssert = new SerializableCallable<Object>() {
      @Override
      public Object call() {
        CacheListener[] listeners =
            getCache().getRegion(D_REFERENCE).getAttributes().getCacheListeners();
        for (CacheListener listener : listeners) {
          if (listener instanceof NonInlineListener) {
            NonInlineListener l = (NonInlineListener) listener;
            assertThat(l.assertException).isFalse();
          }
        }
        return null;
      }
    };

    vm0.invoke(verifyAssert);
    vm1.invoke(verifyAssert);

  }

  private void waitForEntryExpiration(LocalRegion lr, String key) {
    try {
      ExpirationDetector detector;
      do {
        detector = new ExpirationDetector(lr.getEntryExpiryTask(key));
        ExpiryTask.expiryTaskListener = detector;
        ExpiryTask.permitExpiration();
        detector.awaitExecuted(3000, MILLISECONDS);
      } while (!detector.hasExpired() && detector.wasRescheduled());
    } finally {
      ExpiryTask.expiryTaskListener = null;
    }
  }
}
