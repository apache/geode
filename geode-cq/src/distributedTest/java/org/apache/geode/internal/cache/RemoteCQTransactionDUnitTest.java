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

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.TransactionEvent;
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.cache.TransactionWriter;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.execute.CustomerIDPartitionResolver;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Customer;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class RemoteCQTransactionDUnitTest extends JUnit4CacheTestCase {

  protected final String CUSTOMER = "custRegion";
  protected final String ORDER = "orderRegion";
  protected final String D_REFERENCE = "distrReference";

  final CustId expectedCustId = new CustId(6);
  final Customer expectedCustomer = new Customer("customer6", "address6");

  private final SerializableCallable getNumberOfTXInProgress = new SerializableCallable() {
    @Override
    public Object call() throws Exception {
      TXManagerImpl mgr = getGemfireCache().getTxManager();
      return mgr.hostedTransactionsInProgressForTest();
    }
  };

  private final SerializableCallable verifyNoTxState = new SerializableCallable() {
    @Override
    public Object call() throws Exception {
      final TXManagerImpl mgr = getGemfireCache().getTxManager();
      await().untilAsserted(() -> assertEquals(0, mgr.hostedTransactionsInProgressForTest()));
      return null;
    }
  };

  protected enum OP {
    PUT, GET, DESTROY, INVALIDATE, KEYS, VALUES, ENTRIES, PUTALL, GETALL, REMOVEALL
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    try {
      Invoke.invokeInEveryVM(verifyNoTxState);
    } finally {
      closeAllCache();
    }
  }

  void createRegion(boolean accessor, int redundantCopies, InterestPolicy interestPolicy) {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.DISTRIBUTED_ACK);
    af.setDataPolicy(DataPolicy.REPLICATE);
    af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
    getCache().createRegion(D_REFERENCE, af.create());
    af = new AttributesFactory();
    af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
    if (interestPolicy != null) {
      af.setSubscriptionAttributes(new SubscriptionAttributes(interestPolicy));
    }
    af.setPartitionAttributes(new PartitionAttributesFactory<CustId, Customer>()
        .setTotalNumBuckets(4).setLocalMaxMemory(accessor ? 0 : 1)
        .setPartitionResolver(new CustomerIDPartitionResolver("resolver1"))
        .setRedundantCopies(redundantCopies).create());
    getCache().createRegion(CUSTOMER, af.create());
    af.setPartitionAttributes(new PartitionAttributesFactory<OrderId, Order>().setTotalNumBuckets(4)
        .setLocalMaxMemory(accessor ? 0 : 1)
        .setPartitionResolver(new CustomerIDPartitionResolver("resolver2"))
        .setRedundantCopies(redundantCopies).setColocatedWith(CUSTOMER).create());
    getCache().createRegion(ORDER, af.create());
  }

  protected boolean getConcurrencyChecksEnabled() {
    return false;
  }

  void populateData() {
    Region custRegion = getCache().getRegion(CUSTOMER);
    Region orderRegion = getCache().getRegion(ORDER);
    Region refRegion = getCache().getRegion(D_REFERENCE);
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
    accessor.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createRegion(true/* accessor */, redundantCopies, null);
        return null;
      }
    });

    datastore.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createRegion(false/* accessor */, redundantCopies, null);
        populateData();
        return null;
      }
    });
  }

  protected void initAccessorAndDataStore(VM accessor, VM datastore1, VM datastore2,
      final int redundantCopies) {
    datastore2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createRegion(false/* accessor */, redundantCopies, null);
        return null;
      }
    });

    initAccessorAndDataStore(accessor, datastore1, redundantCopies);
  }

  private void initAccessorAndDataStoreWithInterestPolicy(VM accessor, VM datastore1, VM datastore2,
      final int redundantCopies) {
    datastore2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createRegion(false/* accessor */, redundantCopies, InterestPolicy.ALL);
        return null;
      }
    });

    initAccessorAndDataStore(accessor, datastore1, redundantCopies);
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
      getGemfireCache().getLogger()
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

    assertEquals(containsKey, rContainsKC);
    assertEquals(containsKey, rContainsKO);
    assertEquals(containsKey, rContainsKR);
    assertEquals(containsValue, rContainsVR);
    assertEquals(containsValue, rContainsVC);
    assertEquals(containsValue, rContainsVO);

    if (containsKey) {
      Region.Entry eC = custRegion.getEntry(custId);
      for (OrderId o : ordersSet) {
        assertNotNull(orderRegion.getEntry(o));
      }
      Region.Entry eR = refRegion.getEntry(custId);
      assertNotNull(eC);
      assertNotNull(eR);

    } else {
      try {
        Region.Entry eC = custRegion.getEntry(custId);
        assertNull("should have had an EntryNotFoundException:" + eC, eC);

      } catch (EntryNotFoundException enfe) {
        // this is what we expect
      }
      try {
        for (OrderId o : ordersSet) {
          assertNull("should have had an EntryNotFoundException:" + orderRegion.getEntry(o),
              orderRegion.getEntry(o));
        }

      } catch (EntryNotFoundException enfe) {
        // this is what we expect
      }
      try {
        Region.Entry eR = refRegion.getEntry(custId);
        assertNull("should have had an EntryNotFoundException:" + eR, eR);
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
        assertNotNull(custRegion.getEntry(custId));
        assertEquals(expectedCust, custRegion.getEntry(custId).getValue());

        assertNotNull(refRegion.getEntry(custId));
        assertEquals(expectedRef, refRegion.getEntry(custId).getValue());

        break;
      case GET:
        expectedCust = custRegion.get(custId);
        expectedOrder = orderRegion.get(orderId);
        expectedRef = refRegion.get(custId);
        validateContains(custId, Collections.singleton(orderId), true);
        break;
      case DESTROY:
        assertTrue(!custRegion.containsKey(custId));
        assertTrue(!orderRegion.containsKey(orderId));
        assertTrue(!refRegion.containsKey(custId));
        validateContains(custId, Collections.singleton(orderId), false);
        break;
      case INVALIDATE:
        boolean validateContainsKey = true;
        if (!((GemFireCacheImpl) custRegion.getCache()).isClient()) {
          assertTrue(custRegion.containsKey(custId));
          assertTrue(orderRegion.containsKey(orderId));
          assertTrue(refRegion.containsKey(custId));
        }
        assertNull(custRegion.get(custId));
        assertNull(orderRegion.get(orderId));
        assertNull(refRegion.get(custId));
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
    assertNotNull(custRegion);
    assertNotNull(orderRegion);
    assertNotNull(refRegion);

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
        assertEquals(expectedCust, custRegion.getEntry(custId).getValue());
        assertEquals(expectedOrder, orderRegion.getEntry(orderId).getValue());
        getCache().getLogger().info("SWAP:verifyRollback:" + orderRegion);
        getCache().getLogger().info("SWAP:verifyRollback:" + orderRegion.getEntry(orderId2));
        assertNull(getGemfireCache().getTXMgr().getTXState());
        assertNull("" + orderRegion.getEntry(orderId2), orderRegion.getEntry(orderId2));
        assertNull(orderRegion.getEntry(orderId3));
        assertNull(orderRegion.get(orderId2));
        assertNull(orderRegion.get(orderId3));
        assertEquals(expectedRef, refRegion.getEntry(custId).getValue());
        validateContains(custId, Collections.singleton(orderId), true);
        break;
      case GET:
        expectedCust = custRegion.getEntry(custId).getValue();
        expectedOrder = orderRegion.getEntry(orderId).getValue();
        expectedRef = refRegion.getEntry(custId).getValue();
        validateContains(custId, Collections.singleton(orderId), true);
        break;
      case DESTROY:
        assertTrue(!custRegion.containsKey(custId));
        assertTrue(!orderRegion.containsKey(orderId));
        assertTrue(!refRegion.containsKey(custId));
        validateContains(custId, Collections.singleton(orderId), true);
        break;
      case INVALIDATE:
        assertTrue(custRegion.containsKey(custId));
        assertTrue(orderRegion.containsKey(orderId));
        assertTrue(refRegion.containsKey(custId));
        assertNull(custRegion.get(custId));
        assertNull(orderRegion.get(orderId));
        assertNull(refRegion.get(custId));
        validateContains(custId, Collections.singleton(orderId), true, true);
        break;
      default:
        throw new IllegalStateException();
    }
  }

  abstract static class CacheCallback {

    protected boolean isAccessor;
    protected Exception ex = null;

    protected void verifyOrigin(EntryEvent event) {
      try {
        assertEquals(!isAccessor, event.isOriginRemote());
      } catch (Exception e) {
        ex = e;
      }
    }

    protected void verifyPutAll(EntryEvent event) {
      CustId knownCustId = new CustId(1);
      OrderId knownOrderId = new OrderId(2, knownCustId);
      if (event.getKey().equals(knownOrderId)) {
        try {
          assertTrue(event.getOperation().isPutAll());
          assertNotNull(event.getTransactionId());
        } catch (Exception e) {
          ex = e;
        }
      }
    }
  }

  static class TestCacheListener extends CacheCallback implements CacheListener {
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
      getGemfireCache().getLogger()
          .info("SWAP:beforeCreate:" + event + " op:" + event.getOperation());
      verifyOrigin(event);
      verifyPutAll(event);
      setFired(event);
    }

    public void setFired(EntryEvent event) {
      wasFired = true;
    }

    @Override
    public void beforeUpdate(EntryEvent event) throws CacheWriterException {
      getGemfireCache().getLogger()
          .info("SWAP:beforeCreate:" + event + " op:" + event.getOperation());
      verifyOrigin(event);
      verifyPutAll(event);
      setFired(event);
    }

    @Override
    public void beforeDestroy(EntryEvent event) throws CacheWriterException {
      verifyOrigin(event);
      setFired(event);
    }

    @Override
    public void beforeRegionClear(RegionEvent event) throws CacheWriterException {
      setFired(null);
    }

    @Override
    public void beforeRegionDestroy(RegionEvent event) throws CacheWriterException {
      setFired(null);
    }

    @Override
    public void close() {}
  }

  abstract static class txCallback {

    protected boolean isAccessor;
    protected Exception ex = null;

    protected void verify(TransactionEvent txEvent) {
      for (CacheEvent e : (List<CacheEvent>) txEvent.getEvents()) {
        verifyOrigin(e);
        verifyPutAll(e);
      }
    }

    private void verifyOrigin(CacheEvent event) {
      try {
        assertEquals(true, event.isOriginRemote()); // change to !isAccessor after fixing #41498
      } catch (Exception e) {
        ex = e;
      }
    }

    private void verifyPutAll(CacheEvent p_event) {
      if (!(p_event instanceof EntryEvent)) {
        return;
      }
      EntryEvent event = (EntryEvent) p_event;
      CustId knownCustId = new CustId(1);
      OrderId knownOrderId = new OrderId(2, knownCustId);
      if (event.getKey().equals(knownOrderId)) {
        try {
          assertTrue(event.getOperation().isPutAll());
          assertNotNull(event.getTransactionId());
        } catch (Exception e) {
          ex = e;
        }
      }
    }
  }

  static class TestTxListener extends txCallback implements TransactionListener {

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
      return this.listenerInvoked;
    }

    @Override
    public void close() {}
  }

  static class TestTxWriter extends txCallback implements TransactionWriter {
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

  class TXFunction implements Function {

    static final String id = "TXFunction";

    @Override
    public void execute(FunctionContext context) {
      Region r = null;
      r = getGemfireCache().getRegion(CUSTOMER);
      getGemfireCache().getLogger().fine("SWAP:callingPut");
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
    Set<Customer> expectedSet = new HashSet<Customer>();
    for (int i = 0; i < size; i++) {
      expectedSet.add(new Customer("customer" + i, "address" + i));
    }
    return expectedSet;
  }

  Set<CustId> getCustIdSet(int size) {
    Set<CustId> expectedSet = new HashSet<CustId>();
    for (int i = 0; i < size; i++) {
      expectedSet.add(new CustId(i));
    }
    return expectedSet;
  }

  static class OneUpdateCacheListener extends CacheListenerAdapter {

    boolean success = false;

    public boolean getSuccess() {
      return success;
    }

    @Override
    public void afterCreate(EntryEvent event) {
      fail("create not expected");
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      if (!success) {
        System.out.println("WE WIN!");
        success = true;
      } else {
        fail("Should have only had one update");
      }
    }

    @Override
    public void afterDestroy(EntryEvent event) {
      fail("destroy not expected");
    }

    @Override
    public void afterInvalidate(EntryEvent event) {
      fail("invalidate not expected");
    }
  }

  static class OneDestroyAndThenOneCreateCacheWriter extends CacheWriterAdapter {

    private boolean oneDestroy;
    private boolean oneCreate;

    public void checkSuccess() throws Exception {
      if (oneDestroy && oneCreate) {
        // chill
      } else {
        fail("Didn't get both events. oneDestroy=" + oneDestroy + " oneCreate=" + oneCreate);
      }
    }

    @Override
    public void beforeCreate(EntryEvent event) throws CacheWriterException {
      if (!oneDestroy) {
        fail("destroy should have arrived in writer before create");
      } else {
        if (oneCreate) {
          fail("more than one create detected! expecting destroy then create");
        } else {
          oneCreate = true;
        }
      }
    }

    @Override
    public void beforeUpdate(EntryEvent event) throws CacheWriterException {
      fail("update not expected");
    }

    @Override
    public void beforeDestroy(EntryEvent event) throws CacheWriterException {
      if (oneDestroy) {
        fail("only one destroy expected");
      } else {
        if (oneCreate) {
          fail("destroy is supposed to precede create");
        } else {
          oneDestroy = true;
        }
      }
    }

  }

  protected Integer startServer(VM vm) {
    return (Integer) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
        CacheServer s = getCache().addCacheServer();
        s.setPort(port);
        s.start();
        return port;
      }
    });
  }

  protected void createClientRegion(VM vm, final int port, final boolean isEmpty, final boolean ri,
      final boolean CQ) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolServer("localhost"/* getServerHostName(Host.getHost(0)) */, port);
        ccf.setPoolSubscriptionEnabled(true);
        ccf.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
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
        if (CQ) {
          CqAttributesFactory cqf = new CqAttributesFactory();
          cqf.addCqListener(new ClientCQListener());
          CqAttributes ca = cqf.create();
          cCache.getQueryService().newCq("SELECT * FROM " + cust.getFullPath(), ca).execute();
        }
        return null;
      }
    });
  }

  protected static class ClientCQListener implements CqListener {

    boolean invoked = false;

    @Override
    public void onError(CqEvent aCqEvent) {}

    @Override
    public void onEvent(CqEvent aCqEvent) {
      invoked = true;
    }

    @Override
    public void close() {}
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

  @Test
  public void testTXWithCQCommitInDatastoreCQ() throws Exception {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(datastore);

    createClientRegion(client, port, false, true, true);
    datastore.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
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

    Thread.sleep(10000);
    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        ClientListener cl = (ClientListener) custRegion.getAttributes().getCacheListeners()[0];

        assertTrue(((ClientCQListener) custRegion.getCache().getQueryService().getCqs()[0]
            .getCqAttributes().getCqListener()).invoked);
        assertTrue(cl.invoked);
        return null;
      }
    });
  }

  @Test
  public void testTXWithCQCommitInDatastoreConnectedToAccessorCQ() throws Exception {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(accessor);

    createClientRegion(client, port, false, true, true);
    datastore.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        CustId custId = new CustId(1);
        OrderId orderId = new OrderId(1, custId);
        getCache().getCacheTransactionManager().begin();
        custRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

    Thread.sleep(10000);
    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        ClientListener cl = (ClientListener) custRegion.getAttributes().getCacheListeners()[0];
        assertTrue(cl.invoked);
        assertTrue(((ClientCQListener) custRegion.getCache().getQueryService().getCqs()[0]
            .getCqAttributes().getCqListener()).invoked);
        return null;
      }
    });
  }

  @Test
  public void testTXWithCQCommitInDatastoreConnectedToDatastoreCQ() throws Exception {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(datastore);

    createClientRegion(client, port, false, true, true);
    datastore.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        CustId custId = new CustId(1);
        OrderId orderId = new OrderId(1, custId);
        getCache().getCacheTransactionManager().begin();
        custRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

    Thread.sleep(10000);
    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        ClientListener cl = (ClientListener) custRegion.getAttributes().getCacheListeners()[0];
        assertTrue(cl.invoked);
        assertTrue(((ClientCQListener) custRegion.getCache().getQueryService().getCqs()[0]
            .getCqAttributes().getCqListener()).invoked);
        return null;
      }
    });
  }

  @Test
  public void testTXWithCQCommitInAccessorConnectedToDatastoreCQ() throws Exception {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(datastore);

    createClientRegion(client, port, false, true, true);
    accessor.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        CustId custId = new CustId(1);
        OrderId orderId = new OrderId(1, custId);
        getCache().getCacheTransactionManager().begin();
        custRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

    Thread.sleep(10000);
    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        ClientListener cl = (ClientListener) custRegion.getAttributes().getCacheListeners()[0];
        assertTrue(cl.invoked);
        assertTrue(((ClientCQListener) custRegion.getCache().getQueryService().getCqs()[0]
            .getCqAttributes().getCqListener()).invoked);
        return null;
      }
    });
  }

  @Test
  public void testTXWithCQCommitInAccessorConnectedToAccessorCQ() throws Exception {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(accessor);

    createClientRegion(client, port, false, true, true);
    accessor.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        CustId custId = new CustId(1);
        OrderId orderId = new OrderId(1, custId);
        getCache().getCacheTransactionManager().begin();
        custRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

    Thread.sleep(10000);
    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        ClientListener cl = (ClientListener) custRegion.getAttributes().getCacheListeners()[0];
        assertTrue(cl.invoked);
        assertTrue(((ClientCQListener) custRegion.getCache().getQueryService().getCqs()[0]
            .getCqAttributes().getCqListener()).invoked);
        return null;
      }
    });
  }

  @Test
  public void testCQCommitInDatastoreConnectedToAccessorCQ() throws Exception {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(accessor);

    createClientRegion(client, port, false, true, true);
    datastore.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        CustId custId = new CustId(1);
        OrderId orderId = new OrderId(1, custId);
        getCache().getCacheTransactionManager().begin();
        custRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

    Thread.sleep(10000);
    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        Region<CustId, Customer> refRegion = getCache().getRegion(D_REFERENCE);
        ClientListener cl = (ClientListener) custRegion.getAttributes().getCacheListeners()[0];
        assertTrue(cl.invoked);
        assertTrue(((ClientCQListener) custRegion.getCache().getQueryService().getCqs()[0]
            .getCqAttributes().getCqListener()).invoked);
        return null;
      }
    });
  }

}
