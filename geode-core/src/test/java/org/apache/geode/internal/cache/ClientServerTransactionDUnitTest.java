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

import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.naming.Context;
import javax.transaction.UserTransaction;

import com.jayway.awaitility.Awaitility;

import static org.apache.geode.distributed.ConfigurationProperties.*;

import org.apache.geode.cache.*;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.client.*;
import org.apache.geode.cache.execute.*;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.cache.util.TransactionListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Customer;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.tx.ClientTXStateStub;
import org.apache.geode.test.dunit.*;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.dunit.LogWriterUtils.getDUnitLogLevel;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;

/**
 * Tests the basic client-server transaction functionality
 */
@Category(DistributedTest.class)
public class ClientServerTransactionDUnitTest extends RemoteTransactionDUnitTest {

  protected static final int MAX_ENTRIES = 10;

  private enum forop {
    CREATE, UPDATE, DESTROY
  };

  protected static final String OTHER_REGION = "OtherRegion";

  public ClientServerTransactionDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    IgnoredException.addIgnoredException("java.net.SocketException");
    postSetUpClientServerTransactionDUnitTest();
  }

  protected void postSetUpClientServerTransactionDUnitTest() throws Exception {}


  private Integer createRegionsAndStartServerWithTimeout(VM vm, boolean accessor,
      int txTimeoutSecs) {
    return createRegionOnServerWithTimeout(vm, true, accessor, txTimeoutSecs);
  }

  private Integer createRegionOnServerWithTimeout(VM vm, final boolean startServer,
      final boolean accessor, final int txTimeoutSecs) {
    return createRegionOnServerWithTimeout(vm, startServer, accessor, 0, txTimeoutSecs);
  }

  private Integer createRegionsAndStartServer(VM vm, boolean accessor) {
    return createRegionOnServer(vm, true, accessor);
  }

  private void createRegionOnServer(VM vm) {
    createRegionOnServer(vm, false, false);
  }

  private Integer createRegionOnServer(VM vm, final boolean startServer, final boolean accessor) {
    return createRegionOnServer(vm, startServer, accessor, 0);
  }

  private Integer createRegionOnServer(VM vm, final boolean startServer, final boolean accessor,
      final int redundantCopies) {
    return createRegionOnServerWithTimeout(vm, startServer, accessor, redundantCopies, 10);
  }

  private Integer createRegionOnServerWithTimeout(VM vm, final boolean startServer,
      final boolean accessor, final int redundantCopies, final int txTimeoutSecs) {
    return (Integer) vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        createRegion(accessor, redundantCopies, null);
        TXManagerImpl txMgr = (TXManagerImpl) getCache().getCacheTransactionManager();
        txMgr.setTransactionTimeToLiveForTest(txTimeoutSecs);
        if (startServer) {
          int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
          CacheServer s = getCache().addCacheServer();
          s.setPort(port);
          s.start();
          return port;
        }
        return 0;
      }
    });
  }

  private Integer createRegionOnDisconnectedServer(VM vm, final boolean startServer) {
    return (Integer) vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        Properties props = getDistributedSystemProperties();
        props.put(MCAST_PORT, "0");
        props.remove(LOCATORS);
        InternalDistributedSystem system = getSystem(props);
        Cache cache = CacheFactory.create(system);
        cache.createRegion(OTHER_REGION, af.create());
        TXManagerImpl txMgr = (TXManagerImpl) cache.getCacheTransactionManager();
        txMgr.setTransactionTimeToLiveForTest(10);
        if (startServer) {
          int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
          CacheServer s = cache.addCacheServer();
          s.setPort(port);
          s.start();
          return port;
        }
        return 0;
      }
    });
  }


  // private void createClientRegionWithRI(VM vm, final int port, final boolean isEmpty) {
  // createClientRegion(vm, port, isEmpty, true);
  // }

  private void createClientRegion(VM vm, final int port, final boolean isEmpty) {
    createClientRegion2(vm, port, isEmpty, false);
  }

  private void createClientRegionAndPopulateData(VM vm, final int port, final boolean isEmpty) {
    createClientRegion(vm, port, isEmpty);
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        populateData();
        return null;
      }
    });
  }

  private void createClientRegion2(VM vm, final int port, final boolean isEmpty, final boolean ri) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        ClientCacheFactory ccf = new ClientCacheFactory();
        setCCF(port, ccf);
        // these settings were used to manually check that tx operation stats were being updated
        // ccf.set(STATISTIC_SAMPLING_ENABLED, "true");
        // ccf.set(STATISTIC_ARCHIVE_FILE, "clientStats.gfs");
        ClientCache cCache = getClientCache(ccf);
        ClientRegionFactory<Integer, String> crf = cCache.createClientRegionFactory(
            isEmpty ? ClientRegionShortcut.PROXY : ClientRegionShortcut.CACHING_PROXY);
        crf.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
        Region<Integer, String> r = crf.create(D_REFERENCE);
        Region<Integer, String> customer = crf.create(CUSTOMER);
        Region<Integer, String> order = crf.create(ORDER);
        if (ri) {
          r.registerInterestRegex(".*");
          customer.registerInterestRegex(".*");
          order.registerInterestRegex(".*");
        }
        return null;
      }
    });
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.internal.cache.RemoteTransactionDUnitTest#getVMForTransactions(dunit.VM,
   * dunit.VM)
   */
  @Override
  public VM getVMForTransactions(VM accessor, VM datastore) {
    // create a cache server in the accessor VM and then create and return
    // a client VM
    final int serverPort =
        (Integer) accessor.invoke(new SerializableCallable("create cache server") {
          public Object call() throws Exception {
            TXManagerImpl txMgr = (TXManagerImpl) getCache().getCacheTransactionManager();
            txMgr.setTransactionTimeToLiveForTest(10);
            int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
            CacheServer s = getCache().addCacheServer();
            s.setPort(port);
            s.start();
            return port;
          }
        });
    VM clientVM = Host.getHost(0).getVM(2); // superclass always uses 0 and 1
    createClientRegion(clientVM, serverPort, false, false);
    return clientVM;
  }

  private void configureOffheapSystemProperty() {
    Properties p = new Properties();
    // p.setProperty(LOG_LEVEL, "finer");
    p.setProperty(OFF_HEAP_MEMORY_SIZE, "1m");
    this.getSystem(p);
  }

  private void createSubscriptionRegion(boolean isOffHeap, String regionName, int copies,
      int totalBuckets) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(copies).setTotalNumBuckets(totalBuckets);
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());
    attr.setConcurrencyChecksEnabled(true);
    if (isOffHeap) {
      attr.setOffHeap(isOffHeap);
    }
    Region offheapRegion = getCache().createRegion(regionName, attr.create());
    assertNotNull(offheapRegion);
  }

  private void createClient(int port, String regionName) throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
        "true");
    ClientCacheFactory ccf = new ClientCacheFactory();
    ccf.addPoolServer("localhost"/* getServerHostName(Host.getHost(0)) */, port);
    ccf.setPoolMinConnections(0);
    ccf.setPoolSubscriptionEnabled(true);
    ccf.setPoolSubscriptionRedundancy(0);
    ccf.set(LOG_LEVEL, getDUnitLogLevel());
    ClientCache cCache = getClientCache(ccf);
    Region r = cCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
        .addCacheListener(new ClientCacheListener()).create(regionName);
    r.registerInterestRegex(".*");
  }

  class ClientCacheListener extends CacheListenerAdapter {
    private int eventCount;

    @Override
    public void afterCreate(EntryEvent event) {
      onEvent(event);
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      onEvent(event);
    }

    @Override
    public void afterDestroy(EntryEvent event) {
      onEvent(event);
    }

    private void onEvent(EntryEvent event) {
      this.eventCount++;
    }

    public int getEventCount() {
      return this.eventCount;
    }
  };

  private int getClientCacheListnerEventCount(String regionName) {
    Region r = getCache().getRegion(regionName);
    CacheListener<?, ?>[] listeners = r.getAttributes().getCacheListeners();
    for (CacheListener<?, ?> listener : listeners) {
      if (listener instanceof ClientCacheListener) {
        return ((ClientCacheListener) listener).getEventCount();
      }
    }
    return 0;
  }


  @Test
  public void testTwoPoolsNotAllowed() {
    Host host = Host.getHost(0);
    VM datastore1 = host.getVM(0);
    VM datastore2 = host.getVM(1);
    final boolean cachingProxy = false;

    disconnectAllFromDS(); // some other VMs seem to be hanging around and have the region this
                           // tests uses

    final int port1 = createRegionsAndStartServer(datastore1, false);
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
        "true");
    ClientCacheFactory ccf = new ClientCacheFactory();
    setCCF(port1, ccf);

    ClientCache cCache = getClientCache(ccf);

    ClientRegionFactory<CustId, Customer> custrf = cCache.createClientRegionFactory(
        cachingProxy ? ClientRegionShortcut.CACHING_PROXY : ClientRegionShortcut.PROXY);
    ClientRegionFactory<Integer, String> refrf = cCache.createClientRegionFactory(
        cachingProxy ? ClientRegionShortcut.CACHING_PROXY : ClientRegionShortcut.PROXY);
    Region<Integer, String> r = refrf.create(D_REFERENCE);
    Region<CustId, Customer> pr = custrf.create(CUSTOMER);

    // set up a second pool for the other distributed system's region
    final int port2 = createRegionOnDisconnectedServer(datastore2, true);
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer("localhost"/* getServerHostName(Host.getHost(0)) */, port2);
    pf.create("otherServer");

    ClientRegionFactory otherrf = cCache.createClientRegionFactory(
        cachingProxy ? ClientRegionShortcut.CACHING_PROXY : ClientRegionShortcut.PROXY);
    otherrf.setPoolName("otherServer");
    Region<Object, Object> otherRegion = otherrf.create(OTHER_REGION);

    TXManagerImpl mgr = getGemfireCache().getTxManager();
    mgr.begin();
    doTxOps(r, pr);
    boolean exceptionThrown = false;
    try {
      otherRegion.put("tx", "not allowed");
    } catch (TransactionException expected) {
      exceptionThrown = true;
    }

    SerializableCallable disconnect = new SerializableCallable("disconnect") {
      public Object call() throws Exception {
        InternalDistributedSystem.getConnectedInstance().disconnect();
        return null;
      }
    };

    cCache.close();
    datastore1.invoke(disconnect);
    datastore2.invoke(disconnect);

    if (!exceptionThrown) {
      fail("expected TransactionException to be thrown since two pools were used");
    }
  }

  @Test
  public void testCleanupAfterClientFailure() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    final boolean cachingProxy = false;

    disconnectAllFromDS(); // some other VMs seem to be hanging around and have the region this
                           // tests uses

    final int port1 = createRegionsAndStartServerWithTimeout(accessor, true, 5);
    createRegionOnServerWithTimeout(datastore, false, false, 5);

    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
        "true");
    ClientCacheFactory ccf = new ClientCacheFactory();
    setCCF(port1, ccf);
    ClientCache cCache = getClientCache(ccf);
    ClientRegionFactory<CustId, Customer> custrf = cCache.createClientRegionFactory(
        cachingProxy ? ClientRegionShortcut.CACHING_PROXY : ClientRegionShortcut.PROXY);
    ClientRegionFactory<Integer, String> refrf = cCache.createClientRegionFactory(
        cachingProxy ? ClientRegionShortcut.CACHING_PROXY : ClientRegionShortcut.PROXY);
    Region<Integer, String> r = refrf.create(D_REFERENCE);
    Region<CustId, Customer> pr = custrf.create(CUSTOMER);

    TXManagerImpl mgr = getGemfireCache().getTxManager();
    mgr.begin();
    doTxOps(r, pr);

    final DistributedMember myId = cCache.getDistributedSystem().getDistributedMember();

    SerializableCallable verifyExists =
        new SerializableCallable("verify txstate for client exists") {
          public Object call() throws Exception {
            TXManagerImpl txmgr = getGemfireCache().getTxManager();
            Set states = txmgr.getTransactionsForClient((InternalDistributedMember) myId);
            assertEquals(1, states.size()); // only one in-progress transaction
            return null;
          }
        };

    accessor.invoke(verifyExists);
    datastore.invoke(verifyExists);

    cCache.close();

    SerializableCallable verifyExpired = new SerializableCallable("verify txstate is expired") {
      public Object call() throws Exception {
        final TXManagerImpl txmgr = getGemfireCache().getTxManager();
        try {
          Wait.waitForCriterion(new WaitCriterion() {
            public boolean done() {
              Set states = txmgr.getTransactionsForClient((InternalDistributedMember) myId);
              org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
                  .info("found " + states.size() + " tx states for " + myId);
              return states.isEmpty();
            }

            public String description() {
              return "Waiting for transaction state to expire";
            }
          }, 15000, 500, true);
          return null;
        } finally {
          getGemfireCache().getDistributedSystem().disconnect();
        }
      }
    };
    try {
      accessor.invoke(verifyExpired);
      datastore.invoke(verifyExpired);
    } finally {
      cCache.close();
    }
  }

  @Test
  public void testCleanupAfterClientAndProxyFailure() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    final boolean cachingProxy = false;

    disconnectAllFromDS(); // some other VMs seem to be hanging around and have the region this
                           // tests uses

    final int port1 = createRegionsAndStartServerWithTimeout(accessor, true, 5);
    createRegionOnServerWithTimeout(datastore, false, false, 5);

    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
        "true");
    ClientCacheFactory ccf = new ClientCacheFactory();
    setCCF(port1, ccf);
    ClientCache cCache = getClientCache(ccf);
    ClientRegionFactory<CustId, Customer> custrf = cCache.createClientRegionFactory(
        cachingProxy ? ClientRegionShortcut.CACHING_PROXY : ClientRegionShortcut.PROXY);
    ClientRegionFactory<Integer, String> refrf = cCache.createClientRegionFactory(
        cachingProxy ? ClientRegionShortcut.CACHING_PROXY : ClientRegionShortcut.PROXY);
    Region<Integer, String> r = refrf.create(D_REFERENCE);
    Region<CustId, Customer> pr = custrf.create(CUSTOMER);

    TXManagerImpl mgr = getGemfireCache().getTxManager();
    mgr.begin();
    doTxOps(r, pr);

    final DistributedMember myId = cCache.getDistributedSystem().getDistributedMember();

    SerializableCallable verifyExists =
        new SerializableCallable("verify txstate for client exists") {
          public Object call() throws Exception {
            TXManagerImpl txmgr = getGemfireCache().getTxManager();
            Set states = txmgr.getTransactionsForClient((InternalDistributedMember) myId);
            assertEquals(1, states.size()); // only one in-progress transaction
            return null;
          }
        };

    accessor.invoke(verifyExists);
    datastore.invoke(verifyExists);

    accessor.invoke(() -> closeCache());
    accessor.invoke(() -> disconnectFromDS());

    SerializableCallable verifyExpired = new SerializableCallable("verify txstate is expired") {
      public Object call() throws Exception {
        final TXManagerImpl txmgr = getGemfireCache().getTxManager();
        return verifyTXStateExpired(myId, txmgr);
      }
    };
    try {
      datastore.invoke(verifyExpired);
    } finally {
      cCache.close();
    }
  }

  void doTxOps(Region<Integer, String> r, Region<CustId, Customer> pr) {
    for (int i = 0; i < 5; i++) {
      CustId custId = new CustId(i);
      Customer cust = new Customer("name" + i, "address" + i);
      getGemfireCache().getLogger().info("putting:" + custId);
      pr.put(custId, cust);
      r.put(i, "value" + i);
    }
  }

  public static DistributedMember getVMDistributedMember() {
    return InternalDistributedSystem.getAnyInstance().getDistributedMember();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFailoverAfterProxyFailure() throws InterruptedException {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM accessor2 = host.getVM(2);
    final boolean cachingProxy = false;

    disconnectAllFromDS(); // some other VMs seem to be hanging around and have the region this
                           // tests uses

    int[] ports = new int[2];
    ports[0] = createRegionsAndStartServerWithTimeout(accessor, true, 5);
    ports[1] = createRegionsAndStartServerWithTimeout(accessor2, true, 5);
    createRegionOnServerWithTimeout(datastore, false, false, 5);

    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
        "true");
    ClientCacheFactory ccf = new ClientCacheFactory();
    setCCF(ports, ccf);
    ClientCache cCache = getClientCache(ccf);
    ClientRegionFactory<CustId, Customer> custrf = cCache.createClientRegionFactory(
        cachingProxy ? ClientRegionShortcut.CACHING_PROXY : ClientRegionShortcut.PROXY);
    ClientRegionFactory<Integer, String> refrf = cCache.createClientRegionFactory(
        cachingProxy ? ClientRegionShortcut.CACHING_PROXY : ClientRegionShortcut.PROXY);
    Region<Integer, String> r = refrf.create(D_REFERENCE);
    Region<CustId, Customer> pr = custrf.create(CUSTOMER);

    TXManagerImpl mgr = getGemfireCache().getTxManager();
    mgr.begin();
    doTxOps(r, pr);

    final DistributedMember myId = cCache.getDistributedSystem().getDistributedMember();
    final DistributedMember accessorId = (DistributedMember) accessor
        .invoke(() -> ClientServerTransactionDUnitTest.getVMDistributedMember());
    final DistributedMember accessor2Id = (DistributedMember) accessor2
        .invoke(() -> ClientServerTransactionDUnitTest.getVMDistributedMember());

    SerializableCallable verifyExists =
        new SerializableCallable("verify txstate for client exists") {
          public Object call() throws Exception {
            TXManagerImpl txmgr = getGemfireCache().getTxManager();
            Set<TXId> states = txmgr.getTransactionsForClient((InternalDistributedMember) myId);
            assertEquals(1, states.size()); // only one in-progress transaction
            return null;
          }
        };

    datastore.invoke(verifyExists);

    SerializableCallable getProxyServer = new SerializableCallable("get proxy server") {
      public Object call() throws Exception {
        final TXManagerImpl txmgr = getGemfireCache().getTxManager();
        DistributedMember proxyServer = null;
        TXStateProxyImpl tx = null;
        Set<TXStateProxy> states =
            txmgr.getTransactionStatesForClient((InternalDistributedMember) myId);
        assertEquals(1, states.size());
        Iterator<TXStateProxy> iterator = states.iterator();
        if (iterator.hasNext()) {
          tx = (TXStateProxyImpl) iterator.next();
          assertTrue(tx.isRealDealLocal());
          proxyServer = ((TXState) tx.realDeal).getProxyServer();
        }
        return proxyServer;
      }
    };

    final DistributedMember proxy = (DistributedMember) datastore.invoke(getProxyServer);

    if (proxy.equals(accessorId)) {
      accessor.invoke(() -> closeCache());
      accessor.invoke(() -> disconnectFromDS());
    } else {
      assertTrue(proxy.equals(accessor2Id));
      accessor2.invoke(() -> closeCache());
      accessor2.invoke(() -> disconnectFromDS());
    }

    doTxOps(r, pr);

    SerializableCallable verifyProxyServerChanged =
        new SerializableCallable("verify proxy server is updated") {
          public Object call() throws Exception {
            final TXManagerImpl txmgr = getGemfireCache().getTxManager();
            TXStateProxyImpl tx = null;
            Set<TXStateProxy> states =
                txmgr.getTransactionStatesForClient((InternalDistributedMember) myId);
            assertEquals(1, states.size());
            Iterator<TXStateProxy> iterator = states.iterator();
            if (iterator.hasNext()) {
              tx = (TXStateProxyImpl) iterator.next();
              assertTrue(tx.isRealDealLocal());
            }
            return verifyProxyServerChanged(tx, proxy);
          }
        };
    try {
      datastore.invoke(verifyProxyServerChanged);
    } finally {
      cCache.close();
    }
  }

  void setCCF(final int port1, ClientCacheFactory ccf) {
    ccf.addPoolServer("localhost"/* getServerHostName(Host.getHost(0)) */, port1);
    ccf.setPoolSubscriptionEnabled(false);
    ccf.set(LOG_LEVEL, getDUnitLogLevel());
  }

  void setCCF(final int[] ports, ClientCacheFactory ccf) {
    for (int port : ports) {
      ccf.addPoolServer("localhost", port);
    }
    ccf.setPoolSubscriptionEnabled(false);
    ccf.set(LOG_LEVEL, getDUnitLogLevel());
  }

  @Test
  public void testBasicCommitOnEmpty() {
    doBasicTransaction(false, false, true);
  }

  @Test
  public void testBasicCommitOnEmptyUsingJTA() {
    doBasicTransaction(false, true, true);
  }

  @Test
  public void testBasicCommit() {
    doBasicTransaction(true, false, true);
  }

  @Test
  public void testBasicCommitUsingJTA() {
    doBasicTransaction(true, true, true);
  }

  @Test
  public void testBasicRollbackUsingJTA() {
    doBasicTransaction(true, true, false);
  }

  private void doBasicTransaction(final boolean prePopulateData, final boolean useJTA,
      final boolean isCommit) {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    int port1 = createRegionsAndStartServer(server, false);
    if (prePopulateData) {
      createClientRegionAndPopulateData(client, port1, false);
    } else {
      createClientRegion(client, port1, false);
    }

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        String suffix = prePopulateData ? "Updated" : "";
        Region<CustId, Customer> r = getGemfireCache().getRegion(D_REFERENCE);
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        int initSize = prePopulateData ? 5 : 0;
        assertEquals(initSize, pr.size());
        assertEquals(initSize, r.size());

        org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
            .info("Looking up transaction manager");
        TXManagerImpl mgr = (TXManagerImpl) getCache().getCacheTransactionManager();
        Context ctx = getCache().getJNDIContext();
        UserTransaction utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
        org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info("starting transaction");
        if (useJTA) {
          utx.begin();
        } else {
          mgr.begin();
        }
        org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info("done starting transaction");
        for (int i = 0; i < MAX_ENTRIES; i++) {
          CustId custId = new CustId(i);
          Customer cust = new Customer("name" + suffix + i, "address" + suffix + i);
          r.put(custId, cust);
          pr.put(custId, cust);
        }
        for (int i = 0; i < MAX_ENTRIES; i++) {
          CustId custId = new CustId(i);
          Customer cust = new Customer("name" + suffix + i, "address" + suffix + i);
          assertEquals(cust, r.get(custId));
          assertEquals(cust, pr.get(custId));
          org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
              .info("SWAP:get:" + r.get(custId));
          org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
              .info("SWAP:get:" + pr.get(custId));
        }
        org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info("suspending transaction");
        if (!useJTA) {
          TXStateProxy tx = mgr.internalSuspend();
          if (prePopulateData) {
            for (int i = 0; i < 5; i++) {
              CustId custId = new CustId(i);
              Customer cust = new Customer("customer" + i, "address" + i);
              assertEquals(cust, r.get(custId));
              assertEquals(cust, pr.get(custId));
            }
          }
          for (int i = 5; i < MAX_ENTRIES; i++) {
            assertNull(r.get(new CustId(i)));
            assertNull(pr.get(new CustId(i)));
          }
          org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info("resuming transaction");
          mgr.resume(tx);
        }
        assertEquals("r sized should be " + MAX_ENTRIES + " but it is:" + r.size(), MAX_ENTRIES,
            r.size());
        assertEquals("pr sized should be " + MAX_ENTRIES + " but it is:" + pr.size(), MAX_ENTRIES,
            pr.size());
        org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info("committing transaction");
        if (isCommit) {
          if (useJTA) {
            utx.commit();
          } else {
            getCache().getCacheTransactionManager().commit();
          }
        } else {
          if (useJTA) {
            utx.rollback();
          } else {
            getCache().getCacheTransactionManager().rollback();
          }
        }
        org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
            .info("done " + (isCommit ? "committing" : "rollingback") + "transaction");
        int expectedRegionSize = isCommit ? MAX_ENTRIES : 5;

        assertEquals("r sized should be " + expectedRegionSize + " but it is:" + r.size(),
            expectedRegionSize, r.size());
        assertEquals("pr sized should be " + expectedRegionSize + " but it is:" + pr.size(),
            expectedRegionSize, pr.size());

        return null;
      }
    });

    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<Integer, String> r = getGemfireCache().getRegion(D_REFERENCE);
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        int expectedRegionSize = isCommit ? MAX_ENTRIES : 5;
        assertEquals("r sized should be " + expectedRegionSize + " but it is:" + r.size(),
            expectedRegionSize, r.size());
        assertEquals("pr sized should be " + expectedRegionSize + " but it is:" + pr.size(),
            expectedRegionSize, pr.size());
        return null;
      }
    });

    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        String suffix = prePopulateData ? "Updated" : "";
        Region<Integer, String> r = getGemfireCache().getRegion(D_REFERENCE);
        Region<Integer, String> pr = getGemfireCache().getRegion(CUSTOMER);
        for (int i = 0; i < MAX_ENTRIES; i++) {
          CustId custId = new CustId(i);
          Customer cust = new Customer("name" + suffix + i, "address" + suffix + i);
          if (isCommit) {
            assertEquals(cust, r.get(custId));
            assertEquals(cust, pr.get(custId));
          } else {
            assertNotSame(cust, r.get(custId));
            assertNotSame(cust, pr.get(custId));
          }
        }
        return null;
      }
    });

    verifyVersionTags(client, server, null, null);
  }

  @Test
  public void testTXCreationAndCleanupAtCommit() throws Exception {
    doBasicChecks(true);
  }

  @Test
  public void testTXCreationAndCleanupAtRollback() throws Exception {
    doBasicChecks(false);
  }

  private void doBasicChecks(final boolean commit) throws Exception {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    int port1 = createRegionsAndStartServer(server, false);
    createClientRegionAndPopulateData(client, port1, false);

    final TXId txId = (TXId) client.invoke(new DoOpsInTX(OP.PUT));

    server.invoke(new SerializableCallable("verify tx") {
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        assertTrue(mgr.isHostedTxInProgress(txId));
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        TXStateProxy tx = mgr.internalSuspend();
        assertNotNull(tx);
        mgr.resume(tx);
        if (commit) {
          mgr.commit();
        } else {
          mgr.rollback();
        }
        return null;
      }
    });

    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final TXManagerImpl mgr = getGemfireCache().getTxManager();
        WaitCriterion w = new WaitCriterion() {
          public boolean done() {
            return !mgr.isHostedTxInProgress(txId);
          }

          public String description() {
            return "waiting for hosted tx in progress to terminate";
          }
        };
        Wait.waitForCriterion(w, 10000, 200, true);
        return null;
      }
    });
    if (commit) {
      client.invoke(new SerializableCallable() {
        public Object call() throws Exception {
          verifyAfterCommit(OP.PUT);
          System.out.println("expected verification to fail for this VM");
          return null;
        }
      });
    } else {
      client.invoke(new SerializableCallable() {
        public Object call() throws Exception {
          verifyAfterRollback(OP.PUT);
          return null;
        }
      });
    }
  }


  @Test
  public void testPutallRollbackInServer() throws Exception {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    int port1 = createRegionsAndStartServer(server, false);
    createClientRegionAndPopulateData(client, port1, false);

    server.invoke(new SerializableCallable("verify tx") {
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        mgr.begin();
        CustId custId = new CustId(1);
        OrderId orderId = new OrderId(1000, custId);
        Order expectedOrder = new Order("fooOrder");
        Map map = new HashMap();
        map.put(orderId, expectedOrder);
        orderRegion.putAll(map);
        mgr.rollback();
        assertNull(orderRegion.get(orderId));
        return null;
      }
    });
  }

  @Test
  public void testPutallRollbackInClient() throws Exception {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    /* int port1 = */ createRegionsAndStartServer(server, false);

    server.invoke(new SerializableCallable("verify tx") {
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        mgr.begin();
        CustId custId = new CustId(1);
        OrderId orderId = new OrderId(1000, custId);
        Order expectedOrder = new Order("fooOrder");
        Map map = new HashMap();
        map.put(orderId, expectedOrder);
        orderRegion.putAll(map);
        mgr.rollback();
        assertNull(orderRegion.get(orderId));
        return null;
      }
    });
  }

  @Ignore
  @Test
  public void testGetAllRollbackInServer() throws Exception {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    createRegionsAndStartServer(server, false);

    server.invoke(new SerializableCallable("verify getAll tx") {
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        orderRegion.getAttributesMutator().setCacheLoader(new CacheLoader() {
          public void close() {}

          public Object load(LoaderHelper helper) throws CacheLoaderException {
            return new Order(helper.getKey() + "_loaded");
          }
        });
        mgr.begin();
        CustId custId = new CustId(1);
        OrderId orderId = new OrderId(1000, custId);
        Set<OrderId> keys = new HashSet();
        keys.add(orderId);
        Order order = (orderRegion.getAll(keys)).get(orderId);
        assertNotNull(order);
        mgr.rollback();
        assertNull(orderRegion.getEntry(orderId));
        return null;
      }
    });
  }

  @Ignore
  @Test
  public void testGetAllRollbackInClient() throws Exception {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    int port1 = createRegionsAndStartServer(server, false);
    createClientRegionAndPopulateData(client, port1, false);

    server.invoke(new SerializableCallable("add cache loader") {
      public Object call() throws Exception {
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        orderRegion.getAttributesMutator().setCacheLoader(new CacheLoader() {
          public void close() {}

          public Object load(LoaderHelper helper) throws CacheLoaderException {
            return new Order(helper.getKey() + "_loaded");
          }
        });
        return null;
      }
    });

    client.invoke(new SerializableCallable("verify getAll uses tx") {
      public Object call() throws Exception {
        Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        mgr.begin();
        CustId custId = new CustId(1);
        OrderId orderId = new OrderId(1000, custId);
        Set<OrderId> keys = new HashSet();
        keys.add(orderId);
        Order order = (orderRegion.getAll(keys)).get(orderId);
        assertNotNull(order);
        mgr.rollback();
        org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
            .info("entry for " + orderId + " = " + orderRegion.getEntry(orderId));
        assertNull(orderRegion.getEntry(orderId));
        return null;
      }
    });
  }

  @Test
  public void testClientCommitAndDataStoreGetsEvent() throws Exception {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(accessor);

    createClientRegion(client, port, false, true);

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region custRegion = getCache().getRegion(CUSTOMER);
        custRegion.getAttributesMutator().addCacheListener(new ServerListener());
        return null;
      }
    });


    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        CustId custId = new CustId(1);
        // OrderId orderId = new OrderId(1, custId);
        getCache().getCacheTransactionManager().begin();
        custRegion.put(custId, new Customer("foo", "bar"));
        // orderRegion.put(orderId, new Order("fooOrder"));
        // refRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        ServerListener l = (ServerListener) custRegion.getAttributes().getCacheListeners()[0];
        getCache().getLogger().info("SWAP:CLIENTinvoked:" + l.invoked);
        assertTrue(l.invoked);
        return null;
      }
    });
  }



  @Test
  public void testClientCreateAndTwoInvalidates() throws Exception {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(accessor);

    createClientRegion(client, port, false, true);

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region custRegion = getCache().getRegion(CUSTOMER);
        custRegion.getAttributesMutator().addCacheListener(new ClientListener());
        return null;
      }
    });


    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        CustId custId = new CustId(1938493204);
        // OrderId orderId = new OrderId(1, custId);
        getCache().getCacheTransactionManager().begin();
        custRegion.create(custId, new Customer("foo", "bar"));
        custRegion.invalidate(custId);
        custRegion.invalidate(custId);
        // orderRegion.put(orderId, new Order("fooOrder"));
        // refRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

  }



  @Test
  public void testClientCommitsAndJustGets() throws Exception {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(accessor);

    createClientRegion(client, port, false, true);

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region custRegion = getCache().getRegion(CUSTOMER);
        custRegion.getAttributesMutator().addCacheListener(new ClientListener());
        return null;
      }
    });


    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        CustId custId = new CustId(1);
        // OrderId orderId = new OrderId(1, custId);
        getCache().getCacheTransactionManager().begin();
        custRegion.get(custId);
        // orderRegion.put(orderId, new Order("fooOrder"));
        // refRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

  }


  @Test
  public void testClientDoesUnsupportedLocalOps() throws Exception {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(accessor);

    createClientRegion(client, port, false, true);

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region custRegion = getCache().getRegion(CUSTOMER);
        custRegion.getAttributesMutator().addCacheListener(new ClientListener());
        return null;
      }
    });


    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        CustId custId = new CustId(1);
        // OrderId orderId = new OrderId(1, custId);
        custRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().begin();
        try {
          custRegion.localDestroy(custId);
          fail("Should have thrown UOE");
        } catch (UnsupportedOperationInTransactionException uoi) {
          // chill
        }

        try {
          custRegion.localInvalidate(custId);
          fail("Should have thrown UOE");
        } catch (UnsupportedOperationInTransactionException uoi) {
          // chill
        }

        // orderRegion.put(orderId, new Order("fooOrder"));
        // refRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

  }


  @Test
  public void testClientCommitsWithRIAndOnlyGetsOneEvent() throws Exception {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(accessor);

    createClientRegion(client, port, false, true);

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region custRegion = getCache().getRegion(CUSTOMER);
        custRegion.getAttributesMutator().addCacheListener(new ClientListener());
        return null;
      }
    });


    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        CustId custId = new CustId(1);
        // OrderId orderId = new OrderId(1, custId);
        getCache().getCacheTransactionManager().begin();
        custRegion.put(custId, new Customer("foo", "bar"));
        // orderRegion.put(orderId, new Order("fooOrder"));
        // refRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        ClientListener cl = (ClientListener) custRegion.getAttributes().getCacheListeners()[0];
        assertTrue(cl.invoked);
        assertEquals("it should be 1 but its:" + cl.invokeCount, 1, cl.invokeCount);
        return null;
      }
    });
  }

  @Test
  public void testDatastoreCommitsWithPutAllAndRI() throws Exception {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(accessor);

    createClientRegion(client, port, false, true);

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region custRegion = getCache().getRegion(CUSTOMER);
        custRegion.getAttributesMutator().addCacheListener(new ClientListener());
        return null;
      }
    });


    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        CustId custId = new CustId(1);
        // OrderId orderId = new OrderId(1, custId);
        getCache().getCacheTransactionManager().begin();
        Map map = new HashMap();
        map.put(custId, new Customer("foo", "bar"));
        custRegion.putAll(map);
        // orderRegion.put(orderId, new Order("fooOrder"));
        // refRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        final ClientListener cl =
            (ClientListener) custRegion.getAttributes().getCacheListeners()[0];
        Wait.waitForCriterion(new WaitCriterion() {

          @Override
          public boolean done() {
            return cl.invoked;
          }

          @Override
          public String description() {
            return "Listener was not invoked in 30 seconds";
          }
        }, 30000, 100, true);

        assertEquals(1, cl.invokeCount);
        return null;
      }
    });
  }



  @Test
  public void testClientCommitsWithPutAllAndRI() throws Exception {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(accessor);

    createClientRegion(client, port, false, true);

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region custRegion = getCache().getRegion(CUSTOMER);
        custRegion.getAttributesMutator().addCacheListener(new ClientListener());
        return null;
      }
    });


    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        CustId custId = new CustId(1);
        // OrderId orderId = new OrderId(1, custId);
        getCache().getCacheTransactionManager().begin();
        Map map = new HashMap();
        map.put(custId, new Customer("foo", "bar"));
        custRegion.putAll(map);
        // orderRegion.put(orderId, new Order("fooOrder"));
        // refRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        ClientListener cl = (ClientListener) custRegion.getAttributes().getCacheListeners()[0];
        assertTrue(cl.invoked);
        assertTrue(cl.putAllOp);
        assertFalse(cl.isOriginRemote);
        assertEquals("it should be 1 but its:" + cl.invokeCount, 1, cl.invokeCount);
        return null;
      }
    });
  }


  @Test
  public void testClientRollsbackWithPutAllAndRI() throws Exception {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(accessor);

    createClientRegion(client, port, false, true);

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region custRegion = getCache().getRegion(CUSTOMER);
        custRegion.getAttributesMutator().addCacheListener(new ClientListener());
        return null;
      }
    });


    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        CustId custId = new CustId(1);
        // OrderId orderId = new OrderId(1, custId);
        getCache().getCacheTransactionManager().begin();
        Map map = new HashMap();
        map.put(custId, new Customer("foo", "bar"));
        custRegion.putAll(map);
        // orderRegion.put(orderId, new Order("fooOrder"));
        // refRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().rollback();
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        ClientListener cl = (ClientListener) custRegion.getAttributes().getCacheListeners()[0];
        getCache().getLogger().info("SWAP:CLIENTinvoked:" + cl.invoked);
        assertTrue(!cl.invoked);
        assertTrue(!cl.putAllOp);
        assertEquals("it should be 0 but its:" + cl.invokeCount, 0, cl.invokeCount);
        return null;
      }
    });
  }

  @Test
  public void testClientInitiatedInvalidates() throws Exception {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    initAccessorAndDataStore(accessor, datastore, 0);
    int port = startServer(accessor);

    createClientRegion(client, port, false, true);

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region custRegion = getCache().getRegion(CUSTOMER);
        custRegion.getAttributesMutator().addCacheListener(new ClientListener());
        return null;
      }
    });


    /*
     * Test a no-op commit: put/invalidate
     */

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        CustId custId = new CustId(1777777777);
        getCache().getCacheTransactionManager().begin();
        custRegion.put(custId, new Customer("foo", "bar"));
        custRegion.invalidate(custId);
        // orderRegion.put(orderId, new Order("fooOrder"));
        // refRegion.put(custId, new Customer("foo", "bar"));
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });

    /*
     * Validate nothing came through
     */
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        ClientListener cl = (ClientListener) custRegion.getAttributes().getCacheListeners()[0];
        getCache().getLogger().info("SWAP:CLIENTinvoked:" + cl.invoked);
        assertTrue(cl.invoked);
        assertEquals(1, cl.putCount);
        assertEquals(1, cl.invokeCount);
        assertEquals(0, cl.invalidateCount);
        CustId custId = new CustId(1777777777);
        assertTrue(custRegion.containsKey(custId));
        assertTrue(!custRegion.containsValueForKey(custId));
        cl.reset();
        return null;
      }
    });

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        ClientListener cl = (ClientListener) custRegion.getAttributes().getCacheListeners()[0];
        getCache().getLogger().info("SWAP:CLIENTinvoked:" + cl.invoked);
        assertTrue(cl.invoked);
        assertEquals(1, cl.putCount);
        assertEquals(1, cl.invokeCount);
        CustId custId = new CustId(1777777777);
        assertTrue(custRegion.containsKey(custId));
        assertTrue(!custRegion.containsValueForKey(custId));
        cl.reset();
        return null;
      }
    });

    /*
     * Ok lets do a put in tx, then an invalidate in a another tx and make sure it invalidates on
     * client and server
     */

    client.invoke(doAPutInTx);
    client.invoke(doAnInvalidateInTx);

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        ClientListener cl = (ClientListener) custRegion.getAttributes().getCacheListeners()[0];
        getCache().getLogger().info("SWAP:CLIENTinvoked:" + cl.invoked);
        assertTrue(cl.invoked);
        assertEquals("totalEvents should be 2 but its:" + cl.invokeCount, 2, cl.invokeCount);
        assertEquals("it should be 1 but its:" + cl.invalidateCount, 1, cl.invalidateCount);
        assertEquals("it should be 1 but its:" + cl.putCount, 1, cl.putCount);
        CustId custId = new CustId(1);
        assertTrue(custRegion.containsKey(custId));
        assertFalse(custRegion.containsValueForKey(custId));
        return null;
      }
    });

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
        // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
        // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
        ClientListener cl = (ClientListener) custRegion.getAttributes().getCacheListeners()[0];
        getCache().getLogger().info("SWAP:CLIENTinvoked:" + cl.invoked);
        assertTrue(cl.invoked);
        assertEquals("totalEvents should be 2 but its:" + cl.invokeCount, 2, cl.invokeCount);
        assertEquals("it should be 1 but its:" + cl.invalidateCount, 1, cl.invalidateCount);
        assertEquals("it should be 1 but its:" + cl.putCount, 1, cl.putCount);
        return null;
      }
    });



  }



  SerializableCallable validateNoEvents = new SerializableCallable() {
    public Object call() throws Exception {
      Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
      // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
      // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
      ClientListener cl = (ClientListener) custRegion.getAttributes().getCacheListeners()[0];
      getCache().getLogger().info("SWAP:CLIENTinvoked:" + cl.invoked);
      assertTrue(cl.invoked);
      assertEquals("it should be 0 but its:" + cl.invokeCount, 0, cl.invokeCount);
      return null;
    }
  };


  SerializableCallable doAPutInTx = new SerializableCallable() {
    public Object call() throws Exception {
      Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
      // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
      // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
      CustId custId = new CustId(1);
      // OrderId orderId = new OrderId(1, custId);
      getCache().getCacheTransactionManager().begin();
      custRegion.put(custId, new Customer("foo", "bar"));
      getCache().getCacheTransactionManager().commit();
      return null;
    }
  };


  SerializableCallable doAnInvalidateInTx = new SerializableCallable() {
    public Object call() throws Exception {
      Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER);
      // Region<OrderId, Order> orderRegion = getCache().getRegion(ORDER);
      // Region<CustId,Customer> refRegion = getCache().getRegion(D_REFERENCE);
      CustId custId = new CustId(1);
      // OrderId orderId = new OrderId(1, custId);
      getCache().getCacheTransactionManager().begin();
      custRegion.invalidate(custId);
      getCache().getCacheTransactionManager().commit();
      return null;
    }
  };


  /**
   * client connectes to an accessor and completes a transaction
   */
  @Test
  public void testServerDelegate() {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    int port = createRegionsAndStartServer(server1, true);
    createRegionOnServer(server2);

    createClientRegion(client, port, false);

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        Region<Integer, String> r = getGemfireCache().getRegion(D_REFERENCE);
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        CustId custId = new CustId(10);
        mgr.begin();
        pr.put(custId, new Customer("name10", "address10"));
        r.put(10, "value10");
        TXStateProxy txState = mgr.internalSuspend();
        assertNull(pr.get(custId));
        assertNull(r.get(10));
        mgr.resume(txState);
        mgr.commit();
        return null;
      }
    });
    server1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<Integer, String> r = getGemfireCache().getRegion(D_REFERENCE);
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        assertEquals(new Customer("name10", "address10"), pr.get(new CustId(10)));
        assertEquals("value10", r.get(10));
        return null;
      }
    });
  }

  @Test
  public void testCommitWithPRAccessor() {
    doTxWithPRAccessor(true);
  }

  @Test
  public void testRollbackWithPRAccessor() {
    doTxWithPRAccessor(false);
  }

  private void doTxWithPRAccessor(final boolean commit) {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);

    final int port1 = createRegionsAndStartServer(server1, true);
    createRegionOnServer(server2);

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        ClientCacheFactory ccf = new ClientCacheFactory();
        setCCF(port1, ccf);
        ClientCache cCache = getClientCache(ccf);
        ClientRegionFactory<CustId, Customer> custrf =
            cCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
        ClientRegionFactory<Integer, String> refrf =
            cCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
        Region<Integer, String> r = refrf.create(D_REFERENCE);
        Region<CustId, Customer> pr = custrf.create(CUSTOMER);
        // Region<Integer, String> order = refrf.create(ORDER);

        TXManagerImpl mgr = getGemfireCache().getTxManager();
        mgr.begin();
        for (int i = 0; i < 10; i++) {
          CustId custId = new CustId(i);
          Customer cust = new Customer("name" + i, "address" + i);
          pr.put(custId, cust);
          r.put(i, "value" + i);
        }
        return null;
      }
    });

    SerializableCallable countActiveTx = new SerializableCallable() {
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        return mgr.hostedTransactionsInProgressForTest();
      }
    };

    int serv1TxCount = (Integer) server1.invoke(countActiveTx);
    int serv2TxCount = (Integer) server2.invoke(countActiveTx);

    assertEquals(2, serv1TxCount + serv2TxCount);

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        Region<Integer, String> r = getGemfireCache().getRegion(D_REFERENCE);
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        if (commit) {
          mgr.commit();
          for (int i = 0; i < 10; i++) {
            assertEquals(new Customer("name" + i, "address" + i), pr.get(new CustId(i)));
            assertEquals("value" + i, r.get(i));
          }
        } else {
          mgr.rollback();
          for (int i = 0; i < 10; i++) {
            assertNull(pr.get(new CustId(i)));
            assertNull(r.get(i));
          }
        }
        return null;
      }
    });

    serv1TxCount = (Integer) server1.invoke(countActiveTx);
    serv2TxCount = (Integer) server2.invoke(countActiveTx);
    assertEquals(0, serv1TxCount + serv2TxCount);
  }

  /**
   * there is one txState and zero or more txProxyStates
   * 
   * @throws Exception
   */
  @Test
  public void testConnectionAffinity() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);

    IgnoredException.addIgnoredException("java.net.SocketException");

    final int port1 = createRegionsAndStartServer(server1, true);
    final int port2 = createRegionsAndStartServer(server2, false);


    SerializableCallable hostedSize = new SerializableCallable() {
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        return mgr.hostedTransactionsInProgressForTest();
      }
    };

    int txcount = (Integer) server1.invoke(hostedSize) + (Integer) server2.invoke(hostedSize);
    assertTrue("expected count to be 0" + txcount, txcount == 0);



    final TXId txid = (TXId) client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolServer("localhost"/* getServerHostName(Host.getHost(0)) */, port1);
        ccf.addPoolServer("localhost", port2);
        ccf.setPoolLoadConditioningInterval(1);
        ccf.setPoolSubscriptionEnabled(false);
        ccf.set(LOG_LEVEL, getDUnitLogLevel());
        ClientCache cCache = getClientCache(ccf);
        ClientRegionFactory<CustId, Customer> custrf =
            cCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
        ClientRegionFactory<Integer, String> refrf =
            cCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
        Region<Integer, String> r = refrf.create(D_REFERENCE);
        Region<CustId, Customer> pr = custrf.create(CUSTOMER);
        // Region<Integer, String> order = refrf.create(ORDER);

        TXManagerImpl mgr = getGemfireCache().getTxManager();
        mgr.begin();
        int i = 0;
        for (int j = 0; j < 10; j++) {
          CustId custId = new CustId(i);
          Customer cust = new Customer("name" + i, "address" + i);
          getGemfireCache().getLogger().info("SWAP:putting:" + custId);
          pr.put(custId, cust);
          r.put(i, "value" + i);
        }
        return mgr.getTransactionId();
      }
    });

    SerializableCallable activeTx = new SerializableCallable() {
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        TXStateProxy tx = mgr.getHostedTXState(txid);
        mgr.getCache().getLogger().info("SWAP:activeTx:" + tx);
        // rather than returning strings representing objects
        // return different ints to represent different objects
        if (tx != null) {
          TXStateInterface realtx = ((TXStateProxyImpl) tx).getRealDeal(null, null);
          if (realtx instanceof TXState) {
            return 11;
          }
        }
        return 1;
      }
    };



    int myCount = (Integer) server1.invoke(activeTx) + (Integer) server2.invoke(activeTx);
    assertTrue("expected count to be 11 or 12 but was " + myCount, myCount >= 11 && myCount <= 12);

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        mgr.commit();
        return null;
      }
    });
  }

  /**
   * client has a client, an accessor and a datastore pool connects to the accessor and datastore we
   * then close the server in the accessor and verify failover
   */
  @Test
  public void testFailover() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    doFailoverWork(accessor, null, datastore, client, true, false);
  }

  @Test
  public void testFailoverAndCachingProxy() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    doFailoverWork(accessor, null, datastore, client, true, true);
  }

  /**
   * test has a client, two accessors and a datastore pool connects to two accessors. we then close
   * the server in first accessor and verify failover
   */
  @Test
  public void testFailoverWithP2PMessaging() {
    Host host = Host.getHost(0);
    VM accessor1 = host.getVM(0);
    VM accessor2 = host.getVM(1);
    VM datastore = host.getVM(2);
    VM client = host.getVM(3);

    doFailoverWork(accessor1, accessor2, datastore, client, false, false);
  }

  @Category(FlakyTest.class) // GEODE-1933: IllegalStateException: Thread does not have an active
                             // transaction
  @Test
  public void testFailoverWithP2PMessagingAndCachingProxy() {
    Host host = Host.getHost(0);
    VM accessor1 = host.getVM(0);
    VM accessor2 = host.getVM(1);
    VM datastore = host.getVM(2);
    VM client = host.getVM(3);

    doFailoverWork(accessor1, accessor2, datastore, client, false, true);
  }



  private void doFailoverWork(VM accessor1, VM accessor2, VM datastore, VM client,
      boolean serverOnDatastore, final boolean cachingProxy) {
    final int port1 = createRegionsAndStartServer(accessor1, true);
    final int port2 = accessor2 == null ? 0 : createRegionsAndStartServer(accessor2, true);
    final int port3 = serverOnDatastore ? createRegionsAndStartServer(datastore, false)
        : createRegionOnServer(datastore, false, false);

    /* final TXId txid = (TXId) */client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
            "true");
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolServer("localhost"/* getServerHostName(Host.getHost(0)) */, port1);
        if (port2 != 0)
          ccf.addPoolServer("localhost", port2);
        if (port3 != 0)
          ccf.addPoolServer("localhost", port3);
        ccf.setPoolSubscriptionEnabled(false);
        ccf.set(LOG_LEVEL, getDUnitLogLevel());
        ClientCache cCache = getClientCache(ccf);
        ClientRegionFactory<CustId, Customer> custrf = cCache.createClientRegionFactory(
            cachingProxy ? ClientRegionShortcut.CACHING_PROXY : ClientRegionShortcut.PROXY);
        ClientRegionFactory<Integer, String> refrf = cCache.createClientRegionFactory(
            cachingProxy ? ClientRegionShortcut.CACHING_PROXY : ClientRegionShortcut.PROXY);
        Region<Integer, String> r = refrf.create(D_REFERENCE);
        Region<CustId, Customer> pr = custrf.create(CUSTOMER);
        // Region<Integer, String> order = refrf.create(ORDER);

        TXManagerImpl mgr = getGemfireCache().getTxManager();
        mgr.begin();
        for (int i = 0; i < 5; i++) {
          CustId custId = new CustId(i);
          Customer cust = new Customer("name" + i, "address" + i);
          getGemfireCache().getLogger().info("SWAP:putting:" + custId);
          pr.put(custId, cust);
          r.put(i, "value" + i);
        }
        return mgr.getTransactionId();
      }
    });

    accessor1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        for (CacheServer s : getCache().getCacheServers()) {
          getCache().getLogger().info("SWAP:Stopping " + s);
          s.stop();
        }
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        /* TXManagerImpl mgr = */ getGemfireCache().getTxManager();
        Region<Integer, String> r = getGemfireCache().getRegion(D_REFERENCE);
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        for (int i = 5; i < 10; i++) {
          CustId custId = new CustId(i);
          Customer cust = new Customer("name" + i, "address" + i);
          getGemfireCache().getLogger().info("SWAP:AfterFailover:putting:" + custId);
          pr.put(custId, cust);
          r.put(i, "value" + i);
        }
        return null;
      }
    });

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        assertEquals(1, mgr.hostedTransactionsInProgressForTest());
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        Region<Integer, String> r = getGemfireCache().getRegion(D_REFERENCE);
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        mgr.commit();
        for (int i = 0; i < 10; i++) {
          if (cachingProxy) {
            assertTrue(pr.containsKey(new CustId(i)));
            assertTrue(r.containsKey(i));
          }
          assertEquals(new Customer("name" + i, "address" + i), pr.get(new CustId(i)));
          assertEquals("value" + i, r.get(i));
        }
        return null;
      }
    });
  }

  @Test
  public void testGetEntry() {
    Host host = Host.getHost(0);
    // VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    int port = createRegionsAndStartServer(datastore, false);
    createClientRegion(client, port, true);

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        pr.getAttributesMutator().setCacheLoader(new CacheLoader<CustId, Customer>() {
          public void close() {}

          public Customer load(LoaderHelper<CustId, Customer> helper) throws CacheLoaderException {
            throw new RuntimeException("Loader should not be called");
          }
        });
        pr.put(new CustId(10), new Customer("name10", "address10"));
        pr.invalidate(new CustId(10));
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        // Region<Integer, String> r = getGemfireCache().getRegion(D_REFERENCE);
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        CustId key = new CustId(0);
        Customer val = new Customer("name0", "address0");
        pr.put(key, val);
        mgr.begin();
        Entry entry = pr.getEntry(key);
        assertNotNull(entry);
        assertEquals(key, entry.getKey());
        assertEquals(val, entry.getValue());
        Entry entry2 = pr.getEntry(new CustId(10));
        assertNotNull(entry2);
        assertEquals(new CustId(10), entry2.getKey());
        assertNull(entry2.getValue());
        assertNull(pr.getEntry(new CustId(100)));
        mgr.commit();
        return null;
      }
    });
  }



  @Test
  public void testBug42920() {
    Host host = Host.getHost(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    int port = createRegionsAndStartServer(datastore, false);
    createClientRegion(client, port, true);

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        pr.getAttributesMutator().setCacheWriter(new ServerWriter());
        return null;
      }
    });
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        /* CacheTransactionManager mgr = */ getCache().getCacheTransactionManager();
        pr.put(new CustId(0), new Customer("name0", "address0"));
        pr.put(new CustId(1), new Customer("name1", "address1"));
        return null;
      }
    });
  }

  // Disabled due to bug 47083
  @Ignore
  @Test
  public void testCallbacks() {
    Host host = Host.getHost(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);

    int port = createRegionsAndStartServer(datastore, false);
    createClientRegion(client, port, true);

    class ClientTxListener extends TransactionListenerAdapter {
      private boolean afterRollbackInvoked = false;
      boolean afterCommitInvoked = false;

      @Override
      public void afterCommit(TransactionEvent event) {
        afterCommitInvoked = true;
        verifyEvent(event);
      }

      @Override
      public void afterRollback(TransactionEvent event) {
        afterRollbackInvoked = true;
        verifyEvent(event);
      }

      protected void verifyEvent(TransactionEvent event) {
        Iterator it = event.getEvents().iterator();
        int i = 0;
        while (it.hasNext()) {
          EntryEvent ev = (EntryEvent) it.next();
          if (i == 0)
            assertNull(ev.getNewValue());
          if (i > 1) {
            assertEquals(new CustId(i), ev.getKey());
            assertEquals(new Customer("name" + i, "address" + i), ev.getNewValue());
          }
          assertTrue(ev.isOriginRemote());
          i++;
        }
        assertEquals(5, event.getEvents().size());
      }
    }

    class ClientTxWriter implements TransactionWriter {
      boolean txWriterCalled = false;

      public void close() {}

      public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
        txWriterCalled = true;
        Iterator it = event.getEvents().iterator();
        int i = 0;
        while (it.hasNext()) {
          EntryEvent ev = (EntryEvent) it.next();
          if (i == 0)
            assertNull(ev.getNewValue());
          if (i > 1) {
            assertEquals(new CustId(i), ev.getKey());
            assertEquals(new Customer("name" + i, "address" + i), ev.getNewValue());
          }
          assertTrue(ev.isOriginRemote());
          i++;
        }
        assertEquals(5, event.getEvents().size());
      }
    }

    class ClientListener extends CacheListenerAdapter {
      boolean invoked = false;

      @Override
      public void afterCreate(EntryEvent event) {
        CustId c = (CustId) event.getKey();
        if (c.getCustId() > 1) {
          invoked = true;
        }
        // we document that client transaction are proxied to the server
        // and that the callback should be handled appropriately (hence true)
        assertTrue(event.isOriginRemote());
        assertTrue(event.isOriginRemote());
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        assertFalse(event.isOriginRemote());
      }
    }
    class ClientWriter extends CacheWriterAdapter {
      @Override
      public void beforeCreate(EntryEvent event) throws CacheWriterException {
        CustId c = (CustId) event.getKey();
        if (c.getCustId() < 2) {
          return;
        }
        fail("cache writer should not be invoked");
      }

      @Override
      public void beforeUpdate(EntryEvent event) throws CacheWriterException {
        fail("cache writer should not be invoked");
      }
    }

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        CacheTransactionManager mgr = getCache().getCacheTransactionManager();
        mgr.addListener(new ClientTxListener());
        mgr.setWriter(new ClientTxWriter());
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        pr.getAttributesMutator().addCacheListener(new ServerListener());
        pr.getAttributesMutator().setCacheWriter(new ServerWriter());
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        CacheTransactionManager mgr = getCache().getCacheTransactionManager();
        mgr.addListener(new ClientTxListener());
        try {
          mgr.setWriter(new ClientTxWriter());
          fail("expected exception not thrown");
        } catch (IllegalStateException e) {
        }
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        pr.getAttributesMutator().addCacheListener(new ClientListener());
        pr.getAttributesMutator().setCacheWriter(new ClientWriter());
        return null;
      }
    });

    class doOps extends SerializableCallable {
      public doOps(boolean commit) {
        this.commit = commit;
      }

      boolean commit = false;

      public Object call() throws Exception {
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        CacheTransactionManager mgr = getCache().getCacheTransactionManager();
        pr.put(new CustId(0), new Customer("name0", "address0"));
        pr.put(new CustId(1), new Customer("name1", "address1"));
        mgr.begin();
        pr.invalidate(new CustId(0));
        pr.destroy(new CustId(1));
        for (int i = 2; i < 5; i++) {
          pr.put(new CustId(i), new Customer("name" + i, "address" + i));
        }
        if (commit) {
          mgr.commit();
        } else {
          mgr.rollback();
        }
        return null;
      }
    }
    client.invoke(new doOps(false));

    datastore.invoke(new SerializableCallable() {
      @SuppressWarnings("synthetic-access")
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getCacheTransactionManager();
        ClientTxListener l = (ClientTxListener) mgr.getListeners()[0];
        assertTrue(l.afterRollbackInvoked);
        return null;
      }
    });

    client.invoke(new doOps(true));

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getCacheTransactionManager();
        ClientTxListener l = (ClientTxListener) mgr.getListeners()[0];
        assertTrue(l.afterCommitInvoked);
        ClientTxWriter w = (ClientTxWriter) mgr.getWriter();
        assertTrue(w.txWriterCalled);
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getCacheTransactionManager();
        ClientTxListener l = (ClientTxListener) mgr.getListeners()[0];
        assertFalse(l.afterCommitInvoked);
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        ClientListener cl = (ClientListener) pr.getAttributes().getCacheListeners()[0];
        assertTrue(cl.invoked);
        return null;
      }
    });
  }

  @Test
  public void testTXListenerOnRedundant() {
    Host host = Host.getHost(0);
    VM datastore1 = host.getVM(0);
    VM datastore2 = host.getVM(1);
    VM client = host.getVM(2);
    int port = createRegionOnServer(datastore1, true, false, 1);
    createRegionOnServer(datastore2, false, false, 1);
    createClientRegion(client, port, true);

    class RedundantListener extends TransactionListenerAdapter {
      int invoked = 0;

      @Override
      public void afterCommit(TransactionEvent event) {
        invoked++;
      }
    }

    SerializableCallable registerTxListener = new SerializableCallable() {
      public Object call() throws Exception {
        getCache().getCacheTransactionManager().addListener(new RedundantListener());
        return null;
      }
    };

    datastore1.invoke(registerTxListener);
    datastore2.invoke(registerTxListener);

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        getGemfireCache().getCacheTransactionManager().begin();
        pr.put(new CustId(1), new Customer("name1", "address1"));
        getGemfireCache().getCacheTransactionManager().commit();
        return null;
      }
    });

    SerializableCallable listenerInvoked = new SerializableCallable() {
      public Object call() throws Exception {
        RedundantListener l =
            (RedundantListener) getCache().getCacheTransactionManager().getListeners()[0];
        return l.invoked;
      }
    };

    int listenerInvokedCount =
        (Integer) datastore1.invoke(listenerInvoked) + (Integer) datastore2.invoke(listenerInvoked);
    assertEquals(1, listenerInvokedCount);
  }

  @Test
  public void testBasicFunctionExecution() {
    Host host = Host.getHost(0);
    VM datastore = host.getVM(0);
    VM client = host.getVM(1);
    doBasicFunctionExecution(client, null, datastore);
  }

  @Test
  public void testRemotedFunctionExecution() {
    Host host = Host.getHost(0);
    VM datastore = host.getVM(0);
    VM client = host.getVM(1);
    VM accessor = host.getVM(2);
    doBasicFunctionExecution(client, accessor, datastore);

  }

  private void doBasicFunctionExecution(VM client, VM accessor, VM datastore) {
    int datastorePort = createRegionsAndStartServer(datastore, false);
    int accessorPort = accessor == null ? 0 : createRegionsAndStartServer(accessor, true);
    final int port = accessorPort == 0 ? datastorePort : accessorPort;

    createClientRegion(client, port, true);

    class BasicTransactionalFunction extends FunctionAdapter {
      static final String ID = "BasicTransactionalFunction";

      @Override
      public void execute(FunctionContext context) {
        getGemfireCache().getLogger().info("SWAP:in function");
        RegionFunctionContext ctx = (RegionFunctionContext) context;
        Region pr = ctx.getDataSet();
        pr.put(new CustId(0), new Customer("name0", "address0"));
        pr.replace(new CustId(1), new Customer("name1", "address1"));
        pr.put(new CustId(10), new Customer("name10", "address10"));
        pr.put(new CustId(11), new Customer("name11", "address11"));
        Region r = ctx.getDataSet().getRegionService().getRegion(D_REFERENCE);
        r.put(new CustId(10), new Customer("name10", "address10"));
        r.put(new CustId(11), new Customer("name11", "address11"));
        ctx.getResultSender().lastResult(Boolean.TRUE);
      }

      @Override
      public String getId() {
        return ID;
      }
    }

    SerializableCallable registerFunction = new SerializableCallable() {
      public Object call() throws Exception {
        FunctionService.registerFunction(new BasicTransactionalFunction());
        return null;
      }
    };
    datastore.invoke(registerFunction);

    if (accessor != null) {
      accessor.invoke(registerFunction);
    }

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        FunctionService.registerFunction(new BasicTransactionalFunction());
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        Region<CustId, Customer> r = getGemfireCache().getRegion(D_REFERENCE);
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        pr.put(new CustId(0), new Customer("oldname0", "oldaddress0"));
        pr.put(new CustId(1), new Customer("oldname1", "oldaddress1"));
        mgr.begin();
        final Set filter = new HashSet();
        filter.add(new CustId(0));
        filter.add(new CustId(1));
        getGemfireCache().getLogger().info("SWAP:calling execute");
        FunctionService.onRegion(pr).withFilter(filter).execute(BasicTransactionalFunction.ID)
            .getResult();
        assertEquals(new Customer("name0", "address0"), pr.get(new CustId(0)));
        assertEquals(new Customer("name10", "address10"), pr.get(new CustId(10)));
        assertEquals(new Customer("name10", "address10"), r.get(new CustId(10)));
        TXStateProxy tx = mgr.internalSuspend();
        assertEquals(new Customer("oldname0", "oldaddress0"), pr.get(new CustId(0)));
        assertEquals(new Customer("oldname1", "oldaddress1"), pr.get(new CustId(1)));
        assertNull(pr.get(new CustId(10)));
        assertNull(r.get(new CustId(10)));
        mgr.resume(tx);
        mgr.commit();
        assertEquals(new Customer("name0", "address0"), pr.get(new CustId(0)));
        assertEquals(new Customer("name1", "address1"), pr.get(new CustId(1)));
        assertEquals(new Customer("name10", "address10"), pr.get(new CustId(10)));
        assertEquals(new Customer("name10", "address10"), r.get(new CustId(10)));
        return null;
      }
    });
  }

  @Test
  public void testEmptyTX() {
    Host host = Host.getHost(0);
    VM datastore = host.getVM(0);
    VM client = host.getVM(1);

    int port = createRegionsAndStartServer(datastore, false);
    createClientRegion(client, port, false);

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        getCache().getCacheTransactionManager().begin();
        pr.size();
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });
  }

  @Test
  public void testSuspendResumeOnDifferentThreads() {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    final int port1 = createRegionsAndStartServer(server1, false);
    final int port2 = createRegionsAndStartServer(server2, false);

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolServer("localhost"/* getServerHostName(Host.getHost(0)) */, port1);
        ccf.addPoolServer("localhost", port2);
        ccf.setPoolSubscriptionEnabled(false);
        ccf.setPoolLoadConditioningInterval(1);
        ccf.set(LOG_LEVEL, getDUnitLogLevel());
        ClientCache cCache = getClientCache(ccf);
        ClientRegionFactory<CustId, Customer> custrf =
            cCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
        ClientRegionFactory<Integer, String> refrf =
            cCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
        Region<Integer, String> r = refrf.create(D_REFERENCE);
        Region<CustId, Customer> pr = custrf.create(CUSTOMER);

        final TXManagerImpl mgr = getGemfireCache().getTxManager();
        CustId custId = new CustId(10);
        mgr.begin();
        pr.put(custId, new Customer("name10", "address10"));
        r.put(10, "value10");
        final TXStateProxy txState = mgr.internalSuspend();
        assertNull(pr.get(custId));
        assertNull(r.get(10));
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
          public void run() {
            mgr.resume(txState);
            mgr.commit();
            latch.countDown();
          }
        });
        t.start();
        latch.await();
        assertEquals(new Customer("name10", "address10"), pr.get(custId));
        assertEquals("value10", r.get(10));
        return null;
      }
    });
  }

  /////////////////////////////////////////////////////////////////////////
  // The following tests are inherited but since this class adds no new
  // behavior for them they are reimplemented here to not execute
  /////////////////////////////////////////////////////////////////////////



  @Override
  @Test
  public void testPRTXGet() {}

  @Override
  @Test
  public void testPRTXGetOnRemoteWithLoader() {}

  @Override
  @Test
  public void testPRTXGetEntryOnRemoteSide() {}

  @Override
  @Test
  public void testPRTXGetOnLocalWithLoader() {}

  @Override
  @Test
  public void testNonColocatedTX() {}

  @Override
  @Test
  public void testRemoteExceptionThrown() {}

  @Override
  @Test
  public void testSizeForTXHostedOnRemoteNode() {}

  @Override
  @Test
  public void testSizeOnAccessor() {}

  @Override
  @Test
  public void testKeysIterator() {}

  @Override
  @Test
  public void testValuesIterator() {}

  @Override
  @Test
  public void testEntriesIterator() {}

  @Override
  @Test
  public void testKeysIterator1() {}

  @Override
  @Test
  public void testValuesIterator1() {}

  @Override
  @Test
  public void testEntriesIterator1() {}

  @Override
  @Test
  public void testKeysIteratorOnDestroy() {}

  @Override
  @Test
  public void testValuesIteratorOnDestroy() {}

  @Override
  @Test
  public void testEntriesIteratorOnDestroy() {}

  @Override
  @Test
  public void testKeysIterator1OnDestroy() {}

  @Override
  @Test
  public void testValuesIterator1OnDestroy() {}

  @Override
  @Test
  public void testEntriesIterator1OnDestroy() {}

  @Override
  @Test
  public void testKeyIterationOnRR() {}

  @Override
  @Test
  public void testValuesIterationOnRR() {}

  @Override
  @Test
  public void testEntriesIterationOnRR() {}

  @Override
  @Test
  public void testIllegalIteration() {}

  @Override
  @Test
  public void testTxFunctionOnRegion() {}

  @Override
  @Test
  public void testTxFunctionOnMember() {}

  @Override
  @Test
  public void testNestedTxFunction() {}

  @Override
  @Test
  public void testDRFunctionExecution() {}

  @Override
  @Test
  public void testTxFunctionWithOtherOps() {}

  @Override
  @Test
  public void testRemoteJTACommit() {}

  @Override
  @Test
  public void testRemoteJTARollback() {}

  @Override
  @Test
  public void testOriginRemoteIsTrueForRemoteReplicatedRegions() {}

  @Override
  @Test
  public void testRemoteCreateInReplicatedRegion() {}

  @Override
  @Test
  public void testRemoteTxCleanupOnCrash() {}

  @Override
  @Test
  public void testNonColocatedPutAll() {}

  @Override
  @Test
  public void testDestroyCreateConflation() {}

  @Override
  @Test
  public void testTXWithRI() throws Exception {}

  @Override
  @Test
  public void testBug43176() {}

  @Override
  @Test
  public void testTXWithRICommitInDatastore() throws Exception {}

  @Override
  @Test
  public void testListenersNotInvokedOnSecondary() {}

  @Override
  @Test
  public void testBug33073() {}

  @Override
  @Test
  public void testBug43081() throws Exception {}

  @Override
  @Test
  public void testBug45556() {}

  @Test
  public void testBug42942() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);
    final int port = createRegionsAndStartServer(accessor, true);
    createRegionOnServer(datastore);
    createClientRegion(client, port, false);
    final CustId key = new CustId(1);

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> pr = getCache().getRegion(CUSTOMER);
        assertNull(pr.get(key));
        getCache().getCacheTransactionManager().begin();
        pr.putIfAbsent(key, new Customer("name1", "address"));
        assertNotNull(pr.get(key));
        return null;
      }
    });

    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getCache().close();
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        try {
          getCache().getCacheTransactionManager().commit();
          fail("expected exception not thrown");
        } catch (TransactionInDoubtException e) {
        }
        Region<CustId, Customer> pr = getCache().getRegion(CUSTOMER);
        assertFalse(pr.containsKey(key));
        return null;
      }
    });
  }

  @Test
  public void testOnlyGet() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    VM client = host.getVM(2);
    final int port = createRegionsAndStartServer(accessor, true);
    createRegionOnServer(datastore, false, false);
    createClientRegion(client, port, false);

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> pr = getCache().getRegion(CUSTOMER);
        pr.put(new CustId(1), new Customer("name1", "address"));
        getCache().getCacheTransactionManager().begin();
        pr.get(new CustId(1));
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });
  }

  @Test
  public void testBug43237() {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    int port = createRegionsAndStartServer(server, false);
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> pr = getCache().getRegion(CUSTOMER);
        Region<String, String> r = getCache().getRegion(D_REFERENCE);
        pr.getAttributesMutator().addCacheListener(new ServerListener());
        r.getAttributesMutator().addCacheListener(new ServerListener());
        return null;
      }
    });
    createClientRegion(client, port, false);
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> pr = getCache().getRegion(CUSTOMER);
        Region<String, String> r = getCache().getRegion(D_REFERENCE);
        pr.getAttributesMutator().addCacheListener(new ClientListener());
        r.getAttributesMutator().addCacheListener(new ClientListener());
        return null;
      }
    });
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> pr = getCache().getRegion(CUSTOMER);
        Region<String, String> r = getCache().getRegion(D_REFERENCE);
        pr.put(new CustId(1), new Customer("name1", "address1"));
        r.put("key1", "value1");
        ClientListener prl = (ClientListener) pr.getAttributes().getCacheListeners()[0];
        assertEquals(1, pr.getAttributes().getCacheListeners().length);
        ClientListener rl = (ClientListener) r.getAttributes().getCacheListeners()[0];
        assertEquals(1, r.getAttributes().getCacheListeners().length);
        assertFalse(prl.equals(rl));
        prl.reset();
        rl.reset();

        getCache().getCacheTransactionManager().begin();
        pr.put(new CustId(1), new Customer("newname1", "newaddress1"));
        r.put("key1", "newvalue1");
        pr.put(new CustId(2), new Customer("name2", "address2"));
        r.put("key2", "value2");
        getCache().getLogger().info("SWAP:issuingCommit");
        getCache().getCacheTransactionManager().commit();

        assertEquals(1, prl.creates);
        assertEquals(1, prl.updates);
        assertEquals(1, rl.creates);
        assertEquals(1, rl.updates);
        return null;
      }
    });
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> pr = getCache().getRegion(CUSTOMER);
        Region<String, String> r = getCache().getRegion(D_REFERENCE);
        ServerListener prl = (ServerListener) pr.getAttributes().getCacheListeners()[0];
        assertEquals(1, pr.getAttributes().getCacheListeners().length);
        ServerListener rl = (ServerListener) r.getAttributes().getCacheListeners()[0];
        assertEquals(1, r.getAttributes().getCacheListeners().length);
        assertEquals(2, prl.creates);
        assertEquals(1, prl.updates);
        assertEquals(2, rl.creates);
        assertEquals(1, rl.updates);
        return null;
      }
    });
  }

  /**
   * start 3 servers, accessor has r1 and r2; ds1 has r1, ds2 has r2 stop server after distributing
   * commit but b4 replying to client
   */
  @Test
  public void testFailoverAfterCommitDistribution() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);
    VM client = host.getVM(3);

    final int port1 = createRegionsAndStartServer(accessor, true);
    final int port2 = (Integer) datastore1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        return AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      }
    });

    class CreateReplicateRegion extends SerializableCallable {
      String regionName;

      public CreateReplicateRegion(String replicateRegionName) {
        this.regionName = replicateRegionName;
      }

      public Object call() throws Exception {
        RegionFactory rf = getCache().createRegionFactory(RegionShortcut.REPLICATE);
        rf.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
        rf.create(regionName);
        return null;
      }
    }

    accessor.invoke(new CreateReplicateRegion("r1"));
    accessor.invoke(new CreateReplicateRegion("r2"));


    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        disconnectFromDS();
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
            "true");
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolServer("localhost"/* getServerHostName(Host.getHost(0)) */, port1);
        ccf.addPoolServer("localhost", port2);
        ccf.setPoolMinConnections(5);
        ccf.setPoolLoadConditioningInterval(-1);
        ccf.setPoolSubscriptionEnabled(false);
        ccf.set(LOG_LEVEL, getDUnitLogLevel());
        ClientCache cCache = getClientCache(ccf);
        Region r1 =
            cCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("r1");
        Region r2 =
            cCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("r2");
        return null;
      }
    });

    datastore1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        CacheServer s = getCache().addCacheServer();
        getCache().getLogger().info("SWAP:ds1");
        s.setPort(port2);
        s.start();
        return null;
      }
    });
    datastore1.invoke(new CreateReplicateRegion("r1"));
    datastore2.invoke(new CreateReplicateRegion("r2"));

    final TransactionId txId = (TransactionId) client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        ClientCache cCache = (ClientCache) getCache();
        Region r1 = cCache.getRegion("r1");
        Region r2 = cCache.getRegion("r2");
        cCache.getCacheTransactionManager().begin();
        cCache.getLogger().info("SWAP:beganTX");
        r1.put("key1", "value1");
        r2.put("key2", "value2");
        return cCache.getCacheTransactionManager().getTransactionId();
      }
    });

    accessor.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getCache().getLogger().info("SWAP:accessor");
        final TXManagerImpl mgr = (TXManagerImpl) getCache().getCacheTransactionManager();
        assertTrue(mgr.isHostedTxInProgress((TXId) txId));
        TXStateProxyImpl txProxy = (TXStateProxyImpl) mgr.getHostedTXState((TXId) txId);
        final TXState txState = (TXState) txProxy.getRealDeal(null, null);
        txState.setAfterSend(new Runnable() {
          public void run() {
            getCache().getLogger().info("SWAP:closing cache");
            System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "no-flush-on-close", "true");
            try {
              mgr.removeHostedTXState((TXId) txState.getTransactionId());
              getCache().close();
            } finally {
              System.getProperties()
                  .remove(DistributionConfig.GEMFIRE_PREFIX + "no-flush-on-close");
            }
          }
        });
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getCache().getLogger().info("SWAP:commiting transaction");
        getCache().getCacheTransactionManager().commit();
        Region r1 = getCache().getRegion("r1");
        Region r2 = getCache().getRegion("r2");
        assertTrue(r1.containsKey("key1"));
        assertTrue(r2.containsKey("key2"));
        return null;
      }
    });
  }

  /**
   * start a tx in a thread, obtain local locks and wait. start another tx and commit, make sure 2nd
   * thread gets CCE
   */
  @Test
  public void testClientTxLocks() {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    int port = createRegionsAndStartServer(server, false);
    createClientRegion(client, port, false);
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final Region r = getCache().getRegion(CUSTOMER);
        final CountDownLatch outer = new CountDownLatch(1);
        final CountDownLatch inner = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
          public void run() {
            TXManagerImpl mgr = (TXManagerImpl) getCache().getCacheTransactionManager();
            mgr.begin();
            r.put(new CustId(1), new Customer("name1", "address1"));
            Map<CustId, Customer> m = new HashMap<CustId, Customer>();
            m.put(new CustId(2), new Customer("name2", "address2"));
            r.putAll(m);
            TXStateProxyImpl tx = (TXStateProxyImpl) mgr.internalSuspend();
            ClientTXStateStub txStub = (ClientTXStateStub) tx.getRealDeal(null, null);
            txStub.setAfterLocalLocks(new Runnable() {
              public void run() {
                try {
                  inner.countDown();
                  outer.await();
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }
            });
            mgr.resume(tx);
            mgr.commit();
          }
        });
        t.start();
        inner.await();
        getCache().getCacheTransactionManager().begin();
        r.put(new CustId(1), new Customer("name2", "address2"));
        try {
          getCache().getLogger().info("SWAP:Commit expect CCE");
          getCache().getCacheTransactionManager().commit();
          fail("expected CCE not thrown");
        } catch (CommitConflictException cce) {
          getCache().getLogger().info("SWAP:Commit Caught CCE");
        }
        outer.countDown();
        t.join();
        assertTrue(r.containsKey(new CustId(1)));
        assertEquals(new Customer("name1", "address1"), r.get(new CustId(1)));
        return null;
      }
    });
  }

  class TXFunction extends FunctionAdapter {
    @Override
    public void execute(FunctionContext context) {
      List l = (List) context.getArguments();
      CustId cusId = (CustId) l.get(0);
      Customer cust = (Customer) l.get(1);
      TransactionId txId = (TransactionId) l.get(2);
      assertNotNull(cusId);
      assertNotNull(cust);
      RegionFunctionContext rfc = (RegionFunctionContext) context;
      CacheTransactionManager mgr = getCache().getCacheTransactionManager();
      if (txId != null) {
        assertTrue(mgr.isSuspended(txId));
        mgr.resume(txId);
      } else {
        mgr.begin();
      }
      rfc.getDataSet().put(cusId, cust);
      txId = mgr.suspend();
      context.getResultSender().lastResult(txId);
    }

    @Override
    public String getId() {
      return "TXFunction";
    }
  }

  @Test
  public void testBasicResumableTX() {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    final int port = createRegionsAndStartServer(server, false);
    createClientRegion(client, port, false);



    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region cust = getCache().getRegion(CUSTOMER);
        assertNull(cust.get(new CustId(0)));
        assertNull(cust.get(new CustId(1)));
        ArrayList args = new ArrayList();
        args.add(new CustId(0));
        args.add(new Customer("name0", "address0"));
        args.add(null);
        List result = (List) FunctionService.onRegion(cust).withArgs(args).execute(new TXFunction())
            .getResult();
        TransactionId txId = (TransactionId) result.get(0);
        assertNotNull(txId);
        args = new ArrayList();
        args.add(new CustId(1));
        args.add(new Customer("name1", "address1"));
        args.add(txId);
        result = (List) FunctionService.onRegion(cust).withArgs(args).execute(new TXFunction())
            .getResult();
        TransactionId txId2 = (TransactionId) result.get(0);
        assertEquals(txId, txId2);
        result = (List) FunctionService.onServer(getCache()).withArgs(txId)
            .execute(new CommitFunction()).getResult();
        Boolean b = (Boolean) result.get(0);
        assertEquals(Boolean.TRUE, b);
        assertEquals(new Customer("name0", "address0"), cust.get(new CustId(0)));
        assertEquals(new Customer("name1", "address1"), cust.get(new CustId(1)));
        return null;
      }
    });
  }

  /**
   * client connects to server1 which is an accessor. It then does transactional ops in functions,
   * commit is done using internal ClientCommitFunction.
   */
  @Test
  public void testClientCommitFunction() {
    doFunctionWork(true);
  }

  @Test
  public void testClientRollbackFunction() {
    doFunctionWork(false);
  }

  private void doFunctionWork(final boolean commit) {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);

    final int port2 = createRegionsAndStartServer(server2, false);
    final int port = createRegionsAndStartServer(server1, true);

    IgnoredException.addIgnoredException("ClassCastException");
    SerializableRunnable suspectStrings = new SerializableRunnable("suspect string") {
      public void run() {
        InternalDistributedSystem.getLoggerI18n().convertToLogWriter()
            .info("<ExpectedException action=add>" + "ClassCastException" + "</ExpectedException>"
                + "<ExpectedException action=add>" + "TransactionDataNodeHasDeparted"
                + "</ExpectedException>");
      }
    };
    server1.invoke(suspectStrings);
    server2.invoke(suspectStrings);

    try {
      client.invoke(new SerializableCallable() {
        public Object call() throws Exception {
          System.setProperty(
              DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");
          ClientCacheFactory ccf = new ClientCacheFactory();
          ccf.addPoolServer("localhost"/* getServerHostName(Host.getHost(0)) */, port);
          setCCF(port2, ccf);
          // these settings were used to manually check that tx operation stats were being updated
          // ccf.set(STATISTIC_SAMPLING_ENABLED, "true");
          // ccf.set(STATISTIC_ARCHIVE_FILE, "clientStats.gfs");
          ClientCache cCache = getClientCache(ccf);
          ClientRegionFactory<Integer, String> crf =
              cCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
          Region<Integer, String> customer = crf.create(CUSTOMER);
          cCache.getLogger()
              .info("<ExpectedException action=add>" + "ClassCastException" + "</ExpectedException>"
                  + "<ExpectedException action=add>" + "TransactionDataNodeHasDeparted"
                  + "</ExpectedException>");

          Region cust = getCache().getRegion(CUSTOMER);
          org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
              .fine("SWAP:doing first get from client");
          assertNull(cust.get(new CustId(0)));
          assertNull(cust.get(new CustId(1)));
          ArrayList args = new ArrayList();
          args.add(new CustId(0));
          args.add(new Customer("name0", "address0"));
          args.add(null);
          List result = (List) FunctionService.onRegion(cust).withArgs(args)
              .execute(new TXFunction()).getResult();
          TransactionId txId = (TransactionId) result.get(0);
          assertNotNull(txId);
          args = new ArrayList();
          args.add(new CustId(1));
          args.add(new Customer("name1", "address1"));
          args.add(txId);
          result = (List) FunctionService.onRegion(cust).withArgs(args).execute(new TXFunction())
              .getResult();
          TransactionId txId2 = (TransactionId) result.get(0);
          assertEquals(txId, txId2);
          // invoke ClientCommitFunction
          try {
            if (commit) {
              FunctionService.onServer(getCache()).withArgs(new CustId(0))
                  .execute(new CommitFunction()).getResult();
            } else {
              FunctionService.onServer(getCache()).withArgs(new CustId(0))
                  .execute(new RollbackFunction()).getResult();
            }
            fail("expected exception not thrown");
          } catch (FunctionException e) {
            assertTrue(e.getCause() instanceof ClassCastException);
          }
          List list = null;
          if (commit) {
            list = (List) FunctionService.onServer(getCache()).withArgs(txId)
                .execute(new CommitFunction()).getResult();
          } else {
            list = (List) FunctionService.onServer(getCache()).withArgs(txId)
                .execute(new RollbackFunction()).getResult();
          }
          assertEquals(Boolean.TRUE, list.get(0));
          if (commit) {
            assertEquals(new Customer("name0", "address0"), cust.get(new CustId(0)));
            assertEquals(new Customer("name1", "address1"), cust.get(new CustId(1)));
          } else {
            assertNull(cust.get(new CustId(0)));
            assertNull(cust.get(new CustId(1)));
          }
          return null;
        }
      });
    } finally {
      suspectStrings = new SerializableRunnable("suspect string") {
        public void run() {
          InternalDistributedSystem.getLoggerI18n().convertToLogWriter()
              .info("<ExpectedException action=remove>" + "ClassCastException"
                  + "</ExpectedException>" + "<ExpectedException action=remove>"
                  + "TransactionDataNodeHasDeparted" + "</ExpectedException>");
        }
      };
      server1.invoke(suspectStrings);
      server2.invoke(suspectStrings);
      client.invoke(suspectStrings);
    }
  }

  @Test
  public void testClientCommitFunctionWithFailure() {
    doFunctionWithFailureWork(true);
  }

  @Test
  public void testRollbackFunctionWithFailure() {
    doFunctionWithFailureWork(false);
  }

  private void doFunctionWithFailureWork(final boolean commit) {
    IgnoredException.addIgnoredException("TransactionDataNodeHasDepartedException");
    IgnoredException.addIgnoredException("ClassCastException");
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);

    createRegionOnServer(server2);
    final int port = createRegionsAndStartServer(server1, true);

    createClientRegion(client, port, true);

    final TransactionId txId = (TransactionId) client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region cust = getCache().getRegion(CUSTOMER);
        assertNull(cust.get(new CustId(0)));
        assertNull(cust.get(new CustId(1)));
        ArrayList args = new ArrayList();
        args.add(new CustId(0));
        args.add(new Customer("name0", "address0"));
        args.add(null);
        List result = (List) FunctionService.onRegion(cust).withArgs(args).execute(new TXFunction())
            .getResult();
        TransactionId txId = (TransactionId) result.get(0);
        assertNotNull(txId);
        args = new ArrayList();
        args.add(new CustId(1));
        args.add(new Customer("name1", "address1"));
        args.add(txId);
        result = (List) FunctionService.onRegion(cust).withArgs(args).execute(new TXFunction())
            .getResult();
        TransactionId txId2 = (TransactionId) result.get(0);
        assertEquals(txId, txId2);
        // invoke ClientCommitFunction
        try {
          FunctionService.onServer(getCache()).withArgs(new CustId(0)).execute(new CommitFunction())
              .getResult();
          fail("expected exception not thrown");
        } catch (FunctionException e) {
          assertTrue(e.getCause() instanceof ClassCastException);
        }
        return txId;
      }
    });

    server2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        disconnectFromDS();
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region cust = getCache().getRegion(CUSTOMER);
        try {
          List list = null;
          if (commit) {
            list = (List) FunctionService.onServer(getCache()).withArgs(txId)
                .execute(new CommitFunction()).getResult();
          } else {
            list = (List) FunctionService.onServer(getCache()).withArgs(txId)
                .execute(new RollbackFunction()).getResult();
          }
          fail("expected exception not thrown");
        } catch (FunctionException e) {
          assertTrue(e.getCause() instanceof TransactionDataNodeHasDepartedException);
        }
        return null;
      }
    });
  }

  /**
   * start an accessor and two peers, then commit transaction from accessor
   */
  @Test
  public void testCommitFunctionFromPeer() {
    doTestFunctionFromPeer(true);
  }

  @Test
  public void testRollbackFunctionFromPeer() {
    doTestFunctionFromPeer(false);
  }

  private void doTestFunctionFromPeer(final boolean commit) {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM peer1 = host.getVM(1);
    VM peer2 = host.getVM(2);

    createRegionOnServer(peer1);
    createRegionOnServer(peer2);
    createRegionOnServer(accessor, false, true);

    final TransactionId txId = (TransactionId) peer1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        PartitionedRegion r = (PartitionedRegion) getCache().getRegion(CUSTOMER);
        CustId cust = null;
        DistributedMember myId = getCache().getDistributedSystem().getDistributedMember();
        List<CustId> keys = new ArrayList<CustId>();
        for (int i = 0; i < 10; i++) {
          cust = new CustId(i);
          int bucketId = PartitionedRegionHelper.getHashKey(r, cust);
          if (!myId.equals(r.getBucketPrimary(bucketId))) {
            keys.add(cust);
          }
        }
        assertTrue(keys.size() > 2);
        CacheTransactionManager mgr = getCache().getCacheTransactionManager();
        mgr.begin();
        for (CustId custId : keys) {
          r.put(cust, new Customer("newname", "newaddress"));
        }
        return mgr.suspend();
      }
    });

    assertNotNull(txId);

    accessor.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Execution exe = FunctionService.onMember(((TXId) txId).getMemberId()).withArgs(txId);
        List list = null;
        if (commit) {
          list = (List) exe.execute(new CommitFunction()).getResult();
        } else {
          list = (List) exe.execute(new RollbackFunction()).getResult();
        }
        assertEquals(1, list.size());
        assertTrue((Boolean) list.get(0));
        return null;
      }
    });
  }

  @Test
  public void testBug43752() {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    createRegionOnServer(server1);
    createRegionOnServer(server2);

    server1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getCache().getRegion(CUSTOMER);
        CacheTransactionManager mgr = getCache().getCacheTransactionManager();
        mgr.begin();
        try {
          for (int i = 0; i < 5; i++) {
            getCache().getLogger().info("SWAP:put:custId:" + i);
            r.put(new CustId(i), new Customer("name" + i, "address" + i));
          }
          fail("expected exception not thrown");
        } catch (TransactionDataNotColocatedException e) {
          // expected
        }
        mgr.commit();
        return null;
      }
    });
  }

  @Test
  public void testSuspendTimeout() throws Exception {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    final int port = createRegionsAndStartServer(server, false);
    createClientRegion(client, port, true);

    final TransactionId txId = (TransactionId) client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        TXManagerImpl mgr = (TXManagerImpl) getCache().getCacheTransactionManager();
        mgr.setSuspendedTransactionTimeout(1);
        Region r = getCache().getRegion(CUSTOMER);
        assertNull(r.get(new CustId(101)));
        mgr.begin();
        final TXStateProxy txState = mgr.getTXState();
        assertTrue(txState.isInProgress());
        r.put(new CustId(101), new Customer("name101", "address101"));
        TransactionId txId = mgr.suspend(TimeUnit.MILLISECONDS);
        WaitCriterion waitForTxTimeout = new WaitCriterion() {
          public boolean done() {
            return !txState.isInProgress();
          }

          public String description() {
            return "txState stayed in progress indicating that the suspend did not timeout";
          }
        };
        // tx should timeout after 1 ms but to deal with loaded machines and thread
        // scheduling latency wait for 10 seconds before reporting an error.
        Wait.waitForCriterion(waitForTxTimeout, 10 * 1000, 10, true);
        try {
          mgr.resume(txId);
          fail("expected exception not thrown");
        } catch (IllegalStateException expected) {
        }
        assertNull(r.get(new CustId(101)));
        return txId;
      }
    });
    server.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        TXManagerImpl mgr = (TXManagerImpl) getCache().getCacheTransactionManager();
        assertNull(mgr.getHostedTXState((TXId) txId));
        assertEquals(0, mgr.hostedTransactionsInProgressForTest());
        return null;
      }
    });
  }

  /**
   * test that re-tried operations from client do not result in multiple ops in tx
   */
  @Test
  public void testEventTracker() {
    Host host = Host.getHost(0);
    VM delegate = host.getVM(0);
    VM server = host.getVM(1);
    VM client = host.getVM(2);

    final int port1 = createRegionsAndStartServer(delegate, true);
    final int port2 = createRegionsAndStartServer(server, false);

    final TXId txid = (TXId) client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
            "true");
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolServer("localhost"/* getServerHostName(Host.getHost(0)) */, port1);
        if (port2 != 0)
          ccf.addPoolServer("localhost", port2);
        ccf.setPoolSubscriptionEnabled(false);
        ccf.set(LOG_LEVEL, getDUnitLogLevel());
        ClientCache cCache = getClientCache(ccf);
        ClientRegionFactory<CustId, Customer> custrf =
            cCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
        ClientRegionFactory<Integer, String> refrf =
            cCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
        custrf.addCacheListener(new ClientListener());
        Region<Integer, String> r = refrf.create(D_REFERENCE);
        Region<CustId, Customer> pr = custrf.create(CUSTOMER);
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        mgr.begin();
        CustId custId = new CustId(0);
        Customer cust = new Customer("name" + 0, "address" + 0);
        pr.put(custId, cust);
        r.put(0, "value" + 0);
        return mgr.getTransactionId();
      }
    });

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        EntryEventImpl event = null;
        Region<Integer, String> r = getGemfireCache().getRegion(D_REFERENCE);
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        LocalRegion lr = (LocalRegion) pr;
        CustId custId = new CustId(1);
        Customer cust = new Customer("name" + 1, "address" + 1);
        event = lr.newUpdateEntryEvent(custId, cust, null);
        assertNotNull(event);
        event.copyOffHeapToHeap();
        lr.validatedPut(event, System.currentTimeMillis());
        lr.validatedPut(event, System.currentTimeMillis());
        lr.validatedPut(event, System.currentTimeMillis());
        return null;
      }
    });

    server.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        TXStateProxyImpl txProxy = (TXStateProxyImpl) mgr.getHostedTXState(txid);
        assert txProxy.isRealDealLocal();
        TXState tx = (TXState) txProxy.getRealDeal(null, null);
        assert tx != null;
        // 2 for put, 1 for validatedPut
        assertEquals(3, tx.seenEvents.size());
        return null;
      }
    });

    delegate.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        for (CacheServer s : getCache().getCacheServers()) {
          getCache().getLogger().info("SWAP:Stopping " + s);
          s.stop();
        }
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        if (mgr.getTXState() == null) {
          // oops - different RMI thread this time. Spit out a suspect string
          // so that we can remove this log statement & know that the fix of
          // setting the TXState works
          getLogWriter().error("no tx state found for this thread");
          mgr.setTXState(mgr.getHostedTXState(txid));
        }
        mgr.commit();
        Region<CustId, Customer> pr = getGemfireCache().getRegion(CUSTOMER);
        CacheListener<CustId, Customer>[] clarray = pr.getAttributes().getCacheListeners();
        assert clarray.length == 1;
        ClientListener cl = (ClientListener) clarray[0];
        // 1 for put 1 for validatedPut
        assertEquals(2, cl.putCount);
        return null;
      }
    });
  }

  public void verifyVersionTags(VM client, VM server1, VM server2, VM server3) {}

  /**
   * start two servers and a client. make the server throw TransactionException. verify that the
   * exception does not cause the client to failover to the second server see bug 51666
   */
  @Test
  public void testTransactionException() {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);

    final int port1 = createRegionsAndStartServer(server1, true);
    final int port2 = createRegionsAndStartServer(server2, true);

    final Integer troubleKey = Integer.valueOf(1234);
    // add cacheListener to throw exception on server1
    class ExceptionWriter extends CacheWriterAdapter<Integer, String> {
      @Override
      public void beforeCreate(EntryEvent<Integer, String> event) throws CacheWriterException {
        throwException(event);
      }

      @Override
      public void beforeUpdate(EntryEvent<Integer, String> event) throws CacheWriterException {
        throwException(event);
      }

      private void throwException(EntryEvent<Integer, String> event) {
        if (event.getKey().equals(troubleKey)) {
          getCache().getLogger().info("SWAP:In cache writer throwing exception");
          throw new TransactionException("SWAP:TEST");
        }
      }
    }
    server1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region ref = getCache().getRegion(D_REFERENCE);
        getCache().getLogger().info("SWAP:ADDWRITER:server1");
        ref.getAttributesMutator().setCacheWriter(new ExceptionWriter());
        return null;
      }
    });

    /* final TXId txid = (TXId) */client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
            "true");
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolServer("localhost"/* getServerHostName(Host.getHost(0)) */, port1);
        ccf.addPoolServer("localhost", port2);
        ccf.setPoolMinConnections(0);
        ccf.setPoolSubscriptionEnabled(false);
        ccf.set(LOG_LEVEL, getDUnitLogLevel());
        ClientCache cCache = getClientCache(ccf);
        ClientRegionFactory<CustId, Customer> custrf =
            cCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
        ClientRegionFactory<Integer, String> refrf =
            cCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
        Region<Integer, String> r = refrf.create(D_REFERENCE);
        // Region<Integer, String> order = refrf.create(ORDER);

        TXManagerImpl mgr = getGemfireCache().getTxManager();
        mgr.begin();
        for (int i = 0; i < 5; i++) {
          getGemfireCache().getLogger().info("SWAP:putting:" + i);
          r.put(i, "value" + i);
        }
        try {
          getGemfireCache().getLogger().info("SWAP:putting:" + troubleKey);
          r.put(troubleKey, "valueException");
          fail("expected TransactionException exception not thrown");
        } catch (TransactionException e) {
          // expected
        }
        return mgr.getTransactionId();
      }
    });

    // make sure tx has not failed over to server2
    server2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        GemFireCacheImpl cache = getGemfireCache();
        assertEquals(0, cache.getTxManager().hostedTransactionsInProgressForTest());
        return null;
      }
    });

  }

  /**
   * In a server callback, enroll one more region within a transaction (the client does not have
   * this region) commit the transaction to make sure that the client ignores this region. see bug
   * 51922
   */
  @Test
  public void testNotAllRegionsHaveClient() {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(2);
    final String regionName = getName();

    final int port1 = createRegionsAndStartServer(server, true);

    // add cacheListener to throw exception on server1
    class SecurityWriter extends CacheWriterAdapter<Integer, String> {
      @Override
      public void beforeCreate(EntryEvent<Integer, String> event) throws CacheWriterException {
        enrollRegion(event);
      }

      @Override
      public void beforeUpdate(EntryEvent<Integer, String> event) throws CacheWriterException {
        enrollRegion(event);
      }

      private void enrollRegion(EntryEvent<Integer, String> event) {
        Region r = getCache().getRegion(regionName);
        r.put("key", "value");
      }
    }

    server.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region ref = getCache().getRegion(D_REFERENCE);
        Region r = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
        ref.getAttributesMutator().setCacheWriter(new SecurityWriter());
        return null;
      }
    });

    /* final TXId txid = (TXId) */client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
            "true");
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolServer("localhost"/* getServerHostName(Host.getHost(0)) */, port1);
        ccf.setPoolMinConnections(0);
        ccf.setPoolSubscriptionEnabled(false);
        ccf.set(LOG_LEVEL, getDUnitLogLevel());
        ClientCache cCache = getClientCache(ccf);
        ClientRegionFactory<CustId, Customer> custrf =
            cCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
        ClientRegionFactory<Integer, String> refrf =
            cCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
        Region<Integer, String> r = refrf.create(D_REFERENCE);
        // Region<Integer, String> order = refrf.create(ORDER);

        TXManagerImpl mgr = getGemfireCache().getTxManager();
        mgr.begin();
        for (int i = 0; i < 5; i++) {
          getGemfireCache().getLogger().info("SWAP:putting:" + i);
          r.put(i, "value" + i);
        }
        mgr.commit();
        return null;
      }
    });

  }

  @Test
  public void testAdjunctMessage() {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    final String regionName = "testAdjunctMessage";

    final int port1 = createRegionsAndStartServer(server1, false);
    final int port2 = createRegionsAndStartServer(server2, false);

    SerializableCallable createServerRegionWithInterest = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        RegionFactory rf = getCache().createRegionFactory(RegionShortcut.PARTITION);
        rf.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT));
        rf.create(regionName);
        return null;
      }
    };
    server1.invoke(createServerRegionWithInterest);
    server2.invoke(createServerRegionWithInterest);

    // get two colocated keys on server1
    final List<String> keys = (List<String>) server1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        PartitionedRegion pr = (PartitionedRegion) r;
        List<String> server1Keys = new ArrayList<String>();
        for (int i = 0; i < 100; i++) {
          String key = "k" + i;
          // pr.put(key, "v" + i);
          DistributedMember owner = pr.getOwnerForKey(pr.getKeyInfo(key));
          if (owner.equals(pr.getMyId())) {
            server1Keys.add(key);
            if (server1Keys.size() == 2) {
              break;
            }
          }
        }
        return server1Keys;
      }
    });

    class ClientListener extends CacheListenerAdapter {
      Set keys = new HashSet();

      @Override
      public void afterCreate(EntryEvent event) {
        add(event);
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        add(event);
      }

      private void add(EntryEvent event) {
        keys.add(event.getKey());
      }
    }
    client2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
            "true");
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolServer("localhost"/* getServerHostName(Host.getHost(0)) */, port2);
        ccf.setPoolMinConnections(0);
        ccf.setPoolSubscriptionEnabled(true);
        ccf.setPoolSubscriptionRedundancy(0);
        ccf.set(LOG_LEVEL, getDUnitLogLevel());
        ClientCache cCache = getClientCache(ccf);
        Region r = cCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
            .addCacheListener(new ClientListener()).create(regionName);
        r.registerInterestRegex(".*");
        // cCache.readyForEvents();
        return null;
      }
    });
    client1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
            "true");
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolServer("localhost"/* getServerHostName(Host.getHost(0)) */, port1);
        ccf.setPoolMinConnections(0);
        ccf.setPoolSubscriptionEnabled(true);
        ccf.set(LOG_LEVEL, getDUnitLogLevel());
        ClientCache cCache = getClientCache(ccf);
        Region r =
            cCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName);
        getCache().getCacheTransactionManager().begin();
        for (String key : keys) {
          r.put(key, "value");
        }
        getCache().getCacheTransactionManager().commit();
        return null;
      }
    });
    client2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        CacheListener[] listeners = r.getAttributes().getCacheListeners();
        boolean foundListener = false;
        for (CacheListener listener : listeners) {
          if (listener instanceof ClientListener) {
            foundListener = true;
            final ClientListener clientListener = (ClientListener) listener;
            WaitCriterion wc = new WaitCriterion() {
              @Override
              public boolean done() {
                return clientListener.keys.containsAll(keys);
              }

              @Override
              public String description() {
                return "expected:" + keys + " found:" + clientListener.keys;
              }
            };
            Wait.waitForCriterion(wc, 30 * 1000, 500, true);
          }
        }
        assertTrue(foundListener);
        return null;
      }
    });
  }

  @Test
  public void testCreateSubscriptionEventsWithPrWhioleBucketRegionIsDestroyed() {
    testSubscriptionEventsWhenBucketRegionIsDestroyed(false, forop.CREATE);
  }

  @Test
  public void testDestroySubscriptionEventsWithPrWhileBucketRegionIsDestroyed() {
    testSubscriptionEventsWhenBucketRegionIsDestroyed(false, forop.DESTROY);
  }

  @Test
  public void testCreateSubscriptionEventsWithOffheapPrWhioleBucketRegionIsDestroyed() {
    testSubscriptionEventsWhenBucketRegionIsDestroyed(true, forop.CREATE);
  }

  @Test
  public void testDestroySubscriptionEventsWithOffheapPrWhileBucketRegionIsDestroyed() {
    testSubscriptionEventsWhenBucketRegionIsDestroyed(true, forop.DESTROY);
  }

  private void testSubscriptionEventsWhenBucketRegionIsDestroyed(boolean offheap, forop op) {
    int copies = 1;
    int totalBuckets = 1;

    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);

    final String regionName = "SubscriptionPr";

    server1.invoke(() -> {
      configureOffheapSystemProperty();
    });
    server2.invoke(() -> {
      configureOffheapSystemProperty();
    });

    final int port1 = createRegionsAndStartServer(server1, false);
    // Create PR
    server1.invoke(() -> {
      createSubscriptionRegion(offheap, regionName, copies, totalBuckets);
      Region r = getCache().getRegion(regionName);
      r.put("KEY-1", "VALUE-1");
      r.put("KEY-2", "VALUE-2");
    });

    final int port2 = createRegionsAndStartServer(server2, false);

    // Create PR
    server2.invoke(() -> {
      createSubscriptionRegion(offheap, regionName, copies, totalBuckets);
      Region r = getCache().getRegion(regionName);

      Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
        List<Integer> ids = ((PartitionedRegion) r).getLocalBucketsListTestOnly();
        assertFalse(ids.isEmpty());
      });
    });

    // Create client 1
    client1.invoke(() -> {
      createClient(port1, regionName);
    });

    // Create client 2
    client2.invoke(() -> {
      createClient(port2, regionName);
    });

    // Destroy secondary bucket region. This simulates bucket re-balance.
    server2.invoke(() -> {
      BucketRegion br =
          ((PartitionedRegion) getCache().getRegion(regionName)).getBucketRegion("KEY-1");
      AbstractRegionMap arm = (AbstractRegionMap) ((LocalRegion) br).entries;
      arm.setARMLockTestHook(new ARMLockTestHookAdapter() {
        @Override
        public void beforeLock(LocalRegion owner, CacheEvent event) {
          List<Integer> ids =
              ((PartitionedRegion) getCache().getRegion(regionName)).getLocalBucketsListTestOnly();
          assertFalse(ids.isEmpty());
          br.localDestroyRegion();
        }
      });
    });

    server1.invoke(() -> {
      Cache cache = getCache();
      Region r = cache.getRegion(regionName);
      CacheTransactionManager mgr = cache.getCacheTransactionManager();
      mgr.begin();
      if (op == forop.CREATE) {
        r.create("KEY-3", "VALUE-3");
      } else if (op == forop.UPDATE) {
        r.put("KEY-1", "VALUE-1_2");
      } else if (op == forop.DESTROY) {
        r.destroy("KEY-2");
      }
      mgr.commit();
    });

    client1.invoke(() -> {
      Awaitility.await().atMost(30, TimeUnit.SECONDS)
          .until(() -> assertEquals(1, getClientCacheListnerEventCount(regionName)));
    });

    client2.invoke(() -> {
      Awaitility.await().atMost(30, TimeUnit.SECONDS)
          .until(() -> assertEquals(1, getClientCacheListnerEventCount(regionName)));
    });
  }

  Object verifyTXStateExpired(final DistributedMember myId, final TXManagerImpl txmgr) {
    try {
      Wait.waitForCriterion(new WaitCriterion() {
        public boolean done() {
          Set states = txmgr.getTransactionsForClient((InternalDistributedMember) myId);
          org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
              .info("found " + states.size() + " tx states for " + myId);
          return states.isEmpty();
        }

        public String description() {
          return "Waiting for transaction state to expire";
        }
      }, 15000, 500, true);
      return null;
    } finally {
      getGemfireCache().getDistributedSystem().disconnect();
    }
  }

  Object verifyProxyServerChanged(final TXStateProxyImpl tx, final DistributedMember newProxy) {
    try {
      Awaitility.await().pollInterval(10, TimeUnit.MILLISECONDS)
          .pollDelay(10, TimeUnit.MILLISECONDS).atMost(30, TimeUnit.SECONDS)
          .until(() -> !((TXState) tx.realDeal).getProxyServer().equals(newProxy));
      return null;
    } finally {
      getGemfireCache().getDistributedSystem().disconnect();
    }
  }
}
