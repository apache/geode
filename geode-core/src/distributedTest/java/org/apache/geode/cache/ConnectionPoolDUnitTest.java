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
package org.apache.geode.cache;

import static org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier.getInstance;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.ALL;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.Endpoint;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.CertifiableTestCacheListener;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.cache30.TestCacheLoader;
import org.apache.geode.cache30.TestCacheWriter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.EntryExpiryTask;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PoolStats;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifierStats;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.logging.LocalLogWriter;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * This class tests the client connection pool in GemFire. It does so by creating a cache server
 * with a cache and a pre-defined region and a data loader. The client creates the same region with
 * a pool (this happens in the controller VM). the client then spins up 10 different threads and
 * issues gets on keys. The server data loader returns the data to the client.
 * <p>
 * Test uses Groboutils TestRunnable objects to achieve multi threading behavior in the test.
 */
@Category({ClientServerTest.class})
@FixMethodOrder(NAME_ASCENDING)
public class ConnectionPoolDUnitTest extends JUnit4CacheTestCase {

  /**
   * The port on which the cache server was started in this VM
   */
  private static int bridgeServerPort;

  protected static int port = 0;
  protected static int port2 = 0;

  private static int numberOfAfterInvalidates;
  private static int numberOfAfterCreates;
  private static int numberOfAfterUpdates;

  private static final int TYPE_CREATE = 0;
  private static final int TYPE_UPDATE = 1;
  private static final int TYPE_INVALIDATE = 2;
  private static final int TYPE_DESTROY = 3;
  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Before
  public void setup() {
    vm0 = VM.getVM(0);
    vm1 = VM.getVM(1);
    vm2 = VM.getVM(2);
    vm3 = VM.getVM(3);
  }

  @After
  public void tearDown() {

  }

  @Override
  public final void postSetUp() throws Exception {
    // avoid IllegalStateException from HandShake by connecting all vms to
    // system before creating pool
    getSystem();
    Invoke.invokeInEveryVM(new SerializableRunnable("getSystem") {
      @Override
      public void run() {
        getSystem();
      }
    });
    postSetUpConnectionPoolDUnitTest();
  }

  protected void postSetUpConnectionPoolDUnitTest() throws Exception {}

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(() -> {
      Map pools = PoolManager.getAll();
      if (!pools.isEmpty()) {
        logger.warn("found pools remaining after teardown: " + pools);
        assertEquals(0, pools.size());
      }
    });
    postTearDownConnectionPoolDUnitTest();

  }

  protected void postTearDownConnectionPoolDUnitTest() throws Exception {}

  /* GemStoneAddition */
  private static PoolImpl getPool(Region r) {
    PoolImpl result = null;
    String poolName = r.getAttributes().getPoolName();
    if (poolName != null) {
      result = (PoolImpl) PoolManager.find(poolName);
    }
    return result;
  }

  private static TestCacheWriter getTestWriter(Region r) {
    return (TestCacheWriter) r.getAttributes().getCacheWriter();
  }

  /**
   * Create a cache server on the given port without starting it.
   *
   * @since GemFire 5.0.2
   */
  protected void createBridgeServer(int port) {
    CacheServer bridge = getCache().addCacheServer();
    bridge.setPort(port);
    bridge.setMaxThreads(getMaxThreads());
    bridgeServerPort = bridge.getPort();
  }

  /**
   * Starts a cache server on the given port, using the given deserializeValues and
   * notifyBySubscription to serve up the given region.
   *
   * @since GemFire 4.0
   */
  private void startBridgeServer(int port) throws IOException {
    startBridgeServer(port, -1);
  }

  private void startBridgeServer(int port, int socketBufferSize) throws IOException {
    startBridgeServer(port, socketBufferSize, CacheServer.DEFAULT_LOAD_POLL_INTERVAL);
  }

  private void startBridgeServer(int port, int socketBufferSize, long loadPollInterval)
      throws IOException {

    Cache cache = getCache();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    if (socketBufferSize != -1) {
      bridge.setSocketBufferSize(socketBufferSize);
    }
    bridge.setMaxThreads(getMaxThreads());
    bridge.setLoadPollInterval(loadPollInterval);
    bridge.start();
    bridgeServerPort = bridge.getPort();
  }

  /**
   * By default return 0 which turns off selector and gives thread per cnx. Test subclasses can
   * override to run with selector.
   *
   * @since GemFire 5.1
   */
  private int getMaxThreads() {
    return 0;
  }

  /**
   * Stops the cache server that serves up the given cache.
   *
   * @since GemFire 4.0
   */
  private void stopBridgeServer(Cache cache) {
    CacheServer bridge = cache.getCacheServers().iterator().next();
    bridge.stop();
    assertFalse(bridge.isRunning());
  }

  void stopBridgeServers(Cache cache) {
    CacheServer bridge;
    for (CacheServer cacheServer : cache.getCacheServers()) {
      bridge = cacheServer;
      bridge.stop();
      assertFalse(bridge.isRunning());
    }
  }

  private void restartBridgeServers(Cache cache) throws IOException {
    CacheServer bridge;
    for (CacheServer cacheServer : cache.getCacheServers()) {
      bridge = cacheServer;
      bridge.start();
      assertTrue(bridge.isRunning());
    }
  }

  private void createLonerDS() {
    disconnectFromDS();
    InternalDistributedSystem ds = getLonerSystem();
    assertEquals(0, ds.getDistributionManager().getOtherDistributionManagerIds().size());
  }

  /**
   * Returns region attributes for a <code>LOCAL</code> region
   */
  protected <K, V> RegionAttributes<K, V> getRegionAttributes() {
    AttributesFactory<K, V> factory = new AttributesFactory<>();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(false); // test validation expects this behavior
    return factory.create();
  }

  private static String createBridgeClientConnection(String host, int[] ports) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < ports.length; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("name").append(i).append("=");
      sb.append(host).append(":").append(ports[i]);
    }
    return sb.toString();
  }

  private static class EventWrapper {
    final EntryEvent event;
    final Object key;
    final Object val;
    final Object arg;
    final int type;

    EventWrapper(EntryEvent ee, int type) {
      this.event = ee;
      this.key = ee.getKey();
      this.val = ee.getNewValue();
      this.arg = ee.getCallbackArgument();
      this.type = type;
    }

    public String toString() {
      return "EventWrapper: event=" + event + ", type=" + type;
    }
  }

  static class ControlListener extends CacheListenerAdapter {
    final LinkedList<EventWrapper> events = new LinkedList<>();
    final Object CONTROL_LOCK = new Object();

    void waitWhileNotEnoughEvents(long sleepMs, int eventCount) {
      long maxMillis = System.currentTimeMillis() + sleepMs;
      synchronized (this.CONTROL_LOCK) {
        try {
          while (this.events.size() < eventCount) {
            long waitMillis = maxMillis - System.currentTimeMillis();
            if (waitMillis < 10) {
              break;
            }
            this.CONTROL_LOCK.wait(waitMillis);
          }
        } catch (InterruptedException abort) {
          fail("interrupted");
        }
      }
    }

    @Override
    public void afterCreate(EntryEvent e) {

      synchronized (this.CONTROL_LOCK) {
        this.events.add(new EventWrapper(e, TYPE_CREATE));
        this.CONTROL_LOCK.notifyAll();
      }
    }

    @Override
    public void afterUpdate(EntryEvent e) {

      synchronized (this.CONTROL_LOCK) {
        this.events.add(new EventWrapper(e, TYPE_UPDATE));
        this.CONTROL_LOCK.notifyAll();
      }
    }

    @Override
    public void afterInvalidate(EntryEvent e) {

      synchronized (this.CONTROL_LOCK) {
        this.events.add(new EventWrapper(e, TYPE_INVALIDATE));
        this.CONTROL_LOCK.notifyAll();
      }
    }

    @Override
    public void afterDestroy(EntryEvent e) {

      synchronized (this.CONTROL_LOCK) {
        this.events.add(new EventWrapper(e, TYPE_DESTROY));
        this.CONTROL_LOCK.notifyAll();
      }
    }
  }

  /**
   * Create a fake EntryEvent that returns the provided region for {@link CacheEvent#getRegion()}
   * and returns {@link org.apache.geode.cache.Operation#LOCAL_LOAD_CREATE} for {@link
   * CacheEvent#getOperation()}
   *
   * @return fake entry event
   */
  protected static EntryEvent createFakeyEntryEvent(final Region r) {
    return new EntryEvent() {
      @Override
      public Operation getOperation() {
        return Operation.LOCAL_LOAD_CREATE; // fake out pool to exit early
      }

      @Override
      public Region getRegion() {
        return r;
      }

      @Override
      public Object getKey() {
        return null;
      }

      @Override
      public Object getOldValue() {
        return null;
      }

      @Override
      public boolean isOldValueAvailable() {
        return true;
      }

      @Override
      public Object getNewValue() {
        return null;
      }

      public boolean isLocalLoad() {
        return false;
      }

      public boolean isNetLoad() {
        return false;
      }

      public boolean isLoad() {
        return true;
      }

      public boolean isNetSearch() {
        return false;
      }

      @Override
      public TransactionId getTransactionId() {
        return null;
      }

      @Override
      public Object getCallbackArgument() {
        return null;
      }

      @Override
      public boolean isCallbackArgumentAvailable() {
        return true;
      }

      @Override
      public boolean isOriginRemote() {
        return false;
      }

      @Override
      public DistributedMember getDistributedMember() {
        return null;
      }

      public boolean isExpiration() {
        return false;
      }

      public boolean isDistributed() {
        return false;
      }

      public boolean isBridgeEvent() {
        return hasClientOrigin();
      }

      @Override
      public boolean hasClientOrigin() {
        return false;
      }

      public ClientProxyMembershipID getContext() {
        return null;
      }

      @Override
      public SerializedCacheValue getSerializedOldValue() {
        return null;
      }

      @Override
      public SerializedCacheValue getSerializedNewValue() {
        return null;
      }
    };
  }

  public void verifyBalanced(final PoolImpl pool, int expectedServer,
      final int expectedConsPerServer) {
    verifyServerCount(pool, expectedServer);
    WaitCriterion ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return balanced(pool, expectedConsPerServer);
      }

      @Override
      public String description() {
        return "expected " + expectedConsPerServer + " but endpoints=" + outOfBalanceReport(pool);
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
    assertTrue("expected " + expectedConsPerServer + " but endpoints=" + outOfBalanceReport(pool),
        balanced(pool, expectedConsPerServer));
  }

  private boolean balanced(PoolImpl pool, int expectedConsPerServer) {
    for (Endpoint ep : pool.getEndpointMap().values()) {
      if (ep.getStats().getConnections() != expectedConsPerServer) {
        return false;
      }
    }
    return true;
  }

  private String outOfBalanceReport(PoolImpl pool) {
    StringBuilder result = new StringBuilder();
    Iterator it = pool.getEndpointMap().values().iterator();
    result.append("<");
    while (it.hasNext()) {
      Endpoint ep = (Endpoint) it.next();
      result.append("ep=").append(ep);
      result.append(" conCount=").append(ep.getStats().getConnections());
      if (it.hasNext()) {
        result.append(", ");
      }
    }
    result.append(">");
    return result.toString();
  }

  public void waitForDenylistToClear(final PoolImpl pool) {
    WaitCriterion ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return pool.getDenylistedServers().size() == 0;
      }

      @Override
      public String description() {
        return null;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
    assertEquals("unexpected denylistedServers=" + pool.getDenylistedServers(), 0,
        pool.getDenylistedServers().size());
  }

  private void verifyServerCount(final PoolImpl pool, final int expectedCount) {
    getCache().getLogger().info("verifyServerCount expects=" + expectedCount);
    WaitCriterion ev = new WaitCriterion() {
      String excuse;

      @Override
      public boolean done() {
        int actual = pool.getConnectedServerCount();
        if (actual == expectedCount) {
          return true;
        }
        excuse = "Found only " + actual + " servers, expected " + expectedCount;
        return false;
      }

      @Override
      public String description() {
        return excuse;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
  }

  /**
   * Tests that the callback argument is sent to the server
   */
  @Test
  public void test001CallbackArg() throws CacheException {
    final String name = this.getName();



    final Object createCallbackArg = "CREATE CALLBACK ARG";
    final Object updateCallbackArg = "PUT CALLBACK ARG";

    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {

        CacheWriter<Object, Object> cw = new TestCacheWriter() {
          @Override
          public final void beforeUpdate2(EntryEvent event) throws CacheWriterException {
            Object beca = event.getCallbackArgument();
            assertEquals(updateCallbackArg, beca);
          }

          @Override
          public void beforeCreate2(EntryEvent event) throws CacheWriterException {
            Object beca = event.getCallbackArgument();
            assertEquals(createCallbackArg, beca);
          }
        };
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, cw);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);

    vm1.invoke("Create region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);

        ClientServerTestCase.configureConnectionPool(factory, NetworkUtils.getServerHostName(),
            port, -1, true, -1, -1, null);
        createRegion(name, factory.create());
      }
    });
    vm1.invoke("Add entries", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          region.create(i, "old" + i, createCallbackArg);
        }
        for (int i = 0; i < 10; i++) {
          region.put(i, "new" + i, updateCallbackArg);
        }
      }
    });

    vm0.invoke("Check cache writer", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        TestCacheWriter writer = getTestWriter(region);
        assertTrue(writer.wasInvoked());
      }
    });

    vm1.invoke("Close Pool", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    });

    vm0.invoke("Stop CacheServer", new SerializableRunnable() {
      @Override
      public void run() {
        stopBridgeServer(getCache());
      }
    });

  }

  /**
   * Tests that consecutive puts have the callback assigned appropriately.
   */
  @Test
  public void test002CallbackArg2() throws CacheException {
    final String name = this.getName();



    final Object createCallbackArg = "CREATE CALLBACK ARG";
    // final Object updateCallbackArg = "PUT CALLBACK ARG";

    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        CacheWriter<Object, Object> cw = new TestCacheWriter<Object, Object>() {
          @Override
          public void beforeCreate2(EntryEvent event) throws CacheWriterException {
            Integer key = (Integer) event.getKey();
            if (key % 2 == 0) {
              Object beca = event.getCallbackArgument();
              assertEquals(createCallbackArg, beca);
            } else {
              Object beca = event.getCallbackArgument();
              assertNull(beca);
            }
          }
        };
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, cw);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    vm1.invoke("Create region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        createRegion(name, factory.create());
      }
    });
    vm1.invoke("Add entries", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          if (i % 2 == 0) {
            region.create(i, "old" + i, createCallbackArg);

          } else {
            region.create(i, "old" + i);
          }
        }
      }
    });

    vm1.invoke("Close Pool", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    });

    vm0.invoke("Check cache writer", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        TestCacheWriter writer = getTestWriter(region);
        assertTrue(writer.wasInvoked());
      }
    });

    vm0.invoke("Stop CacheServer", new SerializableRunnable() {
      @Override
      public void run() {
        stopBridgeServer(getCache());
      }
    });
  }

  /**
   * Tests for bug 36684 by having two cache servers with cacheloaders that should always return a
   * value and one client connected to each server reading values. If the bug exists, the clients
   * will get null sometimes.
   */
  @Test
  public void test003Bug36684() throws CacheException {
    final String name = this.getName();



    // Create the cache servers with distributed, mirrored region
    SerializableRunnable createServer = new CacheSerializableRunnable("Create Cache Server") {
      @Override
      public void run2() throws CacheException {
        CacheLoader<Object, Object> cl = new CacheLoader<Object, Object>() {
          @Override
          public Object load(LoaderHelper helper) {
            return helper.getKey();
          }

          @Override
          public void close() {

          }
        };
        AttributesFactory<Object, Object> factory =
            getBridgeServerMirroredAckRegionAttributes(cl, null);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    };
    getSystem().getLogWriter().info("before create server");
    vm0.invoke(createServer);
    vm1.invoke(createServer);

    // Create cache server clients
    final int numberOfKeys = 1000;
    final String host0 = NetworkUtils.getServerHostName();
    final int vm0Port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final int vm1Port = vm1.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    SerializableRunnable createClient =
        new CacheSerializableRunnable("Create Cache Server Client") {
          @Override
          public void run2() throws CacheException {
            // reset all static listener variables in case this is being rerun in a subclass
            numberOfAfterInvalidates = 0;
            numberOfAfterCreates = 0;
            numberOfAfterUpdates = 0;
            // create the region
            getLonerSystem();
            AttributesFactory<Object, Object> factory = new AttributesFactory<>();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false); // test validation expects this behavior
            // create bridge writer
            ClientServerTestCase.configureConnectionPool(factory, host0, vm0Port, vm1Port, true, -1,
                -1, null);
            createRegion(name, factory.create());
          }
        };
    getSystem().getLogWriter().info("before create client");
    vm2.invoke(createClient);
    vm3.invoke(createClient);

    // Initialize each client with entries (so that afterInvalidate is called)
    SerializableRunnable initializeClient = new CacheSerializableRunnable("Initialize Client") {
      @Override
      public void run2() throws CacheException {
        // StringBuffer errors = new StringBuffer();
        numberOfAfterInvalidates = 0;
        numberOfAfterCreates = 0;
        numberOfAfterUpdates = 0;
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        for (int i = 0; i < numberOfKeys; i++) {
          String expected = "key-" + i;
          String actual = (String) region.get("key-" + i);
          assertEquals(expected, actual);
        }
      }
    };

    getSystem().getLogWriter().info("before initialize client");
    AsyncInvocation inv2 = vm2.invokeAsync(initializeClient);
    AsyncInvocation inv3 = vm3.invokeAsync(initializeClient);

    ThreadUtils.join(inv2, 30 * 1000);
    ThreadUtils.join(inv3, 30 * 1000);

    if (inv2.exceptionOccurred()) {
      fail("Error occurred in vm2", inv2.getException());
    }
    if (inv3.exceptionOccurred()) {
      fail("Error occurred in vm3", inv3.getException());
    }
  }

  /**
   * Test for client connection loss with CacheLoader Exception on the server.
   */
  @Test
  public void test004ForCacheLoaderException() throws CacheException {
    final String name = this.getName();

    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    // Create the cache servers with distributed, mirrored region
    getSystem().getLogWriter().info("before create server");

    server.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        CacheLoader<Object, Object> cl = new CacheLoader<Object, Object>() {
          @Override
          public Object load(LoaderHelper helper) {
            System.out.println("### CALLING CACHE LOADER....");
            throw new CacheLoaderException(
                "Test for CahceLoaderException causing Client connection to disconnect.");
          }

          @Override
          public void close() {}
        };
        AttributesFactory<Object, Object> factory =
            getBridgeServerMirroredAckRegionAttributes(cl, null);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });

    // Create cache server clients
    final int numberOfKeys = 10;
    final String host0 = NetworkUtils.getServerHostName();
    final int[] port =
        new int[] {server.invoke(ConnectionPoolDUnitTest::getCacheServerPort)};
    final String poolName = "myPool";

    getSystem().getLogWriter().info("before create client");
    client.invoke("Create Cache Server Client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        // create bridge writer
        ClientServerTestCase.configureConnectionPoolWithName(factory, host0, port, true, -1, -1,
            null, poolName);
        createRegion(name, factory.create());
      }
    });

    // Initialize each client with entries (so that afterInvalidate is called)

    getSystem().getLogWriter().info("before initialize client");
    AsyncInvocation inv2 = client.invokeAsync(new CacheSerializableRunnable("Initialize Client") {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        PoolStats stats = ((PoolImpl) PoolManager.find(poolName)).getStats();
        int oldConnects = stats.getConnects();
        int oldDisConnects = stats.getDisConnects();
        try {
          for (int i = 0; i < numberOfKeys; i++) {
            region.get("key-" + i);
          }
        } catch (Exception ex) {
          if (!(ex.getCause() instanceof CacheLoaderException)) {
            fail(
                "UnExpected Exception, expected to receive CacheLoaderException from server, instead found: "
                    + ex.getCause().getClass());
          }
        }
        int newConnects = stats.getConnects();
        int newDisConnects = stats.getDisConnects();

        // newDisConnects);
        if (newConnects != oldConnects && newDisConnects != oldDisConnects) {
          fail("New connection has created for Server side CacheLoaderException.");
        }
      }
    });

    ThreadUtils.join(inv2, 30 * 1000);
    server.invoke("stop CacheServer", () -> stopBridgeServer(getCache()));

  }

  private void validateDS() {
    List l = InternalDistributedSystem.getExistingSystems();
    if (l.size() > 1) {
      getSystem().getLogWriter().info("validateDS: size=" + l.size() + " isDedicatedAdminVM="
          + ClusterDistributionManager.isDedicatedAdminVM() + " l=" + l);
    }
    assertFalse(ClusterDistributionManager.isDedicatedAdminVM());
    assertEquals(1, l.size());
  }

  /**
   * Tests the basic operations of the {@link Pool}
   *
   * @since GemFire 3.5
   */
  @Test
  public void test006Pool() throws CacheException {
    final String name = this.getName();



    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setConcurrencyChecksEnabled(false);
        factory.setCacheLoader(new CacheLoader<Object, Object>() {
          @Override
          public Object load(LoaderHelper helper) {
            // System.err.println("CacheServer data loader called");
            return helper.getKey().toString();
          }

          @Override
          public void close() {

          }
        });
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    vm1.invoke("Create region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        validateDS();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        createRegion(name, factory.create());
      }
    });

    vm1.invoke("Get values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Object value = region.get(i);
          assertEquals(String.valueOf(i), value);
        }
      }
    });

    vm1.invoke("Update values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);

        for (int i = 0; i < 10; i++) {
          region.put(i, i);
        }
      }
    });

    vm2.invoke("Create region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        validateDS();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        createRegion(name, factory.create());
      }
    });
    vm2.invoke("Validate values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Object value = region.get(i);
          assertNotNull(value);
          assertTrue(value instanceof Integer);
          assertEquals(i, ((Integer) value).intValue());
        }
      }
    });

    vm1.invoke("Close Pool", new CacheSerializableRunnable() {
      // do some special close validation here
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        String pName = region.getAttributes().getPoolName();
        PoolImpl p = (PoolImpl) PoolManager.find(pName);
        assertFalse(p.isDestroyed());
        assertEquals(1, p.getAttachCount());
        try {
          p.destroy();
          fail("expected IllegalStateException");
        } catch (IllegalStateException ignored) {
        }
        region.localDestroyRegion();
        assertFalse(p.isDestroyed());
        assertEquals(0, p.getAttachCount());
      }
    });

    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests the BridgeServer failover (bug 31832).
   */
  @Test
  public void test007BridgeServerFailoverCnx1() throws CacheException {
    disconnectAllFromDS();
    basicTestBridgeServerFailover(1);
  }

  /**
   * Test BridgeServer failover with connectionsPerServer set to 0
   */
  @Test
  public void test008BridgeServerFailoverCnx0() throws CacheException {
    basicTestBridgeServerFailover(0);
  }

  private void basicTestBridgeServerFailover(final int cnxCount) throws CacheException {
    final String name = this.getName();



    // Create two cache servers
    SerializableRunnable createCacheServer = new CacheSerializableRunnable("Create Cache Server") {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
        createRegion(name, factory.create());
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }
      }
    };

    vm0.invoke(createCacheServer);
    vm1.invoke(createCacheServer);

    final int port0 = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    final int port1 = vm1.invoke(ConnectionPoolDUnitTest::getCacheServerPort);

    // Create one bridge client in this VM

    vm2.invoke("Create region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        ClientServerTestCase.configureConnectionPool(factory, host0, port0, port1, true, -1,
            cnxCount, null, 100);

        Region<Object, Object> region = createRegion(name, factory.create());

        // force connections to form
        region.put("keyInit", 0);
        region.put("keyInit2", 0);
      }
    });

    vm2.invoke("verify2Servers", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        PoolImpl pool = getPool(region);
        verifyServerCount(pool, 2);
      }
    });

    final String expected = "java.io.IOException";
    final String addExpected = "<ExpectedException action=add>" + expected + "</ExpectedException>";
    final String removeExpected =
        "<ExpectedException action=remove>" + expected + "</ExpectedException>";

    vm2.invoke(() -> {
      LogWriter bgexecLogger = new LocalLogWriter(ALL.intLevel(), System.out);
      bgexecLogger.info(addExpected);
    });

    try { // make sure we removeExpected

      // Bounce the non-current server (I know that VM1 contains the non-current server
      // because ...
      vm1.invoke(() -> stopBridgeServer(getCache()));

      vm2.invoke("verify1Server", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          Region<Object, Object> region = getRootRegion().getSubregion(name);
          PoolImpl pool = getPool(region);
          verifyServerCount(pool, 1);
        }
      });

      vm1.invoke("Restart CacheServer", () -> {
        try {
          Region<Object, Object> region = getRootRegion().getSubregion(name);
          assertNotNull(region);
          startBridgeServer(port1);
        } catch (Exception e) {
          getSystem().getLogWriter().fine(new Exception(e));
          fail("Failed to start CacheServer", e);
        }
      });

      // Pause long enough for the monitor to realize the server has been bounced
      // and reconnect to it.
      vm2.invoke("verify2Servers", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          Region<Object, Object> region = getRootRegion().getSubregion(name);
          PoolImpl pool = getPool(region);
          verifyServerCount(pool, 2);
        }
      });

    } finally {
      vm2.invoke(() -> {
        LogWriter bgexecLogger = new LocalLogWriter(ALL.intLevel(), System.out);
        bgexecLogger.info(removeExpected);
      });
    }

    // Stop the other cache server
    vm0.invoke(() -> stopBridgeServer(getCache()));

    // Run awhile
    vm2.invoke("verify1Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        PoolImpl pool = getPool(region);
        verifyServerCount(pool, 1);
      }
    });

    // Close Pool
    vm2.invoke("Close Pool", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    });

    // Stop the last cache server
    vm1.invoke(() -> stopBridgeServer(getCache()));
  }


  private static volatile boolean stopTestLifetimeExpire = false;

  private static volatile int baselineLifetimeCheck;
  private static volatile int baselineLifetimeExtensions;
  private static volatile int baselineLifetimeConnect;
  private static volatile int baselineLifetimeDisconnect;

  @Test
  public void basicTestLifetimeExpire() throws CacheException {
    final String name = this.getName();



    AsyncInvocation putAI = null;
    AsyncInvocation putAI2 = null;

    try {

      // Create two cache servers

      vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
          factory.setCacheListener(new DelayListener(25));
          createRegion(name, factory.create());
          try {
            startBridgeServer(0);

          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });

      final int port0 = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
      final String host0 = NetworkUtils.getServerHostName();
      vm1.invoke("Create Cache Server", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
          factory.setCacheListener(new DelayListener(25));
          createRegion(name, factory.create());
          try {
            startBridgeServer(0);

          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });
      final int port1 = vm1.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
      // we only had to stop it to reserve a port
      vm1.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));

      // Create one bridge client in this VM

      vm2.invoke("Create region", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          getLonerSystem();
          getCache();
          AttributesFactory<Object, Object> factory = new AttributesFactory<>();
          factory.setScope(Scope.LOCAL);
          factory.setConcurrencyChecksEnabled(false);
          ClientServerTestCase.configureConnectionPool(factory, host0, port0, port1,
              false/* queue */, -1, 0, null, 100, 500, 500);

          Region<Object, Object> region = createRegion(name, factory.create());

          // force connections to form
          region.put("keyInit", 0);
          region.put("keyInit2", 0);
        }
      });

      // Launch async thread that puts objects into cache. This thread will execute until
      // the test has ended.
      putAI = vm2.invokeAsync(new CacheSerializableRunnable("Put objects") {
        @Override
        public void run2() throws CacheException {
          Region<Object, Object> region = getRootRegion().getSubregion(name);
          PoolImpl pool = getPool(region);
          PoolStats stats = pool.getStats();
          baselineLifetimeCheck = stats.getLoadConditioningCheck();
          baselineLifetimeExtensions = stats.getLoadConditioningExtensions();
          baselineLifetimeConnect = stats.getLoadConditioningConnect();
          baselineLifetimeDisconnect = stats.getLoadConditioningDisconnect();
          try {
            int count = 0;
            while (!stopTestLifetimeExpire) {
              count++;
              region.put("keyAI1", count);
            }
          } catch (NoAvailableServersException ex) {
            if (!stopTestLifetimeExpire) {
              throw ex;
            }

          }
        }
      });
      putAI2 = vm2.invokeAsync(new CacheSerializableRunnable("Put objects") {
        @Override
        public void run2() throws CacheException {
          Region<Object, Object> region = getRootRegion().getSubregion(name);
          try {
            int count = 0;
            while (!stopTestLifetimeExpire) {
              count++;
              region.put("keyAI2", count);
            }
          } catch (NoAvailableServersException ex) {
            if (!stopTestLifetimeExpire) {
              throw ex;
            }
          }
        }
      });

      vm2.invoke("verify1Server", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          Region<Object, Object> region = getRootRegion().getSubregion(name);
          PoolImpl pool = getPool(region);
          final PoolStats stats = pool.getStats();
          verifyServerCount(pool, 1);
          WaitCriterion ev = new WaitCriterion() {
            @Override
            public boolean done() {
              return stats.getLoadConditioningCheck() >= (10 + baselineLifetimeCheck);
            }

            @Override
            public String description() {
              return null;
            }
          };
          GeodeAwaitility.await().untilAsserted(ev);

          // make sure no replacements are happening.
          // since we have 2 threads and 2 cnxs and 1 server
          // when lifetimes are up we should only want to connect back to the
          // server we are already connected to and thus just extend our lifetime
          assertTrue(
              "baselineLifetimeCheck=" + baselineLifetimeCheck
                  + " but stats.getLoadConditioningCheck()=" + stats.getLoadConditioningCheck(),
              stats.getLoadConditioningCheck() >= (10 + baselineLifetimeCheck));
          baselineLifetimeCheck = stats.getLoadConditioningCheck();
          assertTrue(stats.getLoadConditioningExtensions() > baselineLifetimeExtensions);
          assertEquals(stats.getLoadConditioningConnect(), baselineLifetimeConnect);
          assertEquals(stats.getLoadConditioningDisconnect(), baselineLifetimeDisconnect);
        }
      });
      assertTrue(putAI.isAlive());
      assertTrue(putAI2.isAlive());
    } finally {
      vm2.invoke("Stop Putters", () -> stopTestLifetimeExpire = true);

      try {
        if (putAI != null) {
          // Verify that no exception has occurred in the putter thread
          ThreadUtils.join(putAI, 30 * 1000);
          if (putAI.exceptionOccurred()) {
            fail("While putting entries: ",
                putAI.getException());
          }
        }

        if (putAI2 != null) {
          // Verify that no exception has occurred in the putter thread
          ThreadUtils.join(putAI, 30 * 1000);
          // FIXME this thread does not terminate
          // if (putAI2.exceptionOccurred()) {
          // fail("While putting entries: ", putAI.getException());
          // }
        }

      } finally {
        vm2.invoke("Stop Putters", () -> stopTestLifetimeExpire = true);
        // Close Pool
        vm2.invoke("Close Pool", new CacheSerializableRunnable() {
          @Override
          public void run2() throws CacheException {
            Region<Object, Object> region = getRootRegion().getSubregion(name);
            String poolName = region.getAttributes().getPoolName();
            region.localDestroyRegion();
            PoolManager.find(poolName).destroy();
          }
        });

        vm1.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
        vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
      }
    }
  }

  /**
   * Tests the create operation of the {@link Pool}
   *
   * @since GemFire 3.5
   */
  @Test
  public void test011PoolCreate() throws CacheException {
    final String name = this.getName();

    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    vm1.invoke("Create region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, false, -1, -1, null);
        createRegion(name, factory.create());
      }
    });
    vm1.invoke("Create values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          region.create(i, i);
        }
      }
    });

    vm2.invoke("Create region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, false, -1, -1, null);
        createRegion(name, factory.create());
      }
    });
    vm2.invoke("Validate values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Object value = region.get(i);
          assertNotNull(value);
          assertTrue(value instanceof Integer);
          assertEquals(i, ((Integer) value).intValue());
        }
      }
    });

    SerializableRunnable close = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests the put operation of the {@link Pool}
   *
   * @since GemFire 3.5
   */
  @Test
  public void test012PoolPut() throws CacheException {
    final String name = this.getName();

    vm0 = VM.getVM(0);
    vm1 = VM.getVM(1);
    vm2 = VM.getVM(2);

    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
        createRegion(name, factory.create());
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });

    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    SerializableRunnable createPool = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        // create bridge writer
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, false, -1, -1, null);
        createRegion(name, factory.create());
      }
    };

    vm1.invoke(createPool);

    vm1.invoke("Put values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          // put string values
          region.put("key-string-" + i, "value-" + i);

          // put object values
          Order order = new Order();
          order.init(i);
          region.put("key-object-" + i, order);

          // put byte[] values
          region.put("key-bytes-" + i, ("value-" + i).getBytes());
        }
      }
    });

    vm2.invoke(createPool);

    vm2.invoke("Get / validate string values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Object value = region.get("key-string-" + i);
          assertNotNull(value);
          assertTrue(value instanceof String);
          assertEquals("value-" + i, value);
        }
      }
    });

    vm2.invoke("Get / validate object values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Object value = region.get("key-object-" + i);
          assertNotNull(value);
          assertTrue(value instanceof Order);
          assertEquals(i, ((Order) value).getIndex());
        }
      }
    });

    vm2.invoke("Get / validate byte[] values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Object value = region.get("key-bytes-" + i);
          assertNotNull(value);
          assertTrue(value instanceof byte[]);
          assertEquals("value-" + i, new String((byte[]) value));
        }
      }
    });

    SerializableRunnable closePool = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(closePool);
    vm2.invoke(closePool);

    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests the put operation of the {@link Pool}
   *
   * @since GemFire 3.5
   */
  @Test
  public void test013PoolPutNoDeserialize() throws CacheException {
    final String name = this.getName();



    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
        createRegion(name, factory.create());
        try {
          startBridgeServer(0);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });

    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    SerializableRunnable createPool = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, false, -1, -1, null);
        createRegion(name, factory.create());
      }
    };

    vm1.invoke(createPool);

    vm1.invoke("Put values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          // put string values
          region.put("key-string-" + i, "value-" + i);

          // put object values
          Order order = new Order();
          order.init(i);
          region.put("key-object-" + i, order);

          // put byte[] values
          region.put("key-bytes-" + i, ("value-" + i).getBytes());
        }
      }
    });

    vm2.invoke(createPool);

    vm2.invoke("Get / validate string values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Object value = region.get("key-string-" + i);
          assertNotNull(value);
          assertTrue(value instanceof String);
          assertEquals("value-" + i, value);
        }
      }
    });

    vm2.invoke("Get / validate object values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Object value = region.get("key-object-" + i);
          assertNotNull(value);
          assertTrue(value instanceof Order);
          assertEquals(i, ((Order) value).getIndex());
        }
      }
    });

    vm2.invoke("Get / validate byte[] values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Object value = region.get("key-bytes-" + i);
          assertNotNull(value);
          assertTrue(value instanceof byte[]);
          assertEquals("value-" + i, new String((byte[]) value));
        }
      }
    });

    SerializableRunnable closePool = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(closePool);
    vm2.invoke(closePool);

    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
    Wait.pause(5 * 1000);
  }

  /**
   * Tests that invalidates and destroys are propagated to {@link Pool}s.
   *
   * @since GemFire 3.5
   */
  @Test
  public void test014InvalidateAndDestroyPropagation() throws CacheException {
    final String name = this.getName();



    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }

    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    SerializableRunnable create = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        CertifiableTestCacheListener l = new CertifiableTestCacheListener(
            org.apache.geode.test.dunit.LogWriterUtils.getLogWriter());
        factory.setCacheListener(l);
        Region rgn = createRegion(name, factory.create());
        rgn.registerInterestRegex(".*", false, false);
      }
    };

    vm1.invoke(create);
    vm1.invoke("Populate region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          region.put(i, "old" + i);
        }
      }
    });
    vm2.invoke(create);
    Wait.pause(5 * 1000);

    vm1.invoke("Turn on history", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        ctl.enableEventHistory();
      }
    });
    vm2.invoke("Update region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          region.put(i, "new" + i, "callbackArg" + i);
        }
      }
    });
    Wait.pause(5 * 1000);

    vm1.invoke("Verify invalidates", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        for (int i = 0; i < 10; i++) {
          Object key = i;
          ctl.waitForInvalidated(key);
          Region.Entry entry = region.getEntry(key);
          assertNotNull(entry);
          assertNull(entry.getValue());
        }
        {
          List l = ctl.getEventHistory();
          assertEquals(10, l.size());
          for (int i = 0; i < 10; i++) {
            Object key = i;
            EntryEvent ee = (EntryEvent) l.get(i);
            assertEquals(key, ee.getKey());
            assertEquals("old" + i, ee.getOldValue());
            assertEquals(Operation.INVALIDATE, ee.getOperation());
            assertEquals("callbackArg" + i, ee.getCallbackArgument());
            assertTrue(ee.isOriginRemote());
          }
        }
      }
    });

    vm2.invoke("Validate original and destroy", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Object key = i;
          assertEquals("new" + i, region.getEntry(key).getValue());
          region.destroy(key, "destroyCB" + i);
        }
      }
    });
    Wait.pause(5 * 1000);

    vm1.invoke("Verify destroys", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        for (int i = 0; i < 10; i++) {
          Object key = i;
          ctl.waitForDestroyed(key);
          Region.Entry entry = region.getEntry(key);
          assertNull(entry);
        }
        {
          List l = ctl.getEventHistory();
          assertEquals(10, l.size());
          for (int i = 0; i < 10; i++) {
            Object key = i;
            EntryEvent ee = (EntryEvent) l.get(i);
            assertEquals(key, ee.getKey());
            assertNull(ee.getOldValue());
            assertEquals(Operation.DESTROY, ee.getOperation());
            assertEquals("destroyCB" + i, ee.getCallbackArgument());
            assertTrue(ee.isOriginRemote());
          }
        }
      }
    });
    vm2.invoke("recreate", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Object key = i;
          region.create(key, "create" + i);
        }
      }
    });
    Wait.pause(5 * 1000);

    vm1.invoke("Verify creates", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        List l = ctl.getEventHistory();
        logger
            .info("history (should be empty): " + l);
        assertEquals(0, l.size());
        // now see if we can get it from the server
        for (int i = 0; i < 10; i++) {
          Object key = i;
          assertEquals("create" + i, region.get(key, "loadCB" + i));
        }
        l = ctl.getEventHistory();
        assertEquals(10, l.size());
        for (int i = 0; i < 10; i++) {
          Object key = i;
          EntryEvent ee = (EntryEvent) l.get(i);
          logger.info("processing " + ee);
          assertEquals(key, ee.getKey());
          assertNull(ee.getOldValue());
          assertEquals("create" + i, ee.getNewValue());
          assertEquals(Operation.LOCAL_LOAD_CREATE, ee.getOperation());
          assertEquals("loadCB" + i, ee.getCallbackArgument());
          assertFalse(ee.isOriginRemote());
        }
      }
    });

    SerializableRunnable close = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests that invalidates and destroys are propagated to {@link Pool}s correctly to
   * DataPolicy.EMPTY + InterestPolicy.ALL
   *
   * @since GemFire 5.0
   */
  @Test
  public void test015InvalidateAndDestroyToEmptyAllPropagation() throws CacheException {
    final String name = this.getName();



    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    SerializableRunnable createEmpty = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        CertifiableTestCacheListener l = new CertifiableTestCacheListener(
            org.apache.geode.test.dunit.LogWriterUtils.getLogWriter());
        factory.setCacheListener(l);
        factory.setDataPolicy(DataPolicy.EMPTY);
        factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
        Region rgn = createRegion(name, factory.create());
        rgn.registerInterestRegex(".*", false, false);
      }
    };
    SerializableRunnable createNormal = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        CertifiableTestCacheListener l = new CertifiableTestCacheListener(
            org.apache.geode.test.dunit.LogWriterUtils.getLogWriter());
        factory.setCacheListener(l);
        Region rgn = createRegion(name, factory.create());
        rgn.registerInterestRegex(".*", false, false);
      }
    };

    vm1.invoke(createEmpty);
    vm1.invoke("Populate region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          region.put(i, "old" + i);
        }
      }
    });

    vm2.invoke(createNormal);
    vm1.invoke("Turn on history", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        ctl.enableEventHistory();
      }
    });
    vm2.invoke("Update region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          region.put(i, "new" + i, "callbackArg" + i);
        }
      }
    });
    Wait.pause(5 * 1000);

    vm1.invoke("Verify invalidates", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        for (int i = 0; i < 10; i++) {
          Object key = i;
          ctl.waitForInvalidated(key);
          Region.Entry entry = region.getEntry(key);
          assertNull(entry); // we are empty!
        }
        {
          List l = ctl.getEventHistory();
          assertEquals(10, l.size());
          for (int i = 0; i < 10; i++) {
            Object key = i;
            EntryEvent ee = (EntryEvent) l.get(i);
            assertEquals(key, ee.getKey());
            assertNull(ee.getOldValue());
            assertFalse(ee.isOldValueAvailable()); // failure
            assertEquals(Operation.INVALIDATE, ee.getOperation());
            assertEquals("callbackArg" + i, ee.getCallbackArgument());
            assertTrue(ee.isOriginRemote());
          }
        }

      }
    });

    vm2.invoke("Validate original and destroy", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Object key = i;
          assertEquals("new" + i, region.getEntry(key).getValue());
          region.destroy(key, "destroyCB" + i);
        }
      }
    });
    Wait.pause(5 * 1000);

    vm1.invoke("Verify destroys", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        for (int i = 0; i < 10; i++) {
          Object key = i;
          ctl.waitForDestroyed(key);
          Region.Entry entry = region.getEntry(key);
          assertNull(entry);
        }
        {
          List l = ctl.getEventHistory();
          assertEquals(10, l.size());
          for (int i = 0; i < 10; i++) {
            Object key = i;
            EntryEvent ee = (EntryEvent) l.get(i);
            assertEquals(key, ee.getKey());
            assertNull(ee.getOldValue());
            assertFalse(ee.isOldValueAvailable());
            assertEquals(Operation.DESTROY, ee.getOperation());
            assertEquals("destroyCB" + i, ee.getCallbackArgument());
            assertTrue(ee.isOriginRemote());
          }
        }
      }
    });
    vm2.invoke("recreate", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Object key = i;
          region.create(key, "create" + i, "createCB" + i);
        }
      }
    });
    Wait.pause(5 * 1000);

    vm1.invoke("Verify creates", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        for (int i = 0; i < 10; i++) {
          Object key = i;
          ctl.waitForInvalidated(key);
          Region.Entry entry = region.getEntry(key);
          assertNull(entry);
        }
        List l = ctl.getEventHistory();
        assertEquals(10, l.size());
        for (int i = 0; i < 10; i++) {
          Object key = i;
          EntryEvent ee = (EntryEvent) l.get(i);
          assertEquals(key, ee.getKey());
          assertNull(ee.getOldValue());
          assertFalse(ee.isOldValueAvailable());
          assertEquals(Operation.INVALIDATE, ee.getOperation());
          assertEquals("createCB" + i, ee.getCallbackArgument());
          assertTrue(ee.isOriginRemote());
        }
        // now see if we can get it from the server
        for (int i = 0; i < 10; i++) {
          Object key = i;
          assertEquals("create" + i, region.get(key, "loadCB" + i));
        }
        l = ctl.getEventHistory();
        assertEquals(10, l.size());
        for (int i = 0; i < 10; i++) {
          Object key = i;
          EntryEvent ee = (EntryEvent) l.get(i);
          assertEquals(key, ee.getKey());
          assertNull(ee.getOldValue());
          assertEquals("create" + i, ee.getNewValue());
          assertEquals(Operation.LOCAL_LOAD_CREATE, ee.getOperation());
          assertEquals("loadCB" + i, ee.getCallbackArgument());
          assertFalse(ee.isOriginRemote());
        }
      }
    });

    SerializableRunnable close = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests that invalidates and destroys are propagated to {@link Pool}s correctly to
   * DataPolicy.EMPTY + InterestPolicy.CACHE_CONTENT
   *
   * @since GemFire 5.0
   */
  @Test
  public void test016InvalidateAndDestroyToEmptyCCPropagation() throws CacheException {
    final String name = this.getName();



    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
        createRegion(name, factory.create());
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    SerializableRunnable createEmpty = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        CertifiableTestCacheListener l = new CertifiableTestCacheListener(
            org.apache.geode.test.dunit.LogWriterUtils.getLogWriter());
        factory.setCacheListener(l);
        factory.setDataPolicy(DataPolicy.EMPTY);
        factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT));
        Region rgn = createRegion(name, factory.create());
        rgn.registerInterestRegex(".*", false, false);
      }
    };
    SerializableRunnable createNormal = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        CertifiableTestCacheListener l = new CertifiableTestCacheListener(
            org.apache.geode.test.dunit.LogWriterUtils.getLogWriter());
        factory.setCacheListener(l);
        Region rgn = createRegion(name, factory.create());
        rgn.registerInterestRegex(".*", false, false);
      }
    };

    vm1.invoke(createEmpty);
    vm1.invoke("Populate region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          region.put(i, "old" + i);
        }
      }
    });

    vm2.invoke(createNormal);
    vm1.invoke("Turn on history", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        ctl.enableEventHistory();
      }
    });
    vm2.invoke("Update region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          region.put(i, "new" + i, "callbackArg" + i);
        }
      }
    });
    Wait.pause(5 * 1000);

    vm1.invoke("Verify invalidates", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        List l = ctl.getEventHistory();
        assertEquals(0, l.size());
      }
    });

    vm2.invoke("Validate original and destroy", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Object key = i;
          assertEquals("new" + i, region.getEntry(key).getValue());
          region.destroy(key, "destroyCB" + i);
        }
      }
    });

    vm1.invoke("Verify destroys", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        List l = ctl.getEventHistory();
        assertEquals(0, l.size());
      }
    });
    vm2.invoke("recreate", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Object key = i;
          region.create(key, "create" + i, "createCB" + i);
        }
      }
    });
    Wait.pause(5 * 1000);

    vm1.invoke("Verify creates", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        List l = ctl.getEventHistory();
        assertEquals(0, l.size());
        // now see if we can get it from the server
        for (int i = 0; i < 10; i++) {
          Object key = i;
          assertEquals("create" + i, region.get(key, "loadCB" + i));
        }
        l = ctl.getEventHistory();
        assertEquals(10, l.size());
        for (int i = 0; i < 10; i++) {
          Object key = i;
          EntryEvent ee = (EntryEvent) l.get(i);
          assertEquals(key, ee.getKey());
          assertNull(ee.getOldValue());
          assertEquals("create" + i, ee.getNewValue());
          assertEquals(Operation.LOCAL_LOAD_CREATE, ee.getOperation());
          assertEquals("loadCB" + i, ee.getCallbackArgument());
          assertFalse(ee.isOriginRemote());
        }
      }
    });

    SerializableRunnable close = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests interest key registration.
   */
  @Test
  public void test017ExpireDestroyHasEntryInCallback() throws CacheException {
    disconnectAllFromDS();
    final String name = this.getName();



    // Create cache server
    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // In lieu of System.setProperty("gemfire.EXPIRE_SENDS_ENTRY_AS_CALLBACK", "true");
        EntryExpiryTask.expireSendsEntryAsCallback = true;
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
        factory.setEntryTimeToLive(new ExpirationAttributes(1, ExpirationAction.DESTROY));
        createRegion(name, factory.create());
        try {
          startBridgeServer(0);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }
      }
    });

    // Create cache server clients
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    SerializableRunnable createClient = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setDataPolicy(DataPolicy.EMPTY);
        factory.setSubscriptionAttributes(new SubscriptionAttributes((InterestPolicy.ALL)));
        factory.setConcurrencyChecksEnabled(false);
        // create bridge writer
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        CertifiableTestCacheListener l = new CertifiableTestCacheListener(
            org.apache.geode.test.dunit.LogWriterUtils.getLogWriter());
        factory.setCacheListener(l);

        Region<Object, Object> r = createRegion(name, factory.create());
        r.registerInterest("ALL_KEYS");
      }
    };

    vm1.invoke(createClient);

    vm1.invoke("Turn on history", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        ctl.enableEventHistory();
      }
    });
    Wait.pause(500);

    // Create some entries on the client
    vm1.invoke("Create entries", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 5; i++) {
          region.put("key-client-" + i, "value-client-" + i);
        }
      }
    });

    // Create some entries on the server
    vm0.invoke("Create entries", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 5; i++) {
          region.put("key-server-" + i, "value-server-" + i);
        }
      }
    });

    // Wait for expiration
    Wait.pause(2000);

    vm1.invoke("Validate listener events", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        int destroyCallbacks = 0;
        List<CacheEvent> l = ctl.getEventHistory();
        for (CacheEvent ce : l) {
          logger.info("--->>> " + ce);
          if (ce.getOperation() == Operation.DESTROY
              && ce.getCallbackArgument() instanceof String) {
            destroyCallbacks++;
          }
        }
        assertEquals(10, destroyCallbacks);
      }
    });

    // Close cache server clients
    SerializableRunnable close = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);

    // Stop cache server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  private <K, V> AttributesFactory<K, V> getBridgeServerRegionAttributes(CacheLoader<K, V> cl,
      CacheWriter<K, V> cw) {
    AttributesFactory<K, V> ret = new AttributesFactory<>();
    if (cl != null) {
      ret.setCacheLoader(cl);
    }
    if (cw != null) {
      ret.setCacheWriter(cw);
    }
    ret.setScope(Scope.DISTRIBUTED_ACK);
    ret.setConcurrencyChecksEnabled(false);
    return ret;
  }

  private <K, V> AttributesFactory<K, V> getBridgeServerMirroredAckRegionAttributes(
      CacheLoader<K, V> cl,
      CacheWriter<K, V> cw) {
    AttributesFactory<K, V> ret = new AttributesFactory<>();
    if (cl != null) {
      ret.setCacheLoader(cl);
    }
    if (cw != null) {
      ret.setCacheWriter(cw);
    }
    ret.setScope(Scope.DISTRIBUTED_ACK);
    ret.setConcurrencyChecksEnabled(false);
    ret.setMirrorType(MirrorType.KEYS_VALUES);

    return ret;
  }

  /**
   * Tests that updates are not sent to VMs that did not ask for them.
   */
  @Test
  public void test018OnlyRequestedUpdates() {
    final String name1 = this.getName() + "-1";
    final String name2 = this.getName() + "-2";



    // Cache server serves up both regions
    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
        createRegion(name1, factory.create());
        createRegion(name2, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    // vm1 sends updates to the server
    vm1.invoke("Create regions", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);

        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);

        Region rgn = createRegion(name1, factory.create());
        rgn.registerInterestRegex(".*", false, false);
        rgn = createRegion(name2, factory.create());
        rgn.registerInterestRegex(".*", false, false);

      }
    });

    // vm2 only wants updates to updates to region1
    vm2.invoke("Create region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);

        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);

        Region rgn = createRegion(name1, factory.create());
        rgn.registerInterestRegex(".*", false, false);
        createRegion(name2, factory.create());
        // no interest registration for region 2
      }
    });

    SerializableRunnable populate = new CacheSerializableRunnable("Populate region") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region1 = getRootRegion().getSubregion(name1);
        for (int i = 0; i < 10; i++) {
          region1.put(i, "Region1Old" + i);
        }
        Region<Object, Object> region2 = getRootRegion().getSubregion(name2);
        for (int i = 0; i < 10; i++) {
          region2.put(i, "Region2Old" + i);
        }
      }
    };
    vm1.invoke(populate);
    vm2.invoke(populate);

    vm1.invoke("Update", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region1 = getRootRegion().getSubregion(name1);
        for (int i = 0; i < 10; i++) {
          region1.put(i, "Region1New" + i);
        }
        Region<Object, Object> region2 = getRootRegion().getSubregion(name2);
        for (int i = 0; i < 10; i++) {
          region2.put(i, "Region2New" + i);
        }
      }
    });

    // Wait for updates to be propagated
    Wait.pause(5 * 1000);

    vm2.invoke("Validate", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region1 = getRootRegion().getSubregion(name1);
        for (int i = 0; i < 10; i++) {
          assertEquals("Region1New" + i, region1.get(i));
        }
        Region<Object, Object> region2 = getRootRegion().getSubregion(name2);
        for (int i = 0; i < 10; i++) {
          assertEquals("Region2Old" + i, region2.get(i));
        }
      }
    });

    vm1.invoke("Close Pool", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // Terminate region1's Pool
        Region<Object, Object> region1 = getRootRegion().getSubregion(name1);
        region1.localDestroyRegion();
        // Terminate region2's Pool
        Region<Object, Object> region2 = getRootRegion().getSubregion(name2);
        region2.localDestroyRegion();
      }
    });

    vm2.invoke("Close Pool", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // Terminate region1's Pool
        Region<Object, Object> region1 = getRootRegion().getSubregion(name1);
        region1.localDestroyRegion();
      }
    });

    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }


  /**
   * Tests interest key registration.
   */
  @Test
  public void test019InterestKeyRegistration() throws CacheException {
    final String name = this.getName();



    // Create cache server
    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        CacheLoader<Object, Object> cl = new CacheLoader<Object, Object>() {
          @Override
          public Object load(LoaderHelper helper) {
            return helper.getKey();
          }

          @Override
          public void close() {

          }
        };
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(cl, null);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });

    // Create cache server clients
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    SerializableRunnable create = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        // create bridge writer
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        createRegion(name, factory.create());
      }
    };

    vm1.invoke(create);
    vm2.invoke(create);

    // Get values for key 1 and key 2 so that there are entries in the clients.
    // Register interest in one of the keys.
    vm1.invoke("Create Entries and Register Interest", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        assertEquals(region.get("key-1"), "key-1");
        assertEquals(region.get("key-2"), "key-2");
        try {
          region.registerInterest("key-1");
        } catch (Exception ex) {
          fail("While registering interest: ", ex);
        }
      }
    });

    vm2.invoke("Create Entries and Register Interest", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        assertEquals(region.get("key-1"), "key-1");
        assertEquals(region.get("key-2"), "key-2");
        try {
          region.registerInterest("key-2");
        } catch (Exception ex) {
          fail("While registering interest: ", ex);
        }
      }
    });

    // Put new values and validate updates (VM1)
    vm1.invoke("Put New Values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        region.put("key-1", "vm1-key-1");
        region.put("key-2", "vm1-key-2");
        // Verify that no invalidates occurred to this region
        assertEquals(region.getEntry("key-1").getValue(), "vm1-key-1");
        assertEquals(region.getEntry("key-2").getValue(), "vm1-key-2");
      }
    });

    Wait.pause(500);
    vm2.invoke("Validate Entries", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        // Verify that 'key-2' was updated, but 'key-1' was not
        // and contains the original value
        assertEquals(region.getEntry("key-1").getValue(), "key-1");
        assertEquals(region.getEntry("key-2").getValue(), "vm1-key-2");
        // assertNull(region.getEntry("key-2").getValue());
      }
    });

    // Put new values and validate updates (VM2)
    vm2.invoke("Put New Values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        region.put("key-1", "vm2-key-1");
        region.put("key-2", "vm2-key-2");
        // Verify that no updates occurred to this region
        assertEquals(region.getEntry("key-1").getValue(), "vm2-key-1");
        assertEquals(region.getEntry("key-2").getValue(), "vm2-key-2");
      }
    });

    Wait.pause(500);
    vm1.invoke("Validate Entries", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        // Verify that 'key-1' was updated, but 'key-2' was not
        // and contains the original value
        assertEquals(region.getEntry("key-2").getValue(), "vm1-key-2");
        assertEquals(region.getEntry("key-1").getValue(), "vm2-key-1");
        // assertNull(region.getEntry("key-1").getValue());
      }
    });

    // Unregister interest
    vm1.invoke("Unregister Interest", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        try {
          region.unregisterInterest("key-1");
        } catch (Exception ex) {
          fail("While registering interest: ", ex);
        }
      }
    });

    vm2.invoke("Unregister Interest", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        try {
          region.unregisterInterest("key-2");
        } catch (Exception ex) {
          fail("While registering interest: ", ex);
        }
      }
    });

    // Put new values and validate updates (VM1)
    vm1.invoke("Put New Values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        region.put("key-1", "vm1-key-1-again");
        region.put("key-2", "vm1-key-2-again");
        // Verify that no updates occurred to this region
        assertEquals(region.getEntry("key-1").getValue(), "vm1-key-1-again");
        assertEquals(region.getEntry("key-2").getValue(), "vm1-key-2-again");
      }
    });

    Wait.pause(500);
    vm2.invoke("Validate Entries", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        // Verify that neither 'key-1' 'key-2' was updated
        // and contain the original value
        assertEquals(region.getEntry("key-1").getValue(), "vm2-key-1");
        assertEquals(region.getEntry("key-2").getValue(), "vm2-key-2");
      }
    });

    // Put new values and validate updates (VM2)
    vm2.invoke("Put New Values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        region.put("key-1", "vm2-key-1-again");
        region.put("key-2", "vm2-key-2-again");
        // Verify that no updates occurred to this region
        assertEquals(region.getEntry("key-1").getValue(), "vm2-key-1-again");
        assertEquals(region.getEntry("key-2").getValue(), "vm2-key-2-again");
      }
    });

    Wait.pause(500);
    vm1.invoke("Validate Entries", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        // Verify that neither 'key-1' 'key-2' was updated
        // and contain the original value
        assertEquals(region.getEntry("key-1").getValue(), "vm1-key-1-again");
        assertEquals(region.getEntry("key-2").getValue(), "vm1-key-2-again");
      }
    });

    // Unregister interest again (to verify that a client can unregister interest
    // in a key that its not interested in with no problem.
    vm1.invoke("Unregister Interest", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        try {
          region.unregisterInterest("key-1");
        } catch (Exception ex) {
          fail("While registering interest: ", ex);
        }
      }
    });

    vm2.invoke("Unregister Interest", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        try {
          region.unregisterInterest("key-2");
        } catch (Exception ex) {
          fail("While registering interest: ", ex);
        }
      }
    });

    // Close cache server clients
    SerializableRunnable close = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    // Stop cache server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests interest list registration.
   */
  @Test
  public void test020InterestListRegistration() throws CacheException {
    final String name = this.getName();



    // Create cache server
    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        CacheLoader<Object, Object> cl = new CacheLoader<Object, Object>() {
          @Override
          public Object load(LoaderHelper helper) {
            return helper.getKey();
          }

          @Override
          public void close() {

          }
        };
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(cl, null);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });

    // Create cache server clients
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    SerializableRunnable create = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        // create bridge writer
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        createRegion(name, factory.create());
      }
    };

    vm1.invoke(create);
    vm2.invoke(create);

    // Get values for key 1 and key 6 so that there are entries in the clients.
    // Register interest in a list of keys.
    vm1.invoke("Create Entries and Register Interest", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        assertEquals(region.get("key-1"), "key-1");
        assertEquals(region.get("key-6"), "key-6");
        try {
          List<Object> list = new ArrayList<>();
          list.add("key-1");
          list.add("key-2");
          list.add("key-3");
          list.add("key-4");
          list.add("key-5");
          region.registerInterest(list);
        } catch (Exception ex) {
          fail("While registering interest: ", ex);
        }
      }
    });

    vm2.invoke("Create Entries and Register Interest", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        assertEquals(region.get("key-1"), "key-1");
        assertEquals(region.get("key-6"), "key-6");
      }
    });

    // Put new values and validate updates (VM2)
    vm2.invoke("Put New Values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        region.put("key-1", "vm2-key-1");
        region.put("key-6", "vm2-key-6");
        // Verify that no updates occurred to this region
        assertEquals(region.getEntry("key-1").getValue(), "vm2-key-1");
        assertEquals(region.getEntry("key-6").getValue(), "vm2-key-6");
      }
    });
    Wait.pause(5 * 1000);

    vm1.invoke("Validate Entries", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        // Verify that 'key-1' was updated
        assertEquals(region.getEntry("key-1").getValue(), "vm2-key-1");
        // Verify that 'key-6' was not invalidated
        assertEquals(region.getEntry("key-6").getValue(), "key-6");
      }
    });

    // Close cache server clients
    SerializableRunnable close = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    // Stop cache server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  static class ConnectionPoolDUnitTestSerializable2 implements java.io.Serializable {
    protected ConnectionPoolDUnitTestSerializable2(String key) {
      _key = key;
    }

    public String getKey() {
      return _key;
    }

    final String _key;
  }

  /**
   * Accessed by reflection DO NOT REMOVE
   */
  private static int getCacheServerPort() {
    return bridgeServerPort;
  }

  private static long getNumberOfAfterCreates() {
    return numberOfAfterCreates;
  }

  private static long getNumberOfAfterUpdates() {
    return numberOfAfterUpdates;
  }

  private static long getNumberOfAfterInvalidates() {
    return numberOfAfterInvalidates;
  }

  /**
   * Creates a "loner" distributed system that has dynamic region creation enabled.
   *
   * @since GemFire 4.3
   */
  private void createDynamicRegionCache(String testName, String connectionPoolName) {
    // note that clients use non-persistent dr factories.

    DynamicRegionFactory.get()
        .open(new DynamicRegionFactory.Config(null, connectionPoolName, false, true));
    logger.info("CREATED IT");
    getCache();
  }

  /**
   * A handy method to poll for arrival of non-null/non-invalid entries
   *
   * @param r the Region to poll
   * @param key the key of the Entry to poll for
   */
  private static void waitForEntry(final Region r, final Object key) {
    WaitCriterion ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return r.containsValueForKey(key);
      }

      @Override
      public String description() {
        return "Waiting for entry " + key + " on region " + r;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
  }

  private static Region waitForSubRegion(final Region r, final String subRegName) {
    // final long start = System.currentTimeMillis();
    WaitCriterion ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return r.getSubregion(subRegName) != null;
      }

      @Override
      public String description() {
        return "Waiting for subregion " + subRegName;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
    return r.getSubregion(subRegName);
  }

  static class CacheServerCacheLoader extends TestCacheLoader implements Declarable {

    CacheServerCacheLoader() {}

    @Override
    public Object load2(LoaderHelper helper) {
      if (helper.getArgument() instanceof Integer) {
        try {
          Thread.sleep((Integer) helper.getArgument());
        } catch (InterruptedException ugh) {
          fail("interrupted");
        }
      }
      return helper.getKey();
    }

    @Override
    public void init(Properties props) {}
  }

  /**
   * Create a server that has a value for every key queried and a unique key/value in the specified
   * Region that uniquely identifies each instance.
   *
   * @param vm the VM on which to create the server
   * @param rName the name of the Region to create on the server
   * @param port the TCP port on which the server should listen
   */
  public void createBridgeServer(VM vm, final String rName, final int port,
      final boolean notifyBySubscription) {
    vm.invoke("Create Region on Server", new CacheSerializableRunnable() {
      @Override
      public void run2() {
        try {
          AttributesFactory<Object, Object> factory = new AttributesFactory<>();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setConcurrencyChecksEnabled(false);
          factory.setCacheLoader(new CacheServerCacheLoader());
          beginCacheXml();
          createRegion(rName, factory.create());
          startBridgeServer(port);
          finishCacheXml(rName + "-" + port);

          Region<Object, Object> region = getRootRegion().getSubregion(rName);
          assertNotNull(region);
          assertNotNull(getRootRegion().getSubregion(rName));
          region.put("BridgeServer", port); // A unique key/value to identify the
          // BridgeServer
        } catch (Exception e) {
          getSystem().getLogWriter().severe(e);
          fail("Failed to start CacheServer " + e);
        }
      }
    });
  }

  // test for bug 35884
  @Test
  public void test021ClientGetOfInvalidServerEntry() throws CacheException {
    final String regionName1 = this.getName() + "-1";

    VM server1 = VM.getVM(0);
    VM client = VM.getVM(2);

    SerializableRunnable createServer = new CacheSerializableRunnable("Create Cache Server") {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.REPLICATE);
        factory.setConcurrencyChecksEnabled(false);
        createRegion(regionName1, factory.create());

        Wait.pause(1000);
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    };

    // Create server1.
    server1.invoke(createServer);

    final int port = server1.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());

    // Init values at server.
    server1.invoke("Create values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region1 = getRootRegion().getSubregion(regionName1);
        // create it invalid
        region1.create("key-string-1", null);
      }
    });

    // now try it with a local scope

    SerializableRunnable createPool2 = new CacheSerializableRunnable("Create region 2") {
      @Override
      public void run2() throws CacheException {
        // Region<Object, Object> region1 = getRootRegion().getSubregion(regionName1);
        // region1.localDestroyRegion();
        getLonerSystem();
        AttributesFactory<Object, Object> regionFactory = new AttributesFactory<>();
        regionFactory.setScope(Scope.LOCAL);
        regionFactory.setConcurrencyChecksEnabled(false);
        logger
            .info("ZZZZZ host0:" + host0 + " port:" + port);
        ClientServerTestCase.configureConnectionPool(regionFactory, host0, port, -1, false, -1, -1,
            null);
        logger
            .info("ZZZZZDone host0:" + host0 + " port:" + port);
        createRegion(regionName1, regionFactory.create());
      }
    };
    client.invoke(createPool2);

    // get the invalid entry on the client.
    client.invoke("get values on client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region1 = getRootRegion().getSubregion(regionName1);
        assertNull(region1.getEntry("key-string-1"));
        assertNull(region1.get("key-string-1"));
      }
    });

    server1.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));

  }

  @Test
  public void test022ClientRegisterUnregisterRequests() throws CacheException {
    final String regionName1 = this.getName() + "-1";

    VM server1 = VM.getVM(0);
    VM client = VM.getVM(2);

    SerializableRunnable createServer = new CacheSerializableRunnable("Create Cache Server") {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.REPLICATE);
        factory.setConcurrencyChecksEnabled(false);
        createRegion(regionName1, factory.create());

        // pause(1000);
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    };

    // Create server1.
    server1.invoke(createServer);

    final int port = server1.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());

    SerializableRunnable createPool = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();

        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);

        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1,
            null);

        Region<Object, Object> region1 = createRegion(regionName1, factory.create());
        region1.getAttributesMutator().addCacheListener(new CertifiableTestCacheListener(
            org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()));
      }
    };

    // Create client.
    client.invoke(createPool);

    // Init values at server.
    server1.invoke("Create values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region1 = getRootRegion().getSubregion(regionName1);
        for (int i = 0; i < 20; i++) {
          region1.put("key-string-" + i, "value-" + i);
        }
      }
    });

    // Put some values on the client.
    client.invoke("Put values client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region1 = getRootRegion().getSubregion(regionName1);

        for (int i = 0; i < 10; i++) {
          region1.put("key-string-" + i, "client-value-" + i);
        }
      }
    });

    SerializableRunnable closePool = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region1 = getRootRegion().getSubregion(regionName1);
        String pName = region1.getAttributes().getPoolName();
        region1.localDestroyRegion();
        PoolImpl p = (PoolImpl) PoolManager.find(pName);
        p.destroy();
      }
    };

    client.invoke(closePool);

    SerializableRunnable validateClientRegisterUnRegister =
        new CacheSerializableRunnable("validate Client Register UnRegister") {
          @Override
          public void run2() throws CacheException {
            for (CacheServer cacheServer : getCache().getCacheServers()) {
              CacheServerImpl bsi = (CacheServerImpl) cacheServer;
              final CacheClientNotifierStats ccnStats =
                  bsi.getAcceptor().getCacheClientNotifier().getStats();
              WaitCriterion ev = new WaitCriterion() {
                @Override
                public boolean done() {
                  return ccnStats.getClientRegisterRequests() == ccnStats
                      .getClientUnRegisterRequests();
                }

                @Override
                public String description() {
                  return null;
                }
              };
              GeodeAwaitility.await().untilAsserted(ev);
              assertEquals("HealthMonitor Client Register/UnRegister mismatch.",
                  ccnStats.getClientRegisterRequests(), ccnStats.getClientUnRegisterRequests());
            }
          }
        };

    server1.invoke(validateClientRegisterUnRegister);

    server1.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));

  }

  /**
   * Tests the containsKeyOnServer operation of the {@link Pool}
   *
   * @since GemFire 5.0.2
   */
  @Test
  public void test023ContainsKeyOnServer() throws CacheException {
    final String name = this.getName();



    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setConcurrencyChecksEnabled(false);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    SerializableRunnable create = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, false, -1, -1, null);
        createRegion(name, factory.create());
      }
    };
    vm1.invoke(create);
    vm2.invoke(create);

    final Integer key1 = 0;
    final String key2 = "0";
    vm2.invoke("Contains key on server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        boolean containsKey;
        containsKey = region.containsKeyOnServer(key1);
        assertFalse(containsKey);
        containsKey = region.containsKeyOnServer(key2);
        assertFalse(containsKey);
      }
    });

    vm1.invoke("Put values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.put(0, 0);
        region.put("0", "0");
      }
    });

    vm2.invoke("Contains key on server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        boolean containsKey;
        containsKey = region.containsKeyOnServer(key1);
        assertTrue(containsKey);
        containsKey = region.containsKeyOnServer(key2);
        assertTrue(containsKey);
      }
    });

    SerializableRunnable close = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };
    vm1.invoke(close);
    vm2.invoke(close);

    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests that invoking {@link Region#create} with a <code>null</code> value does the right thing
   * with the {@link Pool}.
   *
   * @since GemFire 3.5
   */
  @Test
  public void test024CreateNullValue() throws CacheException {
    final String name = this.getName();



    final Object createCallbackArg = "CREATE CALLBACK ARG";

    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    SerializableRunnable create = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);

        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        createRegion(name, factory.create());
      }
    };
    vm1.invoke(create);

    vm2.invoke(create);
    vm2.invoke("Create nulls", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          region.create(i, null, createCallbackArg);
        }
      }
    });

    Wait.pause(1000); // Wait for updates to be propagated

    vm2.invoke("Verify invalidates", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Region.Entry entry = region.getEntry(i);
          assertNotNull(entry);
          assertNull(entry.getValue());
        }
      }
    });

    vm1.invoke("Attempt to create values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          region.create(i, "new" + i);
        }
      }
    });

    Wait.pause(1000); // Wait for updates to be propagated

    vm2.invoke("Verify invalidates", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          Region.Entry entry = region.getEntry(i);
          assertNotNull(entry);
          assertNull(entry.getValue());
        }
      }
    });

    SerializableRunnable close = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests that a {@link Region#localDestroy} is not propagated to the server and that a {@link
   * Region#destroy} is. Also makes sure that callback arguments are passed correctly.
   */
  @Test
  public void test025Destroy() throws CacheException {
    final String name = this.getName();



    final Object callbackArg = "DESTROY CALLBACK";

    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {

        CacheWriter<Object, Object> cw = new TestCacheWriter<Object, Object>() {
          @Override
          public void beforeCreate2(EntryEvent event) throws CacheWriterException {

          }

          @Override
          public void beforeDestroy2(EntryEvent event) throws CacheWriterException {
            Object beca = event.getCallbackArgument();
            assertEquals(callbackArg, beca);
          }
        };
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, cw);
        createRegion(name, factory.create());
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });

    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    SerializableRunnable create = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);

        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);

        Region rgn = createRegion(name, factory.create());
        rgn.registerInterestRegex(".*", false, false);
      }
    };
    vm1.invoke(create);
    vm1.invoke("Populate region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          region.put(i, String.valueOf(i));
        }
      }
    });

    vm2.invoke(create);
    vm2.invoke("Load region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          assertEquals(String.valueOf(i), region.get(i));
        }
      }
    });

    vm1.invoke("Local destroy", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          region.localDestroy(i);
        }
      }
    });

    vm2.invoke("No destroy propagate", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          assertEquals(String.valueOf(i), region.get(i));
        }
      }
    });

    vm1.invoke("Fetch from server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          assertEquals(String.valueOf(i), region.get(i));
        }
      }
    });

    vm0.invoke("Check no server cache writer", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        TestCacheWriter writer = getTestWriter(region);
        writer.wasInvoked();
      }
    });

    vm1.invoke("Distributed destroy", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          region.destroy(i, callbackArg);
        }
      }
    });
    Wait.pause(1000); // Wait for destroys to propagate

    vm1.invoke("Attempt get from server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          assertNull(region.getEntry(i));
        }
      }
    });

    vm2.invoke("Validate destroy propagate", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          assertNull(region.getEntry(i));
        }
      }
    });

    SerializableRunnable close = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests that a {@link Region#localDestroyRegion} is not propagated to the server and that a
   * {@link Region#destroyRegion} is. Also makes sure that callback arguments are passed correctly.
   */
  @Ignore("TODO")
  @Test
  public void testDestroyRegion() throws CacheException {
    final String name = this.getName();



    final Object callbackArg = "DESTROY CALLBACK";

    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {

        CacheWriter<Object, Object> cw = new TestCacheWriter<Object, Object>() {
          @Override
          public void beforeCreate2(EntryEvent event) throws CacheWriterException {

          }

          @Override
          public void beforeRegionDestroy2(RegionEvent event) throws CacheWriterException {

            assertEquals(callbackArg, event.getCallbackArgument());
          }
        };
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, cw);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    SerializableRunnable create = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        createRegion(name, factory.create());
      }
    };

    vm1.invoke(create);
    vm2.invoke(create);

    vm1.invoke("Local destroy region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
        assertNull(getRootRegion().getSubregion(name));
        // close the bridge writer to prevent callbacks on the connections
        // Not necessary since locally destroying the region takes care of this.
        // getPoolClient(region).close();
      }
    });

    vm2.invoke("No destroy propagate", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        assertNotNull(region);
      }
    });

    vm0.invoke("Check no server cache writer", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        TestCacheWriter writer = getTestWriter(region);
        writer.wasInvoked();
      }
    });

    vm1.invoke(create);

    vm1.invoke("Distributed destroy region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        assertNotNull(region);
        region.destroyRegion(callbackArg);
        assertNull(getRootRegion().getSubregion(name));
        // close the bridge writer to prevent callbacks on the connections
        // Not necessary since locally destroying the region takes care of this.
        // getPoolClient(region).close();
      }
    });
    Wait.pause(1000); // Wait for destroys to propagate

    vm2.invoke("Verify destroy propagate", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        assertNull(region);
        // todo close the bridge writer
        // Not necessary since locally destroying the region takes care of this.
      }
    });

    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));

  }

  /**
   * Tests interest list registration with callback arg with DataPolicy.EMPTY and
   * InterestPolicy.ALL
   */
  @Test
  public void test026DPEmptyInterestListRegistrationWithCallbackArg() throws CacheException {
    final String name = this.getName();



    // Create cache server
    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        CacheLoader<Object, Object> cl = new CacheLoader<Object, Object>() {
          @Override
          public Object load(LoaderHelper helper) {
            return helper.getKey();
          }

          @Override
          public void close() {

          }
        };
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(cl, null);
        createRegion(name, factory.create());
        Wait.pause(1000);
        try {
          startBridgeServer(0);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });

    // Create cache server clients
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    SerializableRunnable create = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        // create bridge writer
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        factory.addCacheListener(new ControlListener());
        factory.setDataPolicy(DataPolicy.EMPTY);
        factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
        createRegion(name, factory.create());
      }
    };
    SerializableRunnable createPublisher =
        new CacheSerializableRunnable("Create publisher region") {
          @Override
          public void run2() throws CacheException {
            getLonerSystem();
            AttributesFactory<Object, Object> factory = new AttributesFactory<>();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            // create bridge writer
            ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1,
                null);
            factory.addCacheListener(new ControlListener());
            factory.setDataPolicy(DataPolicy.EMPTY); // make sure empty works with client publishers
            createRegion(name, factory.create());
          }
        };

    vm1.invoke(create);
    vm2.invoke(createPublisher);

    // VM1 Register interest
    vm1.invoke("Create Entries and Register Interest", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        try {
          // This call will cause no value to be put into the region
          region.registerInterest("key-1", InterestResultPolicy.NONE);
        } catch (Exception ex) {
          fail("While registering interest: ", ex);
        }
      }
    });

    // VM2 Put entry (this will cause a create event in both VM1 and VM2)
    vm2.invoke("Put Value", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        region.create("key-1", "key-1-create", "key-1-create");
      }
    });

    // VM2 Put entry (this will cause an update event in both VM1 and VM2)
    vm2.invoke("Put Value", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        region.put("key-1", "key-1-update", "key-1-update");
      }
    });

    // VM2 Destroy entry (this will cause a destroy event)
    vm2.invoke("Destroy Entry", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        region.destroy("key-1", "key-1-destroy");
      }
    });

    final SerializableRunnable assertEvents = new CacheSerializableRunnable("Verify events") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        ControlListener listener = (ControlListener) region.getAttributes().getCacheListeners()[0];
        int eventCount = 3;
        listener.waitWhileNotEnoughEvents(60000, eventCount);
        assertEquals(eventCount, listener.events.size());

        {
          EventWrapper ew = listener.events.get(0);
          assertEquals(TYPE_CREATE, ew.type);
          Object key = "key-1";
          assertEquals(key, ew.event.getKey());
          assertNull(ew.event.getOldValue());
          assertFalse(ew.event.isOldValueAvailable()); // failure
          assertEquals("key-1-create", ew.event.getNewValue());
          assertEquals(Operation.CREATE, ew.event.getOperation());
          assertEquals("key-1-create", ew.event.getCallbackArgument());
          assertTrue(ew.event.isOriginRemote());

          ew = listener.events.get(1);
          assertEquals(TYPE_UPDATE, ew.type);
          assertEquals(key, ew.event.getKey());
          assertNull(ew.event.getOldValue());
          assertFalse(ew.event.isOldValueAvailable());
          assertEquals("key-1-update", ew.event.getNewValue());
          assertEquals(Operation.UPDATE, ew.event.getOperation());
          assertEquals("key-1-update", ew.event.getCallbackArgument());
          assertTrue(ew.event.isOriginRemote());

          ew = listener.events.get(2);
          assertEquals(TYPE_DESTROY, ew.type);
          assertEquals("key-1-destroy", ew.arg);
          assertEquals(key, ew.event.getKey());
          assertNull(ew.event.getOldValue());
          assertFalse(ew.event.isOldValueAvailable());
          assertNull(ew.event.getNewValue());
          assertEquals(Operation.DESTROY, ew.event.getOperation());
          assertEquals("key-1-destroy", ew.event.getCallbackArgument());
          assertTrue(ew.event.isOriginRemote());
        }
      }
    };
    vm1.invoke(assertEvents);

    // Close cache server clients
    SerializableRunnable close = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    // Stop cache server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests interest list registration with callback arg with DataPolicy.EMPTY and
   * InterestPolicy.CACHE_CONTENT
   */
  @Test
  public void test027DPEmptyCCInterestListRegistrationWithCallbackArg() throws CacheException {
    final String name = this.getName();



    // Create cache server
    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        CacheLoader<Object, Object> cl = new CacheLoader<Object, Object>() {
          @Override
          public Object load(LoaderHelper helper) {
            return helper.getKey();
          }

          @Override
          public void close() {

          }
        };
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(cl, null);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });

    // Create cache server clients
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    SerializableRunnable create = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        factory.setCacheListener(new ControlListener());
        factory.setDataPolicy(DataPolicy.EMPTY);
        factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT));
        createRegion(name, factory.create());
      }
    };
    SerializableRunnable createPublisher =
        new CacheSerializableRunnable("Create publisher region") {
          @Override
          public void run2() throws CacheException {
            getLonerSystem();
            AttributesFactory<Object, Object> factory = new AttributesFactory<>();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            // create bridge writer
            ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1,
                null);
            factory.setCacheListener(new ControlListener());
            factory.setDataPolicy(DataPolicy.EMPTY); // make sure empty works with client publishers
            createRegion(name, factory.create());
          }
        };

    vm1.invoke(create);
    vm2.invoke(createPublisher);

    // VM1 Register interest
    vm1.invoke("Create Entries and Register Interest", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        try {
          // This call will cause no value to be put into the region
          region.registerInterest("key-1", InterestResultPolicy.NONE);
        } catch (Exception ex) {
          fail("While registering interest: ", ex);
        }
      }
    });

    // VM2 Put entry (this will cause a create event in both VM1 and VM2)
    vm2.invoke("Put Value", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        region.create("key-1", "key-1-create", "key-1-create");
      }
    });

    // VM2 Put entry (this will cause an update event in both VM1 and VM2)
    vm2.invoke("Put Value", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        region.put("key-1", "key-1-update", "key-1-update");
      }
    });

    // VM2 Destroy entry (this will cause a destroy event)
    vm2.invoke("Destroy Entry", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        region.destroy("key-1", "key-1-destroy");
      }
    });

    final SerializableRunnable assertEvents = new CacheSerializableRunnable("Verify events") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        ControlListener listener = (ControlListener) region.getAttributes().getCacheListeners()[0];
        Wait.pause(1000); // we should not get any events but give some time for the server to send
        // them
        assertEquals(0, listener.events.size());
      }
    };
    vm1.invoke(assertEvents);

    // Close cache server clients
    SerializableRunnable close = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    // Stop cache server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Test dynamic region creation instantiated from a bridge client causing regions to be created on
   * two different cache servers.
   * <p>
   * Also tests the reverse situation, a dynamic region is created on the cache server expecting the
   * same region to be created on the client.
   * <p>
   * Note: This test re-creates Distributed Systems for its own purposes and uses a Loner
   * distributed systems to isolate the Bridge Client.
   */
  @Test
  public void test028DynamicRegionCreation() {
    final String name = this.getName();

    final VM client1 = VM.getVM(0);
    final VM srv1 = VM.getVM(2);
    final VM srv2 = VM.getVM(3);

    final String k1 = name + "-key1";
    final String v1 = name + "-val1";
    final String k2 = name + "-key2";
    final String v2 = name + "-val2";
    final String k3 = name + "-key3";
    final String v3 = name + "-val3";

    client1.invoke(JUnit4DistributedTestCase::disconnectFromDS);
    srv1.invoke(JUnit4DistributedTestCase::disconnectFromDS);
    srv2.invoke(JUnit4DistributedTestCase::disconnectFromDS);
    try {
      // setup servers
      CacheSerializableRunnable ccs = new CacheSerializableRunnable("Create Cache Server") {
        @Override
        public void run2() throws CacheException {
          createDynamicRegionCache(name, null); // Creates a new DS and Cache
          assertTrue(DynamicRegionFactory.get().isOpen());
          try {
            startBridgeServer(0);
          } catch (IOException ugh) {
            fail("cache server startup failed");
          }
          AttributesFactory<Object, Object> factory = new AttributesFactory<>();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);
          factory.setConcurrencyChecksEnabled(false);
          Region<Object, Object> region = createRootRegion(name, factory.create());
          region.put(k1, v1);
          Assert.assertTrue(region.get(k1).equals(v1));
        }
      };
      srv1.invoke(ccs);
      srv2.invoke(ccs);

      final String srv1Host = NetworkUtils.getServerHostName(srv1.getHost());
      final int srv1Port = srv1.invoke(ConnectionPoolDUnitTest::getCacheServerPort);

      final int srv2Port = srv2.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
      // final String srv2Host = getServerHostName(srv2.getHost());

      // setup clients, do basic tests to make sure pool with notifier work as advertised
      client1.invoke("Create Cache Client", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          createLonerDS();
          AttributesFactory<Object, Object> factory = new AttributesFactory<>();
          factory.setConcurrencyChecksEnabled(false);
          Pool cp = ClientServerTestCase.configureConnectionPool(factory, srv1Host, srv1Port,
              srv2Port, true, -1, -1, null);
          {
            final PoolImpl pool = (PoolImpl) cp;
            WaitCriterion ev = new WaitCriterion() {
              @Override
              public boolean done() {
                if (pool.getPrimary() == null) {
                  return false;
                }
                return pool.getRedundants().size() >= 1;
              }

              @Override
              public String description() {
                return null;
              }
            };
            GeodeAwaitility.await().untilAsserted(ev);
            assertNotNull(pool.getPrimary());
            assertTrue("backups=" + pool.getRedundants() + " expected=" + 1,
                pool.getRedundants().size() >= 1);
          }

          createDynamicRegionCache(name, "testPool");

          assertTrue(DynamicRegionFactory.get().isOpen());
          factory.setScope(Scope.LOCAL);
          factory.setConcurrencyChecksEnabled(false);
          factory.setCacheListener(new CertifiableTestCacheListener(
              org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()));
          Region<Object, Object> region = createRootRegion(name, factory.create());


          assertNull(region.getEntry(k1));
          region.registerInterestRegex(".*", InterestResultPolicy.KEYS_VALUES); // this should match
          // the key
          assertEquals(v1, region.getEntry(k1).getValue()); // Update via registered interest

          assertNull(region.getEntry(k2));
          region.put(k2, v2); // use the Pool
          assertEquals(v2, region.getEntry(k2).getValue()); // Ensure that the notifier didn't un-do
          // the put, bug 35355

          region.put(k3, v3); // setup a key for invalidation from a notifier
        }
      });

      srv1.invoke("Validate Server1 update", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          CacheClientNotifier ccn = getInstance();
          final CacheClientNotifierStats ccnStats = ccn.getStats();
          final int eventCount = ccnStats.getEvents();
          Region<Object, Object> r = getRootRegion(name);
          assertNotNull(r);
          assertEquals(v2, r.getEntry(k2).getValue()); // Validate the Pool worked, getEntry works
          // because of the mirror
          assertEquals(v3, r.getEntry(k3).getValue()); // Make sure we have the other entry to use
          // for notification
          r.put(k3, v1); // Change k3, sending some data to the client notifier

          // Wait for the update to propagate to the clients
          WaitCriterion ev = new WaitCriterion() {
            @Override
            public boolean done() {
              return ccnStats.getEvents() > eventCount;
            }

            @Override
            public String description() {
              return "waiting for ccnStat";
            }
          };
          GeodeAwaitility.await().untilAsserted(ev);
        }
      });
      srv2.invoke("Validate Server2 update", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          Region<Object, Object> r = getRootRegion(name);
          assertNotNull(r);
          assertEquals(v2, r.getEntry(k2).getValue()); // Validate the Pool worked, getEntry works
          // because of the mirror
          assertEquals(v1, r.getEntry(k3).getValue()); // From peer update
        }
      });
      client1.invoke("Validate Client notification", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          Region<Object, Object> r = getRootRegion(name);
          assertNotNull(r);
          CertifiableTestCacheListener ctl =
              (CertifiableTestCacheListener) r.getAttributes().getCacheListener();
          ctl.waitForUpdated(k3);
          assertEquals(v1, r.getEntry(k3).getValue()); // Ensure that the notifier updated the entry
        }
      });
      // Ok, now we are ready to do some dynamic region action!
      final String v1Dynamic = v1 + "dynamic";
      final String dynFromClientName = name + "-dynamic-client";
      final String dynFromServerName = name + "-dynamic-server";
      client1.invoke("Client dynamic region creation", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          assertTrue(DynamicRegionFactory.get().isOpen());
          Region<Object, Object> r = getRootRegion(name);
          assertNotNull(r);
          Region<Object, Object> dr =
              DynamicRegionFactory.get().createDynamicRegion(name, dynFromClientName);
          assertNull(dr.get(k1)); // This should be enough to validate the creation on the server
          dr.put(k1, v1Dynamic);
          assertEquals(v1Dynamic, dr.getEntry(k1).getValue());
        }
      });

      // Assert the servers have the dynamic region and the new value
      CacheSerializableRunnable valDR =
          new CacheSerializableRunnable("Validate dynamic region creation on server") {
            @Override
            public void run2() throws CacheException {
              Region<Object, Object> r = getRootRegion(name);
              assertNotNull(r);
              long end = System.currentTimeMillis() + 10000;
              Region dr;
              for (;;) {
                try {
                  dr = r.getSubregion(dynFromClientName);
                  assertNotNull(dr);
                  assertNotNull(getCache().getRegion(name + Region.SEPARATOR + dynFromClientName));
                  break;
                } catch (AssertionError e) {
                  if (System.currentTimeMillis() > end) {
                    throw e;
                  }
                }
              }

              assertEquals(v1Dynamic, dr.getEntry(k1).getValue());
            }
          };
      srv1.invoke(valDR);
      srv2.invoke(valDR);
      // now delete the dynamic region and see if it goes away on servers
      client1.invoke("Client dynamic region destruction", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          assertTrue(DynamicRegionFactory.get().isActive());
          Region<Object, Object> r = getRootRegion(name);
          assertNotNull(r);
          String drName = r.getFullPath() + Region.SEPARATOR + dynFromClientName;

          assertNotNull(getCache().getRegion(drName));
          DynamicRegionFactory.get().destroyDynamicRegion(drName);
          assertNull(getCache().getRegion(drName));
        }
      });
      // Assert the servers no longer have the dynamic region
      CacheSerializableRunnable valNoDR =
          new CacheSerializableRunnable("Validate dynamic region destruction on server") {
            @Override
            public void run2() throws CacheException {
              Region<Object, Object> r = getRootRegion(name);
              assertNotNull(r);
              String drName = r.getFullPath() + Region.SEPARATOR + dynFromClientName;
              assertNull(getCache().getRegion(drName));
              try {
                DynamicRegionFactory.get().destroyDynamicRegion(drName);
                fail("expected RegionDestroyedException");
              } catch (RegionDestroyedException ignored) {
              }
            }
          };
      srv1.invoke(valNoDR);
      srv2.invoke(valNoDR);
      // Now try the reverse, create a dynamic region on the server and see if the client
      // has it
      srv2.invoke("Server dynamic region creation", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          Region<Object, Object> r = getRootRegion(name);
          assertNotNull(r);
          Region<Object, Object> dr =
              DynamicRegionFactory.get().createDynamicRegion(name, dynFromServerName);
          assertNull(dr.get(k1));
          dr.put(k1, v1Dynamic);
          assertEquals(v1Dynamic, dr.getEntry(k1).getValue());
        }
      });
      // Assert the servers have the dynamic region and the new value
      srv1.invoke(new CacheSerializableRunnable(
          "Validate dynamic region creation propagation to other server") {
        @Override
        public void run2() throws CacheException {
          Region<Object, Object> r = getRootRegion(name);
          assertNotNull(r);
          Region<Object, Object> dr = waitForSubRegion(r, dynFromServerName);
          assertNotNull(dr);
          assertNotNull(getCache().getRegion(name + Region.SEPARATOR + dynFromServerName));
          waitForEntry(dr, k1);
          assertNotNull(dr.getEntry(k1));
          assertEquals(v1Dynamic, dr.getEntry(k1).getValue());
        }
      });
      // Assert the clients have the dynamic region and the new value
      client1.invoke("Validate dynamic region creation on client", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          Region<Object, Object> r = getRootRegion(name);
          assertNotNull(r);
          long end = System.currentTimeMillis() + 10000;
          Region<Object, Object> dr;
          for (;;) {
            try {
              dr = r.getSubregion(dynFromServerName);
              assertNotNull(dr);
              assertNotNull(getCache().getRegion(name + Region.SEPARATOR + dynFromServerName));
              break;
            } catch (AssertionError e) {
              if (System.currentTimeMillis() > end) {
                throw e;
              } else {
                Wait.pause(1000);
              }
            }
          }
          waitForEntry(dr, k1);
          assertNotNull(dr.getEntry(k1));
          assertEquals(v1Dynamic, dr.getEntry(k1).getValue());
        }
      });
      // now delete the dynamic region on a server and see if it goes away on client
      srv2.invoke("Server dynamic region destruction", new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          assertTrue(DynamicRegionFactory.get().isActive());
          Region<Object, Object> r = getRootRegion(name);
          assertNotNull(r);
          String drName = r.getFullPath() + Region.SEPARATOR + dynFromServerName;

          assertNotNull(getCache().getRegion(drName));
          DynamicRegionFactory.get().destroyDynamicRegion(drName);
          assertNull(getCache().getRegion(drName));
        }
      });
      srv1.invoke(
          new CacheSerializableRunnable("Validate dynamic region destruction on other server") {
            @Override
            public void run2() throws CacheException {
              Region<Object, Object> r = getRootRegion(name);
              assertNotNull(r);
              String drName = r.getFullPath() + Region.SEPARATOR + dynFromServerName;
              {
                int retry = 100;
                while (retry-- > 0 && getCache().getRegion(drName) != null) {
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException ignore) {
                    fail("interrupted");
                  }
                }
              }
              assertNull(getCache().getRegion(drName));
            }
          });
      // Assert the clients no longer have the dynamic region
      client1
          .invoke("Validate dynamic region destruction on client", new CacheSerializableRunnable() {
            @Override
            public void run2() throws CacheException {
              Region<Object, Object> r = getRootRegion(name);
              assertNotNull(r);
              String drName = r.getFullPath() + Region.SEPARATOR + dynFromServerName;
              {
                int retry = 100;
                while (retry-- > 0 && getCache().getRegion(drName) != null) {
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException ignore) {
                    fail("interrupted");
                  }
                }
              }
              assertNull(getCache().getRegion(drName));
              // sleep to make sure that the dynamic region entry from the internal
              // region,dynamicRegionList in DynamicRegionFactory // ?
              try {
                Thread.sleep(10000);
              } catch (InterruptedException ignore) {
                fail("interrupted");
              }
              try {
                DynamicRegionFactory.get().destroyDynamicRegion(drName);
                fail("expected RegionDestroyedException");
              } catch (RegionDestroyedException ignored) {
              }
            }
          });
    } finally {
      client1.invoke(JUnit4DistributedTestCase::disconnectFromDS); // clean-up loner
      srv1.invoke(JUnit4DistributedTestCase::disconnectFromDS);
      srv2.invoke(JUnit4DistributedTestCase::disconnectFromDS);
    }
  }


  /**
   * Test for bug 36279
   */
  @Test
  public void test029EmptyByteArray() throws CacheException {
    final String name = this.getName();

    final Object createCallbackArg = "CREATE CALLBACK ARG";

    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    SerializableRunnable create = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);

        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        createRegion(name, factory.create());
      }
    };

    vm1.invoke(create);
    vm1.invoke("Create empty byte array", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 1; i++) {
          region.create(i, new byte[0], createCallbackArg);
        }
      }
    });

    vm1.invoke("Verify values on client", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 1; i++) {
          Region.Entry entry = region.getEntry(i);
          assertNotNull(entry);
          byte[] value = (byte[]) entry.getValue();
          assertNotNull(value);
          assertEquals(0, value.length);
        }
      }
    });
    vm0.invoke("Verify values on server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 1; i++) {
          Region.Entry entry = region.getEntry(i);
          assertNotNull(entry);
          byte[] value = (byte[]) entry.getValue();
          assertNotNull(value);
          assertEquals(0, value.length);
        }
      }
    });

    SerializableRunnable close = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);

    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests interest list registration with callback arg
   */
  @Test
  public void test030InterestListRegistrationWithCallbackArg() throws CacheException {
    final String name = this.getName();

    // Create cache server
    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        CacheLoader<Object, Object> cl = new CacheLoader<Object, Object>() {
          @Override
          public Object load(LoaderHelper helper) {
            return helper.getKey();
          }

          @Override
          public void close() {

          }
        };
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(cl, null);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });

    // Create cache server clients
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    SerializableRunnable create = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        // create bridge writer
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        factory.setCacheListener(new ControlListener());
        createRegion(name, factory.create());
      }
    };

    vm1.invoke(create);
    vm2.invoke(create);

    // VM1 Register interest
    vm1.invoke("Create Entries and Register Interest", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        try {
          // This call will cause no value to be put into the region
          region.registerInterest("key-1", InterestResultPolicy.NONE);
        } catch (Exception ex) {
          fail("While registering interest: ", ex);
        }
      }
    });

    // VM2 Put entry (this will cause a create event in both VM1 and VM2)
    vm2.invoke("Put Value", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        region.create("key-1", "key-1-create", "key-1-create");
      }
    });

    // VM2 Put entry (this will cause an update event in both VM1 and VM2)
    vm2.invoke("Put Value", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        region.put("key-1", "key-1-update", "key-1-update");
      }
    });

    // VM2 Destroy entry (this will cause a destroy event)
    vm2.invoke("Destroy Entry", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        region.destroy("key-1", "key-1-destroy");
      }
    });

    final SerializableRunnable assertEvents = new CacheSerializableRunnable("Verify events") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        ControlListener listener = (ControlListener) region.getAttributes().getCacheListeners()[0];
        int eventCount = 3;
        listener.waitWhileNotEnoughEvents(60000, eventCount);
        assertEquals(eventCount, listener.events.size());

        {
          EventWrapper ew = listener.events.get(0);
          assertEquals(ew.type, TYPE_CREATE);
          Object key = "key-1";
          assertEquals(key, ew.event.getKey());
          assertNull(ew.event.getOldValue());
          assertEquals("key-1-create", ew.event.getNewValue());
          assertEquals(Operation.CREATE, ew.event.getOperation());
          assertEquals("key-1-create", ew.event.getCallbackArgument());
          assertTrue(ew.event.isOriginRemote());

          ew = listener.events.get(1);
          assertEquals(ew.type, TYPE_UPDATE);
          assertEquals(key, ew.event.getKey());
          assertEquals("key-1-create", ew.event.getOldValue());
          assertEquals("key-1-update", ew.event.getNewValue());
          assertEquals(Operation.UPDATE, ew.event.getOperation());
          assertEquals("key-1-update", ew.event.getCallbackArgument());
          assertTrue(ew.event.isOriginRemote());

          ew = listener.events.get(2);
          assertEquals(ew.type, TYPE_DESTROY);
          assertEquals("key-1-destroy", ew.arg);
          assertEquals(key, ew.event.getKey());
          assertEquals("key-1-update", ew.event.getOldValue());
          assertNull(ew.event.getNewValue());
          assertEquals(Operation.DESTROY, ew.event.getOperation());
          assertEquals("key-1-destroy", ew.event.getCallbackArgument());
          assertTrue(ew.event.isOriginRemote());
        }
      }
    };
    vm1.invoke(assertEvents);

    // Close cache server clients
    SerializableRunnable close = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    // Stop cache server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests the keySetOnServer operation of the {@link Pool}
   *
   * @since GemFire 5.0.2
   */
  @Test
  public void test031KeySetOnServer() throws CacheException {
    final String name = this.getName();

    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setConcurrencyChecksEnabled(false);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    SerializableRunnable create = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        createRegion(name, factory.create());
      }
    };
    vm1.invoke(create);
    vm2.invoke(create);

    vm2.invoke("Get keys on server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        Set keySet = region.keySetOnServer();
        assertNotNull(keySet);
        assertEquals(0, keySet.size());
      }
    });

    vm1.invoke("Put values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          region.put(i, i);
        }
      }
    });

    vm2.invoke("Get keys on server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        Set keySet = region.keySetOnServer();
        assertNotNull(keySet);
        assertEquals(10, keySet.size());
      }
    });

    SerializableRunnable close = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };
    vm1.invoke(close);
    vm2.invoke(close);

    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests that creating, putting and getting a non-serializable key or value throws the correct
   * (NotSerializableException) exception.
   */
  @Test
  public void test033NotSerializableException() throws CacheException {
    final String name = this.getName();

    vm0.invoke("Create Cache Server", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    SerializableRunnable create = new CacheSerializableRunnable("Create region") {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);

        ClientServerTestCase.configureConnectionPool(factory, host0, port, -1, true, -1, -1, null);
        createRegion(name, factory.create());
      }
    };
    vm1.invoke(create);

    vm1.invoke("Attempt to create a non-serializable value", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        try {
          region.create(1, new ConnectionPoolTestNonSerializable());
          fail("Should not have been able to create a ConnectionPoolTestNonSerializable");
        } catch (Exception e) {
          if (!(e.getCause() instanceof java.io.NotSerializableException)) {
            fail("Unexpected exception while creating a non-serializable value " + e);
          }
        }
      }
    });

    vm1.invoke("Attempt to put a non-serializable value", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        try {
          region.put(1, new ConnectionPoolTestNonSerializable());
          fail("Should not have been able to put a ConnectionPoolTestNonSerializable");
        } catch (Exception e) {
          if (!(e.getCause() instanceof java.io.NotSerializableException)) {
            fail("Unexpected exception while putting a non-serializable value " + e);
          }
        }
      }
    });

    vm1.invoke("Attempt to get a non-serializable key", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        try {
          region.get(new ConnectionPoolTestNonSerializable());
          fail("Should not have been able to get a ConnectionPoolTestNonSerializable");
        } catch (Exception e) {
          if (!(e.getCause() instanceof java.io.NotSerializableException)) {
            fail("Unexpected exception while getting a non-serializable key " + e);
          }
        }
      }
    });

    SerializableRunnable close = new CacheSerializableRunnable("Close Pool") {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);

    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  static class ConnectionPoolTestNonSerializable {
    ConnectionPoolTestNonSerializable() {}
  }

  /**
   * Tests 'notify-all' client updates. This test verifies that: - only invalidates are sent as part
   * of the 'notify-all' mode of client updates - originators of updates are not sent invalidates -
   * non-originators of updates are sent invalidates - multiple invalidates are not sent for the
   * same update
   */
  @Test
  public void test034NotifyAllUpdates() throws CacheException {
    final String name = this.getName();

    disconnectAllFromDS();

    // Create the cache servers with distributed, mirrored region
    SerializableRunnable createServer = new CacheSerializableRunnable("Create Cache Server") {
      @Override
      public void run2() throws CacheException {
        CacheLoader<Object, Object> cl = new CacheLoader<Object, Object>() {
          @Override
          public Object load(LoaderHelper helper) {
            return helper.getKey();
          }

          @Override
          public void close() {

          }
        };
        AttributesFactory<Object, Object> factory =
            getBridgeServerMirroredAckRegionAttributes(cl, null);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    };
    getSystem().getLogWriter().info("before create server");
    vm0.invoke(createServer);
    vm1.invoke(createServer);

    // Create cache server clients
    final int numberOfKeys = 10;
    final String host0 = NetworkUtils.getServerHostName();
    final int vm0Port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final int vm1Port = vm1.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    SerializableRunnable createClient =
        new CacheSerializableRunnable("Create Cache Server Client") {
          @Override
          public void run2() throws CacheException {
            // reset all static listener variables in case this is being rerun in a subclass
            numberOfAfterInvalidates = 0;
            numberOfAfterCreates = 0;
            numberOfAfterUpdates = 0;
            getLonerSystem();
            // create the region
            AttributesFactory<Object, Object> factory = new AttributesFactory<>();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            // create bridge writer
            ClientServerTestCase.configureConnectionPool(factory, host0, vm0Port, vm1Port, true, -1,
                -1, null);
            createRegion(name, factory.create());
          }
        };
    getSystem().getLogWriter().info("before create client");
    vm2.invoke(createClient);
    vm3.invoke(createClient);

    // Initialize each client with entries (so that afterInvalidate is called)
    SerializableRunnable initializeClient = new CacheSerializableRunnable("Initialize Client") {
      @Override
      public void run2() throws CacheException {
        numberOfAfterInvalidates = 0;
        numberOfAfterCreates = 0;
        numberOfAfterUpdates = 0;
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        for (int i = 0; i < numberOfKeys; i++) {
          assertEquals("key-" + i, region.get("key-" + i));
        }
      }
    };
    getSystem().getLogWriter().info("before initialize client");
    vm2.invoke(initializeClient);
    vm3.invoke(initializeClient);

    // Add a CacheListener to both vm2 and vm3
    vm2.invoke("Add CacheListener 1", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        CacheListener listener = new CacheListenerAdapter() {
          @Override
          public void afterCreate(EntryEvent e) {
            numberOfAfterCreates++;
            logger
                .info("vm2 numberOfAfterCreates: " + numberOfAfterCreates);
          }

          @Override
          public void afterUpdate(EntryEvent e) {
            numberOfAfterUpdates++;
            logger
                .info("vm2 numberOfAfterUpdates: " + numberOfAfterUpdates);
          }

          @Override
          public void afterInvalidate(EntryEvent e) {
            numberOfAfterInvalidates++;
            logger
                .info("vm2 numberOfAfterInvalidates: " + numberOfAfterInvalidates);
          }
        };
        region.getAttributesMutator().addCacheListener(listener);
        region.registerInterestRegex(".*", false, false);
      }
    });

    vm3.invoke("Add CacheListener 2", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        CacheListener<Object, Object> listener = new CacheListenerAdapter<Object, Object>() {
          @Override
          public void afterCreate(EntryEvent e) {
            numberOfAfterCreates++;
          }

          @Override
          public void afterUpdate(EntryEvent e) {
            numberOfAfterUpdates++;
          }

          @Override
          public void afterInvalidate(EntryEvent e) {
            numberOfAfterInvalidates++;
          }
        };
        region.getAttributesMutator().addCacheListener(listener);
        region.registerInterestRegex(".*", false, false);
      }
    });

    Wait.pause(3000);

    getSystem().getLogWriter().info("before puts");
    // Use vm2 to put new values
    // This should cause 10 afterUpdates to vm2 and 10 afterInvalidates to vm3
    vm2.invoke("Put New Values", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        for (int i = 0; i < 10; i++) {
          region.put("key-" + i, "key-" + i);
        }
      }
    });
    getSystem().getLogWriter().info("after puts");

    // Wait to make sure all the updates are received
    Wait.pause(1000);

    long vm2AfterCreates = vm2.invoke(ConnectionPoolDUnitTest::getNumberOfAfterCreates);
    long vm2AfterUpdates = vm2.invoke(ConnectionPoolDUnitTest::getNumberOfAfterUpdates);
    long vm2AfterInvalidates =
        vm2.invoke(ConnectionPoolDUnitTest::getNumberOfAfterInvalidates);
    long vm3AfterCreates = vm3.invoke(ConnectionPoolDUnitTest::getNumberOfAfterCreates);
    long vm3AfterUpdates = vm3.invoke(ConnectionPoolDUnitTest::getNumberOfAfterUpdates);
    long vm3AfterInvalidates =
        vm3.invoke(ConnectionPoolDUnitTest::getNumberOfAfterInvalidates);
    logger
        .info("vm2AfterCreates: " + vm2AfterCreates);
    logger
        .info("vm2AfterUpdates: " + vm2AfterUpdates);
    logger
        .info("vm2AfterInvalidates: " + vm2AfterInvalidates);
    logger
        .info("vm3AfterCreates: " + vm3AfterCreates);
    logger
        .info("vm3AfterUpdates: " + vm3AfterUpdates);
    logger
        .info("vm3AfterInvalidates: " + vm3AfterInvalidates);

    assertEquals("VM2 should not have received any afterCreate messages", 0, vm2AfterCreates);
    assertEquals("VM2 should not have received any afterInvalidate messages", 0,
        vm2AfterInvalidates);
    assertEquals(
        "VM2 received " + vm2AfterUpdates + " afterUpdate messages. It should have received "
            + numberOfKeys,
        vm2AfterUpdates, numberOfKeys);

    assertEquals("VM3 should not have received any afterCreate messages", 0, vm3AfterCreates);
    assertEquals("VM3 should not have received any afterUpdate messages", 0, vm3AfterUpdates);
    assertEquals("VM3 received " + vm3AfterInvalidates
        + " afterInvalidate messages. It should have received " + numberOfKeys, vm3AfterInvalidates,
        numberOfKeys);
  }


  static class DelayListener extends CacheListenerAdapter {
    private final int delay;

    DelayListener(int delay) {
      this.delay = delay;
    }

    private void delay() {
      try {
        Thread.sleep(this.delay);
      } catch (InterruptedException ignore) {
        fail("interrupted");
      }
    }

    @Override
    public void afterCreate(EntryEvent event) {
      delay();
    }

    @Override
    public void afterDestroy(EntryEvent event) {
      delay();
    }

    @Override
    public void afterInvalidate(EntryEvent event) {
      delay();
    }

    @Override
    public void afterRegionDestroy(RegionEvent event) {
      delay();
    }

    @Override
    public void afterRegionCreate(RegionEvent event) {
      delay();
    }

    @Override
    public void afterRegionInvalidate(RegionEvent event) {
      delay();
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      delay();
    }

    @Override
    public void afterRegionClear(RegionEvent event) {
      delay();
    }

    @Override
    public void afterRegionLive(RegionEvent event) {
      delay();
    }
  }

  /**
   * Make sure a tx done in a server on an empty region gets sent to clients who have registered
   * interest.
   */
  @Test
  public void test037Bug39526part1() throws CacheException {
    final String name = this.getName();

    // Create the cache servers with distributed, empty region
    SerializableRunnable createServer = new CacheSerializableRunnable("Create Cache Server") {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.EMPTY);
        factory.setConcurrencyChecksEnabled(false);
        createRegion(name, factory.create());
        // pause(1000);
        try {
          startBridgeServer(0);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }
      }
    };
    getSystem().getLogWriter().info("before create server");
    vm0.invoke(createServer);

    // Create cache server client
    final String host0 = NetworkUtils.getServerHostName();
    final int vm0Port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    SerializableRunnable createClient =
        new CacheSerializableRunnable("Create Cache Server Client") {
          @Override
          public void run2() throws CacheException {
            getLonerSystem();
            // create the region
            AttributesFactory<Object, Object> factory = new AttributesFactory<>();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            // create bridge writer
            ClientServerTestCase.configureConnectionPool(factory, host0, vm0Port, -1, true, -1, -1,
                null);
            createRegion(name, factory.create());
            LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
            region.registerInterestRegex(".*");
          }
        };
    getSystem().getLogWriter().info("before create client");
    vm1.invoke(createClient);

    // now do a tx in the server
    SerializableRunnable doServerTx = new CacheSerializableRunnable("doServerTx") {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        Cache cache = getCache();
        CacheTransactionManager txmgr = cache.getCacheTransactionManager();
        txmgr.begin();
        try {
          region.put("k1", "v1");
          region.put("k2", "v2");
          region.put("k3", "v3");
        } finally {
          txmgr.commit();
        }
      }
    };
    getSystem().getLogWriter().info("before doServerTx");
    vm0.invoke(doServerTx);

    // now verify that the client receives the committed data
    SerializableRunnable validateClient =
        new CacheSerializableRunnable("Validate Cache Server Client") {
          @Override
          public void run2() throws CacheException {
            final LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
            // wait for a while for us to have the correct number of entries
            WaitCriterion ev = new WaitCriterion() {
              @Override
              public boolean done() {
                return region.size() == 3;
              }

              @Override
              public String description() {
                return "waiting for region to be size 3";
              }
            };
            GeodeAwaitility.await().untilAsserted(ev);
            // assertIndexDetailsEquals(3, region.size());
            assertTrue(region.containsKey("k1"));
            assertTrue(region.containsKey("k2"));
            assertTrue(region.containsKey("k3"));
            assertEquals("v1", region.getEntry("k1").getValue());
            assertEquals("v2", region.getEntry("k2").getValue());
            assertEquals("v3", region.getEntry("k3").getValue());
          }
        };
    getSystem().getLogWriter().info("before confirmCommitOnClient");
    vm1.invoke(validateClient);
  }

  /**
   * Now confirm that a tx done in a peer of a server (the server having an empty region and wanting
   * all events) sends the tx to its clients
   */
  @Test
  public void test038Bug39526part2() throws CacheException {
    disconnectAllFromDS();
    final String name = this.getName();

    // Create the cache servers with distributed, empty region
    SerializableRunnable createServer = new CacheSerializableRunnable("Create Cache Server") {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setConcurrencyChecksEnabled(false);
        factory.setDataPolicy(DataPolicy.EMPTY);
        factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
        createRegion(name, factory.create());
        try {
          startBridgeServer(0);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }
      }
    };
    getSystem().getLogWriter().info("before create server");
    vm0.invoke(createServer);

    // Create cache server client
    final String host0 = NetworkUtils.getServerHostName();
    final int vm0Port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    SerializableRunnable createClient =
        new CacheSerializableRunnable("Create Cache Server Client") {
          @Override
          public void run2() throws CacheException {
            getLonerSystem();
            // create the region
            AttributesFactory<Object, Object> factory = new AttributesFactory<>();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            // create bridge writer
            ClientServerTestCase.configureConnectionPool(factory, host0, vm0Port, -1, true, -1, -1,
                null);
            createRegion(name, factory.create());
            LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
            region.registerInterestRegex(".*");
          }
        };
    getSystem().getLogWriter().info("before create client");
    vm1.invoke(createClient);

    SerializableRunnable createServerPeer = new CacheSerializableRunnable("Create Server Peer") {
      @Override
      public void run2() throws CacheException {
        AttributesFactory<Object, Object> factory = new AttributesFactory<>();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.EMPTY);
        factory.setConcurrencyChecksEnabled(false);
        createRegion(name, factory.create());
      }
    };
    getSystem().getLogWriter().info("before create server peer");
    vm2.invoke(createServerPeer);

    // now do a tx in the server
    SerializableRunnable doServerTx = new CacheSerializableRunnable("doServerTx") {
      @Override
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        Cache cache = getCache();
        CacheTransactionManager txmgr = cache.getCacheTransactionManager();
        txmgr.begin();
        try {
          region.put("k1", "v1");
          region.put("k2", "v2");
          region.put("k3", "v3");
        } finally {
          txmgr.commit();
        }
      }
    };
    getSystem().getLogWriter().info("before doServerTx");
    vm2.invoke(doServerTx);

    // @todo verify server received it but to do this need a listener in
    // the server

    // now verify that the client receives the committed data
    SerializableRunnable validateClient =
        new CacheSerializableRunnable("Validate Cache Server Client") {
          @Override
          public void run2() throws CacheException {
            final LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
            // wait for a while for us to have the correct number of entries
            WaitCriterion ev = new WaitCriterion() {
              @Override
              public boolean done() {
                return region.size() == 3;
              }

              @Override
              public String description() {
                return "waiting for region to be size 3";
              }
            };
            GeodeAwaitility.await().untilAsserted(ev);
            assertTrue(region.containsKey("k1"));
            assertTrue(region.containsKey("k2"));
            assertTrue(region.containsKey("k3"));
            assertEquals("v1", region.getEntry("k1").getValue());
            assertEquals("v2", region.getEntry("k2").getValue());
            assertEquals("v3", region.getEntry("k3").getValue());
          }
        };
    getSystem().getLogWriter().info("before confirmCommitOnClient");
    vm1.invoke(validateClient);
    disconnectAllFromDS();
  }

  static class Order implements DataSerializable {
    int index;

    public Order() {}

    public void init(int index) {
      this.index = index;
    }

    public int getIndex() {
      return index;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeInt(index);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      index = in.readInt();
    }
  }
}
