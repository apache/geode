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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache30.ClientServerTestCase.TEST_POOL_NAME;
import static org.apache.geode.cache30.ClientServerTestCase.configureConnectionPool;
import static org.apache.geode.cache30.ClientServerTestCase.configureConnectionPoolWithNameAndFactory;
import static org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier.getInstance;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.ALL;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.NotSerializableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.CertifiableTestCacheListener;
import org.apache.geode.cache30.TestCacheWriter;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.EntryExpiryTask;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.cache.PoolStats;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifierStats;
import org.apache.geode.internal.logging.LocalLogWriter;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRule;
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

  private static int numberOfAfterInvalidates;
  private static int numberOfAfterCreates;
  private static int numberOfAfterUpdates;

  private static final int TYPE_CREATE = 0;
  private static final int TYPE_UPDATE = 1;
  private static final int TYPE_INVALIDATE = 2;
  private static final int TYPE_DESTROY = 3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

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
    Invoke.invokeInEveryVM(() -> {
      if (basicGetCache() != null) {
        basicGetCache().close();
      }
      PoolManager.getAll().forEach((key, value) -> value.destroy());
    });
  }

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

  private void startBridgeServer(int port)
      throws IOException {

    Cache cache = getCache();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.setMaxThreads(0);
    bridge.setLoadPollInterval(CacheServer.DEFAULT_LOAD_POLL_INTERVAL);
    bridge.start();
    bridgeServerPort = bridge.getPort();
  }

  /**
   * Stops the cache server that serves up the given cache.
   *
   * @since GemFire 4.0
   */
  private void stopBridgeServer(Cache cache) {
    CacheServer bridge = cache.getCacheServers().iterator().next();
    bridge.stop();
    assertThat(bridge.isRunning()).isFalse();
  }

  private void createLonerDS() {
    disconnectFromDS();
    InternalDistributedSystem ds = getLonerSystem();
    assertThat(ds.getDistributionManager().getOtherDistributionManagerIds()).isEmpty();
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

  private static class EventWrapper {
    final EntryEvent event;
    final Object key;
    final Object val;
    final Object arg;
    final int type;

    EventWrapper(EntryEvent ee, int type) {
      event = ee;
      key = ee.getKey();
      val = ee.getNewValue();
      arg = ee.getCallbackArgument();
      this.type = type;
    }

    public String toString() {
      return "EventWrapper: event=" + event + ", type=" + type;
    }
  }

  static class ControlListener extends CacheListenerAdapter<Object, Object> {
    final LinkedList<EventWrapper> events = new LinkedList<>();
    final Object CONTROL_LOCK = new Object();

    void waitWhileNotEnoughEvents(int eventCount) {
      long maxMillis = System.currentTimeMillis() + (long) 60000;
      synchronized (CONTROL_LOCK) {
        try {
          while (events.size() < eventCount) {
            long waitMillis = maxMillis - System.currentTimeMillis();
            if (waitMillis < 10) {
              break;
            }
            CONTROL_LOCK.wait(waitMillis);
          }
        } catch (InterruptedException abort) {
          fail("interrupted");
        }
      }
    }

    @Override
    public void afterCreate(EntryEvent e) {
      synchronized (CONTROL_LOCK) {
        events.add(new EventWrapper(e, TYPE_CREATE));
        CONTROL_LOCK.notifyAll();
      }
    }

    @Override
    public void afterUpdate(EntryEvent e) {
      synchronized (CONTROL_LOCK) {
        events.add(new EventWrapper(e, TYPE_UPDATE));
        CONTROL_LOCK.notifyAll();
      }
    }

    @Override
    public void afterInvalidate(EntryEvent e) {
      synchronized (CONTROL_LOCK) {
        events.add(new EventWrapper(e, TYPE_INVALIDATE));
        CONTROL_LOCK.notifyAll();
      }
    }

    @Override
    public void afterDestroy(EntryEvent e) {
      synchronized (CONTROL_LOCK) {
        events.add(new EventWrapper(e, TYPE_DESTROY));
        CONTROL_LOCK.notifyAll();
      }
    }
  }


  private void verifyServerCount(final PoolImpl pool, final int expectedCount) {
    getCache().getLogger().info("verifyServerCount expects=" + expectedCount);
    await().alias("Expecting found server count to match expected count")
        .until(() -> pool.getConnectedServerCount() == expectedCount);
  }

  /**
   * Tests that the callback argument is sent to the server
   */
  @Test
  public void test001CallbackArg() throws CacheException {
    final String name = getName();

    final Object createCallbackArg = "CREATE CALLBACK ARG";
    final Object updateCallbackArg = "PUT CALLBACK ARG";

    vm0.invoke("Create Cache Server", () -> {
      CacheWriter<Object, Object> cw = new TestCacheWriter<Object, Object>() {
        @Override
        public final void beforeUpdate2(EntryEvent event) throws CacheWriterException {
          Object beca = event.getCallbackArgument();
          assertThat(updateCallbackArg).isEqualTo(beca);
        }

        @Override
        public void beforeCreate2(EntryEvent event) throws CacheWriterException {
          Object beca = event.getCallbackArgument();
          assertThat(createCallbackArg).isEqualTo(beca);
        }
      };
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, cw);
      createRegion(name, factory);
      startBridgeServer(0);
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);

    vm1.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);

      configureConnectionPool(factory, NetworkUtils.getServerHostName(),
          new int[] {port}, true, -1, -1, null);
      createRegion(name, factory);
    });

    vm1.invoke("Add entries", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        region.create(i, "old" + i, createCallbackArg);
      }
      for (int i = 0; i < 10; i++) {
        region.put(i, "new" + i, updateCallbackArg);
      }
    });

    vm0.invoke("Check cache writer", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      TestCacheWriter writer = getTestWriter(region);
      assertThat(writer.wasInvoked()).isTrue();
    });

    vm1.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    });
  }

  /**
   * Tests that consecutive puts have the callback assigned appropriately.
   */
  @Test
  public void test002CallbackArg2() throws CacheException {
    final String name = getName();
    final Object createCallbackArg = "CREATE CALLBACK ARG";

    vm0.invoke("Create Cache Server", () -> {
      CacheWriter<Object, Object> cacheWriter = new TestCacheWriter<Object, Object>() {
        @Override
        public void beforeCreate2(EntryEvent event) throws CacheWriterException {
          Integer key = (Integer) event.getKey();
          if (key % 2 == 0) {
            Object callbackArgument = event.getCallbackArgument();
            assertThat(createCallbackArg).isEqualTo(callbackArgument);
          } else {
            Object callbackArgument = event.getCallbackArgument();
            assertThat(callbackArgument).isNull();
          }
        }
      };
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, cacheWriter);
      createRegion(name, factory);
      startBridgeServer(0);
    });

    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    vm1.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      createRegion(name, factory);
    });

    vm1.invoke("Add entries", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          region.create(i, "old" + i, createCallbackArg);

        } else {
          region.create(i, "old" + i);
        }
      }
    });

    vm1.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    });

    vm0.invoke("Check cache writer", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      TestCacheWriter writer = getTestWriter(region);
      assertThat(writer.wasInvoked()).isTrue();
    });
  }

  /**
   * Tests for bug 36684 by having two cache servers with cacheloaders that should always return a
   * value and one client connected to each server reading values. If the bug exists, the clients
   * will get null sometimes.
   */
  @Test
  public void test003Bug36684() throws CacheException, InterruptedException {
    final String name = getName();

    // Create the cache servers with distributed, mirrored region
    Stream.of(vm0, vm1).forEach(vm -> {
      vm.invoke("Create Cache Server", () -> {
        CacheLoader<Object, Object> cl = new CacheLoader<Object, Object>() {
          @Override
          public Object load(LoaderHelper helper) {
            return helper.getKey();
          }

          @Override
          public void close() {

          }
        };
        RegionFactory<Object, Object> factory =
            getBridgeServerMirroredAckRegionAttributes(cl);
        createRegion(name, factory);
        startBridgeServer(0);

      });
      logger.info("before create server");
    });

    // Create cache server clients
    final int numberOfKeys = 1000;
    final String host0 = NetworkUtils.getServerHostName();
    final int vm0Port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final int vm1Port = vm1.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    SerializableRunnable createClient = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // reset all static listener variables in case this is being rerun in a subclass
        numberOfAfterInvalidates = 0;
        numberOfAfterCreates = 0;
        numberOfAfterUpdates = 0;
        // create the region
        getLonerSystem();
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false); // test validation expects this behavior
        // create bridge writer
        configureConnectionPool(factory, host0, new int[] {vm0Port, vm1Port}, true, -1,
            -1, null);
        createRegion(name, factory);
      }
    };
    logger.info("before create client");
    vm2.invoke("Create Cache Server Client", createClient);
    vm3.invoke("Create Cache Server Client", createClient);

    // Initialize each client with entries (so that afterInvalidate is called)
    SerializableRunnable initializeClient = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        // StringBuffer errors = new StringBuffer();
        numberOfAfterInvalidates = 0;
        numberOfAfterCreates = 0;
        numberOfAfterUpdates = 0;
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < numberOfKeys; i++) {
          String expected = "key-" + i;
          String actual = (String) region.get("key-" + i);
          assertThat(expected).isEqualTo(actual);
        }
      }
    };

    logger.info("before initialize client");
    AsyncInvocation inv2 = vm2.invokeAsync("Initialize Client", initializeClient);
    AsyncInvocation inv3 = vm3.invokeAsync("Initialize Client", initializeClient);

    inv2.await();
    inv3.await();
  }

  /**
   * Test for client connection loss with CacheLoader Exception on the server.
   */
  @Test
  public void test004ForCacheLoaderException() throws CacheException {
    final String name = getName();

    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    // Create the cache servers with distributed, mirrored region
    logger.info("before create server");

    server.invoke("Create Cache Server", () -> {
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
      RegionFactory<Object, Object> factory =
          getBridgeServerMirroredAckRegionAttributes(cl);
      createRegion(name, factory);
      startBridgeServer(0);
    });

    // Create cache server clients
    final int numberOfKeys = 10;
    final String host0 = NetworkUtils.getServerHostName();
    final int[] port =
        new int[] {server.invoke(ConnectionPoolDUnitTest::getCacheServerPort)};
    final String poolName = "myPool";

    logger.info("before create client");
    client.invoke("Create Cache Server Client", () -> {
      getLonerSystem();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      // create bridge writer
      configureConnectionPoolWithNameAndFactory(factory, host0, port, true,
          -1, -1, null, poolName, PoolManager.createFactory(), -1, -1, -2, -1);
      createRegion(name, factory);
    });

    // Initialize each client with entries (so that afterInvalidate is called)

    logger.info("before initialize client");
    AsyncInvocation inv2 = client.invokeAsync("Initialize Client", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      PoolStats stats = ((PoolImpl) PoolManager.find(poolName)).getStats();
      int oldConnects = stats.getConnects();
      int oldDisConnects = stats.getDisConnects();
      for (int i = 0; i < numberOfKeys; i++) {
        region.get("key-" + i);
      }
      int newConnects = stats.getConnects();
      int newDisConnects = stats.getDisConnects();

      // newDisConnects);
      if (newConnects != oldConnects && newDisConnects != oldDisConnects) {
        fail("New connection has created for Server side CacheLoaderException.");
      }
    });

    ThreadUtils.join(inv2, 30 * 1000);
  }

  private void validateDS() {
    List list = InternalDistributedSystem.getExistingSystems();
    if (list.size() > 1) {
      logger.info("validateDS: size=" + list.size() + " isDedicatedAdminVM="
          + ClusterDistributionManager.isDedicatedAdminVM() + " l=" + list);
    }
    assertThat(ClusterDistributionManager.isDedicatedAdminVM()).isFalse();
    assertThat(1).isEqualTo(list.size());
  }

  /**
   * Tests the basic operations of the {@link Pool}
   *
   * @since GemFire 3.5
   */
  @Test
  public void test006Pool() throws CacheException {
    final String name = getName();

    vm0.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setConcurrencyChecksEnabled(false);
      factory.setCacheLoader(new CacheLoader<Object, Object>() {
        @Override
        public Object load(LoaderHelper helper) {
          return helper.getKey().toString();
        }

        @Override
        public void close() {

        }
      });
      createRegion(name, factory);
      startBridgeServer(0);
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    vm1.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      validateDS();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      createRegion(name, factory);
    });

    vm1.invoke("Get values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Object value = region.get(i);
        assertThat(String.valueOf(i)).isEqualTo(value);
      }
    });

    vm1.invoke("Update values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);

      for (int i = 0; i < 10; i++) {
        region.put(i, i);
      }
    });

    vm2.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      validateDS();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      createRegion(name, factory);
    });
    vm2.invoke("Validate values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Object value = region.get(i);
        assertThat(value).isNotNull();
        assertThat(value).isInstanceOf(Integer.class);
        assertThat(i).isEqualTo(((Integer) value).intValue());
      }
    });

    vm1.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      String pName = region.getAttributes().getPoolName();
      PoolImpl p = (PoolImpl) PoolManager.find(pName);
      assertThat(p.isDestroyed()).isFalse();
      assertThat(1).isEqualTo(p.getAttachCount());
      try {
        p.destroy();
        fail("expected IllegalStateException");
      } catch (IllegalStateException ignored) {
      }
      region.localDestroyRegion();
      assertThat(p.isDestroyed()).isFalse();
      assertThat(p.getAttachCount()).isEqualTo(0);
    });
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
    final String name = getName();

    // Create two cache servers
    Stream.of(vm0, vm1).forEach(vm -> vm.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
      createRegion(name, factory);
      startBridgeServer(0);
    }));

    final int port0 = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    final int port1 = vm1.invoke(ConnectionPoolDUnitTest::getCacheServerPort);

    // Create one bridge client in this VM

    vm2.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      String ServerGroup = null;
      configureConnectionPoolWithNameAndFactory(factory, host0, new int[] {port0, port1}, true,
          -1, cnxCount, ServerGroup, TEST_POOL_NAME, PoolManager.createFactory(), 100, -1, -2,
          -1);
      Region<Object, Object> region = createRegion(name, factory);

      // force connections to form
      region.put("keyInit", 0);
      region.put("keyInit2", 0);
    });

    vm2.invoke("verify2Servers", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      PoolImpl pool = getPool(region);
      verifyServerCount(pool, 2);
    });

    final String expected = "java.io.IOException";
    final String addExpected =
        "<ExpectedException action=add>" + expected + "</ExpectedException>";
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

      vm2.invoke("verify1Server", () -> {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        PoolImpl pool = getPool(region);
        verifyServerCount(pool, 1);
      });

      vm1.invoke("Restart CacheServer", () -> {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        assertThat(region).isNotNull();
        startBridgeServer(port1);
      });

      // Pause long enough for the monitor to realize the server has been bounced
      // and reconnect to it.
      vm2.invoke("verify2Servers", () -> {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        PoolImpl pool = getPool(region);
        verifyServerCount(pool, 2);
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
    vm2.invoke("verify1Server", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      PoolImpl pool = getPool(region);
      verifyServerCount(pool, 1);
    });

    // Close Pool
    vm2.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    });
  }


  private static volatile boolean stopTestLifetimeExpire = false;

  private static volatile int baselineLifetimeCheck;
  private static volatile int baselineLifetimeExtensions;
  private static volatile int baselineLifetimeConnect;
  private static volatile int baselineLifetimeDisconnect;

  @Test
  public void basicTestLifetimeExpire()
      throws CacheException, InterruptedException {
    final String name = getName();

    AsyncInvocation putAI = null;
    AsyncInvocation putAI2 = null;

    try {

      // Create two cache servers

      vm0.invoke("Create Cache Server", () -> {
        RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
        factory.addCacheListener(new DelayListener());
        createRegion(name, factory);
        startBridgeServer(0);

      });

      final int port0 = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
      final String host0 = NetworkUtils.getServerHostName();
      vm1.invoke("Create Cache Server", () -> {
        RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
        factory.addCacheListener(new DelayListener());
        createRegion(name, factory);
        startBridgeServer(0);

      });
      final int port1 = vm1.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
      // we only had to stop it to reserve a port
      vm1.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));

      // Create one bridge client in this VM

      vm2.invoke("Create region", () -> {
        getLonerSystem();
        getCache();
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        configureConnectionPoolWithNameAndFactory(factory, host0, new int[] {port0, port1},
            false, -1, 0, null, TEST_POOL_NAME, PoolManager.createFactory(), 100, 500, 500, -1);

        Region<Object, Object> region = createRegion(name, factory);

        // force connections to form
        region.put("keyInit", 0);
        region.put("keyInit2", 0);
      });

      // Launch async thread that puts objects into cache. This thread will execute until
      // the test has ended.
      putAI = vm2.invokeAsync("Put objects", () -> {
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
      });
      putAI2 = vm2.invokeAsync("Put objects", () -> {
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
      });

      vm2.invoke("verify1Server", () -> {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        PoolImpl pool = getPool(region);
        final PoolStats stats = pool.getStats();
        verifyServerCount(pool, 1);

        await().until(() -> stats.getLoadConditioningCheck() >= (10 + baselineLifetimeCheck));

        // make sure no replacements are happening.
        // since we have 2 threads and 2 cnxs and 1 server
        // when lifetimes are up we should only want to connect back to the
        // server we are already connected to and thus just extend our lifetime
        assertThat(stats.getLoadConditioningCheck() >= (10 + baselineLifetimeCheck))
            .describedAs("baselineLifetimeCheck=" + baselineLifetimeCheck
                + " but stats.getLoadConditioningCheck()=" + stats.getLoadConditioningCheck())
            .isTrue();
        baselineLifetimeCheck = stats.getLoadConditioningCheck();
        assertThat(stats.getLoadConditioningExtensions())
            .isGreaterThan(baselineLifetimeExtensions);
        assertThat(stats.getLoadConditioningConnect()).isEqualTo(baselineLifetimeConnect);
        assertThat(stats.getLoadConditioningDisconnect()).isEqualTo(baselineLifetimeDisconnect);
      });

      await().until(putAI::isAlive);
      await().until(putAI2::isAlive);

    } finally {
      vm2.invoke("Stop Putters", () -> stopTestLifetimeExpire = true);

      try {
        if (putAI != null) {
          // Verify that no exception has occurred in the putter thread
          putAI.await();
        }

        if (putAI2 != null) {
          // Verify that no exception has occurred in the putter thread
          putAI.await();
        }
      } finally {
        vm2.invoke("Stop Putters", () -> stopTestLifetimeExpire = false);
        // Close Pool
        vm2.invoke("Close Pool", () -> {
          Region<Object, Object> region = getRootRegion().getSubregion(name);
          String poolName = region.getAttributes().getPoolName();
          region.localDestroyRegion();
          PoolManager.find(poolName).destroy();
        });
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
    final String name = getName();

    vm0.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
      createRegion(name, factory);
      startBridgeServer(0);
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    vm1.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      configureConnectionPool(factory, host0, new int[] {port}, false, -1, -1, null);
      createRegion(name, factory);
    });
    vm1.invoke("Create values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        region.create(i, i);
      }
    });

    vm2.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      configureConnectionPool(factory, host0, new int[] {port}, false, -1, -1, null);
      createRegion(name, factory);
    });
    vm2.invoke("Validate values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Object value = region.get(i);
        assertThat(value).isNotNull();
        assertThat(value).isInstanceOf(Integer.class);
        assertThat(i).isEqualTo(((Integer) value).intValue());
      }
    });

    SerializableRunnable close = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke("Close Pool", close);
    vm2.invoke("Close Pool", close);
  }

  /**
   * Tests the put operation of the {@link Pool}
   *
   * @since GemFire 3.5
   */
  @Test
  public void test012PoolPut() throws CacheException {
    final String name = getName();

    vm0 = VM.getVM(0);
    vm1 = VM.getVM(1);
    vm2 = VM.getVM(2);

    vm0.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
      createRegion(name, factory);
      startBridgeServer(0);
    });

    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    SerializableRunnable createRegion = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        // create bridge writer
        configureConnectionPool(factory, host0, new int[] {port}, false, -1, -1, null);
        createRegion(name, factory);
      }
    };

    vm1.invoke("Create region", createRegion);

    vm1.invoke("Put values", () -> {
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
    });

    vm2.invoke("Create region", createRegion);

    vm2.invoke("Get / validate string values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Object value = region.get("key-string-" + i);
        assertThat(value).isNotNull();
        assertThat(value).isInstanceOf(String.class);
        assertThat("value-" + i).isEqualTo(value);
      }
    });

    vm2.invoke("Get / validate object values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Object value = region.get("key-object-" + i);
        assertThat(value).isNotNull();
        assertThat(value).isInstanceOf(Order.class);
        assertThat(i).isEqualTo(((Order) value).getIndex());
      }
    });

    vm2.invoke("Get / validate byte[] values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Object value = region.get("key-bytes-" + i);
        assertThat(value).isNotNull();
        assertThat(value).isInstanceOf(byte[].class);
        assertThat("value-" + i).isEqualTo(new String((byte[]) value));
      }
    });

    SerializableRunnable closePool = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(closePool);
    vm2.invoke(closePool);


  }

  /**
   * Tests the put operation of the {@link Pool}
   *
   * @since GemFire 3.5
   */
  @Test
  public void test013PoolPutNoDeserialize() throws CacheException {
    final String name = getName();

    vm0.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
      createRegion(name, factory);
      startBridgeServer(0);
    });

    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    SerializableRunnable createRegion = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        configureConnectionPool(factory, host0, new int[] {port}, false, -1, -1, null);
        createRegion(name, factory);
      }
    };

    vm1.invoke("Create Region", createRegion);

    vm1.invoke("Put values", () -> {
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
    });

    vm2.invoke("Create Region", createRegion);

    vm2.invoke("Get / validate string values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Object value = region.get("key-string-" + i);
        assertThat(value).isNotNull();
        assertThat(value).isInstanceOf(String.class);
        assertThat("value-" + i).isEqualTo(value);
      }
    });

    vm2.invoke("Get / validate object values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Object value = region.get("key-object-" + i);
        assertThat(value).isNotNull();
        assertThat(value).isInstanceOf(Order.class);
        assertThat(i).isEqualTo(((Order) value).getIndex());
      }
    });

    vm2.invoke("Get / validate byte[] values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Object value = region.get("key-bytes-" + i);
        assertThat(value).isNotNull();
        assertThat(value instanceof byte[]).isTrue();
        assertThat("value-" + i).isEqualTo(new String((byte[]) value));
      }
    });

    SerializableRunnable closePool = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke("Close Pool", closePool);
    vm2.invoke("Close Pool", closePool);
  }


  private <K, V> List<CacheEvent<K, V>> assertForOpCount(String regionName, Operation operation,
      int count) {
    Region<K, V> region = getRootRegion().getSubregion(regionName);
    CertifiableTestCacheListener<K, V> ctl =
        (CertifiableTestCacheListener<K, V>) region.getAttributes()
            .getCacheListeners()[0];
    List<CacheEvent<K, V>> list = new ArrayList<>(ctl.getEventHistory());

    await().until(() -> {
      list.addAll(ctl.getEventHistory());
      return list.size() >= count;
    });
    int countOfOps = 0;
    for (CacheEvent<K, V> cacheEvent : list) {

      if (cacheEvent.getOperation() == operation) {
        countOfOps++;
      } else {
        logger.info("assertForOpCount: Got an unexpected message " + cacheEvent);
      }
    }

    assertThat(countOfOps).isEqualTo(count);
    assertThat(countOfOps)
        .describedAs("assertForOpCount: There were excess operations in the list " + list)
        .isEqualTo(list.size());
    return list;
  }

  /**
   * Tests that invalidates and destroys are propagated to {@link Pool}s.
   *
   * @since GemFire 3.5
   */
  @Test
  public void test014InvalidateAndDestroyPropagation() throws CacheException {
    final String name = getName();

    vm0.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
      createRegion(name, factory);
      startBridgeServer(0);

    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    vm1.invoke("Create region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
        factory.addCacheListener(new CertifiableTestCacheListener<>());
        Region rgn = createRegion(name, factory);
        rgn.registerInterestRegex(".*", false, false);
      }
    });
    vm1.invoke("Populate region", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        region.put(i, "old" + i);
      }
    });
    vm2.invoke("Create region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
        factory.addCacheListener(new CertifiableTestCacheListener<>());
        Region rgn = createRegion(name, factory);
        rgn.registerInterestRegex(".*", false, false);
      }
    });

    vm1.invoke("Turn on history", () -> {
      await().until(() -> getRootRegion().getSubregion(name) != null);
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      CertifiableTestCacheListener<Object, Object> ctl =
          (CertifiableTestCacheListener<Object, Object>) region.getAttributes()
              .getCacheListeners()[0];
      ctl.enableEventHistory();
    });

    vm2.invoke("Update region", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        region.put(i, "new" + i, "callbackArg" + i);
      }
    });

    vm1.invoke("Verify invalidates", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      CertifiableTestCacheListener<Object, Object> ctl =
          (CertifiableTestCacheListener<Object, Object>) region.getAttributes()
              .getCacheListeners()[0];

      for (int i = 0; i < 10; i++) {
        Object key = i;
        ctl.waitForInvalidated(key);
        Region.Entry entry = region.getEntry(key);
        assertThat(entry).isNotNull();
        assertThat(entry.getValue()).isNull();
      }

      List<CacheEvent<Object, Object>> list = assertForOpCount(name, Operation.INVALIDATE, 10);

      for (int i = 0; i < 10; i++) {
        Object key = i;
        EntryEvent ee = (EntryEvent) list.get(i);
        assertThat(ee.getKey()).isEqualTo(key);
        assertThat("old" + i).isEqualTo(ee.getOldValue());
        assertThat("callbackArg" + i).isEqualTo(ee.getCallbackArgument());
        assertThat(ee.isOriginRemote()).isTrue();
      }
    });

    vm2.invoke("Validate original and destroy", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Object key = i;
        assertThat(region.getEntry(key).getValue()).isEqualTo("new" + i);
        region.destroy(key, "destroyCB" + i);
        assertThat(region.getEntry(key)).isNull();
      }
    });

    vm1.invoke("Verify destroys", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      CertifiableTestCacheListener<Object, Object> ctl =
          (CertifiableTestCacheListener<Object, Object>) region.getAttributes()
              .getCacheListeners()[0];

      for (int i = 0; i < 10; i++) {
        Object key = i;
        ctl.waitForDestroyed(key);
        Region.Entry entry = region.getEntry(key);
        assertThat(entry).isNull();
      }

      List<CacheEvent<Object, Object>> list = assertForOpCount(name, Operation.DESTROY, 10);

      for (int i = 0; i < 10; i++) {
        Object key = i;
        EntryEvent ee = (EntryEvent) list.get(i);
        assertThat(ee.getKey()).isEqualTo(key);
        assertThat(ee.getOldValue()).isNull();
        assertThat("destroyCB" + i).isEqualTo(ee.getCallbackArgument());
        assertThat(ee.isOriginRemote()).isTrue();
      }
    });

    vm2.invoke("recreate", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Object key = i;
        region.create(key, "recreate" + i, "recreateCB" + i);
      }
    });

    vm1.invoke("Verify Local Load Creates", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);

      await().untilAsserted(() -> {
        for (int i = 0; i < 10; i++) {
          Object key = i;
          assertThat(region.containsKeyOnServer(key)).isTrue();
        }
      });

      for (int i = 0; i < 10; i++) {
        Object key = i;
        region.get(key, "recreateCB");
      }

      List<CacheEvent<Object, Object>> list =
          assertForOpCount(name, Operation.LOCAL_LOAD_CREATE, 10);

      for (int i = 0; i < 10; i++) {
        Object key = i;
        EntryEvent ee = (EntryEvent) list.get(i);
        assertThat(ee.getKey()).isEqualTo(key);
        assertThat(ee.getOldValue()).isNull();
        assertThat(ee.getNewValue()).isEqualTo("recreate" + i);
        assertThat(ee.isOriginRemote()).isFalse();
      }
    });

    vm1.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    });
    vm2.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    });
  }

  /**
   * Tests that invalidates and destroys are propagated to {@link Pool}s correctly to
   * DataPolicy.EMPTY + InterestPolicy.ALL
   *
   * @since GemFire 5.0
   */
  @Test
  public void test015InvalidateAndDestroyToEmptyAllPropagation() throws CacheException {
    final String name = getName();

    vm0.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
      createRegion(name, factory);
      startBridgeServer(0);
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    vm1.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      factory.addCacheListener(new CertifiableTestCacheListener<>());
      factory.setDataPolicy(DataPolicy.EMPTY);
      factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      Region rgn = createRegion(name, factory);
      rgn.registerInterestRegex(".*", false, false);
    });
    vm1.invoke("Populate region", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        region.put(i, "old" + i);
      }
    });

    vm2.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      factory.addCacheListener(new CertifiableTestCacheListener<>());
      Region rgn = createRegion(name, factory);
      rgn.registerInterestRegex(".*", false, false);
    });

    vm1.invoke("Turn on history", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      CertifiableTestCacheListener<Object, Object> ctl =
          (CertifiableTestCacheListener<Object, Object>) region.getAttributes()
              .getCacheListeners()[0];
      ctl.enableEventHistory();
    });

    vm2.invoke("Update region", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        region.put(i, "new" + i, "callbackArg" + i);
      }
    });

    vm1.invoke("Verify invalidates", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      CertifiableTestCacheListener<Object, Object> ctl =
          (CertifiableTestCacheListener<Object, Object>) region.getAttributes()
              .getCacheListeners()[0];

      for (int i = 0; i < 10; i++) {
        Object key = i;
        ctl.waitForInvalidated(key);
        await().until(() -> region.getEntry(key) == null);
      }
      {
        List<CacheEvent<Object, Object>> list = ctl.getEventHistory();
        assertThat(list.size()).isEqualTo(10);
        for (int i = 0; i < 10; i++) {
          Object key = i;
          EntryEvent ee = (EntryEvent) list.get(i);
          assertThat(ee.getKey()).isEqualTo(key);
          assertThat(ee.getOldValue()).isNull();
          assertThat(ee.isOldValueAvailable()).isFalse(); // failure
          assertThat(Operation.INVALIDATE).isEqualTo(ee.getOperation());
          assertThat("callbackArg" + i).isEqualTo(ee.getCallbackArgument());
          assertThat(ee.isOriginRemote()).isTrue();
        }
      }
    });

    vm2.invoke("Validate original and destroy", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Object key = i;
        assertThat("new" + i).isEqualTo(region.getEntry(key).getValue());
        region.destroy(key, "destroyCB" + i);
      }
    });

    vm1.invoke("Verify destroys", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      CertifiableTestCacheListener<Object, Object> ctl =
          (CertifiableTestCacheListener<Object, Object>) region.getAttributes()
              .getCacheListeners()[0];
      for (int i = 0; i < 10; i++) {
        Object key = i;
        ctl.waitForDestroyed(key);
        await().until(() -> region.getEntry(key) == null);
      }
      {
        List<CacheEvent<Object, Object>> list = ctl.getEventHistory();
        assertThat(10).isEqualTo(list.size());
        for (int i = 0; i < 10; i++) {
          Object key = i;
          EntryEvent ee = (EntryEvent) list.get(i);
          assertThat(ee.getKey()).isEqualTo(key);
          assertThat(ee.getOldValue()).isNull();
          assertThat(ee.isOldValueAvailable()).isFalse();
          assertThat(Operation.DESTROY).isEqualTo(ee.getOperation());
          assertThat("destroyCB" + i).isEqualTo(ee.getCallbackArgument());
          assertThat(ee.isOriginRemote()).isTrue();
        }
      }
    });
    vm2.invoke("recreate", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Object key = i;
        region.create(key, "create" + i, "createCB" + i);
      }
    });

    vm1.invoke("Verify creates", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      CertifiableTestCacheListener<Object, Object> ctl =
          (CertifiableTestCacheListener<Object, Object>) region.getAttributes()
              .getCacheListeners()[0];
      for (int i = 0; i < 10; i++) {
        Object key = i;
        ctl.waitForInvalidated(key);
        await().until(() -> region.getEntry(key) == null);
      }
      List<CacheEvent<Object, Object>> list = ctl.getEventHistory();
      assertThat(10).isEqualTo(list.size());
      for (int i = 0; i < 10; i++) {
        Object key = i;
        EntryEvent ee = (EntryEvent) list.get(i);
        assertThat(ee.getKey()).isEqualTo(key);
        assertThat(ee.getOldValue()).isNull();
        assertThat(ee.isOldValueAvailable()).isFalse();
        assertThat(Operation.INVALIDATE).isEqualTo(ee.getOperation());
        assertThat("createCB" + i).isEqualTo(ee.getCallbackArgument());
        assertThat(ee.isOriginRemote()).isTrue();
      }
      // now see if we can get it from the server
      for (int i = 0; i < 10; i++) {
        Object key = i;
        assertThat("create" + i).isEqualTo(region.get(key, "loadCB" + i));
      }

      list = ctl.getEventHistory();
      assertThat(10).isEqualTo(list.size());
      for (int i = 0; i < 10; i++) {
        Object key = i;
        EntryEvent ee = (EntryEvent) list.get(i);
        assertThat(ee.getKey()).isEqualTo(key);
        assertThat(ee.getOldValue()).isNull();
        assertThat(ee.getNewValue()).isEqualTo("create" + i);
        assertThat(Operation.LOCAL_LOAD_CREATE).isEqualTo(ee.getOperation());
        assertThat("loadCB" + i).isEqualTo(ee.getCallbackArgument());
        assertThat(ee.isOriginRemote()).isFalse();
      }
    });

    Stream.of(vm1, vm2).forEach(vm -> vm.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    }));


  }

  /**
   * Tests that invalidates and destroys are propagated to {@link Pool}s correctly to
   * DataPolicy.EMPTY + InterestPolicy.CACHE_CONTENT
   *
   * @since GemFire 5.0
   */
  @Test
  public void test016InvalidateAndDestroyToEmptyCCPropagation() throws CacheException {
    final String name = getName();

    vm0.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
      createRegion(name, factory);
      startBridgeServer(0);
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    vm1.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      factory.addCacheListener(new CertifiableTestCacheListener<>());
      factory.setDataPolicy(DataPolicy.EMPTY);
      factory
          .setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT));
      Region rgn = createRegion(name, factory);
      rgn.registerInterestRegex(".*", false, false);
    });

    vm1.invoke("Populate region", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        region.put(i, "old" + i);
      }
    });

    vm2.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      factory.addCacheListener(new CertifiableTestCacheListener<>());
      Region rgn = createRegion(name, factory);
      rgn.registerInterestRegex(".*", false, false);
    });

    vm1.invoke("Turn on history", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      CertifiableTestCacheListener<Object, Object> ctl =
          (CertifiableTestCacheListener<Object, Object>) region.getAttributes()
              .getCacheListeners()[0];
      ctl.enableEventHistory();
    });

    vm2.invoke("Update region", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        region.put(i, "new" + i, "callbackArg" + i);
      }
    });

    vm1.invoke("Verify invalidates", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      CertifiableTestCacheListener<Object, Object> ctl =
          (CertifiableTestCacheListener<Object, Object>) region.getAttributes()
              .getCacheListeners()[0];
      await().until(() -> ctl.getEventHistory().size() == 0);
    });

    vm2.invoke("Validate original and destroy", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Object key = i;
        assertThat("new" + i).isEqualTo(region.getEntry(key).getValue());
        region.destroy(key, "destroyCB" + i);
      }
    });

    vm1.invoke("Verify destroys", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      CertifiableTestCacheListener<Object, Object> ctl =
          (CertifiableTestCacheListener<Object, Object>) region.getAttributes()
              .getCacheListeners()[0];
      await().until(() -> ctl.getEventHistory().size() == 0);
    });
    vm2.invoke("recreate", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Object key = i;
        region.create(key, "create" + i, "createCB" + i);
      }
    });

    vm1.invoke("Verify creates", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      CertifiableTestCacheListener<Object, Object> ctl =
          (CertifiableTestCacheListener<Object, Object>) region.getAttributes()
              .getCacheListeners()[0];
      await().until(() -> ctl.getEventHistory().size() == 0);

      List<CacheEvent<Object, Object>> list = ctl.getEventHistory();
      assertThat(list).isEmpty();
      // now see if we can get it from the server
      for (int i = 0; i < 10; i++) {
        Object key = i;
        assertThat("create" + i).isEqualTo(region.get(key, "loadCB" + i));
      }
      list = ctl.getEventHistory();
      assertThat(10).isEqualTo(list.size());
      for (int i = 0; i < 10; i++) {
        Object key = i;
        EntryEvent ee = (EntryEvent) list.get(i);
        assertThat(ee.getKey()).isEqualTo(key);
        assertThat(ee.getOldValue()).isNull();
        assertThat(ee.getNewValue()).isEqualTo("create" + i);
        assertThat(Operation.LOCAL_LOAD_CREATE).isEqualTo(ee.getOperation());
        assertThat("loadCB" + i).isEqualTo(ee.getCallbackArgument());
        assertThat(ee.isOriginRemote()).isFalse();
      }
    });

    Stream.of(vm1, vm2).forEach(vm -> vm.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    }));


  }

  /**
   * Tests interest key registration.
   */
  @Test
  public void test017ExpireDestroyHasEntryInCallback() throws CacheException {
    disconnectAllFromDS();
    final String name = getName();

    // Create cache server
    vm0.invoke("Create Cache Server", () -> {
      // In lieu of System.setProperty("gemfire.EXPIRE_SENDS_ENTRY_AS_CALLBACK", "true");
      EntryExpiryTask.expireSendsEntryAsCallback = true;
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
      factory.setEntryTimeToLive(new ExpirationAttributes(1, ExpirationAction.DESTROY));
      createRegion(name, factory);
      startBridgeServer(0);
    });

    // Create cache server clients
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    vm1.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setDataPolicy(DataPolicy.EMPTY);
      factory.setSubscriptionAttributes(new SubscriptionAttributes((InterestPolicy.ALL)));
      factory.setConcurrencyChecksEnabled(false);
      // create bridge writer
      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      factory.addCacheListener(new CertifiableTestCacheListener<>());

      Region<Object, Object> region = createRegion(name, factory);
      region.registerInterest("ALL_KEYS");
    });

    vm1.invoke("Turn on history", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      CertifiableTestCacheListener<Object, Object> ctl =
          (CertifiableTestCacheListener<Object, Object>) region.getAttributes()
              .getCacheListeners()[0];
      ctl.enableEventHistory();
    });

    // Create some entries on the client
    vm1.invoke("Create entries", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 5; i++) {
        region.put("key-client-" + i, "value-client-" + i);
      }
    });

    // Create some entries on the server
    vm0.invoke("Create entries", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 5; i++) {
        region.put("key-server-" + i, "value-server-" + i);
      }
    });

    vm1.invoke("Validate listener events", () -> {

      AtomicInteger destroyCallbacks = new AtomicInteger(10);
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      final CertifiableTestCacheListener<Object, Object> ctl =
          (CertifiableTestCacheListener<Object, Object>) region.getAttributes()
              .getCacheListeners()[0];

      await().until(() -> {
        List<CacheEvent<Object, Object>> list = ctl.getEventHistory();
        for (CacheEvent ce : list) {
          if (ce.getOperation() == Operation.DESTROY
              && ce.getCallbackArgument() instanceof String) {
            destroyCallbacks.decrementAndGet();
          }
        }
        return destroyCallbacks.get() == 0;
      });

    });

    // Close cache server clients
    vm1.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    });

    // Stop cache server

  }

  private <K, V> RegionFactory<K, V> getBridgeServerRegionAttributes(CacheLoader<K, V> cl,
      CacheWriter<K, V> cw) {
    RegionFactory<K, V> regionFactory = getCache().createRegionFactory();
    if (cl != null) {
      regionFactory.setCacheLoader(cl);
    }
    if (cw != null) {
      regionFactory.setCacheWriter(cw);
    }
    regionFactory.setScope(Scope.DISTRIBUTED_ACK);
    regionFactory.setConcurrencyChecksEnabled(false);
    return regionFactory;
  }

  private <K, V> RegionFactory<K, V> getBridgeServerMirroredAckRegionAttributes(
      CacheLoader<K, V> cl) {
    RegionFactory<K, V> regionFactory = getCache().createRegionFactory();
    if (cl != null) {
      regionFactory.setCacheLoader(cl);
    }

    regionFactory.setScope(Scope.DISTRIBUTED_ACK);
    regionFactory.setConcurrencyChecksEnabled(false);
    regionFactory.setDataPolicy(DataPolicy.REPLICATE);
    return regionFactory;
  }

  /**
   * Tests that updates are not sent to VMs that did not ask for them.
   */
  @Test
  public void test018OnlyRequestedUpdates() {
    final String name1 = getName() + "-1";
    final String name2 = getName() + "-2";

    // Cache server serves up both regions
    vm0.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
      createRegion(name1, factory);
      createRegion(name2, factory);
      startBridgeServer(0);
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    // vm1 sends updates to the server
    vm1.invoke("Create regions", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);

      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);

      Region rgn = createRegion(name1, factory);
      rgn.registerInterestRegex(".*", false, false);
      rgn = createRegion(name2, factory);
      rgn.registerInterestRegex(".*", false, false);
    });

    // vm2 only wants updates to updates to region1
    vm2.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);

      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);

      Region rgn = createRegion(name1, factory);
      rgn.registerInterestRegex(".*", false, false);
      createRegion(name2, factory);
      // no interest registration for region 2

    });

    Stream.of(vm1, vm2).forEach(vm -> vm.invoke("Populate region", () -> {
      Region<Object, Object> region1 = getRootRegion().getSubregion(name1);
      for (int i = 0; i < 10; i++) {
        region1.put(i, "Region1Old" + i);
      }
      Region<Object, Object> region2 = getRootRegion().getSubregion(name2);
      for (int i = 0; i < 10; i++) {
        region2.put(i, "Region2Old" + i);
      }
    }));

    vm1.invoke("Update", () -> {
      Region<Object, Object> region1 = getRootRegion().getSubregion(name1);
      for (int i = 0; i < 10; i++) {
        region1.put(i, "Region1New" + i);
      }
      Region<Object, Object> region2 = getRootRegion().getSubregion(name2);
      for (int i = 0; i < 10; i++) {
        region2.put(i, "Region2New" + i);
      }
    });

    // Wait for updates to be propagated
    vm2.invoke("Validate", () -> {
      Region<Object, Object> region1 = getRootRegion().getSubregion(name1);
      for (int i = 0; i < 10; i++) {
        final int testInt = i;
        await().until(() -> (region1.get(testInt).equals("Region1New" + testInt)));
      }
      Region<Object, Object> region2 = getRootRegion().getSubregion(name2);
      for (int i = 0; i < 10; i++) {
        final int testInt = i;
        await().until(() -> region2.get(testInt).equals(("Region2Old" + testInt)));
      }
    });

    vm1.invoke("Close Pool", () -> {
      // Terminate region1's Pool
      Region<Object, Object> region1 = getRootRegion().getSubregion(name1);
      region1.localDestroyRegion();
      // Terminate region2's Pool
      Region<Object, Object> region2 = getRootRegion().getSubregion(name2);
      region2.localDestroyRegion();
    });

    vm2.invoke("Close Pool", () -> {
      // Terminate region1's Pool
      Region<Object, Object> region1 = getRootRegion().getSubregion(name1);
      region1.localDestroyRegion();
    });


  }

  /**
   * Tests interest key registration.
   */
  @Test
  public void test019InterestKeyRegistration() throws CacheException {
    final String name = getName();

    // Create cache server
    vm0.invoke("Create Cache Server", () -> {
      CacheLoader<Object, Object> cl = new CacheLoader<Object, Object>() {
        @Override
        public Object load(LoaderHelper helper) {
          return helper.getKey();
        }

        @Override
        public void close() {

        }
      };
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(cl, null);
      createRegion(name, factory);
      startBridgeServer(0);
    });

    // Create cache server clients
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    Stream.of(vm1, vm2).forEach(vm -> vm.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      // create bridge writer
      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      createRegion(name, factory);
    }));

    // Get values for key 1 and key 2 so that there are entries in the clients.
    // Register interest in one of the keys.
    vm1.invoke("Create Entries and Register Interest", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      assertThat(region.get("key-1")).isEqualTo("key-1");
      assertThat(region.get("key-2")).isEqualTo("key-2");
      region.registerInterest("key-1");
    });

    vm2.invoke("Create Entries and Register Interest", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      assertThat(region.get("key-1")).isEqualTo("key-1");
      assertThat(region.get("key-2")).isEqualTo("key-2");
      region.registerInterest("key-2");
    });

    // Put new values and validate updates (VM1)
    vm1.invoke("Put New Values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put("key-1", "vm1-key-1");
      region.put("key-2", "vm1-key-2");
      // Verify that no invalidates occurred to this region
      assertThat(region.getEntry("key-1").getValue()).isEqualTo("vm1-key-1");
      assertThat(region.getEntry("key-2").getValue()).isEqualTo("vm1-key-2");
    });

    vm2.invoke("Validate Entries", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      // Verify that 'key-2' was updated, but 'key-1' was not
      // and contains the original value
      await().until(() -> region.getEntry("key-1").getValue().equals("key-1"));
      await().until(() -> region.getEntry("key-2").getValue().equals("vm1-key-2"));
    });

    // Put new values and validate updates (VM2)
    vm2.invoke("Put New Values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put("key-1", "vm2-key-1");
      region.put("key-2", "vm2-key-2");
      // Verify that no updates occurred to this region
      assertThat(region.getEntry("key-1").getValue()).isEqualTo("vm2-key-1");
      assertThat(region.getEntry("key-2").getValue()).isEqualTo("vm2-key-2");
    });

    vm1.invoke("Validate Entries", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      // Verify that 'key-1' was updated, but 'key-2' was not
      // and contains the original value
      await().until(() -> region.getEntry("key-2").getValue().equals("vm1-key-2"));
      await().until(() -> region.getEntry("key-1").getValue().equals("vm2-key-1"));
    });

    // Unregister interest
    vm1.invoke("Unregister Interest", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.unregisterInterest("key-1");
    });

    vm2.invoke("Unregister Interest", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.unregisterInterest("key-2");
    });

    // Put new values and validate updates (VM1)
    vm1.invoke("Put New Values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put("key-1", "vm1-key-1-again");
      region.put("key-2", "vm1-key-2-again");
      // Verify that no updates occurred to this region
      assertThat(region.getEntry("key-1").getValue()).isEqualTo("vm1-key-1-again");
      assertThat(region.getEntry("key-2").getValue()).isEqualTo("vm1-key-2-again");
    });

    vm2.invoke("Validate Entries", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      // Verify that neither 'key-1' 'key-2' was updated
      // and contain the original value
      await().until(() -> region.getEntry("key-1").getValue().equals("vm2-key-1"));
      await().until(() -> region.getEntry("key-2").getValue().equals("vm2-key-2"));
    });

    // Put new values and validate updates (VM2)
    vm2.invoke("Put New Values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put("key-1", "vm2-key-1-again");
      region.put("key-2", "vm2-key-2-again");
      // Verify that no updates occurred to this region
      assertThat(region.getEntry("key-1").getValue()).isEqualTo("vm2-key-1-again");
      assertThat(region.getEntry("key-2").getValue()).isEqualTo("vm2-key-2-again");
    });

    vm1.invoke("Validate Entries", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      // Verify that neither 'key-1' 'key-2' was updated
      // and contain the original value
      await().until(
          () -> "vm1-key-1-again".compareTo((String) region.getEntry("key-1").getValue()) == 0);
      await().until(() -> region.getEntry("key-2").getValue() == "vm1-key-2-again");
    });

    // Unregister interest again (to verify that a client can unregister interest
    // in a key that its not interested in with no problem.
    vm1.invoke("Unregister Interest", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.unregisterInterest("key-1");
    });

    vm2.invoke("Unregister Interest", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.unregisterInterest("key-2");
    });

    // Close cache server clients
    Stream.of(vm1, vm2).forEach(vm -> vm.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    }));

    // Stop cache server

  }

  /**
   * Tests interest list registration.
   */
  @Test
  public void test020InterestListRegistration() throws CacheException {
    final String name = getName();

    // Create cache server
    vm0.invoke("Create Cache Server", () -> {

      RegionFactory<Object, Object> regionFactory = getCache().createRegionFactory();
      regionFactory.setScope(Scope.DISTRIBUTED_ACK);
      regionFactory.setConcurrencyChecksEnabled(false);
      Region<Object, Object> region = createRegion(name, regionFactory);
      startBridgeServer(0);
      for (int i = 0; i <= 6; i++) {
        region.put("key-" + i, "key-" + i);
      }
    });

    // Create cache server clients
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    Stream.of(vm1, vm2).forEach(vm -> vm.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      // create bridge writer
      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      createRegion(name, factory);
    }));

    // Get values for key 1 and key 6 so that there are entries in the clients.
    // Register interest in a list of keys.
    vm1.invoke("Create Entries and Register Interest", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      assertThat(region.get("key-1")).isEqualTo("key-1");
      assertThat(region.get("key-6")).isEqualTo("key-6");

      List<Object> list = new ArrayList<>();
      list.add("key-1");
      list.add("key-2");
      list.add("key-3");
      list.add("key-4");
      list.add("key-5");
      region.registerInterest(list);
    });

    vm2.invoke("Create Entries and Register Interest", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      assertThat(region.get("key-1")).isEqualTo("key-1");
      assertThat(region.get("key-6")).isEqualTo("key-6");
    });

    // Put new values and validate updates (VM2)
    vm2.invoke("Put New Values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put("key-1", "vm2-key-1");
      region.put("key-6", "vm2-key-6");
      // Verify that no updates occurred to this region
      assertThat(region.getEntry("key-1").getValue()).isEqualTo("vm2-key-1");
      assertThat(region.getEntry("key-6").getValue()).isEqualTo("vm2-key-6");
    });

    vm1.invoke("Validate Entries", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      // Verify that 'key-1' was updated
      await().until(() -> region.getEntry("key-1").getValue().equals("vm2-key-1"));
      // Verify that 'key-6' was not invalidated
      await().until(() -> region.getEntry("key-6").getValue().equals("key-6"));
    });

    // Close cache server clients

    vm1.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    });

    vm2.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    });

    // Stop cache server

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
  private void createDynamicRegionCache(String connectionPoolName) {
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
    await().alias("Waiting for entry " + key + " on region " + r)
        .until(() -> r.containsValueForKey(key));
  }

  private static <K, V> Region<K, V> waitForSubRegion(final Region<K, V> r,
      final String subRegName) {
    await().alias("Waiting for subregion " + subRegName)
        .until(() -> r.getSubregion(subRegName) != null);
    return r.getSubregion(subRegName);
  }

  @Test
  public void test021ClientGetOfInvalidServerEntry() throws CacheException {
    final String regionName1 = getName() + "-1";

    VM server1 = VM.getVM(0);
    VM client = VM.getVM(2);

    // Create server1.
    server1.invoke("Create Cache Server and values", () -> {
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.REPLICATE);
      factory.setConcurrencyChecksEnabled(false);
      createRegion(regionName1, factory);
      startBridgeServer(0);

      Region<Object, Object> region1 = getRootRegion().getSubregion(regionName1);
      // create it invalid
      region1.create("key-string-1", null);
    });

    final int port = server1.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    // now try it with a local scope

    client.invoke("Create region 2 and get values on client", () -> {
      getLonerSystem();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      configureConnectionPool(factory, host0, new int[] {port}, false, -1, -1,
          null);

      createRegion(regionName1, factory);

      Region<Object, Object> region1 = getRootRegion().getSubregion(regionName1);
      assertThat(region1.getEntry("key-string-1")).isNull();
      assertThat(region1.get("key-string-1")).isNull();
    });
  }

  @Test
  public void test022ClientRegisterUnregisterRequests() throws CacheException {
    final String regionName1 = getName() + "-1";

    VM server1 = vm0;
    VM client = vm2;

    // Create server1.
    server1.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.REPLICATE);
      factory.setConcurrencyChecksEnabled(false);
      createRegion(regionName1, factory);

      startBridgeServer(0);
    });

    final int port = server1.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    client.invoke("Create region", () -> {
      getLonerSystem();
      getCache();

      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);

      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1,
          null);

      Region<Object, Object> region11 = createRegion(regionName1, factory);
      region11.getAttributesMutator().addCacheListener(new CertifiableTestCacheListener<>());
    });

    // Init values at server.
    server1.invoke("Create values", () -> {
      Region<Object, Object> region1 = getRootRegion().getSubregion(regionName1);
      for (int i = 0; i < 20; i++) {
        region1.put("key-string-" + i, "value-" + i);
      }
    });

    // Put some values on the client.
    client.invoke("Put values client and close pool", () -> {
      Region<Object, Object> region1 = getRootRegion().getSubregion(regionName1);

      for (int i = 0; i < 10; i++) {
        region1.put("key-string-" + i, "client-value-" + i);
      }

      String pName = region1.getAttributes().getPoolName();
      region1.localDestroyRegion();
      PoolImpl p = (PoolImpl) PoolManager.find(pName);
      p.destroy();
    });

    server1.invoke("validate Client Register UnRegister", () -> {
      for (CacheServer cacheServer : getCache().getCacheServers()) {
        InternalCacheServer bsi = (InternalCacheServer) cacheServer;
        final CacheClientNotifierStats ccnStats =
            bsi.getAcceptor().getCacheClientNotifier().getStats();

        await().until(() -> ccnStats.getClientRegisterRequests() == ccnStats
            .getClientUnRegisterRequests());
        assertThat(ccnStats.getClientRegisterRequests())
            .describedAs("HealthMonitor Client Register/UnRegister mismatch.")
            .isEqualTo(ccnStats.getClientUnRegisterRequests());
      }
    });
  }

  /**
   * Tests the containsKeyOnServer operation of the {@link Pool}
   *
   * @since GemFire 5.0.2
   */
  @Test
  public void test023ContainsKeyOnServer() throws CacheException {
    final String name = getName();

    vm0.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setConcurrencyChecksEnabled(false);
      createRegion(name, factory);
      startBridgeServer(0);
    });

    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    Stream.of(vm1, vm2).forEach(vm -> vm.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      configureConnectionPool(factory, host0, new int[] {port}, false, -1, -1, null);
      createRegion(name, factory);
    }));

    final Integer key1 = 0;
    final String key2 = "0";
    vm2.invoke("Contains key on server", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      boolean containsKey;
      containsKey = region.containsKeyOnServer(key1);
      assertThat(containsKey).isFalse();
      containsKey = region.containsKeyOnServer(key2);
      assertThat(containsKey).isFalse();
    });

    vm1.invoke("Put values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(0, 0);
      region.put("0", "0");
    });

    vm2.invoke("Contains key on server", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      boolean containsKey = region.containsKeyOnServer(key1);
      assertThat(containsKey).isTrue();
      containsKey = region.containsKeyOnServer(key2);
      assertThat(containsKey).isTrue();
    });

    for (VM vm : Arrays.asList(vm1, vm2)) {
      vm.invoke("Close Pool", () -> {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      });
    }


  }

  /**
   * Tests that invoking {@link Region#create} with a <code>null</code> value does the right thing
   * with the {@link Pool}.
   *
   * @since GemFire 3.5
   */
  @Test
  public void test024CreateNullValue() throws CacheException {
    final String name = getName();

    final Object createCallbackArg = "CREATE CALLBACK ARG";

    vm0.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
      createRegion(name, factory);
      startBridgeServer(0);
    });

    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    Stream.of(vm1, vm2).forEach(vm -> vm.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);

      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      createRegion(name, factory);
    }));

    vm2.invoke("Create nulls", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        region.create(i, null, createCallbackArg);
      }
    });

    vm2.invoke("Verify invalidates", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Region.Entry entry = region.getEntry(i);
        await().until(() -> entry != null);
        await().until(() -> entry.getValue() == null);
      }
    });

    vm1.invoke("Attempt to create values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        region.create(i, "new" + i);
      }
    });

    vm2.invoke("Verify invalidates", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        Region.Entry entry = region.getEntry(i);
        await().until(() -> entry != null);
        await().until(() -> entry.getValue() == null);
      }
    });

    Stream.of(vm1, vm2).forEach(vm -> vm.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    }));
  }

  /**
   * Tests that a {@link Region#localDestroy} is not propagated to the server and that a {@link
   * Region#destroy} is. Also makes sure that callback arguments are passed correctly.
   */
  @Test
  public void test025Destroy() throws CacheException {
    final String name = getName();

    final Object callbackArg = "DESTROY CALLBACK";

    vm0.invoke("Create Cache Server", () -> {
      CacheWriter<Object, Object> cw = new TestCacheWriter<Object, Object>() {
        @Override
        public void beforeCreate2(EntryEvent event) throws CacheWriterException {

        }

        @Override
        public void beforeDestroy2(EntryEvent event) throws CacheWriterException {
          Object beca = event.getCallbackArgument();
          assertThat(callbackArg).isEqualTo(beca);
        }
      };
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, cw);
      createRegion(name, factory);
      startBridgeServer(0);
    });

    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    vm1.invoke("Create and Load region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);

      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);

      Region rgn = createRegion(name, factory);
      rgn.registerInterestRegex(".*", false, false);

      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        region.put(i, String.valueOf(i));
      }
    });

    vm2.invoke("Create and Load region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);

      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);

      Region rgn = createRegion(name, factory);
      rgn.registerInterestRegex(".*", false, false);

      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        assertThat(String.valueOf(i)).isEqualTo(region.get(i));
      }
    });

    vm1.invoke("Local destroy", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        region.localDestroy(i);
      }
    });

    vm2.invoke("No destroy propagate", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        assertThat(String.valueOf(i)).isEqualTo(region.get(i));
      }
    });

    vm1.invoke("Fetch from server", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        assertThat(String.valueOf(i)).isEqualTo(region.get(i));
      }
    });

    vm0.invoke("Check no server cache writer", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      TestCacheWriter writer = getTestWriter(region);
      writer.wasInvoked();
    });

    vm1.invoke(() -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        region.destroy(i, callbackArg);
      }

      for (int i = 0; i < 10; i++) {
        final int testInt = i;
        await().until(() -> region.getEntry(testInt) == null);
      }
    });

    vm2.invoke("Validate destroy propagate", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        final int testInt = i;
        await().until(() -> region.getEntry(testInt) == null);
      }
    });

    Stream.of(vm1, vm2).forEach(vm -> vm.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    }));
  }

  /**
   * Tests that a {@link Region#localDestroyRegion} is not propagated to the server and that a
   * {@link Region#destroyRegion} is. Also makes sure that callback arguments are passed correctly.
   */
  @Ignore("TODO")
  @Test
  public void testDestroyRegion() throws CacheException {
    final String name = getName();

    final Object callbackArg = "DESTROY CALLBACK";

    vm0.invoke("Create Cache Server", () -> {
      CacheWriter<Object, Object> cw = new TestCacheWriter<Object, Object>() {
        @Override
        public void beforeCreate2(EntryEvent event) throws CacheWriterException {

        }

        @Override
        public void beforeRegionDestroy2(RegionEvent event) throws CacheWriterException {
          assertThat(callbackArg).isEqualTo(event.getCallbackArgument());
        }
      };
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, cw);
      createRegion(name, factory);
      startBridgeServer(0);
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    SerializableRunnable create = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
        createRegion(name, factory);
      }
    };

    Stream.of(vm1, vm2).forEach(vm -> vm.invoke("Create region", create));

    vm1.invoke("Local destroy region", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
      assertThat(getRootRegion().getSubregion(name)).isNull();
    });

    vm2.invoke("No destroy propagate", () -> {
      assertThat(getRootRegion().getSubregion(name)).isNotNull();
    });

    vm0.invoke("Check no server cache writer", () -> {
      TestCacheWriter writer = getTestWriter(getRootRegion().getSubregion(name));
      writer.wasInvoked();
    });

    vm1.invoke(create);

    vm1.invoke("Distributed destroy region", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      assertThat(region).isNotNull();
      region.destroyRegion(callbackArg);
      assertThat(getRootRegion().getSubregion(name)).isNull();
    });

    vm2.invoke("Verify destroy propagate",
        () -> await().until(() -> getRootRegion().getSubregion(name) == null));
  }

  /**
   * Tests interest list registration with callback arg with DataPolicy.EMPTY and
   * InterestPolicy.ALL
   */
  @Test
  public void test026DPEmptyInterestListRegistrationWithCallbackArg() throws CacheException {
    final String name = getName();

    // Create cache server
    vm0.invoke("Create Cache Server", () -> {
      CacheLoader<Object, Object> cl = new CacheLoader<Object, Object>() {
        @Override
        public Object load(LoaderHelper helper) {
          return helper.getKey();
        }

        @Override
        public void close() {}
      };
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(cl, null);
      createRegion(name, factory);
      startBridgeServer(0);
    });

    // Create cache server clients
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    vm1.invoke("Create region", () -> {
      getLonerSystem();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      // create bridge writer
      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      factory.addCacheListener(new ControlListener());
      factory.setDataPolicy(DataPolicy.EMPTY);
      factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      createRegion(name, factory);
    });

    vm2.invoke("Create publisher region", () -> {
      getLonerSystem();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      // create bridge writer
      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1,
          null);
      factory.addCacheListener(new ControlListener());
      factory.setDataPolicy(DataPolicy.EMPTY); // make sure empty works with client publishers
      createRegion(name, factory);
    });

    // VM1 Register interest
    vm1.invoke("Create Entries and Register Interest", () -> getRootRegion().getSubregion(name)
        .registerInterest("key-1", InterestResultPolicy.NONE));

    // VM2 Put entry (this will cause a create event in both VM1 and VM2)
    vm2.invoke(() -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.create("key-1", "key-1-create", "key-1-create");

      region.put("key-1", "key-1-update", "key-1-update");

      region.destroy("key-1", "key-1-destroy");
    });

    vm1.invoke("Verify events", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      ControlListener listener = (ControlListener) region.getAttributes().getCacheListeners()[0];
      int eventCount = 3;
      listener.waitWhileNotEnoughEvents(eventCount);
      assertThat(eventCount).isEqualTo(listener.events.size());

      {
        EventWrapper ew = listener.events.get(0);
        assertThat(TYPE_CREATE).isEqualTo(ew.type);
        Object key = "key-1";
        assertThat(key).isEqualTo(ew.event.getKey());
        assertThat(ew.event.getOldValue()).isNull();
        assertThat(ew.event.isOldValueAvailable()).isFalse(); // failure
        assertThat("key-1-create").isEqualTo(ew.event.getNewValue());
        assertThat(Operation.CREATE).isEqualTo(ew.event.getOperation());
        assertThat("key-1-create").isEqualTo(ew.event.getCallbackArgument());
        assertThat(ew.event.isOriginRemote()).isTrue();

        ew = listener.events.get(1);
        assertThat(TYPE_UPDATE).isEqualTo(ew.type);
        assertThat(key).isEqualTo(ew.event.getKey());
        assertThat(ew.event.getOldValue()).isNull();
        assertThat(ew.event.isOldValueAvailable()).isFalse();
        assertThat("key-1-update").isEqualTo(ew.event.getNewValue());
        assertThat(Operation.UPDATE).isEqualTo(ew.event.getOperation());
        assertThat("key-1-update").isEqualTo(ew.event.getCallbackArgument());
        assertThat(ew.event.isOriginRemote()).isTrue();

        ew = listener.events.get(2);
        assertThat(TYPE_DESTROY).isEqualTo(ew.type);
        assertThat("key-1-destroy").isEqualTo(ew.arg);
        assertThat(key).isEqualTo(ew.event.getKey());
        assertThat(ew.event.getOldValue()).isNull();
        assertThat(ew.event.isOldValueAvailable()).isFalse();
        assertThat(ew.event.getNewValue()).isNull();
        assertThat(Operation.DESTROY).isEqualTo(ew.event.getOperation());
        assertThat("key-1-destroy").isEqualTo(ew.event.getCallbackArgument());
        assertThat(ew.event.isOriginRemote()).isTrue();
      }
    });

    Stream.of(vm1, vm2).forEach(vm -> {
      // Close cache server clients
      vm.invoke("Close Pool", () -> {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      });
    });
  }

  /**
   * Tests interest list registration with callback arg with DataPolicy.EMPTY and
   * InterestPolicy.CACHE_CONTENT
   */
  @Test
  public void test027DPEmptyCCInterestListRegistrationWithCallbackArg() throws CacheException {
    final String name = getName();

    // Create cache server
    vm0.invoke("Create Cache Server", () -> {
      CacheLoader<Object, Object> cl = new CacheLoader<Object, Object>() {
        @Override
        public Object load(LoaderHelper helper) {
          return helper.getKey();
        }

        @Override
        public void close() {

        }
      };
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(cl, null);
      createRegion(name, factory);
      startBridgeServer(0);
    });

    // Create cache server clients
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    vm1.invoke("Create region", () -> {
      getLonerSystem();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      factory.addCacheListener(new ControlListener());
      factory.setDataPolicy(DataPolicy.EMPTY);
      factory
          .setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT));
      createRegion(name, factory);
    });
    vm2.invoke("Create publisher region", () -> {
      getLonerSystem();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      // create bridge writer
      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1,
          null);
      factory.addCacheListener(new ControlListener());
      factory.setDataPolicy(DataPolicy.EMPTY); // make sure empty works with client publishers
      createRegion(name, factory);
    });

    // VM1 Register interest
    vm1.invoke("Create Entries and Register Interest", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);

      // This call will cause no value to be put into the region
      region.registerInterest("key-1", InterestResultPolicy.NONE);
    });

    // VM2 Put entry (this will cause a create event in both VM1 and VM2)
    vm2.invoke("Put Value", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.create("key-1", "key-1-create", "key-1-create");
    });

    // VM2 Put entry (this will cause an update event in both VM1 and VM2)
    vm2.invoke("Put Value", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put("key-1", "key-1-update", "key-1-update");
    });

    // VM2 Destroy entry (this will cause a destroy event)
    vm2.invoke("Destroy Entry", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.destroy("key-1", "key-1-destroy");
    });

    vm1.invoke("Verify events", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      ControlListener listener = (ControlListener) region.getAttributes().getCacheListeners()[0];

      await().until(() -> listener.events.size() == 0);
    });

    // Close cache server clients
    Stream.of(vm1, vm2).forEach(vm -> vm.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    }));
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
    final String name = getName();

    final VM client1 = vm0;
    final VM srv1 = vm2;
    final VM srv2 = vm3;

    final String key1 = name + "-key1";
    final String value1 = name + "-val1";
    final String key2 = name + "-key2";
    final String value2 = name + "-val2";
    final String key3 = name + "-key3";
    final String value3 = name + "-val3";

    // setup servers
    Stream.of(srv1, srv2).forEach(vm -> vm.invoke("Create Cache Server", () -> {
      createDynamicRegionCache(null); // Creates a new DS and Cache
      assertThat(DynamicRegionFactory.get().isOpen()).isTrue();
      startBridgeServer(0);

      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.REPLICATE);
      factory.setConcurrencyChecksEnabled(false);
      Region<Object, Object> region = createRootRegion(name, factory);
      region.put(key1, value1);
      assertThat(region.get(key1)).isEqualTo(value1);
    }));
    final String srv1Host = NetworkUtils.getServerHostName();
    final int srv1Port = srv1.invoke(ConnectionPoolDUnitTest::getCacheServerPort);

    final int srv2Port = srv2.invoke(ConnectionPoolDUnitTest::getCacheServerPort);

    // setup clients, do basic tests to make sure pool with notifier work as advertised
    client1.invoke("Create Cache Client", () -> {
      createLonerDS();
      AttributesFactory<Object, Object> factory = new AttributesFactory<>();
      factory.setConcurrencyChecksEnabled(false);
      Pool cp = configureConnectionPool(factory, srv1Host, new int[] {srv1Port,
          srv2Port}, true, -1, -1, null);
      assertThat(cp).isNotNull();

      {
        final PoolImpl pool = (PoolImpl) cp;

        await().untilAsserted(() -> {
          assertThat(pool.getPrimary()).isNotNull();
          assertThat(pool.getRedundants()).isNotEmpty();
        });

        assertThat(pool.getRedundants())
            .describedAs("backups=" + pool.getRedundants() + " expected=" + 1).isNotEmpty();
      }

      createDynamicRegionCache("testPool");

      assertThat(DynamicRegionFactory.get().isOpen()).isTrue();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      factory.addCacheListener(new CertifiableTestCacheListener<>());
      Region<Object, Object> region = createRootRegion(name, factory.create());

      assertThat(region.getEntry(key1)).isNull();
      region.registerInterestRegex(".*", InterestResultPolicy.KEYS_VALUES); // this should match
      // the key
      assertThat(value1).isEqualTo(region.getEntry(key1).getValue()); // Update via registered
      // interest

      assertThat(region.getEntry(key2)).isNull();
      region.put(key2, value2); // use the Pool
      assertThat(value2).isEqualTo(region.getEntry(key2).getValue()); // Ensure that the notifier
      // didn't un-do the put, bug 35355

      region.put(key3, value3); // setup a key for invalidation from a notifier;
    });

    srv1.invoke("Validate Server1 update", () -> {
      CacheClientNotifier ccn = getInstance();
      final CacheClientNotifierStats ccnStats = ccn.getStats();
      final int eventCount = ccnStats.getEvents();
      Region<Object, Object> region = getRootRegion(name);
      assertThat(region).isNotNull();
      assertThat(value2)
          .isEqualTo(region.getEntry(key2).getValue()); // Validate the Pool worked,
      // getEntry works because of the mirror
      assertThat(value3).isEqualTo(region.getEntry(key3).getValue()); // Make sure we have the
      // other entry to use for notification
      region.put(key3, value1); // Change k3, sending some data to the client notifier

      // Wait for the update to propagate to the clients
      await("waiting for ccnStat").until(() -> ccnStats.getEvents() > eventCount);
    });

    srv2.invoke("Validate Server2 update", () -> {
      Region<Object, Object> rootRegion = getRootRegion(name);
      assertThat(rootRegion).isNotNull();
      assertThat(value2).isEqualTo(rootRegion.getEntry(key2).getValue()); // Validate the Pool
      // worked, getEntry works because of the mirror
      assertThat(value1).isEqualTo(rootRegion.getEntry(key3).getValue()); // From peer update;
    });

    client1.invoke("Validate Client notification", () -> {
      Region<Object, Object> rootRegion = getRootRegion(name);
      assertThat(rootRegion).isNotNull();
      CertifiableTestCacheListener<Object, Object> ctl =
          (CertifiableTestCacheListener<Object, Object>) rootRegion.getAttributes()
              .getCacheListeners()[0];
      ctl.waitForUpdated(key3);
      assertThat(value1).isEqualTo(rootRegion.getEntry(key3).getValue()); // Ensure that the
      // notifier updated the entry

    });
    // Ok, now we are ready to do some dynamic region action!
    final String v1Dynamic = value1 + "dynamic";
    final String dynFromClientName = name + "-dynamic-client";
    final String dynFromServerName = name + "-dynamic-server";
    client1.invoke("Client dynamic region creation", () -> {
      assertThat(DynamicRegionFactory.get().isOpen()).isTrue();
      Region<Object, Object> region = getRootRegion(name);
      assertThat(region).isNotNull();
      Region<Object, Object> dynamicRegion =
          DynamicRegionFactory.get().createDynamicRegion(name, dynFromClientName);
      assertThat(dynamicRegion.get(key1)).isNull(); // This should be enough to validate the
      // creation on the
      // server
      dynamicRegion.put(key1, v1Dynamic);
      assertThat(v1Dynamic).isEqualTo(dynamicRegion.getEntry(key1).getValue());
    });

    // Assert the servers have the dynamic region and the new value
    Stream.of(srv1, srv2)
        .forEach(vm -> vm.invoke("Validate dynamic region creation on server", () -> {
          Region<Object, Object> rootRegion = getRootRegion(name);
          assertThat(rootRegion).isNotNull();

          await().untilAsserted(() -> {
            Region<Object, Object> region = rootRegion.getSubregion(dynFromClientName);
            assertThat(region).isNotNull();
            assertThat(getCache().getRegion(name + SEPARATOR + dynFromClientName))
                .isNotNull();
          });

          Region<Object, Object> dynamicRegion = rootRegion.getSubregion(dynFromClientName);
          assertThat(v1Dynamic).isEqualTo(dynamicRegion.getEntry(key1).getValue());
        }));

    // now delete the dynamic region and see if it goes away on servers
    client1.invoke("Client dynamic region destruction", () -> {
      assertThat(DynamicRegionFactory.get().isActive()).isTrue();
      Region<Object, Object> r = getRootRegion(name);
      assertThat(r).isNotNull();
      String drName = r.getFullPath() + SEPARATOR + dynFromClientName;

      assertThat(getCache().getRegion(drName)).isNotNull();
      DynamicRegionFactory.get().destroyDynamicRegion(drName);
      assertThat(getCache().getRegion(drName)).isNull();
    });

    // Assert the servers no longer have the dynamic region

    Stream.of(srv1, srv2)
        .forEach(vm -> vm.invoke("Validate dynamic region destruction on server", () -> {
          Region<Object, Object> r = getRootRegion(name);
          assertThat(r).isNotNull();
          String drName = r.getFullPath() + SEPARATOR + dynFromClientName;
          assertThat(getCache().getRegion(drName)).isNull();
          try {
            DynamicRegionFactory.get().destroyDynamicRegion(drName);
            fail("expected RegionDestroyedException");
          } catch (RegionDestroyedException ignored) {
          }
        }));

    // Now try the reverse, create a dynamic region on the server and see if the client
    // has it
    srv2.invoke("Server dynamic region creation", () -> {
      Region<Object, Object> r = getRootRegion(name);
      assertThat(r).isNotNull();
      Region<Object, Object> dr =
          DynamicRegionFactory.get().createDynamicRegion(name, dynFromServerName);
      assertThat(dr.get(key1)).isNull();
      dr.put(key1, v1Dynamic);
      assertThat(v1Dynamic).isEqualTo(dr.getEntry(key1).getValue());
    });

    // Assert the servers have the dynamic region and the new value
    srv1.invoke("Validate dynamic region creation propagation to other server", () -> {
      Region<Object, Object> r = getRootRegion(name);
      assertThat(r).isNotNull();
      Region<Object, Object> dr = waitForSubRegion(r, dynFromServerName);
      assertThat(dr).isNotNull();
      assertThat(getCache().getRegion(name + SEPARATOR + dynFromServerName))
          .isNotNull();
      waitForEntry(dr, key1);
      assertThat(dr.getEntry(key1)).isNotNull();
      assertThat(v1Dynamic).isEqualTo(dr.getEntry(key1).getValue());
    });

    // Assert the clients have the dynamic region and the new value
    client1.invoke("Validate dynamic region creation on client", () -> {
      Region<Object, Object> r = getRootRegion(name);
      assertThat(r).isNotNull();
      Region<Object, Object> dr;

      await().pollInterval(1, SECONDS).until(() -> r.getSubregion(dynFromServerName) != null
          && getCache().getRegion(name + SEPARATOR + dynFromServerName) != null);

      dr = r.getSubregion(dynFromServerName);
      waitForEntry(dr, key1);
      assertThat(dr.getEntry(key1)).isNotNull();
      assertThat(v1Dynamic).isEqualTo(dr.getEntry(key1).getValue());
    });

    // now delete the dynamic region on a server and see if it goes away on client
    srv2.invoke("Server dynamic region destruction", () -> {
      assertThat(DynamicRegionFactory.get().isActive()).isTrue();
      Region<Object, Object> r = getRootRegion(name);
      assertThat(r).isNotNull();
      String drName = r.getFullPath() + SEPARATOR + dynFromServerName;

      assertThat(getCache().getRegion(drName)).isNotNull();
      DynamicRegionFactory.get().destroyDynamicRegion(drName);
      assertThat(getCache().getRegion(drName)).isNull();
    });

    srv1.invoke("Validate dynamic region destruction on other server", () -> {
      Region<Object, Object> r = getRootRegion(name);
      assertThat(r).isNotNull();
      String drName = r.getFullPath() + SEPARATOR + dynFromServerName;

      await().ignoreException(RegionDestroyedException.class)
          .until(() -> getCache().getRegion(drName) == null);
      assertThat(getCache().getRegion(drName)).isNull();
    });

    // Assert the clients no longer have the dynamic region
    client1.invoke("Validate dynamic region destruction on client", () -> {
      Region<Object, Object> r = getRootRegion(name);
      assertThat(r).isNotNull();
      String drName = r.getFullPath() + SEPARATOR + dynFromServerName;
      await().ignoreException(RegionDestroyedException.class)
          .until(() -> getCache().getRegion(drName) == null);

      Throwable thrown =
          catchThrowable(() -> DynamicRegionFactory.get().destroyDynamicRegion(drName));
      assertThat(thrown).isInstanceOf(RegionDestroyedException.class);
    });
  }

  /**
   * Test for bug 36279
   */
  @Test
  public void test029EmptyByteArray() throws CacheException {
    final String name = getName();

    final Object createCallbackArg = "CREATE CALLBACK ARG";

    vm0.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
      createRegion(name, factory);
      startBridgeServer(0);
    });

    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    vm1.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);

      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      createRegion(name, factory);

      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 1; i++) {
        region.create(i, new byte[0], createCallbackArg);
      }

      for (int i = 0; i < 1; i++) {
        Region.Entry entry = region.getEntry(i);
        assertThat(entry).isNotNull();
        byte[] value = (byte[]) entry.getValue();
        assertThat(value).isNotNull();
        assertThat(value).isEmpty();
      }
    });

    vm0.invoke("Verify values on server", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 1; i++) {
        Region.Entry entry = region.getEntry(i);
        assertThat(entry).isNotNull();
        byte[] value = (byte[]) entry.getValue();
        assertThat(value).isNotNull();
        assertThat(value).isEmpty();
      }
    });

    vm1.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    });
  }

  /**
   * Tests interest list registration with callback arg
   */
  @Test
  public void test030InterestListRegistrationWithCallbackArg() throws CacheException {
    final String name = getName();

    // Create cache server
    vm0.invoke("Create Cache Server", () -> {
      CacheLoader<Object, Object> cl = new CacheLoader<Object, Object>() {
        @Override
        public Object load(LoaderHelper helper) {
          return helper.getKey();
        }

        @Override
        public void close() {

        }
      };
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(cl, null);
      createRegion(name, factory);
      startBridgeServer(0);
    });

    // Create cache server clients
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    Stream.of(vm1, vm2).forEach(vm -> vm.invoke("Create region", () -> {
      getLonerSystem();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      // create bridge writer
      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      factory.addCacheListener(new ControlListener());
      createRegion(name, factory);
    }));

    // VM1 Register interest
    vm1.invoke("Create Entries and Register Interest", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);

      // This call will cause no value to be put into the region
      region.registerInterest("key-1", InterestResultPolicy.NONE);

    });

    // VM2 Put entry (this will cause a create event in both VM1 and VM2)
    vm2.invoke("Put Value", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.create("key-1", "key-1-create", "key-1-create");
    });

    // VM2 Put entry (this will cause an update event in both VM1 and VM2)
    vm2.invoke("Put Value", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put("key-1", "key-1-update", "key-1-update");
    });

    // VM2 Destroy entry (this will cause a destroy event)
    vm2.invoke("Destroy Entry", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.destroy("key-1", "key-1-destroy");
    });

    vm1.invoke("Verify events", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      ControlListener listener = (ControlListener) region.getAttributes().getCacheListeners()[0];
      int eventCount = 3;
      listener.waitWhileNotEnoughEvents(eventCount);
      assertThat(eventCount).isEqualTo(listener.events.size());

      {
        EventWrapper ew = listener.events.get(0);
        assertThat(ew.type).isEqualTo(TYPE_CREATE);
        Object key = "key-1";
        assertThat(key).isEqualTo(ew.event.getKey());
        assertThat(ew.event.getOldValue()).isNull();
        assertThat("key-1-create").isEqualTo(ew.event.getNewValue());
        assertThat(Operation.CREATE).isEqualTo(ew.event.getOperation());
        assertThat("key-1-create").isEqualTo(ew.event.getCallbackArgument());
        assertThat(ew.event.isOriginRemote()).isTrue();

        ew = listener.events.get(1);
        assertThat(ew.type).isEqualTo(TYPE_UPDATE);
        assertThat(key).isEqualTo(ew.event.getKey());
        assertThat("key-1-create").isEqualTo(ew.event.getOldValue());
        assertThat("key-1-update").isEqualTo(ew.event.getNewValue());
        assertThat(Operation.UPDATE).isEqualTo(ew.event.getOperation());
        assertThat("key-1-update").isEqualTo(ew.event.getCallbackArgument());
        assertThat(ew.event.isOriginRemote()).isTrue();

        ew = listener.events.get(2);
        assertThat(ew.type).isEqualTo(TYPE_DESTROY);
        assertThat("key-1-destroy").isEqualTo(ew.arg);
        assertThat(key).isEqualTo(ew.event.getKey());
        assertThat("key-1-update").isEqualTo(ew.event.getOldValue());
        assertThat(ew.event.getNewValue()).isNull();
        assertThat(Operation.DESTROY).isEqualTo(ew.event.getOperation());
        assertThat("key-1-destroy").isEqualTo(ew.event.getCallbackArgument());
        assertThat(ew.event.isOriginRemote()).isTrue();
      }
    });

    // Close cache server clients
    Stream.of(vm1, vm2).forEach(vm -> vm.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    }));
  }

  /**
   * Tests the keySetOnServer operation of the {@link Pool}
   *
   * @since GemFire 5.0.2
   */
  @Test
  public void test031KeySetOnServer() throws CacheException {
    final String name = getName();

    vm0.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setConcurrencyChecksEnabled(false);
      createRegion(name, factory);
      startBridgeServer(0);
    });
    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    Stream.of(vm1, vm2).forEach(vm -> vm.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      createRegion(name, factory);
    }));

    vm2.invoke("Get keys on server", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      Set keySet = region.keySetOnServer();
      assertThat(keySet).isNotNull();
      assertThat(keySet).isEmpty();
    });

    vm1.invoke("Put values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        region.put(i, i);
      }
    });

    vm2.invoke("Get keys on server", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      Set keySet = region.keySetOnServer();
      assertThat(keySet).isNotNull();
      assertThat(10).isEqualTo(keySet.size());
    });

    Stream.of(vm1, vm2).forEach(vm -> vm.invoke("Close Pool", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    }));
  }

  /**
   * Tests that creating, putting and getting a non-serializable key or value throws the correct
   * (NotSerializableException) exception.
   */
  @Test
  public void test033NotSerializableException() throws CacheException {
    final String name = getName();

    vm0.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getBridgeServerRegionAttributes(null, null);
      createRegion(name, factory);
      startBridgeServer(0);
    });

    final int port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    vm1.invoke("Create region", () -> {
      getLonerSystem();
      getCache();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);

      configureConnectionPool(factory, host0, new int[] {port}, true, -1, -1, null);
      createRegion(name, factory);

      Region<Object, Object> region = getRootRegion().getSubregion(name);

      Throwable thrown =
          catchThrowable(() -> region.create(1, new ConnectionPoolTestNonSerializable()));

      assertThat(thrown).hasCauseInstanceOf(NotSerializableException.class);

      Throwable thrown2 =
          catchThrowable(() -> region.put(1, new ConnectionPoolTestNonSerializable()));
      assertThat(thrown2).hasCauseInstanceOf(NotSerializableException.class);

      Throwable thrown3 =
          catchThrowable(() -> region.get(new ConnectionPoolTestNonSerializable()));
      assertThat(thrown3).hasCauseInstanceOf(NotSerializableException.class);

      region.localDestroyRegion();
    });
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
    final String name = getName();

    disconnectAllFromDS();

    for (VM vm : new VM[] {vm0, vm1}) {
      // Create the cache servers with distributed, mirrored region
      vm.invoke("Create Cache Server", () -> {
        CacheLoader<Object, Object> cl = new CacheLoader<Object, Object>() {
          @Override
          public Object load(LoaderHelper helper) {
            return helper.getKey();
          }

          @Override
          public void close() {

          }
        };
        RegionFactory<Object, Object> factory =
            getBridgeServerMirroredAckRegionAttributes(cl);
        createRegion(name, factory);
        startBridgeServer(0);
      });
    }

    // Create cache server clients
    final int numberOfKeys = 10;
    final String host0 = NetworkUtils.getServerHostName();
    final int vm0Port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    final int vm1Port = vm1.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    Stream.of(vm2, vm3).forEach(vm -> {
      logger.info("before create client");
      vm.invoke("Create Cache Server Client", () -> {
        // reset all static listener variables in case this is being rerun in a subclass
        numberOfAfterInvalidates = 0;
        numberOfAfterCreates = 0;
        numberOfAfterUpdates = 0;
        getLonerSystem();
        // create the region
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.LOCAL);
        factory.setConcurrencyChecksEnabled(false);
        // create bridge writer
        configureConnectionPool(factory, host0, new int[] {vm0Port, vm1Port}, true, -1,
            -1, null);
        createRegion(name, factory);
      });
    });

    // Initialize each client with entries (so that afterInvalidate is called)
    Stream.of(vm2, vm3).forEach(vm -> {
      logger.info("before initialize client");
      vm.invoke("Initialize Client", () -> {
        numberOfAfterInvalidates = 0;
        numberOfAfterCreates = 0;
        numberOfAfterUpdates = 0;
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < numberOfKeys; i++) {
          assertThat("key-" + i).isEqualTo(region.get("key-" + i));
        }
      });
    });

    // Add a CacheListener to both vm2 and vm3
    vm2.invoke("Add CacheListener 1", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      CacheListener<Object, Object> listener = new CacheListenerAdapter<Object, Object>() {
        @Override
        public void afterCreate(EntryEvent e) {
          numberOfAfterCreates++;
          logger.info("vm2 numberOfAfterCreates: " + numberOfAfterCreates);
        }

        @Override
        public void afterUpdate(EntryEvent e) {
          numberOfAfterUpdates++;
          logger.info("vm2 numberOfAfterUpdates: " + numberOfAfterUpdates);
        }

        @Override
        public void afterInvalidate(EntryEvent e) {
          numberOfAfterInvalidates++;
          logger.info("vm2 numberOfAfterInvalidates: " + numberOfAfterInvalidates);
        }
      };
      region.getAttributesMutator().addCacheListener(listener);
      region.registerInterestRegex(".*", false, false);
    });

    vm3.invoke("Add CacheListener 2", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
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
    });

    vm2.invoke(() -> await().untilAsserted(() -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < numberOfKeys; i++) {
        assertThat("key-" + i).isEqualTo(region.get("key-" + i));
      }
    }));

    // Use vm2 to put new values
    // This should cause 10 afterUpdates to vm2 and 10 afterInvalidates to vm3
    vm2.invoke("Put New Values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < 10; i++) {
        region.put("key-" + i, "key-" + i);
      }
    });

    vm2.invoke(() -> {
      await("VM2 should not have received any afterCreate messages")
          .until(() -> ConnectionPoolDUnitTest.getNumberOfAfterCreates() == 0);
      await("VM2 should not have received any afterInvalidate messages")
          .until(() -> ConnectionPoolDUnitTest.getNumberOfAfterInvalidates() == 0);
    });

    long vm2AfterUpdates = vm2.invoke(ConnectionPoolDUnitTest::getNumberOfAfterUpdates);
    long vm3AfterInvalidates = vm3.invoke(ConnectionPoolDUnitTest::getNumberOfAfterInvalidates);
    assertThat(vm2AfterUpdates)
        .describedAs("VM2 received " + vm2AfterUpdates
            + " afterUpdate messages. It should have received " + numberOfKeys)
        .isEqualTo(numberOfKeys);
    assertThat(vm3AfterInvalidates)
        .describedAs("VM3 received " + vm3AfterInvalidates
            + " afterInvalidate messages. It should have received " + numberOfKeys)
        .isEqualTo(numberOfKeys);
  }


  static class DelayListener extends CacheListenerAdapter<Object, Object> {
    private final int delay;

    DelayListener() {
      delay = 25;
    }

    private void delay() {
      try {
        Thread.sleep(delay);
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
    final String name = getName();

    // Create the cache servers with distributed, empty region
    logger.info("before create server");
    vm0.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.EMPTY);
      factory.setConcurrencyChecksEnabled(false);
      createRegion(name, factory);
      startBridgeServer(0);
    });

    // Create cache server client
    final String host0 = NetworkUtils.getServerHostName();
    final int vm0Port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    logger.info("before create client");
    vm1.invoke("Create Cache Server Client", () -> {
      getLonerSystem();
      // create the region
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      // create bridge writer
      configureConnectionPool(factory, host0, new int[] {vm0Port}, true, -1, -1,
          null);
      createRegion(name, factory);
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.registerInterestRegex(".*");
    });

    // now do a tx in the server
    logger.info("before doServerTx");
    vm0.invoke("doServerTx", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      Cache cache1 = getCache();
      CacheTransactionManager txMgr = cache1.getCacheTransactionManager();
      txMgr.begin();
      try {
        region.put("k1", "v1");
        region.put("k2", "v2");
        region.put("k3", "v3");
      } finally {
        txMgr.commit();
      }
    });

    // now verify that the client receives the committed data
    logger.info("before confirmCommitOnClient");
    vm1.invoke("Validate Cache Server Client", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      // wait for a while for us to have the correct number of entries
      await().alias("waiting for region to be size 3").until(() -> region.size() == 3);
      assertThat(region.containsKey("k1")).isTrue();
      assertThat(region.containsKey("k2")).isTrue();
      assertThat(region.containsKey("k3")).isTrue();
      assertThat("v1").isEqualTo(region.getEntry("k1").getValue());
      assertThat("v2").isEqualTo(region.getEntry("k2").getValue());
      assertThat("v3").isEqualTo(region.getEntry("k3").getValue());
    });
  }

  /**
   * Now confirm that a tx done in a peer of a server (the server having an empty region and wanting
   * all events) sends the tx to its clients
   */
  @Test
  public void test038Bug39526part2() throws CacheException {
    disconnectAllFromDS();
    final String name = getName();

    // Create the cache servers with distributed, empty region
    logger.info("before create server");
    vm0.invoke("Create Cache Server", () -> {
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setConcurrencyChecksEnabled(false);
      factory.setDataPolicy(DataPolicy.EMPTY);
      factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      createRegion(name, factory);
      startBridgeServer(0);
    });

    // Create cache server client
    final String host0 = NetworkUtils.getServerHostName();
    final int vm0Port = vm0.invoke(ConnectionPoolDUnitTest::getCacheServerPort);
    logger.info("before create client");
    vm1.invoke("Create Cache Server Client", () -> {
      getLonerSystem();
      // create the region
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      // create bridge writer
      configureConnectionPool(factory, host0, new int[] {vm0Port}, true, -1, -1,
          null);
      createRegion(name, factory);
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.registerInterestRegex(".*");
    });

    logger.info("before create server peer");
    vm2.invoke("Create Server Peer", () -> {
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.EMPTY);
      factory.setConcurrencyChecksEnabled(false);
      createRegion(name, factory);
    });

    // now do a tx in the server
    logger.info("before doServerTx");
    vm2.invoke("doServerTx", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      Cache cache1 = getCache();
      CacheTransactionManager txmgr = cache1.getCacheTransactionManager();
      txmgr.begin();
      try {
        region.put("k1", "v1");
        region.put("k2", "v2");
        region.put("k3", "v3");
      } finally {
        txmgr.commit();
      }
    });

    // now verify that the client receives the committed data
    logger.info("before confirmCommitOnClient");
    vm1.invoke("Validate Cache Server Client", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      // wait for a while for us to have the correct number of entries

      await().alias("waiting for region to be size 3").until(() -> region.size() == 3);
      assertThat(region.containsKey("k1")).isTrue();
      assertThat(region.containsKey("k2")).isTrue();
      assertThat(region.containsKey("k3")).isTrue();
      assertThat("v1").isEqualTo(region.getEntry("k1").getValue());
      assertThat("v2").isEqualTo(region.getEntry("k2").getValue());
      assertThat("v3").isEqualTo(region.getEntry("k3").getValue());
    });
    disconnectAllFromDS();
  }

  @SuppressWarnings("WeakerAccess")
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
    public void fromData(DataInput in) throws IOException {
      index = in.readInt();
    }
  }
}
