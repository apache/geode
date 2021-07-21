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
package org.apache.geode.cache30;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/**
 * Provides helper methods for testing clients and servers. This test case was created by
 * refactoring methods from ConnectionPoolDUnitTest into this class.
 *
 * @since GemFire 4.2.1
 */
public abstract class ClientServerTestCase extends JUnit4CacheTestCase {

  public static final String NON_EXISTENT_KEY = "NON_EXISTENT_KEY";

  public static boolean AUTO_LOAD_BALANCE = false;
  public static final String TEST_POOL_NAME = "testPool";



  @Override
  public final void postSetUp() throws Exception {
    // this makes sure we don't have any connection left over from previous tests
    disconnectAllFromDS();
    postSetUpClientServerTestCase();
  }

  protected void postSetUpClientServerTestCase() throws Exception {}

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    preTearDownClientServerTestCase();
    // this makes sure we don't leave anything for the next tests
    disconnectAllFromDS();
  }

  protected void preTearDownClientServerTestCase() throws Exception {}

  /**
   * Starts a cache server on the given port
   *
   * @since GemFire 4.0
   */
  public int startBridgeServer(int port) throws IOException {

    Cache cache = getCache();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.setMaxThreads(getMaxThreads());
    bridge.start();
    return bridge.getPort();
  }

  /**
   * Defaults to 0 which means no selector in server. Subclasses can override setting this to a
   * value > 0 to enable selector.
   */
  public int getMaxThreads() {
    return 0;
  }

  /**
   * Stops the cache server that serves up the given cache.
   *
   * @since GemFire 4.0
   */
  public void stopBridgeServers(Cache cache) {
    CacheServer bridge;
    for (CacheServer cacheServer : cache.getCacheServers()) {
      bridge = cacheServer;
      bridge.stop();
      assertThat(bridge.isRunning()).isFalse();
    }
  }

  /**
   * Returns region attributes for a <code>LOCAL</code> region
   */
  protected <K, V> RegionAttributes<K, V> getRegionAttributes() {
    AttributesFactory<K, V> factory = new AttributesFactory<>();
    factory.setScope(Scope.LOCAL);
    return factory.create();
  }

  public static Pool configureConnectionPool(AttributesFactory factory, String host, int port1,
      int port2, boolean establish, int redundancy, int connectionsPerServer, String serverGroup,
      int pingInterval, int idleTimeout, int lifetimeTimeout) {
    int[] ports;
    if (port2 != -1) {
      ports = new int[] {port1, port2};
    } else {
      ports = new int[] {port1};
    }
    return configureConnectionPool(factory, host, ports, establish, redundancy,
        connectionsPerServer, serverGroup, pingInterval, idleTimeout, lifetimeTimeout);
  }

  public static Pool configureConnectionPool(AttributesFactory factory, String host, int port1,
      int port2, boolean establish, int redundancy, int connectionsPerServer, String serverGroup) {
    return configureConnectionPool(factory, host, port1, port2, establish, redundancy,
        connectionsPerServer, serverGroup, -1, -1,
        -2);
  }

  public static Pool configureConnectionPool(AttributesFactory factory, String host, int[] ports,
      boolean establish, int redundancy, int connectionsPerServer, String serverGroup) {
    return configureConnectionPool(factory, host, ports, establish, redundancy,
        connectionsPerServer, serverGroup, -1/* pingInterval */, -1/* idleTimeout */,
        -2/* lifetimeTimeout */);
  }

  public static Pool configureConnectionPool(AttributesFactory factory, String host, int[] ports,
      boolean establish, int redundancy, int connectionsPerServer, String serverGroup,
      int pingInterval, int idleTimeout, int lifetimeTimeout) {
    return configureConnectionPoolWithNameAndFactory(factory, host, ports, establish, redundancy,
        connectionsPerServer, serverGroup, null, PoolManager.createFactory(), pingInterval,
        idleTimeout, lifetimeTimeout, -1);
  }

  public static Pool configureConnectionPool(AttributesFactory factory, String host, int[] ports,
      boolean establish, int redundancy, int connectionsPerServer, String serverGroup,
      int pingInterval, int idleTimeout, int lifetimeTimeout, int statisticInterval) {
    return configureConnectionPoolWithNameAndFactory(factory, host, ports, establish, redundancy,
        connectionsPerServer, serverGroup, null, PoolManager.createFactory(), pingInterval,
        idleTimeout, lifetimeTimeout, statisticInterval);
  }

  public static Pool configureConnectionPoolWithNameAndFactory(AttributesFactory factory,
      String host, int[] ports, boolean establish, int redundancy, int connectionsPerServer,
      String serverGroup, String poolName, PoolFactory pf) {
    return configureConnectionPoolWithNameAndFactory(factory, host, ports, establish, redundancy,
        connectionsPerServer, serverGroup, poolName, pf, -1, -1, -2, -1);
  }

  /**
   * this method creates a client connection pool and configures it. If the ports array is not empty
   * it is used to configure the client pool. Otherwise the pool is configured to use the dunit
   * locator.
   */
  public static Pool configureConnectionPoolWithNameAndFactory(AttributesFactory factory,
      String host, int[] ports, boolean establish, int redundancy, int connectionsPerServer,
      String serverGroup, String poolName, PoolFactory pf, int pingInterval, int idleTimeout,
      int lifetimeTimeout, int statisticInterval) {

    if (AUTO_LOAD_BALANCE || ports.length == 0) {
      pf.addLocator(host, DistributedTestUtils.getLocatorPort());
    } else {
      for (int port : ports) {
        pf.addServer(host, port);
      }
    }

    // TODO - probably should pass in minConnections rather than connections per server
    if (connectionsPerServer != -1 && ports != null) {
      pf.setMinConnections(connectionsPerServer * ports.length);
    }
    if (pingInterval != -1) {
      pf.setPingInterval(pingInterval);
    }
    if (idleTimeout != -1) {
      pf.setIdleTimeout(idleTimeout);
    }
    if (statisticInterval != -1) {
      pf.setStatisticInterval(statisticInterval);
    }
    if (lifetimeTimeout != -2) {
      pf.setLoadConditioningInterval(lifetimeTimeout);
    }
    if (establish) {
      pf.setSubscriptionEnabled(true);
      pf.setSubscriptionRedundancy(redundancy);
      pf.setSubscriptionAckInterval(1);
    }
    if (serverGroup != null) {
      pf.setServerGroup(serverGroup);
    }
    String rpoolName = TEST_POOL_NAME;
    if (poolName != null) {
      rpoolName = poolName;
    }
    Pool pool = pf.create(rpoolName);
    if (factory != null) {
      factory.setPoolName(rpoolName);
    }
    return pool;
  }


  public static <K, V> Pool configureConnectionPool(RegionFactory<K, V> factory, String host,
      int[] ports, boolean establish, int redundancy, int connectionsPerServer,
      String serverGroup) {
    return configureConnectionPoolWithNameAndFactory(factory, host, ports, establish, redundancy,
        connectionsPerServer, serverGroup, TEST_POOL_NAME, PoolManager.createFactory(), -1, -1, -2,
        -1);
  }

  public static <K, V> Pool configureConnectionPoolWithNameAndFactory(RegionFactory<K, V> factory,
      String host, int[] ports, boolean establish, int redundancy, int connectionsPerServer,
      String serverGroup, String poolName, PoolFactory pf) {
    return configureConnectionPoolWithNameAndFactory(factory, host, ports, establish, redundancy,
        connectionsPerServer, serverGroup, poolName, pf, -1, -1, -2, -1);
  }

  public static <K, V> Pool configureConnectionPoolWithNameAndFactory(RegionFactory<K, V> factory,
      String host, int[] ports, boolean establish, int redundancy, int connectionsPerServer,
      String serverGroup, String poolName, PoolFactory pf, int pingInterval, int idleTimeout,
      int lifetimeTimeout, int statisticInterval) {

    if (AUTO_LOAD_BALANCE || ports.length == 0) {
      pf.addLocator(host, DistributedTestUtils.getLocatorPort());
    } else {
      for (int port : ports) {
        pf.addServer(host, port);
      }
    }

    // TODO - probably should pass in minConnections rather than connections per server
    if (connectionsPerServer != -1 && ports != null) {
      pf.setMinConnections(connectionsPerServer * ports.length);
    }
    if (pingInterval != -1) {
      pf.setPingInterval(pingInterval);
    }
    if (idleTimeout != -1) {
      pf.setIdleTimeout(idleTimeout);
    }
    if (statisticInterval != -1) {
      pf.setStatisticInterval(statisticInterval);
    }
    if (lifetimeTimeout != -2) {
      pf.setLoadConditioningInterval(lifetimeTimeout);
    }
    if (establish) {
      pf.setSubscriptionEnabled(true);
      pf.setSubscriptionRedundancy(redundancy);
      pf.setSubscriptionAckInterval(1);
    }
    if (serverGroup != null) {
      pf.setServerGroup(serverGroup);
    }
    String rpoolName = TEST_POOL_NAME;
    if (poolName != null) {
      rpoolName = poolName;
    }
    Pool pool = pf.create(rpoolName);
    if (factory != null) {
      factory.setPoolName(rpoolName);
    }
    return pool;
  }



  public static DistributedMember getMemberId() {
    await("Waiting for client to connect " + getSystemStatic().getMemberId())
        .until(() -> getSystemStatic().getDistributedMember().getMembershipPort() > 0);
    return getSystemStatic().getDistributedMember();
  }

  public static class CacheServerCacheLoader extends TestCacheLoader<Object, Object> {

    public CacheServerCacheLoader() {}

    @Override
    public Object load2(LoaderHelper helper) {
      if (helper.getArgument() instanceof Integer) {
        try {
          Thread.sleep((Integer) helper.getArgument());
        } catch (InterruptedException ugh) {
          fail("interrupted");
        }
      }
      Object ret = helper.getKey();

      if (ret instanceof String) {
        if (ret != null && ret.equals(NON_EXISTENT_KEY)) {
          return null;
        }
      }
      return ret;

    }

  }

  private static final String BridgeServerKey = "BridgeServerKey";

  /**
   * Create a server that has a value for every key queried and a unique key/value in the specified
   * Region that uniquely identifies each instance.
   *
   * @param vm the VM on which to create the server
   * @param rName the name of the Region to create on the server
   * @param port the TCP port on which the server should listen
   */
  protected void createBridgeServer(VM vm, final String rName, final int port) {
    vm.invoke("Create Region on Server", new CacheSerializableRunnable() {
      @Override
      public void run2() {
        try {
          AttributesFactory<Object, Object> factory = new AttributesFactory<>();
          factory.setScope(Scope.DISTRIBUTED_ACK); // can't be local since used with
                                                   // registerInterest
          factory.setCacheLoader(new CacheServerCacheLoader());
          beginCacheXml();
          createRootRegion(rName, factory.create());
          startBridgeServer(port);
          finishCacheXml(rName + "-" + port);

          Region<Object, Object> region = getRootRegion(rName);
          assertThat(region).isNotNull();
          region.put(BridgeServerKey, port); // A unique key/value to identify the
                                             // BridgeServer
        } catch (Exception e) {
          getSystem().getLogWriter().severe(e);
          fail("Failed to start CacheServer " + e);
        }
      }
    });
  }

  protected static int[] createUniquePorts() {
    return AvailablePortHelper.getRandomAvailableTCPPorts(1);
  }

}
