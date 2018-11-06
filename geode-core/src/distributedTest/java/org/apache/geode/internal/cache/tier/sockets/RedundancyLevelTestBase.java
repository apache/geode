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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.NoSubscriptionServersAvailableException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.RegisterInterestTracker;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.ClientServerObserver;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Tests Redundancy Level Functionality
 */
@Category({ClientSubscriptionTest.class})
public class RedundancyLevelTestBase extends JUnit4DistributedTestCase {

  static volatile boolean registerInterestCalled = false;
  static volatile boolean makePrimaryCalled = false;

  static Cache cache = null;

  VM server0 = null;
  VM server1 = null;
  VM server2 = null;
  VM server3 = null;

  static int PORT1;
  static int PORT2;
  static int PORT3;
  static int PORT4;

  static String SERVER1;
  static String SERVER2;
  static String SERVER3;
  static String SERVER4;

  private static final String k1 = "k1";
  private static final String k2 = "k2";

  private static final String REGION_NAME = "RedundancyLevelTestBase_region";

  static PoolImpl pool = null;

  private static ClientServerObserver oldBo = null;

  static boolean FailOverDetectionByCCU = false;

  @BeforeClass
  public static void caseSetUp() throws Exception {
    disconnectAllFromDS();
  }

  @Override
  public final void postSetUp() {
    server0 = VM.getVM(0);
    server1 = VM.getVM(1);
    server2 = VM.getVM(2);
    server3 = VM.getVM(3);

    IgnoredException.addIgnoredException("java.net.SocketException||java.net.ConnectException");

    // start servers first
    PORT1 = server0.invoke(RedundancyLevelTestBase::createServerCache);
    PORT2 = server1.invoke(RedundancyLevelTestBase::createServerCache);
    PORT3 = server2.invoke(RedundancyLevelTestBase::createServerCache);
    PORT4 = server3.invoke(RedundancyLevelTestBase::createServerCache);

    String hostName = NetworkUtils.getServerHostName();
    SERVER1 = hostName + PORT1;
    SERVER2 = hostName + PORT2;
    SERVER3 = hostName + PORT3;
    SERVER4 = hostName + PORT4;

    CacheServerTestUtil.disableShufflingOfEndpoints();
  }

  public static void doPuts() throws InterruptedException {
    putEntriesK1andK2();
    putEntriesK1andK2();
    putEntriesK1andK2();
    putEntriesK1andK2();
  }

  private static void putEntriesK1andK2() throws InterruptedException {
    Region<String, String> r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertThat(r1).isNotNull();
    r1.put(k1, k1);
    r1.put(k2, k2);
    assertThat(r1.get(k1)).isEqualTo(k1);
    assertThat(r1.get(k2)).isEqualTo(k2);
  }

  static void verifyDispatcherIsAlive() {

    await()
        .until(() -> cache.getCacheServers().size(), equalTo(1));

    CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertThat(bs).isNotNull();
    assertThat(bs.getAcceptor()).isNotNull();
    assertThat(bs.getAcceptor().getCacheClientNotifier()).isNotNull();

    final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();

    await().until(() -> ccn.getClientProxies().size(),
        greaterThan(0));

    Iterator<CacheClientProxy> cacheClientProxyIterator = ccn.getClientProxies().iterator();

    if (ccn.getClientProxies().iterator().hasNext()) {

      final CacheClientProxy proxy = cacheClientProxyIterator.next();
      await()
          .until(() -> proxy._messageDispatcher != null && proxy._messageDispatcher.isAlive());
    }
  }

  static void verifyDispatcherIsNotAlive() {

    await()
        .until(() -> cache.getCacheServers().size(), equalTo(1));

    CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertThat(bs).isNotNull();
    assertThat(bs.getAcceptor()).isNotNull();
    assertThat(bs.getAcceptor().getCacheClientNotifier()).isNotNull();

    final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();

    await().until(() -> ccn.getClientProxies().size(),
        greaterThan(0));

    Iterator<CacheClientProxy> cacheClientProxyIterator = ccn.getClientProxies().iterator();
    if (cacheClientProxyIterator.hasNext()) {
      CacheClientProxy proxy = cacheClientProxyIterator.next();
      assertThat(proxy._messageDispatcher.isAlive())
          .describedAs("Dispatcher on secondary should not be alive").isFalse();
    }
  }

  static void verifyRedundantServersContain(final String server) {

    await()
        .until(() -> pool.getRedundantNames().contains(server));
  }

  /**
   * @param connectedServers is the expected number of connected servers (1 or more)
   * @param redundantServers is the expected number of redundant servers (0 or more)
   */
  static void verifyConnectedAndRedundantServers(final int connectedServers,
      final int redundantServers) {
    if (connectedServers < 1) {
      throw new IllegalArgumentException("can't test for < 1 connected server via API");
    }
    if (redundantServers < 0) {
      throw new IllegalArgumentException("can't test for < 0 redundant server via API");
    }
    await(
        "Live server count didn't match expected and/or redundant server count didn't match expected in time")
            .until(() -> {
              try {
                return pool.getConnectedServerCount() == connectedServers
                    && pool.getRedundantNames().size() == redundantServers;
              } catch (final NoSubscriptionServersAvailableException e) {
                // when zero connected servers are actually available, we'll see this error
              }
              return false;
            });
  }

  public static void createEntriesK1andK2() {
    Region<String, String> r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertThat(r1).isNotNull();
    if (!r1.containsKey(k1)) {
      r1.create(k1, k1);
    }
    if (!r1.containsKey(k2)) {
      r1.create(k2, k2);
    }
    assertThat(r1.getEntry(k1).getValue()).isEqualTo(k1);
    assertThat(r1.getEntry(k2).getValue()).isEqualTo(k2);
  }

  static void registerK1AndK2() {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertThat(r).isNotNull();
    List<String> list = new ArrayList<>();
    list.add(k1);
    list.add(k2);
    r.registerInterest(list, InterestResultPolicy.KEYS_VALUES);
  }

  public static void unregisterInterest() {
    Region<String, String> r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.unregisterInterest("k1");
  }

  static void verifyNoCCP() {
    assertThat(cache.getCacheServers().size()).describedAs("More than one BridgeServer")
        .isEqualTo(1);
    CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();

    assertThat(bs).isNotNull();
    assertThat(bs.getAcceptor()).isNotNull();
    assertThat(bs.getAcceptor().getCacheClientNotifier()).isNotNull();

    // no client is connected to this server
    assertThat(bs.getAcceptor().getCacheClientNotifier().getClientProxies().size()).isEqualTo(0);
  }

  static void verifyCCP() {
    await()
        .until(() -> cache.getCacheServers().size(), equalTo(1));
    CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();

    assertThat(bs).isNotNull();
    assertThat(bs.getAcceptor()).isNotNull();
    assertThat(bs.getAcceptor().getCacheClientNotifier()).isNotNull();

    // one client is connected to this server
    final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();
    await().until(() -> ccn.getClientProxies().size(),
        equalTo(1));
  }

  static void verifyInterestRegistration() {
    await("Number of cache servers (" + cache.getCacheServers().size() + ") never became 1")
        .until(() -> cache.getCacheServers().size(), equalTo(1));

    CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertThat(bs).isNotNull();
    assertThat(bs.getAcceptor()).isNotNull();
    assertThat(bs.getAcceptor().getCacheClientNotifier()).isNotNull();

    final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();
    await("Notifier's proxies is empty")
        .until(() -> ccn.getClientProxies().size(), greaterThan(0));

    Iterator<CacheClientProxy> cacheClientProxyIterator = ccn.getClientProxies().iterator();

    assertThat(cacheClientProxyIterator.hasNext()).describedAs("A CCP was expected . Wasn't it?")
        .isTrue();
    final CacheClientProxy ccp = cacheClientProxyIterator.next();

    await().until(() -> {
      Set keysMap = ccp.cils[RegisterInterestTracker.interestListIndex]
          .getProfile(Region.SEPARATOR + REGION_NAME).getKeysOfInterestFor(ccp.getProxyID());
      if (keysMap == null) {
        return false;
      }
      if (keysMap.size() != 2) {
        return false;
      }
      return true;
    });

    Set keysMap = ccp.cils[RegisterInterestTracker.interestListIndex]
        .getProfile(Region.SEPARATOR + REGION_NAME).getKeysOfInterestFor(ccp.getProxyID());
    assertThat(keysMap.contains(k1)).isTrue();
    assertThat(keysMap.contains(k2)).isTrue();
  }

  public static void stopServer() {
    Iterator<CacheServer> iterator = cache.getCacheServers().iterator();
    if (iterator.hasNext()) {
      CacheServer server = iterator.next();
      server.stop();
    }
  }

  public static void startServer() throws IOException {
    Cache c = CacheFactory.getAnyInstance();
    CacheServerImpl bs = (CacheServerImpl) c.getCacheServers().iterator().next();
    assertThat(bs).isNotNull();
    bs.start();
  }

  private void createCache(Properties props) {
    DistributedSystem ds = getSystem(props);
    assertThat(ds).isNotNull();
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertThat(cache).isNotNull();
  }


  public static void createClientCache(String host, int port1, int port2, int port3, int port4,
      int redundancy) throws Exception {
    createClientCache(host, port1, port2, port3, port4, redundancy,
        3000, /* default socket timeout of 250 milliseconds */
        10 /* default retry interval */);
  }

  public static void createClientCache(String host, int port1, int port2, int port3, int port4,
      int redundancy, int socketReadTimeout, int retryInterval)
      throws Exception {
    if (!FailOverDetectionByCCU) {
      oldBo = ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
        public void beforeFailoverByCacheClientUpdater(ServerLocation epFailed) {
          try {
            Thread.sleep(300000);
          } catch (InterruptedException ie) {
            // expected - test will shut down the cache which will interrupt
            // the CacheClientUpdater thread that invoked this method
            Thread.currentThread().interrupt();
          }
        }
      });
    }

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new RedundancyLevelTestBase().createCache(props);

    PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(host, PORT1)
        .addServer(host, PORT2).addServer(host, PORT3).addServer(host, PORT4)
        .setSubscriptionEnabled(true).setReadTimeout(socketReadTimeout).setSocketBufferSize(32768)
        .setMinConnections(8).setSubscriptionRedundancy(redundancy).setRetryAttempts(5)
        .setPingInterval(retryInterval).create("DurableClientReconnectDUnitTestPool");

    AttributesFactory<String, String> factory = new AttributesFactory<>();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    RegionAttributes<String, String> attrs = factory.createRegionAttributes();
    cache.createRegion(REGION_NAME, attrs);
    pool = p;
    createEntriesK1andK2();
    registerK1AndK2();
  }

  private static Integer createServerCache() throws Exception {
    new RedundancyLevelTestBase().createCache(new Properties());
    AttributesFactory<String, String> factory = new AttributesFactory<>();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEnableSubscriptionConflation(true);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes<String, String> attrs = factory.createRegionAttributes();
    cache.createVMRegion(REGION_NAME, attrs);

    CacheServer server1 = cache.addCacheServer();

    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setMaximumTimeBetweenPings(180000);
    server1.setPort(port);
    server1.start();
    return server1.getPort();
  }

  static void verifyOrderOfEndpoints() {}

  @Override
  public final void preTearDown() {
    try {
      if (!FailOverDetectionByCCU) {
        ClientServerObserverHolder.setInstance(oldBo);
      }

      FailOverDetectionByCCU = false;

      // close the clients first
      closeCache();

      // then close the servers
      server0.invoke(RedundancyLevelTestBase::closeCache);
      server1.invoke(RedundancyLevelTestBase::closeCache);
      server2.invoke(RedundancyLevelTestBase::closeCache);
      server3.invoke(RedundancyLevelTestBase::closeCache);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    CacheServerTestUtil.resetDisableShufflingOfEndpointsFlag();
  }

  private static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}
