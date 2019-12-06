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
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.NoSubscriptionServersAvailableException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.RegisterInterestTracker;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.ClientServerObserver;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Tests Redundancy Level Functionality
 */
@Category({ClientSubscriptionTest.class})
public class RedundancyLevelPart3DUnitTest implements Serializable {

  private static final String K1 = "k1";
  private static final String K2 = "k2";
  private static final String REGION_NAME = "RedundancyLevelTestBase_region";
  private static final AtomicBoolean registerInterestCalled = new AtomicBoolean(false);
  private static final AtomicBoolean makePrimaryCalled = new AtomicBoolean(false);

  private static InternalCache cache;
  private static String hostname;
  private static ClientServerObserver clientServerObserver = null;
  private static PoolImpl pool = null;
  private static int port0;
  private static int port1;
  private static int port2;
  private static int port3;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp() {
    hostname = NetworkUtils.getServerHostName();

    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);

    IgnoredException.addIgnoredException("java.net.SocketException||java.net.ConnectException");

    port0 = vm0.invoke(() -> createServerCache());
    port1 = vm1.invoke(() -> createServerCache());
    port2 = vm2.invoke(() -> createServerCache());
    port3 = vm3.invoke(() -> createServerCache());

    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
        "true");
  }

  @After
  public void tearDown() {
    ClientServerObserverHolder.setInstance(clientServerObserver);

    for (VM vm : toArray(getController(), vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> cacheRule.closeAndNullCache());
    }

    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
        "false");
    disconnectAllFromDS();
  }

  /**
   * This tests failing of a primary server in a situation where the rest of the server are all
   * redundant. After every failure, the order, the dispatcher, the interest registration and the
   * makePrimary calls are verified. The failure detection in these tests could be either through
   * CCU or cache operation, whichever occurs first
   */
  @Test
  public void testRegisterInterestAndMakePrimaryWithFullRedundancy() {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
        "true");

    createClientCache(3);
    createEntriesK1andK2();
    registerK1AndK2();

    assertThat(pool.getRedundantNames().size()).isEqualTo(3);

    vm0.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsAlive);
    vm1.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsNotAlive);
    vm2.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsNotAlive);
    vm3.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsNotAlive);

    for (VM vm : toArray(vm0, vm1, vm2, vm3)) {
      vm.invoke(RedundancyLevelPart3DUnitTest::verifyInterestRegistration);
    }

    PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = true;
    PoolImpl.BEFORE_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = true;
    PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = true;
    registerInterestCalled.set(false);
    makePrimaryCalled.set(false);

    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      @Override
      public void beforeInterestRegistration() {
        registerInterestCalled.set(true);
      }

      @Override
      public void beforeInterestRecovery() {
        registerInterestCalled.set(true);
      }

      @Override
      public void beforePrimaryIdentificationFromBackup() {
        makePrimaryCalled.set(true);
      }
    });

    vm0.invoke(RedundancyLevelPart3DUnitTest::stopServer);
    doPuts();
    vm1.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsAlive);
    vm2.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsNotAlive);
    vm3.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsNotAlive);
    verifyConnectedAndRedundantServers(3, 2);
    assertThat(registerInterestCalled).describedAs(
        "register interest should not have been called since we failed to a redundant server")
        .isFalse();
    assertThat(makePrimaryCalled).describedAs(
        "make primary should have been called since primary did fail and a new primary was to be chosen")
        .isTrue();

    assertThat(pool.getRedundantNames().size()).isEqualTo(2);
    makePrimaryCalled.set(false);
    vm1.invoke(RedundancyLevelPart3DUnitTest::stopServer);
    doPuts();
    vm2.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsAlive);
    vm3.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsNotAlive);
    verifyConnectedAndRedundantServers(2, 1);
    assertThat(registerInterestCalled).describedAs(
        "register interest should not have been called since we failed to a redundant server")
        .isFalse();
    assertThat(makePrimaryCalled).describedAs(
        "make primary should have been called since primary did fail and a new primary was to be chosen")
        .isTrue();

    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    makePrimaryCalled.set(false);
    vm2.invoke(RedundancyLevelPart3DUnitTest::stopServer);
    doPuts();
    vm3.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsAlive);
    verifyConnectedAndRedundantServers(1, 0);

    assertThat(registerInterestCalled).describedAs(
        "register interest should not have been called since we failed to a redundant server")
        .isFalse();
    assertThat(makePrimaryCalled).describedAs(
        "make primary should have been called since primary did fail and a new primary was to be chosen")
        .isTrue();

    assertThat(pool.getRedundantNames().size()).isEqualTo(0);
    vm3.invoke(RedundancyLevelPart3DUnitTest::stopServer);
    vm0.invoke(RedundancyLevelPart3DUnitTest::startServer);
    verifyConnectedAndRedundantServers(1, 0);
    doPuts();
    vm0.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsAlive);
    vm0.invoke(RedundancyLevelPart3DUnitTest::verifyInterestRegistration);
    if (!registerInterestCalled.get()) {
      Assertions
          .fail("register interest should have been called since a recovered server came up");
    }
    assertThat(pool.getRedundantNames().size()).isEqualTo(0);
    PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = false;
    PoolImpl.BEFORE_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false;
    PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = false;
  }

  /**
   * This tests failing of a primary server in a situation where the rest of the server are all non
   * redundant. After every failure, the order, the dispatcher, the interest registration and the
   * makePrimary calls are verified. The failure detection in these tests could be either through
   * CCU or cache operation, whichever occurs first
   */
  @Test
  public void testRegisterInterestAndMakePrimaryWithZeroRedundancy() {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
        "true");
    createClientCache(0);
    createEntriesK1andK2();
    registerK1AndK2();
    assertThat(pool.getRedundantNames().size()).isEqualTo(0);
    vm0.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsAlive);
    vm0.invoke(RedundancyLevelPart3DUnitTest::verifyInterestRegistration);
    vm0.invoke(RedundancyLevelPart3DUnitTest::stopServer);
    verifyConnectedAndRedundantServers(3, 0);
    doPuts();
    vm1.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsAlive);
    vm1.invoke(RedundancyLevelPart3DUnitTest::verifyInterestRegistration);
    assertThat(pool.getRedundantNames().size()).isEqualTo(0);
    vm1.invoke(RedundancyLevelPart3DUnitTest::stopServer);
    verifyConnectedAndRedundantServers(2, 0);
    doPuts();
    vm2.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsAlive);
    vm2.invoke(RedundancyLevelPart3DUnitTest::verifyInterestRegistration);
    assertThat(pool.getRedundantNames().size()).isEqualTo(0);
    vm2.invoke(RedundancyLevelPart3DUnitTest::stopServer);
    verifyConnectedAndRedundantServers(1, 0);
    doPuts();
    vm3.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsAlive);
    vm3.invoke(RedundancyLevelPart3DUnitTest::verifyInterestRegistration);
    assertThat(pool.getRedundantNames().size()).isEqualTo(0);
    vm3.invoke(RedundancyLevelPart3DUnitTest::stopServer);
    vm0.invoke(RedundancyLevelPart3DUnitTest::startServer);
    verifyConnectedAndRedundantServers(1, 0);
    doPuts();
    vm0.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsAlive);
    vm0.invoke(RedundancyLevelPart3DUnitTest::verifyInterestRegistration);
    assertThat(pool.getRedundantNames().size()).isEqualTo(0);

  }

  /**
   * This tests failing of a primary server in a situation where only one of the rest of the servers
   * is redundant. After every failure, the order, the dispatcher, the interest registration and the
   * makePrimary calls are verified. The failure detection in these tests could be either through
   * CCU or cache operation, whichever occurs first
   */
  @Test
  public void testRegisterInterestAndMakePrimaryWithRedundancyOne() {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
        "true");
    createClientCache(1);
    createEntriesK1andK2();
    registerK1AndK2();
    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    vm0.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsAlive);
    vm0.invoke(RedundancyLevelPart3DUnitTest::stopServer);
    doPuts();
    vm1.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsAlive);
    vm2.invoke(RedundancyLevelPart3DUnitTest::verifyCCP);
    vm2.invoke(RedundancyLevelPart3DUnitTest::verifyInterestRegistration);
    verifyConnectedAndRedundantServers(3, 1);
    assertThat(pool.getRedundantNames().size()).isEqualTo(1);
    vm1.invoke(RedundancyLevelPart3DUnitTest::stopServer);
    doPuts();
    vm2.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsAlive);
    vm2.invoke(RedundancyLevelPart3DUnitTest::stopServer);
    doPuts();
    vm3.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsAlive);
    vm3.invoke(RedundancyLevelPart3DUnitTest::verifyInterestRegistration);
    verifyConnectedAndRedundantServers(1, 0);
    assertThat(pool.getRedundantNames().size()).isEqualTo(0);
    vm3.invoke(RedundancyLevelPart3DUnitTest::stopServer);
    vm0.invoke(RedundancyLevelPart3DUnitTest::startServer);
    verifyConnectedAndRedundantServers(1, 0);
    doPuts();
    vm0.invoke(RedundancyLevelPart3DUnitTest::verifyDispatcherIsAlive);
    vm0.invoke(RedundancyLevelPart3DUnitTest::verifyInterestRegistration);
    assertThat(pool.getRedundantNames().size()).isEqualTo(0);
  }

  public static void doPuts() {
    Region<String, String> region = cache.getRegion(REGION_NAME);
    assertThat(region).isNotNull();
    for (int i = 0; i < 4; i++) {
      region.put(K1, K1);
      region.put(K2, K2);
      assertThat(region.get(K1)).isEqualTo(K1);
      assertThat(region.get(K2)).isEqualTo(K2);
    }
  }

  static void verifyDispatcherIsAlive() {
    await().until(() -> cache.getCacheServers().size(), equalTo(1));

    CacheServerImpl cacheServer = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertThat(cacheServer).isNotNull();
    assertThat(cacheServer.getAcceptor()).isNotNull();
    assertThat(cacheServer.getAcceptor().getCacheClientNotifier()).isNotNull();

    final CacheClientNotifier cacheClientNotifier =
        cacheServer.getAcceptor().getCacheClientNotifier();

    await().until(() -> cacheClientNotifier.getClientProxies().size(), greaterThan(0));

    Iterator<CacheClientProxy> cacheClientProxyIterator =
        cacheClientNotifier.getClientProxies().iterator();

    if (cacheClientNotifier.getClientProxies().iterator().hasNext()) {

      final CacheClientProxy proxy = cacheClientProxyIterator.next();
      await()
          .until(() -> proxy._messageDispatcher != null && proxy._messageDispatcher.isAlive());
    }
  }

  static void verifyDispatcherIsNotAlive() {
    await().until(() -> cache.getCacheServers().size(), equalTo(1));

    CacheServerImpl cacheServer = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertThat(cacheServer).isNotNull();
    assertThat(cacheServer.getAcceptor()).isNotNull();
    assertThat(cacheServer.getAcceptor().getCacheClientNotifier()).isNotNull();

    final CacheClientNotifier cacheClientNotifier =
        cacheServer.getAcceptor().getCacheClientNotifier();

    await().until(() -> cacheClientNotifier.getClientProxies().size(),
        greaterThan(0));

    Iterator<CacheClientProxy> cacheClientProxyIterator =
        cacheClientNotifier.getClientProxies().iterator();
    if (cacheClientProxyIterator.hasNext()) {
      CacheClientProxy proxy = cacheClientProxyIterator.next();
      assertThat(proxy._messageDispatcher.isAlive())
          .describedAs("Dispatcher on secondary should not be alive").isFalse();
    }
  }

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
    Region<String, String> region = cache.getRegion(REGION_NAME);
    assertThat(region).isNotNull();
    if (!region.containsKey(K1)) {
      region.create(K1, K1);
    }
    if (!region.containsKey(K2)) {
      region.create(K2, K2);
    }
    assertThat(region.getEntry(K1).getValue()).isEqualTo(K1);
    assertThat(region.getEntry(K2).getValue()).isEqualTo(K2);
  }

  static void registerK1AndK2() {
    Region<Object, Object> region = cache.getRegion(REGION_NAME);
    assertThat(region).isNotNull();
    List<String> list = new ArrayList<>();
    list.add(K1);
    list.add(K2);
    region.registerInterest(list, InterestResultPolicy.KEYS_VALUES);
  }

  static void verifyCCP() {
    await().until(() -> cache.getCacheServers().size(), equalTo(1));

    CacheServerImpl cacheServer = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertThat(cacheServer).isNotNull();
    assertThat(cacheServer.getAcceptor()).isNotNull();
    assertThat(cacheServer.getAcceptor().getCacheClientNotifier()).isNotNull();

    final CacheClientNotifier cacheClientNotifier =
        cacheServer.getAcceptor().getCacheClientNotifier();
    await().until(() -> cacheClientNotifier.getClientProxies().size(),
        equalTo(1));
  }

  static void verifyInterestRegistration() {
    await().until(() -> cache.getCacheServers().size(), equalTo(1));

    CacheServerImpl cacheServer = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertThat(cacheServer).isNotNull();
    assertThat(cacheServer.getAcceptor()).isNotNull();
    assertThat(cacheServer.getAcceptor().getCacheClientNotifier()).isNotNull();

    final CacheClientNotifier cacheClientNotifier =
        cacheServer.getAcceptor().getCacheClientNotifier();
    await("No CacheClientNotifier proxies")
        .until(() -> cacheClientNotifier.getClientProxies().size(), greaterThan(0));

    Iterator<CacheClientProxy> cacheClientProxyIterator =
        cacheClientNotifier.getClientProxies().iterator();

    assertThat(cacheClientProxyIterator.hasNext()).describedAs("A CacheClientProxy was expected")
        .isTrue();
    final CacheClientProxy cacheClientProxy = cacheClientProxyIterator.next();

    await().until(() -> {
      Set<?> keysMap = cacheClientProxy.cils[RegisterInterestTracker.interestListIndex]
          .getProfile(REGION_NAME).getKeysOfInterestFor(cacheClientProxy.getProxyID());
      if (keysMap == null) {
        return false;
      }
      return 2 == keysMap.size();
    });

    Set<?> keysMap = cacheClientProxy.cils[RegisterInterestTracker.interestListIndex]
        .getProfile(REGION_NAME).getKeysOfInterestFor(cacheClientProxy.getProxyID());

    assertThat(keysMap.contains(K1)).isTrue();
    assertThat(keysMap.contains(K2)).isTrue();
  }

  public static void stopServer() {
    Iterator<CacheServer> iterator = cache.getCacheServers().iterator();
    if (iterator.hasNext()) {
      CacheServer server = iterator.next();
      server.stop();
    }
  }

  public static void startServer() throws IOException {
    CacheServerImpl cacheServer = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertThat(cacheServer).isNotNull();
    cacheServer.start();
  }

  public void createClientCache(int redundancy) {
    clientServerObserver =
        ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
          @Override
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

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");

    cache = cacheRule.getOrCreateCache(props);

    pool = (PoolImpl) PoolManager.createFactory().addServer(hostname, port0)
        .addServer(hostname, RedundancyLevelPart3DUnitTest.port1).addServer(hostname,
            RedundancyLevelPart3DUnitTest.port2)
        .addServer(hostname,
            RedundancyLevelPart3DUnitTest.port3)
        .setSubscriptionEnabled(true).setReadTimeout(3000).setSocketBufferSize(32768)
        .setMinConnections(8).setSubscriptionRedundancy(redundancy).setRetryAttempts(5)
        .setPingInterval(10).create("DurableClientReconnectDUnitTestPool");

    RegionFactory<String, String> regionFactory = cache.createRegionFactory();
    regionFactory.setScope(Scope.DISTRIBUTED_ACK);
    regionFactory.setPoolName(pool.getName());
    regionFactory.create(REGION_NAME);
    createEntriesK1andK2();
    registerK1AndK2();
  }

  private int createServerCache() throws Exception {
    cache = cacheRule.getOrCreateCache();

    RegionFactory<String, String> regionFactory =
        cache.createRegionFactory(RegionShortcut.REPLICATE);
    regionFactory.setEnableSubscriptionConflation(true);
    regionFactory.create(REGION_NAME);

    CacheServer cacheServer = cache.addCacheServer();

    cacheServer.setMaximumTimeBetweenPings(180000);
    cacheServer.setPort(0);
    cacheServer.start();
    return cacheServer.getPort();
  }
}
