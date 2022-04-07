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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEMFIRE_PREFIX;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.NoSubscriptionServersAvailableException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.RegisterInterestTracker;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.ClientServerObserver;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Tests Redundancy Level Functionality
 */
@Category({ClientSubscriptionTest.class})
public class RedundancyLevelPart1DUnitTest implements Serializable {

  private static final long TIMEOUT_MILLIS = GeodeAwaitility.getTimeout().toMillis();
  private static final String K1 = "k1";
  private static final String K2 = "k2";
  private static final String REGION_NAME = "RedundancyLevelTestBase_region";
  private static final int CONNECTED_SERVERS = 3;
  private static final int DEFAULT_SOCKET_READ_TIMEOUT = 3000;
  private static final int DEFAULT_RETRY_INTERVAL = 10;
  private static final int DEFAULT_CONNECTED_SERVER_COUNT = 4;

  private static String server0;
  private static String server1;
  private static String server2;
  private static String server3;
  private static PoolImpl pool = null;
  private static AtomicBoolean failOverDetectionByCCU = new AtomicBoolean(false);
  private static ClientServerObserver clientServerObserver = null;
  private static InternalCache cache;
  private static String hostname;

  private final transient CountDownLatch latch = new CountDownLatch(1);

  private int port0;
  private int port1;
  private int port2;
  private int port3;
  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setUp() {
    hostname = NetworkUtils.getServerHostName();

    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);

    IgnoredException.addIgnoredException("java.net.SocketException||java.net.ConnectException");

    port0 = vm0.invoke(this::createServerCache);
    port1 = vm1.invoke(this::createServerCache);
    port2 = vm2.invoke(this::createServerCache);
    port3 = vm3.invoke(this::createServerCache);

    server0 = hostname + port0;
    server1 = hostname + port1;
    server2 = hostname + port2;
    server3 = hostname + port3;

    System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");
  }

  @After
  public void tearDown() {
    latch.countDown();
    if (!failOverDetectionByCCU.get()) {
      ClientServerObserverHolder.setInstance(clientServerObserver);
    }

    for (VM vm : toArray(getController(), vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> cacheRule.closeAndNullCache());
    }

    disconnectAllFromDS();
  }

  /**
   * Redundancy level not specified, an EP which dies of should be removed from the fail over set as
   * well as the live server map
   */
  @Test
  public void testRedundancyNotSpecifiedNonPrimaryServerFail() {

    createClientCache(
        0, DEFAULT_SOCKET_READ_TIMEOUT, DEFAULT_RETRY_INTERVAL);
    vm2.invoke(this::stopServer);
    verifyConnectedAndRedundantServers(0);

    await().untilAsserted(() -> assertThat(pool.getCurrentServerNames()).doesNotContain(server2));
  }

  /**
   * Redundancy level not specified. If an EP which dies of is a Primary EP , then the EP should be
   * removed from the live server map, added to dead server map.
   */
  @Test
  public void testRedundancyNotSpecifiedPrimaryServerFails() {

    // Asif: Increased the socket read timeout to 3000 sec because the registering
    // of keys was timing out sometimes causing fail over to EP4 causing
    // below assertion to fail
    createClientCache(0, 3000, 100);

    await().untilAsserted(() -> assertThat(server0).isEqualTo(pool.getPrimaryName()));

    vm0.invoke(this::stopServer);
    verifyConnectedAndRedundantServers(0);

    await().untilAsserted(() -> {
      assertThat(pool.getCurrentServerNames()).doesNotContain(server0);
      assertThat(pool.getPrimaryName()).isNotEqualTo(server0);
      assertThat(pool.getPrimaryName()).isEqualTo(server1);
    });
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & was not part of the fail over
   * list , then it should be removed from Live Server Map & added to dead server map. It should not
   * change the current failover set. Failover detection by LSM
   */
  @Test
  public void testRedundancySpecifiedNonFailoverEPFails() {
    createClientCache(
        1, DEFAULT_SOCKET_READ_TIMEOUT, DEFAULT_RETRY_INTERVAL);

    await().untilAsserted(() -> {
      assertThat(pool.getConnectedServerCount()).isEqualTo(DEFAULT_CONNECTED_SERVER_COUNT);
      assertThat(pool.getRedundantNames()).hasSize(1);
      assertThat(pool.getRedundantNames()).contains(server1);
    });

    vm2.invoke(this::stopServer);

    await().untilAsserted(() -> assertThat(pool.getRedundantNames()).contains(server1));

    verifyConnectedAndRedundantServers(1);
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & was not part of the fail over
   * list , then it should be removed from Live Server Map & added to dead server map. It should not
   * change the current failover set. Failover detection by Put operation.
   */
  @Test
  public void testRedundancySpecifiedNonFailoverEPFailsDetectionByPut() {
    createClientCache(1, 500, 1000);

    await().untilAsserted(() -> {
      assertThat(pool.getConnectedServerCount()).isEqualTo(DEFAULT_CONNECTED_SERVER_COUNT);
      assertThat(pool.getRedundantNames()).hasSize(1);
      assertThat(pool.getRedundantNames()).contains(server1);
    });

    vm2.invoke(this::stopServer);
    doPuts();

    await().untilAsserted(() -> assertThat(pool.getRedundantNames()).contains(server1));

    verifyConnectedAndRedundantServers(1);
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part of the fail over list
   * , then it should be removed from live server map & added to dead server map. A new EP should be
   * picked from the Live Server Map to compensate for the failure. Failure Detection by LSM.
   */
  @Test
  public void testRedundancySpecifiedNonPrimaryEPFails() {
    createClientCache(
        1, DEFAULT_SOCKET_READ_TIMEOUT, DEFAULT_RETRY_INTERVAL);

    await().untilAsserted(() -> {
      assertThat(pool.getConnectedServerCount()).isEqualTo(DEFAULT_CONNECTED_SERVER_COUNT);
      assertThat(pool.getRedundantNames().size()).isEqualTo(1);
      assertThat(pool.getRedundantNames().contains(server1)).isTrue();
      assertThat(server0).isEqualTo(pool.getPrimaryName());
    });

    vm1.invoke(this::stopServer);

    await().untilAsserted(() -> assertThat(pool.getRedundantNames()).contains(server2));

    verifyConnectedAndRedundantServers(1);
    vm2.invoke(this::verifyInterestRegistration);
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part of the fail over list
   * , then it should be removed from live server map & added to dead server map. A new EP should be
   * picked from the Live Server Map to compensate for the failure. Failure Detection by CCU.
   */
  @Test
  public void testRedundancySpecifiedNonPrimaryEPFailsDetectionByCCU() {
    failOverDetectionByCCU.set(true);
    createClientCache(1, 250, 500);

    await().untilAsserted(() -> {
      assertThat(pool.getConnectedServerCount()).isEqualTo(DEFAULT_CONNECTED_SERVER_COUNT);
      assertThat(pool.getRedundantNames().size()).isEqualTo(1);
      assertThat(pool.getRedundantNames().contains(server1)).isTrue();
      assertThat(server0).isEqualTo(pool.getPrimaryName());
    });

    vm1.invoke(this::stopServer);

    await().untilAsserted(() -> assertThat(pool.getRedundantNames()).contains(server2));

    verifyConnectedAndRedundantServers(1);
    vm2.invoke(this::verifyInterestRegistration);
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part of the fail over list
   * , then it should be removed from live server map & added to dead server map. A new EP should be
   * picked from the Live Server Map to compensate for the failure. Failure Detection by Register
   * Interest.
   */
  @Test
  public void testRedundancySpecifiedNonPrimaryEPFailsDetectionByRegisterInterest() {

    createClientCache(1, 250, 500);

    await().untilAsserted(() -> {
      assertThat(pool.getConnectedServerCount()).isEqualTo(DEFAULT_CONNECTED_SERVER_COUNT);
      assertThat(pool.getRedundantNames().size()).isEqualTo(1);
      assertThat(pool.getRedundantNames().contains(server1)).isTrue();
      assertThat(server0).isEqualTo(pool.getPrimaryName());
    });

    vm1.invoke(this::stopServer);
    createEntriesK1andK2();
    registerK1AndK2();

    await().untilAsserted(() -> assertThat(pool.getRedundantNames()).contains(server2));

    verifyConnectedAndRedundantServers(1);
    vm2.invoke(this::verifyInterestRegistration);
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part of the fail over list
   * , then it should be removed from live server map & added to dead server map. A new EP should be
   * picked from the Live Server Map to compensate for the failure. Failure Detection by Unregister
   * Interest.
   */
  @Test
  public void testRedundancySpecifiedNonPrimaryEPFailsDetectionByUnregisterInterest() {

    createClientCache(1, 250, 500);

    await().untilAsserted(() -> {
      assertThat(pool.getConnectedServerCount()).isEqualTo(DEFAULT_CONNECTED_SERVER_COUNT);
      assertThat(pool.getRedundantNames().size()).isEqualTo(1);
      assertThat(pool.getRedundantNames().contains(server1)).isTrue();
      assertThat(server0).isEqualTo(pool.getPrimaryName());
    });

    vm1.invoke(this::stopServer);
    unregisterInterest();

    await().untilAsserted(() -> assertThat(pool.getRedundantNames()).contains(server2));

    verifyConnectedAndRedundantServers(1);
  }

  /**
   * Redundancy level specified & less than total Eps. If an EP dies & is part of the fail over list
   * , then it should be removed from live server map & added to dead server map. A new EP should be
   * picked from the Live Server Map to compensate for the failure. Failure Detection by Put
   * operation.
   */
  @Test
  public void testRedundancySpecifiedNonPrimaryEPFailsDetectionByPut() {

    createClientCache(1, 250, 500);

    await().untilAsserted(() -> {
      assertThat(pool.getConnectedServerCount()).isEqualTo(DEFAULT_CONNECTED_SERVER_COUNT);
      assertThat(pool.getRedundantNames().size()).isEqualTo(1);
      assertThat(pool.getRedundantNames().contains(server1)).isTrue();
      assertThat(server0).isEqualTo(pool.getPrimaryName());
    });

    vm1.invoke(this::stopServer);
    doPuts();

    await().untilAsserted(() -> assertThat(pool.getRedundantNames()).contains(server2));

    verifyConnectedAndRedundantServers(1);
    vm2.invoke(this::verifyInterestRegistration);

  }

  private void doPuts() {
    Region<String, String> region = cache.getRegion(REGION_NAME);
    assertThat(region).isNotNull();
    region.put(K1, K1);
    region.put(K2, K2);
    assertThat(region.get(K1)).isEqualTo(K1);
    assertThat(region.get(K2)).isEqualTo(K2);
  }

  private void verifyConnectedAndRedundantServers(final int redundantServers) {
    if (redundantServers < 0) {
      throw new IllegalArgumentException("can't test for < 0 redundant server via API");
    }
    await(
        "Live server count didn't match expected and/or redundant server count didn't match expected in time")
            .until(() -> {
              try {
                return pool.getConnectedServerCount() == CONNECTED_SERVERS
                    && pool.getRedundantNames().size() == redundantServers;
              } catch (final NoSubscriptionServersAvailableException e) {
                // when zero connected servers are actually available, we'll see this error
              }
              return false;
            });
  }

  private void createEntriesK1andK2() {
    Region<String, String> r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);

    assertThat(r1).isNotNull();

    if (!r1.containsKey(K1)) {
      r1.create(K1, K1);
    }
    if (!r1.containsKey(K2)) {
      r1.create(K2, K2);
    }

    assertThat(r1.getEntry(K1).getValue()).isEqualTo(K1);
    assertThat(r1.getEntry(K2).getValue()).isEqualTo(K2);
  }

  private void registerK1AndK2() {
    Region<Object, Object> region = cache.getRegion(REGION_NAME);
    assertThat(region).isNotNull();
    List<String> list = new ArrayList<>();
    list.add(K1);
    list.add(K2);
    region.registerInterest(list, InterestResultPolicy.KEYS_VALUES);
  }

  private static void unregisterInterest() {
    Region<String, String> r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r.unregisterInterest("k1");
  }

  private void stopServer() {
    Iterator<CacheServer> iterator = cache.getCacheServers().iterator();
    if (iterator.hasNext()) {
      CacheServer server = iterator.next();
      server.stop();
    }
  }

  private void createClientCache(int redundancy, int socketReadTimeout, int retryInterval) {
    if (!failOverDetectionByCCU.get()) {
      clientServerObserver =
          ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
            @Override
            public void beforeFailoverByCacheClientUpdater(ServerLocation epFailed) {
              try {
                latch.await(TIMEOUT_MILLIS, MILLISECONDS);
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

    cache = cacheRule.getOrCreateCache(props);

    pool = (PoolImpl) PoolManager.createFactory()
        .addServer(hostname, port0)
        .addServer(hostname, port1)
        .addServer(hostname, port2)
        .addServer(hostname, port3)
        .setSubscriptionEnabled(true)
        .setReadTimeout(socketReadTimeout)
        .setSocketBufferSize(32768)
        .setMinConnections(8)
        .setSubscriptionRedundancy(redundancy)
        .setRetryAttempts(5)
        .setPingInterval(retryInterval)
        .create("DurableClientReconnectDUnitTestPool");

    cache.createRegionFactory()
        .setScope(Scope.DISTRIBUTED_ACK)
        .setPoolName(pool.getName())
        .create(REGION_NAME);

    createEntriesK1andK2();
    registerK1AndK2();
  }

  private int createServerCache() throws Exception {
    cache = cacheRule.getOrCreateCache();

    cache.createRegionFactory(RegionShortcut.REPLICATE).setEnableSubscriptionConflation(true)
        .create(REGION_NAME);

    CacheServer cacheServer = cache.addCacheServer();

    cacheServer.setMaximumTimeBetweenPings(180000);
    cacheServer.setPort(0);
    cacheServer.start();

    return cacheServer.getPort();
  }

  private void verifyInterestRegistration() {
    await().untilAsserted(() -> assertThat(cache.getCacheServers()).hasSize(1));

    InternalCacheServer cacheServer =
        (InternalCacheServer) cache.getCacheServers().iterator().next();
    assertThat(cacheServer).isNotNull();
    assertThat(cacheServer.getAcceptor()).isNotNull();
    assertThat(cacheServer.getAcceptor().getCacheClientNotifier()).isNotNull();

    CacheClientNotifier cacheClientNotifier =
        cacheServer.getAcceptor().getCacheClientNotifier();
    await().untilAsserted(
        () -> assertThat(cacheClientNotifier.getClientProxies().size()).isGreaterThan(0));

    Iterator<CacheClientProxy> cacheClientProxyIterator =
        cacheClientNotifier.getClientProxies().iterator();

    assertThat(cacheClientProxyIterator.hasNext()).describedAs("A CacheClientProxy was expected")
        .isTrue();
    CacheClientProxy cacheClientProxy = cacheClientProxyIterator.next();

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

}
